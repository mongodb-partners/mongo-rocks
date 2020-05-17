/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "rocks_begin_transaction_block.h"
#include "rocks_recovery_unit.h"
#include "rocks_snapshot_manager.h"

#include <rocksdb/db.h>

#include "mongo/base/checked_cast.h"
#include "mongo/db/server_options.h"
#include "mongo/util/log.h"

namespace mongo {
    void RocksSnapshotManager::setCommittedSnapshot(const Timestamp& ts) {
        stdx::lock_guard<stdx::mutex> lock(_committedSnapshotMutex);

        invariant(!_committedSnapshot || *_committedSnapshot <= ts);
        _committedSnapshot = ts;
    }

    void RocksSnapshotManager::setLocalSnapshot(const Timestamp& timestamp) {
        stdx::lock_guard<stdx::mutex> lock(_localSnapshotMutex);
        _localSnapshot = timestamp;
    }

    boost::optional<Timestamp> RocksSnapshotManager::getLocalSnapshot() {
        stdx::lock_guard<stdx::mutex> lock(_localSnapshotMutex);
        return _localSnapshot;
    }

    void RocksSnapshotManager::dropAllSnapshots() {
        stdx::lock_guard<stdx::mutex> lock(_committedSnapshotMutex);
        _committedSnapshot = boost::none;
    }

    boost::optional<Timestamp> RocksSnapshotManager::getMinSnapshotForNextCommittedRead() const {
        if (!serverGlobalParams.enableMajorityReadConcern) {
            return boost::none;
        }

        stdx::lock_guard<stdx::mutex> lock(_committedSnapshotMutex);
        return _committedSnapshot;
    }

    Timestamp RocksSnapshotManager::beginTransactionOnCommittedSnapshot(
        rocksdb::TOTransactionDB* db, std::unique_ptr<rocksdb::TOTransaction>* txn) const {
        RocksBeginTxnBlock txnOpen(db, txn);
        stdx::lock_guard<stdx::mutex> lock(_committedSnapshotMutex);
        uassert(ErrorCodes::ReadConcernMajorityNotAvailableYet,
                "Committed view disappeared while running operation", _committedSnapshot);

        auto status = txnOpen.setTimestamp(_committedSnapshot.get());
        invariant(status.isOK(), status.reason());

        txnOpen.done();
        return *_committedSnapshot;
    }

    Timestamp RocksSnapshotManager::beginTransactionOnLocalSnapshot(
        rocksdb::TOTransactionDB* db, std::unique_ptr<rocksdb::TOTransaction>* txn) const {
        RocksBeginTxnBlock txnOpen(db, txn);
        stdx::lock_guard<stdx::mutex> lock(_localSnapshotMutex);
        invariant(_localSnapshot);
        LOG(3) << "begin_transaction on local snapshot " << _localSnapshot.get().toString();
        auto status = txnOpen.setTimestamp(_localSnapshot.get());
        invariant(status.isOK(), status.reason());

        txnOpen.done();
        return *_localSnapshot;
    }

}  // namespace mongo
