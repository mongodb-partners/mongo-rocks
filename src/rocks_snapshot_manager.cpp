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

#include "rocks_snapshot_manager.h"
#include "rocks_recovery_unit.h"

#include <rocksdb/db.h>

#include "mongo/base/checked_cast.h"
#include "mongo/util/log.h"

namespace mongo {
    void RocksSnapshotManager::setCommittedSnapshot(const Timestamp& ts) {
        stdx::lock_guard<stdx::mutex> lock(_mutex);

        uint64_t nameU64 = ts.asULL();
        invariant(!_committedSnapshot || *_committedSnapshot <= nameU64);
        _committedSnapshot = nameU64;
    }

    void RocksSnapshotManager::dropAllSnapshots() {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        _committedSnapshot = boost::none;
        _snapshotMap.clear();
        _snapshots.clear();
    }

    bool RocksSnapshotManager::haveCommittedSnapshot() const {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        return bool(_committedSnapshot);
    }

    std::shared_ptr<RocksSnapshotManager::SnapshotHolder>
    RocksSnapshotManager::getCommittedSnapshot() const {
        stdx::lock_guard<stdx::mutex> lock(_mutex);

        uassert(ErrorCodes::ReadConcernMajorityNotAvailableYet,
                "Committed view disappeared while running operation", _committedSnapshot);

        return _snapshotMap.at(*_committedSnapshot);
    }

    RocksSnapshotManager::SnapshotHolder::SnapshotHolder(OperationContext* opCtx, uint64_t name_) {
        name = name_;
        auto rru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        snapshot = rru->getPreparedSnapshot();
        db = rru->getDB();
    }

    RocksSnapshotManager::SnapshotHolder::~SnapshotHolder() {
        if (snapshot != nullptr) {
            invariant(db != nullptr);
            db->ReleaseSnapshot(snapshot);
        }
    }

}  // namespace mongo
