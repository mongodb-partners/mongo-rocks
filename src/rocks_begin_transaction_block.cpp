/**
 *    Copyright (C) 2018 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
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

#include "rocks_begin_transaction_block.h"
#include <cstdio>
#include "mongo/platform/basic.h"
#include "mongo/util/log.h"
#include "rocks_util.h"

namespace mongo {
    RocksBeginTxnBlock::RocksBeginTxnBlock(rocksdb::TOTransactionDB* db,
                                           std::unique_ptr<rocksdb::TOTransaction>* txn,
                                           PrepareConflictBehavior prepareConflictBehavior,
                                           RoundUpPreparedTimestamps roundUpPreparedTimestamps,
                                           RoundUpReadTimestamp roundUpReadTimestamp)
        : _db(db) {
        invariant(!_rollback);
        rocksdb::WriteOptions wOpts;
        rocksdb::TOTransactionOptions txnOpts;

        if (prepareConflictBehavior == PrepareConflictBehavior::kIgnoreConflicts) {
            txnOpts.ignore_prepare = true;
            txnOpts.read_only = true;
        } else if (prepareConflictBehavior ==
                   PrepareConflictBehavior::kIgnoreConflictsAllowWrites) {
            txnOpts.ignore_prepare = true;
        }

        if (roundUpPreparedTimestamps == RoundUpPreparedTimestamps::kRound) {
            txnOpts.timestamp_round_prepared = true;
        }
        if (roundUpReadTimestamp == RoundUpReadTimestamp::kRound) {
            txnOpts.timestamp_round_read = true;
        }

        _transaction = _db->BeginTransaction(wOpts, txnOpts);
        invariant(_transaction);
        txn->reset(_transaction);
        _rollback = true;
    }

    RocksBeginTxnBlock::~RocksBeginTxnBlock() {
        if (_rollback) {
            invariant(_transaction->Rollback().ok());
        }
    }

    Status RocksBeginTxnBlock::setReadSnapshot(Timestamp readTs) {
        invariant(_rollback);
        rocksdb::RocksTimeStamp ts(readTs.asULL());
        auto status = _transaction->SetReadTimeStamp(ts);
        if (!status.ok()) {
            return rocksToMongoStatus(status);
        }

        status = _transaction->GetReadTimeStamp(&ts);
        invariant(status.ok(), status.ToString());
        _readTimestamp = Timestamp(ts);
        return Status::OK();
    }

    void RocksBeginTxnBlock::done() {
        invariant(_rollback);
        _rollback = false;
    }

    Timestamp RocksBeginTxnBlock::getTimestamp() const {
        invariant(!_readTimestamp.isNull());
        return _readTimestamp;
    }

}  // namespace mongo
