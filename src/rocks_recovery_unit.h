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

#pragma once

#include <atomic>
#include <map>
#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#include <rocksdb/iterator.h>
#include <rocksdb/slice.h>
#include <rocksdb/utilities/totransaction.h>
#include <rocksdb/utilities/totransaction_db.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/write_batch.h>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/owned_pointer_vector.h"
#include "mongo/db/record_id.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/util/timer.h"

#include "rocks_compaction_scheduler.h"
#include "rocks_counter_manager.h"
#include "rocks_durability_manager.h"
#include "rocks_snapshot_manager.h"

namespace rocksdb {
    class DB;
    class Snapshot;
    class WriteBatchWithIndex;
    class Comparator;
    class Status;
    class Slice;
    class Iterator;
}

namespace mongo {

    // Same as rocksdb::Iterator, but adds couple more useful functions
    class RocksIterator : public rocksdb::Iterator {
    public:
        virtual ~RocksIterator() {}

        // This Seek is specific because it will succeed only if it finds a key with `target`
        // prefix. If there is no such key, it will be !Valid()
        virtual void SeekPrefix(const rocksdb::Slice& target) = 0;
    };

    class OperationContext;
    class RocksOplogManager;

    class RocksRecoveryUnit : public RecoveryUnit {
        MONGO_DISALLOW_COPYING(RocksRecoveryUnit);

    public:
        RocksRecoveryUnit(rocksdb::TOTransactionDB* db, RocksOplogManager* oplogManager,
                          RocksSnapshotManager* snapshotManager,
                          RocksCounterManager* counterManager,
                          RocksCompactionScheduler* compactionScheduler,
                          RocksDurabilityManager* durabilityManager, bool durable);
        virtual ~RocksRecoveryUnit();

        void beginUnitOfWork(OperationContext* opCtx) override;
        void prepareUnitOfWork() override;
        void commitUnitOfWork() override;
        void abortUnitOfWork() override;

        bool waitUntilDurable() override;

        bool waitUntilUnjournaledWritesDurable() override;

        void registerChange(Change* change) override;

        void abandonSnapshot() override;
        void preallocateSnapshot() override;

        Status obtainMajorityCommittedSnapshot() override;

        boost::optional<Timestamp> getPointInTimeReadTimestamp() const override;

        SnapshotId getSnapshotId() const override;

        Status setTimestamp(Timestamp timestamp) override;

        void setCommitTimestamp(Timestamp timestamp) override;

        void clearCommitTimestamp() override;

        Timestamp getCommitTimestamp() override;

        void setPrepareTimestamp(Timestamp timestamp) override;

        void setIgnorePrepared(bool ignore) override;

        void setTimestampReadSource(ReadSource source,
                                    boost::optional<Timestamp> provided = boost::none) override;

        ReadSource getTimestampReadSource() const override;

        void* writingPtr(void* data, size_t len) override;

        void setRollbackWritesDisabled() override {}

        virtual void setOrderedCommit(bool orderedCommit) override {
            _orderedCommit = orderedCommit;
        }

        // local api
        void setIsOplogReader() { _isOplogReader = true; }

        rocksdb::Status Get(const rocksdb::Slice& key, std::string* value);

        RocksIterator* NewIterator(std::string prefix, bool isOplog = false);

        static RocksIterator* NewIteratorWithTxn(rocksdb::TOTransaction* txn, std::string prefix);

        void incrementCounter(const rocksdb::Slice& counterKey, std::atomic<long long>* counter,
                              long long delta);

        long long getDeltaCounter(const rocksdb::Slice& counterKey);

        void resetDeltaCounters();

        RocksRecoveryUnit* newRocksRecoveryUnit() {
            return new RocksRecoveryUnit(_db, _oplogManager, _snapshotManager, _counterManager,
                                         _compactionScheduler, _durabilityManager, _durable);
        }

        struct Counter {
            std::atomic<long long>* _value;
            long long _delta;
            Counter() : Counter(nullptr, 0) {}
            Counter(std::atomic<long long>* value, long long delta)
                : _value(value), _delta(delta) {}
        };

        typedef std::unordered_map<std::string, Counter> CounterMap;

        static RocksRecoveryUnit* getRocksRecoveryUnit(OperationContext* opCtx);

        static int getTotalLiveRecoveryUnits() { return _totalLiveRecoveryUnits.load(); }

        rocksdb::DB* getDB() const { return _db; }

        rocksdb::TOTransaction* getTransaction();

        bool inActiveTxn() const { return _active; }

        void assertInActiveTxn() const;

    private:
        void _abort();
        void _commit();

        void _txnClose(bool commit);
        void _txnOpen();

        /**
         * Starts a transaction at the current all-committed timestamp.
         * Returns the timestamp the transaction was started at.
         */
        Timestamp _beginTransactionAtAllCommittedTimestamp();

        rocksdb::TOTransactionDB* _db;                   // not owned
        RocksOplogManager* _oplogManager;                // not owned
        RocksSnapshotManager* _snapshotManager;          // not owned
        RocksCounterManager* _counterManager;            // not owned
        RocksCompactionScheduler* _compactionScheduler;  // not owned
        RocksDurabilityManager* _durabilityManager;      // not owned

        std::unique_ptr<rocksdb::TOTransaction> _transaction;

        bool _durable;
        bool _areWriteUnitOfWorksBanned;
        bool _inUnitOfWork;
        bool _active;
        bool _isTimestamped;

        // Specifies which external source to use when setting read timestamps on transactions.
        ReadSource _timestampReadSource;

        // Commits are assumed ordered.  Unordered commits are assumed to always need to reserve a
        // new optime, and thus always call oplogDiskLocRegister() on the record store.
        bool _orderedCommit;
        Timestamp _commitTimestamp;

        Timestamp _prepareTimestamp;
        boost::optional<Timestamp> _lastTimestampSet;
        uint64_t _mySnapshotId;
        Timestamp _majorityCommittedSnapshot;
        Timestamp _readAtTimestamp;
        std::unique_ptr<Timer> _timer;
        CounterMap _deltaCounters;

        bool _isOplogReader;
        typedef std::vector<std::unique_ptr<Change>> Changes;
        Changes _changes;

        static std::atomic<int> _totalLiveRecoveryUnits;
    };
}
