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
#include <vector>
#include <unordered_map>

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/owned_pointer_vector.h"
#include "mongo/db/record_id.h"
#include "mongo/db/storage/recovery_unit.h"

#include "rocks_compaction_scheduler.h"
#include "rocks_transaction.h"
#include "rocks_counter_manager.h"
#include "rocks_snapshot_manager.h"
#include "rocks_durability_manager.h"

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

    class RocksRecoveryUnit : public RecoveryUnit {
        MONGO_DISALLOW_COPYING(RocksRecoveryUnit);
    public:
        RocksRecoveryUnit(RocksTransactionEngine* transactionEngine,
                          RocksSnapshotManager* snapshotManager, rocksdb::DB* db,
                          RocksCounterManager* counterManager,
                          RocksCompactionScheduler* compactionScheduler,
                          RocksDurabilityManager* durabilityManager, bool durable);
        virtual ~RocksRecoveryUnit();

        virtual void beginUnitOfWork(OperationContext* opCtx);
        virtual void commitUnitOfWork();
        virtual void abortUnitOfWork();

        virtual bool waitUntilDurable();

        virtual void abandonSnapshot();

        Status setReadFromMajorityCommittedSnapshot() final;
        bool isReadingFromMajorityCommittedSnapshot() const final {
            return _readFromMajorityCommittedSnapshot;
        }

        boost::optional<SnapshotName> getMajorityCommittedSnapshot() const final;

        virtual void* writingPtr(void* data, size_t len) { invariant(!"don't call writingPtr"); }

        virtual void registerChange(Change* change);

        virtual void setRollbackWritesDisabled() {}

        virtual SnapshotId getSnapshotId() const;

        // local api

        rocksdb::WriteBatchWithIndex* writeBatch();

        const rocksdb::Snapshot* getPreparedSnapshot();
        void dbReleaseSnapshot(const rocksdb::Snapshot* snapshot);

        // Returns snapshot, creating one if needed. Considers _readFromMajorityCommittedSnapshot.
        const rocksdb::Snapshot* snapshot();

        bool hasSnapshot() { return _snapshot != nullptr || _snapshotHolder.get() != nullptr; }

        RocksTransaction* transaction() { return &_transaction; }

        rocksdb::Status Get(const rocksdb::Slice& key, std::string* value);

        RocksIterator* NewIterator(std::string prefix, bool isOplog = false);

        static RocksIterator* NewIteratorNoSnapshot(rocksdb::DB* db, std::string prefix);

        void incrementCounter(const rocksdb::Slice& counterKey,
                              std::atomic<long long>* counter, long long delta);

        long long getDeltaCounter(const rocksdb::Slice& counterKey);

        void setOplogReadTill(const RecordId& loc);
        RecordId getOplogReadTill() const { return _oplogReadTill; }

        RocksRecoveryUnit* newRocksRecoveryUnit() {
            return new RocksRecoveryUnit(_transactionEngine, _snapshotManager, _db, _counterManager,
                                         _compactionScheduler, _durabilityManager, _durable);
        }

        struct Counter {
            std::atomic<long long>* _value;
            long long _delta;
            Counter() : Counter(nullptr, 0) {}
            Counter(std::atomic<long long>* value, long long delta) : _value(value),
                                                                      _delta(delta) {}
        };

        typedef std::unordered_map<std::string, Counter> CounterMap;

        static RocksRecoveryUnit* getRocksRecoveryUnit(OperationContext* opCtx);

        static int getTotalLiveRecoveryUnits() { return _totalLiveRecoveryUnits.load(); }

        void prepareForCreateSnapshot(OperationContext* opCtx);

        void setCommittedSnapshot(const rocksdb::Snapshot* committedSnapshot);

        rocksdb::DB* getDB() const { return _db; }

    private:
        void _releaseSnapshot();

        void _commit();

        void _abort();
        RocksTransactionEngine* _transactionEngine;      // not owned
        RocksSnapshotManager* _snapshotManager;          // not owned
        rocksdb::DB* _db;                                // not owned
        RocksCounterManager* _counterManager;            // not owned
        RocksCompactionScheduler* _compactionScheduler;  // not owned
        RocksDurabilityManager* _durabilityManager;      // not owned

        const bool _durable;

        RocksTransaction _transaction;

        rocksdb::WriteBatchWithIndex _writeBatch;

        // bare because we need to call ReleaseSnapshot when we're done with this
        const rocksdb::Snapshot* _snapshot; // owned

        // snapshot that got prepared in prepareForCreateSnapshot
        // it is consumed by getPreparedSnapshot()
        const rocksdb::Snapshot* _preparedSnapshot;  // owned

        CounterMap _deltaCounters;

        typedef OwnedPointerVector<Change> Changes;
        Changes _changes;

        uint64_t _myTransactionCount;

        RecordId _oplogReadTill;

        static std::atomic<int> _totalLiveRecoveryUnits;

        // If we read from a committed snapshot, then ownership of the snapshot
        // should be shared here to ensure that it is not released early
        std::shared_ptr<RocksSnapshotManager::SnapshotHolder> _snapshotHolder;

        bool _readFromMajorityCommittedSnapshot = false;
        bool _areWriteUnitOfWorksBanned = false;
    };

}
