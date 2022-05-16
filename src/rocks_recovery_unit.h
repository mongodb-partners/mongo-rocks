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
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/write_batch.h>

#include "mongo/db/modules/rocks/src/totdb/totransaction.h"
#include "mongo/db/modules/rocks/src/totdb/totransaction_db.h"

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
}  // namespace rocksdb

namespace mongo {
    using RoundUpPreparedTimestamps = RocksBeginTxnBlock::RoundUpPreparedTimestamps;
    using RoundUpReadTimestamp = RocksBeginTxnBlock::RoundUpReadTimestamp;
    class RocksEngine;

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
        RocksRecoveryUnit(const RocksRecoveryUnit&) = delete;
        RocksRecoveryUnit& operator=(const RocksRecoveryUnit&) = delete;

    public:
        RocksRecoveryUnit(bool durable, RocksEngine* engine);
        virtual ~RocksRecoveryUnit();

        void beginUnitOfWork(OperationContext* opCtx) override;
        void prepareUnitOfWork() override;
        void commitUnitOfWork() override;
        void abortUnitOfWork() override;

        bool waitUntilDurable() override;

        bool waitUntilUnjournaledWritesDurable(bool stableCheckpoint = true) override;

        void registerChange(Change* change) override;

        void abandonSnapshot() override;
        void preallocateSnapshot() override;

        Status obtainMajorityCommittedSnapshot() override;

        // TODO(cuixin), need to know why the abstract interface layer remove the const
        boost::optional<Timestamp> getPointInTimeReadTimestamp() override;

        SnapshotId getSnapshotId() const override;

        Status setTimestamp(Timestamp timestamp) override;

        void setCommitTimestamp(Timestamp timestamp) override;

        void clearCommitTimestamp() override;

        Timestamp getCommitTimestamp() const override;

        void setDurableTimestamp(Timestamp timestamp) override;

        Timestamp getDurableTimestamp() const override;

        void setPrepareTimestamp(Timestamp timestamp) override;

        Timestamp getPrepareTimestamp() const override;

        void setPrepareConflictBehavior(PrepareConflictBehavior behavior) override;

        PrepareConflictBehavior getPrepareConflictBehavior() const override;

        void setRoundUpPreparedTimestamps(bool value) override;

        void setCatalogConflictingTimestamp(Timestamp timestamp) override;

        Timestamp getCatalogConflictingTimestamp() const override;

        void setTimestampReadSource(ReadSource source,
                                    boost::optional<Timestamp> provided = boost::none) override;

        ReadSource getTimestampReadSource() const override;

        virtual void setOrderedCommit(bool orderedCommit) override {
            _orderedCommit = orderedCommit;
        }

        // local api
        void setIsOplogReader() { _isOplogReader = true; }

        /**
         * Returns a session without starting a new txn. Will not close any already
         * running session.
         */

        rocksdb::TOTransaction* getTxnNoCreate();

        rocksdb::Status Get(rocksdb::ColumnFamilyHandle* cf, const rocksdb::Slice& key, std::string* value);

        RocksIterator* NewIterator(rocksdb::ColumnFamilyHandle* cf, std::string prefix, bool isOplog = false);

        static RocksIterator* NewIteratorWithTxn(rocksdb::TOTransaction* txn,
                                                 rocksdb::ColumnFamilyHandle* cf, std::string prefix);

        void incrementCounter(const rocksdb::Slice& counterKey, std::atomic<long long>* counter,
                              long long delta);

        long long getDeltaCounter(const rocksdb::Slice& counterKey);

        void resetDeltaCounters();

        RocksRecoveryUnit* newRocksRecoveryUnit() {
            return new RocksRecoveryUnit(_durable, _engine);
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

        rocksdb::TOTransaction* getTransaction();

        bool inActiveTxn() const { return _isActive(); }

        void assertInActiveTxn() const;

        boost::optional<int64_t> getOplogVisibilityTs();

        /**
         * State transitions:
         *
         *   /------------------------> Inactive <-----------------------------\
         *   |                             |                                   |
         *   |                             |                                   |
         *   |              /--------------+--------------\                    |
         *   |              |                             |                    | abandonSnapshot()
         *   |              |                             |                    |
         *   |   beginUOW() |                             | _txnOpen()         |
         *   |              |                             |                    |
         *   |              V                             V                    |
         *   |    InactiveInUnitOfWork          ActiveNotInUnitOfWork ---------/
         *   |              |                             |
         *   |              |                             |
         *   |   _txnOpen() |                             | beginUOW()
         *   |              |                             |
         *   |              \--------------+--------------/
         *   |                             |
         *   |                             |
         *   |                             V
         *   |                           Active
         *   |                             |
         *   |                             |
         *   |              /--------------+--------------\
         *   |              |                             |
         *   |              |                             |
         *   |   abortUOW() |                             | commitUOW()
         *   |              |                             |
         *   |              V                             V
         *   |          Aborting                      Committing
         *   |              |                             |
         *   |              |                             |
         *   |              |                             |
         *   \--------------+-----------------------------/
         *
         */
        enum class State {
            kInactive,
            kInactiveInUnitOfWork,
            kActiveNotInUnitOfWork,
            kActive,
            kAborting,
            kCommitting,
        };
        State getState_forTest() const;

        rocksdb::TOTransactionDB* getDB();
        RocksOplogManager* getOplogManager();
        RocksSnapshotManager* getSnapshotManager();
        RocksCompactionScheduler* getCompactionScheduler();
        RocksDurabilityManager* getDurabilityManager();

    private:
        void _abort();
        void _commit();

        void _txnClose(bool commit);
        void _txnOpen();

        /**
         * Starts a transaction at the current all_durable timestamp.
         * Returns the timestamp the transaction was started at.
         */
        Timestamp _beginTransactionAtAllDurableTimestamp();

        /**
         * Starts a transaction at the no-overlap timestamp. Returns the timestamp the transaction
         * was started at.
         */
        Timestamp _beginTransactionAtNoOverlapTimestamp(
            std::unique_ptr<rocksdb::TOTransaction>* txn);

        /**
         * Returns the timestamp at which the current transaction is reading.
         */
        Timestamp _getTransactionReadTimestamp(rocksdb::TOTransaction* txn);

        /**
         * Transitions to new state.
         */
        void _setState(State newState);

        /**
         * Returns true if active.
         */
        bool _isActive() const;

        /**
         * Returns true if currently managed by a WriteUnitOfWork.
         */
        bool _inUnitOfWork() const;

        /**
         * Returns true if currently running commit or rollback handlers
         */
        bool _isCommittingOrAborting() const;

        std::unique_ptr<rocksdb::TOTransaction> _transaction;

        bool _durable;
        bool _areWriteUnitOfWorksBanned;
        State _state = State::kInactive;
        bool _isTimestamped;

        // Specifies which external source to use when setting read timestamps on transactions.
        ReadSource _timestampReadSource;

        // Commits are assumed ordered.  Unordered commits are assumed to always need to reserve a
        // new optime, and thus always call oplogDiskLocRegister() on the record store.
        bool _orderedCommit;

        // When 'true', data read from disk should not be kept in the storage engine cache.
        bool _readOnce = false;

        // The behavior of handling prepare conflicts.
        PrepareConflictBehavior _prepareConflictBehavior{PrepareConflictBehavior::kEnforce};
        // Dictates whether to round up prepare and commit timestamp of a prepared transaction.
        RoundUpPreparedTimestamps _roundUpPreparedTimestamps{RoundUpPreparedTimestamps::kNoRound};

        Timestamp _commitTimestamp;
        Timestamp _durableTimestamp;
        Timestamp _prepareTimestamp;
        boost::optional<Timestamp> _lastTimestampSet;
        uint64_t _mySnapshotId;
        Timestamp _majorityCommittedSnapshot;
        Timestamp _readAtTimestamp;
        Timestamp _catalogConflictTimestamp;
        std::unique_ptr<Timer> _timer;
        CounterMap _deltaCounters;
        bool _isOplogReader;
        boost::optional<int64_t> _oplogVisibleTs = boost::none;
        typedef std::vector<std::unique_ptr<Change>> Changes;
        Changes _changes;
        static std::atomic<int> _totalLiveRecoveryUnits;
        RocksEngine* _engine; // not owned
    };
}  // namespace mongo
