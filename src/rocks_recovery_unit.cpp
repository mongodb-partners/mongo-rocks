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

#include "rocks_recovery_unit.h"

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/slice.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/write_batch.h>

#include "mongo/base/checked_cast.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/server_options.h"
#include "mongo/db/storage/journal_listener.h"
#include "mongo/util/log.h"
#include "mongo/util/stacktrace.h"

#include "rocks_util.h"

#include "rocks_begin_transaction_block.h"
#include "rocks_oplog_manager.h"
#include "rocks_snapshot_manager.h"
#include "rocks_engine.h"

namespace mongo {
    namespace {
        // Always notifies prepare conflict waiters when a transaction commits or aborts,
        // even when the transaction is not prepared. This should always be enabled if
        // WTPrepareConflictForReads is used, which fails randomly. If this is not enabled,
        // no prepare conflicts will be resolved, because the recovery unit may not
        // ever actually be in a prepared state.
        MONGO_FAIL_POINT_DEFINE(RocksAlwaysNotifyPrepareConflictWaiters);

        // SnapshotIds need to be globally unique, as they are used in a WorkingSetMember to
        // determine if documents changed, but a different recovery unit may be used across a
        // getMore, so there is a chance the snapshot ID will be reused.
        AtomicWord<unsigned long long> nextSnapshotId{1};

        logger::LogSeverity kSlowTransactionSeverity = logger::LogSeverity::Debug(1);

        /**
         * Returns a string representation of RocksRecoveryUnit::State for logging.
         */
        std::string toString(RocksRecoveryUnit::State state) {
            switch (state) {
                case RocksRecoveryUnit::State::kInactive:
                    return "Inactive";
                case RocksRecoveryUnit::State::kInactiveInUnitOfWork:
                    return "InactiveInUnitOfWork";
                case RocksRecoveryUnit::State::kActiveNotInUnitOfWork:
                    return "ActiveNotInUnitOfWork";
                case RocksRecoveryUnit::State::kActive:
                    return "Active";
                case RocksRecoveryUnit::State::kCommitting:
                    return "Committing";
                case RocksRecoveryUnit::State::kAborting:
                    return "Aborting";
            }
            MONGO_UNREACHABLE;
        }
    }  // namespace

    namespace {
        class PrefixStrippingIterator : public RocksIterator {
        public:
            // baseIterator is consumed
            PrefixStrippingIterator(rocksdb::ColumnFamilyHandle* cf, std::string prefix,
                                    rocksdb::TOTransaction* txn, Iterator* baseIterator,
                                    RocksCompactionScheduler* compactionScheduler,
                                    std::unique_ptr<rocksdb::Slice> upperBound)
                : _cf(cf),
                  _rocksdbSkippedDeletionsInitial(0),
                  _prefix(std::move(prefix)),
                  _nextPrefix(rocksGetNextPrefix(_prefix)),
                  _prefixSlice(_prefix.data(), _prefix.size()),
                  _prefixSliceEpsilon(_prefix.data(), _prefix.size() + 1),
                  _baseIterator(baseIterator),
                  _compactionScheduler(compactionScheduler),
                  _upperBound(std::move(upperBound)),
                  _txn(txn) {
                *_upperBound.get() = rocksdb::Slice(_nextPrefix);
            }

            ~PrefixStrippingIterator() {}

            virtual bool Valid() const {
                return _baseIterator->Valid() && _baseIterator->key().starts_with(_prefixSlice) &&
                       _baseIterator->key().size() > _prefixSlice.size();
            }

            virtual void SeekToFirst() {
                startOp();
                // seek to first key bigger than prefix
                _baseIterator->Seek(_prefixSliceEpsilon);
                endOp();
            }
            virtual void SeekToLast() {
                startOp();
                // we can't have upper bound set to _nextPrefix since we need to seek to it
                *_upperBound.get() = rocksdb::Slice("\xFF\xFF\xFF\xFF");
                _baseIterator->Seek(_nextPrefix);

                // reset back to original value
                *_upperBound.get() = rocksdb::Slice(_nextPrefix);
                if (!_baseIterator->Valid()) {
                    _baseIterator->SeekToLast();
                }
                if (_baseIterator->Valid() && !_baseIterator->key().starts_with(_prefixSlice)) {
                    _baseIterator->Prev();
                }
                endOp();
            }

            virtual void Seek(const rocksdb::Slice& target) {
                startOp();
                std::unique_ptr<char[]> buffer(new char[_prefix.size() + target.size()]);
                memcpy(buffer.get(), _prefix.data(), _prefix.size());
                memcpy(buffer.get() + _prefix.size(), target.data(), target.size());
                auto key = rocksdb::Slice(buffer.get(), _prefix.size() + target.size());
                _baseIterator->Seek(rocksdb::Slice(buffer.get(), _prefix.size() + target.size()));
                endOp();
            }

            virtual void Next() {
                startOp();
                _baseIterator->Next();
                endOp();
            }

            virtual void Prev() {
                startOp();
                _baseIterator->Prev();
                endOp();
            }

            virtual void SeekForPrev(const rocksdb::Slice& target) {
                // TODO(wolfkdy): impl
            }

            virtual rocksdb::Slice key() const {
                rocksdb::Slice strippedKey = _baseIterator->key();
                strippedKey.remove_prefix(_prefix.size());
                return strippedKey;
            }
            virtual rocksdb::Slice value() const { return _baseIterator->value(); }
            virtual rocksdb::Status status() const {
                if (_baseIterator->status().IsPrepareConflict() && !Valid() &&
                    _baseIterator->Valid()) {
                    // in this situation, we have seeked to neighbouring prefix
                    invariant(!_baseIterator->key().starts_with(_prefixSlice));
                    return rocksdb::Status::OK();
                }
                return _baseIterator->status();
            }

            // RocksIterator specific functions

            // This Seek is specific because it will succeed only if it finds a key with `target`
            // prefix. If there is no such key, it will be !Valid()
            virtual void SeekPrefix(const rocksdb::Slice& target) {
                std::unique_ptr<char[]> buffer(new char[_prefix.size() + target.size()]);
                memcpy(buffer.get(), _prefix.data(), _prefix.size());
                memcpy(buffer.get() + _prefix.size(), target.data(), target.size());

                std::string tempUpperBound = rocksGetNextPrefix(
                    rocksdb::Slice(buffer.get(), _prefix.size() + target.size()));

                *_upperBound.get() = rocksdb::Slice(tempUpperBound);
                if (target.size() == 0) {
                    // if target is empty, we'll try to seek to <prefix>, which is not good
                    _baseIterator->Seek(_prefixSliceEpsilon);
                } else {
                    _baseIterator->Seek(
                        rocksdb::Slice(buffer.get(), _prefix.size() + target.size()));
                }
                // reset back to original value
                *_upperBound.get() = rocksdb::Slice(_nextPrefix);
            }

            rocksdb::TOTransaction* getTxn() { return _txn; }

        private:
            void startOp() {
                if (_compactionScheduler == nullptr) {
                    return;
                }
                if (rocksdb::GetPerfLevel() == rocksdb::PerfLevel::kDisable) {
                    rocksdb::SetPerfLevel(rocksdb::kEnableCount);
                }
                _rocksdbSkippedDeletionsInitial = get_internal_delete_skipped_count();
            }
            void endOp() {
                if (_compactionScheduler == nullptr) {
                    return;
                }
                int skippedDeletionsOp =
                    get_internal_delete_skipped_count() - _rocksdbSkippedDeletionsInitial;
                if (skippedDeletionsOp >=
                    RocksCompactionScheduler::getSkippedDeletionsThreshold()) {
                    _compactionScheduler->reportSkippedDeletionsAboveThreshold(_cf, _prefix);
                }
            }

            rocksdb::ColumnFamilyHandle* _cf;
            int _rocksdbSkippedDeletionsInitial;
            std::string _prefix;
            std::string _nextPrefix;
            rocksdb::Slice _prefixSlice;
            // the first possible key bigger than prefix. we use this for SeekToFirst()
            rocksdb::Slice _prefixSliceEpsilon;
            std::unique_ptr<Iterator> _baseIterator;

            // can be nullptr
            RocksCompactionScheduler* _compactionScheduler;  // not owned

            std::unique_ptr<rocksdb::Slice> _upperBound;
            rocksdb::TOTransaction* _txn;  // not owned
        };

    }  // anonymous namespace

    std::atomic<int> RocksRecoveryUnit::_totalLiveRecoveryUnits(0);

    RocksRecoveryUnit::RocksRecoveryUnit(rocksdb::TOTransactionDB* db,
                                         RocksOplogManager* oplogManager,
                                         RocksSnapshotManager* snapshotManager,
                                         RocksCompactionScheduler* compactionScheduler,
                                         RocksDurabilityManager* durabilityManager,
                                         bool durable, RocksEngine* engine)
        : _db(db),
          _oplogManager(oplogManager),
          _snapshotManager(snapshotManager),
          _compactionScheduler(compactionScheduler),
          _durabilityManager(durabilityManager),
          _durable(durable),
          _areWriteUnitOfWorksBanned(false),
          _isTimestamped(false),
          _timestampReadSource(ReadSource::kUnset),
          _orderedCommit(true),
          _mySnapshotId(nextSnapshotId.fetchAndAdd(1)),
          _isOplogReader(false),
          _engine(engine) {
        RocksRecoveryUnit::_totalLiveRecoveryUnits.fetch_add(1, std::memory_order_relaxed);
    }

    RocksRecoveryUnit::~RocksRecoveryUnit() {
        invariant(!_inUnitOfWork(), toString(_state));
        RocksRecoveryUnit::_totalLiveRecoveryUnits--;
        _abort();
    }

    void RocksRecoveryUnit::_commit() {
        // Since we cannot have both a _lastTimestampSet and a _commitTimestamp, we set the
        // commit time as whichever is non-empty. If both are empty, then _lastTimestampSet will
        // be boost::none and we'll set the commit time to that.
        auto commitTs = _commitTimestamp.isNull() ? _lastTimestampSet : _commitTimestamp;

        bool notifyDone = !_prepareTimestamp.isNull();

        if (_isActive()) {
            _txnClose(true /*commit*/);
        }
        _setState(State::kCommitting);

        if (MONGO_FAIL_POINT(RocksAlwaysNotifyPrepareConflictWaiters)) {
            notifyDone = true;
        }

        if (notifyDone) {
            _durabilityManager->notifyPreparedUnitOfWorkHasCommittedOrAborted();
        }

        try {
            for (Changes::const_iterator it = _changes.begin(), end = _changes.end(); it != end;
                 ++it) {
                (*it)->commit(commitTs);
            }
            _changes.clear();
        } catch (...) {
            std::terminate();
        }

        for (auto pair : _deltaCounters) {
            auto& counter = pair.second;
            counter._value->fetch_add(counter._delta, std::memory_order::memory_order_relaxed);
            long long newValue = counter._value->load(std::memory_order::memory_order_relaxed);
            _engine->getCounterManager()->updateCounter(pair.first, newValue);
        }

        _deltaCounters.clear();
        _setState(State::kInactive);
    }

    void RocksRecoveryUnit::_abort() {
        bool notifyDone = !_prepareTimestamp.isNull();

        if (_isActive()) {
            _txnClose(false /* commit */);
        }
        _setState(State::kAborting);

        if (MONGO_FAIL_POINT(RocksAlwaysNotifyPrepareConflictWaiters)) {
            notifyDone = true;
        }

        if (notifyDone) {
            _durabilityManager->notifyPreparedUnitOfWorkHasCommittedOrAborted();
        }
        try {
            for (Changes::const_reverse_iterator it = _changes.rbegin(), end = _changes.rend();
                 it != end; ++it) {
                const auto& change = *it;
                LOG(2) << "CUSTOM ROLLBACK " << redact(demangleName(typeid(*change)));
                change->rollback();
            }
            _changes.clear();
        } catch (...) {
            std::terminate();
        }

        _deltaCounters.clear();
        _setState(State::kInactive);
    }

    void RocksRecoveryUnit::beginUnitOfWork(OperationContext* opCtx) {
        invariant(!_inUnitOfWork(), toString(_state));
        invariant(
            !_isCommittingOrAborting(),
            str::stream() << "cannot begin unit of work while commit or rollback handlers are "
                             "running: "
                          << toString(_state));
        _setState(_isActive() ? State::kActive : State::kInactiveInUnitOfWork);
    }

    void RocksRecoveryUnit::prepareUnitOfWork() {
        invariant(_inUnitOfWork(), toString(_state));
        invariant(!_prepareTimestamp.isNull());

        getTransaction();

        LOG(1) << "preparing transaction at time: " << _prepareTimestamp;
        auto status =
            _transaction->SetPrepareTimeStamp(rocksdb::RocksTimeStamp(_prepareTimestamp.asULL()));
        invariant(status.ok(), status.ToString());
        status = _transaction->Prepare();
        invariant(status.ok(), status.ToString());
    }

    void RocksRecoveryUnit::commitUnitOfWork() {
        invariant(_inUnitOfWork(), toString(_state));
        _commit();
    }

    void RocksRecoveryUnit::abortUnitOfWork() {
        invariant(_inUnitOfWork(), toString(_state));
        _abort();
    }

    bool RocksRecoveryUnit::waitUntilDurable() {
        invariant(!_inUnitOfWork(), toString(_state));
        _durabilityManager->waitUntilDurable(false);
        return true;
    }

    // TODO(cuixin), consider stableCheckpoint
    bool RocksRecoveryUnit::waitUntilUnjournaledWritesDurable(bool stableCheckpoint) {
        invariant(!_inUnitOfWork(), toString(_state));
        waitUntilDurable();
        return true;
    }

    void RocksRecoveryUnit::registerChange(Change* change) {
        invariant(_inUnitOfWork(), toString(_state));
        _changes.push_back(std::unique_ptr<Change>{change});
    }

    void RocksRecoveryUnit::assertInActiveTxn() const {
        if (_isActive()) {
            return;
        }
        severe() << "Recovery unit is not active. Current state: " << toString(_state);
        fassertFailed(28576);
    }

    boost::optional<int64_t> RocksRecoveryUnit::getOplogVisibilityTs() {
        if (!_isOplogReader) {
            return boost::none;
        }

        getTransaction();
        return _oplogVisibleTs;
    }

    rocksdb::TOTransaction* RocksRecoveryUnit::getTransaction() {
        if (!_isActive()) {
            _txnOpen();
            _setState(_inUnitOfWork() ? State::kActive : State::kActiveNotInUnitOfWork);
        }
        return _transaction.get();
    }

    rocksdb::TOTransaction* RocksRecoveryUnit::getTxnNoCreate() { return _transaction.get(); }

    void RocksRecoveryUnit::abandonSnapshot() {
        invariant(!_inUnitOfWork(), toString(_state));
        _deltaCounters.clear();
        if (_isActive()) {
            // Can't be in a WriteUnitOfWork, so safe to rollback
            _txnClose(false);
        }
        _setState(State::kInactive);
    }

    void RocksRecoveryUnit::preallocateSnapshot() { getTransaction(); }

    void RocksRecoveryUnit::_txnClose(bool commit) {
        invariant(_isActive(), toString(_state));
        if (_timer) {
            const int transactionTime = _timer->millis();
            if (transactionTime >= serverGlobalParams.slowMS) {
                LOG(kSlowTransactionSeverity)
                    << "Slow Rocks transaction. Lifetime of SnapshotId " << _mySnapshotId << " was "
                    << transactionTime << "ms";
            }
        }
        rocksdb::Status status;
        if (commit) {
            if (!_commitTimestamp.isNull()) {
                // There is currently no scenario where it is intentional to commit before the
                // current
                // read timestamp.
                invariant(_readAtTimestamp.isNull() || _commitTimestamp >= _readAtTimestamp);
                status = _transaction->SetCommitTimeStamp(
                    rocksdb::RocksTimeStamp(_commitTimestamp.asULL()));
                invariant(status.ok(), status.ToString());
                _isTimestamped = true;
            }
            if (!_durableTimestamp.isNull()) {
                status = _transaction->SetDurableTimeStamp(
                    rocksdb::RocksTimeStamp(_durableTimestamp.asULL()));
                invariant(status.ok(), status.ToString());
            }
            status = _transaction->Commit();
            LOG(3) << "Rocks commit_transaction for snapshot id " << _mySnapshotId;
        } else {
            status = _transaction->Rollback();
            invariant(status.ok(), status.ToString());
            LOG(3) << "Rocks rollback_transaction for snapshot id " << _mySnapshotId;
        }

        _transaction.reset(nullptr);

        if (_isTimestamped) {
            if (!_orderedCommit) {
                _oplogManager->triggerJournalFlush();
            }
            _isTimestamped = false;
        }
        invariant(status.ok(), status.ToString());
        invariant(!_lastTimestampSet || _commitTimestamp.isNull(),
                  str::stream() << "Cannot have both a _lastTimestampSet and a "
                                   "_commitTimestamp. _lastTimestampSet: "
                                << _lastTimestampSet->toString()
                                << ". _commitTimestamp: " << _commitTimestamp.toString());

        // We reset the _lastTimestampSet between transactions. Since it is legal for one
        // transaction on a RecoveryUnit to call setTimestamp() and another to call
        // setCommitTimestamp().
        _lastTimestampSet = boost::none;

        _prepareTimestamp = Timestamp();
        _durableTimestamp = Timestamp();
        _catalogConflictTimestamp = Timestamp();
        _roundUpPreparedTimestamps = RoundUpPreparedTimestamps::kNoRound;
        _mySnapshotId = nextSnapshotId.fetchAndAdd(1);
        _isOplogReader = false;
        _oplogVisibleTs = boost::none;
        _orderedCommit = true;  // Default value is true; we assume all writes are ordered.
    }

    SnapshotId RocksRecoveryUnit::getSnapshotId() const {
        // TODO: use actual rocks snapshot id
        return SnapshotId(_mySnapshotId);
    }

    Status RocksRecoveryUnit::obtainMajorityCommittedSnapshot() {
        invariant(_timestampReadSource == ReadSource::kMajorityCommitted);
        auto snapshotName = _snapshotManager->getMinSnapshotForNextCommittedRead();
        if (!snapshotName) {
            return {ErrorCodes::ReadConcernMajorityNotAvailableYet,
                    "Read concern majority reads are currently not possible."};
        }
        _majorityCommittedSnapshot = Timestamp(*snapshotName);
        return Status::OK();
    }
    boost::optional<Timestamp> RocksRecoveryUnit::getPointInTimeReadTimestamp() {
        // After a ReadSource has been set on this RecoveryUnit, callers expect that this method
        // returns
        // the read timestamp that will be used for current or future transactions. Because callers
        // use
        // this timestamp to inform visiblity of operations, it is therefore necessary to open a
        // transaction to establish a read timestamp, but only for ReadSources that are expected to
        // have
        // read timestamps.
        switch (_timestampReadSource) {
            case ReadSource::kUnset:
            case ReadSource::kNoTimestamp:
                return boost::none;
            case ReadSource::kMajorityCommitted:
                // This ReadSource depends on a previous call to obtainMajorityCommittedSnapshot()
                // and
                // does not require an open transaction to return a valid timestamp.
                invariant(!_majorityCommittedSnapshot.isNull());
                return _majorityCommittedSnapshot;
            case ReadSource::kProvided:
                // The read timestamp is set by the user and does not require a transaction to be
                // open.
                invariant(!_readAtTimestamp.isNull());
                return _readAtTimestamp;

            // The following ReadSources can only establish a read timestamp when a transaction is
            // opened.
            case ReadSource::kNoOverlap:
            case ReadSource::kLastApplied:
            case ReadSource::kAllDurableSnapshot:
                break;
        }

        // Ensure a transaction is opened.
        getTransaction();

        switch (_timestampReadSource) {
            case ReadSource::kLastApplied:
                // The lastApplied timestamp is not always available, so it is not possible to
                // invariant
                // that it exists as other ReadSources do.
                if (!_readAtTimestamp.isNull()) {
                    return _readAtTimestamp;
                }
                return boost::none;
            case ReadSource::kNoOverlap:
            case ReadSource::kAllDurableSnapshot:
                invariant(!_readAtTimestamp.isNull());
                return _readAtTimestamp;

            // The follow ReadSources returned values in the first switch block.
            case ReadSource::kUnset:
            case ReadSource::kNoTimestamp:
            case ReadSource::kMajorityCommitted:
            case ReadSource::kProvided:
                MONGO_UNREACHABLE;
        }
        MONGO_UNREACHABLE;
    }

    void RocksRecoveryUnit::_txnOpen() {
        invariant(!_isActive(), toString(_state));
        invariant(!_isCommittingOrAborting(),
                  str::stream() << "commit or rollback handler reopened transaction: "
                                << toString(_state));

        // Only start a timer for transaction's lifetime if we're going to log it.
        if (shouldLog(kSlowTransactionSeverity)) {
            _timer.reset(new Timer());
        }

        switch (_timestampReadSource) {
            case ReadSource::kUnset:
            case ReadSource::kNoTimestamp: {
                if (_isOplogReader) {
                    _oplogVisibleTs =
                        static_cast<std::int64_t>(_oplogManager->getOplogReadTimestamp());
                }
                RocksBeginTxnBlock(_db, &_transaction, _prepareConflictBehavior,
                                   _roundUpPreparedTimestamps)
                    .done();
                break;
            }
            case ReadSource::kMajorityCommitted: {
                // We reset _majorityCommittedSnapshot to the actual read timestamp used when the
                // transaction was started.
                _majorityCommittedSnapshot = _snapshotManager->beginTransactionOnCommittedSnapshot(
                    _db, &_transaction, _prepareConflictBehavior, _roundUpPreparedTimestamps);
                break;
            }
            case ReadSource::kLastApplied: {
                if (_snapshotManager->getLocalSnapshot()) {
                    _readAtTimestamp = _snapshotManager->beginTransactionOnLocalSnapshot(
                        _db, &_transaction, _prepareConflictBehavior, _roundUpPreparedTimestamps);
                } else {
                    RocksBeginTxnBlock(_db, &_transaction, _prepareConflictBehavior,
                                       _roundUpPreparedTimestamps)
                        .done();
                }
                break;
            }
            case ReadSource::kNoOverlap: {
                _readAtTimestamp = _beginTransactionAtNoOverlapTimestamp(&_transaction);
                break;
            }
            case ReadSource::kAllDurableSnapshot: {
                if (_readAtTimestamp.isNull()) {
                    _readAtTimestamp = _beginTransactionAtAllDurableTimestamp();
                    break;
                }
                // Intentionally continue to the next case to read at the _readAtTimestamp.
            }
            case ReadSource::kProvided: {
                RocksBeginTxnBlock txnOpen(_db, &_transaction, _prepareConflictBehavior,
                                           _roundUpPreparedTimestamps);
                auto status = txnOpen.setReadSnapshot(_readAtTimestamp);

                if (!status.isOK() && status.code() == ErrorCodes::BadValue) {
                    uasserted(ErrorCodes::SnapshotTooOld,
                              str::stream() << "Read timestamp " << _readAtTimestamp.toString()
                                            << " is older than the oldest available timestamp.");
                }
                uassertStatusOK(status);
                txnOpen.done();
                break;
            }
        }

        LOG(0) << "Rocks begin_transaction for snapshot id " << _mySnapshotId << ",src "
               << (int)_timestampReadSource;
    }

    Timestamp RocksRecoveryUnit::_beginTransactionAtAllDurableTimestamp() {
        RocksBeginTxnBlock txnOpen(_db, &_transaction, _prepareConflictBehavior,
                                   _roundUpPreparedTimestamps, RoundUpReadTimestamp::kRound);
        Timestamp txnTimestamp = _oplogManager->fetchAllDurableValue();
        auto status = txnOpen.setReadSnapshot(txnTimestamp);
        invariant(status.isOK(), status.reason());
        auto readTimestamp = txnOpen.getTimestamp();
        txnOpen.done();
        return readTimestamp;
    }

    Timestamp RocksRecoveryUnit::_beginTransactionAtNoOverlapTimestamp(
        std::unique_ptr<rocksdb::TOTransaction>* txn) {
        auto lastApplied = _snapshotManager->getLocalSnapshot();
        Timestamp allDurable = Timestamp(_oplogManager->fetchAllDurableValue());

        // When using timestamps for reads and writes, it's important that readers and writers don't
        // overlap with the timestamps they use. In other words, at any point in the system there
        // should
        // be a timestamp T such that writers only commit at times greater than T and readers only
        // read
        // at, or earlier than T. This time T is called the no-overlap point. Using the `kNoOverlap`
        // ReadSource will compute the most recent known time that is safe to read at.

        // The no-overlap point is computed as the minimum of the storage engine's all_durable time
        // and replication's last applied time. On primaries, the last applied time is updated as
        // transactions commit, which is not necessarily in the order they appear in the oplog. Thus
        // the all_durable time is an appropriate value to read at.

        // On secondaries, however, the all_durable time, as computed by the storage engine, can
        // advance before oplog application completes a batch. This is because the all_durable time
        // is only computed correctly if the storage engine is informed of commit timestamps in
        // increasing order. Because oplog application processes a batch of oplog entries out of
        // order,
        // the timestamping requirement is not satisfied. Secondaries, however, only update the last
        // applied time after a batch completes. Thus last applied is a valid no-overlap point on
        // secondaries.

        // By taking the minimum of the two values, storage can compute a legal time to read at
        // without
        // knowledge of the replication state. The no-overlap point is the minimum of the
        // all_durable
        // time, which represents the point where no transactions will commit any earlier, and
        // lastApplied, which represents the highest optime a node has applied, a point no readers
        // should read afterward.
        Timestamp readTimestamp = (lastApplied) ? std::min(*lastApplied, allDurable) : allDurable;

        RocksBeginTxnBlock txnOpen(_db, txn, _prepareConflictBehavior, _roundUpPreparedTimestamps,
                                   RoundUpReadTimestamp::kRound);
        auto status = txnOpen.setReadSnapshot(readTimestamp);
        fassert(51090, status);

        // We might have rounded to oldest between calling getAllDurable and setReadSnapshot. We
        // need
        // to get the actual read timestamp we used.
        readTimestamp = _getTransactionReadTimestamp(txn->get());
        txnOpen.done();
        return readTimestamp;
    }

    Timestamp RocksRecoveryUnit::_getTransactionReadTimestamp(rocksdb::TOTransaction* txn) {
        rocksdb::RocksTimeStamp read_timestamp;
        auto s = txn->GetReadTimeStamp(&read_timestamp);
        invariant(s.ok());
        return Timestamp(read_timestamp);
    }

    Status RocksRecoveryUnit::setTimestamp(Timestamp timestamp) {
        LOG(3) << "Rocks set timestamp of future write operations to " << timestamp;
        invariant(_inUnitOfWork(), toString(_state));
        invariant(_prepareTimestamp.isNull());
        invariant(_commitTimestamp.isNull(),
                  str::stream() << "Commit timestamp set to " << _commitTimestamp.toString()
                                << " and trying to set WUOW timestamp to " << timestamp.toString());

        _lastTimestampSet = timestamp;

        getTransaction();
        invariant(_transaction.get());
        auto status = _transaction->SetCommitTimeStamp(rocksdb::RocksTimeStamp(timestamp.asULL()));
        if (status.ok()) {
            _isTimestamped = true;
        }
        return rocksToMongoStatus(status);
    }

    void RocksRecoveryUnit::setCommitTimestamp(Timestamp timestamp) {
        // This can be called either outside of a WriteUnitOfWork or in a prepared transaction after
        // setPrepareTimestamp() is called. Prepared transactions ensure the correct timestamping
        // semantics and the set-once commitTimestamp behavior is exactly what prepared transactions
        // want.
        invariant(!_inUnitOfWork() || !_prepareTimestamp.isNull(), toString(_state));
        invariant(_commitTimestamp.isNull(),
                  str::stream() << "Commit timestamp set to " << _commitTimestamp.toString()
                                << " and trying to set it to " << timestamp.toString());
        invariant(!_lastTimestampSet, str::stream() << "Last timestamp set is "
                                                    << _lastTimestampSet->toString()
                                                    << " and trying to set commit timestamp to "
                                                    << timestamp.toString());
        invariant(!_isTimestamped);

        _commitTimestamp = timestamp;
    }

    Timestamp RocksRecoveryUnit::getCommitTimestamp() const { return _commitTimestamp; }

    void RocksRecoveryUnit::setDurableTimestamp(Timestamp timestamp) {
        invariant(
            _durableTimestamp.isNull(),
            str::stream() << "Trying to reset durable timestamp when it was already set. wasSetTo: "
                          << _durableTimestamp.toString() << " setTo: " << timestamp.toString());

        _durableTimestamp = timestamp;
    }

    Timestamp RocksRecoveryUnit::getDurableTimestamp() const { return _durableTimestamp; }

    void RocksRecoveryUnit::clearCommitTimestamp() {
        invariant(!_inUnitOfWork(), toString(_state));
        invariant(!_commitTimestamp.isNull());
        invariant(!_lastTimestampSet, str::stream() << "Last timestamp set is "
                                                    << _lastTimestampSet->toString()
                                                    << " and trying to clear commit timestamp.");
        invariant(!_isTimestamped);

        _commitTimestamp = Timestamp();
    }

    void RocksRecoveryUnit::setPrepareTimestamp(Timestamp timestamp) {
        invariant(_inUnitOfWork(), toString(_state));
        invariant(_prepareTimestamp.isNull(),
                  str::stream() << "Trying to set prepare timestamp to " << timestamp.toString()
                                << ". It's already set to " << _prepareTimestamp.toString());
        invariant(_commitTimestamp.isNull(),
                  str::stream() << "Commit timestamp is " << _commitTimestamp.toString()
                                << " and trying to set prepare timestamp to "
                                << timestamp.toString());
        invariant(!_lastTimestampSet, str::stream() << "Last timestamp set is "
                                                    << _lastTimestampSet->toString()
                                                    << " and trying to set prepare timestamp to "
                                                    << timestamp.toString());

        _prepareTimestamp = timestamp;
    }

    Timestamp RocksRecoveryUnit::getPrepareTimestamp() const {
        // invariant(_inUnitOfWork(), toString(_state));
        // invariant(!_prepareTimestamp.isNull());
        // invariant(_commitTimestamp.isNull(),
        //           str::stream() << "Commit timestamp is " << _commitTimestamp.toString()
        //           << " and trying to get prepare timestamp of "
        //           << _prepareTimestamp.toString());
        // invariant(!_lastTimestampSet,
        //           str::stream() << "Last timestamp set is " << _lastTimestampSet->toString()
        //           << " and trying to get prepare timestamp of "
        //           << _prepareTimestamp.toString());

        return _prepareTimestamp;
    }

    void RocksRecoveryUnit::setPrepareConflictBehavior(PrepareConflictBehavior behavior) {
        // If there is an open storage transaction, it is not valid to try to change the behavior of
        // ignoring prepare conflicts, since that behavior is applied when the transaction is
        // opened.
        invariant(
            !_isActive(),
            str::stream() << "Current state: " << toString(_state)
                          << ". Invalid internal state while setting prepare conflict behavior to: "
                          << static_cast<int>(behavior));

        _prepareConflictBehavior = behavior;
    }

    PrepareConflictBehavior RocksRecoveryUnit::getPrepareConflictBehavior() const {
        return _prepareConflictBehavior;
    }

    void RocksRecoveryUnit::setRoundUpPreparedTimestamps(bool value) {
        // This cannot be called after RocksRecoveryUnit::_txnOpen.
        invariant(!_isActive(), str::stream() << "Can't change round up prepared timestamps flag "
                                              << "when current state is " << toString(_state));
        _roundUpPreparedTimestamps =
            (value) ? RoundUpPreparedTimestamps::kRound : RoundUpPreparedTimestamps::kNoRound;
    }

    void RocksRecoveryUnit::setTimestampReadSource(ReadSource readSource,
                                                   boost::optional<Timestamp> provided) {
        LOG(3) << "setting timestamp read source: " << static_cast<int>(readSource)
               << ", provided timestamp: " << ((provided) ? provided->toString() : "none");

        invariant(!_isActive() || _timestampReadSource == readSource,
                  str::stream() << "Current state: " << toString(_state)
                                << ". Invalid internal state while setting timestamp read source: "
                                << static_cast<int>(readSource) << ", provided timestamp: "
                                << (provided ? provided->toString() : "none"));
        invariant(!provided == (readSource != ReadSource::kProvided));
        invariant(!(provided && provided->isNull()));

        _timestampReadSource = readSource;
        _readAtTimestamp = (provided) ? *provided : Timestamp();
    }

    RecoveryUnit::ReadSource RocksRecoveryUnit::getTimestampReadSource() const {
        return _timestampReadSource;
    }

    void RocksRecoveryUnit::_setState(State newState) { _state = newState; }

    bool RocksRecoveryUnit::_isActive() const {
        return State::kActiveNotInUnitOfWork == _state || State::kActive == _state;
    }

    bool RocksRecoveryUnit::_inUnitOfWork() const {
        return State::kInactiveInUnitOfWork == _state || State::kActive == _state;
    }

    bool RocksRecoveryUnit::_isCommittingOrAborting() const {
        return State::kCommitting == _state || State::kAborting == _state;
    }

    void RocksRecoveryUnit::setCatalogConflictingTimestamp(Timestamp timestamp) {
        // This cannot be called after a storage snapshot is allocated.
        invariant(!_isActive(), toString(_state));
        invariant(_timestampReadSource == ReadSource::kNoTimestamp,
                  str::stream() << "Illegal to set catalog conflicting timestamp for a read source "
                                << static_cast<int>(_timestampReadSource));
        invariant(_catalogConflictTimestamp.isNull(),
                  str::stream() << "Trying to set catalog conflicting timestamp to "
                                << timestamp.toString() << ". It's already set to "
                                << _catalogConflictTimestamp.toString());
        invariant(!timestamp.isNull());

        _catalogConflictTimestamp = timestamp;
    }

    Timestamp RocksRecoveryUnit::getCatalogConflictingTimestamp() const {
        return _catalogConflictTimestamp;
    }

    rocksdb::Status RocksRecoveryUnit::Get(rocksdb::ColumnFamilyHandle* cf,
                                           const rocksdb::Slice& key, std::string* value) {
        invariant(getTransaction());
        rocksdb::ReadOptions options;
        return _transaction->Get(options, cf, key, value);
    }

    RocksIterator* RocksRecoveryUnit::NewIterator(rocksdb::ColumnFamilyHandle* cf,
                                                  std::string prefix, bool isOplog) {
        std::unique_ptr<rocksdb::Slice> upperBound(new rocksdb::Slice());
        rocksdb::ReadOptions options;
        options.iterate_upper_bound = upperBound.get();
        invariant(getTransaction());
        auto it = _transaction->GetIterator(options, cf);
        return new PrefixStrippingIterator(cf, std::move(prefix), _transaction.get(), it,
                                           isOplog ? nullptr : _compactionScheduler,
                                           std::move(upperBound));
    }

    RocksIterator* RocksRecoveryUnit::NewIteratorWithTxn(rocksdb::TOTransaction* txn,
                                                         rocksdb::ColumnFamilyHandle* cf,
                                                         std::string prefix) {
        invariant(txn);
        std::unique_ptr<rocksdb::Slice> upperBound(new rocksdb::Slice());
        rocksdb::ReadOptions options;
        options.iterate_upper_bound = upperBound.get();
        auto it = txn->GetIterator(options);
        return new PrefixStrippingIterator(cf, std::move(prefix), txn, it, nullptr,
                                           std::move(upperBound));
    }

    void RocksRecoveryUnit::incrementCounter(const rocksdb::Slice& counterKey,
                                             std::atomic<long long>* counter, long long delta) {
        if (delta == 0) {
            return;
        }

        auto pair = _deltaCounters.find(counterKey.ToString());
        if (pair == _deltaCounters.end()) {
            _deltaCounters[counterKey.ToString()] =
                mongo::RocksRecoveryUnit::Counter(counter, delta);
        } else {
            pair->second._delta += delta;
        }
    }

    long long RocksRecoveryUnit::getDeltaCounter(const rocksdb::Slice& counterKey) {
        auto counter = _deltaCounters.find(counterKey.ToString());
        if (counter == _deltaCounters.end()) {
            return 0;
        } else {
            return counter->second._delta;
        }
    }

    void RocksRecoveryUnit::resetDeltaCounters() { _deltaCounters.clear(); }

    RocksRecoveryUnit* RocksRecoveryUnit::getRocksRecoveryUnit(OperationContext* opCtx) {
        return checked_cast<RocksRecoveryUnit*>(opCtx->recoveryUnit());
    }
}  // namespace mongo
