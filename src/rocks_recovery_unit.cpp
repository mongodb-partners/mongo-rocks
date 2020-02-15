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
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include "mongo/base/checked_cast.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/server_options.h"
#include "mongo/db/storage/journal_listener.h"
#include "mongo/util/log.h"

#include "rocks_transaction.h"
#include "rocks_util.h"

#include "rocks_snapshot_manager.h"

namespace mongo {
namespace {
    // SnapshotIds need to be globally unique, as they are used in a WorkingSetMember to
    // determine if documents changed, but a different recovery unit may be used across a getMore,
    // so there is a chance the snapshot ID will be reused.
    AtomicUInt64 nextSnapshotId{1};

    logger::LogSeverity kSlowTransactionSeverity = logger::LogSeverity::Debug(1);

    class PrefixStrippingIterator : public RocksIterator {
    public:
        // baseIterator is consumed
        PrefixStrippingIterator(std::string prefix, Iterator* baseIterator,
                                RocksCompactionScheduler* compactionScheduler,
                                std::unique_ptr<rocksdb::Slice> upperBound)
            : _rocksdbSkippedDeletionsInitial(0),
              _prefix(std::move(prefix)),
              _nextPrefix(rocksGetNextPrefix(_prefix)),
              _prefixSlice(_prefix.data(), _prefix.size()),
              _prefixSliceEpsilon(_prefix.data(), _prefix.size() + 1),
              _baseIterator(baseIterator),
              _compactionScheduler(compactionScheduler),
              _upperBound(std::move(upperBound)) {
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
        virtual rocksdb::Status status() const { return _baseIterator->status(); }

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
            int skippedDeletionsOp = get_internal_delete_skipped_count() -
                                     _rocksdbSkippedDeletionsInitial;
            if (skippedDeletionsOp >=
                RocksCompactionScheduler::getSkippedDeletionsThreshold()) {
                _compactionScheduler->reportSkippedDeletionsAboveThreshold(_prefix);
            }
        }

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
    };

}  // anonymous namespace

std::atomic<int> RocksRecoveryUnit::_totalLiveRecoveryUnits(0);

RocksRecoveryUnit::RocksRecoveryUnit(rocksdb::TOTransactionDB* db,
                  RocksOplogManager* oplogManager,
                  RocksSnapshotManager* snapshotManager,
                  RocksCounterManager* counterManager,
                  RocksCompactionScheduler* compactionScheduler,
                  RocksDurabilityManager* durabilityManager, bool durable)
    : _db(db),
      _oplogManager(oplogManager),
      _snapshotManager(snapshotManager),
      _counterManager(counterManager),
      _compactionScheduler(compactionScheduler),
      _durabilityManager(durabilityManager),
      _durable(durable),
      _areWriteUnitOfWorksBanned(false),
      _inUnitOfWork(false),
      _active(false),
      _isTimestamped(false),
      _timestampReadSource(ReadSource::kUnset),
      _orderedCommit(true),
      _mySnapshotId(nextSnapshotId.fetchAndAdd(1)),
      _isOplogReader(false) {
    RocksRecoveryUnit::_totalLiveRecoveryUnits.fetch_add(1, std::memory_order_relaxed);
}

RocksRecoveryUnit::~RocksRecoveryUnit() {
    invariant(!_inUnitOfWork);
    _abort(); 
}

// TODO(wolfkdy): make _commit/commitUnitOfWork _abort/abortUnitOfWork symmetrical
void RocksRecoveryUnit::_commit() {
    for (auto pair : _deltaCounters) {
        auto& counter = pair.second;
        counter._value->fetch_add(counter._delta, std::memory_order::memory_order_relaxed);
        long long newValue = counter._value->load(std::memory_order::memory_order_relaxed);
        _counterManager->updateCounter(pair.first, newValue);
    }

    if (_active) {
        _txnClose(true /* commit */);
    }

    _deltaCounters.clear();
}

void RocksRecoveryUnit::_abort() {
    if (_active) {
        _txnClose(false /* commit */);
    }
    try {
        for (Changes::const_reverse_iterator it = _changes.rbegin(), end = _changes.rend();
                it != end; ++it) {
            Change* change = *it;
            LOG(2) << "CUSTOM ROLLBACK " << redact(demangleName(typeid(*change)));
            change->rollback();
        }
        _changes.clear();
    }
    catch (...) {
        std::terminate();
    }

    _deltaCounters.clear();
}

void RocksRecoveryUnit::beginUnitOfWork(OperationContext* opCtx) {
    invariant(!_areWriteUnitOfWorksBanned);
    invariant(!_inUnitOfWork);
    _inUnitOfWork = true;
}

void RocksRecoveryUnit::prepareUnitOfWork() {
    invariant(0, "RocksRecoveryUnit::prepareUnitOfWork should not be called");
}


void RocksRecoveryUnit::commitUnitOfWork() {
    invariant(_inUnitOfWork);
    _inUnitOfWork = false;
    auto commitTs = _commitTimestamp.isNull() ? _lastTimestampSet : _commitTimestamp;
    _commit();

    try {
        for (Changes::const_iterator it = _changes.begin(), end = _changes.end(); it != end;
                ++it) {
            (*it)->commit(commitTs);
        }
        _changes.clear();
    } catch (...) {
        std::terminate();
    }
}

void RocksRecoveryUnit::abortUnitOfWork() {
    invariant(_inUnitOfWork);
    _inUnitOfWork = false;
    _abort();
}

bool RocksRecoveryUnit::waitUntilDurable() {
    invariant(!_inUnitOfWork);
    _durabilityManager->waitUntilDurable(false);
    return true;
}

bool RocksRecoveryUnit::waitUntilUnjournaledWritesDurable() {
    waitUntilDurable();
    return true;
}

void RocksRecoveryUnit::registerChange(Change* change) {
    invariant(_inUnitOfWork);
    _changes.push_back(std::unique_ptr<Change>{change});
}

void RocksRecoveryUnit::assertInActiveTxn() const {
    invariant(_active, "WiredTigerRecoveryUnit::assertInActiveTxn failed");
}

rocksdb::TOTransaction* RocksRecoveryUnit::getTransaction() {
    if (!_active) {
        _txnOpen();
    }
    _invariant(_active);
    return _transaction.get();
}

void RocksRecoveryUnit::abandonSnapshot() {
    invariant(!_inUnitOfWork);
    _deltaCounters.clear();
    if (_active) {
        // Can't be in a WriteUnitOfWork, so safe to rollback
        _txnClose(false);
    }
    _areWriteUnitOfWorksBanned = false;
}

void RocksRecoveryUnit::preallocateSnapshot() {
    getTransaction();
}

void* RocksRecoveryUnit::writingPtr(void* data, size_t len) {
    // This API should not be used for anything other than the MMAP V1 storage engine
    MONGO_UNREACHABLE;
}

void RocksRecoveryUnit::_txnClose(bool commit) {
    _invariant(_active);
    if (_timer) {
        const int transactionTime = _timer->millis();
        if (transactionTime >= serverGlobalParams.slowMS) {
            LOG(kSlowTransactionSeverity) << "Slow Rocks transaction. Lifetime of SnapshotId "
                                          << _mySnapshotId << " was " << transactionTime << "ms";
        }
    }
    rocksdb::Status status;
    if (commit) {
        if (!_commitTimestamp.isNull()) {
            status = _transaction->SetCommitTimestamp(
                rocksdb::RocksTimeStamp(_commitTimestamp.asULL()));
            invariant(status.ok(), status.ToString());
            _isTimestamped = true;
        }
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
                            << ". _commitTimestamp: "
                            << _commitTimestamp.toString());

    // We reset the _lastTimestampSet between transactions. Since it is legal for one
    // transaction on a RecoveryUnit to call setTimestamp() and another to call
    // setCommitTimestamp().
    _lastTimestampSet = boost::none;

    _active = false;
    _prepareTimestamp = Timestamp();
    _mySnapshotId = nextSnapshotId.fetchAndAdd(1);
    _isOplogReader = false;
    _orderedCommit = true;  // Default value is true; we assume all writes are ordered.
}

SnapshotId RocksRecoveryUnit::getSnapshotId() const {
    // TODO: use actual rocks snapshot id
    return SnapshotId(_mySnapshotId);
}

Status RocksRecoveryUnit::obtainMajorityCommittedSnapshot() {
    invariant(_timestampReadSource == ReadSource::kMajorityCommitted);
    auto snapshotName = 
        Timestamp(_snapshotManager->getMinSnapshotForNextCommittedRead());
    if (!snapshotName) {
        return {ErrorCodes::ReadConcernMajorityNotAvailableYet,
                "Read concern majority reads are currently not possible."};
    }
    _majorityCommittedSnapshot = *snapshotName;
    return Status::OK();
}

boost::optional<Timestamp> RocksRecoveryUnit::getPointInTimeReadTimestamp() const {
    if (_timestampReadSource == ReadSource::kProvided ||
        _timestampReadSource == ReadSource::kLastAppliedSnapshot ||
        _timestampReadSource == ReadSource::kAllCommittedSnapshot) {
        invariant(!_readAtTimestamp.isNull());
        return _readAtTimestamp;
    }

    if (_timestampReadSource == ReadSource::kLastApplied && !_readAtTimestamp.isNull()) {
        return _readAtTimestamp;
    }

    if (_timestampReadSource == ReadSource::kMajorityCommitted) {
        invariant(!_majorityCommittedSnapshot.isNull());
        return _majorityCommittedSnapshot;
    }

    return boost::none;
}

void RocksRecoveryUnit::_txnOpen() {
    invariant(!_active);

    // Only start a timer for transaction's lifetime if we're going to log it.
    if (shouldLog(kSlowTransactionSeverity)) {
        _timer.reset(new Timer());
    }

    switch (_timestampReadSource) {
        case ReadSource::kUnset:
        case ReadSource::kNoTimestamp: {
            RocksBeginTxnBlock txnOpen(_db, _transaction);

            if (_isOplogReader) {
                auto status =
                    txnOpen.setTimestamp(Timestamp(_oplogManager->getOplogReadTimestamp()),
                                         RocksBeginTxnBlock::RoundToOldest::kRound);
                invariant(status.ok(), status.reason());
            }
            txnOpen.done();
            break;
        }
        case ReadSource::kMajorityCommitted: {
            // We reset _majorityCommittedSnapshot to the actual read timestamp used when the
            // transaction was started.
            _majorityCommittedSnapshot =
                _snapshotManager->beginTransactionOnCommittedSnapshot(_db, _transaction);
            break;
        }
        case ReadSource::kLastApplied: {
            if (_snapshotManager->getLocalSnapshot()) {
                _readAtTimestamp = _snapshotManager->beginTransactionOnLocalSnapshot(
                    _db, _transaction);
            } else {
                RocksBeginTxnBlock(_db, _transaction).done();
            }
            break;
        }
        case ReadSource::kAllCommittedSnapshot: {
            if (_readAtTimestamp.isNull()) {
                _readAtTimestamp = _beginTransactionAtAllCommittedTimestamp();
                break;
            }
            // Intentionally continue to the next case to read at the _readAtTimestamp.
        }
        case ReadSource::kLastAppliedSnapshot: {
            // Only ever read the last applied timestamp once, and continue reusing it for
            // subsequent transactions.
            if (_readAtTimestamp.isNull()) {
                _readAtTimestamp = _snapshotManager->beginTransactionOnLocalSnapshot(
                    _db, _transaction);
                break;
            }
            // Intentionally continue to the next case to read at the _readAtTimestamp.
        }
        case ReadSource::kProvided: {
            RocksBeginTxnBlock txnOpen(_db, _transaction);
            auto status = txnOpen.setTimestamp(_readAtTimestamp);

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

    LOG(3) << "Rocks begin_transaction for snapshot id " << _mySnapshotId;
    _active = true;
}

Timestamp RocksRecoveryUnit::_beginTransactionAtAllCommittedTimestamp() {
    RocksBeginTxnBlock txnOpen(_db, _transaction);
    Timestamp txnTimestamp = Timestamp(_oplogManager->fetchAllCommittedValue());
    auto status =
        txnOpen.setTimestamp(txnTimestamp, RocksBeginTxnBlock::RoundToOldest::kRound);
    invariant(status.isOK(), status.reason());
    txnOpen.done();
    return txnOpen.getTimestamp();
}

Status RocksRecoveryUnit::setTimestamp(Timestamp timestamp) {
    LOG(3) << "Rocks set timestamp of future write operations to " << timestamp;
    invariant(_inUnitOfWork);
    invariant(_prepareTimestamp.isNull());
    invariant(_commitTimestamp.isNull(),
              str::stream() << "Commit timestamp set to " << _commitTimestamp.toString()
                            << " and trying to set WUOW timestamp to "
                            << timestamp.toString());

    _lastTimestampSet = timestamp;

    getTransaction();
    invariant(_transaction.get());
    auto status = _transaction->SetCommitTimeStamp(
        rocksdb::RocksTimeStamp(timestamp.asULL()));
    if (status.ok()) {
        _isTimestamped = true;
    }
    return rocksToMongoStatus(status);
}

void RocksRecoveryUnit::setCommitTimestamp(Timestamp timestamp) {
    invariant(!_inUnitOfWork);
    invariant(_commitTimestamp.isNull(),
              str::stream() << "Commit timestamp set to " << _commitTimestamp.toString()
                            << " and trying to set it to "
                            << timestamp.toString());
    invariant(!_lastTimestampSet,
              str::stream() << "Last timestamp set is " << _lastTimestampSet->toString()
                            << " and trying to set commit timestamp to "
                            << timestamp.toString());
    invariant(!_isTimestamped);

    _commitTimestamp = timestamp;
}

Timestamp RocksRecoveryUnit::getCommitTimestamp() {
    return _commitTimestamp;
}

void RocksRecoveryUnit::clearCommitTimestamp() {
    invariant(!_inUnitOfWork);
    invariant(!_commitTimestamp.isNull());
    invariant(!_lastTimestampSet,
              str::stream() << "Last timestamp set is " << _lastTimestampSet->toString()
                            << " and trying to clear commit timestamp.");
    invariant(!_isTimestamped);

    _commitTimestamp = Timestamp();
}

void RocksRecoveryUnit::setPrepareTimestamp(Timestamp timestamp) {
    invariant(_inUnitOfWork);
    invariant(_prepareTimestamp.isNull());
    invariant(_commitTimestamp.isNull());

    invariant(0, "RocksRecoveryUnit::setPrepareTimestamp should not be called");
}

void RocksRecoveryUnit::setTimestampReadSource(ReadSource readSource,
                                                    boost::optional<Timestamp> provided) {
    LOG(3) << "setting timestamp read source: " << static_cast<int>(readSource)
           << ", provided timestamp: " << ((provided) ? provided->toString() : "none");

    invariant(!_active || _timestampReadSource == ReadSource::kUnset ||
              _timestampReadSource == readSource);
    invariant(!provided == (readSource != ReadSource::kProvided));
    invariant(!(provided && provided->isNull()));

    _timestampReadSource = readSource;
    _readAtTimestamp = (provided) ? *provided : Timestamp();
}

RecoveryUnit::ReadSource RocksRecoveryUnit::getTimestampReadSource() const {
    return _timestampReadSource;
}

rocksdb::Status RocksRecoveryUnit::Get(const rocksdb::Slice& key, std::string* value) {
    invariant(_transaction.get());
    rocksdb::ReadOptions options;    
    return _transaction->Get(options, key, value);
}

RocksIterator* RocksRecoveryUnit::NewIterator(std::string prefix, bool isOplog) {
    std::unique_ptr<rocksdb::Slice> upperBound(new rocksdb::Slice());
    rocksdb::ReadOptions options;
    options.iterate_upper_bound = upperBound.get();
    invariant(getTransaction());
    auto it = _transaction->GetIterator(options);
    return new PrefixStrippingIterator(
        std::move(prefix), it,
        isOplog ? nullptr : _compactionScheduler,
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

void RocksRecoveryUnit::resetDeltaCounters() {
    _deltaCounters.clear();
}

RocksRecoveryUnit* RocksRecoveryUnit::getRocksRecoveryUnit(OperationContext* opCtx) {
    return checked_cast<RocksRecoveryUnit*>(opCtx->recoveryUnit());
}
}
