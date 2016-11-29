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
#include "mongo/db/storage/journal_listener.h"
#include "mongo/util/log.h"

#include "rocks_transaction.h"
#include "rocks_util.h"

#include "rocks_snapshot_manager.h"

namespace mongo {
    namespace {
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
              // noop since we don't use it and it's only available in
              // RocksDB 4.12 and higher
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
                _rocksdbSkippedDeletionsInitial =
                    rocksdb::perf_context.internal_delete_skipped_count;
            }
            void endOp() {
                if (_compactionScheduler == nullptr) {
                    return;
                }
                int skippedDeletionsOp = rocksdb::perf_context.internal_delete_skipped_count -
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

    RocksRecoveryUnit::RocksRecoveryUnit(RocksTransactionEngine* transactionEngine,
                                         RocksSnapshotManager* snapshotManager, rocksdb::DB* db,
                                         RocksCounterManager* counterManager,
                                         RocksCompactionScheduler* compactionScheduler,
                                         RocksDurabilityManager* durabilityManager,
                                         bool durable)
        : _transactionEngine(transactionEngine),
          _snapshotManager(snapshotManager),
          _db(db),
          _counterManager(counterManager),
          _compactionScheduler(compactionScheduler),
          _durabilityManager(durabilityManager),
          _durable(durable),
          _transaction(transactionEngine),
          _writeBatch(rocksdb::BytewiseComparator(), 0, true),
          _snapshot(nullptr),
          _preparedSnapshot(nullptr),
          _myTransactionCount(1) {
        RocksRecoveryUnit::_totalLiveRecoveryUnits.fetch_add(1, std::memory_order_relaxed);
    }

    RocksRecoveryUnit::~RocksRecoveryUnit() {
        if (_preparedSnapshot) {
            // somebody didn't call getPreparedSnapshot() after prepareForCreateSnapshot()
            _db->ReleaseSnapshot(_preparedSnapshot);
            _preparedSnapshot = nullptr;
        }
        _abort();
        RocksRecoveryUnit::_totalLiveRecoveryUnits.fetch_sub(1, std::memory_order_relaxed);
    }

    void RocksRecoveryUnit::beginUnitOfWork(OperationContext* opCtx) {
        invariant(!_areWriteUnitOfWorksBanned);
    }

    void RocksRecoveryUnit::commitUnitOfWork() {
        if (_writeBatch.GetWriteBatch()->Count() > 0) {
            _commit();
        }

        try {
            for (Changes::const_iterator it = _changes.begin(), end = _changes.end(); it != end;
                    ++it) {
                (*it)->commit();
            }
            _changes.clear();
        }
        catch (...) {
            std::terminate();
        }

        _releaseSnapshot();
    }

    void RocksRecoveryUnit::abortUnitOfWork() {
        _abort();
    }

    bool RocksRecoveryUnit::waitUntilDurable() {
        _durabilityManager->waitUntilDurable(false);
        return true;
    }

    void RocksRecoveryUnit::abandonSnapshot() {
        _deltaCounters.clear();
        _writeBatch.Clear();
        _releaseSnapshot();
        _areWriteUnitOfWorksBanned = false;
    }

    rocksdb::WriteBatchWithIndex* RocksRecoveryUnit::writeBatch() { return &_writeBatch; }

    void RocksRecoveryUnit::setOplogReadTill(const RecordId& record) { _oplogReadTill = record; }

    void RocksRecoveryUnit::registerChange(Change* change) { _changes.push_back(change); }

    Status RocksRecoveryUnit::setReadFromMajorityCommittedSnapshot() {
        if (!_snapshotManager->haveCommittedSnapshot()) {
            return {ErrorCodes::ReadConcernMajorityNotAvailableYet,
                    "Read concern majority reads are currently not possible."};
        }
        invariant(_snapshot == nullptr);

        _readFromMajorityCommittedSnapshot = true;
        return Status::OK();
    }

    boost::optional<SnapshotName> RocksRecoveryUnit::getMajorityCommittedSnapshot() const {
        if (!_readFromMajorityCommittedSnapshot)
            return {};
        return SnapshotName(_snapshotManager->getCommittedSnapshot().get()->name);
    }

    SnapshotId RocksRecoveryUnit::getSnapshotId() const { return SnapshotId(_myTransactionCount); }

    void RocksRecoveryUnit::_releaseSnapshot() {
        if (_snapshot) {
            _transaction.abort();
            _db->ReleaseSnapshot(_snapshot);
            _snapshot = nullptr;
        }
        _snapshotHolder.reset();

        _myTransactionCount++;
    }

    void RocksRecoveryUnit::prepareForCreateSnapshot(OperationContext* opCtx) {
        invariant(!_readFromMajorityCommittedSnapshot);
        _areWriteUnitOfWorksBanned = true;
        if (_preparedSnapshot) {
            // release old one, in case somebody calls prepareForCreateSnapshot twice in a row
            _db->ReleaseSnapshot(_preparedSnapshot);
        }
        _preparedSnapshot = _db->GetSnapshot();
    }

    void RocksRecoveryUnit::_commit() {
        rocksdb::WriteBatch* wb = _writeBatch.GetWriteBatch();
        for (auto pair : _deltaCounters) {
            auto& counter = pair.second;
            counter._value->fetch_add(counter._delta, std::memory_order::memory_order_relaxed);
            long long newValue = counter._value->load(std::memory_order::memory_order_relaxed);
            _counterManager->updateCounter(pair.first, newValue, wb);
        }

        if (wb->Count() != 0) {
            // Order of operations here is important. It needs to be synchronized with
            // _transaction.recordSnapshotId() and _db->GetSnapshot() and
            rocksdb::WriteOptions writeOptions;
            writeOptions.disableWAL = !_durable;
            auto status = _db->Write(writeOptions, wb);
            invariantRocksOK(status);
            _transaction.commit();
        }
        _deltaCounters.clear();
        _writeBatch.Clear();
    }

    void RocksRecoveryUnit::_abort() {
        try {
            for (Changes::const_reverse_iterator it = _changes.rbegin(), end = _changes.rend();
                    it != end; ++it) {
                Change* change = *it;
                LOG(2) << "CUSTOM ROLLBACK " << demangleName(typeid(*change));
                change->rollback();
            }
            _changes.clear();
        }
        catch (...) {
            std::terminate();
        }

        _deltaCounters.clear();
        _writeBatch.Clear();

        _releaseSnapshot();
    }

    const rocksdb::Snapshot* RocksRecoveryUnit::getPreparedSnapshot() {
        auto ret = _preparedSnapshot;
        _preparedSnapshot = nullptr;
        return ret;
    }

    void RocksRecoveryUnit::dbReleaseSnapshot(const rocksdb::Snapshot* snapshot) {
        _db->ReleaseSnapshot(snapshot);
    }

    const rocksdb::Snapshot* RocksRecoveryUnit::snapshot() {
        if (_readFromMajorityCommittedSnapshot) {
            if (_snapshotHolder.get() == nullptr) {
                _snapshotHolder = _snapshotManager->getCommittedSnapshot();
            }
            return _snapshotHolder->snapshot;
        }
        if (!_snapshot) {
            // RecoveryUnit might be used for writing, so we need to call recordSnapshotId().
            // Order of operations here is important. It needs to be synchronized with
            // _db->Write() and _transaction.commit()
            _transaction.recordSnapshotId();
            _snapshot = _db->GetSnapshot();
        }
        return _snapshot;
    }

    rocksdb::Status RocksRecoveryUnit::Get(const rocksdb::Slice& key, std::string* value) {
        if (_writeBatch.GetWriteBatch()->Count() > 0) {
            std::unique_ptr<rocksdb::WBWIIterator> wb_iterator(_writeBatch.NewIterator());
            wb_iterator->Seek(key);
            if (wb_iterator->Valid() && wb_iterator->Entry().key == key) {
                const auto& entry = wb_iterator->Entry();
                if (entry.type == rocksdb::WriteType::kDeleteRecord) {
                    return rocksdb::Status::NotFound();
                }
                *value = std::string(entry.value.data(), entry.value.size());
                return rocksdb::Status::OK();
            }
        }
        rocksdb::ReadOptions options;
        options.snapshot = snapshot();
        return _db->Get(options, key, value);
    }

    RocksIterator* RocksRecoveryUnit::NewIterator(std::string prefix, bool isOplog) {
        std::unique_ptr<rocksdb::Slice> upperBound(new rocksdb::Slice());
        rocksdb::ReadOptions options;
        options.iterate_upper_bound = upperBound.get();
        options.snapshot = snapshot();
        auto iterator = _writeBatch.NewIteratorWithBase(_db->NewIterator(options));
        auto prefixIterator = new PrefixStrippingIterator(std::move(prefix), iterator,
                                                          isOplog ? nullptr : _compactionScheduler,
                                                          std::move(upperBound));
        return prefixIterator;
    }

    RocksIterator* RocksRecoveryUnit::NewIteratorNoSnapshot(rocksdb::DB* db, std::string prefix) {
        std::unique_ptr<rocksdb::Slice> upperBound(new rocksdb::Slice());
        rocksdb::ReadOptions options;
        options.iterate_upper_bound = upperBound.get();
        auto iterator = db->NewIterator(rocksdb::ReadOptions());
        return new PrefixStrippingIterator(std::move(prefix), iterator, nullptr,
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

    RocksRecoveryUnit* RocksRecoveryUnit::getRocksRecoveryUnit(OperationContext* opCtx) {
        return checked_cast<RocksRecoveryUnit*>(opCtx->recoveryUnit());
    }
}
