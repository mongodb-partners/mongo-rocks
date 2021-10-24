/**
 *    Copyright (C) 2014 MongoDB Inc.
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

#include "mongo/platform/basic.h"

#include "rocks_record_store.h"

#include <algorithm>
#include <memory>
#include <mutex>

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/experimental.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include "mongo/base/checked_cast.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/modules/rocks/src/rocks_parameters_gen.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/oplog_hack.h"
#include "mongo/db/server_recovery.h"
#include "mongo/platform/endian.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/background.h"
#include "mongo/util/concurrency/idle_thread_block.h"
#include "mongo/util/log.h"
#include "mongo/util/str.h"

#include "rocks_compaction_scheduler.h"
#include "rocks_counter_manager.h"
#include "rocks_durability_manager.h"
#include "rocks_engine.h"
#include "rocks_oplog_manager.h"
#include "rocks_prepare_conflict.h"
#include "rocks_recovery_unit.h"
#include "rocks_util.h"

namespace mongo {

    using namespace fmt::literals;
    using std::string;
    using std::unique_ptr;

    static int64_t cappedMaxSizeSlackFromSize(int64_t cappedMaxSize) {
        return std::min(cappedMaxSize / 10, int64_t(16 * 1024 * 1024));
    }

    MONGO_FAIL_POINT_DEFINE(RocksWriteConflictException);
    MONGO_FAIL_POINT_DEFINE(RocksWriteConflictExceptionForReads);

    namespace {
        AtomicWord<std::uint32_t> minSSTFileCountReserved{4}; 
    }
    ExportedMinSSTFileCountReservedParameter::ExportedMinSSTFileCountReservedParameter(StringData name,
            ServerParameterType spt)
        : ServerParameter(name, spt), _data(&minSSTFileCountReserved) {}
    
    void ExportedMinSSTFileCountReservedParameter::append(OperationContext* opCtx, BSONObjBuilder& b,
                                            const std::string& name) {
        b.append(name, _data->load());
    }

    Status ExportedMinSSTFileCountReservedParameter::setFromString(const std::string& str) {
        int num = 0;
        Status status = parseNumberFromString(str, &num);
        if (!status.isOK()) {
            return status;
        }
        if (num < 1 || num > (1000 * 1000)) {
            return Status(ErrorCodes::BadValue, "minSSTFileCountReserved must be between 1 and 1 million, inclusive");
        }
        _data->store(static_cast<uint32_t>(num));
        return Status::OK();
    }

    RocksRecordStore::RocksRecordStore(RocksEngine* engine, rocksdb::ColumnFamilyHandle* cf,
          OperationContext* opCtx, Params params)
        : RecordStore(params.ns),
          _engine(engine),
          _db(engine->getDB()),
          _cf(cf),
          _oplogManager(NamespaceString::oplog(params.ns) ? engine->getOplogManager() : nullptr),
          _counterManager(engine->getCounterManager()),
          _compactionScheduler(engine->getCompactionScheduler()),
          _prefix(params.prefix),
          _isCapped(params.isCapped),
          _cappedMaxSize(params.cappedMaxSize),
          _cappedMaxSizeSlack(cappedMaxSizeSlackFromSize(params.cappedMaxSize)),
          _cappedMaxDocs(params.cappedMaxDocs),
          _cappedCallback(params.cappedCallback),
          _cappedDeleteCheckCount(0),
          _isOplog(NamespaceString::oplog(params.ns)),
          _ident(params.ident),
          _dataSizeKey(std::string("\0\0\0\0", 4) + "datasize-" + params.ident),
          _numRecordsKey(std::string("\0\0\0\0", 4) + "numrecords-" + params.ident),
          _cappedOldestKey(NamespaceString::oplog(params.ns) ? 
                           std::string("\0\0\0\0", 4) + "cappedOldestKey-" + params.ident : ""),
          _shuttingDown(false),
          _tracksSizeAdjustments(params.tracksSizeAdjustments) {

        LOG(1) << "opening collection " << params.ns << " with prefix "
               << rocksdb::Slice(_prefix).ToString(true);

        if (_isCapped) {
            invariant(_cappedMaxSize > 0);
            invariant(_cappedMaxDocs == -1 || _cappedMaxDocs > 0);
        } else {
            invariant(_cappedMaxSize == -1);
            invariant(_cappedMaxDocs == -1);
        }

        _loadCountFromCountManager(opCtx);
        // Get next id
        auto txn = std::unique_ptr<rocksdb::TOTransaction>(
            _db->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TOTransactionOptions()));
        invariant(txn);
        std::unique_ptr<RocksIterator> iter(
            RocksRecoveryUnit::NewIteratorWithTxn(txn.get(), _cf, _prefix));
        // first check if the collection is empty
        iter->SeekPrefix("");
        bool emptyCollection = !iter->Valid();
        if (!emptyCollection) {
            // if it's not empty, find next RecordId
            rocksPrepareConflictRetry(opCtx, [&] {
                iter->SeekToLast();
                return iter->status();
            });
            dassert(iter->Valid());
            rocksdb::Slice lastSlice = iter->key();
            RecordId lastId = _makeRecordId(lastSlice);
            _nextIdNum.store(lastId.repr() + 1);
        } else {
            // Need to start at 1 so we are always higher than RecordId::min()
            _nextIdNum.store(1);
            _dataSize.store(0);
            _numRecords.store(0);
            if(!_isOplog) {
                _counterManager->updateCounter(_numRecordsKey, 0);
                _counterManager->updateCounter(_dataSizeKey, 0);
                sizeRecoveryState(getGlobalServiceContext()).markCollectionAsAlwaysNeedsSizeAdjustment(_ident);
            }
        }


        _hasBackgroundThread = RocksEngine::initRsOplogBackgroundThread(params.ns);
        invariant(_isOplog == (_oplogManager != nullptr));
        invariant(_isOplog == NamespaceString::oplog(_cf->GetName()));
        if (_isOplog) {
            _engine->startOplogManager(opCtx, this);
        }
    }

    RocksRecordStore::~RocksRecordStore() {
        {
            stdx::lock_guard<stdx::timed_mutex> lk(_cappedDeleterMutex);
            _shuttingDown = true;
        }

        if (!isTemp()) {
            LOG(1) << "~RocksRecordStore for: " << ns();
        } else {
            LOG(1) << "~RocksRecordStore for temporary ident: " << getIdent();
        }

        invariant(_isOplog == (_oplogManager != nullptr));
        if (_isOplog) {
            _engine->haltOplogManager();
        }
    }

    void RocksRecordStore::_loadCountFromCountManager(OperationContext* opCtx) {
        if (_isOplog) {
            long long v = _counterManager->loadCounter(_cappedOldestKey);
            if(v > 0) {
                _cappedOldestKeyHint = RecordId(_counterManager->loadCounter(_cappedOldestKey)); 
            }
            return;
        }
        _numRecords.store(_counterManager->loadCounter(_numRecordsKey));
        _dataSize.store(_counterManager->loadCounter(_dataSizeKey));
        if (_dataSize.load() < 0) {
            _dataSize.store(0);
        }
        if (_numRecords.load() < 0) {
            _numRecords.store(0);
        }
    }

    int64_t RocksRecordStore::storageSize(OperationContext* opCtx, BSONObjBuilder* extraInfo,
                                          int infoLevel) const {
        long long size = _dataSize.load();
        if (_isOplog) {
            size = dataSize(opCtx);
        }
        // We need to make it multiple of 256 to make
        // jstests/concurrency/fsm_workloads/convert_to_capped_collection.js happy
        return static_cast<int64_t>(
            std::max(size & (~255), static_cast<long long>(256)));
    }

    RecordData RocksRecordStore::dataFor(OperationContext* opCtx, const RecordId& loc) const {
        dassert(opCtx->lockState()->isReadLocked());
        RecordData rd = _getDataFor(_cf, _prefix, opCtx, loc);
        massert(28605, "Didn't find RecordId in RocksRecordStore", (rd.data() != nullptr));
        return rd;
    }

    void RocksRecordStore::deleteRecord(OperationContext* opCtx, const RecordId& dl) {
        invariant(!_isOplog);
        std::string key(_makePrefixedKey(_prefix, dl));

        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        auto txn = ru->getTransaction();
        invariant(txn);

        std::string oldValue;
        auto status = rocksPrepareConflictRetry(opCtx, [&] { return ru->Get(_cf, key, &oldValue); });
        invariantRocksOK(status);
        int oldLength = oldValue.size();

        invariantRocksOK(ROCKS_OP_CHECK(txn->Delete(_cf, key)));
        _changeNumRecords(opCtx, -1);
        _increaseDataSize(opCtx, -oldLength);
    }

    long long RocksRecordStore::dataSize(OperationContext* opCtx) const {
        if (_isOplog) {
            uint64_t curSizeAllMemTables = 0, liveSSTTotalSize = 0;
            invariant(_db->GetRootDB()->GetIntProperty(_cf,
                                                       rocksdb::Slice("rocksdb.cur-size-all-mem-tables"),
                                                       &curSizeAllMemTables));
            invariant(_db->GetRootDB()->GetIntProperty(_cf,
                                                       rocksdb::Slice("rocksdb.live-sst-files-size"),
                                                       &liveSSTTotalSize));
            return curSizeAllMemTables + liveSSTTotalSize;
        }
        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        return _dataSize.load(std::memory_order::memory_order_relaxed) +
               ru->getDeltaCounter(_dataSizeKey);
    }

    long long RocksRecordStore::numRecords(OperationContext* opCtx) const {
        if (_isOplog) {
            uint64_t cntMem = 0, cntImm = 0;
            int64_t cntSST = 0;
            invariant(_db->GetRootDB()->GetIntProperty(_cf,
                                                       rocksdb::Slice("rocksdb.num-entries-active-mem-table"),
                                                       &cntMem));
            invariant(_db->GetRootDB()->GetIntProperty(_cf,
                                                       rocksdb::Slice("rocksdb.num-entries-imm-mem-tables"),
                                                       &cntImm));

            std::vector<rocksdb::LiveFileMetaData> allFiles;
            _db->GetRootDB()->GetLiveFilesMetaData(&allFiles);
            for (const auto& f : allFiles) {
              if (!NamespaceString::oplog(f.column_family_name)) {
                continue;
              }
              invariant(f.num_entries >= f.num_deletions);
              cntSST += f.num_entries - f.num_deletions;
            }
            return cntMem + cntImm + cntSST;
        }
        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        return _numRecords.load(std::memory_order::memory_order_relaxed) +
               ru->getDeltaCounter(_numRecordsKey);
    }

    bool RocksRecordStore::cappedAndNeedDelete(long long dataSizeDelta,
                                               long long numRecordsDelta) const {
        invariant(_isCapped);

        if (_dataSize.load() + dataSizeDelta > _cappedMaxSize) return true;

        if ((_cappedMaxDocs != -1) && (_numRecords.load() + numRecordsDelta > _cappedMaxDocs))
            return true;

        return false;
    }

    int64_t RocksRecordStore::_cappedDeleteAsNeeded(OperationContext* opCtx,
                                                    const RecordId& justInserted) {
        if (!_tracksSizeAdjustments) {
            return 0;
        }

        if (!_isCapped) {
            return 0;
        }

        // We only want to do the checks occasionally as they are expensive.
        // This variable isn't thread safe, but has loose semantics anyway.
        dassert(!_isOplog || _cappedMaxDocs == -1);

        long long dataSizeDelta = 0, numRecordsDelta = 0;
        if (!_isOplog) {
            auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
            dataSizeDelta = ru->getDeltaCounter(_dataSizeKey);
            numRecordsDelta = ru->getDeltaCounter(_numRecordsKey);
        }

        if (!cappedAndNeedDelete(dataSizeDelta, numRecordsDelta)) {
            return 0;
        }

        // ensure only one thread at a time can do deletes, otherwise they'll conflict.
        stdx::unique_lock<stdx::timed_mutex> lock(_cappedDeleterMutex, stdx::defer_lock);

        if (_cappedMaxDocs != -1) {
            lock.lock();  // Max docs has to be exact, so have to check every time.
        } else if (_hasBackgroundThread) {
            // We are foreground, and there is a background thread,

            // Check if we need some back pressure.
            if ((_dataSize.load() - _cappedMaxSize) < _cappedMaxSizeSlack) {
                return 0;
            }

            // Back pressure needed!
            // We're not actually going to delete anything, but we're going to syncronize
            // on the deleter thread.

            if (!lock.try_lock()) {
                (void)lock.try_lock_for(stdx::chrono::milliseconds(200));
            }
            return 0;
        } else {
            if (!lock.try_lock()) {
                // Someone else is deleting old records. Apply back-pressure if too far behind,
                // otherwise continue.
                if ((_dataSize.load() - _cappedMaxSize) < _cappedMaxSizeSlack) return 0;

                if (!lock.try_lock_for(stdx::chrono::milliseconds(200))) return 0;

                // If we already waited, let someone else do cleanup unless we are significantly
                // over the limit.
                if ((_dataSize.load() - _cappedMaxSize) < (2 * _cappedMaxSizeSlack)) return 0;
            }
        }

        return cappedDeleteAsNeeded_inlock(opCtx, justInserted);
    }

    // TODO(wolfkdy): delete oplogs only when there are enough sst files and use compact-filter.
    int64_t RocksRecordStore::cappedDeleteAsNeeded_inlock(OperationContext* opCtx,
                                                          const RecordId& justInserted) {
        invariant(!_isOplog);
        // we do this is a sub transaction in case it aborts
        RocksRecoveryUnit* realRecoveryUnit =
            checked_cast<RocksRecoveryUnit*>(opCtx->releaseRecoveryUnit().release());
        invariant(realRecoveryUnit);
        WriteUnitOfWork::RecoveryUnitState const realRUstate = opCtx->setRecoveryUnit(
            std::unique_ptr<RocksRecoveryUnit>(realRecoveryUnit->newRocksRecoveryUnit()),
            WriteUnitOfWork::RecoveryUnitState::kNotInUnitOfWork);

        int64_t dataSize = _dataSize.load() + realRecoveryUnit->getDeltaCounter(_dataSizeKey);
        int64_t numRecords = _numRecords.load() + realRecoveryUnit->getDeltaCounter(_numRecordsKey);

        int64_t sizeOverCap = (dataSize > _cappedMaxSize) ? dataSize - _cappedMaxSize : 0;
        int64_t sizeSaved = 0;
        int64_t docsOverCap = 0, docsRemoved = 0;
        if (_cappedMaxDocs != -1 && numRecords > _cappedMaxDocs) {
            docsOverCap = numRecords - _cappedMaxDocs;
        }
        BSONObj emptyBson;

        try {
            WriteUnitOfWork wuow(opCtx);
            auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
            invariant(ru);
            auto txn = ru->getTransaction();
            invariant(txn);
            std::unique_ptr<rocksdb::Iterator> iter;
            iter.reset(ru->NewIterator(_cf, _prefix));
            int64_t storage;
            rocksPrepareConflictRetry(opCtx, [&] {
                iter->Seek(RocksRecordStore::_makeKey(_cappedOldestKeyHint, &storage));
                return iter->status();
            });

            RecordId newestOld;
            while ((sizeSaved < sizeOverCap || docsRemoved < docsOverCap) &&
                   (docsRemoved < 20000) && iter->Valid()) {
                newestOld = _makeRecordId(iter->key());

                // don't go past the record we just inserted
                if (newestOld >= justInserted) {
                    break;
                }

                if (_shuttingDown) {
                    break;
                }

                std::string key(_makePrefixedKey(_prefix, newestOld));
                invariantRocksOK(ROCKS_OP_CHECK(txn->Delete(_cf, key)));
                rocksdb::Slice oldValue;
                ++docsRemoved;
                oldValue = iter->value();
                sizeSaved += oldValue.size();
                {
                    stdx::lock_guard<Latch> lk(_cappedCallbackMutex);
                    if (_cappedCallback) {
                        uassertStatusOK(_cappedCallback->aboutToDeleteCapped(
                            opCtx, newestOld,
                            RecordData(static_cast<const char*>(oldValue.data()),
                                       oldValue.size())));
                    }
                }

                rocksPrepareConflictRetry(opCtx, [&] {
                    iter->Next();
                    return iter->status();
                });
            }

            if (!iter->Valid() && !iter->status().ok()) {
                log() << "RocksDB iterator failure when trying to delete capped, ignoring: "
                      << redact(iter->status().ToString());
            }

            if (docsRemoved > 0) {
                _changeNumRecords(opCtx, -docsRemoved);
                _increaseDataSize(opCtx, -sizeSaved);
                wuow.commit();
            }

            if (iter->Valid()) {
                auto oldestAliveRecordId = _makeRecordId(iter->key());
                // we check if there's outstanding transaction that is older than
                // oldestAliveRecordId. If there is, we should not skip deleting that record next
                // time we clean up the capped collection. If there isn't, we know for certain this
                // is the record we'll start out deletions from next time
                _cappedOldestKeyHint = oldestAliveRecordId;
            }
        } catch (const WriteConflictException&) {
            opCtx->releaseRecoveryUnit();
            opCtx->setRecoveryUnit(std::unique_ptr<RecoveryUnit>(realRecoveryUnit), realRUstate);
            log() << "got conflict truncating capped, ignoring";
            return 0;
        } catch (...) {
            opCtx->releaseRecoveryUnit();
            opCtx->setRecoveryUnit(std::unique_ptr<RecoveryUnit>(realRecoveryUnit), realRUstate);
            throw;
        }

        opCtx->releaseRecoveryUnit();
        opCtx->setRecoveryUnit(std::unique_ptr<RecoveryUnit>(realRecoveryUnit), realRUstate);

        return docsRemoved;
    }

    bool RocksRecordStore::reclaimOplog(OperationContext* opCtx) {
        if (!_engine->supportsRecoverToStableTimestamp()) {
            // For non-RTT storage engines, the oplog can always be truncated.
            return reclaimOplog(opCtx, Timestamp::max());
        }
        const auto lastStableCheckpointTimestamp = _engine->getLastStableRecoveryTimestamp();
        return reclaimOplog(opCtx,
                            lastStableCheckpointTimestamp ? *lastStableCheckpointTimestamp : Timestamp::min());
    }

    bool RocksRecordStore::reclaimOplog(OperationContext* opCtx, Timestamp persistedTimestamp) {
        std::vector<rocksdb::LiveFileMetaData> allFiles, oplogFiles, pendingDelFiles;
        ssize_t oplogTotalBytes = 0, pendingDelSize = 0;
        std::string maxDelKey;
        _db->GetRootDB()->GetLiveFilesMetaData(&allFiles);
        for (const auto& f : allFiles) {
            if (!NamespaceString::oplog(f.column_family_name)) {
                continue;
            }
            auto largestTs = _prefixedKeyToTimestamp(f.largestkey);
            if (largestTs > persistedTimestamp) {
                continue;
            }
            oplogFiles.push_back(f);
            oplogTotalBytes += f.size;
        }
        std::sort(oplogFiles.begin(), oplogFiles.end(),
                  [&](const rocksdb::LiveFileMetaData& a, const rocksdb::LiveFileMetaData& b) {
            return a.smallestkey < b.smallestkey;
        });
        for (const auto& f : oplogFiles) {
            if (oplogTotalBytes - pendingDelSize > _cappedMaxSize + static_cast<ssize_t>(f.size)) {
                pendingDelSize += f.size;
                pendingDelFiles.push_back(f);
                maxDelKey = std::max(maxDelKey, f.largestkey);
            }
        }
        if (pendingDelFiles.size() < static_cast<uint32_t>(minSSTFileCountReserved.load())) {
            return false;
        }
        {
            // update _cappedOldestKeyHint
            auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
            invariant(ru);
            std::unique_ptr<RocksIterator> iterator(ru->NewIterator(_cf, _prefix, _isOplog));
            rocksdb::Slice maxDelKeyWithoutPrefix(maxDelKey.data(), maxDelKey.size());
            maxDelKeyWithoutPrefix.remove_prefix(_prefix.size());
            rocksPrepareConflictRetry(opCtx, [&] {
                iterator->Seek(rocksdb::Slice(maxDelKeyWithoutPrefix));
                return iterator->status();
            });
            if (!iterator->Valid()) {
                LOG(0) << "reclaimOplog oplog seekto " << _prefixedKeyToTimestamp(maxDelKey)
                       << " failed " << rocksToMongoStatus(iterator->status());
                return false;
            }
            rocksPrepareConflictRetry(opCtx, [&] {
                iterator->Next();
                return iterator->status();
            });
            if (!iterator->Valid()) {
                if (!iterator->status().ok()) {
                    LOG(0) << "reclaimOplog oplog lookup next " << _prefixedKeyToTimestamp(maxDelKey)
                           << " failed " << rocksToMongoStatus(iterator->status());
                }
                return false;
            }
            LOG(0) << "reclaimOplog update _cappedOldestKeyHint from "
                   << Timestamp(_cappedOldestKeyHint.repr()) << " to "
                   << Timestamp(_makeRecordId(iterator->key()).repr())
                   << " maxDelKey is " << _prefixedKeyToTimestamp(maxDelKey);
            // TODO(deyukong): we should persist _cappedOldestKeyHint because
            // there is a small chance that DeleteFilesInRange successes but CompactRange fails
            // and then crashes and restarts. In this case, there is a hole at the front of oplogs.
            // Currently, we just log this error.
            _cappedOldestKeyHint = std::max(_cappedOldestKeyHint, _makeRecordId(iterator->key()));

            invariant(!_cappedOldestKey.empty());
            _counterManager->updateCounter(_cappedOldestKey, _cappedOldestKeyHint.repr());
            _counterManager->sync();
            LOG(0) << "cuixin: save _cappedOldestKeyHint: " << _cappedOldestKeyHint;
            {
                // for test
                _loadCountFromCountManager(opCtx);
            }
        }
        auto s = _compactionScheduler->compactOplog(_cf, _prefix, maxDelKey);

        if (s.isOK()) {
            LOG(0) << "reclaimOplog to " << _prefixedKeyToTimestamp(maxDelKey) << " success";
        } else {
            LOG(0) << "reclaimOplog to " << _prefixedKeyToTimestamp(maxDelKey) << " fail " << s;
        }
        return s.isOK();
    }

    StatusWith<RecordId> RocksRecordStore::insertRecord(OperationContext* opCtx, const char* data,
                                                        int len, Timestamp timestamp) {
        dassert(opCtx->lockState()->isWriteLocked());
        if (_isCapped && len > _cappedMaxSize) {
            return StatusWith<RecordId>(ErrorCodes::BadValue,
                                        "object to insert exceeds cappedMaxSize");
        }

        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        auto txn = ru->getTransaction();
        invariant(txn);

        RecordId loc;
        if (_isOplog) {
            StatusWith<RecordId> status = oploghack::extractKey(data, len);
            if (!status.isOK()) {
                return status;
            }
            loc = status.getValue();
        } else {
            loc = _nextId();
        }

        Timestamp ts;
        if (timestamp.isNull() && _isOplog) {
            ts = Timestamp(loc.repr());
            opCtx->recoveryUnit()->setOrderedCommit(false);
        } else {
            ts = timestamp;
        }
        if (!ts.isNull()) {
            auto s = opCtx->recoveryUnit()->setTimestamp(ts);
            invariant(s.isOK(), s.reason());
        }
        invariantRocksOK(
            ROCKS_OP_CHECK(txn->Put(_cf, _makePrefixedKey(_prefix, loc), rocksdb::Slice(data, len))));

        _changeNumRecords(opCtx, 1);
        _increaseDataSize(opCtx, len);

        _cappedDeleteAsNeeded(opCtx, loc);

        return StatusWith<RecordId>(loc);
    }

    bool RocksRecordStore::haveCappedWaiters() {
        stdx::lock_guard<Latch> cappedCallbackLock(_cappedCallbackMutex);
        return _cappedCallback && _cappedCallback->haveCappedWaiters();
    }

    void RocksRecordStore::notifyCappedWaitersIfNeeded() {
        stdx::lock_guard<Latch> cappedCallbackLock(_cappedCallbackMutex);
        // This wakes up cursors blocking in await_data.
        if (_cappedCallback) {
            _cappedCallback->notifyCappedWaitersIfNeeded();
        }
    }

    Status RocksRecordStore::insertRecordsWithDocWriter(OperationContext* opCtx,
                                                        const DocWriter* const* docs,
                                                        const Timestamp* timestamps, size_t nDocs,
                                                        RecordId* idsOut) {
        std::unique_ptr<Record[]> records(new Record[nDocs]);

        size_t totalSize = 0;
        for (size_t i = 0; i < nDocs; i++) {
            const size_t docSize = docs[i]->documentSize();
            records[i].data =
                RecordData(nullptr, docSize);  // We fill in the real ptr in next loop.
            totalSize += docSize;
        }

        std::unique_ptr<char[]> buffer(new char[totalSize]);
        char* pos = buffer.get();
        for (size_t i = 0; i < nDocs; i++) {
            docs[i]->writeDocument(pos);
            const size_t size = records[i].data.size();
            records[i].data = RecordData(pos, size);
            pos += size;
        }
        invariant(pos == (buffer.get() + totalSize));

        for (size_t i = 0; i < nDocs; ++i) {
            auto s =
                insertRecord(opCtx, records[i].data.data(), records[i].data.size(), Timestamp());
            if (!s.isOK()) return s.getStatus();
            if (idsOut) idsOut[i] = s.getValue();
        }

        return Status::OK();
    }

    Status RocksRecordStore::updateRecord(OperationContext* opCtx, const RecordId& loc,
                                          const char* data, int len) {
        std::string key(_makePrefixedKey(_prefix, loc));

        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        auto txn = ru->getTransaction();
        invariant(txn);

        std::string old_value;
        auto status = rocksPrepareConflictRetry(opCtx, [&] { return ru->Get(_cf, key, &old_value); });
        invariantRocksOK(status);

        int old_length = old_value.size();

        invariantRocksOK(ROCKS_OP_CHECK(txn->Put(_cf, key, rocksdb::Slice(data, len))));

        _increaseDataSize(opCtx, len - old_length);

        _cappedDeleteAsNeeded(opCtx, loc);

        return Status::OK();
    }

    bool RocksRecordStore::updateWithDamagesSupported() const { return false; }

    StatusWith<RecordData> RocksRecordStore::updateWithDamages(
        OperationContext* opCtx, const RecordId& loc, const RecordData& oldRec,
        const char* damageSource, const mutablebson::DamageVector& damages) {
        MONGO_UNREACHABLE;
    }

    std::unique_ptr<SeekableRecordCursor> RocksRecordStore::getCursor(OperationContext* opCtx,
                                                                      bool forward) const {
        RecordId startIterator;
        if (_isOplog && forward) {
            auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
            // If we already have a snapshot we don't know what it can see, unless we know no
            // one else could be writing (because we hold an exclusive lock).
            invariant(!ru->inActiveTxn() ||
                      opCtx->lockState()->isCollectionLockedForMode(NamespaceString(_ns), MODE_X));

            startIterator = _cappedOldestKeyHint;
            ru->setIsOplogReader();
        }

        return stdx::make_unique<Cursor>(opCtx, _db, _cf, _prefix, forward, _isCapped, _isOplog,
                                         startIterator);
    }

    Status RocksRecordStore::truncate(OperationContext* opCtx) {
        // We can't use getCursor() here because we need to ignore the visibility of records (i.e.
        // we need to delete all records, regardless of visibility)
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        std::unique_ptr<RocksIterator> iterator(ru->NewIterator(_cf, _prefix, _isOplog));
        if (!_isOplog) {
            for (rocksPrepareConflictRetry(opCtx,
                                       [&] {
                                           iterator->SeekToFirst();
                                           return iterator->status();
                                       });
             iterator->Valid(); rocksPrepareConflictRetry(opCtx, [&] {
                 iterator->Next();
                 return iterator->status();
             })) {
                 deleteRecord(opCtx, _makeRecordId(iterator->key()));
           }
        } else {
            iterator->SeekToFirst();
            if(iterator->Valid()){
                const bool inclusive = true;
                const bool isTruncate = true;
                cappedTruncateAfter(opCtx, _makeRecordId(iterator->key()), inclusive, isTruncate);
            }
        }

        return rocksToMongoStatus(iterator->status());
    }

    Status RocksRecordStore::compact(OperationContext* opCtx) {
        std::string beginString(_makePrefixedKey(_prefix, RecordId()));
        std::string endString(_makePrefixedKey(_prefix, RecordId::max()));
        rocksdb::Slice beginRange(beginString);
        rocksdb::Slice endRange(endString);
        return rocksToMongoStatus(_db->CompactRange(&beginRange, &endRange));
    }

    void RocksRecordStore::validate(OperationContext* opCtx, ValidateCmdLevel level,
                                    ValidateResults* results, BSONObjBuilder* output) {
        // NOTE(cuixin): SERVER-38886 Refactor RecordStore::validate implementations
        // should not do any work, the code is move to _genericRecordStoreValidate
    }

    void RocksRecordStore::appendCustomStats(OperationContext* opCtx, BSONObjBuilder* result,
                                             double scale) const {
        result->appendBool("capped", _isCapped);
        if (_isCapped) {
            result->appendIntOrLL("max", _cappedMaxDocs);
            result->appendIntOrLL("maxSize", _cappedMaxSize / scale);
        }
    }

    Status RocksRecordStore::oplogDiskLocRegister(OperationContext* opCtx, const Timestamp& opTime,
                                                  bool orderedCommit) {
        invariant(_isOplog);
        StatusWith<RecordId> record = oploghack::keyForOptime(opTime);

        opCtx->recoveryUnit()->setOrderedCommit(orderedCommit);
        if (!orderedCommit) {
            // This labels the current transaction with a timestamp.
            // This is required for oplog visibility to work correctly, as RocksDB uses the
            // transaction list to determine where there are holes in the oplog.
            return opCtx->recoveryUnit()->setTimestamp(opTime);
        }
        invariant(_oplogManager);
        _oplogManager->setOplogReadTimestamp(opTime);
        return Status::OK();
    }

    void RocksRecordStore::waitForAllEarlierOplogWritesToBeVisible(OperationContext* opCtx) const {
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        invariant(_oplogManager);
        if (_oplogManager->isRunning()) {
            _oplogManager->waitForAllEarlierOplogWritesToBeVisible(this, opCtx);
        }
    }

    void RocksRecordStore::updateStatsAfterRepair(OperationContext* opCtx, long long numRecords,
                                                  long long dataSize) {
        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        ru->resetDeltaCounters();
        if (!_isOplog) {
            _numRecords.store(numRecords);
            _dataSize.store(dataSize);
            _counterManager->updateCounter(_numRecordsKey, numRecords);
            _counterManager->updateCounter(_dataSizeKey, dataSize);
        }
    }

    /**
     * Return the RecordId of an oplog entry as close to startingPosition as possible without
     * being higher. If there are no entries <= startingPosition, return RecordId().
     */
    boost::optional<RecordId> RocksRecordStore::oplogStartHack(
        OperationContext* opCtx, const RecordId& startingPosition) const {
        dassert(opCtx->lockState()->isReadLocked());

        if (!_isOplog) {
            return boost::none;
        }

        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        ru->setIsOplogReader();

        RecordId searchFor = startingPosition;
        auto visibilityTs = ru->getOplogVisibilityTs();
        if (visibilityTs && searchFor.repr() > *visibilityTs) {
            searchFor = RecordId(*visibilityTs);
        }

        std::unique_ptr<rocksdb::Iterator> iter(ru->NewIterator(_cf, _prefix));
        int64_t storage;
        rocksPrepareConflictRetry(opCtx, [&] {
            iter->Seek(_makeKey(searchFor, &storage));
            return iter->status();
        });
        if (!iter->Valid()) {
            rocksPrepareConflictRetry(opCtx, [&] {
                iter->SeekToLast();
                return iter->status();
            });
            if (iter->Valid()) {
                // startingPosition is bigger than everything else
                auto rcd = _makeRecordId(iter->key());
                return rcd >= _cappedOldestKeyHint ? rcd : RecordId();
            } else {
                invariantRocksOK(iter->status());
                // record store is empty
                return RecordId();
            }
        }

        // We're at or past target:
        // 1) if we're at -- return
        // 2) if we're past -- do a prev()
        RecordId foundKey = _makeRecordId(iter->key());
        int cmp = searchFor.compare(foundKey);
        if (cmp != 0) {
            // RocksDB invariant -- iterator needs to land at or past target when Seek-ing
            invariant(cmp < 0);
            // we're past target -- prev()
            rocksPrepareConflictRetry(opCtx, [&] {
                iter->Prev();
                return iter->status();
            });
        }

        if (!iter->Valid()) {
            invariantRocksOK(iter->status());
            // there are no entries <= searchFor
            return RecordId();
        }
        auto rcd = _makeRecordId(iter->key());
        return rcd >= _cappedOldestKeyHint ? rcd : RecordId();
    }

    void RocksRecordStore::cappedTruncateAfter(OperationContext* opCtx, RecordId end,
                                               bool inclusive) {
        cappedTruncateAfter(opCtx, end, inclusive, false);
    }
    void RocksRecordStore::cappedTruncateAfter(OperationContext* opCtx, RecordId end,
                                               bool inclusive, bool isTruncate) {
        if (isTruncate) {
            invariant(inclusive && _isOplog);
        }

        // Only log messages at a lower level here for testing.
        int logLevel = getTestCommandsEnabled() ? 0 : 2;

        std::unique_ptr<SeekableRecordCursor> cursor = getCursor(opCtx, true);
        LOG(logLevel) << "Truncating capped collection '" << _ns
                      << "' in RocksDB record store, (inclusive=" << inclusive << ")";

        auto record = cursor->seekExact(end);
        invariant(record, str::stream() << "Failed to seek to the record located at " << end);

        int64_t recordsRemoved = 0;
        RecordId lastKeptId;
        RecordId firstRemovedId;
        RecordId lastRemovedId;

        if (inclusive) {
            std::unique_ptr<SeekableRecordCursor> reverseCursor = getCursor(opCtx, false);
            invariant(reverseCursor->seekExact(end));
            auto prev = reverseCursor->next();
            if (prev) {
                lastKeptId = prev->id;
            } else {
                invariant(_isOplog && isTruncate);
                lastKeptId = RecordId();
            }
            firstRemovedId = end;
            LOG(0) << "lastKeptId: " << Timestamp(lastKeptId.repr());
        } else {
            // If not deleting the record located at 'end', then advance the cursor to the first
            // record
            // that is being deleted.
            record = cursor->next();
            if (!record) {
                LOG(logLevel) << "No records to delete for truncation";
                return;  // No records to delete.
            }
            lastKeptId = end;
            firstRemovedId = record->id;
            LOG(0) << "firstRemovedId: " << Timestamp(lastKeptId.repr());
        }

        WriteUnitOfWork wuow(opCtx);
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        // NOTE(wolfkdy): truncate del should have no commit-ts
        invariant(ru->getCommitTimestamp() == Timestamp());
        {
            stdx::lock_guard<Latch> cappedCallbackLock(_cappedCallbackMutex);
            do {
                if (_cappedCallback) {
                    uassertStatusOK(
                        _cappedCallback->aboutToDeleteCapped(opCtx, record->id, record->data));
                }
                if (!_isOplog) {
                    deleteRecord(opCtx, record->id);
                }
                recordsRemoved++;
                lastRemovedId = record->id;
                LOG(logLevel) << "Record id to delete for truncation of '" << _ns
                              << "': " << record->id << " (" << Timestamp(record->id.repr()) << ")";
            } while ((record = cursor->next()));
        }
        wuow.commit();
        // Compute the number and associated sizes of the records to delete.
        // Truncate the collection starting from the record located at 'firstRemovedId' to the end
        // of
        // the collection.
        LOG(0) << "Truncating collection '" << _ns << "' from " << firstRemovedId << " ("
                      << Timestamp(firstRemovedId.repr()) << ")"
                      << " to the (" <<  Timestamp(lastRemovedId.repr()) 
                      << "). Number of records to delete: " << recordsRemoved;

        if (_isOplog) {
            int retryCnt = 0;
            Timestamp truncTs(lastKeptId.repr());
            {
                auto alterClient =
                    opCtx->getServiceContext()->makeClient("reconstruct-check-oplog-removed");
                AlternativeClientRegion acr(alterClient);
                const auto tmpOpCtx = cc().makeOperationContext();
                /* TODO(wolfkdy): RocksHarnessHelper did not register global rocksEngine
                 * so RocksRecoveryUnit wont be set atomitly by StorageClientObserver::onCreateOperationContext
                 * remove this line below when this issue is fixed
                 */ 
                tmpOpCtx->setRecoveryUnit(std::unique_ptr<RecoveryUnit>(_engine->newRecoveryUnit()),
                                          WriteUnitOfWork::RecoveryUnitState::kNotInUnitOfWork);
                auto checkRemovedOK = [&] {
                    RocksRecoveryUnit::getRocksRecoveryUnit(tmpOpCtx.get())->abandonSnapshot();
                    std::unique_ptr<SeekableRecordCursor> cursor1 = getCursor(tmpOpCtx.get(), true);
                    auto rocksCursor = dynamic_cast<RocksRecordStore::Cursor*>(cursor1.get());
                    auto record = rocksCursor->seekToLast();
                    
                    Timestamp lastTs = record? Timestamp(record->id.repr()) : Timestamp();
                    invariant(lastTs >= truncTs);
                    LOG(logLevel) << "lastTs is " << lastTs << ", truncTs: " << truncTs;
                    return (lastTs == truncTs);
                };
                while(true) {
                    auto s = _compactionScheduler->compactOplog(_cf,
                                                                _makePrefixedKey(_prefix, firstRemovedId),
                                                                _makePrefixedKey(_prefix, lastRemovedId));
                    invariant(s.isOK());
                    invariant(retryCnt++ < 10);
                    if(checkRemovedOK()) {
                        break;
                    }
                    LOG(logLevel) << "retryCnt: " << retryCnt;
                }

            }
            if (!isTruncate) {
                // Immediately rewind visibility to our truncation point, to prevent new
                // transactions from appearing.
                LOG(logLevel) << "Rewinding oplog visibility point to " << truncTs
                              << " after truncation.";

                if (!serverGlobalParams.enableMajorityReadConcern &&
                    _engine->getOldestTimestamp() > truncTs) {
                    // If majority read concern is disabled, we must set the oldest timestamp along
                    // with the commit timestamp. Otherwise, the commit timestamp might be set
                    // behind the oldest timestamp.
                    const bool force = true;
                    _engine->setOldestTimestamp(truncTs, force);
                } else {
                    const bool force = false;
                    invariantRocksOK(_engine->getDB()->SetTimeStamp(
                        rocksdb::TimeStampType::kCommitted,
                        rocksdb::RocksTimeStamp(truncTs.asULL()), force));
                }

                _oplogManager->setOplogReadTimestamp(truncTs);
                LOG(1) << "truncation new read timestamp: " << truncTs;
            }
        }
    }

    RecordId RocksRecordStore::_nextId() {
        invariant(!_isOplog);
        return RecordId(_nextIdNum.fetchAndAdd(1));
    }

    rocksdb::Slice RocksRecordStore::_makeKey(const RecordId& loc, int64_t* storage) {
        *storage = endian::nativeToBig(loc.repr());
        return rocksdb::Slice(reinterpret_cast<const char*>(storage), sizeof(*storage));
    }

    std::string RocksRecordStore::_makePrefixedKey(const std::string& prefix, const RecordId& loc) {
        int64_t storage;
        auto encodedLoc = _makeKey(loc, &storage);
        std::string key(prefix);
        key.append(encodedLoc.data(), encodedLoc.size());
        return key;
    }

    Timestamp RocksRecordStore::_prefixedKeyToTimestamp(const std::string& key) const {
        rocksdb::Slice slice(key);
        slice.remove_prefix(_prefix.size());
        return Timestamp(_makeRecordId(slice).repr());
    }

    RecordId RocksRecordStore::_makeRecordId(const rocksdb::Slice& slice) {
        invariant(slice.size() == sizeof(int64_t));
        int64_t repr = endian::bigToNative(*reinterpret_cast<const int64_t*>(slice.data()));
        RecordId a(repr);
        return RecordId(repr);
    }

    bool RocksRecordStore::findRecord(OperationContext* opCtx, const RecordId& loc,
                                      RecordData* out) const {
        RecordData rd = _getDataFor(_cf, _prefix, opCtx, loc);
        if (rd.data() == NULL) return false;
        *out = rd;
        return true;
    }

    RecordData RocksRecordStore::_getDataFor(rocksdb::ColumnFamilyHandle* cf,
                                             const std::string& prefix, OperationContext* opCtx,
                                             const RecordId& loc) {
        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);

        std::string valueStorage;
        auto key = _makePrefixedKey(prefix, loc);
        auto status = rocksPrepareConflictRetry(opCtx, [&] { return ru->Get(cf, key, &valueStorage); });
        if (status.IsNotFound()) {
            return RecordData(nullptr, 0);
        }
        invariantRocksOK(status);

        SharedBuffer data = SharedBuffer::allocate(valueStorage.size());
        memcpy(data.get(), valueStorage.data(), valueStorage.size());
        return RecordData(data, valueStorage.size());
    }

    void RocksRecordStore::_changeNumRecords(OperationContext* opCtx, int64_t amount) {
        if (!_tracksSizeAdjustments) {
            return;
        }
        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        ru->incrementCounter(_numRecordsKey, &_numRecords, amount);
    }

    void RocksRecordStore::_increaseDataSize(OperationContext* opCtx, int64_t amount) {
        if (!_tracksSizeAdjustments) {
            return;
        }
        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        ru->incrementCounter(_dataSizeKey, &_dataSize, amount);
    }

    // --------

    RocksRecordStore::Cursor::Cursor(OperationContext* opCtx, rocksdb::TOTransactionDB* db,
                                     rocksdb::ColumnFamilyHandle* cf,
                                     std::string prefix, bool forward, bool isCapped, bool isOplog,
                                     RecordId startIterator)
        : _opCtx(opCtx),
          _db(db),
          _cf(cf),
          _prefix(std::move(prefix)),
          _forward(forward),
          _isCapped(isCapped),
          _isOplog(isOplog) {
        if (_isOplog) {
            _oplogVisibleTs =
                RocksRecoveryUnit::getRocksRecoveryUnit(opCtx)->getOplogVisibilityTs();
        }
        if (!startIterator.isNull()) {
            // This is a hack to speed up first/last record retrieval from the oplog
            _needFirstSeek = false;
            _lastLoc = startIterator;
            iterator();
            _skipNextAdvance = true;
            _eof = false;
        }
    }

    // requires !_eof
    void RocksRecordStore::Cursor::positionIterator() {
        _skipNextAdvance = false;
        int64_t locStorage;
        auto seekTarget = RocksRecordStore::_makeKey(_lastLoc, &locStorage);
        if (!_iterator->Valid() || _iterator->key() != seekTarget) {
            rocksPrepareConflictRetry(_opCtx, [&] {
                _iterator->Seek(seekTarget);
                return _iterator->status();
            });
            if (!_iterator->Valid()) {
                invariantRocksOK(_iterator->status());
            }
        }

        if (_forward) {
            // If _skipNextAdvance is true we landed after where we were. Return our new location on
            // the next call to next().
            _skipNextAdvance = !_iterator->Valid() || _lastLoc != _makeRecordId(_iterator->key());
        } else {
            // Seek() lands on or after the key, while reverse cursors need to land on or before.
            if (!_iterator->Valid()) {
                // Nothing left on or after.
                rocksPrepareConflictRetry(_opCtx, [&] {
                    _iterator->SeekToLast();
                    return _iterator->status();
                });
                invariantRocksOK(_iterator->status());
                _skipNextAdvance = true;
            } else {
                if (_lastLoc != _makeRecordId(_iterator->key())) {
                    // Landed after. This is true: iterator->key() > _lastLoc
                    // Since iterator is valid and Seek() landed after key,
                    // iterator will still be valid after we call Prev().
                    _skipNextAdvance = true;
                    rocksPrepareConflictRetry(_opCtx, [&] {
                        _iterator->Prev();
                        return _iterator->status();
                    });
                }
            }
        }
        // _lastLoc != _makeRecordId(_iterator->key()) indicates that the record _lastLoc was
        // deleted. In this case, mark _eof only if the collection is capped.
        _eof = !_iterator->Valid() || (_isCapped && _lastLoc != _makeRecordId(_iterator->key()));
    }

    rocksdb::Iterator* RocksRecordStore::Cursor::iterator() {
        if (_iterator.get() != nullptr) {
            return _iterator.get();
        }
        _iterator.reset(RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx)->NewIterator(
            _cf, _prefix, _isOplog /* isOplog */));
        if (!_needFirstSeek) {
            positionIterator();
        }
        return _iterator.get();
    }

    boost::optional<Record> RocksRecordStore::Cursor::next() {
        if (_eof) {
            return {};
        }

        auto iter = iterator();
        // ignore _eof

        if (!_skipNextAdvance) {
            if (_needFirstSeek) {
                _needFirstSeek = false;
                if (_forward) {
                    rocksPrepareConflictRetry(_opCtx, [&] {
                        iter->SeekToFirst();
                        return iter->status();
                    });
                } else {
                    rocksPrepareConflictRetry(_opCtx, [&] {
                        iter->SeekToLast();
                        return iter->status();
                    });
                }
            } else {
                if (_forward) {
                    rocksPrepareConflictRetry(_opCtx, [&] {
                        iter->Next();
                        return iter->status();
                    });
                } else {
                    rocksPrepareConflictRetry(_opCtx, [&] {
                        iter->Prev();
                        return iter->status();
                    });
                }
            }
        }
        _skipNextAdvance = false;

        return curr();
    }

    boost::optional<Record> RocksRecordStore::Cursor::seekExact(const RecordId& id) {
        if (_oplogVisibleTs && id.repr() > *_oplogVisibleTs) {
            _eof = true;
            return {};
        }
        _needFirstSeek = false;
        _skipNextAdvance = false;
        _iterator.reset();

        auto key = _makePrefixedKey(_prefix, id);
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx);
        auto status =
            rocksPrepareConflictRetry(_opCtx, [&] { return ru->Get(_cf, key, &_seekExactResult); });

        if (status.IsNotFound()) {
            _eof = true;
            return {};
        }
        invariantRocksOK(status);

        _eof = false;
        _lastLoc = id;

        return {{_lastLoc, {_seekExactResult.data(), static_cast<int>(_seekExactResult.size())}}};
    }
    
    boost::optional<Record> RocksRecordStore::Cursor::seekToLast() {
        _needFirstSeek = false;
        _skipNextAdvance = false;
        // do not support backwoard
        invariant(_forward);
        auto iter = iterator();
        rocksPrepareConflictRetry(_opCtx, [&] { 
                                  iter->SeekToLast(); 
                                  return iter->status(); 
                                  });
        return curr();
    }

    void RocksRecordStore::Cursor::save() {
        try {
            if (_iterator) {
                _iterator.reset();
            }
            _oplogVisibleTs = boost::none;
        } catch (const WriteConflictException&) {
            // Ignore since this is only called when we are about to kill our transaction
            // anyway.
        }
    }

    void RocksRecordStore::Cursor::saveUnpositioned() {
        save();
        _eof = true;
    }

    bool RocksRecordStore::Cursor::restore() {
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx);
        if (_isOplog && _forward) {
            ru->setIsOplogReader();
            _oplogVisibleTs = ru->getOplogVisibilityTs();
        }
        if (!_iterator.get()) {
            _iterator.reset(ru->NewIterator(_cf, _prefix, _isOplog));
        }
        _skipNextAdvance = false;

        if (_eof) return true;
        if (_needFirstSeek) return true;

        positionIterator();
        // Return false if the collection is capped and we reached an EOF. Otherwise return true.
        if (_isOplog) {
            invariant(_isCapped);
        }
        return _isCapped && _eof ? false : true;
    }

    void RocksRecordStore::Cursor::detachFromOperationContext() {
        _opCtx = nullptr;
        _iterator.reset();
    }

    void RocksRecordStore::Cursor::reattachToOperationContext(OperationContext* opCtx) {
        _opCtx = opCtx;
        // iterator recreated in restore()
    }

    boost::optional<Record> RocksRecordStore::Cursor::curr() {
        if (!_iterator->Valid()) {
            invariantRocksOK(_iterator->status());
            _eof = true;
            return {};
        }
        if (_oplogVisibleTs && _makeRecordId(_iterator->key()).repr() > *_oplogVisibleTs) {
            _eof = true;
            return {};
        }
        _eof = false;
        _lastLoc = _makeRecordId(_iterator->key());

        auto dataSlice = _iterator->value();
        return {{_lastLoc, {dataSlice.data(), static_cast<int>(dataSlice.size())}}};
    }

    Status RocksRecordStore::updateCappedSize(OperationContext* opCtx, long long cappedSize) {
        if (_cappedMaxSize == cappedSize) {
            return Status::OK();
        }
        _cappedMaxSize = cappedSize;
        _cappedMaxSizeSlack = cappedMaxSizeSlackFromSize(cappedSize);
        return Status::OK();
    }

    Status RocksRecordStore::insertRecords(OperationContext* opCtx, std::vector<Record>* records,
                                           const std::vector<Timestamp>& timestamps) {
        int index = 0;
        for (auto& record : *records) {
            StatusWith<RecordId> res =
                insertRecord(opCtx, record.data.data(), record.data.size(), timestamps[index++]);
            if (!res.isOK()) return res.getStatus();

            record.id = res.getValue();
        }
        return Status::OK();
    }
}  // namespace mongo
