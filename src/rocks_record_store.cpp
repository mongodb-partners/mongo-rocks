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

#include <mutex>
#include <memory>
#include <algorithm>

#include <boost/thread/locks.hpp>

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/experimental.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include "mongo/base/checked_cast.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/oplog_hack.h"
#include "mongo/platform/endian.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/background.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

#include "rocks_counter_manager.h"
#include "rocks_engine.h"
#include "rocks_recovery_unit.h"
#include "rocks_util.h"

namespace mongo {

    using std::string;

    namespace {

        class CappedInsertChange : public RecoveryUnit::Change {
        public:
            CappedInsertChange(CappedVisibilityManager* cappedVisibilityManager,
                               CappedCallback* cappedCallback, const RecordId& record)
                : _cappedVisibilityManager(cappedVisibilityManager),
                  _cappedCallback(cappedCallback),
                  _record(record) {}

            virtual void commit() { _cappedVisibilityManager->dealtWithCappedRecord(_record); }

            virtual void rollback() {
                _cappedVisibilityManager->dealtWithCappedRecord(_record);
                if (_cappedCallback) {
                    _cappedCallback->notifyCappedWaitersIfNeeded();
                }
            }

        private:
            CappedVisibilityManager* _cappedVisibilityManager;
            CappedCallback* _cappedCallback;
            RecordId _record;
        };
    }  // namespace

    void CappedVisibilityManager::addUncommittedRecord(OperationContext* txn,
                                                       const RecordId& record) {
        stdx::lock_guard<stdx::mutex> lk(_lock);
        _addUncommittedRecord_inlock(txn, record);
    }

    void CappedVisibilityManager::_addUncommittedRecord_inlock(OperationContext* txn,
                                                               const RecordId& record) {
        dassert(_uncommittedRecords.empty() || _uncommittedRecords.back() < record);
        _uncommittedRecords.push_back(record);
        txn->recoveryUnit()->registerChange(new CappedInsertChange(this, _cappedCallback, record));
        _oplog_highestSeen = record;
    }

    RecordId CappedVisibilityManager::getNextAndAddUncommittedRecord(
        OperationContext* txn, std::function<RecordId()> nextId) {
        stdx::lock_guard<stdx::mutex> lk(_lock);
        RecordId record = nextId();
        _addUncommittedRecord_inlock(txn, record);
        return record;
    }

    void CappedVisibilityManager::dealtWithCappedRecord(const RecordId& record) {
        stdx::lock_guard<stdx::mutex> lk(_lock);
        std::vector<RecordId>::iterator it =
            std::find(_uncommittedRecords.begin(), _uncommittedRecords.end(), record);
        invariant(it != _uncommittedRecords.end());
        _uncommittedRecords.erase(it);
    }

    bool CappedVisibilityManager::isCappedHidden(const RecordId& record) const {
        stdx::lock_guard<stdx::mutex> lk(_lock);
        if (_uncommittedRecords.empty()) {
            return false;
        }
        return _uncommittedRecords.front() <= record;
    }

    void CappedVisibilityManager::updateHighestSeen(const RecordId& record) {
        if (record > _oplog_highestSeen) {
            stdx::lock_guard<stdx::mutex> lk(_lock);
            if (record > _oplog_highestSeen) {
                _oplog_highestSeen = record;
            }
        }
    }

    void CappedVisibilityManager::setHighestSeen(const RecordId& record) {
        stdx::lock_guard<stdx::mutex> lk(_lock);
        _oplog_highestSeen = record;
    }

    RecordId CappedVisibilityManager::oplogStartHack() const {
        stdx::lock_guard<stdx::mutex> lk(_lock);
        if (_uncommittedRecords.empty()) {
            return _oplog_highestSeen;
        } else {
            return _uncommittedRecords.front();
        }
    }

    RecordId CappedVisibilityManager::lowestCappedHiddenRecord() const {
        stdx::lock_guard<stdx::mutex> lk(_lock);
        return _uncommittedRecords.empty() ? RecordId() : _uncommittedRecords.front();
    }

    // this object keeps track of keys in oplog. The format is this:
    // <prefix>RecordId --> dataSize (small endian 32 bytes)
    // <prefix> is oplog_prefix+1 (reserved by rocks_engine.cpp)
    // That way we can cheaply delete old record in the oplog without actually reading oplog
    // collection.
    // All of the locking is done somewhere else -- we write exactly the same data as oplog, so we
    // assume oplog already locked the relevant keys
    class RocksOplogKeyTracker {
    public:
        RocksOplogKeyTracker(std::string prefix) : _prefix(std::move(prefix)) {}
        void insertKey(RocksRecoveryUnit* ru, const RecordId& loc, int len) {
            uint32_t lenLittleEndian = endian::nativeToLittle(static_cast<uint32_t>(len));
            ru->writeBatch()->Put(RocksRecordStore::_makePrefixedKey(_prefix, loc),
                                  rocksdb::Slice(reinterpret_cast<const char*>(&lenLittleEndian),
                                                 sizeof(lenLittleEndian)));
        }
        void deleteKey(RocksRecoveryUnit* ru, const RecordId& loc) {
            ru->writeBatch()->Delete(RocksRecordStore::_makePrefixedKey(_prefix, loc));
        }
        rocksdb::Iterator* newIterator(RocksRecoveryUnit* ru) {
            return ru->NewIterator(_prefix, true);
        }
        int decodeSize(const rocksdb::Slice& value) {
            uint32_t size =
                endian::littleToNative(*reinterpret_cast<const uint32_t*>(value.data()));
            return static_cast<int>(size);
        }

    private:
        std::string _prefix;
    };

    RocksRecordStore::RocksRecordStore(StringData ns, StringData id, rocksdb::DB* db,
                                       RocksCounterManager* counterManager, std::string prefix,
                                       bool isCapped, int64_t cappedMaxSize, int64_t cappedMaxDocs,
                                       CappedCallback* cappedCallback)
        : RecordStore(ns),
          _db(db),
          _counterManager(counterManager),
          _prefix(std::move(prefix)),
          _isCapped(isCapped),
          _cappedMaxSize(cappedMaxSize),
          _cappedMaxSizeSlack(std::min(cappedMaxSize / 10, int64_t(16 * 1024 * 1024))),
          _cappedMaxDocs(cappedMaxDocs),
          _cappedCallback(cappedCallback),
          _cappedDeleteCheckCount(0),
          _isOplog(NamespaceString::oplog(ns)),
          _oplogKeyTracker(_isOplog
                               ? new RocksOplogKeyTracker(std::move(rocksGetNextPrefix(_prefix)))
                               : nullptr),
          _cappedOldestKeyHint(0),
          _cappedVisibilityManager(
              (_isCapped || _isOplog) ? new CappedVisibilityManager(_cappedCallback) : nullptr),
          _ident(id.toString()),
          _dataSizeKey(std::string("\0\0\0\0", 4) + "datasize-" + id.toString()),
          _numRecordsKey(std::string("\0\0\0\0", 4) + "numrecords-" + id.toString()),
          _shuttingDown(false) {
        _oplogSinceLastCompaction.reset();

        LOG(1) << "opening collection " << ns << " with prefix "
               << rocksdb::Slice(_prefix).ToString(true);

        if (_isCapped) {
            invariant(_cappedMaxSize > 0);
            invariant(_cappedMaxDocs == -1 || _cappedMaxDocs > 0);
        }
        else {
            invariant(_cappedMaxSize == -1);
            invariant(_cappedMaxDocs == -1);
        }

        // Get next id
        std::unique_ptr<RocksIterator> iter(
            RocksRecoveryUnit::NewIteratorNoSnapshot(_db, _prefix));
        // first check if the collection is empty
        iter->SeekPrefix("");
        bool emptyCollection = !iter->Valid();
        if (!emptyCollection) {
            // if it's not empty, find next RecordId
            iter->SeekToLast();
            dassert(iter->Valid());
            rocksdb::Slice lastSlice = iter->key();
            RecordId lastId = _makeRecordId(lastSlice);
            if (_isOplog || _isCapped) {
                _cappedVisibilityManager->updateHighestSeen(lastId);
            }
            _nextIdNum.store(lastId.repr() + 1);
        } else {
            // Need to start at 1 so we are always higher than RecordId::min()
            _nextIdNum.store(1);
        }

        // load metadata
        _numRecords.store(_counterManager->loadCounter(_numRecordsKey));
        _dataSize.store(_counterManager->loadCounter(_dataSizeKey));
        invariant(_dataSize.load() >= 0);
        invariant(_numRecords.load() >= 0);

        _hasBackgroundThread = RocksEngine::initRsOplogBackgroundThread(ns);
    }

    RocksRecordStore::~RocksRecordStore() {
        {
            stdx::lock_guard<boost::timed_mutex> lk(_cappedDeleterMutex);
            _shuttingDown = true;
        }
        delete _oplogKeyTracker;
    }

    int64_t RocksRecordStore::storageSize(OperationContext* txn, BSONObjBuilder* extraInfo,
                                          int infoLevel) const {
        // We need to make it multiple of 256 to make
        // jstests/concurrency/fsm_workloads/convert_to_capped_collection.js happy
        return static_cast<int64_t>(
            std::max(_dataSize.load() & (~255), static_cast<long long>(256)));
    }

    RecordData RocksRecordStore::dataFor(OperationContext* txn, const RecordId& loc) const {
        RecordData rd = _getDataFor(_db, _prefix, txn, loc);
        massert(28605, "Didn't find RecordId in RocksRecordStore", (rd.data() != nullptr));
        return rd;
    }

    void RocksRecordStore::deleteRecord( OperationContext* txn, const RecordId& dl ) {
        std::string key(_makePrefixedKey(_prefix, dl));

        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(txn);
        if (!ru->transaction()->registerWrite(key)) {
            throw WriteConflictException();
        }

        std::string oldValue;
        auto status = ru->Get(key, &oldValue);
        invariantRocksOK(status);
        int oldLength = oldValue.size();

        ru->writeBatch()->Delete(key);
        if (_isOplog) {
            _oplogKeyTracker->deleteKey(ru, dl);
        }

        _changeNumRecords(txn, -1);
        _increaseDataSize(txn, -oldLength);
    }

    long long RocksRecordStore::dataSize(OperationContext* txn) const {
        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(txn);
        return _dataSize.load(std::memory_order::memory_order_relaxed) +
               ru->getDeltaCounter(_dataSizeKey);
    }

    long long RocksRecordStore::numRecords(OperationContext* txn) const {
        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit( txn );
        return _numRecords.load(std::memory_order::memory_order_relaxed) +
            ru->getDeltaCounter(_numRecordsKey);
    }

    bool RocksRecordStore::cappedAndNeedDelete(long long dataSizeDelta,
                                               long long numRecordsDelta) const {
        invariant(_isCapped);

        if (_dataSize.load() + dataSizeDelta > _cappedMaxSize)
            return true;

        if ((_cappedMaxDocs != -1) && (_numRecords.load() + numRecordsDelta > _cappedMaxDocs))
            return true;

        return false;
    }

    int64_t RocksRecordStore::cappedDeleteAsNeeded(OperationContext* txn,
                                                   const RecordId& justInserted) {
        if (!_isCapped) {
          return 0;
        }

        // We only want to do the checks occasionally as they are expensive.
        // This variable isn't thread safe, but has loose semantics anyway.
        dassert(!_isOplog || _cappedMaxDocs == -1);

        long long dataSizeDelta = 0, numRecordsDelta = 0;
        if (!_isOplog) {
            auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(txn);
            dataSizeDelta = ru->getDeltaCounter(_dataSizeKey);
            numRecordsDelta = ru->getDeltaCounter(_numRecordsKey);
        }

        if (!cappedAndNeedDelete(dataSizeDelta, numRecordsDelta)) {
            return 0;
        }

        // ensure only one thread at a time can do deletes, otherwise they'll conflict.
       boost::unique_lock<boost::timed_mutex> lock(_cappedDeleterMutex, boost::defer_lock);

        if (_cappedMaxDocs != -1) {
            lock.lock(); // Max docs has to be exact, so have to check every time.
        }
        else if(_hasBackgroundThread) {
            // We are foreground, and there is a background thread,

            // Check if we need some back pressure.
            if ((_dataSize.load() - _cappedMaxSize) < _cappedMaxSizeSlack) {
                return 0;
            }

            // Back pressure needed!
            // We're not actually going to delete anything, but we're going to syncronize
            // on the deleter thread.

            if (!lock.try_lock()) {
                (void)lock.try_lock_for(boost::chrono::milliseconds(200));
            }
            return 0;
        } else {
            if (!lock.try_lock()) {
                // Someone else is deleting old records. Apply back-pressure if too far behind,
                // otherwise continue.
                if ((_dataSize.load() - _cappedMaxSize) < _cappedMaxSizeSlack)
                    return 0;

                if (!lock.try_lock_for(boost::chrono::milliseconds(200)))
                    return 0;

                // If we already waited, let someone else do cleanup unless we are significantly
                // over the limit.
                if ((_dataSize.load() - _cappedMaxSize) < (2 * _cappedMaxSizeSlack))
                    return 0;
            }
        }

        return cappedDeleteAsNeeded_inlock(txn, justInserted);
    }

    int64_t RocksRecordStore::cappedDeleteAsNeeded_inlock(OperationContext* txn,
                                                          const RecordId& justInserted) {
        // we do this is a sub transaction in case it aborts
        RocksRecoveryUnit* realRecoveryUnit =
            checked_cast<RocksRecoveryUnit*>(txn->releaseRecoveryUnit());
        invariant(realRecoveryUnit);
        OperationContext::RecoveryUnitState const realRUstate =
            txn->setRecoveryUnit(realRecoveryUnit->newRocksRecoveryUnit(),
                                 OperationContext::kNotInUnitOfWork);

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
            WriteUnitOfWork wuow(txn);
            auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(txn);
            std::unique_ptr<rocksdb::Iterator> iter;
            if (_isOplog) {
                // we're using _oplogKeyTracker to find which keys to delete -- this is much faster
                // because we don't need to read any values. We theoretically need values to pass
                // the document to the cappedCallback, but the callback is only using
                // documents to remove them from indexes. opLog doesn't have indexes, so there
                // should be no need for us to reconstruct the document to pass it to the callback
                iter.reset(_oplogKeyTracker->newIterator(ru));
            } else {
                iter.reset(ru->NewIterator(_prefix));
            }
            int64_t storage;
            iter->Seek(RocksRecordStore::_makeKey(_cappedOldestKeyHint, &storage));

            RecordId newestOld;
            while ((sizeSaved < sizeOverCap || docsRemoved < docsOverCap) &&
                   (docsRemoved < 20000) && iter->Valid()) {

                newestOld = _makeRecordId(iter->key());

                if (_cappedVisibilityManager->isCappedHidden(newestOld)) {
                    // this means we have an older record that hasn't been committed yet. let's
                    // wait until it gets committed before deleting
                    break;
                }

                // don't go past the record we just inserted
                if (newestOld >= justInserted) {
                    break;
                }

                if (_shuttingDown) {
                    break;
                }

                std::string key(_makePrefixedKey(_prefix, newestOld));
                if (!ru->transaction()->registerWrite(key)) {
                    log() << "got conflict truncating capped, total docs removed " << docsRemoved;
                    break;
                }

                rocksdb::Slice oldValue;
                ++docsRemoved;
                if (_isOplog) {
                    // trick the callback by giving it empty bson document
                    oldValue = rocksdb::Slice(emptyBson.objdata(), emptyBson.objsize());
                    // we keep data size in the value
                    sizeSaved += _oplogKeyTracker->decodeSize(iter->value());
                } else {
                    oldValue = iter->value();
                    sizeSaved += oldValue.size();
                }

                if (_cappedCallback) {
                    uassertStatusOK(_cappedCallback->aboutToDeleteCapped(
                        txn, newestOld,
                        RecordData(static_cast<const char*>(oldValue.data()), oldValue.size())));
                }

                ru->writeBatch()->Delete(key);
                if (_isOplog) {
                    _oplogKeyTracker->deleteKey(ru, newestOld);
                }

                iter->Next();
            }

            if (!iter->status().ok()) {
                log() << "RocksDB iterator failure when trying to delete capped, ignoring: "
                      << iter->status().ToString();
            }

            if (docsRemoved > 0) {
                _changeNumRecords(txn, -docsRemoved);
                _increaseDataSize(txn, -sizeSaved);
                wuow.commit();
            }

            if (iter->Valid()) {
                auto oldestAliveRecordId = _makeRecordId(iter->key());
                // we check if there's outstanding transaction that is older than
                // oldestAliveRecordId. If there is, we should not skip deleting that record next
                // time we clean up the capped collection. If there isn't, we know for certain this
                // is the record we'll start out deletions from next time
                if (!_cappedVisibilityManager->isCappedHidden(oldestAliveRecordId)) {
                    _cappedOldestKeyHint = oldestAliveRecordId;
                }
            }
        }
        catch ( const WriteConflictException& wce ) {
            delete txn->releaseRecoveryUnit();
            txn->setRecoveryUnit(realRecoveryUnit, realRUstate);
            log() << "got conflict truncating capped, ignoring";
            return 0;
        }
        catch ( ... ) {
            delete txn->releaseRecoveryUnit();
            txn->setRecoveryUnit(realRecoveryUnit, realRUstate);
            throw;
        }

        delete txn->releaseRecoveryUnit();
        txn->setRecoveryUnit(realRecoveryUnit, realRUstate);

        if (_isOplog) {
            if (_oplogSinceLastCompaction.minutes() >= kOplogCompactEveryMins) {
                log() << "Scheduling oplog compactions";
                _oplogSinceLastCompaction.reset();
                // schedule compaction for oplog
                std::string oldestAliveKey(_makePrefixedKey(_prefix, _cappedOldestKeyHint));
                rocksdb::Slice begin(_prefix), end(oldestAliveKey);
                rocksdb::experimental::SuggestCompactRange(_db, &begin, &end);

                // schedule compaction for oplog tracker
                std::string oplogKeyTrackerPrefix(rocksGetNextPrefix(_prefix));
                oldestAliveKey = _makePrefixedKey(oplogKeyTrackerPrefix, _cappedOldestKeyHint);
                begin = rocksdb::Slice(oplogKeyTrackerPrefix);
                end = rocksdb::Slice(oldestAliveKey);
                rocksdb::experimental::SuggestCompactRange(_db, &begin, &end);
            }
        }

        return docsRemoved;
    }

    StatusWith<RecordId> RocksRecordStore::insertRecord( OperationContext* txn,
                                                        const char* data,
                                                        int len,
                                                        bool enforceQuota ) {

        if ( _isCapped && len > _cappedMaxSize ) {
            return StatusWith<RecordId>( ErrorCodes::BadValue,
                                       "object to insert exceeds cappedMaxSize" );
        }

        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit( txn );

        RecordId loc;
        if (_isOplog) {
            StatusWith<RecordId> status = oploghack::extractKey(data, len);
            if (!status.isOK()) {
                return status;
            }
            loc = status.getValue();
            _cappedVisibilityManager->updateHighestSeen(loc);
        } else if (_isCapped) {
            loc = _cappedVisibilityManager->getNextAndAddUncommittedRecord(
                txn, [&]() { return _nextId(); });
        } else {
            loc = _nextId();
        }

        // No need to register the write here, since we just allocated a new RecordId so no other
        // transaction can access this key before we commit
        ru->writeBatch()->Put(_makePrefixedKey(_prefix, loc), rocksdb::Slice(data, len));
        if (_isOplog) {
            _oplogKeyTracker->insertKey(ru, loc, len);
        }

        _changeNumRecords( txn, 1 );
        _increaseDataSize( txn, len );

        cappedDeleteAsNeeded(txn, loc);

        return StatusWith<RecordId>( loc );
    }

    StatusWith<RecordId> RocksRecordStore::insertRecord( OperationContext* txn,
                                                        const DocWriter* doc,
                                                        bool enforceQuota ) {
        const int len = doc->documentSize();

        std::unique_ptr<char[]> buf(new char[len]);
        doc->writeDocument( buf.get() );

        return insertRecord( txn, buf.get(), len, enforceQuota );
    }

    StatusWith<RecordId> RocksRecordStore::updateRecord( OperationContext* txn,
                                                        const RecordId& loc,
                                                        const char* data,
                                                        int len,
                                                        bool enforceQuota,
                                                        UpdateNotifier* notifier ) {
        std::string key(_makePrefixedKey(_prefix, loc));

        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit( txn );
        if (!ru->transaction()->registerWrite(key)) {
            throw WriteConflictException();
        }

        std::string old_value;
        auto status = ru->Get(key, &old_value);
        invariantRocksOK(status);

        int old_length = old_value.size();

        ru->writeBatch()->Put(key, rocksdb::Slice(data, len));
        if (_isOplog) {
            _oplogKeyTracker->insertKey(ru, loc, len);
        }

        _increaseDataSize(txn, len - old_length);

        cappedDeleteAsNeeded(txn, loc);

        return StatusWith<RecordId>( loc );
    }

    bool RocksRecordStore::updateWithDamagesSupported() const {
        return false;
    }

    StatusWith<RecordData> RocksRecordStore::updateWithDamages(
        OperationContext* txn,
        const RecordId& loc,
        const RecordData& oldRec,
        const char* damageSource,
        const mutablebson::DamageVector& damages) {
        MONGO_UNREACHABLE;
    }

    std::unique_ptr<SeekableRecordCursor> RocksRecordStore::getCursor(OperationContext* txn,
                                                                      bool forward) const {
        RecordId startIterator;

        if (_isOplog) {
            if (forward) {
                auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(txn);
                if (!ru->hasSnapshot() || ru->getOplogReadTill().isNull()) {
                    // we don't have snapshot, we can update our view
                    ru->setOplogReadTill(_cappedVisibilityManager->oplogStartHack());
                }

                startIterator = _cappedOldestKeyHint;
            } else {  // backward iterator + beginning not set
                startIterator = _cappedVisibilityManager->oplogStartHack();
            }
        } else if (_isCapped && forward) {
            // seek to first in capped collection. we know that there are no records smaller than
            // _cappedOldestKeyHint that are alive
            startIterator = _cappedOldestKeyHint;
        }

        // TODO use startIterator.
        return stdx::make_unique<Cursor>(txn, _db, _prefix, _cappedVisibilityManager, forward,
                                         _isCapped);
    }

    Status RocksRecordStore::truncate(OperationContext* txn) {
        // We can't use getCursor() here because we need to ignore the visibility of records (i.e.
        // we need to delete all records, regardless of visibility)
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(txn);
        std::unique_ptr<RocksIterator> iterator(ru->NewIterator(_prefix, _isOplog));
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
            deleteRecord(txn, _makeRecordId(iterator->key()));
        }

        return rocksToMongoStatus(iterator->status());
    }

    Status RocksRecordStore::compact( OperationContext* txn,
                                      RecordStoreCompactAdaptor* adaptor,
                                      const CompactOptions* options,
                                      CompactStats* stats ) {
        std::string beginString(_makePrefixedKey(_prefix, RecordId()));
        std::string endString(_makePrefixedKey(_prefix, RecordId::max()));
        rocksdb::Slice beginRange(beginString);
        rocksdb::Slice endRange(endString);
        return rocksToMongoStatus(_db->CompactRange(&beginRange, &endRange));
    }

    Status RocksRecordStore::validate( OperationContext* txn,
                                       bool full,
                                       bool scanData,
                                       ValidateAdaptor* adaptor,
                                       ValidateResults* results,
                                       BSONObjBuilder* output ) {
        long long nrecords = 0;
        long long dataSizeTotal = 0;
        if (scanData) {
            auto cursor = getCursor(txn, true);
            results->valid = true;
            while (auto record = cursor->next()) {
                ++nrecords;
                if (full) {
                    size_t dataSize;
                    Status status = adaptor->validate(record->data, &dataSize);
                    if (!status.isOK()) {
                        results->valid = false;
                        results->errors.push_back(str::stream() << record->id << " is corrupted");
                    }
                    dataSizeTotal += static_cast<long long>(dataSize);
                }
            }

            if (full && results->valid) {
                long long storedNumRecords = numRecords(txn);
                long long storedDataSize = dataSize(txn);

                if (nrecords != storedNumRecords || dataSizeTotal != storedDataSize) {
                    warning() << _ident << ": Existing record and data size counters ("
                              << storedNumRecords << " records " << storedDataSize << " bytes) "
                              << "are inconsistent with full validation results (" << nrecords
                              << " records " << dataSizeTotal << " bytes). "
                              << "Updating counters with new values.";
                    if (nrecords != storedNumRecords) {
                        _changeNumRecords(txn, nrecords - storedNumRecords);
                        _increaseDataSize(txn, dataSizeTotal - storedDataSize);
                    }
                }
            }
            output->appendNumber("nrecords", nrecords);
        } else {
            output->appendNumber("nrecords", numRecords(txn));
        }

        return Status::OK();
    }

    void RocksRecordStore::appendCustomStats( OperationContext* txn,
                                              BSONObjBuilder* result,
                                              double scale ) const {
        string statsString;
        result->appendBool("capped", _isCapped);
        if (_isCapped) {
            result->appendIntOrLL("max", _cappedMaxDocs);
            result->appendIntOrLL("maxSize", _cappedMaxSize / scale);
        }
    }

    Status RocksRecordStore::oplogDiskLocRegister(OperationContext* txn, const Timestamp& opTime) {
        invariant(_isOplog);
        StatusWith<RecordId> record = oploghack::keyForOptime(opTime);
        if (record.isOK()) {
            _cappedVisibilityManager->addUncommittedRecord(txn, record.getValue());
        }

        return record.getStatus();
    }

    void RocksRecordStore::updateStatsAfterRepair(OperationContext* txn, long long numRecords,
                                                  long long dataSize) {
        _numRecords.store(numRecords);
        _dataSize.store(dataSize);
        rocksdb::WriteBatch wb;
        _counterManager->updateCounter(_numRecordsKey, numRecords, &wb);
        _counterManager->updateCounter(_dataSizeKey, dataSize, &wb);
        if (wb.Count() > 0) {
            auto s = _db->Write(rocksdb::WriteOptions(), &wb);
            invariantRocksOK(s);
        }
    }

    /**
     * Return the RecordId of an oplog entry as close to startingPosition as possible without
     * being higher. If there are no entries <= startingPosition, return RecordId().
     */
    boost::optional<RecordId> RocksRecordStore::oplogStartHack(
        OperationContext* txn, const RecordId& startingPosition) const {

        if (!_isOplog) {
            return boost::none;
        }

        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(txn);
        ru->setOplogReadTill(_cappedVisibilityManager->oplogStartHack());

        // we use _oplogKeyTracker, which contains exactly the same keys as oplog. the difference is
        // that values are different (much smaller), so reading is faster. in this case, we only
        // need keys (we never touch the values), so this works nicely
        std::unique_ptr<rocksdb::Iterator> iter(_oplogKeyTracker->newIterator(ru));
        int64_t storage;
        iter->Seek(_makeKey(startingPosition, &storage));
        if (!iter->Valid()) {
            iter->SeekToLast();
            if (iter->Valid()) {
                // startingPosition is bigger than everything else
                return _makeRecordId(iter->key());
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
        int cmp = startingPosition.compare(foundKey);
        if (cmp != 0) {
            // RocksDB invariant -- iterator needs to land at or past target when Seek-ing
            invariant(cmp < 0);
            // we're past target -- prev()
            iter->Prev();
        }

        if (!iter->Valid()) {
            invariantRocksOK(iter->status());
            // there are no entries <= startingPosition
            return RecordId();
        }

        return _makeRecordId(iter->key());
    }

    void RocksRecordStore::temp_cappedTruncateAfter( OperationContext* txn,
                                                     RecordId end,
                                                     bool inclusive ) {
        // copied from WiredTigerRecordStore::temp_cappedTruncateAfter()
        WriteUnitOfWork wuow(txn);
        RecordId lastKeptId = end;
        int64_t recordsRemoved = 0;

        if (inclusive) {
            auto reverseCursor = getCursor(txn, false);
            invariant(reverseCursor->seekExact(end));
            auto prev = reverseCursor->next();
            lastKeptId = prev ? prev->id : RecordId::min();
        }

        auto cursor = getCursor(txn, true);
        for (auto record = cursor->seekExact(end); record; record = cursor->next()) {
            if (end < record->id || (inclusive && end == record->id)) {
                if (_cappedCallback) {
                    uassertStatusOK(
                        _cappedCallback->aboutToDeleteCapped(txn, record->id, record->data));
                }
                deleteRecord(txn, record->id);
                ++recordsRemoved;
            }
        }

        if (recordsRemoved) {
            // Forget that we've ever seen a higher timestamp than we now have.
            _cappedVisibilityManager->setHighestSeen(lastKeptId);
        }

        wuow.commit();
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

    RecordId RocksRecordStore::_makeRecordId(const rocksdb::Slice& slice) {
        invariant(slice.size() == sizeof(int64_t));
        int64_t repr = endian::bigToNative(*reinterpret_cast<const int64_t*>(slice.data()));
        RecordId a(repr);
        return RecordId(repr);
    }

    bool RocksRecordStore::findRecord( OperationContext* txn,
                                       const RecordId& loc, RecordData* out ) const {
        RecordData rd = _getDataFor(_db, _prefix, txn, loc);
        if ( rd.data() == NULL )
            return false;
        *out = rd;
        return true;
    }

    RecordData RocksRecordStore::_getDataFor(rocksdb::DB* db, const std::string& prefix,
                                             OperationContext* txn, const RecordId& loc) {
        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(txn);

        std::string valueStorage;
        auto status = ru->Get(_makePrefixedKey(prefix, loc), &valueStorage);
        if (status.IsNotFound()) {
            return RecordData(nullptr, 0);
        }
        invariantRocksOK(status);

        SharedBuffer data = SharedBuffer::allocate(valueStorage.size());
        memcpy(data.get(), valueStorage.data(), valueStorage.size());
        return RecordData(data, valueStorage.size());
    }

    void RocksRecordStore::_changeNumRecords(OperationContext* txn, int64_t amount) {
        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit(txn);
        ru->incrementCounter(_numRecordsKey, &_numRecords, amount);
    }

    void RocksRecordStore::_increaseDataSize(OperationContext* txn, int64_t amount) {
        RocksRecoveryUnit* ru = RocksRecoveryUnit::getRocksRecoveryUnit( txn );
        ru->incrementCounter(_dataSizeKey, &_dataSize, amount);
    }

    // --------

    RocksRecordStore::Cursor::Cursor(
            OperationContext* txn,
            rocksdb::DB* db,
            std::string prefix,
            std::shared_ptr<CappedVisibilityManager> cappedVisibilityManager,
            bool forward,
            bool isCapped)
        : _txn(txn),
          _db(db),
          _prefix(std::move(prefix)),
          _cappedVisibilityManager(cappedVisibilityManager),
          _forward(forward),
          _isCapped(isCapped),
          _readUntilForOplog(RocksRecoveryUnit::getRocksRecoveryUnit(txn)->getOplogReadTill()) {
        _currentSequenceNumber =
          RocksRecoveryUnit::getRocksRecoveryUnit(txn)->snapshot()->GetSequenceNumber();
    }

    // requires !_eof
    void RocksRecordStore::Cursor::positionIterator() {
        int64_t locStorage;
        _iterator->Seek(RocksRecordStore::_makeKey(_lastLoc, &locStorage));
        invariantRocksOK(_iterator->status());

        if (_forward) {
            _lastMoveWasRestore =
                !_iterator->Valid() || _lastLoc != _makeRecordId(_iterator->key());
        }
        else {
            // Seek() lands on or after the key, while reverse cursors need to land on or before.
            if (!_iterator->Valid()) {
                // Nothing left on or after.
                _iterator->SeekToLast();
                invariantRocksOK(_iterator->status());
                _lastMoveWasRestore = true;
            }
            else {
                _lastMoveWasRestore = _lastLoc != _makeRecordId(_iterator->key());
                if (_lastMoveWasRestore) {
                    // Landed after.
                    // Since iterator is valid and Seek() landed after key,
                    // iterator will still be valid after we call Prev().
                    _iterator->Prev();
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
        _iterator.reset(RocksRecoveryUnit::getRocksRecoveryUnit(_txn)
                ->NewIterator(_prefix, /* isOplog */ !_readUntilForOplog.isNull()));
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

        if (_lastMoveWasRestore) {
            _lastMoveWasRestore = false;
            return curr();
        }

        bool mustAdvance = true;
        if (_needFirstSeek && !_forward && _cappedVisibilityManager != nullptr ) {
            const RecordId reverseCappedInitialSeekPoint =
                _readUntilForOplog.isNull() ? _cappedVisibilityManager->lowestCappedHiddenRecord()
                                            : _readUntilForOplog;

            if (!reverseCappedInitialSeekPoint.isNull()) {
                int64_t keyStorage;
                auto encodedKey = _makeKey(reverseCappedInitialSeekPoint, &keyStorage);
                iter->Seek(encodedKey);
                if (iter->Valid()) {
                    // we don't want to SeekToLast(), we just need to do a Prev()
                    _needFirstSeek = false;
                    // we must do a Prev() if a iterator key is hidden. this happens in two cases:
                    // * we seeked past our seek target. in that case we're sure that the target is
                    // hidden
                    // * we seeked exactly at our seek target, but the target is hidden
                    mustAdvance =
                        (iter->key() != encodedKey) ||
                        _cappedVisibilityManager->isCappedHidden(reverseCappedInitialSeekPoint);
                } else {
                    invariantRocksOK(iter->status());
                }
            }
        }

        if (_needFirstSeek) {
            _needFirstSeek = false;
            if (_forward) {
                iter->SeekToFirst();
            } else {
                iter->SeekToLast();
            }
        } else if (mustAdvance) {
            if (_forward) {
                iter->Next();
            } else {
                iter->Prev();
            }
        }

        return curr();
    }

    boost::optional<Record> RocksRecordStore::Cursor::seekExact(const RecordId& id) {
        _needFirstSeek = false;
        _lastMoveWasRestore = false;
        _iterator.reset();

        rocksdb::Status status = RocksRecoveryUnit::getRocksRecoveryUnit(_txn)
            ->Get(_makePrefixedKey(_prefix, id), &_seekExactResult);

        if (status.IsNotFound()) {
            _eof = true;
            return {};
        } else if (!status.ok()) {
            invariantRocksOK(status);
            return {};
        }

        _eof = false;
        _lastLoc = id;

        return {{_lastLoc, {_seekExactResult.data(), static_cast<int>(_seekExactResult.size())}}};
    }

    void RocksRecordStore::Cursor::save() {}

    void RocksRecordStore::Cursor::saveUnpositioned() { _eof = true; }

    bool RocksRecordStore::Cursor::restore() {
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(_txn);
        if (!_iterator.get() || _currentSequenceNumber != ru->snapshot()->GetSequenceNumber()) {
            _iterator.reset(ru->NewIterator(_prefix, /* isOplog */ !_readUntilForOplog.isNull()));
            _currentSequenceNumber = ru->snapshot()->GetSequenceNumber();
        }

        if (_eof) return true;
        if (_needFirstSeek) return true;

        positionIterator();
        return _cappedVisibilityManager ? !_lastMoveWasRestore : true;
    }

    void RocksRecordStore::Cursor::detachFromOperationContext() {
        _txn = nullptr;
        _iterator.reset();
    }

    void RocksRecordStore::Cursor::reattachToOperationContext(OperationContext* txn) {
        _txn = txn;
        // iterator recreated in restore()
    }

    boost::optional<Record> RocksRecordStore::Cursor::curr() {
        invariantRocksOK(_iterator->status());
        if (!_iterator->Valid()) {
            _eof = true;
            return {};
        }
        _eof = false;
        _lastLoc = _makeRecordId(_iterator->key());

        if (_cappedVisibilityManager) {  // isCapped?
            if (_readUntilForOplog.isNull()) {
                // this is the normal capped case
                if (_cappedVisibilityManager->isCappedHidden(_lastLoc)) {
                    _eof = true;
                    return {};
                }
            } else {
                // this is for oplogs
                if (_lastLoc > _readUntilForOplog ||
                    (_lastLoc == _readUntilForOplog &&
                     _cappedVisibilityManager->isCappedHidden(_lastLoc))) {
                    _eof = true;
                    return {};
                }
            }
        }  // isCapped?

        auto dataSlice = _iterator->value();
        return {{_lastLoc, {dataSlice.data(), static_cast<int>(dataSlice.size())}}};
    }
}
