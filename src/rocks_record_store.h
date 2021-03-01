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

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <rocksdb/options.h>

#include "mongo/db/storage/capped_callback.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/platform/mutex.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/timer.h"
#include "mongo_rate_limiter_checker.h"

template <typename F>
auto ROCKS_CHECK_HELP(F&& f) {
#ifdef __linux__
    if (mongo::getMongoRateLimiter() != nullptr) {
        mongo::getMongoRateLimiter()->request(1);
    }
#endif
    return f();
}
#define ROCKS_OP_CHECK(f) ROCKS_CHECK_HELP([&] { return f; })
#define ROCKS_READ_CHECK(f) ROCKS_CHECK_HELP(f)

/**
 * Either executes the specified operation and returns it's value or randomly throws a write
 * conflict exception if the RocksWriteConflictException failpoint is enabled. This is only checked
 * on cursor methods that make modifications.
 */
#define ROCKS_OP_CHECK(x)                                \
    (((MONGO_FAIL_POINT(RocksWriteConflictException)))   \
         ? (rocksdb::Status::Busy("failpoint simulate")) \
         : (x))

/**
 * Identical to ROCKS_OP_CHECK except this is checked on cursor seeks/advancement.
 */
#define ROCKS_READ_CHECK(x)                                    \
    (((MONGO_FAIL_POINT(RocksWriteConflictExceptionForReads))) \
         ? (rocksdb::Status::Busy("failpoint simulate"))       \
         : (x))

namespace rocksdb {
    class TOTransactionDB;
    class Iterator;
    class Slice;
    class ColumnFamilyHandle;
}  // namespace rocksdb

namespace mongo {

    class RocksCounterManager;
    class RocksDurabilityManager;
    class RocksCompactionScheduler;
    class RocksRecoveryUnit;
    class RocksOplogKeyTracker;
    class RocksRecordStore;
    class RocksOplogManager;
    class RocksEngine;

    typedef std::list<RecordId> SortedRecordIds;

    class RocksRecordStore : public RecordStore {
    public:
        struct Params {
            StringData ns;
            std::string ident;
            std::string prefix;
            bool isCapped;
            int64_t cappedMaxSize;
            int64_t cappedMaxDocs;
            CappedCallback* cappedCallback;
            bool tracksSizeAdjustments;
            Params()
                : isCapped(false),
                  cappedMaxSize(-1),
                  cappedMaxDocs(-1),
                  cappedCallback(nullptr),
                  tracksSizeAdjustments(true) {}
        };
        RocksRecordStore(RocksEngine* engine, rocksdb::ColumnFamilyHandle* cf,
		         OperationContext* opCtx, Params params);

        virtual ~RocksRecordStore();

        // name of the RecordStore implementation
        virtual const char* name() const { return "rocks"; }

        virtual long long dataSize(OperationContext* opCtx) const;

        virtual long long numRecords(OperationContext* opCtx) const;

        virtual bool isCapped() const { return _isCapped; }

        virtual int64_t storageSize(OperationContext* opCtx, BSONObjBuilder* extraInfo = NULL,
                                    int infoLevel = 0) const;

        virtual bool isInRecordIdOrder() const override { return true; }

        const std::string& getIdent() const override { return _ident; }

        // CRUD related

        virtual RecordData dataFor(OperationContext* opCtx, const RecordId& loc) const;

        virtual bool findRecord(OperationContext* opCtx, const RecordId& loc,
                                RecordData* out) const;

        virtual void deleteRecord(OperationContext* opCtx, const RecordId& dl);

        virtual StatusWith<RecordId> insertRecord(OperationContext* opCtx, const char* data,
                                                  int len, Timestamp timestamp);

        virtual Status insertRecords(OperationContext* opCtx, std::vector<Record>* records,
                                     const std::vector<Timestamp>& timestamps);

        virtual Status insertRecordsWithDocWriter(OperationContext* opCtx,
                                                  const DocWriter* const* docs,
                                                  const Timestamp* timestamps, size_t nDocs,
                                                  RecordId* idsOut);

        virtual Status updateRecord(OperationContext* opCtx, const RecordId& oldLocation,
                                    const char* data, int len);

        virtual bool updateWithDamagesSupported() const;

        virtual StatusWith<RecordData> updateWithDamages(OperationContext* opCtx,
                                                         const RecordId& loc,
                                                         const RecordData& oldRec,
                                                         const char* damageSource,
                                                         const mutablebson::DamageVector& damages);

        std::unique_ptr<SeekableRecordCursor> getCursor(OperationContext* opCtx,
                                                        bool forward) const final;

        virtual Status truncate(OperationContext* opCtx);

        virtual bool compactSupported() const { return true; }
        virtual bool compactsInPlace() const { return true; }

        virtual Status compact(OperationContext* opCtx) final;

        virtual void validate(OperationContext* opCtx, ValidateCmdLevel level,
                              ValidateResults* results, BSONObjBuilder* output);

        virtual void appendCustomStats(OperationContext* opCtx, BSONObjBuilder* result,
                                       double scale) const;

        virtual void cappedTruncateAfter(OperationContext* opCtx, RecordId end, bool inclusive);

        virtual boost::optional<RecordId> oplogStartHack(OperationContext* opCtx,
                                                         const RecordId& startingPosition) const;

        virtual Status oplogDiskLocRegister(OperationContext* opCtx, const Timestamp& opTime,
                                            bool orderedCommit);

        void waitForAllEarlierOplogWritesToBeVisible(OperationContext* opCtx) const override;

        virtual void updateStatsAfterRepair(OperationContext* opCtx, long long numRecords,
                                            long long dataSize);

        virtual Status updateCappedSize(OperationContext* opCtx,
                                        long long cappedSize) override final;

        void setCappedCallback(CappedCallback* cb) {
            stdx::lock_guard<Latch> lk(_cappedCallbackMutex);
            _cappedCallback = cb;
        }
        int64_t cappedMaxDocs() const {
            invariant(_isCapped);
            return _cappedMaxDocs;
        }
        int64_t cappedMaxSize() const {
            invariant(_isCapped);
            return _cappedMaxSize;
        }
        bool isOplog() const { return _isOplog; }

        bool haveCappedWaiters();

        void notifyCappedWaitersIfNeeded();

        stdx::timed_mutex& cappedDeleterMutex() { return _cappedDeleterMutex; }

        void setCounterManager_ForTest(RocksCounterManager* m) { _counterManager = m; }

        static rocksdb::Comparator* newRocksCollectionComparator();

        class CappedInsertChange;

    private:
        // we just need to expose _makePrefixedKey to RocksOplogKeyTracker
        friend class RocksOplogKeyTracker;
        // NOTE: Cursor might outlive the RecordStore. That's why we use all those
        // shared_ptrs
        class Cursor : public SeekableRecordCursor {
        public:
            Cursor(OperationContext* opCtx, rocksdb::TOTransactionDB* db, rocksdb::ColumnFamilyHandle* cf,
                   std::string prefix, bool forward, bool isCapped, bool isOplog, RecordId startIterator);

            boost::optional<Record> next() final;
            boost::optional<Record> seekExact(const RecordId& id) final;
            void save() final;
            void saveUnpositioned() final;
            bool restore() final;
            void detachFromOperationContext() final;
            void reattachToOperationContext(OperationContext* opCtx) final;

        private:
            /**
             * Returns the current position of _iterator and updates _eof and _lastLoc.
             * Correctly handles !_iterator->Valid().
             * Hides records that shouldn't be seen due to _cappedVisibilityManager.
             */
            boost::optional<Record> curr();

            OperationContext* _opCtx;
            rocksdb::TOTransactionDB* _db;     // not owned
            rocksdb::ColumnFamilyHandle* _cf;  // not owned
            std::string _prefix;
            bool _forward;
            bool _isCapped;
            bool _isOplog;
            bool _eof = false;
            bool _needFirstSeek = true;
            bool _skipNextAdvance = false;
            RecordId _lastLoc;
            std::unique_ptr<rocksdb::Iterator> _iterator;
            std::string _seekExactResult;
            void positionIterator();
            rocksdb::Iterator* iterator();
            boost::optional<std::int64_t> _oplogVisibleTs = boost::none;
        };

        static RecordId _makeRecordId(const rocksdb::Slice& slice);

        static RecordData _getDataFor(rocksdb::ColumnFamilyHandle* cf, const std::string& prefix,
                                      OperationContext* opCtx, const RecordId& loc);

        RecordId _nextId();
        bool cappedAndNeedDelete(long long dataSizeDelta, long long numRecordsDelta) const;

        // NOTE(wolfkdy): mongo4.2 uses _initNextIdIfNeeded to lazy init. accurate db bootstrap.
        // mongoRocks has not yet implemented this.

        // The use of this function requires that the passed in storage outlives the returned Slice
        static rocksdb::Slice _makeKey(const RecordId& loc, int64_t* storage);
        static std::string _makePrefixedKey(const std::string& prefix, const RecordId& loc);

        void _changeNumRecords(OperationContext* opCtx, int64_t amount);
        void _increaseDataSize(OperationContext* opCtx, int64_t amount);
        /**
         * Delete records from this record store as needed while _cappedMaxSize or _cappedMaxDocs is
         * exceeded.
         *
         * _inlock version to be called once a lock has been acquired.
         */
        int64_t _cappedDeleteAsNeeded(OperationContext* opCtx, const RecordId& justInserted);

    public:
        int64_t cappedDeleteAsNeeded_inlock(OperationContext* opCtx, const RecordId& justInserted);

    private:
        RocksEngine* _engine;                            // not owned
        rocksdb::TOTransactionDB* _db;                   // not owned
        rocksdb::ColumnFamilyHandle* _cf;                // not owned
        RocksOplogManager* _oplogManager;                // not owned
        RocksCounterManager* _counterManager;            // not owned
        RocksCompactionScheduler* _compactionScheduler;  // not owned
        std::string _prefix;

        const bool _isCapped;
        int64_t _cappedMaxSize;
        int64_t _cappedMaxSizeSlack;  // when to start applying backpressure
        const int64_t _cappedMaxDocs;
        CappedCallback* _cappedCallback;
        mutable Mutex _cappedCallbackMutex =
            MONGO_MAKE_LATCH("WiredTigerRecordStore::_cappedCallbackMutex");

        mutable stdx::timed_mutex _cappedDeleterMutex;  // see comment in ::cappedDeleteAsNeeded
        int _cappedDeleteCheckCount;                    // see comment in ::cappedDeleteAsNeeded

        const bool _isOplog;
        // nullptr iff _isOplog == false
        RocksOplogKeyTracker* _oplogKeyTracker;
        // keep track of when we compacted oplog last time. only valid when _isOplog == true.
        // Protected by _cappedDeleterMutex.
        Timer _oplogSinceLastCompaction;
        // compact oplog every 30 min
        static const int kOplogCompactEveryMins = 30;
        // compact oplog every 500K deletes
        static const int kOplogCompactEveryDeletedRecords = 500000;

        // invariant: there is no live records earlier than _cappedOldestKeyHint. There might be
        // some records that are dead after _cappedOldestKeyHint.
        // SeekToFirst() on an capped collection is an expensive operation because bunch of keys at
        // the start are deleted. To reduce the overhead, we remember the next key to delete and
        // seek directly to it. This will not work correctly if somebody inserted a key before this
        // _cappedOldestKeyHint. However, we prevent this from happening by using
        // _cappedVisibilityManager and checking isCappedHidden() during deletions
        RecordId _cappedOldestKeyHint;

        std::string _ident;
        AtomicWord<unsigned long long> _nextIdNum{0};
        std::atomic<long long> _dataSize;
        std::atomic<long long> _numRecords;

        const std::string _dataSizeKey;
        const std::string _numRecordsKey;

        bool _shuttingDown;
        bool _hasBackgroundThread;
        bool _tracksSizeAdjustments;
    };

    // Rocks failpoint to throw write conflict exceptions randomly
    MONGO_FAIL_POINT_DECLARE(RocksWriteConflictException);
    MONGO_FAIL_POINT_DECLARE(RocksWriteConflictExceptionForReads);

    // Prevents oplog writes from being considered durable on the primary. Once activated, new
    // writes will not be considered durable until deactivated. It is unspecified whether writes
    // that commit before activation will become visible while active.
    MONGO_FAIL_POINT_DECLARE(RocksPausePrimaryOplogDurabilityLoop);

}  // namespace mongo
