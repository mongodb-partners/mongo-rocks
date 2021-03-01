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

#include "rocks_compaction_scheduler.h"

#include <queue>

#include "mongo/db/client.h"
#include "mongo/platform/mutex.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/background.h"
#include "mongo/util/concurrency/idle_thread_block.h"
#include "mongo/util/concurrency/notification.h"
#include "mongo/util/log.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/fail_point_service.h"
#include "rocks_util.h"
#include "rocks_record_store.h"

#include <rocksdb/compaction_filter.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/experimental.h>
#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/utilities/totransaction.h>
#include <rocksdb/utilities/totransaction_db.h>

namespace mongo {
    namespace {
        // The order in which compaction ops are executed (priority).
        // Smaller values run first.
        enum : uint32_t {
            kOrderOplog,
            kOrderFull,
            kOrderRange,
            kOrderDroppedRange,
        };

        MONGO_FAIL_POINT_DEFINE(RocksCompactionSchedulerPause);

        class OplogCompactionFilter: public rocksdb::CompactionFilter {
        public:
            explicit OplogCompactionFilter(const std::string& from, const std::string& until)
                : _from(from), _until(until) {}

            // filter is not called from multiple threads simultaneously
            virtual bool Filter(int level, const rocksdb::Slice& key,
                                const rocksdb::Slice& existing_value, std::string* new_value,
                                bool* value_changed) const {
                return key.compare(rocksdb::Slice(_until)) <= 0 &&
                       key.compare(rocksdb::Slice(_from)) >= 0;
            }

// IgnoreSnapshots is available since RocksDB 4.3
#if defined(ROCKSDB_MAJOR) && (ROCKSDB_MAJOR > 4 || (ROCKSDB_MAJOR == 4 && ROCKSDB_MINOR >= 3))
            virtual bool IgnoreSnapshots() const override { return true; }
#endif

            virtual const char* Name() const { return "OplogCompactionFilter"; }

        private:
            const std::string _from;
            const std::string _until;
        };

        class PrefixDeletingCompactionFilter : public rocksdb::CompactionFilter {
        public:
            explicit PrefixDeletingCompactionFilter(std::unordered_set<uint32_t> droppedPrefixes)
                : _droppedPrefixes(std::move(droppedPrefixes)),
                  _prefixCache(0),
                  _droppedCache(false) {}

            // filter is not called from multiple threads simultaneously
            virtual bool Filter(int level, const rocksdb::Slice& key,
                                const rocksdb::Slice& existing_value, std::string* new_value,
                                bool* value_changed) const {
                uint32_t prefix = 0;
                if (!extractPrefix(key, &prefix)) {
                    // this means there is a key in the database that's shorter than 4 bytes. this
                    // should never happen and this is a corruption. however, it's not compaction
                    // filter's job to report corruption, so we just silently continue
                    return false;
                }
                if (prefix == _prefixCache) {
                    return _droppedCache;
                }
                _prefixCache = prefix;
                _droppedCache = _droppedPrefixes.find(prefix) != _droppedPrefixes.end();
                return _droppedCache;
            }

// IgnoreSnapshots is available since RocksDB 4.3
#if defined(ROCKSDB_MAJOR) && (ROCKSDB_MAJOR > 4 || (ROCKSDB_MAJOR == 4 && ROCKSDB_MINOR >= 3))
            virtual bool IgnoreSnapshots() const override { return true; }
#endif

            virtual const char* Name() const { return "PrefixDeletingCompactionFilter"; }

        private:
            std::unordered_set<uint32_t> _droppedPrefixes;
            mutable uint32_t _prefixCache;
            mutable bool _droppedCache;
        };

        class PrefixDeletingCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
        public:
            explicit PrefixDeletingCompactionFilterFactory(
                const RocksCompactionScheduler* scheduler)
                : _compactionScheduler(scheduler) {}

            virtual std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
                const rocksdb::CompactionFilter::Context& context) override {
                auto oplogDelUntil = _compactionScheduler->getOplogDeleteUntil();
                if (oplogDelUntil != boost::none && oplogDelUntil->first == context.column_family_id) {
                    if (!context.is_manual_compaction) {
                        return std::unique_ptr<rocksdb::CompactionFilter>(nullptr);
                    }
                    return std::unique_ptr<rocksdb::CompactionFilter>(
                        new OplogCompactionFilter(oplogDelUntil->second.first, oplogDelUntil->second.second));
                }
                auto droppedPrefixes = _compactionScheduler->getDroppedPrefixes();
                if (droppedPrefixes.size() == 0) {
                    // no compaction filter needed
                    return std::unique_ptr<rocksdb::CompactionFilter>(nullptr);
                } else {
                    std::unordered_set<uint32_t> prefixes;
                    for (const auto& kv : droppedPrefixes) {
                        prefixes.insert(kv.first);
                    }
                    return std::unique_ptr<rocksdb::CompactionFilter>(
                        new PrefixDeletingCompactionFilter(std::move(prefixes)));
                }
            }

            virtual const char* Name() const override {
                return "PrefixDeletingCompactionFilterFactory";
            }

        private:
            const RocksCompactionScheduler* _compactionScheduler;
        };
    }  // namespace

    class CompactionBackgroundJob : public BackgroundJob {
    public:
        CompactionBackgroundJob(rocksdb::DB* db, RocksCompactionScheduler* compactionScheduler);
        virtual ~CompactionBackgroundJob();

        // schedule compact range operation for execution in _compactionThread
        void scheduleCompactOp(rocksdb::ColumnFamilyHandle* cf, const std::string& begin,
                               const std::string& end, bool rangeDropped, uint32_t order,
                               boost::optional<std::shared_ptr<Notification<Status>>>);

    private:
        // struct with compaction operation data
        struct CompactOp {
            rocksdb::ColumnFamilyHandle* _cf;
            std::string _start_str;
            std::string _end_str;
            bool _rangeDropped;
            uint32_t _order;
            boost::optional<std::shared_ptr<Notification<Status>>> _notification;
            bool operator>(const CompactOp& other) const { return _order > other._order; }
        };

        static const char* const _name;

        // BackgroundJob
        virtual std::string name() const override { return _name; }
        virtual void run() override;

        void compact(const CompactOp& op);

        rocksdb::DB* _db;                                // not owned
        RocksCompactionScheduler* _compactionScheduler;  // not owned

        bool _compactionThreadRunning = true;
        Mutex _compactionMutex = MONGO_MAKE_LATCH("CompactionBackgroundJob::_compactionMutex");
        stdx::condition_variable _compactionWakeUp;
        using CompactQueue =
            std::priority_queue<CompactOp, std::vector<CompactOp>, std::greater<CompactOp>>;
        CompactQueue _compactionQueue;
    };

    const char* const CompactionBackgroundJob::_name = "RocksCompactionThread";

    CompactionBackgroundJob::CompactionBackgroundJob(rocksdb::DB* db,
                                                     RocksCompactionScheduler* compactionScheduler)
        : _db(db), _compactionScheduler(compactionScheduler) {
        go();
    }

    CompactionBackgroundJob::~CompactionBackgroundJob() {
        {
            stdx::lock_guard<Latch> lk(_compactionMutex);
            _compactionThreadRunning = false;
            // Clean up the queue
            while (!_compactionQueue.empty()) {
                auto task = _compactionQueue.top();
                if (task._notification != boost::none) {
                    (*task._notification)->set(Status(ErrorCodes::ShutdownInProgress, "compact Q shutting down"));
                }
                _compactionQueue.pop();
            }
        }
// From 4.13 public release, CancelAllBackgroundWork() flushes all memtables for databases
// containing writes that have bypassed the WAL (writes issued with WriteOptions::disableWAL=true)
// before shutting down background threads, so it's safe to be called even if --nojournal mode
// is set.
#if defined(ROCKSDB_MAJOR) && (ROCKSDB_MAJOR > 4 || (ROCKSDB_MAJOR == 4 && ROCKSDB_MINOR >= 13))
        rocksdb::CancelAllBackgroundWork(_db);
#endif
        _compactionWakeUp.notify_one();
        wait();
    }

    namespace {
        template <class T>
        class unlock_guard {
        public:
            unlock_guard(T& lk) : lk_(lk) { lk_.unlock(); }

            ~unlock_guard() { lk_.lock(); }

            unlock_guard(const unlock_guard&) = delete;
            unlock_guard& operator=(const unlock_guard&) = delete;

        private:
            T& lk_;
        };
    }  // namespace

    void CompactionBackgroundJob::run() {
        Client::initThread(_name);
        stdx::unique_lock<Latch> lk(_compactionMutex);
        while (_compactionThreadRunning) {
            // check if we have something to compact
            if (MONGO_FAIL_POINT(RocksCompactionSchedulerPause)) {
                unlock_guard<decltype(lk)> rlk(lk);
                mongo::sleepsecs(1);
                continue;
            }
            if (_compactionQueue.empty()) {
                MONGO_IDLE_THREAD_BLOCK;
                _compactionWakeUp.wait(lk);
            } else {
                // get item from queue
                const CompactOp op(std::move(_compactionQueue.top()));
                _compactionQueue.pop();
                // unlock mutex for the time of compaction
                unlock_guard<decltype(lk)> rlk(lk);
                // do compaction
                compact(op);
            }
        }
        lk.unlock();
        LOG(1) << "Compaction thread terminating" << std::endl;
    }

    void CompactionBackgroundJob::scheduleCompactOp(rocksdb::ColumnFamilyHandle* cf,
                                                    const std::string& begin, const std::string& end,
                                                    bool rangeDropped, uint32_t order,
                                                    boost::optional<std::shared_ptr<Notification<Status>>> notification) {
        {
            stdx::lock_guard<Latch> lk(_compactionMutex);
            _compactionQueue.push({cf, begin, end, rangeDropped, order, notification});
        }
        _compactionWakeUp.notify_one();
    }

    void CompactionBackgroundJob::compact(const CompactOp& op) {
        rocksdb::Slice start_slice(op._start_str);
        rocksdb::Slice end_slice(op._end_str);

        rocksdb::Slice* start = !op._start_str.empty() ? &start_slice : nullptr;
        rocksdb::Slice* end = !op._end_str.empty() ? &end_slice : nullptr;
        const bool isOplog = NamespaceString::oplog(op._cf->GetName());
        LOG(1) << "Starting compaction of cf: " << op._cf->GetName()
               << " range: " << (start ? start->ToString(true) : "<begin>")
               << " .. " << (end ? end->ToString(true) : "<end>") << " (rangeDropped is "
               << op._rangeDropped << ")" << " (isOplog is " << isOplog << ")";

        if (op._rangeDropped) {
            auto s = rocksdb::DeleteFilesInRange(_db, op._cf, start, end);
            if (!s.ok()) {
                // Do not fail the entire procedure, since there is still chance
                // to purge the range below, in CompactRange
                log() << "Failed to delete files in compacted range: " << s.ToString();
            }
        }

        rocksdb::CompactRangeOptions compact_options;
        compact_options.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForce;
        // if auto-compaction runs parallelly, oplog compact-range may leave a hole.
        compact_options.exclusive_manual_compaction = isOplog;
        compact_options.ignore_pin_timestamp = isOplog;

        auto s = _db->CompactRange(compact_options, op._cf, start, end);
        if (!s.ok()) {
            log() << "Failed to compact range: " << s.ToString();
            if (op._notification != boost::none) {
                (*op._notification)->set(rocksToMongoStatus(s));
            }
            // Let's leave as quickly as possible if in shutdown
            stdx::lock_guard<Latch> lk(_compactionMutex);
            if (!_compactionThreadRunning) {
                return;
            }
        } else {
            if (op._notification != boost::none) {
                (*op._notification)->set(Status::OK());
            }
        }

        _compactionScheduler->notifyCompacted(op._start_str, op._end_str, op._rangeDropped, s.ok());
    }

    // first four bytes are the default prefix 0
    const std::string RocksCompactionScheduler::kDroppedPrefix("\0\0\0\0droppedprefix-", 18);

    RocksCompactionScheduler::RocksCompactionScheduler()
        : _db(nullptr), _metaCf(nullptr), _droppedPrefixesCount(0) {}

    void RocksCompactionScheduler::start(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf) {
        _db = db;
        _metaCf = cf;
        _timer.reset();
        _compactionJob.reset(new CompactionBackgroundJob(db, this));
    }

    void RocksCompactionScheduler::reportSkippedDeletionsAboveThreshold(rocksdb::ColumnFamilyHandle* cf,
                                                                        const std::string& prefix) {
        bool schedule = false;
        {
            stdx::lock_guard<Latch> lk(_lock);
            if (_timer.minutes() >= kMinCompactionIntervalMins) {
                schedule = true;
                _timer.reset();
            }
        }
        if (schedule) {
            log() << "Scheduling compaction to clean up tombstones for cf: "
                  << cf->GetName()
                  << ", prefix: "
                  << rocksdb::Slice(prefix).ToString(true);
            // we schedule compaction now (ignoring error)
            compactPrefix(cf, prefix);
        }
    }

    RocksCompactionScheduler::~RocksCompactionScheduler() {
        // We need this to avoid incomplete type deletion
        _compactionJob.reset();
    }

    void RocksCompactionScheduler::compactAll() {
        // NOTE(wolfkdy): compactAll only compacts DefaultColumnFamily
        // oplog cf is handled in RocksRecordStore.
        compact(_db->DefaultColumnFamily(), std::string(), std::string(), false, kOrderFull, boost::none);
    }

    Status RocksCompactionScheduler::compactOplog(rocksdb::ColumnFamilyHandle* cf,
                                                  const std::string& begin, 
                                                  const std::string& end) {
        {
            stdx::lock_guard<stdx::mutex> lk(_droppedDataMutex);
            invariant(begin <= end);
            if (_oplogDeleteUntil != boost::none) {
                invariant(cf->GetID() == _oplogDeleteUntil->first);
            }
            _oplogDeleteUntil = {cf->GetID(), {begin, end}};
        }
        auto notification = std::make_shared<Notification<Status>>();
        compact(cf, begin, end, false, kOrderOplog, notification);
        auto s = notification->get();
        if (!s.isOK()) {
            LOG(0) << "compactOplog to " << rocksdb::Slice(end).ToString() << " failed " << s;
        }
        return s;
    }

    void RocksCompactionScheduler::compactPrefix(rocksdb::ColumnFamilyHandle* cf, const std::string& prefix) {
        compact(cf, prefix, rocksGetNextPrefix(prefix), false, kOrderRange, boost::none);
    }

    void RocksCompactionScheduler::compactDroppedPrefix(rocksdb::ColumnFamilyHandle* cf,
                                                        const std::string& prefix) {
        LOG(0) << "Compacting dropped prefix: " << rocksdb::Slice(prefix).ToString(true)
               << " from cf: " << cf->GetName();
        compact(cf, prefix, rocksGetNextPrefix(prefix), true, kOrderDroppedRange, boost::none);
    }

    void RocksCompactionScheduler::compact(rocksdb::ColumnFamilyHandle* cf,
                                           const std::string& begin, const std::string& end,
                                           bool rangeDropped, uint32_t order,
                                           boost::optional<std::shared_ptr<Notification<Status>>> notification) {
        _compactionJob->scheduleCompactOp(cf, begin, end, rangeDropped, order, notification);
    }

    rocksdb::CompactionFilterFactory* RocksCompactionScheduler::createCompactionFilterFactory()
        const {
        return new PrefixDeletingCompactionFilterFactory(this);
    }

    std::unordered_map<uint32_t, BSONObj> RocksCompactionScheduler::getDroppedPrefixes() const {
        stdx::lock_guard<Latch> lk(_droppedDataMutex);
        // this will copy the set. that way compaction filter has its own copy and doesn't need to
        // worry about thread safety
        std::unordered_map<uint32_t, BSONObj> ret;
        for(auto p : _droppedPrefixes) {
          ret.emplace(p.first, p.second.copy());
        }
        return ret;
    }

    uint32_t RocksCompactionScheduler::loadDroppedPrefixes(rocksdb::Iterator* iter,
                                                           std::vector<rocksdb::ColumnFamilyHandle*> cfs) {
        invariant(iter);
        const uint32_t rocksdbSkippedDeletionsInitial =
            (uint32_t)get_internal_delete_skipped_count();
        int dropped_count = 0;
        uint32_t int_prefix = 0;

        // NOTE(cuixin): only invoke in rocksengine contruct function, no need check conflict
        for (iter->Seek(kDroppedPrefix); iter->Valid() && iter->key().starts_with(kDroppedPrefix);
             iter->Next()) {
            invariantRocksOK(iter->status());
            rocksdb::Slice prefix(iter->key());
            prefix.remove_prefix(kDroppedPrefix.size());

            // let's instruct the compaction scheduler to compact dropped prefix
            ++dropped_count;
            bool ok = extractPrefix(prefix, &int_prefix);
            invariant(ok);
            {
                stdx::lock_guard<Latch> lk(_droppedDataMutex);
                _droppedPrefixes.emplace(int_prefix, BSONObj(iter->value().data()).copy());
            }
            LOG(1) << "Compacting dropped prefix: " << prefix.ToString(true);
            for (auto cf : cfs) {
                compactDroppedPrefix(cf, prefix.ToString());
            }
        }
        log() << dropped_count << " dropped prefixes need compaction";

        const uint32_t skippedDroppedPrefixMarkers =
            (uint32_t)get_internal_delete_skipped_count() - rocksdbSkippedDeletionsInitial;
        _droppedPrefixesCount.fetch_add(skippedDroppedPrefixMarkers, std::memory_order_relaxed);
        return int_prefix;
    }

    boost::optional<std::pair<uint32_t, std::pair<std::string, std::string>>> RocksCompactionScheduler::getOplogDeleteUntil() const {
        stdx::lock_guard<stdx::mutex> lk(_droppedDataMutex);
        return _oplogDeleteUntil;
    }

    Status RocksCompactionScheduler::dropPrefixesAtomic(
        rocksdb::ColumnFamilyHandle* cf,
        const std::vector<std::string>& prefixesToDrop,
        rocksdb::TOTransaction* txn,
        const BSONObj& debugInfo) {
        // We record the fact that we're deleting this prefix. That way we ensure that the prefix is
        // always deleted
        for (const auto& prefix : prefixesToDrop) {
            auto s = txn->Put(_metaCf, kDroppedPrefix + prefix,
                              rocksdb::Slice(debugInfo.objdata(), debugInfo.objsize()));
            if (!s.ok()) {
                log() << "dropPrefixesAtomic error: " << s.ToString();
                return rocksToMongoStatus(s);
            }
            log() << "put into: " << _metaCf->GetName()
                  << " prefix:" << rocksdb::Slice(prefix).ToString(true)
                  << " debugInfo:" << debugInfo;
        }

        auto s = txn->Commit();
        if (!s.ok()) {
            return rocksToMongoStatus(s);
        }

        // instruct compaction filter to start deleting
        {
            stdx::lock_guard<Latch> lk(_droppedDataMutex);
            for (const auto& prefix : prefixesToDrop) {
                uint32_t int_prefix;
                bool ok = extractPrefix(prefix, &int_prefix);
                invariant(ok);
                _droppedPrefixes.emplace(int_prefix, debugInfo);
            }
        }

        // Suggest compaction for the prefixes that we need to drop, So that
        // we free space as fast as possible.
        for (auto& prefix : prefixesToDrop) {
            compactDroppedPrefix(cf, prefix);
        }

        return Status::OK();
    }

    void RocksCompactionScheduler::notifyCompacted(const std::string& begin, const std::string& end,
                                                   bool rangeDropped, bool opSucceeded) {
        if (rangeDropped) {
            droppedPrefixCompacted(begin, opSucceeded);
        }
    }

    void RocksCompactionScheduler::droppedPrefixCompacted(const std::string& prefix,
                                                          bool opSucceeded) {
        uint32_t int_prefix;
        bool ok = extractPrefix(prefix, &int_prefix);
        log() << "compact droppedPrefix: " << rocksdb::Slice(prefix).ToString(true)
              << (opSucceeded ? " success" : " failed");
        invariant(ok);
        {
            stdx::lock_guard<Latch> lk(_droppedDataMutex);
            _droppedPrefixes.erase(int_prefix);
        }
        if (opSucceeded) {
            rocksdb::WriteOptions syncOptions;
            syncOptions.sync = true;
            _db->Delete(syncOptions, _metaCf, kDroppedPrefix + prefix);

            // This operation only happens from one thread, so no concurrent
            // updates are possible.
            // The only time this counter may be modified from a different
            // thread is during startup loading of dropped prefixes.
            // But that's not a big issue, we'll eventually sync and call
            // compaction next time if needed.
            if (_droppedPrefixesCount.fetch_add(1, std::memory_order_relaxed) >=
                kSkippedDeletionsThreshold) {
                log() << "Compacting dropped prefixes markers";
                _droppedPrefixesCount.store(0, std::memory_order_relaxed);
                // Let's compact the full default (system) prefix 0.
                compactPrefix(_metaCf, encodePrefix(0));
            }
        }
    }
}  // namespace mongo
