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
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/background.h"
#include "mongo/util/log.h"
#include "rocks_util.h"

#include <rocksdb/compaction_filter.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/experimental.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>

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
                auto droppedPrefixes = _compactionScheduler->getDroppedPrefixes();
                if (droppedPrefixes.size() == 0) {
                    // no compaction filter needed
                    return std::unique_ptr<rocksdb::CompactionFilter>(nullptr);
                } else {
                    return std::unique_ptr<rocksdb::CompactionFilter>(
                        new PrefixDeletingCompactionFilter(std::move(droppedPrefixes)));
                }
            }

            virtual const char* Name() const override {
                return "PrefixDeletingCompactionFilterFactory";
            }

        private:
            const RocksCompactionScheduler* _compactionScheduler;
        };
    } // end of anon namespace

    class CompactionBackgroundJob : public BackgroundJob {
    public:
        CompactionBackgroundJob(rocksdb::DB* db, RocksCompactionScheduler* compactionScheduler);
        virtual ~CompactionBackgroundJob();

        // schedule compact range operation for execution in _compactionThread
        void scheduleCompactOp(const std::string& begin, const std::string& end, bool rangeDropped,
                               uint32_t order);

    private:
        // struct with compaction operation data
        struct CompactOp {
            std::string _start_str;
            std::string _end_str;
            bool _rangeDropped;

            uint32_t _order;
            bool operator>(const CompactOp& other) const { return _order > other._order; }
        };

        static const char * const _name;

        // BackgroundJob
        virtual std::string name() const override { return _name; }
        virtual void run() override;

        void compact(const CompactOp& op);

        rocksdb::DB* _db;  // not owned
        RocksCompactionScheduler* _compactionScheduler;  // not owned

        bool _compactionThreadRunning = true;
        stdx::mutex _compactionMutex;
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
            stdx::lock_guard<stdx::mutex> lk(_compactionMutex);
            _compactionThreadRunning = false;
            // Clean up the queue
            CompactQueue tmp;
            _compactionQueue.swap(tmp);
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
          unlock_guard(T& lk) : lk_(lk) {
            lk_.unlock();
          }

          ~unlock_guard() {
            lk_.lock();
          }

          unlock_guard(const unlock_guard&) = delete;
          unlock_guard& operator=(const unlock_guard&) = delete;

        private:
          T& lk_;
        };
    }

    void CompactionBackgroundJob::run() {
        Client::initThread(_name);
        stdx::unique_lock<stdx::mutex> lk(_compactionMutex);
        while (_compactionThreadRunning) {
            // check if we have something to compact
            if (_compactionQueue.empty())
                _compactionWakeUp.wait(lk);
            else {
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

    void CompactionBackgroundJob::scheduleCompactOp(const std::string& begin,
                                                    const std::string& end, bool rangeDropped,
                                                    uint32_t order) {
        {
            stdx::lock_guard<stdx::mutex> lk(_compactionMutex);
            _compactionQueue.push({begin, end, rangeDropped, order});
        }
        _compactionWakeUp.notify_one();
    }

    void CompactionBackgroundJob::compact(const CompactOp& op) {
        rocksdb::Slice start_slice(op._start_str);
        rocksdb::Slice end_slice(op._end_str);

        rocksdb::Slice* start = !op._start_str.empty() ? &start_slice : nullptr;
        rocksdb::Slice* end = !op._end_str.empty() ? &end_slice : nullptr;

        LOG(1) << "Starting compaction of range: "
              << (start ? start->ToString(true) : "<begin>") << " .. "
              << (end ? end->ToString(true) : "<end>")
              << " (rangeDropped is " << op._rangeDropped << ")";

        if (op._rangeDropped) {
            auto s = rocksdb::DeleteFilesInRange(_db, _db->DefaultColumnFamily(), start, end);
            if (!s.ok()) {
                log() << "Failed to delete files in compacted range: " << s.ToString();
            }
        }

        rocksdb::CompactRangeOptions compact_options;
        compact_options.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForce;
        compact_options.exclusive_manual_compaction = false;
        auto s = _db->CompactRange(compact_options, start, end);
        if (!s.ok()) {
            log() << "Failed to compact range: " << s.ToString();

            // Let's leave as quickly as possible if in shutdown
            stdx::lock_guard<stdx::mutex> lk(_compactionMutex);
            if (!_compactionThreadRunning) {
                return;
            }
        }

        _compactionScheduler->notifyCompacted(op._start_str, op._end_str, op._rangeDropped, s.ok());
    }

    // first four bytes are the default prefix 0
    const std::string RocksCompactionScheduler::kDroppedPrefix("\0\0\0\0droppedprefix-", 18);

    RocksCompactionScheduler::RocksCompactionScheduler() : _db(nullptr), _droppedPrefixesCount(0) {}

    void RocksCompactionScheduler::start(rocksdb::DB* db) {
        _db = db;
        _timer.reset();
        _compactionJob.reset(new CompactionBackgroundJob(db, this));
    }

    void RocksCompactionScheduler::reportSkippedDeletionsAboveThreshold(const std::string& prefix) {
        bool schedule = false;
        {
            stdx::lock_guard<stdx::mutex> lk(_lock);
            if (_timer.minutes() >= kMinCompactionIntervalMins) {
                schedule = true;
                _timer.reset();
            }
        }
        if (schedule) {
            log() << "Scheduling compaction to clean up tombstones for prefix "
                  << rocksdb::Slice(prefix).ToString(true);
            // we schedule compaction now (ignoring error)
            compactPrefix(prefix);
        }
    }

    RocksCompactionScheduler::~RocksCompactionScheduler() {
        // We need this to avoid incomplete type deletion
        _compactionJob.reset();
    }

    void RocksCompactionScheduler::compactAll() {
        compact(std::string(), std::string(), false, kOrderFull);
    }

    void RocksCompactionScheduler::compactOplog(const std::string& begin, const std::string& end) {
        compact(begin, end, false, kOrderOplog);
    }

    void RocksCompactionScheduler::compactPrefix(const std::string& prefix) {
        compact(prefix, rocksGetNextPrefix(prefix), false, kOrderRange);
    }

    void RocksCompactionScheduler::compactDroppedPrefix(const std::string& prefix) {
        compact(prefix, rocksGetNextPrefix(prefix), true, kOrderDroppedRange);
    }

    void RocksCompactionScheduler::compact(const std::string& begin, const std::string& end,
                                           bool rangeDropped, uint32_t order) {
        _compactionJob->scheduleCompactOp(begin, end, rangeDropped, order);
    }

    rocksdb::CompactionFilterFactory* RocksCompactionScheduler::createCompactionFilterFactory()
        const {
        return new PrefixDeletingCompactionFilterFactory(this);
    }

    std::unordered_set<uint32_t> RocksCompactionScheduler::getDroppedPrefixes() const {
        stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
        // this will copy the set. that way compaction filter has its own copy and doesn't need to
        // worry about thread safety
        return _droppedPrefixes;
    }

    void RocksCompactionScheduler::loadDroppedPrefixes(rocksdb::Iterator* iter) {
        invariant(iter);
        const uint32_t rocksdbSkippedDeletionsInitial =
            rocksdb::perf_context.internal_delete_skipped_count;
        int dropped_count = 0;
        for (iter->Seek(kDroppedPrefix); iter->Valid() && iter->key().starts_with(kDroppedPrefix);
             iter->Next()) {
            invariantRocksOK(iter->status());
            rocksdb::Slice prefix(iter->key());
            prefix.remove_prefix(kDroppedPrefix.size());

            // let's instruct the compaction scheduler to compact dropped prefix
            ++dropped_count;
            uint32_t int_prefix;
            bool ok = extractPrefix(prefix, &int_prefix);
            invariant(ok);
            {
                stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
                _droppedPrefixes.insert(int_prefix);
            }
            LOG(1) << "Compacting dropped prefix: " << prefix.ToString(true);
            compactDroppedPrefix(prefix.ToString());
        }
        log() << dropped_count << " dropped prefixes need compaction";

        const uint32_t skippedDroppedPrefixMarkers =
            rocksdb::perf_context.internal_delete_skipped_count - rocksdbSkippedDeletionsInitial;
        _droppedPrefixesCount.fetch_add(skippedDroppedPrefixMarkers, std::memory_order_relaxed);
    }

    Status RocksCompactionScheduler::dropPrefixesAtomic(
        const std::vector<std::string>& prefixesToDrop, const rocksdb::WriteOptions& syncOptions,
        rocksdb::WriteBatch& wb) {
        // We record the fact that we're deleting this prefix. That way we ensure that the prefix is
        // always deleted
        for (const auto& prefix : prefixesToDrop) {
            wb.Put(kDroppedPrefix + prefix, "");
        }

        auto s = _db->Write(syncOptions, &wb);
        if (!s.ok()) {
            return rocksToMongoStatus(s);
        }

        // instruct compaction filter to start deleting
        {
            stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
            for (const auto& prefix : prefixesToDrop) {
                uint32_t int_prefix;
                bool ok = extractPrefix(prefix, &int_prefix);
                invariant(ok);
                _droppedPrefixes.insert(int_prefix);
            }
        }

        // Suggest compaction for the prefixes that we need to drop, So that
        // we free space as fast as possible.
        for (auto& prefix : prefixesToDrop) {
            compactDroppedPrefix(prefix);
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
        invariant(ok);
        {
            stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
            _droppedPrefixes.erase(int_prefix);
        }
        if (opSucceeded) {
            rocksdb::WriteOptions syncOptions;
            syncOptions.sync = true;
            _db->Delete(syncOptions, kDroppedPrefix + prefix);

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
                compactPrefix(kDroppedPrefix);
            }
        }
    }
}
