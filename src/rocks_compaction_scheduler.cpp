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

#include <deque>

#include "mongo/platform/basic.h"

#include "rocks_compaction_scheduler.h"

#include "mongo/db/client.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/background.h"
#include "mongo/util/log.h"
#include "rocks_util.h"

#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/experimental.h>
#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>

namespace mongo {
    class CompactionBackgroundJob : public BackgroundJob {
    public:
        CompactionBackgroundJob(rocksdb::DB* db);
        virtual ~CompactionBackgroundJob();

        // schedule compact range operation for execution in _compactionThread
        Status scheduleCompactOp(const std::string& begin = std::string(), const std::string& end = std::string(),
                                 bool rangeDropped = false, const std::function<void(bool)>& cleanup = std::function<void(bool)>());

    private:
        // struct with compaction operation data
        struct CompactOp {
            void doCompact(rocksdb::DB* db) const;

            std::string _start_str;
            std::string _end_str;
            bool _rangeDropped;
            std::function<void(bool)> _cleanup;
        };

        static const char * const _name;

        // BackgroundJob
        virtual std::string name() const override { return _name; }
        virtual void run() override;

        rocksdb::DB* _db;  // not owned

        bool _compactionThreadRunning = true;
        stdx::mutex _compactionMutex;
        stdx::condition_variable _compactionWakeUp;
        std::deque<CompactOp> _compactionQueue;
    };

    const char* const CompactionBackgroundJob::_name = "RocksCompactionThread";

    CompactionBackgroundJob::CompactionBackgroundJob(rocksdb::DB* db)
        : _db(db) {
        go();
    }

    CompactionBackgroundJob::~CompactionBackgroundJob() {
        {
            stdx::lock_guard<stdx::mutex> lk(_compactionMutex);
            _compactionThreadRunning = false;
            _compactionQueue.clear();
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
                const CompactOp op(std::move(_compactionQueue.front()));
                _compactionQueue.pop_front();
                // unlock mutex for the time of compaction
                unlock_guard<decltype(lk)> rlk(lk);
                // do compaction
                op.doCompact(_db);
            }
        }
        lk.unlock();
        LOG(1) << "compaction thread terminating" << std::endl;
    }

    Status CompactionBackgroundJob::scheduleCompactOp(const std::string& begin, const std::string& end,
                                                       bool rangeDropped, const std::function<void(bool)>& cleanup) {
        {
            stdx::lock_guard<stdx::mutex> lk(_compactionMutex);
            _compactionQueue.push_back({begin, end, rangeDropped, cleanup});
        }
        _compactionWakeUp.notify_one();
        return Status::OK();
    }

    void CompactionBackgroundJob::CompactOp::doCompact(rocksdb::DB* db) const {
        rocksdb::Slice start_slice(_start_str);
        rocksdb::Slice end_slice(_end_str);

        rocksdb::Slice* start = !_start_str.empty() ? &start_slice : nullptr;
        rocksdb::Slice* end = !_end_str.empty() ? &end_slice : nullptr;

        LOG(1) << "starting compaction of range: "
              << (start ? start->ToString(true) : "<begin>") << " .. "
              << (end ? end->ToString(true) : "<end>")
              << " (_rangeDropped is " << _rangeDropped << ")";

        if (_rangeDropped) {
            auto s = rocksdb::DeleteFilesInRange(db, db->DefaultColumnFamily(), start, end);
            if (!s.ok()) {
                log() << "failed to delete files in range: " << s.ToString();
            }
        }

        rocksdb::CompactRangeOptions compact_options;
        compact_options.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForce;
        compact_options.exclusive_manual_compaction = false;
        auto s = db->CompactRange(compact_options, start, end);
        if (!s.ok()) {
            log() << "failed to compact range: " << s.ToString();
        }

        if (_cleanup) {
            _cleanup(s.ok());
        }
    }

    // first four bytes are the default prefix 0
    const std::string RocksCompactionScheduler::kDroppedPrefix("\0\0\0\0droppedprefix-", 18);

    RocksCompactionScheduler::RocksCompactionScheduler(rocksdb::DB* db)
        : _db(db), _compactionJob(new CompactionBackgroundJob(db)) {
        _timer.reset();
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

    Status RocksCompactionScheduler::compactAll() {
        return compactRange(std::string(), std::string());
    }

    Status RocksCompactionScheduler::compactRange(const std::string& start, const std::string& end) {
        return _compactionJob->scheduleCompactOp(start, end);
    }

    Status RocksCompactionScheduler::compactPrefix(const std::string& prefix) {
        return compactRange(prefix, rocksGetNextPrefix(prefix));
    }

    Status RocksCompactionScheduler::compactDroppedRange(const std::string& start, const std::string& end,
                                                         const std::function<void(bool)>& cleanup) {
        return _compactionJob->scheduleCompactOp(start, end, true, cleanup);
    }

    Status RocksCompactionScheduler::compactDroppedPrefix(const std::string& prefix,
                                                          const std::function<void(bool)>& cleanup) {
        return compactDroppedRange(prefix, rocksGetNextPrefix(prefix), cleanup);
    }

    std::unordered_set<uint32_t> RocksCompactionScheduler::getDroppedPrefixes() const {
        stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
        // this will copy the set. that way compaction filter has its own copy and doesn't need to
        // worry about thread safety
        return _droppedPrefixes;
    }

    void RocksCompactionScheduler::loadDroppedPrefixes(rocksdb::Iterator* iter) {
        invariant(iter);
        int dropped_count = 0;
        for (iter->Seek(kDroppedPrefix); iter->Valid() && iter->key().starts_with(kDroppedPrefix);
             iter->Next()) {
            invariantRocksOK(iter->status());
            rocksdb::Slice prefix(iter->key());
            std::string prefixkey(prefix.ToString());
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
            LOG(1) << "compacting dropped prefix: " << prefix.ToString(true);
            auto s = compactDroppedPrefix(prefix.ToString(), [=](bool opSucceeded) {
                {
                    stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
                    _droppedPrefixes.erase(int_prefix);
                }
                if (opSucceeded) {
                    rocksdb::WriteOptions syncOptions;
                    syncOptions.sync = true;
                    _db->Delete(syncOptions, prefixkey);
                }
            });
            if (!s.isOK()) {
                log() << "failed to schedule compaction for prefix " << prefix.ToString(true);
            }
        }
        log() << dropped_count << " dropped prefixes need compaction";
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
            auto s = compactDroppedPrefix(prefix, [=](bool opSucceeded) {
                {
                    uint32_t int_prefix;
                    bool ok = extractPrefix(prefix, &int_prefix);
                    invariant(ok);
                    stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
                    _droppedPrefixes.erase(int_prefix);
                }
                if (opSucceeded) {
                    rocksdb::WriteOptions syncOptions;
                    syncOptions.sync = true;
                    _db->Delete(syncOptions, kDroppedPrefix + prefix);
                }
            });
            if (!s.isOK()) {
                log() << "failed to schedule compaction for prefix "
                      << rocksdb::Slice(prefix).ToString(true);
            }
        }

        return Status::OK();
    }
}
