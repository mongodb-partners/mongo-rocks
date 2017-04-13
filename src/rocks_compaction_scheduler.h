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
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/timer.h"

namespace rocksdb {
    class CompactionFilterFactory;
    class DB;
    class Iterator;
    class WriteOptions;
    class WriteBatch;
}

namespace mongo {

    class CompactionBackgroundJob;

    class RocksCompactionScheduler {
    public:
        RocksCompactionScheduler();
        ~RocksCompactionScheduler();

        void start(rocksdb::DB* db);

        static int getSkippedDeletionsThreshold() { return kSkippedDeletionsThreshold; }

        void reportSkippedDeletionsAboveThreshold(const std::string& prefix);

        // schedule compact range operation for execution in _compactionThread
        void compactAll();
        void compactRange(const std::string& begin, const std::string& end);
        void compactPrefix(const std::string& prefix);

        rocksdb::CompactionFilterFactory* createCompactionFilterFactory() const;
        std::unordered_set<uint32_t> getDroppedPrefixes() const;
        void loadDroppedPrefixes(rocksdb::Iterator* iter);
        Status dropPrefixesAtomic(const std::vector<std::string>& prefixesToDrop,
                                  const rocksdb::WriteOptions& syncOptions,
                                  rocksdb::WriteBatch& wb);
        void notifyCompacted(const std::string& begin, const std::string& end, bool rangeDropped,
                             bool opSucceeded);

    private:
        void compactDroppedRange(const std::string& begin, const std::string& end);
        void compactDroppedPrefix(const std::string& prefix);
        void droppedPrefixCompacted(const std::string& prefix, bool opSucceeded);

    private:
        stdx::mutex _lock;
        // protected by _lock
        Timer _timer;

        rocksdb::DB* _db;  // not owned

        // Don't trigger compactions more often than every 10min
        static const int kMinCompactionIntervalMins = 10;
        // We'll compact the prefix if any operation on the prefix reports more than 50.000
        // deletions it had to skip over (this is about 10ms extra overhead)
        static const int kSkippedDeletionsThreshold = 50000;

        // thread for async execution of range compactions
        std::unique_ptr<CompactionBackgroundJob> _compactionJob;

        // set of all prefixes that are deleted. we delete them in the background thread
        mutable stdx::mutex _droppedPrefixesMutex;
        std::unordered_set<uint32_t> _droppedPrefixes;
        std::atomic<uint32_t> _droppedPrefixesCount;

        static const std::string kDroppedPrefix;
    };
}
