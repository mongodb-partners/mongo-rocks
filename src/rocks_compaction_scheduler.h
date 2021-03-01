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
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/platform/mutex.h"
#include "mongo/util/timer.h"
#include "mongo/util/concurrency/notification.h"
#include "mongo/bson/bsonobj.h"

namespace rocksdb {
    class CompactionFilterFactory;
    class ColumnFamilyHandle;
    class DB;
    class Iterator;
    struct WriteOptions;
    class WriteBatch;
    class TOTransaction;
    class TOTransactionDB;
}

namespace mongo {

    class CompactionBackgroundJob;

    class RocksCompactionScheduler {
    public:
        RocksCompactionScheduler();
        ~RocksCompactionScheduler();

        void start(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf);

        static int getSkippedDeletionsThreshold() { return kSkippedDeletionsThreshold; }

        void reportSkippedDeletionsAboveThreshold(rocksdb::ColumnFamilyHandle* cf, const std::string& prefix);

        // schedule compact range operation for execution in _compactionThread
        void compactAll();
        Status compactOplog(rocksdb::ColumnFamilyHandle* cf, const std::string& begin, const std::string& end);

        rocksdb::CompactionFilterFactory* createCompactionFilterFactory() const;
        std::unordered_map<uint32_t, BSONObj> getDroppedPrefixes() const;
        boost::optional<std::pair<uint32_t, std::pair<std::string, std::string>>> getOplogDeleteUntil() const;

        // load dropped prefixes, and re-schedule compaction of each dropped prefix.
        // as we don't know which cf a prefix exists, we have to compact each prefix out of each cf.
        // since prefix is globally unique, we don't worry about deleting unexpceted data.
        uint32_t loadDroppedPrefixes(rocksdb::Iterator* iter, const std::vector<rocksdb::ColumnFamilyHandle*>);
        Status dropPrefixesAtomic(rocksdb::ColumnFamilyHandle* cf,
                                  const std::vector<std::string>& prefixesToDrop,
                                  rocksdb::TOTransaction* txn,
                                  const BSONObj& debugInfo);
        void notifyCompacted(const std::string& begin, const std::string& end, bool rangeDropped,
                             bool opSucceeded);

    private:
        void compactPrefix(rocksdb::ColumnFamilyHandle* cf, const std::string& prefix);
        void compactDroppedPrefix(rocksdb::ColumnFamilyHandle* cf, const std::string& prefix);
        void compact(rocksdb::ColumnFamilyHandle* cf, const std::string& begin, const std::string& end,
                     bool rangeDropped, uint32_t order, boost::optional<std::shared_ptr<Notification<Status>>>);
        void droppedPrefixCompacted(const std::string& prefix, bool opSucceeded);

    private:
        Mutex _lock = MONGO_MAKE_LATCH("RocksCompactionScheduler::_lock");
        // protected by _lock
        Timer _timer;

        rocksdb::DB* _db;  // not owned

        // not owned, cf where compaction_scheduler's metadata exists.
        rocksdb::ColumnFamilyHandle* _metaCf;

        // Don't trigger compactions more often than every 10min
        static const int kMinCompactionIntervalMins = 10;
        // We'll compact the prefix if any operation on the prefix reports more than 50.000
        // deletions it had to skip over (this is about 10ms extra overhead)
        static const int kSkippedDeletionsThreshold = 50000;

        // thread for async execution of range compactions
        std::unique_ptr<CompactionBackgroundJob> _compactionJob;

        // set of all prefixes that are deleted. we delete them in the background thread

        mutable Mutex _droppedDataMutex =
            MONGO_MAKE_LATCH("RocksCompactionScheduler::_droppedDataMutex");

        std::unordered_map<uint32_t, BSONObj> _droppedPrefixes;

        std::atomic<uint32_t> _droppedPrefixesCount;
        boost::optional<std::pair<uint32_t, std::pair<std::string, std::string>>> _oplogDeleteUntil;
        static const std::string kDroppedPrefix;
    };
}  // namespace mongo
