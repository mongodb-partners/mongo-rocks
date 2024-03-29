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

#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {

    class RocksGlobalOptions {
    public:
        RocksGlobalOptions()
            : cacheSizeGB(0),
              maxWriteMBPerSec(1024),
              compression("snappy"),
              crashSafeCounters(false),
              counters(true),
              singleDeleteIndex(false),
              logLevel("info"),
              maxConflictCheckSizeMB(200) {}

        Status store(const optionenvironment::Environment& params);
        static Status validateRocksdbLogLevel(const std::string& value);
        static Status validateRocksdbCompressor(const std::string& value);
        size_t cacheSizeGB;
        int maxWriteMBPerSec;

        std::string compression;
        std::string configString;

        bool crashSafeCounters;
        bool counters;
        bool singleDeleteIndex;

        std::string logLevel;
        int maxConflictCheckSizeMB;
        int maxBackgroundJobs;
        long maxTotalWalSize;
        long dbWriteBufferSize;
        long writeBufferSize;
        long delayedWriteRate;
        int numLevels;
        int maxWriteBufferNumber;
        int level0FileNumCompactionTrigger;
        int level0SlowdownWritesTrigger;
        int level0StopWritesTrigger;
        long maxBytesForLevelBase;
        int softPendingCompactionMBLimit;
        int hardPendingCompactionMBLimit;
    };

    extern RocksGlobalOptions rocksGlobalOptions;
}  // namespace mongo
