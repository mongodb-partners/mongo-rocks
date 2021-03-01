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

#include <rocksdb/status.h>
#include <string>
#include "mongo/util/assert_util.h"

#ifdef __linux__
#include <fstream>
#include <sstream>
#include <boost/algorithm/string/finder.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/filesystem.hpp>
#include "mongo/util/procparser.h"
#include "mongo/base/parse_number.h"
#include "mongo/bson/bsonobjbuilder.h"
#endif

namespace mongo {

#ifdef __linux__
    namespace {
        const char* const kDiskFields[] = {
            "reads",
            "reads_merged",
            "read_sectors",
            "read_time_ms",
            "writes",
            "writes_merged",
            "write_sectors",
            "write_time_ms",
            "io_in_progress",
            "io_time_ms",
            "io_queued_ms",
        };

        const size_t kDiskFieldCount = std::extent<decltype(kDiskFields)>::value;

        StatusWith<std::string> readFileAsString(StringData filename) {
            std::ifstream ifile(filename.rawData());
            std::ostringstream buffer;
            char ch;
            while (buffer && ifile.get(ch)) {
                buffer.put(ch);
            }
            ifile.close();
            return buffer.str(); 
        }

        Status parseProcDiskStatsByDevNo(const std::vector<StringData>& disks,
                                         StringData data,
                                         BSONObjBuilder* builder,
                                         bool byDevNo) {
            bool foundKeys = false;
            std::vector<uint64_t> stats;
            stats.reserve(kDiskFieldCount);

            using string_split_iterator = boost::split_iterator<StringData::const_iterator>;

            // Split the file by lines.
            // token_compress_on means the iterator skips over consecutive '\n'. This should not be a
            // problem in normal /proc/diskstats output.
            for (string_split_iterator lineIt = string_split_iterator(
                     data.begin(),
                     data.end(),
                     boost::token_finder([](char c) { return c == '\n'; }, boost::token_compress_on));
                 lineIt != string_split_iterator();
                 ++lineIt) {

                StringData line((*lineIt).begin(), (*lineIt).end());

                // Skip leading whitespace so that the split_iterator starts on non-whitespace otherwise we
                // get an empty first token. Device major numbers (the first number on each line) are right
                // aligned to 4 spaces and start from
                // single digits.
                auto beginNonWhitespace =
                    std::find_if_not(line.begin(), line.end(), [](char c) { return c == ' '; });

                // Split the line by spaces since that is the only delimiter for diskstats files.
                // token_compress_on means the iterator skips over consecutive ' '.
                string_split_iterator partIt = string_split_iterator(
                    beginNonWhitespace,
                    line.end(),
                    boost::token_finder([](char c) { return c == ' '; }, boost::token_compress_on));

                // Skip processing this line if the line is blank
                if (partIt == string_split_iterator()) {
                    continue;
                }
                StringData mainDevNo((*partIt).begin(), (*partIt).end());

                ++partIt;

                // Skip processing this line if we only have a device major number.
                if (partIt == string_split_iterator()) {
                    continue;
                }
                StringData subDevNo((*partIt).begin(), (*partIt).end());

                ++partIt;

                // Skip processing this line if we only have a device major minor.
                if (partIt == string_split_iterator()) {
                    continue;
                }

                StringData disk((*partIt).begin(), (*partIt).end());
                std::string diskStr = disk.toString();
                if (byDevNo) {
                    diskStr = mainDevNo.toString() + ":" + subDevNo.toString();
                }

                // Skip processing this line if we only have a block device name.
                if (partIt == string_split_iterator()) {
                    continue;
                }

                ++partIt;

                // Check if the disk is in the list. /proc/diskstats will have extra disks, and may not have
                // the disk we want.
                if (disks.empty() || std::find(disks.begin(), disks.end(), diskStr) != disks.end()) {
                    foundKeys = true;

                    stats.clear();

                    // Only generate a disk document if the disk has some activity. For instance, there
                    // could be a CD-ROM drive that is not used.
                    bool hasSomeNonZeroStats = false;

                    for (size_t index = 0; partIt != string_split_iterator() && index < kDiskFieldCount;
                         ++partIt, ++index) {

                        StringData stringValue((*partIt).begin(), (*partIt).end());

                        uint64_t value;

                        if (!parseNumberFromString(stringValue, &value).isOK()) {
                            value = 0;
                        }

                        if (value != 0) {
                            hasSomeNonZeroStats = true;
                        }

                        stats.push_back(value);
                    }

                    if (hasSomeNonZeroStats) {
                        // Start a new document with disk as the name.
                        BSONObjBuilder sub(builder->subobjStart(diskStr));

                        for (size_t index = 0; index < stats.size() && index < kDiskFieldCount; ++index) {
                            sub.appendNumber(kDiskFields[index], static_cast<long long>(stats[index]));
                        }

                        sub.doneFast();
                    }
                }
            }

            return foundKeys ? Status::OK()
                             : Status(ErrorCodes::NoSuchKey, "Failed to find any keys in diskstats string");
        }

        Status parseProcDiskStatsFileByDevNo(StringData filename, 
                                             const std::vector<StringData>& disks, 
                                             BSONObjBuilder* builder) {
            auto swString = readFileAsString(filename);
            if (!swString.isOK()) {
                return swString.getStatus();
            }
            return parseProcDiskStatsByDevNo(disks, swString.getValue(), builder, true);
        }
    }
#endif

    inline std::string rocksGetNextPrefix(const rocksdb::Slice& prefix) {
        // next prefix lexicographically, assume same length
        std::string nextPrefix(prefix.data(), prefix.size());
        for (int i = static_cast<int>(nextPrefix.size()) - 1; i >= 0; --i) {
            nextPrefix[i]++;
            // if it's == 0, that means we've overflowed, so need to keep adding
            if (nextPrefix[i] != 0) {
                break;
            }
        }
        return nextPrefix;
    }

    std::string encodePrefix(uint32_t prefix);
    bool extractPrefix(const rocksdb::Slice& slice, uint32_t* prefix);
    int get_internal_delete_skipped_count();

    Status rocksToMongoStatus_slow(const rocksdb::Status& status, const char* prefix);

    /**
     * converts rocksdb status to mongodb status
     */
    inline Status rocksToMongoStatus(const rocksdb::Status& status, const char* prefix = NULL) {
        if (MONGO_likely(status.ok())) {
            return Status::OK();
        }
        return rocksToMongoStatus_slow(status, prefix);
    }

#define invariantRocksOK(expression)                                                               \
    do {                                                                                           \
        auto _invariantRocksOK_status = expression;                                                \
        if (MONGO_unlikely(!_invariantRocksOK_status.ok())) {                                      \
            invariantOKFailed(#expression, rocksToMongoStatus(_invariantRocksOK_status), __FILE__, \
                              __LINE__);                                                           \
        }                                                                                          \
    } while (false)

#define checkRocks

}  // namespace mongo
