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

#ifdef __linux__
#include <rocksdb/rate_limiter.h>
#include "mongo/bson/bsonobjbuilder.h"

namespace mongo {

const uint64_t kMinMongoRateLimitRequestTokens = 100;
const uint64_t kInitMongoRateLimitRequestTokens = 1000000;

class MongoRateLimiter {
public:
    MongoRateLimiter(rocksdb::RateLimiter* rateLimiter) 
        : _rateLimiter(rateLimiter), _requestTokens(0) {}
    virtual ~MongoRateLimiter() {}

    virtual void resetRequestTokens() {
        _requestTokens.store(0, std::memory_order_relaxed);
    }
    virtual int64_t getRequestTokens() {
        return _requestTokens.load(std::memory_order_relaxed);
    }
    virtual void resetTokensPerSecond(int64_t tokens_per_second) {
        _rateLimiter->SetBytesPerSecond(tokens_per_second);
    }
    virtual int64_t getTokensPerSecond() {
        return _rateLimiter->GetBytesPerSecond();
    }
    virtual void request(const int64_t bytes) {
        auto requestTokens = _requestTokens.load(std::memory_order_relaxed);
        _requestTokens.store(requestTokens + bytes, std::memory_order_relaxed);
        _rateLimiter->Request(bytes, rocksdb::Env::IOPriority::IO_HIGH);
    }

private:
    std::unique_ptr<rocksdb::RateLimiter> _rateLimiter;
    std::atomic<int64_t> _requestTokens;
};

struct DiskStats {
    DiskStats() : micros(0), reads(0), writes(0), read_sectors(0), write_sectors(0) {}
    DiskStats(const BSONObj& diskStatsObj) {
        micros = curTimeMicros64();
        reads = static_cast<uint64_t>(diskStatsObj.getField("reads").safeNumberLong());
        writes = static_cast<uint64_t>(diskStatsObj.getField("writes").safeNumberLong());
        read_sectors =
            static_cast<uint64_t>(diskStatsObj.getField("read_sectors").safeNumberLong());
        write_sectors =
            static_cast<uint64_t>(diskStatsObj.getField("write_sectors").safeNumberLong());
    }
    uint64_t micros;
    uint64_t reads;
    uint64_t writes;
    uint64_t read_sectors;
    uint64_t write_sectors;
};

MongoRateLimiter* getMongoRateLimiter();

void startMongoRateLimiterChecker();
}
#endif
