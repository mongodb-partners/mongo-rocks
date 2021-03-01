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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kControl

#include "mongo_rate_limiter_checker.h"

#ifdef __linux__
#include "rocks_parameters.h"
#include "rocks_util.h"
#include "mongo/db/server_options.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/util/background.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"
#include "mongo/util/time_support.h"
#include "mongo/db/service_context.h"

namespace mongo {

/**
 * Thread for Mongo Rate Limiter Checker
 */
class MongoRateLimiterChecker : public BackgroundJob {
public:
    std::string name() const {
        return "MongoRateLimiterChecker";
    }

    Status init() {
        log() << "[Mongo Rate Limiter Checker]: inited: "
              << "disk is " << getMongoRateLimitParameter().getDisk() << "; " 
              << "iops is " << getMongoRateLimitParameter().getIops() << "; " 
              << "mbps is " << getMongoRateLimitParameter().getMbps() << "; "; 
        if (getMongoRateLimitParameter().getDisk() == "") {
            return Status(ErrorCodes::InternalError, str::stream() << "disk is empty");
        }
        if (_mongoRateLimiter.get() == nullptr) {
            _mongoRateLimiter = std::make_unique<MongoRateLimiter>(rocksdb::NewGenericRateLimiter(kInitMongoRateLimitRequestTokens));
        }

        // read /proc/diskstats data 
        auto readStatus = readProcDiskStats(getMongoRateLimitParameter().getDisk());
        if (!readStatus.isOK()) {
            return Status(ErrorCodes::InternalError,
                          str::stream() << "failed to read /proc/diskstats: "
                                        << readStatus.getStatus().toString());
        }

        DiskStats currStats(readStatus.getValue().getField(getMongoRateLimitParameter().getDisk()).Obj());
        _prevStats = currStats;

        return Status::OK();
    }

    void run() {
        while (!globalInShutdownDeprecated()) {
            invariant(_mongoRateLimiter.get() != nullptr);

            // Check whether tokens is exhuasted or not
            _mongoRateLimiter->resetRequestTokens();
            int64_t requestTokensBefore = _mongoRateLimiter->getRequestTokens();
            sleepsecs(1);
            int64_t requestTokensAfter = _mongoRateLimiter->getRequestTokens();
            int64_t requestTokensGap = requestTokensAfter - requestTokensBefore;
            bool exhausted = requestTokensGap >= _mongoRateLimiter->getTokensPerSecond();

            // read /proc/diskstats data 
            auto readStatus = readProcDiskStats(getMongoRateLimitParameter().getDisk());
            if (!readStatus.isOK()) {
                log() << "[Mongo Rate Limiter Checker]: disk is " << getMongoRateLimitParameter().getDisk() << "; readStatus is "
                      << readStatus.getStatus().toString() << ";";
                break;
            }

            DiskStats currStats(readStatus.getValue().getField(getMongoRateLimitParameter().getDisk()).Obj());
            auto ratioStatus = calculateResetRatio(exhausted, currStats);
            if (ratioStatus.isOK()) {
                int64_t newTokens = static_cast<int64_t>(_mongoRateLimiter->getTokensPerSecond() *
                                                         ratioStatus.getValue());
                _mongoRateLimiter->resetTokensPerSecond(
                    std::max(newTokens, static_cast<int64_t>(kMinMongoRateLimitRequestTokens)));
            }

            _prevStats = currStats;
        }
    }

    StatusWith<BSONObj> readProcDiskStats(const std::string& targetDisk) {
        if (targetDisk.empty()) {
            return Status(ErrorCodes::InternalError, str::stream() << "disk info is empty");
        }

        std::vector<StringData> disks{targetDisk};
        BSONObjBuilder disksBuilder;
        auto status = parseProcDiskStatsFileByDevNo("/proc/diskstats", disks, &disksBuilder);
        if (!status.isOK()) {
            return Status(ErrorCodes::InternalError,
                          str::stream() << "failed to read /proc/diskstats: " << status.toString());
        }
        disksBuilder.doneFast();

        auto wholeDiskInfo = disksBuilder.obj();
        if (!wholeDiskInfo.hasField(targetDisk)) {
            return Status(ErrorCodes::InternalError,
                          str::stream() << "no disk(" << targetDisk << ") info: " << wholeDiskInfo);
        }
        return wholeDiskInfo;
    }

    StatusWith<double> calculateResetRatio(bool exhausted, DiskStats& currStats) {
        // NOTE: iops and kbps is the value for 1 second
        double ratio = 1000000 / static_cast<double>(currStats.micros - _prevStats.micros);
        uint64_t iops = 0;
        uint64_t kbps = 0;
        if (currStats.reads >= _prevStats.reads && currStats.writes >= _prevStats.writes) {
            iops = static_cast<uint64_t>(
                ((currStats.reads - _prevStats.reads) + (currStats.writes - _prevStats.writes)) *
                ratio);
        }
        if (currStats.read_sectors >= _prevStats.read_sectors &&
            currStats.write_sectors >= _prevStats.write_sectors) {
            // NOTE: the factor (divided by 2) is for that size of sector is 512 Bytes
            kbps = static_cast<uint64_t>(((currStats.read_sectors - _prevStats.read_sectors) +
                                          (currStats.write_sectors - _prevStats.write_sectors)) *
                                         ratio / 2);
        }
        if (iops == 0 || kbps == 0) {
            return Status(ErrorCodes::InternalError,
                          str::stream() << "do not reset: iops is " << iops << "; kbps is "
                                        << kbps);
        }
        double resetRatio = std::min(getMongoRateLimitParameter().getIops() / static_cast<double>(iops),
                                     (getMongoRateLimitParameter().getMbps() * 1024) / static_cast<double>(kbps));
        double actualResetRatio = std::max(0.9, std::min(resetRatio, 1.1));
        bool willReset =
            exhausted || iops >= getMongoRateLimitParameter().getIops() || kbps >= getMongoRateLimitParameter().getMbps() * 1024;
        LOG(1) << "[Mongo Rate Limiter Checker]: exhausted: " << exhausted
               << "; tokens: " << _mongoRateLimiter->getTokensPerSecond() << "; iops: " << iops
               << "/" << getMongoRateLimitParameter().getIops() << "; kbps: " << kbps << "/"
               << (getMongoRateLimitParameter().getMbps() * 1024) << "; resetRatio: " << resetRatio << "=>"
               << actualResetRatio << "; (" << (willReset ? "true" : "false") << ");";
        if (willReset) {
            return actualResetRatio;
        }
        return Status(ErrorCodes::InternalError,
                      str::stream() << "do not reset: not meet condition");
    }

    MongoRateLimiter* getMongoRateLimiter() {
        return _mongoRateLimiter.get();
    }
private:
    std::unique_ptr<MongoRateLimiter> _mongoRateLimiter;
    DiskStats _prevStats;
};

namespace {
// Only one instance of the MongoRateLimiterChecker exists
MongoRateLimiterChecker mongoRateLimiterChecker;
}

MongoRateLimiter* getMongoRateLimiter() {
    return mongoRateLimiterChecker.getMongoRateLimiter();
}

void startMongoRateLimiterChecker() {
    auto status = mongoRateLimiterChecker.init();
    if (!status.isOK()) {
        log() << "[Mongo Rate Limiter Checker]: init failed; status is " << status.toString() << ";";
        return;
    }
    mongoRateLimiterChecker.go();
}

}
#endif
