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

#include "rocks_parameters.h"
#include "rocks_util.h"

#include "mongo/logger/parse_log_component_settings.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

#include <rocksdb/status.h>
#include <rocksdb/cache.h>
#include <rocksdb/experimental.h>
#include <rocksdb/db.h>
#include <rocksdb/convenience.h>
#include <rocksdb/options.h>
#include <rocksdb/version.h>

namespace mongo {

    RocksRateLimiterServerParameter::RocksRateLimiterServerParameter(RocksEngine* engine)
        : ServerParameter(ServerParameterSet::getGlobal(), "rocksdbRuntimeConfigMaxWriteMBPerSec",
                          false, true),
          _engine(engine) {}

    void RocksRateLimiterServerParameter::append(OperationContext* txn, BSONObjBuilder& b,
                                                 const std::string& name) {
        b.append(name, _engine->getMaxWriteMBPerSec());
    }

    Status RocksRateLimiterServerParameter::set(const BSONElement& newValueElement) {
        if (!newValueElement.isNumber()) {
            return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be a number");
        }
        return _set(newValueElement.numberInt());
    }

    Status RocksRateLimiterServerParameter::setFromString(const std::string& str) {
        int num = 0;
        Status status = parseNumberFromString(str, &num);
        if (!status.isOK()) return status;
        return _set(num);
    }

    Status RocksRateLimiterServerParameter::_set(int newNum) {
        if (newNum <= 0) {
            return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be > 0");
        }
        log() << "RocksDB: changing rate limiter to " << newNum << "MB/s";
        _engine->setMaxWriteMBPerSec(newNum);

        return Status::OK();
    }

    RocksBackupServerParameter::RocksBackupServerParameter(RocksEngine* engine)
        : ServerParameter(ServerParameterSet::getGlobal(), "rocksdbBackup", false, true),
          _engine(engine) {}

    void RocksBackupServerParameter::append(OperationContext* txn, BSONObjBuilder& b,
                                            const std::string& name) {
        b.append(name, "");
    }

    Status RocksBackupServerParameter::set(const BSONElement& newValueElement) {
        auto str = newValueElement.str();
        if (str.size() == 0) {
            return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be a string");
        }
        return setFromString(str);
    }

    Status RocksBackupServerParameter::setFromString(const std::string& str) {
        return _engine->backup(str);
    }

    RocksCompactServerParameter::RocksCompactServerParameter(RocksEngine* engine)
        : ServerParameter(ServerParameterSet::getGlobal(), "rocksdbCompact", false, true),
          _engine(engine) {}

    void RocksCompactServerParameter::append(OperationContext* txn, BSONObjBuilder& b,
                                             const std::string& name) {
        b.append(name, "");
    }

    Status RocksCompactServerParameter::set(const BSONElement& newValueElement) {
        return setFromString("");
    }

    Status RocksCompactServerParameter::setFromString(const std::string& str) {
        _engine->getCompactionScheduler()->compactAll();
        return Status::OK();
    }

    RocksCacheSizeParameter::RocksCacheSizeParameter(RocksEngine* engine)
        : ServerParameter(ServerParameterSet::getGlobal(), "rocksdbRuntimeConfigCacheSizeGB", false,
                          true),
          _engine(engine) {}

    void RocksCacheSizeParameter::append(OperationContext* txn, BSONObjBuilder& b,
                                         const std::string& name) {
        const long long bytesInGB = 1024 * 1024 * 1024LL;
        long long cacheSizeInGB = _engine->getBlockCache()->GetCapacity() / bytesInGB;
        b.append(name, cacheSizeInGB);
    }

    Status RocksCacheSizeParameter::set(const BSONElement& newValueElement) {
        if (!newValueElement.isNumber()) {
            return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be a number");
        }
        return _set(newValueElement.numberInt());
    }

    Status RocksCacheSizeParameter::setFromString(const std::string& str) {
        int num = 0;
        Status status = parseNumberFromString(str, &num);
        if (!status.isOK()) return status;
        return _set(num);
    }

    Status RocksCacheSizeParameter::_set(int newNum) {
        if (newNum <= 0) {
            return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be > 0");
        }
        log() << "RocksDB: changing block cache size to " << newNum << "GB";
        const long long bytesInGB = 1024 * 1024 * 1024LL;
        size_t newSizeInBytes = static_cast<size_t>(newNum * bytesInGB);
        _engine->getBlockCache()->SetCapacity(newSizeInBytes);

        return Status::OK();
    }    
    
    RocksOptionsParameter::RocksOptionsParameter(RocksEngine* engine)
        : ServerParameter(ServerParameterSet::getGlobal(), "rocksdbOptions", false,
                          true),
          _engine(engine) {}

    void RocksOptionsParameter::append(OperationContext* txn, BSONObjBuilder& b,
                                         const std::string& name) {
        std::string columnOptions;
        std::string dbOptions;
        std::string fullOptionsStr;
        rocksdb::Options fullOptions = _engine->getDB()->GetOptions();
        rocksdb::Status s = GetStringFromColumnFamilyOptions(&columnOptions, fullOptions);
        if (!s.ok()) { // If we failed, append the error for the user to see.
            b.append(name, s.ToString()); 
            return;
        }
        
        fullOptionsStr.append(columnOptions);
        
        s = GetStringFromDBOptions(&dbOptions, fullOptions);
        if (!s.ok()) { // If we failed, append the error for the user to see.
            b.append(name, s.ToString()); 
            return;
        }
        
        fullOptionsStr.append(dbOptions);

        b.append(name, fullOptionsStr);
    }

    Status RocksOptionsParameter::set(const BSONElement& newValueElement) {        
        // In case the BSON element is not a string, the conversion will fail, 
        // raising an exception catched by the outer layer.
        // Which will generate an error message that looks like this:
        // wrong type for field (rocksdbOptions) 3 != 2        
        return setFromString(newValueElement.String());
    }

    Status RocksOptionsParameter::setFromString(const std::string& str) {
#if defined(ROCKSDB_MAJOR) && (ROCKSDB_MAJOR > 4 || (ROCKSDB_MAJOR == 4 && ROCKSDB_MINOR >= 13))        
        log() << "RocksDB: Attempting to apply settings: " << str;
        
        std::unordered_map<std::string, std::string> optionsMap;
        rocksdb::Status s = rocksdb::StringToMap(str, &optionsMap);
        if (!s.ok()) {
            return Status(ErrorCodes::BadValue, s.ToString());
        }
        
        s = _engine->getDB()->SetOptions(optionsMap);
        if (!s.ok()) {
            return Status(ErrorCodes::BadValue, s.ToString());
        }
        
        return Status::OK();
#else
        return Status(ErrorCodes::BadValue, "This action is supported for RocksDB 4.13 and up");
#endif
    }
}
