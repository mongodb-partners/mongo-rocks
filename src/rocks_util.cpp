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


#include "rocks_util.h"

#include <string>
#include <rocksdb/status.h>

#include "mongo/platform/endian.h"

namespace mongo {
    std::string encodePrefix(uint32_t prefix) {
        uint32_t bigEndianPrefix = endian::nativeToBig(prefix);
        return std::string(reinterpret_cast<const char*>(&bigEndianPrefix), sizeof(uint32_t));
    }

    // we encode prefixes in big endian because we want to quickly jump to the max prefix
    // (iter->SeekToLast())
    bool extractPrefix(const rocksdb::Slice& slice, uint32_t* prefix) {
        if (slice.size() < sizeof(uint32_t)) {
            return false;
        }
        *prefix = endian::bigToNative(*reinterpret_cast<const uint32_t*>(slice.data()));
        return true;
    }

    Status rocksToMongoStatus_slow(const rocksdb::Status& status, const char* prefix) {
        if (status.ok()) {
            return Status::OK();
        }

        if (status.IsCorruption()) {
            return Status(ErrorCodes::BadValue, status.ToString());
        }

        return Status(ErrorCodes::InternalError, status.ToString());
    }

}  // namespace mongo
