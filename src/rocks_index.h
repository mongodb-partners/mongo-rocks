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

#include "mongo/db/storage/sorted_data_interface.h"

#include <atomic>
#include <string>

#include <rocksdb/db.h>

#include "mongo/bson/ordering.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/storage/key_string.h"

#pragma once

namespace rocksdb {
    class DB;
    class ColumnFamilyHandle;
}

namespace mongo {

    class RocksRecoveryUnit;

    class RocksIndexBase : public SortedDataInterface {
        RocksIndexBase(const RocksIndexBase&) = delete;
        RocksIndexBase& operator=(const RocksIndexBase&) = delete;

    public:
        RocksIndexBase(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, std::string prefix,
                       std::string ident, Ordering order, const BSONObj& config);

        virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext* opCtx,
                                                           bool dupsAllowed) = 0;

        virtual void fullValidate(OperationContext* opCtx, long long* numKeysOut,
                                  ValidateResults* fullResults) const;

        virtual bool appendCustomStats(OperationContext* /* opCtx */, BSONObjBuilder* /* output */,
                                       double /* scale */) const {
            // nothing to say here, really
            return false;
        }

        virtual bool isEmpty(OperationContext* opCtx);

        virtual long long getSpaceUsedBytes(OperationContext* opCtx) const;

        virtual Status initAsEmpty(OperationContext* opCtx);

        static void generateConfig(BSONObjBuilder* configBuilder, int formatVersion,
                                   IndexDescriptor::IndexVersion descVersion);

    protected:
        static std::string _makePrefixedKey(const std::string& prefix, const KeyString& encodedKey);

        rocksdb::DB* _db;  // not owned

        rocksdb::ColumnFamilyHandle* _cf; // not owned

        // Each key in the index is prefixed with _prefix
        std::string _prefix;
        std::string _ident;

        // very approximate index storage size
        std::atomic<long long> _indexStorageSize;

        // used to construct RocksCursors
        const Ordering _order;
        KeyString::Version _keyStringVersion;

        class StandardBulkBuilder;
        class UniqueBulkBuilder;
        friend class UniqueBulkBuilder;
    };

    class RocksUniqueIndex : public RocksIndexBase {
    public:
        RocksUniqueIndex(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, std::string prefix,
                         std::string ident, Ordering order, const BSONObj& config,
                         std::string collectionNamespace, std::string indexName,
                         const BSONObj& keyPattern, bool partial = false, bool isIdIdx = false);

        virtual StatusWith<SpecialFormatInserted> insert(OperationContext* opCtx,
                                                         const BSONObj& key, const RecordId& loc,
                                                         bool dupsAllowed);
        virtual void unindex(OperationContext* opCtx, const BSONObj& key, const RecordId& loc,
                             bool dupsAllowed);
        virtual std::unique_ptr<SortedDataInterface::Cursor> newCursor(OperationContext* opCtx,
                                                                       bool forward) const;

        virtual Status dupKeyCheck(OperationContext* opCtx, const BSONObj& key);

        virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext* opCtx,
                                                           bool dupsAllowed) override;

    private:
        StatusWith<SpecialFormatInserted> _insertTimestampSafe(OperationContext* opCtx,
                                                               const BSONObj& key,
                                                               const RecordId& loc,
                                                               bool dupsAllowed);

        StatusWith<SpecialFormatInserted> _insertTimestampUnsafe(OperationContext* opCtx,
                                                                 const BSONObj& key,
                                                                 const RecordId& loc,
                                                                 bool dupsAllowed);

        void _unindexTimestampUnsafe(OperationContext* opCtx, const BSONObj& key,
                                     const RecordId& loc, bool dupsAllowed);

        void _unindexTimestampSafe(OperationContext* opCtx, const BSONObj& key, const RecordId& loc,
                                   bool dupsAllowed);

        bool _keyExistsTimestampSafe(OperationContext* opCtx, const KeyString& prefixedKey);

        std::string _collectionNamespace;
        std::string _indexName;
        const BSONObj _keyPattern;
        const bool _partial;
        const bool _isIdIndex;
    };

    class RocksStandardIndex : public RocksIndexBase {
    public:
        RocksStandardIndex(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, std::string prefix,
                           std::string ident, Ordering order, const BSONObj& config);

        virtual StatusWith<SpecialFormatInserted> insert(OperationContext* opCtx,
                                                         const BSONObj& key, const RecordId& loc,
                                                         bool dupsAllowed);
        virtual void unindex(OperationContext* opCtx, const BSONObj& key, const RecordId& loc,
                             bool dupsAllowed);
        virtual std::unique_ptr<SortedDataInterface::Cursor> newCursor(OperationContext* opCtx,
                                                                       bool forward) const;
        virtual Status dupKeyCheck(OperationContext* opCtx, const BSONObj& key) {
            // dupKeyCheck shouldn't be called for non-unique indexes
            invariant(false);
            return Status::OK();
        }

        virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext* opCtx,
                                                           bool dupsAllowed) override;

        void enableSingleDelete() { useSingleDelete = true; }

    private:
        bool useSingleDelete;
    };

}  // namespace mongo
