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

#include "rocks_index.h"

#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include "mongo/base/checked_cast.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/storage/index_entry_comparison.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

#include "rocks_engine.h"
#include "rocks_record_store.h"
#include "rocks_recovery_unit.h"
#include "rocks_util.h"

namespace mongo {

    using std::string;
    using std::stringstream;
    using std::vector;

    namespace {
        static const int kKeyStringV0Version = 0;
        static const int kKeyStringV1Version = 1;
        static const int kMinimumIndexVersion = kKeyStringV0Version;
        static const int kMaximumIndexVersion = kKeyStringV1Version;

        /**
         * Strips the field names from a BSON object
         */
        BSONObj stripFieldNames( const BSONObj& obj ) {
            BSONObjBuilder b;
            BSONObjIterator i( obj );
            while ( i.more() ) {
                BSONElement e = i.next();
                b.appendAs( e, "" );
            }
            return b.obj();
        }

        string dupKeyError(const BSONObj& key, const std::string& collectionNamespace,
                           const std::string& indexName) {
            stringstream ss;
            ss << "E11000 duplicate key error";
            ss << " collection: " << collectionNamespace;
            ss << " index: " << indexName;
            ss << " dup key: " << key.toString();
            return ss.str();
        }

        const int kTempKeyMaxSize = 1024;  // Do the same as the heap implementation

        Status checkKeySize(const BSONObj& key) {
            if (key.objsize() >= kTempKeyMaxSize) {
                string msg = mongoutils::str::stream()
                             << "RocksIndex::insert: key too large to index, failing " << ' '
                             << key.objsize() << ' ' << key;
                return Status(ErrorCodes::KeyTooLong, msg);
            }
            return Status::OK();
        }

        /**
         * Functionality shared by both unique and standard index
         */
        class RocksCursorBase : public SortedDataInterface::Cursor {
        public:
            RocksCursorBase(OperationContext* txn, rocksdb::DB* db, std::string prefix,
                            bool forward, Ordering order, KeyString::Version keyStringVersion)
                : _db(db),
                  _prefix(prefix),
                  _forward(forward),
                  _order(order),
                  _keyStringVersion(keyStringVersion),
                  _key(keyStringVersion),
                  _typeBits(keyStringVersion),
                  _query(keyStringVersion),
                  _txn(txn) {
                _currentSequenceNumber = RocksRecoveryUnit::getRocksRecoveryUnit(txn)->snapshot()
                    ->GetSequenceNumber();
            }

            boost::optional<IndexKeyEntry> next(RequestedInfo parts) override {
                // Advance on a cursor at the end is a no-op
                if (_eof) {
                    return {};
                }
                if (!_lastMoveWasRestore) {
                    advanceCursor();
                }
                updatePosition();
                return curr(parts);
            }

            void setEndPosition(const BSONObj& key, bool inclusive) override {
                if (key.isEmpty()) {
                    // This means scan to end of index.
                    _endPosition.reset();
                    return;
                }

                // NOTE: this uses the opposite rules as a normal seek because a forward scan should
                // end after the key if inclusive and before if exclusive.
                const auto discriminator = _forward == inclusive ? KeyString::kExclusiveAfter
                                                                 : KeyString::kExclusiveBefore;
                _endPosition = stdx::make_unique<KeyString>(_keyStringVersion);
                _endPosition->resetToKey(stripFieldNames(key), _order, discriminator);
            }

            boost::optional<IndexKeyEntry> seek(const BSONObj& key, bool inclusive,
                                                RequestedInfo parts) override {
                const BSONObj finalKey = stripFieldNames(key);

                const auto discriminator = _forward == inclusive ? KeyString::kExclusiveBefore
                    : KeyString::kExclusiveAfter;

                // By using a discriminator other than kInclusive, there is no need to distinguish
                // unique vs non-unique key formats since both start with the key.
                _query.resetToKey(finalKey, _order, discriminator);

                seekCursor(_query);
                updatePosition();
                return curr(parts);
            }

            boost::optional<IndexKeyEntry> seek(const IndexSeekPoint& seekPoint,
                                                RequestedInfo parts) override {
                // make a key representing the location to which we want to advance.
                BSONObj key = IndexEntryComparison::makeQueryObject(seekPoint, _forward);

                // makeQueryObject handles the discriminator in the real exclusive cases.
                const auto discriminator = _forward ? KeyString::kExclusiveBefore
                                                    : KeyString::kExclusiveAfter;
                _query.resetToKey(key, _order, discriminator);
                seekCursor(_query);
                updatePosition();
                return curr(parts);
            }

            void save() override {
                if (!_lastMoveWasRestore) {
                    _savedEOF = _eof;
                }
            }

            void saveUnpositioned() override {
                _savedEOF = true;
            }

            void restore() override {
                auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(_txn);
                if (!_iterator.get() ||
                    _currentSequenceNumber != ru->snapshot()->GetSequenceNumber()) {
                    _iterator.reset(ru->NewIterator(_prefix));
                    _currentSequenceNumber = ru->snapshot()->GetSequenceNumber();

                    if (!_savedEOF) {
                        _lastMoveWasRestore = !seekCursor(_key);
                    }
                }
            }

            void detachFromOperationContext() final {
                _txn = nullptr;
                _iterator.reset();
            }

            void reattachToOperationContext(OperationContext* txn) final {
                _txn = txn;
                // iterator recreated in restore()
            }

        protected:
            // Called after _key has been filled in. Must not throw WriteConflictException.
            virtual void updateLocAndTypeBits() = 0;

            boost::optional<IndexKeyEntry> curr(RequestedInfo parts) const {
                if (_eof) {
                    return {};
                }

                BSONObj bson;
                if (parts & kWantKey) {
                    bson =  KeyString::toBson(_key.getBuffer(), _key.getSize(), _order, _typeBits);
                }

                return {{std::move(bson), _loc}};
            }

            void advanceCursor() {
                if (_eof) {
                    return;
                }
                if (_iterator.get() == nullptr) {
                    _iterator.reset(RocksRecoveryUnit::getRocksRecoveryUnit(_txn)
                            ->NewIterator(_prefix));
                    _iterator->SeekPrefix(rocksdb::Slice(_key.getBuffer(), _key.getSize()));
                    // advanceCursor() should only ever be called in states where the above seek
                    // will succeed in finding the exact key
                    invariant(_iterator->Valid());
                }
                if (_forward) {
                    _iterator->Next();
                } else {
                    _iterator->Prev();
                }
                _updateOnIteratorValidity();
            }

            // Seeks to query. Returns true on exact match.
            bool seekCursor(const KeyString& query) {
                auto * iter = iterator();
                const rocksdb::Slice keySlice(query.getBuffer(), query.getSize());
                iter->Seek(keySlice);
                if (!_updateOnIteratorValidity()) {
                    if (!_forward) {
                        // this will give lower bound behavior for backwards
                        iter->SeekToLast();
                        _updateOnIteratorValidity();
                    }
                    return false;
                }

                if (iter->key() == keySlice) {
                    return true;
                }

                if (!_forward) {
                    // if we can't find the exact result going backwards, we
                    // need to call Prev() so that we're at the first value
                    // less than (to the left of) what we were searching for,
                    // rather than the first value greater than (to the right
                    // of) the value we were searching for.
                    iter->Prev();
                    _updateOnIteratorValidity();
                }

                return false;
            }

            void updatePosition() {
                _lastMoveWasRestore = false;
                if (_eof) {
                    _loc = RecordId();
                    return;
                }

                if (_iterator.get() == nullptr) {
                    // _iterator is out of position because we just did a seekExact
                    _key.resetFromBuffer(_query.getBuffer(), _query.getSize());
                } else {
                    auto key = _iterator->key();
                    _key.resetFromBuffer(key.data(), key.size());
                }

                if (_endPosition) {
                    int cmp = _key.compare(*_endPosition);
                    if (_forward ? cmp > 0 : cmp < 0) {
                        _eof = true;
                        return;
                    }
                }

                updateLocAndTypeBits();
            }

            // ensure that _iterator is initialized and return a pointer to it
            RocksIterator * iterator() {
                if (_iterator.get() == nullptr) {
                    _iterator.reset(RocksRecoveryUnit::getRocksRecoveryUnit(_txn)
                            ->NewIterator(_prefix));
                }
                return _iterator.get();
            }

            // Update _eof based on _iterator->Valid() and return _iterator->Valid()
            bool _updateOnIteratorValidity() {
                if (_iterator->Valid()) {
                    _eof = false;
                    return true;
                } else {
                    _eof = true;
                    invariantRocksOK(_iterator->status());
                    return false;
                }
            }

            rocksdb::Slice _valueSlice() {
                if (_iterator.get() == nullptr) {
                    return rocksdb::Slice(_value);
                }
                return rocksdb::Slice(_iterator->value());
            }

            rocksdb::DB* _db;                                       // not owned
            std::string _prefix;
            std::unique_ptr<RocksIterator> _iterator;
            const bool _forward;
            bool _lastMoveWasRestore = false;
            Ordering _order;

            // These are for storing savePosition/restorePosition state
            bool _savedEOF = false;
            RecordId _savedRecordId;
            rocksdb::SequenceNumber _currentSequenceNumber;

            KeyString::Version _keyStringVersion;
            KeyString _key;
            KeyString::TypeBits _typeBits;
            RecordId _loc;

            KeyString _query;

            std::unique_ptr<KeyString> _endPosition;

            bool _eof = false;
            OperationContext* _txn;

            // stores the value associated with the latest call to seekExact()
            std::string _value;
        };

        class RocksStandardCursor final : public RocksCursorBase {
        public:
            RocksStandardCursor(OperationContext* txn, rocksdb::DB* db, std::string prefix,
                                bool forward, Ordering order, KeyString::Version keyStringVersion)
                : RocksCursorBase(txn, db, prefix, forward, order, keyStringVersion) {
                iterator();
            }

            virtual void updateLocAndTypeBits() {
                _loc = KeyString::decodeRecordIdAtEnd(_key.getBuffer(), _key.getSize());
                BufReader br(_valueSlice().data(), _valueSlice().size());
                _typeBits.resetFromBuffer(&br);
            }
        };

        class RocksUniqueCursor final : public RocksCursorBase {
        public:
            RocksUniqueCursor(OperationContext* txn, rocksdb::DB* db, std::string prefix,
                              bool forward, Ordering order, KeyString::Version keyStringVersion)
                : RocksCursorBase(txn, db, prefix, forward, order, keyStringVersion) {}

            boost::optional<IndexKeyEntry> seekExact(const BSONObj& key,
                                                     RequestedInfo parts) override {
                _eof = false;
                _iterator.reset();

                std::string prefixedKey(_prefix);
                _query.resetToKey(stripFieldNames(key), _order);
                prefixedKey.append(_query.getBuffer(), _query.getSize());
                rocksdb::Status status = RocksRecoveryUnit::getRocksRecoveryUnit(_txn)
                    ->Get(prefixedKey, &_value);

                if (status.IsNotFound()) {
                    _eof = true;
                } else if (!status.ok()) {
                    invariantRocksOK(status);
                }
                updatePosition();
                return curr(parts);
            }

            void updateLocAndTypeBits() {
                // We assume that cursors can only ever see unique indexes in their "pristine"
                // state,
                // where no duplicates are possible. The cases where dups are allowed should hold
                // sufficient locks to ensure that no cursor ever sees them.
                BufReader br(_valueSlice().data(), _valueSlice().size());
                _loc = KeyString::decodeRecordId(&br);
                _typeBits.resetFromBuffer(&br);

                if (!br.atEof()) {
                    severe() << "Unique index cursor seeing multiple records for key "
                             << redact(curr(kWantKey)->key);
                    fassertFailed(28609);
                }
            }
        };

    } // namespace

    /**
     * Bulk builds a non-unique index.
     */
    class RocksIndexBase::StandardBulkBuilder : public SortedDataBuilderInterface {
    public:
        StandardBulkBuilder(RocksStandardIndex* index, OperationContext* txn) : _index(index),
                                                                                _txn(txn) {}

        Status addKey(const BSONObj& key, const RecordId& loc) {
            return _index->insert(_txn, key, loc, true);
        }

        void commit(bool mayInterrupt) {
            WriteUnitOfWork uow(_txn);
            uow.commit();
        }

    private:
        RocksStandardIndex* _index;
        OperationContext* _txn;
    };

    /**
     * Bulk builds a unique index.
     *
     * In order to support unique indexes in dupsAllowed mode this class only does an actual insert
     * after it sees a key after the one we are trying to insert. This allows us to gather up all
     * duplicate locs and insert them all together. This is necessary since bulk cursors can only
     * append data.
     */
    class RocksIndexBase::UniqueBulkBuilder : public SortedDataBuilderInterface {
    public:
        UniqueBulkBuilder(std::string prefix, Ordering ordering,
                          KeyString::Version keyStringVersion, std::string collectionNamespace,
                          std::string indexName, OperationContext* txn,
                          bool dupsAllowed)
            : _prefix(std::move(prefix)),
              _ordering(ordering),
              _keyStringVersion(keyStringVersion),
              _collectionNamespace(std::move(collectionNamespace)),
              _indexName(std::move(indexName)),
              _txn(txn),
              _dupsAllowed(dupsAllowed),
              _keyString(keyStringVersion) {}

        Status addKey(const BSONObj& newKey, const RecordId& loc) {
            Status s = checkKeySize(newKey);
            if (!s.isOK()) {
                return s;
            }

            const int cmp = newKey.woCompare(_key, _ordering);
            if (cmp != 0) {
                if (!_key.isEmpty()) { // _key.isEmpty() is only true on the first call to addKey().
                    invariant(cmp > 0); // newKey must be > the last key
                    // We are done with dups of the last key so we can insert it now.
                    doInsert();
                }
                invariant(_records.empty());
            }
            else {
                // Dup found!
                if (!_dupsAllowed) {
                    return Status(ErrorCodes::DuplicateKey, dupKeyError(newKey, _collectionNamespace, _indexName));
                }

                // If we get here, we are in the weird mode where dups are allowed on a unique
                // index, so add ourselves to the list of duplicate locs. This also replaces the
                // _key which is correct since any dups seen later are likely to be newer.
            }

            _key = newKey.getOwned();
            _keyString.resetToKey(_key, _ordering);
            _records.push_back(std::make_pair(loc, _keyString.getTypeBits()));

            return Status::OK();
        }

        void commit(bool mayInterrupt) {
            WriteUnitOfWork uow(_txn);
            if (!_records.empty()) {
                // This handles inserting the last unique key.
                doInsert();
            }
            uow.commit();
        }

    private:
        void doInsert() {
            invariant(!_records.empty());

            KeyString value(_keyStringVersion);
            for (size_t i = 0; i < _records.size(); i++) {
                value.appendRecordId(_records[i].first);
                // When there is only one record, we can omit AllZeros TypeBits. Otherwise they need
                // to be included.
                if (!(_records[i].second.isAllZeros() && _records.size() == 1)) {
                    value.appendTypeBits(_records[i].second);
                }
            }

            std::string prefixedKey(RocksIndexBase::_makePrefixedKey(_prefix, _keyString));
            rocksdb::Slice valueSlice(value.getBuffer(), value.getSize());

            auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(_txn);
            ru->writeBatch()->Put(prefixedKey, valueSlice);

            _records.clear();
        }

        std::string _prefix;
        Ordering _ordering;
        const KeyString::Version _keyStringVersion;
        std::string _collectionNamespace;
        std::string _indexName;
        OperationContext* _txn;
        const bool _dupsAllowed;
        BSONObj _key;
        KeyString _keyString;
        std::vector<std::pair<RecordId, KeyString::TypeBits>> _records;
    };

    /// RocksIndexBase

    RocksIndexBase::RocksIndexBase(rocksdb::DB* db, std::string prefix, std::string ident,
                                   Ordering order, const BSONObj& config)
        : _db(db),
          _prefix(prefix),
          _ident(std::move(ident)),
          _order(order)
    {
        uint64_t storageSize;
        std::string nextPrefix = rocksGetNextPrefix(_prefix);
        rocksdb::Range wholeRange(_prefix, nextPrefix);
        _db->GetApproximateSizes(&wholeRange, 1, &storageSize);
        _indexStorageSize.store(static_cast<long long>(storageSize), std::memory_order_relaxed);

        int indexFormatVersion = 0; // default
        if (config.hasField("index_format_version")) {
          indexFormatVersion = config.getField("index_format_version").numberInt();
        }

        if (indexFormatVersion < kMinimumIndexVersion ||
            indexFormatVersion > kMaximumIndexVersion) {
            Status indexVersionStatus(
                ErrorCodes::UnsupportedFormat,
                "Unrecognized index format -- you might want to upgrade MongoDB");
            fassertFailedWithStatusNoTrace(40264, indexVersionStatus);
        }

        _keyStringVersion = indexFormatVersion >= kKeyStringV1Version ? KeyString::Version::V1
                                                                      : KeyString::Version::V0;
    }

    void RocksIndexBase::fullValidate(OperationContext* txn, long long* numKeysOut,
                                      ValidateResults* fullResults) const {
        if (numKeysOut) {
            std::unique_ptr<SortedDataInterface::Cursor> cursor(newCursor(txn, 1));

            *numKeysOut = 0;
            const auto requestedInfo = Cursor::kJustExistance;
            for (auto entry = cursor->seek(BSONObj(), true, requestedInfo);
                    entry; entry = cursor->next(requestedInfo)) {
                (*numKeysOut)++;
            }
        }
    }

    bool RocksIndexBase::isEmpty(OperationContext* txn) {
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(txn);
        std::unique_ptr<rocksdb::Iterator> it(ru->NewIterator(_prefix));

        it->SeekToFirst();
        return !it->Valid();
    }

    Status RocksIndexBase::initAsEmpty(OperationContext* txn) {
        // no-op
        return Status::OK();
    }

    long long RocksIndexBase::getSpaceUsedBytes(OperationContext* txn) const {
        // There might be some bytes in the WAL that we don't count here. Some
        // tests depend on the fact that non-empty indexes have non-zero sizes
        return static_cast<long long>(
            std::max(_indexStorageSize.load(std::memory_order_relaxed), static_cast<long long>(1)));
    }

    void RocksIndexBase::generateConfig(BSONObjBuilder* configBuilder, int formatVersion,
                                        IndexDescriptor::IndexVersion descVersion) {
        if (formatVersion >= 3 && descVersion >= IndexDescriptor::IndexVersion::kV2) {
          configBuilder->append("index_format_version", static_cast<int32_t>(kMaximumIndexVersion));
        } else {
          // keep it backwards compatible
          configBuilder->append("index_format_version", static_cast<int32_t>(kMinimumIndexVersion));
        }
    }

    std::string RocksIndexBase::_makePrefixedKey(const std::string& prefix,
                                                 const KeyString& encodedKey) {
        std::string key(prefix);
        key.append(encodedKey.getBuffer(), encodedKey.getSize());
        return key;
    }

    /// RocksUniqueIndex

    RocksUniqueIndex::RocksUniqueIndex(rocksdb::DB* db, std::string prefix, std::string ident,
                                       Ordering order, const BSONObj& config,
                                       std::string collectionNamespace, std::string indexName,
                                       bool partial)
        : RocksIndexBase(db, prefix, ident, order, config),
          _collectionNamespace(std::move(collectionNamespace)),
          _indexName(std::move(indexName)),
          _partial(partial) {}

    Status RocksUniqueIndex::insert(OperationContext* txn, const BSONObj& key, const RecordId& loc,
                                    bool dupsAllowed) {
        Status s = checkKeySize(key);
        if (!s.isOK()) {
            return s;
        }

        KeyString encodedKey(_keyStringVersion, key, _order);
        std::string prefixedKey(_makePrefixedKey(_prefix, encodedKey));

        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(txn);
        if (!ru->transaction()->registerWrite(prefixedKey)) {
            throw WriteConflictException();
        }

        _indexStorageSize.fetch_add(static_cast<long long>(prefixedKey.size()),
                                    std::memory_order_relaxed);

        std::string currentValue;
        auto getStatus = ru->Get(prefixedKey, &currentValue);
        if (!getStatus.ok() && !getStatus.IsNotFound()) {
            return rocksToMongoStatus(getStatus);
        } else if (getStatus.IsNotFound()) {
            // nothing here. just insert the value
            KeyString value(_keyStringVersion, loc);
            if (!encodedKey.getTypeBits().isAllZeros()) {
                value.appendTypeBits(encodedKey.getTypeBits());
            }
            rocksdb::Slice valueSlice(value.getBuffer(), value.getSize());
            ru->writeBatch()->Put(prefixedKey, valueSlice);
            return Status::OK();
        }

        // we are in a weird state where there might be multiple values for a key
        // we put them all in the "list"
        // Note that we can't omit AllZeros when there are multiple locs for a value. When we remove
        // down to a single value, it will be cleaned up.

        bool insertedLoc = false;
        KeyString valueVector(_keyStringVersion);
        BufReader br(currentValue.data(), currentValue.size());
        while (br.remaining()) {
            RecordId locInIndex = KeyString::decodeRecordId(&br);
            if (loc == locInIndex) {
                return Status::OK();  // already in index
            }

            if (!insertedLoc && loc < locInIndex) {
                valueVector.appendRecordId(loc);
                valueVector.appendTypeBits(encodedKey.getTypeBits());
                insertedLoc = true;
            }

            // Copy from old to new value
            valueVector.appendRecordId(locInIndex);
            valueVector.appendTypeBits(KeyString::TypeBits::fromBuffer(_keyStringVersion, &br));
        }

        if (!dupsAllowed) {
            return Status(ErrorCodes::DuplicateKey, dupKeyError(key, _collectionNamespace, _indexName));
        }

        if (!insertedLoc) {
            // This loc is higher than all currently in the index for this key
            valueVector.appendRecordId(loc);
            valueVector.appendTypeBits(encodedKey.getTypeBits());
        }

        rocksdb::Slice valueVectorSlice(valueVector.getBuffer(), valueVector.getSize());
        ru->writeBatch()->Put(prefixedKey, valueVectorSlice);
        return Status::OK();
    }

    void RocksUniqueIndex::unindex(OperationContext* txn, const BSONObj& key, const RecordId& loc,
                                   bool dupsAllowed) {
        // When DB parameter failIndexKeyTooLong is set to false,
        // this method may be called for non-existing
        // keys with the length exceeding the maximum allowed.
        // Since such keys cannot be in the storage in any case,
        // executing the following code results in:
        // - corruption of index storage size value, and
        // - an attempt to single-delete non-existing key which may
        //   potentially lead to consecutive single-deletion of the key.
        // Filter out long keys to prevent the problems described.
        if (!checkKeySize(key).isOK()) {
            return;
        }

        KeyString encodedKey(_keyStringVersion, key, _order);
        std::string prefixedKey(_makePrefixedKey(_prefix, encodedKey));

        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(txn);
        // We can't let two threads unindex the same key
        if (!ru->transaction()->registerWrite(prefixedKey)) {
            throw WriteConflictException();
        }

        if (!dupsAllowed) {
            if (_partial) {
                // Check that the record id matches.  We may be called to unindex records that are
                // not present in the index due to the partial filter expression.
                std::string val;
                auto s = ru->Get(prefixedKey, &val);
                if (s.IsNotFound()) {
                    return;
                }
                BufReader br(val.data(), val.size());
                fassert(90416, br.remaining());
                if (KeyString::decodeRecordId(&br) != loc) {
                    return;
                }
                // Ensure there aren't any other values in here.
                KeyString::TypeBits::fromBuffer(_keyStringVersion, &br);
                fassert(90417, !br.remaining());
            }
            _indexStorageSize.fetch_sub(static_cast<long long>(prefixedKey.size()),
                                        std::memory_order_relaxed);
            ru->writeBatch()->Delete(prefixedKey);
            return;
        }

        // dups are allowed, so we have to deal with a vector of RecordIds.
        std::string currentValue;
        auto getStatus = ru->Get(prefixedKey, &currentValue);
        if (getStatus.IsNotFound()) {
            return;
        }
        invariantRocksOK(getStatus);

        bool foundLoc = false;
        std::vector<std::pair<RecordId, KeyString::TypeBits>> records;

        BufReader br(currentValue.data(), currentValue.size());
        while (br.remaining()) {
            RecordId locInIndex = KeyString::decodeRecordId(&br);
            KeyString::TypeBits typeBits = KeyString::TypeBits::fromBuffer(_keyStringVersion, &br);

            if (loc == locInIndex) {
                if (records.empty() && !br.remaining()) {
                    // This is the common case: we are removing the only loc for this key.
                    // Remove the whole entry.
                    _indexStorageSize.fetch_sub(static_cast<long long>(prefixedKey.size()),
                                                std::memory_order_relaxed);
                    ru->writeBatch()->Delete(prefixedKey);
                    return;
                }

                foundLoc = true;
                continue;
            }

            records.push_back(std::make_pair(locInIndex, typeBits));
        }

        if (!foundLoc) {
            warning().stream() << loc << " not found in the index for key " << redact(key);
            return; // nothing to do
        }

        // Put other locs for this key back in the index.
        KeyString newValue(_keyStringVersion);
        invariant(!records.empty());
        for (size_t i = 0; i < records.size(); i++) {
            newValue.appendRecordId(records[i].first);
            // When there is only one record, we can omit AllZeros TypeBits. Otherwise they need
            // to be included.
            if (!(records[i].second.isAllZeros() && records.size() == 1)) {
                newValue.appendTypeBits(records[i].second);
            }
        }

        rocksdb::Slice newValueSlice(newValue.getBuffer(), newValue.getSize());
        ru->writeBatch()->Put(prefixedKey, newValueSlice);
        _indexStorageSize.fetch_sub(static_cast<long long>(prefixedKey.size()),
                                    std::memory_order_relaxed);
    }

    std::unique_ptr<SortedDataInterface::Cursor> RocksUniqueIndex::newCursor(OperationContext* txn,
                                                                             bool forward) const {
        return stdx::make_unique<RocksUniqueCursor>(txn, _db, _prefix, forward, _order,
                                                    _keyStringVersion);
    }

    Status RocksUniqueIndex::dupKeyCheck(OperationContext* txn, const BSONObj& key,
                                         const RecordId& loc) {
        KeyString encodedKey(_keyStringVersion, key, _order);
        std::string prefixedKey(_makePrefixedKey(_prefix, encodedKey));

        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(txn);
        std::string value;
        auto getStatus = ru->Get(prefixedKey, &value);
        if (!getStatus.ok() && !getStatus.IsNotFound()) {
            return rocksToMongoStatus(getStatus);
        } else if (getStatus.IsNotFound()) {
            // not found, not duplicate key
            return Status::OK();
        }

        // If the key exists, check if we already have this loc at this key. If so, we don't
        // consider that to be a dup.
        BufReader br(value.data(), value.size());
        while (br.remaining()) {
            if (KeyString::decodeRecordId(&br) == loc) {
                return Status::OK();
            }

            KeyString::TypeBits::fromBuffer(_keyStringVersion,
                                            &br);  // Just calling this to advance reader.
        }
        return Status(ErrorCodes::DuplicateKey, dupKeyError(key, _collectionNamespace, _indexName));
    }

    SortedDataBuilderInterface* RocksUniqueIndex::getBulkBuilder(OperationContext* txn,
                                                                 bool dupsAllowed) {
        return new RocksIndexBase::UniqueBulkBuilder(_prefix, _order, _keyStringVersion,
                                                     _collectionNamespace, _indexName, txn,
                                                     dupsAllowed);
    }

    /// RocksStandardIndex
    RocksStandardIndex::RocksStandardIndex(rocksdb::DB* db, std::string prefix, std::string ident,
                                           Ordering order, const BSONObj& config)
        : RocksIndexBase(db, prefix, ident, order, config),
          useSingleDelete(false) {}

    Status RocksStandardIndex::insert(OperationContext* txn, const BSONObj& key,
                                      const RecordId& loc, bool dupsAllowed) {
        invariant(dupsAllowed);
        Status s = checkKeySize(key);
        if (!s.isOK()) {
            return s;
        }

        KeyString encodedKey(_keyStringVersion, key, _order, loc);
        std::string prefixedKey(_makePrefixedKey(_prefix, encodedKey));
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(txn);
        if (!ru->transaction()->registerWrite(prefixedKey)) {
            throw WriteConflictException();
        }

        rocksdb::Slice value;
        if (!encodedKey.getTypeBits().isAllZeros()) {
            value =
                rocksdb::Slice(reinterpret_cast<const char*>(encodedKey.getTypeBits().getBuffer()),
                               encodedKey.getTypeBits().getSize());
        }

        _indexStorageSize.fetch_add(static_cast<long long>(prefixedKey.size()),
                                    std::memory_order_relaxed);

        ru->writeBatch()->Put(prefixedKey, value);

        return Status::OK();
    }

    void RocksStandardIndex::unindex(OperationContext* txn, const BSONObj& key, const RecordId& loc,
                                     bool dupsAllowed) {
        invariant(dupsAllowed);
        // When DB parameter failIndexKeyTooLong is set to false,
        // this method may be called for non-existing
        // keys with the length exceeding the maximum allowed.
        // Since such keys cannot be in the storage in any case,
        // executing the following code results in:
        // - corruption of index storage size value, and
        // - an attempt to single-delete non-existing key which may
        //   potentially lead to consecutive single-deletion of the key.
        // Filter out long keys to prevent the problems described.
        if (!checkKeySize(key).isOK()) {
            return;
        }

        KeyString encodedKey(_keyStringVersion, key, _order, loc);
        std::string prefixedKey(_makePrefixedKey(_prefix, encodedKey));

        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(txn);
        if (!ru->transaction()->registerWrite(prefixedKey)) {
            throw WriteConflictException();
        }

        _indexStorageSize.fetch_sub(static_cast<long long>(prefixedKey.size()),
                                    std::memory_order_relaxed);
        if (useSingleDelete) {
            ru->writeBatch()->SingleDelete(prefixedKey);
        } else {
            ru->writeBatch()->Delete(prefixedKey);
        }
    }

    std::unique_ptr<SortedDataInterface::Cursor> RocksStandardIndex::newCursor(
            OperationContext* txn,
            bool forward) const {
        return stdx::make_unique<RocksStandardCursor>(txn, _db, _prefix, forward, _order,
                                                      _keyStringVersion);
    }

    SortedDataBuilderInterface* RocksStandardIndex::getBulkBuilder(OperationContext* txn,
                                                                   bool dupsAllowed) {
        invariant(dupsAllowed);
        return new RocksIndexBase::StandardBulkBuilder(this, txn);
    }

}  // namespace mongo
