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
#include "mongo/util/str.h"

#include "rocks_engine.h"
#include "rocks_prepare_conflict.h"
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
        static const std::string emptyItem("");

        bool hasFieldNames(const BSONObj& obj) {
            BSONForEach(e, obj) {
                if (e.fieldName()[0]) return true;
            }
            return false;
        }

        /**
         * Strips the field names from a BSON object
         */
        BSONObj stripFieldNames(const BSONObj& obj) {
            if (!hasFieldNames(obj)) return obj;

            BSONObjBuilder b;
            BSONForEach(e, obj) { b.appendAs(e, StringData()); }
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
    }  // namespace

    /// RocksIndexBase

    void RocksIndexBase::fullValidate(OperationContext* opCtx, long long* numKeysOut,
                                      ValidateResults* fullResults) const {
        if (numKeysOut) {
            std::unique_ptr<SortedDataInterface::Cursor> cursor(newCursor(opCtx, 1));

            *numKeysOut = 0;
            const auto requestedInfo = Cursor::kJustExistance;
            for (auto entry = cursor->seek(BSONObj(), true, requestedInfo); entry;
                 entry = cursor->next(requestedInfo)) {
                (*numKeysOut)++;
            }
        }
    }

    Status RocksIndexBase::initAsEmpty(OperationContext* opCtx) {
        // no-op
        return Status::OK();
    }

    std::string RocksIndexBase::_makePrefixedKey(const std::string& prefix,
                                                 const KeyString& encodedKey) {
        std::string key(prefix);
        key.append(encodedKey.getBuffer(), encodedKey.getSize());
        return key;
    }
    /**
     * Bulk builds a non-unique index.
     */
    class RocksIndexBase::StandardBulkBuilder : public SortedDataBuilderInterface {
    public:
        StandardBulkBuilder(RocksStandardIndex* index, OperationContext* opCtx)
            : _index(index), _opCtx(opCtx) {}

        StatusWith<SpecialFormatInserted> addKey(const BSONObj& key, const RecordId& loc) {
            return _index->insert(_opCtx, key, loc, true);
        }

        SpecialFormatInserted commit(bool mayInterrupt) override {
            WriteUnitOfWork uow(_opCtx);
            uow.commit();
            return SpecialFormatInserted::NoSpecialFormatInserted;
        }

    private:
        RocksStandardIndex* _index;
        OperationContext* _opCtx;
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
        UniqueBulkBuilder(rocksdb::ColumnFamilyHandle* cf, std::string prefix, Ordering ordering,
                          KeyString::Version keyStringVersion, std::string collectionNamespace,
                          std::string indexName, OperationContext* opCtx, bool dupsAllowed,
                          const BSONObj& keyPattern, bool isIdIndex)
            : _cf(cf),
              _prefix(std::move(prefix)),
              _ordering(ordering),
              _keyStringVersion(keyStringVersion),
              _collectionNamespace(std::move(collectionNamespace)),
              _indexName(std::move(indexName)),
              _opCtx(opCtx),
              _dupsAllowed(dupsAllowed),
              _keyString(keyStringVersion),
              _keyPattern(keyPattern),
              _isIdIndex(isIdIndex) {}

        StatusWith<SpecialFormatInserted> addKey(const BSONObj& newKey, const RecordId& loc) {
            if (_isIdIndex) {
                return addKeyTimestampUnsafe(newKey, loc);
            } else {
                return addKeyTimestampSafe(newKey, loc);
            }
        }

        StatusWith<SpecialFormatInserted> addKeyTimestampSafe(const BSONObj& newKey,
                                                              const RecordId& loc) {
            // Do a duplicate check, but only if dups aren't allowed.
            if (!_dupsAllowed) {
                const int cmp = newKey.woCompare(_previousKey, _ordering);
                if (cmp == 0) {
                    // Duplicate found!
                    return buildDupKeyErrorStatus(newKey,
                                                  NamespaceString(StringData(_collectionNamespace)),
                                                  _indexName, _keyPattern);
                } else {
                    // _previousKey.isEmpty() is only true on the first call to addKey().
                    // newKey must be > the last key
                    invariant(_previousKey.isEmpty() || cmp > 0);
                }
            }

            _keyString.resetToKey(newKey, _ordering, loc);
            std::string prefixedKey(RocksIndexBase::_makePrefixedKey(_prefix, _keyString));
            std::string valueItem = _keyString.getTypeBits().isAllZeros()
                                        ? emptyItem
                                        : std::string(_keyString.getTypeBits().getBuffer(),
                                                      _keyString.getTypeBits().getSize());

            auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx);
            invariant(ru);
            auto transaction = ru->getTransaction();
            invariant(transaction);
            invariantRocksOK(transaction->Put(_cf, prefixedKey, valueItem));

            // Don't copy the key again if dups are allowed.
            if (!_dupsAllowed) {
                _previousKey = newKey.getOwned();
            }
            if (_keyString.getTypeBits().isLongEncoding())
                return StatusWith<SpecialFormatInserted>(
                    SpecialFormatInserted::LongTypeBitsInserted);

            return StatusWith<SpecialFormatInserted>(
                SpecialFormatInserted::NoSpecialFormatInserted);
        }

        StatusWith<SpecialFormatInserted> addKeyTimestampUnsafe(const BSONObj& newKey,
                                                                const RecordId& loc) {
            SpecialFormatInserted specialFormatInserted =
                SpecialFormatInserted::NoSpecialFormatInserted;

            const int cmp = newKey.woCompare(_previousKey, _ordering);
            if (cmp != 0) {
                if (!_previousKey.isEmpty()) {  // _previousKey.isEmpty() is only true on the first
                                                // call to addKey().
                    invariant(cmp > 0);  // newKey must be > the last key
                    // We are done with dups of the last key so we can insert it now.
                    specialFormatInserted = doInsert();
                }
                invariant(_records.empty());
            } else {
                // Dup found!
                if (!_dupsAllowed) {
                    return buildDupKeyErrorStatus(newKey,
                                                  NamespaceString(StringData(_collectionNamespace)),
                                                  _indexName, _keyPattern);
                }

                // If we get here, we are in the weird mode where dups are allowed on a unique
                // index, so add ourselves to the list of duplicate locs. This also replaces the
                // _previousKey which is correct since any dups seen later are likely to be newer.
            }

            _previousKey = newKey.getOwned();
            _keyString.resetToKey(_previousKey, _ordering);
            _records.push_back(std::make_pair(loc, _keyString.getTypeBits()));

            return StatusWith<SpecialFormatInserted>(
                SpecialFormatInserted::NoSpecialFormatInserted);
        }

        SpecialFormatInserted commit(bool mayInterrupt) override {
            SpecialFormatInserted specialFormatInserted =
                SpecialFormatInserted::NoSpecialFormatInserted;
            WriteUnitOfWork uow(_opCtx);
            if (!_records.empty()) {
                // This handles inserting the last unique key.
                specialFormatInserted = doInsert();
            }
            uow.commit();
            return specialFormatInserted;
        }

    private:
        SpecialFormatInserted doInsert() {
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

            auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx);
            invariant(ru);
            auto transaction = ru->getTransaction();
            invariant(transaction);
            invariantRocksOK(transaction->Put(_cf, prefixedKey, valueSlice));

            _records.clear();

            return SpecialFormatInserted::NoSpecialFormatInserted;
        }

        rocksdb::ColumnFamilyHandle* _cf;  // not owned
        std::string _prefix;
        Ordering _ordering;
        const KeyString::Version _keyStringVersion;
        std::string _collectionNamespace;
        std::string _indexName;
        OperationContext* _opCtx;
        const bool _dupsAllowed;
        BSONObj _previousKey;
        KeyString _keyString;
        BSONObj _keyPattern;
        const bool _isIdIndex;
        std::vector<std::pair<RecordId, KeyString::TypeBits>> _records;
    };

    namespace {
        /**
         * Functionality shared by both unique and standard index
         */
        class RocksCursorBase : public SortedDataInterface::Cursor {
        public:
            RocksCursorBase(OperationContext* opCtx, rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf,
                            std::string prefix,
                            bool forward, Ordering order, KeyString::Version keyStringVersion)
                : _db(db),
                  _cf(cf),
                  _prefix(prefix),
                  _forward(forward),
                  _order(order),
                  _keyStringVersion(keyStringVersion),
                  _key(keyStringVersion),
                  _typeBits(keyStringVersion),
                  _query(keyStringVersion),
                  _opCtx(opCtx) {}

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
                const auto discriminator =
                    _forward ? KeyString::kExclusiveBefore : KeyString::kExclusiveAfter;
                _query.resetToKey(key, _order, discriminator);
                seekCursor(_query);
                updatePosition();
                return curr(parts);
            }

            void save() override {
                try {
                    if (_iterator) {
                        _iterator.reset();
                    }
                } catch (const WriteConflictException&) {
                }
                if (!_lastMoveWasRestore) {
                    _savedEOF = _eof;
                }
            }

            void saveUnpositioned() override {
                save();
                _savedEOF = true;
            }

            void restore() override {
                if (!_iterator) {
                    auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx);
                    invariant(ru);
                    _iterator.reset(ru->NewIterator(_cf, _prefix));
                    invariant(_iterator);
                }

                // TODO(cuixin): rocks need an interface let iterator get txn
                // invariant(ru->getTransaction() == _iterator->getSession());

                if (!_savedEOF) {
                    _lastMoveWasRestore = !seekCursor(_key);
                }
            }

            void detachFromOperationContext() final {
                _opCtx = nullptr;
                _iterator.reset();
            }

            void reattachToOperationContext(OperationContext* opCtx) final {
                _opCtx = opCtx;
                // iterator recreated in restore()
            }

        protected:
            // Called after _key has been filled in. Must not throw WriteConflictException.
            virtual void updateLocAndTypeBits() {
                _loc = KeyString::decodeRecordIdAtEnd(_key.getBuffer(), _key.getSize());
                BufReader br(_valueSlice().data(), _valueSlice().size());
                _typeBits.resetFromBuffer(&br);
            }

            boost::optional<IndexKeyEntry> curr(RequestedInfo parts) const {
                if (_eof) {
                    return {};
                }

                BSONObj bson;
                if (parts & kWantKey) {
                    bson = KeyString::toBson(_key.getBuffer(), _key.getSize(), _order, _typeBits);
                }

                return {{std::move(bson), _loc}};
            }

            void advanceCursor() {
                if (_eof) {
                    return;
                }
                if (_iterator.get() == nullptr) {
                    _iterator.reset(
                        RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx)->NewIterator(_cf, _prefix));
                    rocksPrepareConflictRetry(_opCtx, [&] {
                        _iterator->SeekPrefix(rocksdb::Slice(_key.getBuffer(), _key.getSize()));
                        return _iterator->status();
                    });
                    // advanceCursor() should only ever be called in states where the above seek
                    // will succeed in finding the exact key
                    invariant(_iterator->Valid());
                }
                rocksPrepareConflictRetry(_opCtx, [&] {
                    _forward ? _iterator->Next() : _iterator->Prev();
                    return _iterator->status();
                });
                _updateOnIteratorValidity();
            }

            // Seeks to query. Returns true on exact match.
            bool seekCursor(const KeyString& query) {
                auto* iter = iterator();
                const rocksdb::Slice keySlice(query.getBuffer(), query.getSize());
                rocksPrepareConflictRetry(_opCtx, [&] {
                    iter->Seek(keySlice);
                    return iter->status();
                });
                if (!_updateOnIteratorValidity()) {
                    if (!_forward) {
                        // this will give lower bound behavior for backwards
                        rocksPrepareConflictRetry(_opCtx, [&] {
                            iter->SeekToLast();
                            return iter->status();
                        });
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
                    rocksPrepareConflictRetry(_opCtx, [&] {
                        iter->Prev();
                        return iter->status();
                    });
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
            RocksIterator* iterator() {
                if (_iterator.get() == nullptr) {
                    _iterator.reset(
                        RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx)->NewIterator(_cf, _prefix));
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

            rocksdb::DB* _db;  // not owned
            rocksdb::ColumnFamilyHandle* _cf;  // not owned
            std::string _prefix;
            std::unique_ptr<RocksIterator> _iterator;
            const bool _forward;
            bool _lastMoveWasRestore = false;
            Ordering _order;

            // These are for storing savePosition/restorePosition state
            bool _savedEOF = false;
            RecordId _savedRecordId;

            KeyString::Version _keyStringVersion;
            KeyString _key;
            KeyString::TypeBits _typeBits;
            RecordId _loc;

            KeyString _query;

            std::unique_ptr<KeyString> _endPosition;

            bool _eof = false;
            OperationContext* _opCtx;

            // stores the value associated with the latest call to seekExact()
            std::string _value;
        };

        class RocksStandardCursor final : public RocksCursorBase {
        public:
            RocksStandardCursor(OperationContext* opCtx, rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf,
                                std::string prefix,
                                bool forward, Ordering order, KeyString::Version keyStringVersion)
                : RocksCursorBase(opCtx, db, cf, prefix, forward, order, keyStringVersion) {
                iterator();
            }
        };

        class RocksUniqueCursor final : public RocksCursorBase {
        public:
            RocksUniqueCursor(OperationContext* opCtx, rocksdb::DB* db,
                              rocksdb::ColumnFamilyHandle* cf, std::string prefix, bool forward,
                              Ordering order, KeyString::Version keyStringVersion,
                              std::string indexName, bool isIdIndex)
                : RocksCursorBase(opCtx, db, cf, prefix, forward, order, keyStringVersion),
                  _indexName(std::move(indexName)),
                  _isIdIndex(isIdIndex) {}

            boost::optional<IndexKeyEntry> seekExact(const BSONObj& key,
                                                     RequestedInfo parts) override {
                if (!_isIdIndex) {
                    return RocksCursorBase::seekExact(key, parts);
                }
                _eof = false;
                _iterator.reset();

                std::string prefixedKey(_prefix);
                _query.resetToKey(stripFieldNames(key), _order);
                prefixedKey.append(_query.getBuffer(), _query.getSize());
                rocksdb::Status status = rocksPrepareConflictRetry(_opCtx, [&] {
                    return RocksRecoveryUnit::getRocksRecoveryUnit(_opCtx)->Get(_cf, prefixedKey, &_value);
                });

                if (status.IsNotFound()) {
                    _eof = true;
                } else if (!status.ok()) {
                    invariantRocksOK(status);
                }
                updatePosition();
                return curr(parts);
            }

            void updateLocAndTypeBits() {
                // _id indexes remain at the old format
                if (!_isIdIndex) {
                    RocksCursorBase::updateLocAndTypeBits();
                    return;
                }
                BufReader br(_valueSlice().data(), _valueSlice().size());
                _loc = KeyString::decodeRecordId(&br);
                _typeBits.resetFromBuffer(&br);

                if (!br.atEof()) {
                    severe() << "Unique index cursor seeing multiple records for key "
                             << redact(curr(kWantKey)->key) << " in index " << _indexName;
                    fassertFailed(28609);
                }
            }

        private:
            std::string _indexName;
            const bool _isIdIndex;
        };
    }  // namespace

    /// RocksIndexBase
    RocksIndexBase::RocksIndexBase(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf,
                                   std::string prefix, std::string ident, Ordering order,
                                   const BSONObj& config)
        : _db(db), _cf(cf), _prefix(prefix), _ident(std::move(ident)), _order(order) {
        uint64_t storageSize;
        std::string nextPrefix = rocksGetNextPrefix(_prefix);
        rocksdb::Range wholeRange(_prefix, nextPrefix);
        _db->GetApproximateSizes(&wholeRange, 1, &storageSize);
        _indexStorageSize.store(static_cast<long long>(storageSize), std::memory_order_relaxed);

        int indexFormatVersion = 0;  // default
        if (config.hasField("index_format_version")) {
            indexFormatVersion = config.getField("index_format_version").numberInt();
        }

        if (indexFormatVersion < kMinimumIndexVersion ||
            indexFormatVersion > kMaximumIndexVersion) {
            Status indexVersionStatus(
                ErrorCodes::UnsupportedFormat,
                "Unrecognized index format -- you might want to upgrade MongoDB");
            fassertFailedWithStatusNoTrace(ErrorCodes::InternalError, indexVersionStatus);
        }

        _keyStringVersion = indexFormatVersion >= kKeyStringV1Version ? KeyString::Version::V1
                                                                      : KeyString::Version::V0;
    }

    bool RocksIndexBase::isEmpty(OperationContext* opCtx) {
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        std::unique_ptr<rocksdb::Iterator> it(ru->NewIterator(_cf, _prefix));

        auto s = rocksPrepareConflictRetry(opCtx, [&] {
            it->SeekToFirst();
            return it->status();
        });
        return !it->Valid();
    }

    long long RocksIndexBase::getSpaceUsedBytes(OperationContext* opCtx) const {
        // There might be some bytes in the WAL that we don't count here. Some
        // tests depend on the fact that non-empty indexes have non-zero sizes
        return static_cast<long long>(
            std::max(_indexStorageSize.load(std::memory_order_relaxed), static_cast<long long>(1)));
    }

    void RocksIndexBase::generateConfig(BSONObjBuilder* configBuilder, int formatVersion,
                                        IndexDescriptor::IndexVersion descVersion) {
        if (formatVersion >= 3 && descVersion >= IndexDescriptor::IndexVersion::kV2) {
            configBuilder->append("index_format_version",
                                  static_cast<int32_t>(kMaximumIndexVersion));
        } else {
            // keep it backwards compatible
            configBuilder->append("index_format_version",
                                  static_cast<int32_t>(kMinimumIndexVersion));
        }
    }

    /// RocksUniqueIndex

    RocksUniqueIndex::RocksUniqueIndex(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf,
                                       std::string prefix, std::string ident, Ordering order,
                                       const BSONObj& config, std::string collectionNamespace,
                                       std::string indexName, const BSONObj& keyPattern,
                                       bool partial, bool isIdIdx)
        : RocksIndexBase(db, cf, prefix, ident, order, config),
          _collectionNamespace(std::move(collectionNamespace)),
          _indexName(std::move(indexName)),
          _keyPattern(keyPattern),
          _partial(partial),
          _isIdIndex(isIdIdx) {}

    std::unique_ptr<SortedDataInterface::Cursor> RocksUniqueIndex::newCursor(
        OperationContext* opCtx, bool forward) const {
        return stdx::make_unique<RocksUniqueCursor>(opCtx, _db, _cf, _prefix, forward, _order,
                                                    _keyStringVersion, _indexName, _isIdIndex);
    }

    SortedDataBuilderInterface* RocksUniqueIndex::getBulkBuilder(OperationContext* opCtx,
                                                                 bool dupsAllowed) {
        return new RocksIndexBase::UniqueBulkBuilder(_cf, _prefix, _order, _keyStringVersion,
                                                     _collectionNamespace, _indexName, opCtx,
                                                     dupsAllowed, _keyPattern, _isIdIndex);
    }

    StatusWith<SpecialFormatInserted> RocksUniqueIndex::insert(OperationContext* opCtx,
                                                               const BSONObj& key,
                                                               const RecordId& loc,
                                                               bool dupsAllowed) {
        if (_isIdIndex) {
            return _insertTimestampUnsafe(opCtx, key, loc, dupsAllowed);
        } else {
            return _insertTimestampSafe(opCtx, key, loc, dupsAllowed);
        }
    }

    bool RocksUniqueIndex::_keyExistsTimestampSafe(OperationContext* opCtx, const KeyString& key) {
        std::unique_ptr<RocksIterator> iter;
        iter.reset(RocksRecoveryUnit::getRocksRecoveryUnit(opCtx)->NewIterator(_cf, _prefix));
        auto s = rocksPrepareConflictRetry(opCtx, [&] {
            iter->SeekPrefix(rocksdb::Slice(key.getBuffer(), key.getSize()));
            return iter->status();
        });
        return iter->Valid();
    }

    StatusWith<SpecialFormatInserted> RocksUniqueIndex::_insertTimestampSafe(
        OperationContext* opCtx, const BSONObj& key, const RecordId& loc, bool dupsAllowed) {
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        invariant(ru->getTransaction());
        if (!dupsAllowed) {
            const KeyString encodedKey(_keyStringVersion, key, _order);
            const std::string prefixedKey(RocksIndexBase::_makePrefixedKey(_prefix, encodedKey));
            invariantRocksOK(ru->getTransaction()->GetForUpdate(_cf, prefixedKey));
            if (_keyExistsTimestampSafe(opCtx, encodedKey)) {
                return buildDupKeyErrorStatus(key,
                                              NamespaceString(StringData(_collectionNamespace)),
                                              _indexName, _keyPattern);
            }
        }
        const KeyString tableKey(_keyStringVersion, key, _order, loc);
        const std::string prefixedKey(RocksIndexBase::_makePrefixedKey(_prefix, tableKey));
        std::string valueItem =
            tableKey.getTypeBits().isAllZeros()
                ? emptyItem
                : std::string(tableKey.getTypeBits().getBuffer(), tableKey.getTypeBits().getSize());
        invariantRocksOK(ROCKS_OP_CHECK(ru->getTransaction()->Put(_cf, prefixedKey, valueItem)));
        _indexStorageSize.fetch_add(static_cast<long long>(prefixedKey.size()),
                                    std::memory_order_relaxed);

        if (tableKey.getTypeBits().isLongEncoding())
            return StatusWith<SpecialFormatInserted>(SpecialFormatInserted::LongTypeBitsInserted);

        return StatusWith<SpecialFormatInserted>(SpecialFormatInserted::NoSpecialFormatInserted);
    }

    StatusWith<SpecialFormatInserted> RocksUniqueIndex::_insertTimestampUnsafe(
        OperationContext* opCtx, const BSONObj& key, const RecordId& loc, bool dupsAllowed) {
        dassert(opCtx->lockState()->isWriteLocked());
        invariant(loc.isValid());
        dassert(!hasFieldNames(key));

        KeyString encodedKey(_keyStringVersion, key, _order);
        std::string prefixedKey(_makePrefixedKey(_prefix, encodedKey));

        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        invariant(ru->getTransaction());
        _indexStorageSize.fetch_add(static_cast<long long>(prefixedKey.size()),
                                    std::memory_order_relaxed);

        std::string currentValue;
        auto getStatus =
            rocksPrepareConflictRetry(opCtx, [&] { return ru->Get(_cf, prefixedKey, &currentValue); });

        if (getStatus.IsNotFound()) {
            // nothing here. just insert the value
            KeyString value(_keyStringVersion, loc);
            if (!encodedKey.getTypeBits().isAllZeros()) {
                value.appendTypeBits(encodedKey.getTypeBits());
            }
            rocksdb::Slice valueSlice(value.getBuffer(), value.getSize());
            invariantRocksOK(
                ROCKS_OP_CHECK(ru->getTransaction()->Put(_cf, prefixedKey, valueSlice)));
            return StatusWith<SpecialFormatInserted>(
                SpecialFormatInserted::NoSpecialFormatInserted);
        }

        if (!getStatus.ok()) {
            return rocksToMongoStatus(getStatus);
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
                return StatusWith<SpecialFormatInserted>(
                    SpecialFormatInserted::NoSpecialFormatInserted);  // already in index
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
            return buildDupKeyErrorStatus(key,
                                          NamespaceString(StringData(_collectionNamespace)),
                                          _indexName, _keyPattern);
        }

        if (!insertedLoc) {
            // This loc is higher than all currently in the index for this key
            valueVector.appendRecordId(loc);
            valueVector.appendTypeBits(encodedKey.getTypeBits());
        }

        rocksdb::Slice valueVectorSlice(valueVector.getBuffer(), valueVector.getSize());
        auto txn = ru->getTransaction();
        invariant(txn);

        invariantRocksOK(ROCKS_OP_CHECK(txn->Put(_cf, prefixedKey, valueVectorSlice)));
        return StatusWith<SpecialFormatInserted>(SpecialFormatInserted::NoSpecialFormatInserted);
    }

    void RocksUniqueIndex::_unindexTimestampSafe(OperationContext* opCtx, const BSONObj& key,
                                                 const RecordId& loc, bool dupsAllowed) {
        KeyString encodedKey(_keyStringVersion, key, _order, loc);
        std::string prefixedKey(_makePrefixedKey(_prefix, encodedKey));

        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        auto transaction = ru->getTransaction();
        invariant(transaction);

        invariantRocksOK(ROCKS_OP_CHECK(transaction->Delete(_cf, prefixedKey)));
        _indexStorageSize.fetch_sub(static_cast<long long>(prefixedKey.size()),
                                    std::memory_order_relaxed);
    }

    void RocksUniqueIndex::unindex(OperationContext* opCtx, const BSONObj& key, const RecordId& loc,
                                   bool dupsAllowed) {
        if (_isIdIndex) {
            _unindexTimestampUnsafe(opCtx, key, loc, dupsAllowed);
        } else {
            _unindexTimestampSafe(opCtx, key, loc, dupsAllowed);
        }
    }

    void RocksUniqueIndex::_unindexTimestampUnsafe(OperationContext* opCtx, const BSONObj& key,
                                                   const RecordId& loc, bool dupsAllowed) {
        KeyString encodedKey(_keyStringVersion, key, _order);
        std::string prefixedKey(_makePrefixedKey(_prefix, encodedKey));

        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        auto transaction = ru->getTransaction();
        invariant(transaction);

        auto triggerWriteConflictAtPoint = [&]() {
            // NOTE(wolfkdy): can only be called when a Get returns NOT_FOUND, to avoid SI's
            // write skew. this function has the semantics of GetForUpdate.
            // DO NOT use this if you dont know if the exists or not.
            invariantRocksOK(ROCKS_OP_CHECK(transaction->Delete(_cf, prefixedKey)));
        };

        if (!dupsAllowed) {
            std::string tmpVal;
            if (_partial) {
                // Check that the record id matches.  We may be called to unindex records that are
                // not present in the index due to the partial filter expression.
                auto s =
                    rocksPrepareConflictRetry(opCtx, [&] { return ru->Get(_cf, prefixedKey, &tmpVal); });
                if (s.IsNotFound()) {
                    // NOTE(wolfkdy): SERVER-28546
                    triggerWriteConflictAtPoint();
                    return;
                }
                invariantRocksOK(s);
                BufReader br(tmpVal.data(), tmpVal.size());
                invariant(br.remaining());
                if (KeyString::decodeRecordId(&br) != loc) {
                    return;
                }
                // Ensure there aren't any other values in here.
                KeyString::TypeBits::fromBuffer(_keyStringVersion, &br);
                invariant(!br.remaining());
            }
            invariantRocksOK(ROCKS_OP_CHECK(transaction->Delete(_cf, prefixedKey)));
            _indexStorageSize.fetch_sub(static_cast<long long>(prefixedKey.size()),
                                        std::memory_order_relaxed);
            return;
        }

        // dups are allowed, so we have to deal with a vector of RecordIds.
        std::string currentValue;
        auto getStatus =
            rocksPrepareConflictRetry(opCtx, [&] { return ru->Get(_cf, prefixedKey, &currentValue); });
        if (getStatus.IsNotFound()) {
            // NOTE(wolfkdy): SERVER-28546
            triggerWriteConflictAtPoint();
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
                    invariantRocksOK(ROCKS_OP_CHECK(transaction->Delete(_cf, prefixedKey)));
                    _indexStorageSize.fetch_sub(static_cast<long long>(prefixedKey.size()),
                                                std::memory_order_relaxed);
                    return;
                }

                foundLoc = true;
                continue;
            }

            records.push_back(std::make_pair(locInIndex, typeBits));
        }

        if (!foundLoc) {
            warning().stream() << loc << " not found in the index for key " << redact(key);
            return;  // nothing to do
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
        invariantRocksOK(ROCKS_OP_CHECK(transaction->Put(_cf, prefixedKey, newValueSlice)));
        _indexStorageSize.fetch_sub(static_cast<long long>(prefixedKey.size()),
                                    std::memory_order_relaxed);
    }

    Status RocksUniqueIndex::dupKeyCheck(OperationContext* opCtx, const BSONObj& key) {
        if (!_isIdIndex) {
            KeyString encodedKey(_keyStringVersion, key, _order);
            if (_keyExistsTimestampSafe(opCtx, encodedKey)) {
                return buildDupKeyErrorStatus(key,
                                              NamespaceString(StringData(_collectionNamespace)),
                                              _indexName, _keyPattern);
            } else {
                return Status::OK();
            }
        }
        KeyString encodedKey(_keyStringVersion, key, _order);
        std::string prefixedKey(_makePrefixedKey(_prefix, encodedKey));

        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        std::string value;
        auto getStatus =
            rocksPrepareConflictRetry(opCtx, [&] { return ru->Get(_cf, prefixedKey, &value); });
        if (!getStatus.ok() && !getStatus.IsNotFound()) {
            return rocksToMongoStatus(getStatus);
        } else if (getStatus.IsNotFound()) {
            // not found, not duplicate key
            return Status::OK();
        }

        // If the key exists, check if we already have this loc at this key. If so, we don't
        // consider that to be a dup.
        int records = 0;
        BufReader br(value.data(), value.size());
        while (br.remaining()) {
            KeyString::decodeRecordId(&br);
            records++;

            KeyString::TypeBits::fromBuffer(_keyStringVersion,
                                            &br);  // Just calling this to advance reader.
        }

        if (records > 1) {
            return buildDupKeyErrorStatus(key,
                                          NamespaceString(StringData(_collectionNamespace)),
                                          _indexName, _keyPattern);
        }
        return Status::OK();
    }

    /// RocksStandardIndex
    RocksStandardIndex::RocksStandardIndex(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf,
                                           std::string prefix, std::string ident, Ordering order,
                                           const BSONObj& config)
        : RocksIndexBase(db, cf, prefix, ident, order, config), useSingleDelete(false) {}

    StatusWith<SpecialFormatInserted> RocksStandardIndex::insert(OperationContext* opCtx,
                                                                 const BSONObj& key,
                                                                 const RecordId& loc,
                                                                 bool dupsAllowed) {
        dassert(opCtx->lockState()->isWriteLocked());
        invariant(loc.isValid());
        dassert(!hasFieldNames(key));
        invariant(dupsAllowed);

        KeyString encodedKey(_keyStringVersion, key, _order, loc);
        std::string prefixedKey(_makePrefixedKey(_prefix, encodedKey));
        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        auto transaction = ru->getTransaction();

        rocksdb::Slice value;
        if (!encodedKey.getTypeBits().isAllZeros()) {
            value =
                rocksdb::Slice(reinterpret_cast<const char*>(encodedKey.getTypeBits().getBuffer()),
                               encodedKey.getTypeBits().getSize());
        }

        invariantRocksOK(ROCKS_OP_CHECK(transaction->Put(_cf, prefixedKey, value)));
        _indexStorageSize.fetch_add(static_cast<long long>(prefixedKey.size()),
                                    std::memory_order_relaxed);

        return StatusWith<SpecialFormatInserted>(SpecialFormatInserted::NoSpecialFormatInserted);
    }

    void RocksStandardIndex::unindex(OperationContext* opCtx, const BSONObj& key,
                                     const RecordId& loc, bool dupsAllowed) {
        invariant(dupsAllowed);
        dassert(opCtx->lockState()->isWriteLocked());
        invariant(loc.isValid());
        dassert(!hasFieldNames(key));

        KeyString encodedKey(_keyStringVersion, key, _order, loc);
        std::string prefixedKey(_makePrefixedKey(_prefix, encodedKey));

        auto ru = RocksRecoveryUnit::getRocksRecoveryUnit(opCtx);
        invariant(ru);
        auto transaction = ru->getTransaction();
        invariant(transaction);

        if (useSingleDelete) {
            warning() << "mongoRocks4.0+ nolonger supports singleDelete, fallback to Delete";
        }
        invariantRocksOK(ROCKS_OP_CHECK(transaction->Delete(_cf, prefixedKey)));
        _indexStorageSize.fetch_sub(static_cast<long long>(prefixedKey.size()),
                                    std::memory_order_relaxed);
    }

    std::unique_ptr<SortedDataInterface::Cursor> RocksStandardIndex::newCursor(
        OperationContext* opCtx, bool forward) const {
        return stdx::make_unique<RocksStandardCursor>(opCtx, _db, _cf, _prefix, forward, _order,
                                                      _keyStringVersion);
    }

    SortedDataBuilderInterface* RocksStandardIndex::getBulkBuilder(OperationContext* opCtx,
                                                                   bool dupsAllowed) {
        invariant(dupsAllowed);
        return new RocksIndexBase::StandardBulkBuilder(this, opCtx);
    }

}  // namespace mongo
