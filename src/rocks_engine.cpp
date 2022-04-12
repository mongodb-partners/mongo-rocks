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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "rocks_engine.h"

#include <algorithm>
#include <mutex>

#include <boost/filesystem.hpp>
#include <rocksdb/cache.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/comparator.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/experimental.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/version.h>

#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/locker.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/server_options.h"
#include "mongo/db/server_recovery.h"
#include "mongo/db/snapshot_window_options.h"
#include "mongo/db/storage/journal_listener.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/background.h"
#include "mongo/util/concurrency/idle_thread_block.h"
#include "mongo/util/concurrency/ticketholder.h"
#include "mongo/util/debug_util.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/util/processinfo.h"

#include "mongo/db/modules/rocks/src/rocks_parameters_gen.h"
#include "rocks_counter_manager.h"
#include "rocks_global_options.h"
#include "rocks_index.h"
#include "rocks_record_store.h"
#include "rocks_recovery_unit.h"
#include "rocks_util.h"
#include "mongo_rate_limiter_checker.h"

#define ROCKS_TRACE log()
#define LOG_FOR_RECOVERY(level) \
    MONGO_LOG_COMPONENT(level, ::mongo::logger::LogComponent::kStorageRecovery)
#define LOG_FOR_ROLLBACK(level) \
    MONGO_LOG_COMPONENT(level, ::mongo::logger::LogComponent::kReplicationRollback)

namespace mongo {

    class RocksEngine::RocksJournalFlusher : public BackgroundJob {
    public:
        explicit RocksJournalFlusher(RocksDurabilityManager* durabilityManager)
            : BackgroundJob(false /* deleteSelf */), _durabilityManager(durabilityManager) {}

        virtual std::string name() const { return "RocksJournalFlusher"; }

        virtual void run() override {
            ThreadClient tc(name(), getGlobalServiceContext());
            LOG(1) << "starting " << name() << " thread";

            while (!_shuttingDown.load()) {
                try {
                    _durabilityManager->waitUntilDurable(false);
                } catch (const AssertionException& e) {
                    invariant(e.code() == ErrorCodes::ShutdownInProgress);
                }

                int ms = storageGlobalParams.journalCommitIntervalMs.load();
                if (!ms) {
                    ms = 100;
                }

                MONGO_IDLE_THREAD_BLOCK;
                sleepmillis(ms);
            }
            LOG(1) << "stopping " << name() << " thread";
        }

        void shutdown() {
            _shuttingDown.store(true);
            wait();
        }

    private:
        RocksDurabilityManager* _durabilityManager;  // not owned
        std::atomic<bool> _shuttingDown{false};      // NOLINT
    };

    namespace {
        TicketHolder openWriteTransaction(128);
        TicketHolder openReadTransaction(128);
        rocksdb::TOComparator comparator;
    }  // namespace

    ROpenWriteTransactionParam::ROpenWriteTransactionParam(StringData name, ServerParameterType spt)
        : ServerParameter(name, spt), _data(&openWriteTransaction) {}

    void ROpenWriteTransactionParam::append(OperationContext* opCtx, BSONObjBuilder& b,
                                            const std::string& name) {
        b.append(name, _data->outof());
    }

    Status ROpenWriteTransactionParam::setFromString(const std::string& str) {
        int num = 0;
        Status status = parseNumberFromString(str, &num);
        if (!status.isOK()) {
            return status;
        }
        if (num <= 0) {
            return {ErrorCodes::BadValue, str::stream() << name() << " has to be > 0"};
        }
        return _data->resize(num);
    }

    ROpenReadTransactionParam::ROpenReadTransactionParam(StringData name, ServerParameterType spt)
        : ServerParameter(name, spt), _data(&openReadTransaction) {}

    void ROpenReadTransactionParam::append(OperationContext* opCtx, BSONObjBuilder& b,
                                           const std::string& name) {
        b.append(name, _data->outof());
    }

    Status ROpenReadTransactionParam::setFromString(const std::string& str) {
        int num = 0;
        Status status = parseNumberFromString(str, &num);
        if (!status.isOK()) {
            return status;
        }
        if (num <= 0) {
            return {ErrorCodes::BadValue, str::stream() << name() << " has to be > 0"};
        }
        return _data->resize(num);
    }

    // first four bytes are the default prefix 0
    const std::string RocksEngine::kMetadataPrefix("\0\0\0\0metadata-", 13);

    const int RocksEngine::kDefaultJournalDelayMillis = 100;

    RocksEngine::RocksEngine(const std::string& path, bool durable, int formatVersion,
                             bool readOnly)
        : _path(path),
          _durable(durable),
          _formatVersion(formatVersion),
          _maxPrefix(0),
          _keepDataHistory(serverGlobalParams.enableMajorityReadConcern),
          _stableTimestamp(0),
          _initialDataTimestamp(0) {

        {  // create block cache
            uint64_t cacheSizeGB = rocksGlobalOptions.cacheSizeGB;
            if (cacheSizeGB == 0) {
                ProcessInfo pi;
                unsigned long long memSizeMB = pi.getMemSizeMB();
                if (memSizeMB > 0) {
                    // reserve 1GB for system and binaries, and use 30% of the rest
                    double cacheMB = (memSizeMB - 1024) * 0.5;
                    cacheSizeGB = static_cast<uint64_t>(cacheMB / 1024);
                }
                if (cacheSizeGB < 1) {
                    cacheSizeGB = 1;
                }
            }
            _blockCache = rocksdb::NewLRUCache(cacheSizeGB * 1024 * 1024 * 1024LL, 6);
        }
        _maxWriteMBPerSec = rocksGlobalOptions.maxWriteMBPerSec;
        _rateLimiter.reset(
            rocksdb::NewGenericRateLimiter(static_cast<int64_t>(_maxWriteMBPerSec) * 1024 * 1024));
        if (rocksGlobalOptions.counters) {
            _statistics = rocksdb::CreateDBStatistics();
        }

        log() << "clusterRole is " << static_cast<int>(serverGlobalParams.clusterRole) << ";";

        // used in building options for the db
        _compactionScheduler.reset(new RocksCompactionScheduler());

        // Until the Replication layer installs a real callback, prevent truncating the oplog.
        setOldestActiveTransactionTimestampCallback(
            [](Timestamp) { return StatusWith(boost::make_optional(Timestamp::min())); });


        // TODO(wolfkdy): support readOnly mode
        invariant(!readOnly);

        // open DB
        _initDatabase();

        _counterManager.reset(
            new RocksCounterManager(_db.get(), _defaultCf.get(), rocksGlobalOptions.crashSafeCounters));

        // open iterator
        auto txn = std::unique_ptr<rocksdb::TOTransaction>(
            _db->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TOTransactionOptions()));
        // metadata is write no-timestamped, so read no-timestamped
        rocksdb::ReadOptions readOpts;
        auto iter = std::unique_ptr<rocksdb::Iterator>(txn->GetIterator(readOpts, _defaultCf.get()));

        // find maxPrefix
        iter->SeekToLast();
        if (iter->Valid()) {
            // otherwise the DB is empty, so we just keep it at 0
            bool ok = extractPrefix(iter->key(), &_maxPrefix);
            // this is DB corruption here
            invariant(ok);
        }

        // Log ident to prefix map. also update _maxPrefix if there's any prefix bigger than
        // current _maxPrefix. Here we have no need to check conflict state since we'are
        // bootstraping and there wouldn't be any Prepares.
        {
            for (iter->Seek(kMetadataPrefix);
                 iter->Valid() && iter->key().starts_with(kMetadataPrefix); iter->Next()) {
                invariantRocksOK(iter->status());
                rocksdb::Slice ident(iter->key());
                ident.remove_prefix(kMetadataPrefix.size());
                // this could throw DBException, which then means DB corruption. We just let it fly
                // to the caller
                BSONObj identConfig(iter->value().data());
                BSONElement element = identConfig.getField("prefix");

                if (element.eoo() || !element.isNumber()) {
                    log() << "Mongo metadata in RocksDB database is corrupted.";
                    invariant(false);
                }
                uint32_t identPrefix = static_cast<uint32_t>(element.numberInt());

                _identMap[StringData(ident.data(), ident.size())] = identConfig.getOwned();

                _maxPrefix = std::max(_maxPrefix, identPrefix);
            }
        }

        // just to be extra sure. we need this if last collection is oplog -- in that case we
        // reserve prefix+1 for oplog key tracker
        ++_maxPrefix;

        // start compaction thread and load dropped prefixes
        _compactionScheduler->start(_db.get(), _defaultCf.get());
        auto maxDroppedPrefix = _compactionScheduler->loadDroppedPrefixes(iter.get(),
                                                                          {_defaultCf.get(), _oplogCf.get()});
        _maxPrefix = std::max(_maxPrefix, maxDroppedPrefix);

        _durabilityManager.reset(
            new RocksDurabilityManager(_db.get(), _durable, _defaultCf.get(), _oplogCf.get()));
        _oplogManager.reset(new RocksOplogManager(_db.get(), this, _durabilityManager.get()));

        rocksdb::RocksTimeStamp ts(0);
        auto status = _db->QueryTimeStamp(rocksdb::TimeStampType::kStable, &ts);
        if (!status.IsNotFound()) {
            invariant(status.ok(), status.ToString());
            _recoveryTimestamp = Timestamp(ts);
            if (!_recoveryTimestamp.isNull()) {
                setInitialDataTimestamp(_recoveryTimestamp);
                setStableTimestamp(_recoveryTimestamp, false);
                LOG_FOR_RECOVERY(0) << "Rocksdb recoveryTimestamp. Ts: " << _recoveryTimestamp;
            }
        }

        if (_durable) {
            _journalFlusher = stdx::make_unique<RocksJournalFlusher>(_durabilityManager.get());
            _journalFlusher->go();
        }

        Locker::setGlobalThrottling(&openReadTransaction, &openWriteTransaction);
    }

    RocksEngine::~RocksEngine() { cleanShutdown(); }

    void RocksEngine::_initDatabase() {
        // open DB
        rocksdb::TOTransactionDB* db = nullptr;
        rocksdb::Status s;
        std::vector<std::string> columnFamilies;

        const bool newDB = [&]() {
            const auto path = boost::filesystem::path(_path) / "CURRENT";
            return !boost::filesystem::exists(path);
        }();
        if (newDB) {
            // init manifest so list column families will not fail when db is empty.
            invariantRocksOK(rocksdb::TOTransactionDB::Open(
                _options(false /* isOplog */),
                rocksdb::TOTransactionDBOptions(rocksGlobalOptions.maxConflictCheckSizeMB), _path,
                &db));
            invariantRocksOK(db->Close());
        }

        const bool hasOplog = [&]() {
            s = rocksdb::DB::ListColumnFamilies(_options(false /* isOplog */), _path, &columnFamilies);
            invariantRocksOK(s);

            auto it = std::find(columnFamilies.begin(), columnFamilies.end(),
                                NamespaceString::kRsOplogNamespace.toString());
            return (it != columnFamilies.end());
        }();

        // init oplog columnfamily if not exists.
        if (!hasOplog) {
            rocksdb::ColumnFamilyHandle* cf = nullptr;
            invariantRocksOK(rocksdb::TOTransactionDB::Open(
                _options(false /* isOplog */),
                rocksdb::TOTransactionDBOptions(rocksGlobalOptions.maxConflictCheckSizeMB), _path,
                &db));
            invariantRocksOK(db->CreateColumnFamily(_options(true /* isOplog */),
                                                    NamespaceString::kRsOplogNamespace.toString(),
                                                    &cf));
            invariantRocksOK(db->DestroyColumnFamilyHandle(cf));
            invariantRocksOK(db->Close());
            log() << "init oplog cf success";
        }

        std::vector<rocksdb::ColumnFamilyHandle*> cfs;
        s = rocksdb::TOTransactionDB::Open(
            _options(false /* isOplog */),
            rocksdb::TOTransactionDBOptions(rocksGlobalOptions.maxConflictCheckSizeMB), _path,
            {{rocksdb::kDefaultColumnFamilyName, _options(false /* isOplog */)},
             {NamespaceString::kRsOplogNamespace.toString(), _options(true /* isOplog */)}},
            &cfs, &db);
        invariantRocksOK(s);
        invariant(cfs.size() == 2);
        invariant(cfs[0]->GetName() == rocksdb::kDefaultColumnFamilyName);
        invariant(cfs[1]->GetName() == NamespaceString::kRsOplogNamespace.toString());
        _db.reset(db);
        _defaultCf.reset(cfs[0]);
        _oplogCf.reset(cfs[1]);

        rocksdb::RocksTimeStamp ts(0);
        auto status = _db->QueryTimeStamp(rocksdb::TimeStampType::kStable, &ts);
        if (!status.IsNotFound() && Timestamp(ts).asULL() >= 1) {
            invariantRocksOK(_db->RollbackToStable(_defaultCf.get()));
        }
    }

    void RocksEngine::appendGlobalStats(BSONObjBuilder& b) {
        BSONObjBuilder bb(b.subobjStart("concurrentTransactions"));
        {
            BSONObjBuilder bbb(bb.subobjStart("write"));
            bbb.append("out", openWriteTransaction.used());
            bbb.append("available", openWriteTransaction.available());
            bbb.append("totalTickets", openWriteTransaction.outof());
            bbb.done();
        }
        {
            BSONObjBuilder bbb(bb.subobjStart("read"));
            bbb.append("out", openReadTransaction.used());
            bbb.append("available", openReadTransaction.available());
            bbb.append("totalTickets", openReadTransaction.outof());
            bbb.done();
        }
        bb.done();
    }

    std::map<int, std::vector<uint64_t>> RocksEngine::getDefaultCFNumEntries() const {
        std::map<int, std::vector<uint64_t>> numEntriesMap;

        std::vector<rocksdb::LiveFileMetaData> allFiles;
        _db->GetRootDB()->GetLiveFilesMetaData(&allFiles);
        for (const auto& f : allFiles) {
            if (NamespaceString::oplog(f.column_family_name)) {
                continue;
            }

            if (numEntriesMap.find(f.level) == numEntriesMap.end()) {
                numEntriesMap[f.level] = std::vector<uint64_t>(2, 0);
            }

            numEntriesMap[f.level][0] += f.num_entries;
            numEntriesMap[f.level][1] += f.num_deletions;
        }

        return numEntriesMap;
    }

    Status RocksEngine::okToRename(OperationContext* opCtx, StringData fromNS, StringData toNS,
                                   StringData ident, const RecordStore* originalRecordStore) const {
        _counterManager->sync();
        return Status::OK();
    }

    int64_t RocksEngine::getIdentSize(OperationContext* opCtx, StringData ident) {
        stdx::lock_guard<Latch> lk(_identObjectMapMutex);

        auto indexIter = _identIndexMap.find(ident);
        if (indexIter != _identIndexMap.end()) {
            return static_cast<int64_t>(indexIter->second->getSpaceUsedBytes(opCtx));
        }
        auto collectionIter = _identCollectionMap.find(ident);
        if (collectionIter != _identCollectionMap.end()) {
            return collectionIter->second->storageSize(opCtx);
        }

        // this can only happen if collection or index exists, but it's not opened (i.e.
        // getRecordStore or getSortedDataInterface are not called)
        return 1;
    }

    int RocksEngine::flushAllFiles(OperationContext* opCtx, bool sync) {
        LOG(1) << "RocksEngine::flushAllFiles";
        _counterManager->sync();
        _durabilityManager->waitUntilDurable(true);
        return 1;
    }

    Status RocksEngine::beginBackup(OperationContext* opCtx) {
        _counterManager->sync();
        return rocksToMongoStatus(_db->PauseBackgroundWork());
    }

    void RocksEngine::endBackup(OperationContext* opCtx) { _db->ContinueBackgroundWork(); }

    void RocksEngine::setOldestActiveTransactionTimestampCallback(
        StorageEngine::OldestActiveTransactionTimestampCallback callback) {
        stdx::lock_guard<Latch> lk(_oldestActiveTransactionTimestampCallbackMutex);
        _oldestActiveTransactionTimestampCallback = std::move(callback);
    };

    RecoveryUnit* RocksEngine::newRecoveryUnit() {
        return new RocksRecoveryUnit(_db.get(), _oplogManager.get(), &_snapshotManager,
                                     _compactionScheduler.get(),
                                     _durabilityManager.get(), _durable, this);
    }

    Status RocksEngine::createRecordStore(OperationContext* opCtx, StringData ns, StringData ident,
                                          const CollectionOptions& options) {
        BSONObjBuilder configBuilder;
        auto s = _createIdent(ident, &configBuilder);
        if (s.isOK() && NamespaceString::oplog(ns)) {
            _oplogIdent = ident.toString();
            // oplog needs two prefixes, so we also reserve the next one
            uint64_t oplogTrackerPrefix = 0;
            {
                stdx::lock_guard<Latch> lk(_identMapMutex);
                oplogTrackerPrefix = ++_maxPrefix;
            }
            // we also need to write out the new prefix to the database. this is just an
            // optimization
            std::string encodedPrefix(encodePrefix(oplogTrackerPrefix));
            s = rocksToMongoStatus(
                _db->Put(rocksdb::WriteOptions(), _defaultCf.get(), encodedPrefix, rocksdb::Slice()));
        }
        return s;
    }

    std::unique_ptr<RecordStore> RocksEngine::getRecordStore(OperationContext* opCtx, StringData ns,
                                                             StringData ident,
                                                             const CollectionOptions& options) {
        rocksdb::ColumnFamilyHandle* cf = nullptr;
        if (NamespaceString::oplog(ns)) {
            _oplogIdent = ident.toString();
            cf = _oplogCf.get();
        } else {
            cf = _defaultCf.get();
        }

        auto config = _getIdentConfig(ident);
        std::string prefix = _extractPrefix(config);

        RocksRecordStore::Params params;
        params.ns = ns;
        params.ident = ident.toString();
        params.prefix = prefix;
        params.isCapped = options.capped;
        params.cappedMaxSize =
            params.isCapped ? (options.cappedSize ? options.cappedSize : 4096) : -1;
        params.cappedMaxDocs =
            params.isCapped ? (options.cappedMaxDocs ? options.cappedMaxDocs : -1) : -1;
        params.cappedCallback = nullptr;
        params.tracksSizeAdjustments = true;
        std::unique_ptr<RocksRecordStore> recordStore =
            stdx::make_unique<RocksRecordStore>(this, cf, opCtx, params);

        {
            stdx::lock_guard<Latch> lk(_identObjectMapMutex);
            _identCollectionMap[ident] = recordStore.get();
        }

        return std::move(recordStore);
    }

    Status RocksEngine::createSortedDataInterface(OperationContext* opCtx,
                                                  const CollectionOptions& collOptions,
                                                  StringData ident, const IndexDescriptor* desc) {
        BSONObjBuilder configBuilder;
        // let index add its own config things
        RocksIndexBase::generateConfig(&configBuilder, _formatVersion, desc->version());
        return _createIdent(ident, &configBuilder);
    }

    SortedDataInterface* RocksEngine::getSortedDataInterface(OperationContext* opCtx,
                                                             StringData ident,
                                                             const IndexDescriptor* desc) {
        auto config = _getIdentConfig(ident);
        std::string prefix = _extractPrefix(config);

        // oplog have no indexes
        invariant(!desc->parentNS().isOplog());

        RocksIndexBase* index;
        if (desc->unique()) {
            index = new RocksUniqueIndex(_db.get(), _defaultCf.get(), prefix, ident.toString(),
                                         Ordering::make(desc->keyPattern()), std::move(config),
                                         desc->parentNS().toString(), desc->indexName(),
                                         desc->keyPattern(), desc->isPartial());
        } else {
            auto si = new RocksStandardIndex(_db.get(), _defaultCf.get(), prefix, ident.toString(),
                                             Ordering::make(desc->keyPattern()), std::move(config));
            if (rocksGlobalOptions.singleDeleteIndex) {
                si->enableSingleDelete();
            }
            index = si;
        }
        {
            stdx::lock_guard<Latch> lk(_identObjectMapMutex);
            _identIndexMap[ident] = index;
        }
        return index;
    }

    // TODO(wolfkdy); this interface is not fully reviewed
    std::unique_ptr<RecordStore> RocksEngine::makeTemporaryRecordStore(OperationContext* opCtx,
                                                                       StringData ident) {
        BSONObjBuilder configBuilder;
        auto s = _createIdent(ident, &configBuilder);
        if (!s.isOK()) {
            return nullptr;
        }
        auto config = _getIdentConfig(ident);
        std::string prefix = _extractPrefix(config);

        RocksRecordStore::Params params;
        params.ns = "";
        params.ident = ident.toString();
        params.prefix = prefix;
        params.isCapped = false;
        params.cappedMaxSize = -1;
        params.cappedMaxDocs = -1;
        params.cappedCallback = nullptr;
        params.tracksSizeAdjustments = false;

        std::unique_ptr<RocksRecordStore> recordStore =
            stdx::make_unique<RocksRecordStore>(this, _defaultCf.get(), opCtx, params);

        {
            stdx::lock_guard<Latch> lk(_identObjectMapMutex);
            _identCollectionMap[ident] = recordStore.get();
        }

        return std::move(recordStore);
    }

    // cannot be rolled back
    Status RocksEngine::dropIdent(OperationContext* opCtx, StringData ident) {
        auto config = _tryGetIdentConfig(ident);
        // happens rarely when dropped prefix markers are persisted but metadata changes
        // are lost due to system crash on standalone with default acknowledgement behavior
        if (config.isEmpty()) {
            log() << "Cannot find ident " << ident << " to drop, ignoring";
            return Status::OK();
        }

        rocksdb::WriteOptions writeOptions;
        writeOptions.sync = true;
        rocksdb::TOTransactionOptions txnOptions;
        std::unique_ptr<rocksdb::TOTransaction> txn(_db->BeginTransaction(writeOptions, txnOptions));

        auto status = txn->Delete(_defaultCf.get(), kMetadataPrefix + ident.toString());
        if (!status.ok()) {
            log() << "dropIdent error: " << status.ToString();
            txn->Rollback();
            return rocksToMongoStatus(status);
        }

        // calculate which prefixes we need to drop
        std::vector<std::string> prefixesToDrop;
        prefixesToDrop.push_back(_extractPrefix(config));
        if (_oplogIdent == ident.toString()) {
            // if we're dropping oplog, we also need to drop keys from RocksOplogKeyTracker (they
            // are stored at prefix+1)
            prefixesToDrop.push_back(rocksGetNextPrefix(prefixesToDrop[0]));
        }

        auto cf = (_oplogIdent == ident.toString()) ? _oplogCf.get() : _defaultCf.get();
        // we need to make sure this is on disk before starting to delete data in compactions
        auto s = _compactionScheduler->dropPrefixesAtomic(cf, prefixesToDrop, txn.get(), config);

        if (s.isOK()) {
            // remove from map
            stdx::lock_guard<Latch> lk(_identMapMutex);
            _identMap.erase(ident);
        }
        return s;
    }

    bool RocksEngine::hasIdent(OperationContext* opCtx, StringData ident) const {
        stdx::lock_guard<Latch> lk(_identMapMutex);
        return _identMap.find(ident) != _identMap.end();
    }

    std::vector<std::string> RocksEngine::getAllIdents(OperationContext* opCtx) const {
        std::vector<std::string> indents;
        stdx::lock_guard<Latch> lk(_identMapMutex);
        for (auto& entry : _identMap) {
            indents.push_back(entry.first);
        }
        return indents;
    }

    void RocksEngine::cleanShutdown() {
        if (_journalFlusher) {
            _journalFlusher->shutdown();
            _journalFlusher.reset();
        }
        _durabilityManager.reset();
        _snapshotManager.dropAllSnapshots();
        _counterManager->sync();
        _counterManager.reset();
        _compactionScheduler.reset();
        _defaultCf.reset();
        _oplogCf.reset();
        _db.reset();
    }

    void RocksEngine::setJournalListener(JournalListener* jl) {
        _durabilityManager->setJournalListener(jl);
    }

    void RocksEngine::setOldestTimestampFromStable() {
        Timestamp stableTimestamp(_stableTimestamp.load());

        // TODO(wolfkdy): impl failpoint RocksSetOldestTSToStableTS

        // Calculate what the oldest_timestamp should be from the stable_timestamp. The oldest
        // timestamp should lag behind stable by 'targetSnapshotHistoryWindowInSeconds' to create a
        // window of available snapshots. If the lag window is not yet large enough, we will not
        // update/forward the oldest_timestamp yet and instead return early.
        Timestamp newOldestTimestamp = _calculateHistoryLagFromStableTimestamp(stableTimestamp);
        if (newOldestTimestamp.isNull()) {
            return;
        }

        setOldestTimestamp(newOldestTimestamp, false);
    }

    Timestamp RocksEngine::getAllDurableTimestamp() const {
        return Timestamp(_oplogManager->fetchAllDurableValue());
    }

    Timestamp RocksEngine::getOldestOpenReadTimestamp() const {MONGO_UNREACHABLE}

    boost::optional<Timestamp> RocksEngine::getOplogNeededForCrashRecovery() const {
        return boost::none;
    }

    bool RocksEngine::isCacheUnderPressure(OperationContext* opCtx) const {
        // TODO(wolfkdy): review rocksdb's memory-stats for answer
        return false;
    }

    Timestamp RocksEngine::getStableTimestamp() const { return Timestamp(_stableTimestamp.load()); }
    Timestamp RocksEngine::getOldestTimestamp() const { return Timestamp(_oldestTimestamp.load()); }
    Timestamp RocksEngine::getCheckpointTimestamp() const {
        return Timestamp(_lastStableCheckpointTimestamp.load());
    }

    void RocksEngine::setMaxWriteMBPerSec(int maxWriteMBPerSec) {
        _maxWriteMBPerSec = maxWriteMBPerSec;
        _rateLimiter->SetBytesPerSecond(static_cast<int64_t>(_maxWriteMBPerSec) * 1024 * 1024);
    }

    Status RocksEngine::backup(const std::string& path) {
        rocksdb::Checkpoint* checkpoint;
        auto s = rocksdb::Checkpoint::Create(_db.get(), &checkpoint);
        if (s.ok()) {
            s = checkpoint->CreateCheckpoint(path);
        }
        delete checkpoint;
        return rocksToMongoStatus(s);
    }

    Status RocksEngine::_createIdent(StringData ident, BSONObjBuilder* configBuilder) {
        BSONObj config;
        uint32_t prefix = 0;
        {
            stdx::lock_guard<Latch> lk(_identMapMutex);
            if (_identMap.find(ident) != _identMap.end()) {
                // already exists
                return Status::OK();
            }

            prefix = ++_maxPrefix;
            configBuilder->append("prefix", static_cast<int32_t>(prefix));

            config = configBuilder->obj();
            _identMap[ident] = config.copy();
        }

        BSONObjBuilder builder;

        auto s = _db->Put(rocksdb::WriteOptions(), _defaultCf.get(), kMetadataPrefix + ident.toString(),
                          rocksdb::Slice(config.objdata(), config.objsize()));

        if (s.ok()) {
            // As an optimization, add a key <prefix> to the DB
            std::string encodedPrefix(encodePrefix(prefix));
            s = _db->Put(rocksdb::WriteOptions(), _defaultCf.get(), encodedPrefix, rocksdb::Slice());
        }

        return rocksToMongoStatus(s);
    }

    BSONObj RocksEngine::_getIdentConfig(StringData ident) {
        stdx::lock_guard<Latch> lk(_identMapMutex);
        auto identIter = _identMap.find(ident);
        invariant(identIter != _identMap.end());
        return identIter->second.copy();
    }

    BSONObj RocksEngine::_tryGetIdentConfig(StringData ident) {
        stdx::lock_guard<Latch> lk(_identMapMutex);
        auto identIter = _identMap.find(ident);
        const bool identFound = (identIter != _identMap.end());
        return identFound ? identIter->second.copy() : BSONObj();
    }

    std::string RocksEngine::_extractPrefix(const BSONObj& config) {
        return encodePrefix(config.getField("prefix").numberInt());
    }

    rocksdb::Options RocksEngine::_options(bool isOplog) const {
        // default options
        rocksdb::Options options;
        options.rate_limiter = _rateLimiter;
        rocksdb::BlockBasedTableOptions table_options;
        table_options.block_cache = _blockCache;
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
        table_options.block_size = 16 * 1024;  // 16KB
        table_options.format_version = 2;
        options.comparator = &comparator;
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

        options.write_buffer_size = rocksGlobalOptions.writeBufferSize;
        options.max_write_buffer_number = rocksGlobalOptions.maxWriteBufferNumber;
        options.max_background_jobs = rocksGlobalOptions.maxBackgroundJobs;
        options.max_total_wal_size = rocksGlobalOptions.maxTotalWalSize;
        options.db_write_buffer_size = rocksGlobalOptions.dbWriteBufferSize;
        options.num_levels = rocksGlobalOptions.numLevels;

        options.delayed_write_rate = rocksGlobalOptions.delayedWriteRate;
        options.level0_file_num_compaction_trigger =
            rocksGlobalOptions.level0FileNumCompactionTrigger;
        options.level0_slowdown_writes_trigger = rocksGlobalOptions.level0SlowdownWritesTrigger;
        options.level0_stop_writes_trigger = rocksGlobalOptions.level0StopWritesTrigger;
        options.soft_pending_compaction_bytes_limit =
            static_cast<unsigned long long>(rocksGlobalOptions.softPendingCompactionMBLimit) * 1024 * 1024;
        options.hard_pending_compaction_bytes_limit =
            static_cast<unsigned long long>(rocksGlobalOptions.hardPendingCompactionMBLimit) * 1024 * 1024;
        options.target_file_size_base = 64 * 1024 * 1024;  // 64MB
        options.level_compaction_dynamic_level_bytes = true;
        options.max_bytes_for_level_base = rocksGlobalOptions.maxBytesForLevelBase;
        // This means there is no limit on open files. Make sure to always set ulimit so that it can
        // keep all RocksDB files opened.
        options.max_open_files = -1;
        options.optimize_filters_for_hits = true;
        options.compaction_filter_factory.reset(
            _compactionScheduler->createCompactionFilterFactory());
        options.enable_thread_tracking = true;
        // Enable concurrent memtable
        options.allow_concurrent_memtable_write = true;
        options.enable_write_thread_adaptive_yield = true;

        options.compression_per_level.resize(3);
        options.compression_per_level[0] = rocksdb::kNoCompression;
        options.compression_per_level[1] = rocksdb::kNoCompression;
        if (isOplog) {
            // NOTE(deyukong): with kNoCompression, storageSize precisely matches non-compressed userdata size.
            // In this way, oplog capping will be timely.
            options.compression_per_level[2] = rocksdb::kNoCompression;
        } else {
            if (rocksGlobalOptions.compression == "snappy") {
                options.compression_per_level[2] = rocksdb::kSnappyCompression;
            } else if (rocksGlobalOptions.compression == "zlib") {
                options.compression_per_level[2] = rocksdb::kZlibCompression;
            } else if (rocksGlobalOptions.compression == "none") {
                options.compression_per_level[2] = rocksdb::kNoCompression;
            } else if (rocksGlobalOptions.compression == "lz4") {
                options.compression_per_level[2] = rocksdb::kLZ4Compression;
            } else if (rocksGlobalOptions.compression == "lz4hc") {
                options.compression_per_level[2] = rocksdb::kLZ4HCCompression;
            } else {
                log() << "Unknown compression, will use default (snappy)";
                options.compression_per_level[2] = rocksdb::kSnappyCompression;
            }
        }
        options.info_log =
            std::shared_ptr<rocksdb::Logger>(new MongoRocksLogger());
        options.statistics = _statistics;

        // create the DB if it's not already present
        options.create_if_missing = true;
        // options.wal_dir = _path + "/journal";

        // allow override
        if (!rocksGlobalOptions.configString.empty()) {
            rocksdb::Options base_options(options);
            auto s = rocksdb::GetOptionsFromString(base_options, rocksGlobalOptions.configString,
                                                   &options);
            if (!s.ok()) {
                log() << "Invalid rocksdbConfigString \"" << redact(rocksGlobalOptions.configString)
                      << "\"";
                invariantRocksOK(s);
            }
        }

        return options;
    }

    namespace {

        MONGO_FAIL_POINT_DEFINE(RocksPreserveSnapshotHistoryIndefinitely);
        MONGO_FAIL_POINT_DEFINE(RocksSetOldestTSToStableTS);

    }  // namespace

    Timestamp RocksEngine::getInitialDataTimestamp() const {
        return Timestamp(_initialDataTimestamp.load());
    }

    void RocksEngine::setStableTimestamp(Timestamp stableTimestamp, bool force) {
        if (!_keepDataHistory || stableTimestamp.isNull()) {
            return;
        }

        _stableTimestamp.store(stableTimestamp.asULL());
        invariantRocksOK(_db->SetTimeStamp(rocksdb::TimeStampType::kStable, 
                                           rocksdb::RocksTimeStamp(_stableTimestamp.load()), 
                                           force));

        const Timestamp initialDataTimestamp(_initialDataTimestamp.load());

        // cases:
        //
        // First, initialDataTimestamp is Timestamp(0, 1) -> (i.e: during initial sync).
        //
        // Second, enableMajorityReadConcern is false. In this case, we are not tracking a
        // stable timestamp. 
        //
        // Third, stableTimestamp < initialDataTimestamp: 
        //
        // Fourth, stableTimestamp >= initialDataTimestamp: Take stable checkpoint. Steady
        // state case.
        if (initialDataTimestamp.asULL() <= 1) {
            ;
        } else if (!_keepDataHistory) {
            // Ensure '_lastStableCheckpointTimestamp' is set such that oplog truncation may
            // take place entirely based on the oplog size.
            _lastStableCheckpointTimestamp.store(std::numeric_limits<uint64_t>::max());
        } else if (stableTimestamp < initialDataTimestamp) {
            LOG_FOR_RECOVERY(2)
                << "Stable timestamp is behind the initial data timestamp, skipping "
                "a checkpoint. StableTimestamp: "
                << stableTimestamp.toString()
                << " InitialDataTimestamp: " << initialDataTimestamp.toString();
        } else {
            LOG_FOR_RECOVERY(2) << "Performing stable checkpoint. StableTimestamp: "
                << stableTimestamp;

            // Publish the checkpoint time after the checkpoint becomes durable.
            _lastStableCheckpointTimestamp.store(stableTimestamp.asULL());
        }

        setOldestTimestamp(stableTimestamp, force);
    }

    void RocksEngine::setInitialDataTimestamp(Timestamp initialDataTimestamp) {
        _initialDataTimestamp.store(initialDataTimestamp.asULL());
    }

    // TODO(wolfkdy): in 4.0.3, setOldestTimestamp considers oplogReadTimestamp
    // it disappears in mongo4.2, find why it happens
    void RocksEngine::setOldestTimestamp(Timestamp oldestTimestamp, bool force) {
        // Set the oldest timestamp to the stable timestamp to ensure that there is no lag window
        // between the two.
        if (MONGO_FAIL_POINT(RocksSetOldestTSToStableTS)) {
            force = false;
        }
        if (MONGO_FAIL_POINT(RocksPreserveSnapshotHistoryIndefinitely)) {
            return;
        }

        rocksdb::RocksTimeStamp ts(oldestTimestamp.asULL());

        if (force) {
            invariantRocksOK(_db->SetTimeStamp(rocksdb::TimeStampType::kOldest, ts, force));
            invariantRocksOK(_db->SetTimeStamp(rocksdb::TimeStampType::kCommitted, ts, force));
            _oldestTimestamp.store(oldestTimestamp.asULL());
            LOG(2) << "oldest_timestamp and commit_timestamp force set to " << oldestTimestamp;
        } else {
            invariantRocksOK(_db->SetTimeStamp(rocksdb::TimeStampType::kOldest, ts, force));
            if (_oldestTimestamp.load() < oldestTimestamp.asULL()) {
                _oldestTimestamp.store(oldestTimestamp.asULL());
            }
            LOG(2) << "oldest_timestamp set to " << oldestTimestamp;
        }
    }

    Timestamp RocksEngine::_calculateHistoryLagFromStableTimestamp(Timestamp stableTimestamp) {
        // The oldest_timestamp should lag behind the stable_timestamp by
        // 'targetSnapshotHistoryWindowInSeconds' seconds.

        if (stableTimestamp.getSecs() <
            static_cast<unsigned>(
                snapshotWindowParams.targetSnapshotHistoryWindowInSeconds.load())) {
            // The history window is larger than the timestamp history thus far. We must wait for
            // the history to reach the window size before moving oldest_timestamp forward.
            return Timestamp();
        }

        Timestamp calculatedOldestTimestamp(
            stableTimestamp.getSecs() -
                snapshotWindowParams.targetSnapshotHistoryWindowInSeconds.load(),
            stableTimestamp.getInc());

        if (calculatedOldestTimestamp.asULL() <= _oldestTimestamp.load()) {
            // The stable_timestamp is not far enough ahead of the oldest_timestamp for the
            // oldest_timestamp to be moved forward: the window is still too small.
            return Timestamp();
        }

        return calculatedOldestTimestamp;
    }

    bool RocksEngine::supportsRecoverToStableTimestamp() const { return _keepDataHistory; }

    bool RocksEngine::supportsRecoveryTimestamp() const { return true; }

    bool RocksEngine::canRecoverToStableTimestamp() const {
        static const std::uint64_t allowUnstableCheckpointsSentinel =
            static_cast<std::uint64_t>(Timestamp::kAllowUnstableCheckpointsSentinel.asULL());
        const std::uint64_t initialDataTimestamp = _initialDataTimestamp.load();
        // Illegal to be called when the dataset is incomplete.
        invariant(initialDataTimestamp > allowUnstableCheckpointsSentinel);
        return _stableTimestamp.load() >= initialDataTimestamp;
    }

    StatusWith<Timestamp> RocksEngine::recoverToStableTimestamp(OperationContext* opCtx) {
        if (!supportsRecoverToStableTimestamp()) {
            severe() << "Rocksdb is configured to not support recover to a stable timestamp";
            fassertFailed(ErrorCodes::InternalError);
        }

        if (!canRecoverToStableTimestamp()) {
            Timestamp stableTS = getStableTimestamp();
            Timestamp initialDataTS = getInitialDataTimestamp();
            return Status(ErrorCodes::UnrecoverableRollbackError,
                          str::stream()
                          << "No stable timestamp available to recover to. Initial data timestamp: "
                          << initialDataTS.toString() << ", Stable timestamp: " << stableTS.toString());
        }

        LOG_FOR_ROLLBACK(0) << "RocksEngine::RecoverToStableTimestamp syncing size storer to disk.";
        _counterManager->sync();

        if (_journalFlusher) {
            _journalFlusher->shutdown();
            _journalFlusher.reset();
        }
        LOG_FOR_ROLLBACK(0) << "RocksEngine::RecoverToStableTimestamp shutting down journal";
        const auto stableTimestamp = getStableTimestamp();
        const auto initialDataTimestamp = getInitialDataTimestamp();

        LOG_FOR_ROLLBACK(0) << "Rolling back to the stable timestamp. StableTimestamp: "
            << stableTimestamp
            << " Initial Data Timestamp: " << initialDataTimestamp;
        auto s = _compactionScheduler->rollbackToStable(_defaultCf.get());
        if (!s.isOK()) {
            return {ErrorCodes::UnrecoverableRollbackError,
                    str::stream() << "Error rolling back to stable. Err: " << s};
        }

        setInitialDataTimestamp(initialDataTimestamp);
        setStableTimestamp(stableTimestamp, false);

        if (_durable) {
            _journalFlusher = stdx::make_unique<RocksJournalFlusher>(_durabilityManager.get());
            _journalFlusher->go();
        }
        _counterManager.reset(new RocksCounterManager(_db.get(), _defaultCf.get(),
                                                      rocksGlobalOptions.crashSafeCounters));
        return {stableTimestamp};
    }

    boost::optional<Timestamp> RocksEngine::getRecoveryTimestamp() const {
        if (!supportsRecoveryTimestamp()) {
            severe() << "RocksDB is configured to not support providing a recovery timestamp";
            fassertFailed(ErrorCodes::InternalError);
        }

        if (_recoveryTimestamp.isNull()) {
            return boost::none;
        }

        return _recoveryTimestamp;
    }

    /**
     * Returns a timestamp value that is at or before the last checkpoint. Everything before
     * this
     * value is guaranteed to be persisted on disk and replication recovery will not need to
     * replay documents with an earlier time.
     */
    boost::optional<Timestamp> RocksEngine::getLastStableRecoveryTimestamp() const {
        if (!supportsRecoverToStableTimestamp()) {
            severe() << "Rocksdb is configured to not support recover to a stable timestamp";
            fassertFailed(ErrorCodes::InternalError);
        }

        const auto ret = _lastStableCheckpointTimestamp.load();
        if (ret) {
            return Timestamp(ret);
        }

        if (!_recoveryTimestamp.isNull()) {
            return _recoveryTimestamp;
        }

        return boost::none;
    }

    bool RocksEngine::supportsReadConcernSnapshot() const { return true; }

    bool RocksEngine::supportsReadConcernMajority() const { return _keepDataHistory; }

    void RocksEngine::startOplogManager(OperationContext* opCtx,
                                        RocksRecordStore* oplogRecordStore) {
        stdx::lock_guard<Latch> lock(_oplogManagerMutex);
        if (_oplogManagerCount == 0)
            _oplogManager->start(opCtx, oplogRecordStore);
        _oplogManagerCount++;
    }

    void RocksEngine::haltOplogManager() {
        stdx::unique_lock<Latch> lock(_oplogManagerMutex);
        invariant(_oplogManagerCount > 0);
        _oplogManagerCount--;
        if (_oplogManagerCount == 0) {
            _oplogManager->halt();
        }
    }

    void RocksEngine::replicationBatchIsComplete() const { _oplogManager->triggerJournalFlush(); }
}  // namespace mongo
