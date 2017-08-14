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

#include <rocksdb/version.h>
#include <rocksdb/cache.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/experimental.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/table.h>
#include <rocksdb/convenience.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/utilities/checkpoint.h>

#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/locker.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/storage/journal_listener.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/background.h"
#include "mongo/util/concurrency/idle_thread_block.h"
#include "mongo/util/concurrency/ticketholder.h"
#include "mongo/util/log.h"
#include "mongo/util/processinfo.h"

#include "rocks_counter_manager.h"
#include "rocks_global_options.h"
#include "rocks_record_store.h"
#include "rocks_recovery_unit.h"
#include "rocks_index.h"
#include "rocks_util.h"

#define ROCKS_TRACE log()

namespace mongo {

    class RocksEngine::RocksJournalFlusher : public BackgroundJob {
    public:
        explicit RocksJournalFlusher(RocksDurabilityManager* durabilityManager)
            : BackgroundJob(false /* deleteSelf */), _durabilityManager(durabilityManager) {}

        virtual std::string name() const { return "RocksJournalFlusher"; }

        virtual void run() {
            Client::initThread(name().c_str());

            LOG(1) << "starting " << name() << " thread";

            while (!_shuttingDown.load()) {
                try {
                    _durabilityManager->waitUntilDurable(false);
                } catch (const UserException& e) {
                    invariant(e.getCode() == ErrorCodes::ShutdownInProgress);
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
        // ServerParameter to limit concurrency, to prevent thousands of threads running
        // concurrent searches and thus blocking the entire DB.
        class RocksTicketServerParameter : public ServerParameter {
            MONGO_DISALLOW_COPYING(RocksTicketServerParameter);

        public:
            RocksTicketServerParameter(TicketHolder* holder, const std::string& name)
                : ServerParameter(ServerParameterSet::getGlobal(), name, true, true), _holder(holder) {};
            virtual void append(OperationContext* opCtx, BSONObjBuilder& b, const std::string& name) {
                b.append(name, _holder->outof());
            }
            virtual Status set(const BSONElement& newValueElement) {
                if (!newValueElement.isNumber())
                    return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be a number");
                return _set(newValueElement.numberInt());
            }
            virtual Status setFromString(const std::string& str) {
                int num = 0;
                Status status = parseNumberFromString(str, &num);
                if (!status.isOK())
                    return status;
                return _set(num);
            }

        private:
            Status _set(int newNum) {
                if (newNum <= 0) {
                    return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be > 0");
                }

                return _holder->resize(newNum);
            }

            TicketHolder* _holder;
        };

        TicketHolder openWriteTransaction(128);
        RocksTicketServerParameter openWriteTransactionParam(&openWriteTransaction,
                                                        "rocksdbConcurrentWriteTransactions");

        TicketHolder openReadTransaction(128);
        RocksTicketServerParameter openReadTransactionParam(&openReadTransaction,
                                                       "rocksdbConcurrentReadTransactions");

    }  // anonymous namespace

    // first four bytes are the default prefix 0
    const std::string RocksEngine::kMetadataPrefix("\0\0\0\0metadata-", 12);

    RocksEngine::RocksEngine(const std::string& path, bool durable, int formatVersion,
                             bool readOnly)
        : _path(path), _durable(durable), _formatVersion(formatVersion), _maxPrefix(0) {
        {  // create block cache
            uint64_t cacheSizeGB = rocksGlobalOptions.cacheSizeGB;
            if (cacheSizeGB == 0) {
                ProcessInfo pi;
                unsigned long long memSizeMB = pi.getMemSizeMB();
                if (memSizeMB > 0) {
                    // reserve 1GB for system and binaries, and use 30% of the rest
                    double cacheMB = (memSizeMB - 1024) * 0.3;
                    cacheSizeGB = static_cast<uint64_t>(cacheMB / 1024);
                }
                if (cacheSizeGB < 1) {
                    cacheSizeGB = 1;
                }
            }
            _block_cache = rocksdb::NewLRUCache(cacheSizeGB * 1024 * 1024 * 1024LL, 6);
        }
        _maxWriteMBPerSec = rocksGlobalOptions.maxWriteMBPerSec;
        _rateLimiter.reset(
            rocksdb::NewGenericRateLimiter(static_cast<int64_t>(_maxWriteMBPerSec) * 1024 * 1024));
        if (rocksGlobalOptions.counters) {
            _statistics = rocksdb::CreateDBStatistics();
        }

        // used in building options for the db
        _compactionScheduler.reset(new RocksCompactionScheduler());

        // open DB
        rocksdb::DB* db;
        rocksdb::Status s;
        if (readOnly) {
            s = rocksdb::DB::OpenForReadOnly(_options(), path, &db);
        } else {
            s = rocksdb::DB::Open(_options(), path, &db);
        }
        invariantRocksOK(s);
        _db.reset(db);

        _counterManager.reset(
            new RocksCounterManager(_db.get(), rocksGlobalOptions.crashSafeCounters));

        // open iterator
        std::unique_ptr<rocksdb::Iterator> iter(_db->NewIterator(rocksdb::ReadOptions()));

        // find maxPrefix
        iter->SeekToLast();
        if (iter->Valid()) {
            // otherwise the DB is empty, so we just keep it at 0
            bool ok = extractPrefix(iter->key(), &_maxPrefix);
            // this is DB corruption here
            invariant(ok);
        }

        // load ident to prefix map. also update _maxPrefix if there's any prefix bigger than
        // current _maxPrefix
        {
            stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
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

                _identMap[StringData(ident.data(), ident.size())] =
                    identConfig.getOwned();

                _maxPrefix = std::max(_maxPrefix, identPrefix);
            }
        }

        // just to be extra sure. we need this if last collection is oplog -- in that case we
        // reserve prefix+1 for oplog key tracker
        ++_maxPrefix;

        // start compaction thread and load dropped prefixes
        _compactionScheduler->start(_db.get());
        _compactionScheduler->loadDroppedPrefixes(iter.get());

        _durabilityManager.reset(new RocksDurabilityManager(_db.get(), _durable));

        if (_durable) {
            _journalFlusher = stdx::make_unique<RocksJournalFlusher>(_durabilityManager.get());
            _journalFlusher->go();
        }

        Locker::setGlobalThrottling(&openReadTransaction, &openWriteTransaction);
    }

    RocksEngine::~RocksEngine() { cleanShutdown(); }

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

    RecoveryUnit* RocksEngine::newRecoveryUnit() {
        return new RocksRecoveryUnit(&_transactionEngine, &_snapshotManager, _db.get(),
                                     _counterManager.get(), _compactionScheduler.get(),
                                     _durabilityManager.get(), _durable);
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
                stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
                oplogTrackerPrefix = ++_maxPrefix;
            }
            // we also need to write out the new prefix to the database. this is just an
            // optimization
            std::string encodedPrefix(encodePrefix(oplogTrackerPrefix));
            s = rocksToMongoStatus(
                _db->Put(rocksdb::WriteOptions(), encodedPrefix, rocksdb::Slice()));
        }
        return s;
    }

    std::unique_ptr<RecordStore> RocksEngine::getRecordStore(OperationContext* opCtx, StringData ns,
                                             StringData ident, const CollectionOptions& options) {
        if (NamespaceString::oplog(ns)) {
            _oplogIdent = ident.toString();
        }

        auto config = _getIdentConfig(ident);
        std::string prefix = _extractPrefix(config);

        std::unique_ptr<RocksRecordStore> recordStore =
            options.capped
                ? stdx::make_unique<RocksRecordStore>(
                      ns, ident, _db.get(), _counterManager.get(), _durabilityManager.get(),
                      _compactionScheduler.get(), prefix,
                      true, options.cappedSize ? options.cappedSize : 4096,  // default size
                      options.cappedMaxDocs ? options.cappedMaxDocs : -1)
                : stdx::make_unique<RocksRecordStore>(ns, ident, _db.get(), _counterManager.get(),
                                                      _durabilityManager.get(), _compactionScheduler.get(),
                                                      prefix);

        {
            stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
            _identCollectionMap[ident] = recordStore.get();
        }
        return std::move(recordStore);
    }

    Status RocksEngine::createSortedDataInterface(OperationContext* opCtx, StringData ident,
                                                  const IndexDescriptor* desc) {
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

        RocksIndexBase* index;
        if (desc->unique()) {
            index = new RocksUniqueIndex(_db.get(), prefix, ident.toString(),
                                         Ordering::make(desc->keyPattern()), std::move(config),
                                         desc->parentNS(), desc->indexName(), desc->isPartial());
        } else {
            auto si = new RocksStandardIndex(_db.get(), prefix, ident.toString(),
                                             Ordering::make(desc->keyPattern()), std::move(config));
            if (rocksGlobalOptions.singleDeleteIndex) {
                si->enableSingleDelete();
            }
            index = si;
        }
        {
            stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
            _identIndexMap[ident] = index;
        }
        return index;
    }

    // cannot be rolled back
    Status RocksEngine::dropIdent(OperationContext* opCtx, StringData ident) {
        rocksdb::WriteBatch wb;
        wb.Delete(kMetadataPrefix + ident.toString());

        // calculate which prefixes we need to drop
        std::vector<std::string> prefixesToDrop;
        prefixesToDrop.push_back(_extractPrefix(_getIdentConfig(ident)));
        if (_oplogIdent == ident.toString()) {
            // if we're dropping oplog, we also need to drop keys from RocksOplogKeyTracker (they
            // are stored at prefix+1)
            prefixesToDrop.push_back(rocksGetNextPrefix(prefixesToDrop[0]));
        }

        // we need to make sure this is on disk before starting to delete data in compactions
        rocksdb::WriteOptions syncOptions;
        syncOptions.sync = true;
        auto s = _compactionScheduler->dropPrefixesAtomic(prefixesToDrop, syncOptions, wb);

        if (s.isOK()) {
            // remove from map
            stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
            _identMap.erase(ident);
        }
        return s;
    }

    bool RocksEngine::hasIdent(OperationContext* opCtx, StringData ident) const {
        stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
        return _identMap.find(ident) != _identMap.end();
    }

    std::vector<std::string> RocksEngine::getAllIdents(OperationContext* opCtx) const {
        std::vector<std::string> indents;
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
        _db.reset();
    }

    void RocksEngine::setJournalListener(JournalListener* jl) {
        _durabilityManager->setJournalListener(jl);
    }

    int64_t RocksEngine::getIdentSize(OperationContext* opCtx, StringData ident) {
        stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);

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
        return rocksToMongoStatus(_db->PauseBackgroundWork());
    }

    void RocksEngine::endBackup(OperationContext* opCtx) { _db->ContinueBackgroundWork(); }

    void RocksEngine::setMaxWriteMBPerSec(int maxWriteMBPerSec) {
        _maxWriteMBPerSec = maxWriteMBPerSec;
        _rateLimiter->SetBytesPerSecond(static_cast<int64_t>(_maxWriteMBPerSec) * 1024 * 1024);
    }

    Status RocksEngine::hotBackup(const std::string& path) {
        // Our files are in db/ directory to avoid collision with Mongo's files
        return backup(path + "/db");
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

    // non public api
    Status RocksEngine::_createIdent(StringData ident, BSONObjBuilder* configBuilder) {
        BSONObj config;
        uint32_t prefix = 0;
        {
            stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
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

        auto s = _db->Put(rocksdb::WriteOptions(), kMetadataPrefix + ident.toString(),
                          rocksdb::Slice(config.objdata(), config.objsize()));

        if (s.ok()) {
            // As an optimization, add a key <prefix> to the DB
            std::string encodedPrefix(encodePrefix(prefix));
            s = _db->Put(rocksdb::WriteOptions(), encodedPrefix, rocksdb::Slice());
        }

        return rocksToMongoStatus(s);
    }

    BSONObj RocksEngine::_getIdentConfig(StringData ident) {
        stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
        auto identIter = _identMap.find(ident);
        invariant(identIter != _identMap.end());
        return identIter->second.copy();
    }

    std::string RocksEngine::_extractPrefix(const BSONObj& config) {
        return encodePrefix(config.getField("prefix").numberInt());
    }

    rocksdb::Options RocksEngine::_options() const {
        // default options
        rocksdb::Options options;
        options.rate_limiter = _rateLimiter;
        rocksdb::BlockBasedTableOptions table_options;
        table_options.block_cache = _block_cache;
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
        table_options.block_size = 16 * 1024; // 16KB
        table_options.format_version = 2;
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

        options.write_buffer_size = 64 * 1024 * 1024;  // 64MB
        options.level0_slowdown_writes_trigger = 8;
        options.max_write_buffer_number = 4;
        options.max_background_compactions = 8;
        options.max_background_flushes = 2;
        options.target_file_size_base = 64 * 1024 * 1024; // 64MB
        options.soft_rate_limit = 2.5;
        options.hard_rate_limit = 3;
        options.level_compaction_dynamic_level_bytes = true;
        options.max_bytes_for_level_base = 512 * 1024 * 1024;  // 512 MB
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

        options.statistics = _statistics;

        // create the DB if it's not already present
        options.create_if_missing = true;
        options.wal_dir = _path + "/journal";

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
}
