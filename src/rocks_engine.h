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

#include <list>
#include <map>
#include <memory>
#include <string>

#include <boost/optional.hpp>

#include <rocksdb/cache.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/statistics.h>
#include <rocksdb/status.h>

#include "mongo/bson/ordering.h"
#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/util/string_map.h"

#include "rocks_compaction_scheduler.h"
#include "rocks_counter_manager.h"
#include "rocks_durability_manager.h"
#include "rocks_oplog_manager.h"
#include "rocks_snapshot_manager.h"

namespace rocksdb {
    class ColumnFamilyHandle;
    struct ColumnFamilyDescriptor;
    struct ColumnFamilyOptions;
    class DB;
    class Comparator;
    class Iterator;
    struct Options;
    struct ReadOptions;
}  // namespace rocksdb

namespace mongo {

    struct CollectionOptions;
    class RocksIndexBase;
    class RocksRecordStore;
    class JournalListener;
    class RocksOplogManager;

    class RocksEngine final : public KVEngine {
        RocksEngine(const RocksEngine&) = delete;
        RocksEngine& operator=(const RocksEngine&) = delete;

    public:
        static const int kDefaultJournalDelayMillis;
        RocksEngine(const std::string& path, bool durable, int formatVersion, bool readOnly);
        ~RocksEngine();

        virtual bool supportsDocLocking() const override { return true; }

        virtual bool supportsDirectoryPerDB() const override { return false; }

        virtual bool isDurable() const override { return _durable; }

        virtual bool isEphemeral() const override { return false; }

        void setOldestActiveTransactionTimestampCallback(
            StorageEngine::OldestActiveTransactionTimestampCallback callback) override;

        RecoveryUnit* newRecoveryUnit() override;

        Status createRecordStore(OperationContext* opCtx, StringData ns, StringData ident,
                                 const CollectionOptions& options) override;

        std::unique_ptr<RecordStore> getRecordStore(OperationContext* opCtx, StringData ns,
                                                    StringData ident,
                                                    const CollectionOptions& options) override;

        std::unique_ptr<RecordStore> makeTemporaryRecordStore(OperationContext* opCtx,
                                                              StringData ident) override;

        Status createSortedDataInterface(OperationContext* opCtx,
                                         const CollectionOptions& collOptions, StringData ident,
                                         const IndexDescriptor* desc) override;

        SortedDataInterface* getSortedDataInterface(OperationContext* opCtx, StringData ident,
                                                    const IndexDescriptor* desc) override;

        Status dropIdent(OperationContext* opCtx, StringData ident) override;

        virtual Status okToRename(OperationContext* opCtx, StringData fromNS, StringData toNS,
                                  StringData ident,
                                  const RecordStore* originalRecordStore) const override;

        int flushAllFiles(OperationContext* opCtx, bool sync) override;

        Status beginBackup(OperationContext* opCtx) override;

        void endBackup(OperationContext* opCtx) override;

        int64_t getIdentSize(OperationContext* opCtx, StringData ident);

        Status repairIdent(OperationContext* opCtx, StringData ident) { return Status::OK(); }

        bool hasIdent(OperationContext* opCtx, StringData ident) const override;

        std::vector<std::string> getAllIdents(OperationContext* opCtx) const override;

        void cleanShutdown();

        SnapshotManager* getSnapshotManager() const final {
            return (SnapshotManager*)&_snapshotManager;
        }

        RocksSnapshotManager* getRocksSnapshotManager() { return &_snapshotManager; }

        void setJournalListener(JournalListener* jl);

        void setStableTimestamp(Timestamp stableTimestamp, bool force) override;

        void setInitialDataTimestamp(Timestamp initialDataTimestamp) override;

        void setOldestTimestampFromStable() override;

        /**
         * This method will set the oldest timestamp and commit timestamp to the input value.
         * Callers
         * must be serialized along with `setStableTimestamp`. If force=false, this function does
         * not
         * set the commit timestamp and may choose to lag the oldest timestamp.
         */
        void setOldestTimestamp(Timestamp oldestTimestamp, bool force) override;

        bool supportsRecoverToStableTimestamp() const override;

        bool supportsRecoveryTimestamp() const override;

        StatusWith<Timestamp> recoverToStableTimestamp(OperationContext* opCtx) override;

        boost::optional<Timestamp> getRecoveryTimestamp() const override;

        /**
         * Returns a stable timestamp value that is guaranteed to exist on recoverToStableTimestamp.
         * Replication recovery will not need to replay documents with an earlier time.
         *
         * Only returns a stable timestamp when it has advanced to >= the initial data timestamp.
         * Replication recoverable rollback is unsafe when stable < initial during repl initial sync
         * due
         * to initial sync's cloning phase without timestamps.
         *
         * For the persisted mode of this engine, further guarantees a stable timestamp value that
         * is at
         * or before the last checkpoint. Everything before this value is guaranteed to be persisted
         * on
         * disk. This supports replication recovery on restart.
         */
        boost::optional<Timestamp> getLastStableRecoveryTimestamp() const override;

        Timestamp getAllDurableTimestamp() const override;

        Timestamp getOldestOpenReadTimestamp() const override;

        bool supportsReadConcernSnapshot() const final;

        /*
         * This function is called when replication has completed a batch.  In this function, we
         * refresh our oplog visiblity read-at-timestamp value.
         */
        void replicationBatchIsComplete() const override;

        bool isCacheUnderPressure(OperationContext* opCtx) const override;

        bool supportsReadConcernMajority() const final;

        /*
         * An oplog manager is always accessible, but this method will start the background thread
         * to
         * control oplog entry visibility for reads.
         *
         * On mongod, the background thread will be started when the first oplog record store is
         * created, and stopped when the last oplog record store is destroyed, at shutdown time. For
         * unit tests, the background thread may be started and stopped multiple times as tests
         * create
         * and destroy oplog record stores.
         */
        void startOplogManager(OperationContext* opCtx, RocksRecordStore* oplogRecordStore);

        void haltOplogManager();

        /*
         * Always returns a non-nil pointer. However, the WiredTigerOplogManager may not have been
         * initialized and its background refreshing thread may not be running.
         *
         * A caller that wants to get the oplog read timestamp, or call
         * `waitForAllEarlierOplogWritesToBeVisible`, is advised to first see if the oplog manager
         * is
         * running with a call to `isRunning`.
         *
         * A caller that simply wants to call `triggerJournalFlush` may do so without concern.
         */
        RocksOplogManager* getOplogManager() const { return _oplogManager.get(); }

        // TODO(cuixin): setInitRsOplogBackgroundThreadCallback is used for test, rocks no need

        /**
         * Initializes a background job to remove excess documents in the oplog collections.
         * This applies to the capped collections in the local.oplog.* namespaces (specifically
         * local.oplog.rs for replica sets and local.oplog.$main for master/slave replication).
         * Returns true if a background job is running for the namespace.
         */
        static bool initRsOplogBackgroundThread(StringData ns);
        static void appendGlobalStats(BSONObjBuilder& b);

        Timestamp getStableTimestamp() const override;
        Timestamp getOldestTimestamp() const override;
        Timestamp getCheckpointTimestamp() const override;
        Timestamp getInitialDataTimestamp() const;
    
        /**
         * Returns the minimum possible Timestamp value in the oplog that replication may need for
         * recovery in the event of a crash. This value gets updated every time a checkpoint is
         * completed. This value is typically a lagged version of what's needed for rollback.
         *
         * Returns boost::none when called on an ephemeral database.
         */
        boost::optional<Timestamp> getOplogNeededForCrashRecovery() const final;

        // rocks specific api

        rocksdb::TOTransactionDB* getDB() { return _db.get(); }
        const rocksdb::TOTransactionDB* getDB() const { return _db.get(); }
        size_t getBlockCacheUsage() const { return _blockCache->GetUsage(); }
        std::shared_ptr<rocksdb::Cache> getBlockCache() { return _blockCache; }

        RocksCounterManager* getCounterManager() const { return _counterManager.get(); }

        RocksCompactionScheduler* getCompactionScheduler() const {
            return _compactionScheduler.get();
        }

        RocksDurabilityManager* getDurabilityManager() const { return _durabilityManager.get(); }

        int getMaxWriteMBPerSec() const { return _maxWriteMBPerSec; }
        void setMaxWriteMBPerSec(int maxWriteMBPerSec);

        Status backup(const std::string& path);

        rocksdb::Statistics* getStatistics() const { return _statistics.get(); }

        std::map<int, std::vector<uint64_t>> getDefaultCFNumEntries() const;

        rocksdb::ColumnFamilyHandle* getOplogCFHandle() const { return _oplogCf.get(); }
        rocksdb::ColumnFamilyHandle* getDefaultCfHandle() const { return _defaultCf.get(); }
        bool canRecoverToStableTimestamp() const;
        rocksdb::ColumnFamilyHandle* getDefaultCf_ForTest() const { return _defaultCf.get(); }
        rocksdb::ColumnFamilyHandle* getOplogCf_ForTest() const { return _oplogCf.get(); }
        rocksdb::ColumnFamilyHandle* getCf_ForTest(const std::string& ns) const {
            return NamespaceString::oplog(ns)? getOplogCf_ForTest() : getDefaultCf_ForTest();
        }

    private:
        Status _createIdent(StringData ident, BSONObjBuilder* configBuilder);
        BSONObj _getIdentConfig(StringData ident);
        BSONObj _tryGetIdentConfig(StringData ident);
        std::string _extractPrefix(const BSONObj& config);

        /**
         * Uses the 'stableTimestamp', the 'targetSnapshotHistoryWindowInSeconds' setting and the
         * current _oldestTimestamp to calculate what the new oldest_timestamp should be, in order
         * to
         * maintain a window of available snapshots on the storage engine from oldest to stable
         * timestamp.
         *
         * If the returned Timestamp isNull(), oldest_timestamp should not be moved forward.
         */
        Timestamp _calculateHistoryLagFromStableTimestamp(Timestamp stableTimestamp);

        mutable Mutex _oldestActiveTransactionTimestampCallbackMutex =
            MONGO_MAKE_LATCH("::_oldestActiveTransactionTimestampCallbackMutex");
        StorageEngine::OldestActiveTransactionTimestampCallback
            _oldestActiveTransactionTimestampCallback;

        rocksdb::Options _options(bool isOplog, bool trimHisotry) const;

        void _initDatabase();

        std::string _path;
        std::unique_ptr<rocksdb::TOTransactionDB> _db;
        std::shared_ptr<rocksdb::Cache> _blockCache;
        int _maxWriteMBPerSec;
        std::shared_ptr<rocksdb::RateLimiter> _rateLimiter;
        // can be nullptr
        std::shared_ptr<rocksdb::Statistics> _statistics;

        const bool _durable;
        const int _formatVersion;

        // ident map stores mapping from ident to a BSON config
        mutable Mutex _identMapMutex = MONGO_MAKE_LATCH("RocksEngine::_identMapMutex");
        typedef StringMap<BSONObj> IdentMap;
        IdentMap _identMap;
        std::string _oplogIdent;

        // protected by _identMapMutex
        uint32_t _maxPrefix;

        // If _keepDataHistory is true, then the storage engine keeps all history after the oldest
        // timestamp, and RocksEngine is responsible for advancing the oldest timestamp. If
        // _keepDataHistory is false (i.e. majority reads are disabled), then we only keep history
        // after the "no holes point", and RocksOplogManager is responsible for advancing the oldest
        // timestamp.
        const bool _keepDataHistory = true;

        // _identObjectMapMutex protects both _identIndexMap and _identCollectionMap. It should
        // never be locked together with _identMapMutex
        mutable Mutex _identObjectMapMutex = MONGO_MAKE_LATCH("RocksEngine::_identObjectMapMutex");
        // mapping from ident --> index object. we don't own the object
        StringMap<RocksIndexBase*> _identIndexMap;
        // mapping from ident --> collection object
        StringMap<RocksRecordStore*> _identCollectionMap;

        RocksSnapshotManager _snapshotManager;

        // CounterManages manages counters like numRecords and dataSize for record stores
        std::unique_ptr<RocksCounterManager> _counterManager;

        std::unique_ptr<RocksCompactionScheduler> _compactionScheduler;

        // _defaultCf is rocksdb's "default" cf, it holds everything other than oplog
        // 1. user-collections, user-indexes
        // 2. mongoRocks' metadata(prefixed with kMetadataPrefix)
        // 3. dropped data(prefixed with kDroppedPrefix)
        // 4. table's counters and sizers
        std::unique_ptr<rocksdb::ColumnFamilyHandle> _defaultCf;

        // _oplogCf holds oplogs
        std::unique_ptr<rocksdb::ColumnFamilyHandle> _oplogCf;

        // Mutex to protect use of _oplogManagerCount by this instance of KV engine.
        mutable Mutex _oplogManagerMutex = MONGO_MAKE_LATCH("::_oplogManagerMutex");
        std::size_t _oplogManagerCount = 0;
        std::unique_ptr<RocksOplogManager> _oplogManager;

        static const std::string kMetadataPrefix;
        static const std::string kStablePrefix;

        std::unique_ptr<RocksDurabilityManager> _durabilityManager;
        class RocksJournalFlusher;
        std::unique_ptr<RocksJournalFlusher> _journalFlusher;  // Depends on _durabilityManager

        // Tracks the stable and oldest timestamps we've set on the storage engine.
        AtomicWord<std::uint64_t> _oldestTimestamp;
        AtomicWord<std::uint64_t> _stableTimestamp;
        AtomicWord<std::uint64_t> _initialDataTimestamp;
        AtomicWord<std::uint64_t> _lastStableCheckpointTimestamp;
        Timestamp _recoveryTimestamp;
    };
}  // namespace mongo
