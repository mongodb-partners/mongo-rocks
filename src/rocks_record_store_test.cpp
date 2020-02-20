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

#include "mongo/platform/basic.h"

#include <boost/filesystem/operations.hpp>
#include <memory>
#include <vector>

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/utilities/totransaction.h>
#include <rocksdb/utilities/totransaction_db.h>

#include "mongo/base/checked_cast.h"
#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/json.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/repl/repl_settings.h"
#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/kv/kv_engine_test_harness.h"
#include "mongo/db/storage/kv/kv_prefix.h"
#include "mongo/db/storage/record_store_test_harness.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/clock_source_mock.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/scopeguard.h"

#include "rocks_compaction_scheduler.h"
#include "rocks_oplog_manager.h"
#include "rocks_record_store.h"
#include "rocks_recovery_unit.h"
#include "rocks_snapshot_manager.h"

namespace mongo {

    using std::string;

    class RocksRecordStoreHarnessHelper final : public RecordStoreHarnessHelper {
    public:
        RocksRecordStoreHarnessHelper()
            : _dbpath("rocks_test"),
              _engine(_dbpath.path(), true /* durable */, 3 /* kRocksFormatVersion */,
                      false /* readOnly */) {
            repl::ReplicationCoordinator::set(serviceContext(),
                                              std::make_unique<repl::ReplicationCoordinatorMock>(
                                                  serviceContext(), repl::ReplSettings()));
        }

        ~RocksRecordStoreHarnessHelper() {}

        virtual std::unique_ptr<RecordStore> newNonCappedRecordStore() {
            return newNonCappedRecordStore("a.b");
        }

        std::unique_ptr<RecordStore> newNonCappedRecordStore(const std::string& ns) {
            RocksRecoveryUnit* ru = dynamic_cast<RocksRecoveryUnit*>(_engine.newRecoveryUnit());
            OperationContextNoop opCtx(ru);
            return stdx::make_unique<RocksRecordStore>(
                &opCtx, ns, "1", _engine.getDB(), _engine.getOplogManager(),
                _engine.getCounterManager(), _engine.getCompactionScheduler(), "prefix");
        }

        std::unique_ptr<RecordStore> newCappedRecordStore(int64_t cappedMaxSize,
                                                          int64_t cappedMaxDocs) final {
            return newCappedRecordStore("a.b", cappedMaxSize, cappedMaxDocs);
        }

        std::unique_ptr<RecordStore> newCappedRecordStore(const std::string& ns,
                                                          int64_t cappedMaxSize,
                                                          int64_t cappedMaxDocs) {
            RocksRecoveryUnit* ru = dynamic_cast<RocksRecoveryUnit*>(_engine.newRecoveryUnit());
            OperationContextNoop opCtx(ru);
            return stdx::make_unique<RocksRecordStore>(
                &opCtx, ns, "1", _engine.getDB(), _engine.getOplogManager(),
                _engine.getCounterManager(), _engine.getCompactionScheduler(), "prefix",
                cappedMaxSize, cappedMaxDocs);
        }

        std::unique_ptr<RecoveryUnit> newRecoveryUnit() final {
            return stdx::make_unique<RocksRecoveryUnit>(
                _engine.getDB(), _engine.getOplogManager(),
                checked_cast<RocksSnapshotManager*>(_engine.getSnapshotManager()),
                _engine.getCounterManager(), _engine.getCompactionScheduler(),
                _engine.getDurabilityManager(), true /* durale */);
        }

        bool supportsDocLocking() final { return true; }

    private:
        unittest::TempDir _dbpath;
        ClockSourceMock _cs;

        RocksEngine _engine;
    };

    std::unique_ptr<HarnessHelper> makeHarnessHelper() {
        return stdx::make_unique<RocksRecordStoreHarnessHelper>();
    }

    MONGO_INITIALIZER(RegisterHarnessFactory)(InitializerContext* const) {
        mongo::registerHarnessHelperFactory(makeHarnessHelper);
        return Status::OK();
    }

}  // namespace mongo
