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
#include "mongo/db/storage/kv/kv_prefix.h"
#include "mongo/db/storage/recovery_unit_test_harness.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/death_test.h"
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

    class RocksRecoveryUnitHarnessHelper final : public RecoveryUnitHarnessHelper {
    public:
        RocksRecoveryUnitHarnessHelper()
            : _dbpath("rocks_test"),
              _engine(_dbpath.path(), true /* durable */, 3 /* kRocksFormatVersion */,
                      false /* readOnly */) {
            repl::ReplicationCoordinator::set(
                getGlobalServiceContext(),
                std::unique_ptr<repl::ReplicationCoordinator>(new repl::ReplicationCoordinatorMock(
                    getGlobalServiceContext(), repl::ReplSettings())));
        }

        virtual ~RocksRecoveryUnitHarnessHelper() {}

        std::unique_ptr<RecoveryUnit> newRecoveryUnit() final {
            return std::unique_ptr<RecoveryUnit>(_engine.newRecoveryUnit());
        }

        virtual std::unique_ptr<RecordStore> createRecordStore(OperationContext* opCtx,
                                                               const std::string& ns) final {
            return stdx::make_unique<RocksRecordStore>(&_engine, _engine.getCf_ForTest(ns), opCtx, ns, "1", "prefix");
        }

    private:
        unittest::TempDir _dbpath;
        ClockSourceMock _cs;

        RocksEngine _engine;
    };

    std::unique_ptr<HarnessHelper> makeHarnessHelper() {
        return stdx::make_unique<RocksRecoveryUnitHarnessHelper>();
    }

    MONGO_INITIALIZER(RegisterHarnessFactory)(InitializerContext* const) {
        mongo::registerHarnessHelperFactory(makeHarnessHelper);
        return Status::OK();
    }

    class RocksRecoveryUnitTestFixture : public unittest::Test {
    public:
        typedef std::pair<ServiceContext::UniqueClient, ServiceContext::UniqueOperationContext>
            ClientAndCtx;

        ClientAndCtx makeClientAndOpCtx(RecoveryUnitHarnessHelper* harnessHelper,
                                        const std::string& clientName) {
            auto sc = harnessHelper->serviceContext();
            auto client = sc->makeClient(clientName);
            auto opCtx = client->makeOperationContext();
            opCtx->setRecoveryUnit(harnessHelper->newRecoveryUnit().release(),
                                   WriteUnitOfWork::RecoveryUnitState::kNotInUnitOfWork);
            return std::make_pair(std::move(client), std::move(opCtx));
        }

        void setUp() override {
            harnessHelper = newRecoveryUnitHarnessHelper();
            clientAndCtx = makeClientAndOpCtx(harnessHelper.get(), "writer");
            ru = checked_cast<RocksRecoveryUnit*>(clientAndCtx.second->recoveryUnit());
        }

        std::unique_ptr<RecoveryUnitHarnessHelper> harnessHelper;
        ClientAndCtx clientAndCtx;
        RocksRecoveryUnit* ru;

    private:
    };

    TEST_F(RocksRecoveryUnitTestFixture, SetReadSource) {
        ru->setTimestampReadSource(RecoveryUnit::ReadSource::kProvided, Timestamp(1, 1));
        ASSERT_EQ(RecoveryUnit::ReadSource::kProvided, ru->getTimestampReadSource());
        ASSERT_EQ(Timestamp(1, 1), ru->getPointInTimeReadTimestamp());
    }

    TEST_F(RocksRecoveryUnitTestFixture, LocalReadOnADocumentBeingPreparedTriggersPrepareConflict) {
        // NOTE(wolfkdy): mongoRocks-r4.0.x does not need prepare
    }

    TEST_F(RocksRecoveryUnitTestFixture,
           AvailableReadOnADocumentBeingPreparedDoesNotTriggerPrepareConflict) {
        // NOTE(wolfkdy): mongoRocks-r4.0.x does not need prepare
    }

    TEST_F(RocksRecoveryUnitTestFixture, WriteOnADocumentBeingPreparedTriggersWTRollback) {
        // NOTE(wolfkdy): mongoRocks-r4.0.x does not need prepare
    }

    TEST_F(RocksRecoveryUnitTestFixture,
           ChangeIsPassedEmptyLastTimestampSetOnCommitWithNoTimestamp) {
        boost::optional<Timestamp> commitTs = boost::none;
        auto opCtx = clientAndCtx.second.get();
        {
            WriteUnitOfWork wuow(opCtx);
            opCtx->recoveryUnit()->onCommit(
                [&](boost::optional<Timestamp> commitTime) { commitTs = commitTime; });
            wuow.commit();
        }
        ASSERT(!commitTs);
    }

    TEST_F(RocksRecoveryUnitTestFixture, ChangeIsPassedLastTimestampSetOnCommit) {
        boost::optional<Timestamp> commitTs = boost::none;
        auto opCtx = clientAndCtx.second.get();
        Timestamp ts1(5, 5);
        Timestamp ts2(6, 6);
        {
            WriteUnitOfWork wuow(opCtx);
            opCtx->recoveryUnit()->onCommit(
                [&](boost::optional<Timestamp> commitTime) { commitTs = commitTime; });
            ASSERT_OK(opCtx->recoveryUnit()->setTimestamp(ts1));
            ASSERT(!commitTs);
            ASSERT_OK(opCtx->recoveryUnit()->setTimestamp(ts2));
            ASSERT(!commitTs);
            // NOTE(wolfkdy): totdb only support setting monotonic commit-ts
            // ASSERT_OK(opCtx->recoveryUnit()->setTimestamp(ts1));
            // ASSERT(!commitTs);
            wuow.commit();
            ASSERT_EQ(*commitTs, ts2);
        }
        ASSERT_EQ(*commitTs, ts2);
    }

    TEST_F(RocksRecoveryUnitTestFixture, ChangeIsNotPassedLastTimestampSetOnAbort) {
        boost::optional<Timestamp> commitTs = boost::none;
        auto opCtx = clientAndCtx.second.get();
        Timestamp ts1(5, 5);
        {
            WriteUnitOfWork wuow(opCtx);
            opCtx->recoveryUnit()->onCommit(
                [&](boost::optional<Timestamp> commitTime) { commitTs = commitTime; });
            ASSERT_OK(opCtx->recoveryUnit()->setTimestamp(ts1));
            ASSERT(!commitTs);
        }
        ASSERT(!commitTs);
    }

    TEST_F(RocksRecoveryUnitTestFixture, ChangeIsPassedCommitTimestamp) {
        boost::optional<Timestamp> commitTs = boost::none;
        auto opCtx = clientAndCtx.second.get();
        Timestamp ts1(5, 5);

        opCtx->recoveryUnit()->setCommitTimestamp(ts1);
        ASSERT(!commitTs);

        {
            WriteUnitOfWork wuow(opCtx);
            opCtx->recoveryUnit()->onCommit(
                [&](boost::optional<Timestamp> commitTime) { commitTs = commitTime; });
            ASSERT(!commitTs);
            wuow.commit();
            ASSERT_EQ(*commitTs, ts1);
        }
        ASSERT_EQ(*commitTs, ts1);
    }

    TEST_F(RocksRecoveryUnitTestFixture, ChangeIsNotPassedCommitTimestampIfCleared) {
        boost::optional<Timestamp> commitTs = boost::none;
        auto opCtx = clientAndCtx.second.get();
        Timestamp ts1(5, 5);

        opCtx->recoveryUnit()->setCommitTimestamp(ts1);
        ASSERT(!commitTs);
        opCtx->recoveryUnit()->clearCommitTimestamp();
        ASSERT(!commitTs);

        {
            WriteUnitOfWork wuow(opCtx);
            opCtx->recoveryUnit()->onCommit(
                [&](boost::optional<Timestamp> commitTime) { commitTs = commitTime; });
            ASSERT(!commitTs);
            wuow.commit();
        }
        ASSERT(!commitTs);
    }

    TEST_F(RocksRecoveryUnitTestFixture, ChangeIsPassedNewestCommitTimestamp) {
        boost::optional<Timestamp> commitTs = boost::none;
        auto opCtx = clientAndCtx.second.get();
        Timestamp ts1(5, 5);
        Timestamp ts2(6, 6);

        opCtx->recoveryUnit()->setCommitTimestamp(ts2);
        ASSERT(!commitTs);
        opCtx->recoveryUnit()->clearCommitTimestamp();
        ASSERT(!commitTs);
        opCtx->recoveryUnit()->setCommitTimestamp(ts1);
        ASSERT(!commitTs);

        {
            WriteUnitOfWork wuow(opCtx);
            opCtx->recoveryUnit()->onCommit(
                [&](boost::optional<Timestamp> commitTime) { commitTs = commitTime; });
            ASSERT(!commitTs);
            wuow.commit();
            ASSERT_EQ(*commitTs, ts1);
        }
        ASSERT_EQ(*commitTs, ts1);
    }

    TEST_F(RocksRecoveryUnitTestFixture, ChangeIsNotPassedCommitTimestampOnAbort) {
        boost::optional<Timestamp> commitTs = boost::none;
        auto opCtx = clientAndCtx.second.get();
        Timestamp ts1(5, 5);

        opCtx->recoveryUnit()->setCommitTimestamp(ts1);
        ASSERT(!commitTs);

        {
            WriteUnitOfWork wuow(opCtx);
            opCtx->recoveryUnit()->onCommit(
                [&](boost::optional<Timestamp> commitTime) { commitTs = commitTime; });
            ASSERT(!commitTs);
        }
        ASSERT(!commitTs);
    }

    TEST_F(RocksRecoveryUnitTestFixture, CommitTimestampBeforeSetTimestampOnCommit) {
        boost::optional<Timestamp> commitTs = boost::none;
        auto opCtx = clientAndCtx.second.get();
        Timestamp ts1(5, 5);
        Timestamp ts2(6, 6);

        opCtx->recoveryUnit()->setCommitTimestamp(ts2);
        ASSERT(!commitTs);

        {
            WriteUnitOfWork wuow(opCtx);
            opCtx->recoveryUnit()->onCommit(
                [&](boost::optional<Timestamp> commitTime) { commitTs = commitTime; });
            ASSERT(!commitTs);
            wuow.commit();
            ASSERT_EQ(*commitTs, ts2);
        }
        ASSERT_EQ(*commitTs, ts2);
        opCtx->recoveryUnit()->clearCommitTimestamp();

        {
            WriteUnitOfWork wuow(opCtx);
            opCtx->recoveryUnit()->onCommit(
                [&](boost::optional<Timestamp> commitTime) { commitTs = commitTime; });
            ASSERT_OK(opCtx->recoveryUnit()->setTimestamp(ts1));
            ASSERT_EQ(*commitTs, ts2);
            wuow.commit();
            ASSERT_EQ(*commitTs, ts1);
        }
        ASSERT_EQ(*commitTs, ts1);
    }

    TEST_F(RocksRecoveryUnitTestFixture, CommitTimestampAfterSetTimestampOnCommit) {
        boost::optional<Timestamp> commitTs = boost::none;
        auto opCtx = clientAndCtx.second.get();
        Timestamp ts1(5, 5);
        Timestamp ts2(6, 6);

        {
            WriteUnitOfWork wuow(opCtx);
            opCtx->recoveryUnit()->onCommit(
                [&](boost::optional<Timestamp> commitTime) { commitTs = commitTime; });
            ASSERT(!commitTs);
            ASSERT_OK(opCtx->recoveryUnit()->setTimestamp(ts2));
            ASSERT(!commitTs);
            wuow.commit();
            ASSERT_EQ(*commitTs, ts2);
        }
        ASSERT_EQ(*commitTs, ts2);

        opCtx->recoveryUnit()->setCommitTimestamp(ts1);
        ASSERT_EQ(*commitTs, ts2);

        {
            WriteUnitOfWork wuow(opCtx);
            opCtx->recoveryUnit()->onCommit(
                [&](boost::optional<Timestamp> commitTime) { commitTs = commitTime; });
            ASSERT_EQ(*commitTs, ts2);
            wuow.commit();
            ASSERT_EQ(*commitTs, ts1);
        }
        ASSERT_EQ(*commitTs, ts1);
    }

    TEST_F(RocksRecoveryUnitTestFixture, CommitTimestampBeforeSetTimestampOnAbort) {
        boost::optional<Timestamp> commitTs = boost::none;
        auto opCtx = clientAndCtx.second.get();
        Timestamp ts1(5, 5);
        Timestamp ts2(6, 6);

        opCtx->recoveryUnit()->setCommitTimestamp(ts2);
        ASSERT(!commitTs);

        {
            WriteUnitOfWork wuow(opCtx);
            opCtx->recoveryUnit()->onCommit(
                [&](boost::optional<Timestamp> commitTime) { commitTs = commitTime; });
            ASSERT(!commitTs);
        }
        ASSERT(!commitTs);
        opCtx->recoveryUnit()->clearCommitTimestamp();

        {
            WriteUnitOfWork wuow(opCtx);
            opCtx->recoveryUnit()->onCommit(
                [&](boost::optional<Timestamp> commitTime) { commitTs = commitTime; });
            ASSERT_OK(opCtx->recoveryUnit()->setTimestamp(ts1));
            ASSERT(!commitTs);
        }
        ASSERT(!commitTs);
    }

    TEST_F(RocksRecoveryUnitTestFixture, CommitTimestampAfterSetTimestampOnAbort) {
        boost::optional<Timestamp> commitTs = boost::none;
        auto opCtx = clientAndCtx.second.get();
        Timestamp ts1(5, 5);
        Timestamp ts2(6, 6);

        {
            WriteUnitOfWork wuow(opCtx);
            opCtx->recoveryUnit()->onCommit(
                [&](boost::optional<Timestamp> commitTime) { commitTs = commitTime; });
            ASSERT(!commitTs);
            ASSERT_OK(opCtx->recoveryUnit()->setTimestamp(ts2));
            ASSERT(!commitTs);
        }
        ASSERT(!commitTs);

        opCtx->recoveryUnit()->setCommitTimestamp(ts1);
        ASSERT(!commitTs);

        {
            WriteUnitOfWork wuow(opCtx);
            opCtx->recoveryUnit()->onCommit(
                [&](boost::optional<Timestamp> commitTime) { commitTs = commitTime; });
            ASSERT(!commitTs);
        }
        ASSERT(!commitTs);
    }

    TEST_F(RocksRecoveryUnitTestFixture, CommitUnitOfWork) {
        auto opCtx = clientAndCtx.second.get();
        const auto rs = harnessHelper->createRecordStore(opCtx, "table1");
        ru->beginUnitOfWork(opCtx);
        StatusWith<RecordId> s = rs->insertRecord(opCtx, "data", 4, Timestamp(), false);
        ASSERT_TRUE(s.isOK());
        ASSERT_EQUALS(1, rs->numRecords(opCtx));
        ru->commitUnitOfWork();
        RecordData rd;
        ASSERT_TRUE(rs->findRecord(opCtx, s.getValue(), &rd));
    }

    TEST_F(RocksRecoveryUnitTestFixture, AbortUnitOfWork) {
        auto opCtx = clientAndCtx.second.get();
        const auto rs = harnessHelper->createRecordStore(opCtx, "table1");
        ru->beginUnitOfWork(opCtx);
        StatusWith<RecordId> s = rs->insertRecord(opCtx, "data", 4, Timestamp(), false);
        ASSERT_TRUE(s.isOK());
        ASSERT_EQUALS(1, rs->numRecords(opCtx));
        ru->abortUnitOfWork();
        ASSERT_FALSE(rs->findRecord(opCtx, s.getValue(), nullptr));
    }

    DEATH_TEST_F(RocksRecoveryUnitTestFixture, CommitMustBeInUnitOfWork, "invariant") {
        auto opCtx = clientAndCtx.second.get();
        opCtx->recoveryUnit()->commitUnitOfWork();
    }

    DEATH_TEST_F(RocksRecoveryUnitTestFixture, AbortMustBeInUnitOfWork, "invariant") {
        auto opCtx = clientAndCtx.second.get();
        opCtx->recoveryUnit()->abortUnitOfWork();
    }

    DEATH_TEST_F(RocksRecoveryUnitTestFixture, PrepareMustBeInUnitOfWork, "invariant") {
        auto opCtx = clientAndCtx.second.get();
        opCtx->recoveryUnit()->prepareUnitOfWork();
    }

    DEATH_TEST_F(RocksRecoveryUnitTestFixture, WaitUntilDurableMustBeOutOfUnitOfWork, "invariant") {
        auto opCtx = clientAndCtx.second.get();
        opCtx->recoveryUnit()->beginUnitOfWork(opCtx);
        opCtx->recoveryUnit()->waitUntilDurable();
    }

    DEATH_TEST_F(RocksRecoveryUnitTestFixture, AbandonSnapshotMustBeOutOfUnitOfWork, "invariant") {
        auto opCtx = clientAndCtx.second.get();
        opCtx->recoveryUnit()->beginUnitOfWork(opCtx);
        opCtx->recoveryUnit()->abandonSnapshot();
    }

}  // namespace mongo
