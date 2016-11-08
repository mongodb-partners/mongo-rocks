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

#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/storage/record_store_test_harness.h"
#include "mongo/unittest/unittest.h"
#include "mongo/unittest/temp_dir.h"

#include "rocks_record_store.h"
#include "rocks_recovery_unit.h"
#include "rocks_transaction.h"
#include "rocks_snapshot_manager.h"

namespace mongo {

    using std::string;

    class RocksRecordStoreHarnessHelper final : public HarnessHelper {
    public:
        RocksRecordStoreHarnessHelper() : _tempDir(_testNamespace) {
            boost::filesystem::remove_all(_tempDir.path());
            rocksdb::DB* db;
            rocksdb::Options options;
            options.create_if_missing = true;
            auto s = rocksdb::DB::Open(options, _tempDir.path(), &db);
            ASSERT(s.ok());
            _db.reset(db);
            _counterManager.reset(new RocksCounterManager(_db.get(), true));
            _durabilityManager.reset(new RocksDurabilityManager(_db.get(), true));
        }

        virtual std::unique_ptr<RecordStore> newNonCappedRecordStore() {
          return newNonCappedRecordStore("foo.bar");
        }
        std::unique_ptr<RecordStore> newNonCappedRecordStore(const std::string& ns) {
            return stdx::make_unique<RocksRecordStore>(ns, "1", _db.get(), _counterManager.get(),
                                                       _durabilityManager.get(), "prefix");
        }

        std::unique_ptr<RecordStore> newCappedRecordStore(int64_t cappedMaxSize,
                                                          int64_t cappedMaxDocs) final {
            return newCappedRecordStore("a.b", cappedMaxSize, cappedMaxDocs);
        }

        std::unique_ptr<RecordStore> newCappedRecordStore(const std::string& ns,
                                                          int64_t cappedMaxSize,
                                                          int64_t cappedMaxDocs) {
            return stdx::make_unique<RocksRecordStore>(ns, "1", _db.get(), _counterManager.get(),
                                                       _durabilityManager.get(), "prefix", true,
                                                       cappedMaxSize, cappedMaxDocs);
        }

        RecoveryUnit* newRecoveryUnit() final {
            return new RocksRecoveryUnit(&_transactionEngine, &_snapshotManager, _db.get(),
                                         _counterManager.get(), nullptr, _durabilityManager.get(),
                                         true);
        }

        bool supportsDocLocking() final {
          return true;
        }

    private:
        string _testNamespace = "mongo-rocks-record-store-test";
        unittest::TempDir _tempDir;
        std::unique_ptr<rocksdb::DB> _db;
        RocksTransactionEngine _transactionEngine;
        RocksSnapshotManager _snapshotManager;
        std::unique_ptr<RocksDurabilityManager> _durabilityManager;
        std::unique_ptr<RocksCounterManager> _counterManager;
    };

    std::unique_ptr<HarnessHelper> newHarnessHelper() {
        return stdx::make_unique<RocksRecordStoreHarnessHelper>();
    }

    TEST(RocksRecordStoreTest, Isolation1 ) {
        std::unique_ptr<HarnessHelper> harnessHelper(newHarnessHelper());
        std::unique_ptr<RecordStore> rs(harnessHelper->newNonCappedRecordStore());

        RecordId loc1;
        RecordId loc2;

        {
            ServiceContext::UniqueOperationContext opCtx( harnessHelper->newOperationContext() );
            {
                WriteUnitOfWork uow( opCtx.get() );

                StatusWith<RecordId> res = rs->insertRecord( opCtx.get(), "a", 2, false );
                ASSERT_OK( res.getStatus() );
                loc1 = res.getValue();

                res = rs->insertRecord( opCtx.get(), "a", 2, false );
                ASSERT_OK( res.getStatus() );
                loc2 = res.getValue();

                uow.commit();
            }
        }

        {
            ServiceContext::UniqueOperationContext t1( harnessHelper->newOperationContext() );
            auto client2 = harnessHelper->serviceContext()->makeClient("c2");
            auto t2 = harnessHelper->newOperationContext(client2.get());

            std::unique_ptr<WriteUnitOfWork> w1( new WriteUnitOfWork( t1.get() ) );
            std::unique_ptr<WriteUnitOfWork> w2( new WriteUnitOfWork( t2.get() ) );

            rs->dataFor( t1.get(), loc1 );
            rs->dataFor( t2.get(), loc1 );

            ASSERT_OK( rs->updateRecord( t1.get(), loc1, "b", 2, false, NULL ) );
            ASSERT_OK( rs->updateRecord( t1.get(), loc2, "B", 2, false, NULL ) );

            // this should throw
            ASSERT_THROWS(rs->updateRecord(t2.get(), loc1, "c", 2, false, NULL),
                          WriteConflictException);

            w1->commit(); // this should succeed
        }
    }

    TEST(RocksRecordStoreTest, Isolation2 ) {
        std::unique_ptr<HarnessHelper> harnessHelper( newHarnessHelper() );
        std::unique_ptr<RecordStore> rs( harnessHelper->newNonCappedRecordStore() );

        RecordId loc1;
        RecordId loc2;

        {
            ServiceContext::UniqueOperationContext opCtx( harnessHelper->newOperationContext() );
            {
                WriteUnitOfWork uow( opCtx.get() );

                StatusWith<RecordId> res = rs->insertRecord( opCtx.get(), "a", 2, false );
                ASSERT_OK( res.getStatus() );
                loc1 = res.getValue();

                res = rs->insertRecord( opCtx.get(), "a", 2, false );
                ASSERT_OK( res.getStatus() );
                loc2 = res.getValue();

                uow.commit();
            }
        }

        {
            ServiceContext::UniqueOperationContext t1( harnessHelper->newOperationContext() );
            auto client2 = harnessHelper->serviceContext()->makeClient("c2");
            auto t2 = harnessHelper->newOperationContext(client2.get());

            // ensure we start transactions
            rs->dataFor( t1.get(), loc2 );
            rs->dataFor( t2.get(), loc2 );

            {
                WriteUnitOfWork w( t1.get() );
                ASSERT_OK( rs->updateRecord( t1.get(), loc1, "b", 2, false, NULL ) );
                w.commit();
            }

            {
                WriteUnitOfWork w( t2.get() );
                ASSERT_EQUALS(string("a"), rs->dataFor(t2.get(), loc1).data());
                // this should fail as our version of loc1 is too old
                ASSERT_THROWS(rs->updateRecord(t2.get(), loc1, "c", 2, false, NULL),
                              WriteConflictException);
            }
        }
    }

    StatusWith<RecordId> insertBSON(ServiceContext::UniqueOperationContext& opCtx,
                                   std::unique_ptr<RecordStore>& rs,
                                   const Timestamp& opTime) {
        BSONObj obj = BSON( "ts" << opTime );
        WriteUnitOfWork wuow(opCtx.get());
        RocksRecordStore* rrs = dynamic_cast<RocksRecordStore*>(rs.get());
        invariant( rrs );
        Status status = rrs->oplogDiskLocRegister( opCtx.get(), opTime );
        if (!status.isOK())
            return StatusWith<RecordId>( status );
        StatusWith<RecordId> res = rs->insertRecord(opCtx.get(),
                                                   obj.objdata(),
                                                   obj.objsize(),
                                                   false);
        if (res.isOK())
            wuow.commit();
        return res;
    }

    TEST(RocksRecordStoreTest, OplogHack) {
        RocksRecordStoreHarnessHelper harnessHelper;
        std::unique_ptr<RecordStore> rs(harnessHelper.newNonCappedRecordStore("local.oplog.foo"));
        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

            // always illegal
            ASSERT_EQ(insertBSON(opCtx, rs, Timestamp(2,-1)).getStatus(),
                  ErrorCodes::BadValue);

            {
                BSONObj obj = BSON("not_ts" << Timestamp(2,1));
                ASSERT_EQ(rs->insertRecord(opCtx.get(), obj.objdata(), obj.objsize(),
                                           false ).getStatus(),
                          ErrorCodes::BadValue);

                obj = BSON( "ts" << "not an Timestamp" );
                ASSERT_EQ(rs->insertRecord(opCtx.get(), obj.objdata(), obj.objsize(),
                                           false ).getStatus(),
                          ErrorCodes::BadValue);
            }

            // currently dasserts
            // ASSERT_EQ(insertBSON(opCtx, rs, BSON("ts" << Timestamp(-2,1))).getStatus(),
            // ErrorCodes::BadValue);

            // success cases
            ASSERT_EQ(insertBSON(opCtx, rs, Timestamp(1,1)).getValue(),
                      RecordId(1,1));

            ASSERT_EQ(insertBSON(opCtx, rs, Timestamp(1,2)).getValue(),
                      RecordId(1,2));

            ASSERT_EQ(insertBSON(opCtx, rs, Timestamp(2,2)).getValue(),
                      RecordId(2,2));
        }

        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
            // find start
            ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(0,1)), RecordId()); // nothing <=
            ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(2,1)), RecordId(1,2)); // between
            ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(2,2)), RecordId(2,2)); // ==
            ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(2,3)), RecordId(2,2)); // > highest
        }

        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
            rs->temp_cappedTruncateAfter(opCtx.get(), RecordId(2,2),  false); // no-op
        }

        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
            ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(2,3)), RecordId(2,2));
        }

        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
            rs->temp_cappedTruncateAfter(opCtx.get(), RecordId(1,2),  false); // deletes 2,2
        }

        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
            ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(2,3)), RecordId(1,2));
        }

        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
            rs->temp_cappedTruncateAfter(opCtx.get(), RecordId(1,2),  true); // deletes 1,2
        }

        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
            ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(2,3)), RecordId(1,1));
        }

        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
            WriteUnitOfWork wuow(opCtx.get());
            ASSERT_OK(rs->truncate(opCtx.get())); // deletes 1,1 and leaves collection empty
            wuow.commit();
        }

        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
            ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(2,3)), RecordId());
        }
    }

    void testDeleteSeekExactRecord(bool forward, bool capped) {
        RocksRecordStoreHarnessHelper harnessHelper;
        std::unique_ptr<RecordStore> rs;
        if (capped) {
            rs = harnessHelper.newCappedRecordStore("local.oplog.foo", 100000, -1);
        } else {
            rs = harnessHelper.newNonCappedRecordStore("local.oplog.foo");
        }
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
        ASSERT_EQ(insertBSON(opCtx, rs, Timestamp(1,1)).getValue(),
                  RecordId(1,1));

        ASSERT_EQ(insertBSON(opCtx, rs, Timestamp(1,2)).getValue(),
                  RecordId(1,2));

        ASSERT_EQ(insertBSON(opCtx, rs, Timestamp(2,2)).getValue(),
                  RecordId(2,2));

        auto cursor = rs->getCursor(opCtx.get(), forward);
        auto record = cursor->seekExact(RecordId(1,2));
        ASSERT(record);
        cursor->save();
        rs->deleteRecord(opCtx.get(), RecordId(1,2));
        cursor->restore();

        if (!capped) {
            auto next = cursor->next();
            ASSERT(next);
            ASSERT_EQ(next->id, forward ? RecordId(2,2) : RecordId(1,1));
        }
        ASSERT(!cursor->next());
    }

    TEST(RocksRecordStoreTest, DeleteSeekExactRecord_Forward_Capped) {
        testDeleteSeekExactRecord(true, true);
    }

    TEST(RocksRecordStoreTest, DeleteSeekExactRecord_Forward_NonCapped) {
        testDeleteSeekExactRecord(true, false);
    }

    TEST(RocksRecordStoreTest, DeleteSeekExactRecord_Reversed_Capped) {
        testDeleteSeekExactRecord(false, true);
    }

    TEST(RocksRecordStoreTest, DeleteSeekExactRecord_Reversed_NonCapped) {
        testDeleteSeekExactRecord(false, false);
    }

    TEST(RocksRecordStoreTest, OplogHackOnNonOplog) {
        RocksRecordStoreHarnessHelper harnessHelper;
        std::unique_ptr<RecordStore> rs(
                harnessHelper.newNonCappedRecordStore("local.NOT_oplog.foo"));

        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        BSONObj obj = BSON( "ts" << Timestamp(2,-1) );
        {
            WriteUnitOfWork wuow( opCtx.get() );
            ASSERT_OK(rs->insertRecord(opCtx.get(), obj.objdata(),
                                       obj.objsize(), false ).getStatus());
            wuow.commit();
        }
        ASSERT_TRUE(rs->oplogStartHack(opCtx.get(), RecordId(0,1)) == boost::none);
    }

    TEST(RocksRecordStoreTest, CappedOrder) {
        std::unique_ptr<RocksRecordStoreHarnessHelper> harnessHelper(
                new RocksRecordStoreHarnessHelper());
        std::unique_ptr<RecordStore> rs(harnessHelper->newCappedRecordStore("a.b", 100000,10000));

        RecordId loc1;

        { // first insert a document
            ServiceContext::UniqueOperationContext opCtx( harnessHelper->newOperationContext() );
            {
                WriteUnitOfWork uow( opCtx.get() );
                StatusWith<RecordId> res = rs->insertRecord( opCtx.get(), "a", 2, false );
                ASSERT_OK( res.getStatus() );
                loc1 = res.getValue();
                uow.commit();
            }
        }

        {
            ServiceContext::UniqueOperationContext opCtx( harnessHelper->newOperationContext() );
            auto cursor = rs->getCursor(opCtx.get());
            auto record = cursor->seekExact(loc1);
            ASSERT( record );
            ASSERT_EQ( loc1, record->id );
            ASSERT( !cursor->next() );
        }

        {
            // now we insert 2 docs, but commit the 2nd one fiirst
            // we make sure we can't find the 2nd until the first is commited
            ServiceContext::UniqueOperationContext t1( harnessHelper->newOperationContext() );
            std::unique_ptr<WriteUnitOfWork> w1( new WriteUnitOfWork( t1.get() ) );
            rs->insertRecord( t1.get(), "b", 2, false );
            // do not commit yet

            { // create 2nd doc
                auto client2 = harnessHelper->serviceContext()->makeClient("c2");
                auto t2 = harnessHelper->newOperationContext(client2.get());
                {
                    WriteUnitOfWork w2( t2.get() );
                    rs->insertRecord( t2.get(), "c", 2, false );
                    w2.commit();
                }
            }

            { // state should be the same
                auto client2 = harnessHelper->serviceContext()->makeClient("c2");
                auto opCtx = harnessHelper->newOperationContext(client2.get());
                auto cursor = rs->getCursor(opCtx.get());
                auto record = cursor->seekExact(loc1);
                ASSERT( record );
                ASSERT_EQ( loc1, record->id );
                ASSERT( !cursor->next() );
            }

            w1->commit();
        }

        { // now all 3 docs should be visible
            ServiceContext::UniqueOperationContext opCtx( harnessHelper->newOperationContext() );
            auto cursor = rs->getCursor(opCtx.get());
            auto record = cursor->seekExact(loc1);
            ASSERT( record );
            ASSERT_EQ( loc1, record->id );
            ASSERT( cursor->next() );
            ASSERT( cursor->next() );
            ASSERT( !cursor->next() );
        }
    }

    RecordId _oplogOrderInsertOplog( OperationContext* txn,
                                    std::unique_ptr<RecordStore>& rs,
                                    int inc ) {
        Timestamp opTime = Timestamp(5,inc);
        RocksRecordStore* rrs = dynamic_cast<RocksRecordStore*>(rs.get());
        Status status = rrs->oplogDiskLocRegister( txn, opTime );
        ASSERT_OK( status );
        BSONObj obj = BSON( "ts" << opTime );
        StatusWith<RecordId> res = rs->insertRecord( txn, obj.objdata(), obj.objsize(), false );
        ASSERT_OK( res.getStatus() );
        return res.getValue();
    }

    TEST(RocksRecordStoreTest, OplogOrder) {
        std::unique_ptr<RocksRecordStoreHarnessHelper> harnessHelper(
            new RocksRecordStoreHarnessHelper());
        std::unique_ptr<RecordStore> rs(
            harnessHelper->newCappedRecordStore("local.oplog.foo", 100000, -1));
        {
            const RocksRecordStore* rrs = dynamic_cast<RocksRecordStore*>(rs.get());
            ASSERT( rrs->isOplog() );
        }

        RecordId loc1;

        { // first insert a document
            ServiceContext::UniqueOperationContext opCtx( harnessHelper->newOperationContext() );
            {
                WriteUnitOfWork uow( opCtx.get() );
                loc1 = _oplogOrderInsertOplog( opCtx.get(), rs, 1 );
                uow.commit();
            }
        }

        {
            ServiceContext::UniqueOperationContext opCtx( harnessHelper->newOperationContext() );
            auto cursor = rs->getCursor(opCtx.get());
            auto record = cursor->seekExact(loc1);
            ASSERT( record );
            ASSERT_EQ( loc1, record->id );
            ASSERT(!cursor->next());
        }

        {
            // now we insert 2 docs, but commit the 2nd one first.
            // we make sure we can't find the 2nd until the first is committed.
            ServiceContext::UniqueOperationContext earlyReader(
                harnessHelper->newOperationContext());
            auto earlyCursor = rs->getCursor(earlyReader.get());
            ASSERT_EQ(earlyCursor->seekExact(loc1)->id, loc1);
            earlyCursor->save();
            earlyReader->recoveryUnit()->abandonSnapshot();

            auto client1 = harnessHelper->serviceContext()->makeClient("c1");
            auto t1 = harnessHelper->newOperationContext(client1.get());
            WriteUnitOfWork w1(t1.get());
            _oplogOrderInsertOplog(t1.get(), rs, 20);
            // do not commit yet

            {  // create 2nd doc
                auto client2 = harnessHelper->serviceContext()->makeClient("c2");
                auto t2 = harnessHelper->newOperationContext(client2.get());
                {
                    WriteUnitOfWork w2(t2.get());
                    _oplogOrderInsertOplog(t2.get(), rs, 30);
                    w2.commit();
                }
            }

            {  // Other operations should not be able to see 2nd doc until w1 commits.
                earlyCursor->restore();
                ASSERT(!earlyCursor->next());

                auto client2 = harnessHelper->serviceContext()->makeClient("c2");
                auto opCtx = harnessHelper->newOperationContext(client2.get());
                auto cursor = rs->getCursor(opCtx.get());
                auto record = cursor->seekExact(loc1);
                ASSERT_EQ(loc1, record->id);
                ASSERT(!cursor->next());
            }

            w1.commit();
        }

        rs->waitForAllEarlierOplogWritesToBeVisible(harnessHelper->newOperationContext().get());

        {  // now all 3 docs should be visible
            ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
            auto cursor = rs->getCursor(opCtx.get());
            auto record = cursor->seekExact(loc1);
            ASSERT_EQ(loc1, record->id);
            ASSERT(cursor->next());
            ASSERT(cursor->next());
            ASSERT(!cursor->next());
        }

        // Rollback the last two oplog entries, then insert entries with older optimes and ensure that
        // the visibility rules aren't violated. See SERVER-21645
        {
            ServiceContext::UniqueOperationContext txn(harnessHelper->newOperationContext());
            rs->temp_cappedTruncateAfter(txn.get(), loc1, /*inclusive*/ false);
        }

        {
            // Now we insert 2 docs with timestamps earlier than before, but commit the 2nd one first.
            // We make sure we can't find the 2nd until the first is commited.
            ServiceContext::UniqueOperationContext earlyReader(harnessHelper->newOperationContext());
            auto earlyCursor = rs->getCursor(earlyReader.get());
            ASSERT_EQ(earlyCursor->seekExact(loc1)->id, loc1);
            earlyCursor->save();
            earlyReader->recoveryUnit()->abandonSnapshot();

            auto client1 = harnessHelper->serviceContext()->makeClient("c1");
            auto t1 = harnessHelper->newOperationContext(client1.get());
            WriteUnitOfWork w1(t1.get());
            _oplogOrderInsertOplog( t1.get(), rs, 2 );
            // do not commit yet

            { // create 2nd doc
                auto client2 = harnessHelper->serviceContext()->makeClient("c2");
                auto t2 = harnessHelper->newOperationContext(client2.get());
                {
                    WriteUnitOfWork w2( t2.get() );
                    _oplogOrderInsertOplog( t2.get(), rs, 3 );
                    w2.commit();
                }
            }

            {  // Other operations should not be able to see 2nd doc until w1 commits.
                ASSERT(earlyCursor->restore());
                ASSERT(!earlyCursor->next());

                auto client2 = harnessHelper->serviceContext()->makeClient("c2");
                auto opCtx = harnessHelper->newOperationContext(client2.get());
                auto cursor = rs->getCursor(opCtx.get());
                auto record = cursor->seekExact(loc1);
                ASSERT( record );
                ASSERT_EQ( loc1, record->id );
                ASSERT(!cursor->next());
            }

            w1.commit();
        }

        rs->waitForAllEarlierOplogWritesToBeVisible(harnessHelper->newOperationContext().get());

        { // now all 3 docs should be visible
            ServiceContext::UniqueOperationContext opCtx( harnessHelper->newOperationContext() );
            auto cursor = rs->getCursor(opCtx.get());
            auto record = cursor->seekExact(loc1);
            ASSERT( record );
            ASSERT_EQ( loc1, record->id );
            ASSERT( cursor->next() );
            ASSERT( cursor->next() );
            ASSERT( !cursor->next() );
        }
    }

}
