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
#include <string>

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/storage/sorted_data_interface_test_harness.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"

#include "rocks_engine.h"
#include "rocks_index.h"
#include "rocks_recovery_unit.h"
#include "rocks_transaction.h"
#include "rocks_snapshot_manager.h"

namespace mongo {

    using std::string;

    class RocksIndexHarness final : public HarnessHelper {
    public:
        RocksIndexHarness() : _order(Ordering::make(BSONObj())), _tempDir(_testNamespace) {
            boost::filesystem::remove_all(_tempDir.path());
            rocksdb::DB* db;
            rocksdb::Options options;
            options.create_if_missing = true;
            auto s = rocksdb::DB::Open(options, _tempDir.path(), &db);
            ASSERT(s.ok());
            _db.reset(db);
            _counterManager = stdx::make_unique<RocksCounterManager>(_db.get(), true);
            _durabilityManager.reset(new RocksDurabilityManager(_db.get(), true));
        }

        std::unique_ptr<SortedDataInterface> newSortedDataInterface(bool unique) {
            BSONObjBuilder configBuilder;
            RocksIndexBase::generateConfig(&configBuilder, 3, IndexDescriptor::IndexVersion::kV2);
            if (unique) {
                return stdx::make_unique<RocksUniqueIndex>(_db.get(), "prefix", "ident", _order,
                                                           configBuilder.obj());
            } else {
                return stdx::make_unique<RocksStandardIndex>(_db.get(), "prefix", "ident", _order,
                                                             configBuilder.obj());
            }
        }

        std::unique_ptr<RecoveryUnit> newRecoveryUnit() {
            return stdx::make_unique<RocksRecoveryUnit>(&_transactionEngine, &_snapshotManager,
                                                        _db.get(), _counterManager.get(),
                                                        nullptr, _durabilityManager.get(), true);
        }

    private:
        Ordering _order;
        string _testNamespace = "mongo-rocks-sorted-data-test";
        unittest::TempDir _tempDir;
        std::unique_ptr<rocksdb::DB> _db;
        RocksTransactionEngine _transactionEngine;
        RocksSnapshotManager _snapshotManager;
        std::unique_ptr<RocksDurabilityManager> _durabilityManager;
        std::unique_ptr<RocksCounterManager> _counterManager;
    };

    std::unique_ptr<HarnessHelper> newHarnessHelper() {
        return stdx::make_unique<RocksIndexHarness>();
    }

    TEST(RocksIndexTest, Isolation) {
        const std::unique_ptr<HarnessHelper> harnessHelper(newHarnessHelper());

        const std::unique_ptr<SortedDataInterface>
        sorted(harnessHelper->newSortedDataInterface(true));

        {
            const ServiceContext::UniqueOperationContext opCtx(
                harnessHelper->newOperationContext());
            ASSERT(sorted->isEmpty(opCtx.get()));
        }

        {
            const ServiceContext::UniqueOperationContext opCtx(
                harnessHelper->newOperationContext());
            {
                WriteUnitOfWork uow(opCtx.get());

                ASSERT_OK(sorted->insert(opCtx.get(), key1, loc1, false));
                ASSERT_OK(sorted->insert(opCtx.get(), key2, loc2, false));

                uow.commit();
            }
        }

        {
            const ServiceContext::UniqueOperationContext t1(harnessHelper->newOperationContext());
            const auto client2 = harnessHelper->serviceContext()->makeClient("c2");
            const auto t2 = harnessHelper->newOperationContext(client2.get());

            const std::unique_ptr<WriteUnitOfWork> w1(new WriteUnitOfWork(t1.get()));
            const std::unique_ptr<WriteUnitOfWork> w2(new WriteUnitOfWork(t2.get()));

            ASSERT_OK(sorted->insert(t1.get(), key3, loc3, false));
            ASSERT_OK(sorted->insert(t2.get(), key4, loc4, false));

            // this should throw
            ASSERT_THROWS(sorted->insert(t2.get(), key3, loc5, false), WriteConflictException);

            w1->commit();  // this should succeed
        }

        {
            const ServiceContext::UniqueOperationContext t1(harnessHelper->newOperationContext());
            const auto client2 = harnessHelper->serviceContext()->makeClient("c2");
            const auto t2 = harnessHelper->newOperationContext(client2.get());

            const std::unique_ptr<WriteUnitOfWork> w2(new WriteUnitOfWork(t2.get()));
            // ensure we start w2 transaction
            ASSERT_OK(sorted->insert(t2.get(), key4, loc4, false));

            {
                const std::unique_ptr<WriteUnitOfWork> w1(new WriteUnitOfWork(t1.get()));

                {
                    WriteUnitOfWork w(t1.get());
                    ASSERT_OK(sorted->insert(t1.get(), key5, loc3, false));
                    w.commit();
                }
                w1->commit();
            }

            // this should throw
            ASSERT_THROWS(sorted->insert(t2.get(), key5, loc3, false), WriteConflictException);
        }
    }

    void testSeekExactRemoveNext(bool forward, bool unique) {
        auto harnessHelper = newHarnessHelper();
        auto opCtx = harnessHelper->newOperationContext();
        auto sorted = harnessHelper->newSortedDataInterface(unique,
                {{key1, loc1}, {key2, loc1}, {key3, loc1}});
        auto cursor = sorted->newCursor(opCtx.get(), forward);
        ASSERT_EQ(cursor->seekExact(key2), IndexKeyEntry(key2, loc1));
        cursor->save();
        removeFromIndex(opCtx, sorted, {{key2, loc1}});
        cursor->restore();
        ASSERT_EQ(cursor->next(), forward ? IndexKeyEntry(key3, loc1) : IndexKeyEntry(key1, loc1));
        ASSERT_EQ(cursor->next(), boost::none);
    }

    TEST(RocksIndexTest, SeekExactRemoveNext_Forward_Unique) {
        testSeekExactRemoveNext(true, true);
    }

    TEST(RocksIndexTest, SeekExactRemoveNext_Forward_Standard) {
        testSeekExactRemoveNext(true, false);
    }

    TEST(RocksIndexTest, SeekExactRemoveNext_Reverse_Unique) {
        testSeekExactRemoveNext(false, true);
    }

    TEST(RocksIndexTest, SeekExactRemoveNext_Reverse_Standard) {
        testSeekExactRemoveNext(false, false);
    }
}
