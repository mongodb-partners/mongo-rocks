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

#include <mutex>
#include <set>

#include "mongo/base/checked_cast.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/util/background.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"

#include "rocks_engine.h"
#include "rocks_record_store.h"
#include "rocks_recovery_unit.h"

namespace mongo {

    namespace {

        std::set<NamespaceString> _backgroundThreadNamespaces;
        stdx::mutex _backgroundThreadMutex;

        class RocksRecordStoreThread : public BackgroundJob {
        public:
            RocksRecordStoreThread(const NamespaceString& ns)
                : BackgroundJob(true /* deleteSelf */), _ns(ns) {
                _name = std::string("RocksRecordStoreThread for ") + _ns.toString();
            }

            virtual std::string name() const { return _name; }

            /**
             * @return if any oplog records are deleted.
             */
            bool _deleteExcessDocuments() {
                if (!getGlobalServiceContext()->getStorageEngine()) {
                    LOG(1) << "no global storage engine yet";
                    return false;
                }

                const auto opCtx = cc().makeOperationContext();

                try {
                    // A Global IX lock should be good enough to protect the oplog truncation from
                    // interruptions such as restartCatalog. PBWM, database lock or collection lock is not
                    // needed. This improves concurrency if oplog truncation takes long time.
                    ShouldNotConflictWithSecondaryBatchApplicationBlock shouldNotConflictBlock(
                        opCtx.get()->lockState());
                    Lock::GlobalLock lk(opCtx.get(), MODE_IX);
        
                    RocksRecordStore* rs = nullptr;
                    {
                        // Release the database lock right away because we don't want to
                        // block other operations on the local database and given the
                        // fact that oplog collection is so special, Global IX lock can
                        // make sure the collection exists.
                        Lock::DBLock dbLock(opCtx.get(), _ns.db(), MODE_IX);
                        Database* db = DatabaseHolder::getDatabaseHolder().get(opCtx.get(), _ns.db());
                        if (!db) {
                            LOG(2) << "no local database yet";
                            return false;
                        }
                        // We need to hold the database lock while getting the collection. Otherwise a
                        // concurrent collection creation would write to the map in the Database object
                        // while we concurrently read the map.
                        Collection* collection = db->getCollection(opCtx.get(), _ns);
                        if (!collection) {
                            LOG(2) << "no collection " << _ns;
                            return false;
                        }
                        rs = checked_cast<RocksRecordStore*>(collection->getRecordStore());
                    }
        
                    return rs->reclaimOplog(opCtx.get());
                } catch (const std::exception& e) {
                    severe() << "error in RocksRecordStoreThread: " << redact(e.what());
                    fassertFailedNoTrace(!"error in RocksRecordStoreThread");
                } catch (...) {
                    fassertFailedNoTrace(!"unknown error in RocksRecordStoreThread");
                }
                MONGO_UNREACHABLE
            }

            virtual void run() {
                Client::initThread(_name.c_str());

                while (!globalInShutdownDeprecated()) {
                    bool removed = _deleteExcessDocuments();
                    LOG(2) << "RocksRecordStoreThread deleted " << removed;
                    if (!removed) {
                        // If we removed 0 documents, sleep a bit in case we're on a laptop
                        // or something to be nice.
                        sleepmillis(1000);
                    } else {
                        // wake up every 100ms
                        sleepmillis(100);
                    }
                }

                log() << "shutting down";
            }

        private:
            NamespaceString _ns;
            std::string _name;
        };

    }  // namespace

    // static
    bool RocksEngine::initRsOplogBackgroundThread(StringData ns) {
        if (!NamespaceString::oplog(ns)) {
            return false;
        }

        if (storageGlobalParams.repair || storageGlobalParams.readOnly) {
            LOG(1) << "not starting RocksRecordStoreThread for " << ns
                   << " because we are either in repair or read-only mode";
            return false;
        }

        stdx::lock_guard<stdx::mutex> lock(_backgroundThreadMutex);
        NamespaceString nss(ns);
        if (_backgroundThreadNamespaces.count(nss)) {
            log() << "RocksRecordStoreThread " << ns << " already started";
        } else {
            log() << "Starting RocksRecordStoreThread " << ns;
            BackgroundJob* backgroundThread = new RocksRecordStoreThread(nss);
            backgroundThread->go();
            _backgroundThreadNamespaces.insert(nss);
        }
        return true;
    }

}  // namespace mongo
