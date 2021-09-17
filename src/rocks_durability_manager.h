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

#pragma once

#include "mongo/db/operation_context.h"
#include "mongo/platform/basic.h"
#include "mongo/stdx/condition_variable.h"

namespace rocksdb {
    class DB;
}

namespace mongo {

    class JournalListener;

    class RocksDurabilityManager {
        RocksDurabilityManager(const RocksDurabilityManager&) = delete;
        RocksDurabilityManager& operator=(const RocksDurabilityManager&) = delete;

    public:
        RocksDurabilityManager(rocksdb::DB* db, bool durable,
                               rocksdb::ColumnFamilyHandle* defaultCf,
                               rocksdb::ColumnFamilyHandle* oplogCf);

        void setJournalListener(JournalListener* jl);

        void waitUntilDurable(bool forceFlush);

        /**
         * Waits until a prepared unit of work has ended (either been commited or aborted). This
         * should be used when encountering ROCKS_PREPARE_CONFLICT errors. The caller is required to
         * retry the conflicting WiredTiger API operation. A return from this function does not
         * guarantee that the conflicting transaction has ended, only that one prepared unit of work
         * in the process has signaled that it has ended. Accepts an OperationContext that will
         * throw an AssertionException when interrupted.
         *
         * This method is provided in RocksDurabilityManager and not RecoveryUnit because all
         * recovery units share the same RocksDurabilityManager, and we want a recovery unit on one
         * thread to signal all recovery units waiting for prepare conflicts across all other
         * threads.
         */
        void waitUntilPreparedUnitOfWorkCommitsOrAborts(OperationContext* opCtx,
                                                        uint64_t lastCount);

        /**
         * Notifies waiters that the caller's perpared unit of work has ended
         * (either committed or aborted).
         */
        void notifyPreparedUnitOfWorkHasCommittedOrAborted();

        std::uint64_t getPrepareCommitOrAbortCount() const {
            return _prepareCommitOrAbortCounter.loadRelaxed();
        }

    private:
        rocksdb::DB* _db;  // not owned

        bool _durable;
        rocksdb::ColumnFamilyHandle* _defaultCf;  // not owned
        rocksdb::ColumnFamilyHandle* _oplogCf;    // not owned
        // Notified when we commit to the journal.
        JournalListener* _journalListener;

        // Protects _journalListener.
        Mutex _journalListenerMutex =
            MONGO_MAKE_LATCH("RocksDurabilityManager::_journalListenerMutex");
        AtomicWord<unsigned> _lastSyncTime;
        Mutex _lastSyncMutex = MONGO_MAKE_LATCH("RocksDurabilityManager::_lastSyncMutex");

        // Mutex and cond var for waiting on prepare commit or abort.
        Mutex _prepareCommittedOrAbortedMutex =
            MONGO_MAKE_LATCH("RocksDurabilityManager::_prepareCommittedOrAbortedMutex");

        stdx::condition_variable _prepareCommittedOrAbortedCond;

        AtomicWord<std::uint64_t> _prepareCommitOrAbortCounter{0};
    };
}  // namespace mongo
