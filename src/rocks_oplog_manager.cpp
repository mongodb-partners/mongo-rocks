/**
 *    Copyright (C) 2017 MongoDB Inc.
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

#include <cstring>

#include "mongo/stdx/mutex.h"
#include "mongo/util/concurrency/idle_thread_block.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"
#include "rocks_engine.h"
#include "rocks_oplog_manager.h"

namespace mongo {
namespace {
// This is the minimum valid timestamp; it can be used for reads that need to see all untimestamped
// data but no timestamped data.  We cannot use 0 here because 0 means see all timestamped data.
const uint64_t kMinimumTimestamp = 1;
}  // namespace

MONGO_FAIL_POINT_DEFINE(RocksPausePrimaryOplogDurabilityLoop);

RocksOplogManager::RocksOplogManager(rocksdb::TOTransactionDB* db,
                                     RocksEngine* kvEngine,
                                     RocksDurabilityManager* durabilityManager)
    : _db(db),
      _kvEngine(kvEngine),
      _durabilityManager(durabilityManager) {
}

void RocksOplogManager::start(OperationContext* opCtx, RocksRecordStore* oplogRecordStore,
                              bool updateOldestTimestamp) {
    invariant(!_isRunning);
    auto reverseOplogCursor =
        oplogRecordStore->getCursor(opCtx, false /* false = reverse cursor */);
    auto lastRecord = reverseOplogCursor->next();
    if (lastRecord) {
        _oplogMaxAtStartup = lastRecord->id;

        // Although the oplog may have holes, using the top of the oplog should be safe. In the
        // event of a secondary crashing, replication recovery will truncate the oplog, resetting
        // visibility to the truncate point. In the event of a primary crashing, it will perform
        // rollback before servicing oplog reads.
        auto oplogVisibility = Timestamp(_oplogMaxAtStartup.repr());
        setOplogReadTimestamp(oplogVisibility);
        LOG(1) << "Setting oplog visibility at startup. Val: " << oplogVisibility;
    } else {
        _oplogMaxAtStartup = RecordId();
        // Avoid setting oplog visibility to 0. That means "everything is visible".
        setOplogReadTimestamp(Timestamp(kMinimumTimestamp));
    }

    // Need to obtain the mutex before starting the thread, as otherwise it may race ahead
    // see _shuttingDown as true and quit prematurely.
    stdx::lock_guard<stdx::mutex> lk(_oplogVisibilityStateMutex);
    _oplogJournalThread = stdx::thread(&RocksOplogManager::_oplogJournalThreadLoop,
                                       this,
                                       oplogRecordStore,
                                       updateOldestTimestamp);
    _isRunning = true;
    _shuttingDown = false;
}

void RocksOplogManager::halt() {
    {
        stdx::lock_guard<stdx::mutex> lk(_oplogVisibilityStateMutex);
        invariant(_isRunning);
        _shuttingDown = true;
        _isRunning = false;
    }

    if (_oplogJournalThread.joinable()) {
        _opsWaitingForJournalCV.notify_one();
        _oplogJournalThread.join();
    }
}

void RocksOplogManager::waitForAllEarlierOplogWritesToBeVisible(
    const RocksRecordStore* oplogRecordStore, OperationContext* opCtx) {
    invariant(opCtx->lockState()->isNoop() || !opCtx->lockState()->inAWriteUnitOfWork());

    // In order to reliably detect rollback situations, we need to fetch the latestVisibleTimestamp
    // prior to querying the end of the oplog.
    auto currentLatestVisibleTimestamp = getOplogReadTimestamp();

    // Procedure: issue a read on a reverse cursor (which is not subject to the oplog visibility
    // rules), see what is last, and wait for that to become visible.
    std::unique_ptr<SeekableRecordCursor> cursor =
        oplogRecordStore->getCursor(opCtx, false /* false = reverse cursor */);
    auto lastRecord = cursor->next();
    if (!lastRecord) {
        LOG(2) << "Trying to query an empty oplog";
        opCtx->recoveryUnit()->abandonSnapshot();
        return;
    }
    const auto waitingFor = lastRecord->id;
    // Close transaction before we wait.
    opCtx->recoveryUnit()->abandonSnapshot();

    stdx::unique_lock<stdx::mutex> lk(_oplogVisibilityStateMutex);

    // Prevent any scheduled journal flushes from being delayed and blocking this wait excessively.
    _opsWaitingForVisibility++;
    invariant(_opsWaitingForVisibility > 0);
    auto exitGuard = MakeGuard([&] { _opsWaitingForVisibility--; });

    opCtx->waitForConditionOrInterrupt(_opsBecameVisibleCV, lk, [&] {
        auto newLatestVisibleTimestamp = getOplogReadTimestamp();
        if (newLatestVisibleTimestamp < currentLatestVisibleTimestamp) {
            LOG(1) << "oplog latest visible timestamp went backwards";
            // If the visibility went backwards, this means a rollback occurred.
            // Thus, we are finished waiting.
            return true;
        }
        currentLatestVisibleTimestamp = newLatestVisibleTimestamp;

        // currentLatestVisibleTimestamp might be Timestamp "1" if there are no oplog documents
        // inserted since the last mongod restart.  In this case, we need to simulate what timestamp
        // the last oplog document had when it was written, which is the _oplogMaxAtStartup value.
        RecordId latestVisible =
            std::max(RecordId(currentLatestVisibleTimestamp), _oplogMaxAtStartup);
        if (latestVisible < waitingFor) {
            LOG(2) << "Operation is waiting for " << waitingFor << "; latestVisible is "
                   << currentLatestVisibleTimestamp << " oplogMaxAtStartup is "
                   << _oplogMaxAtStartup;
        }
        return latestVisible >= waitingFor;
    });
}

void RocksOplogManager::triggerJournalFlush() {
    stdx::lock_guard<stdx::mutex> lk(_oplogVisibilityStateMutex);
    if (!_opsWaitingForJournal) {
        _opsWaitingForJournal = true;
        _opsWaitingForJournalCV.notify_one();
    }
}

void RocksOplogManager::_oplogJournalThreadLoop(RocksRecordStore* oplogRecordStore,
                                                const bool updateOldestTimestamp) noexcept {
    Client::initThread("RocksOplogJournalThread");

    // This thread updates the oplog read timestamp, the timestamp used to read from the oplog with
    // forward cursors.  The timestamp is used to hide oplog entries that might be committed but
    // have uncommitted entries ahead of them.
    while (true) {
        stdx::unique_lock<stdx::mutex> lk(_oplogVisibilityStateMutex);
        {
            MONGO_IDLE_THREAD_BLOCK;
            _opsWaitingForJournalCV.wait(lk,
                                         [&] { return _shuttingDown || _opsWaitingForJournal; });

            // If we're not shutting down and nobody is actively waiting for the oplog to become
            // durable, delay journaling a bit to reduce the sync rate.
            auto journalDelay = Milliseconds(storageGlobalParams.journalCommitIntervalMs.load());
            if (journalDelay == Milliseconds(0)) {
                journalDelay = Milliseconds(WiredTigerKVEngine::kDefaultJournalDelayMillis);
            }
            auto now = Date_t::now();
            auto deadline = now + journalDelay;
            auto shouldSyncOpsWaitingForJournal = [&] {
                return _shuttingDown || _opsWaitingForVisibility ||
                    oplogRecordStore->haveCappedWaiters();
            };

            // Eventually it would be more optimal to merge this with the normal journal flushing
            // and block for either oplog tailers or operations waiting for oplog visibility. For
            // now this loop will poll once a millisecond up to the journalDelay to see if we have
            // any waiters yet. This reduces sync-related I/O on the primary when secondaries are
            // lagged, but will avoid significant delays in confirming majority writes on replica
            // sets with infrequent writes.
            // Callers of waitForAllEarlierOplogWritesToBeVisible() like causally consistent reads
            // will preempt this delay.
            while (now < deadline &&
                   !_opsWaitingForJournalCV.wait_until(
                       lk, now.toSystemTimePoint(), shouldSyncOpsWaitingForJournal)) {
                now += Milliseconds(1);
            }
        }

        while (!_shuttingDown && MONGO_FAIL_POINT(RocksPausePrimaryOplogDurabilityLoop)) {
            lk.unlock();
            sleepmillis(10);
            lk.lock();
        }

        if (_shuttingDown) {
            log() << "oplog journal thread loop shutting down";
            return;
        }
        invariant(_opsWaitingForJournal);
        _opsWaitingForJournal = false;
        lk.unlock();

        const uint64_t newTimestamp = fetchAllCommittedValue().asULL();

        // The newTimestamp may actually go backward during secondary batch application,
        // where we commit data file changes separately from oplog changes, so ignore
        // a non-incrementing timestamp.
        if (newTimestamp <= _oplogReadTimestamp.load()) {
            LOG(2) << "no new oplog entries were made visible: " << newTimestamp;
            continue;
        }

        // In order to avoid oplog holes after an unclean shutdown, we must ensure this proposed
        // oplog read timestamp's documents are durable before publishing that timestamp.
        _durabilityManager->waitUntilDurable(false);

        lk.lock();
        // Publish the new timestamp value.  Avoid going backward.
        auto oldTimestamp = getOplogReadTimestamp();
        if (newTimestamp > oldTimestamp) {
            _setOplogReadTimestamp(lk, newTimestamp);
        }
        lk.unlock();

        if (updateOldestTimestamp) {
            const bool force = false;
            _kvEngine->setOldestTimestamp(Timestamp(newTimestamp), force);
        }

        // Wake up any await_data cursors and tell them more data might be visible now.
        oplogRecordStore->notifyCappedWaitersIfNeeded();
    }
}

std::uint64_t RocksOplogManager::getOplogReadTimestamp() const {
    return _oplogReadTimestamp.load();
}

void RocksOplogManager::setOplogReadTimestamp(Timestamp ts) {
    stdx::lock_guard<stdx::mutex> lk(_oplogVisibilityStateMutex);
    _setOplogReadTimestamp(lk, ts.asULL());
}

void RocksOplogManager::_setOplogReadTimestamp(WithLock, uint64_t newTimestamp) {
    _oplogReadTimestamp.store(newTimestamp);
    _opsBecameVisibleCV.notify_all();
    LOG(2) << "setting new oplogReadTimestamp: " << newTimestamp;
}

uint64_t RocksOplogManager::fetchAllCommittedValue() {
    rocksdb::RocksTimeStamp ts(0);
    auto status = _db->QueryTimeStamp(rocksdb::TimeStampType::kAllCommitted, &ts);
    if (status.IsNotFound()) {
        return kMinimumTimestamp;
    } else {
        invariant(status.ok(), status.ToString());
    }
    return Timestamp(ts);
}

}  // namespace mongo
