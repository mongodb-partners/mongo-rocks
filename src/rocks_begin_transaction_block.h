/**
 *    Copyright (C) 2018 MongoDB Inc.
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

#include <rocksdb/utilities/totransaction.h>
#include <rocksdb/utilities/totransaction_db.h>
#include "mongo/base/status.h"
#include "mongo/bson/timestamp.h"

namespace mongo {

    /**
     * When constructed, this object begins a Rocks transaction on the provided session. The
     * transaction will be rolled back if done() is not called before the object is destructed.
     */
    class RocksBeginTxnBlock {
    public:
        // Whether or not to ignore prepared transactions.
        enum class IgnorePrepared {
            kNoIgnore,  // Do not ignore prepared transactions and return prepare conflicts.
            kIgnore     // Ignore prepared transactions and show prepared, but uncommitted data.
        };

        // Whether or not to round up to the oldest timestamp when the read timestamp is behind it.
        enum class RoundToOldest {
            kNoRound,  // Do not round to the oldest timestamp. BadValue error may be returned.
            kRound     // Round the read timestamp up to the oldest timestamp when it is behind.
        };

        RocksBeginTxnBlock(rocksdb::TOTransactionDB* db,
                           std::unique_ptr<rocksdb::TOTransaction>* txn);
        ~RocksBeginTxnBlock();

        /**
         * Sets the read timestamp on the opened transaction. Cannot be called after a call to
         * done().
         */
        Status setTimestamp(Timestamp, RoundToOldest roundToOldest = RoundToOldest::kNoRound);

        /* Get the read timestamp on the opened transaction */
        Timestamp getTimestamp() const;

        /**
         * End the begin transaction block. Must be called to ensure the opened transaction
         * is not be rolled back.
         */
        void done();

    private:
        rocksdb::TOTransactionDB* _db;
        rocksdb::TOTransaction* _transaction;
        bool _rollback = false;
        Timestamp _readTimestamp;
    };

}  // namespace mongo
