/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/db/modules/rocks/src/rocks_prepare_conflict.h"

#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"

namespace mongo {

    // When set, simulates WT_PREPARE_CONFLICT returned from WiredTiger API calls.
    MONGO_FAIL_POINT_DEFINE(RocksPrepareConflictForReads);

    MONGO_FAIL_POINT_DEFINE(RocksSkipPrepareConflictRetries);

    MONGO_FAIL_POINT_DEFINE(RocksPrintPrepareConflictLog);

    void rocksPrepareConflictLog(int attempts) {
        LOG(1) << "Caught conflict, attempt " << attempts
               << ". Waiting for unit of work to commit or abort.";
    }

    void rocksPrepareConflictFailPointLog() {
        log() << "RocksPrintPrepareConflictLog fail point enabled.";
    }

}  // namespace mongo
