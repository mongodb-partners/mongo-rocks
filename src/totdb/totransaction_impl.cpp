//  Copyright (c) 2019-present.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage
#include "mongo/db/modules/rocks/src/totdb/totransaction_impl.h"
#include "mongo/db/modules/rocks/src/totdb/totransaction_db.h"
#include "mongo/db/modules/rocks/src/totdb/totransaction_db_impl.h"
#include "mongo/util/log.h"
#include "mongo/util/stacktrace.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "third_party/s2/util/coding/coder.h"

namespace rocksdb {

namespace {

TOComparator wbwidx_default_comparator;
const RocksTimeStamp nots_ts = 1;

}  // namespace

std::set<std::string> TOTransaction::timestampPrefixes;
std::shared_mutex TOTransaction::prefixes_mutex;

void TOTransaction::enableTimestamp(const std::string& prefix) {
  std::unique_lock<std::shared_mutex> wl(prefixes_mutex);
  timestampPrefixes.insert(prefix);
}

bool TOTransaction::isEnableTimestamp(const Slice& key) {
  std::shared_lock<std::shared_mutex> rl(prefixes_mutex);
  Slice prefix(key.data(), sizeof(uint32_t));
  return timestampPrefixes.count(prefix.ToString()) > 0;
}

Status PrepareConflict() {
  return Status(Status::Code::kInvalidArgument, Status::SubCode::kNone,
                Status::Severity::kSoftError, "prepare conflict");
}

bool IsPrepareConflict(const Status& s) {
  return s.code() == Status::Code::kInvalidArgument && s.subcode() == Status::SubCode::kNone &&
         s.severity() == Status::Severity::kSoftError &&
         std::string(s.getState()) == "prepare conflict";
}

struct WriteOptions;

std::atomic<TransactionID> TOTransactionImpl::txn_id_counter_(1);

TransactionID TOTransactionImpl::GenTxnID() {
  return txn_id_counter_.fetch_add(1);
}

TOTransactionImpl::TOTransactionImpl(TOTransactionDB* txn_db,
              const WriteOptions& write_options,
              const TOTxnOptions& txn_option,
              const std::shared_ptr<ActiveTxnNode>& core)
    : txn_id_(0),
      db_(txn_db->GetRootDB()), 
      write_options_(write_options),
      txn_option_(txn_option),
      core_(core) {
      txn_db_impl_ = dynamic_cast<TOTransactionDBImpl*>(txn_db);
      invariant(txn_db_impl_);
}

TOTransactionImpl::~TOTransactionImpl() {
  // Do rollback if this transaction is not committed or rolled back
  if (core_->state_ < kCommitted) {
    Rollback();
  }
}

Status TOTransactionImpl::SetReadTimeStamp(const RocksTimeStamp& timestamp) {
  if (core_->state_ >= kCommitted) {
    return Status::NotSupported("this txn is committed or rollback");
  } 
  
  if (core_->read_ts_set_) {
    return Status::NotSupported("set read ts is supposed to be set only once");
  }

  LOG(1) << "TOTDB txn id: " << txn_id_
         << "set read ts: " << timestamp
         << "force: " << core_->timestamp_round_read_;

  Status s = txn_db_impl_->AddReadQueue(core_, timestamp);
  if (!s.ok()) {
    return s;
  }
  invariant(core_->read_ts_set_);
  invariant(core_->read_ts_ >= timestamp);
  Encoder(core_->read_ts_buffer_, sizeof(core_->read_ts_)).put64(core_->read_ts_);
  // If we already have a snapshot, it may be too early to match
  // the timestamp (including the one we just read, if rounding
  // to oldest).  Get a new one.
  invariant(core_->txn_snapshot != nullptr);
  txn_db_impl_->ReleaseSnapshot(core_->txn_snapshot);
  core_->txn_snapshot = txn_db_impl_->GetSnapshot();
  return s;
}

Status TOTransactionImpl::SetPrepareTimeStamp(const RocksTimeStamp& timestamp) {
  if (core_->state_ != kStarted) {
    return Status::NotSupported(
        "this txn is prepared or committed or rollback");
  }

  if (core_->prepare_ts_set_) {
    return Status::NotSupported("prepare ts is already set");
  }

  if (core_->commit_ts_set_) {
    return Status::NotSupported(
        "should not have been set before the prepare timestamp");
  }
  return txn_db_impl_->SetPrepareTimeStamp(core_, timestamp);
}

Status TOTransactionImpl::Prepare() {
  if (core_->state_ != kStarted) {
    return Status::NotSupported(
        "this txn is prepared or committed or rollback");
  }

  if (!core_->prepare_ts_set_) {
    return Status::NotSupported("prepare ts not set when prepare");
  }

  if (core_->commit_ts_set_) {
    return Status::NotSupported(
        "commit ts should not have been set when prepare");
  }

  return txn_db_impl_->PrepareTransaction(core_);
}

Status TOTransactionImpl::SetCommitTimeStamp(const RocksTimeStamp& timestamp) {
  if (core_->state_ >= kCommitted) {
    return Status::NotSupported("this txn is committed or rollback");
  }
  auto s = txn_db_impl_->SetCommitTimeStamp(core_, timestamp);
  if (!s.ok()) {
    return s;
  }
  invariant(core_->commit_ts_set_ &&
         (core_->first_commit_ts_ <= core_->commit_ts_));

  LOG(2) << "TOTDB txn id " << core_->txn_id_ << "set commit ts " << timestamp;
  return Status::OK();
}

Status TOTransactionImpl::SetDurableTimeStamp(const RocksTimeStamp& timestamp) {
  if (core_->state_ >= kCommitted) {
    return Status::NotSupported("this txn is committed or rollback");
  }

  auto s = txn_db_impl_->SetDurableTimeStamp(core_, timestamp);
  if (!s.ok()) {
    return s;
  }
  invariant(core_->durable_ts_set_);

  LOG(2) << "TOTDB txn id " << core_->txn_id_ << "set durable ts " << timestamp;
  return Status::OK();
}

Status TOTransactionImpl::GetReadTimeStamp(RocksTimeStamp* timestamp) const {
  if ((!timestamp) || (!core_->read_ts_set_)) {
      return Status::InvalidArgument("need set read ts, and parameter should not be null");
  }

  *timestamp = core_->read_ts_;

  return Status::OK();
}

WriteBatchWithIndex* TOTransactionImpl::GetWriteBatch() {
  return &(core_->write_batch_);
}

TOTransactionImpl::ActiveTxnNode::ActiveTxnNode()
  : txn_id_(0),
    commit_txn_id_(0),
    commit_ts_set_(false),
    commit_ts_(0),
    first_commit_ts_(0),
    read_ts_set_(false),
    read_ts_(std::numeric_limits<RocksTimeStamp>::max()),
    read_ts_slice_(read_ts_buffer_, sizeof(read_ts_buffer_)),
    prepare_ts_set_(false),
    prepare_ts_(0),
    durable_ts_set_(false),
    durable_ts_(0),
    timestamp_published_(false),
    timestamp_round_prepared_(false),
    timestamp_round_read_(false),
    state_(TOTransaction::kStarted),
    txn_snapshot(nullptr),
    write_batch_(&wbwidx_default_comparator, 0, true /*overwrite_keys*/, 0) {
  Encoder(read_ts_buffer_, sizeof(read_ts_)).put64(read_ts_);
}

const TOTransactionImpl::ActiveTxnNode* TOTransactionImpl::GetCore() const {
  return core_.get();
}

Status TOTransactionImpl::Put(ColumnFamilyHandle* column_family, const Slice& key,
           const Slice& value) {
  if (txn_db_impl_->IsReadOnly()) {
    return Status::NotSupported("readonly db cannot accept put");
  }
  if (core_->state_ >= kPrepared) {
    return Status::NotSupported("txn is already prepared, committed rollback");
  }
  if (core_->read_only_) {
    if (core_->ignore_prepare_) {
      return Status::NotSupported(
          "Transactions with ignore_prepare=true cannot perform updates");
    }
    return Status::NotSupported("Attempt to update in a read-only transaction");
  }

  const TxnKey txn_key(column_family->GetID(), key.ToString());
  Status s = CheckWriteConflict(txn_key);

  if (s.ok()) {
    written_keys_.emplace(std::move(txn_key));
    GetWriteBatch()->Put(column_family, key, value);
    auto commit_ts = TOTransaction::isEnableTimestamp(key) ? core_->commit_ts_ : nots_ts;
    asof_commit_timestamps_.emplace_back(commit_ts);
  }
  return s;
}

Status TOTransactionImpl::Put(const Slice& key, const Slice& value) {
  return Put(db_->DefaultColumnFamily(), key, value);
}

Status TOTransactionImpl::Get(ReadOptions& options,
           ColumnFamilyHandle* column_family, const Slice& key,
           std::string* value) {
  if (core_->state_ >= kPrepared) {
    return Status::NotSupported("txn is already prepared, committed rollback");
  } 
  // Check the options, if read ts is set use read ts
  options.timestamp = &core_->read_ts_slice_;
  invariant(core_->txn_snapshot);
  options.snapshot = core_->txn_snapshot;

  invariant(options.timestamp->size() == sizeof(RocksTimeStamp));
  const TxnKey txn_key(column_family->GetID(), key.ToString());
  if (written_keys_.find(txn_key) != written_keys_.end()) {
    return GetWriteBatch()->GetFromBatchAndDB(db_, options, column_family, key,
                                              value);
  }

  return txn_db_impl_->GetConsiderPrepare(core_, options, column_family, key,
                                          value);
}

Status TOTransactionImpl::Get(ReadOptions& options, const Slice& key,
           std::string* value) {
  return Get(options, db_->DefaultColumnFamily(), key, value);
}

Status TOTransactionImpl::Delete(ColumnFamilyHandle* column_family, const Slice& key) {
  if (txn_db_impl_->IsReadOnly()) {
    return Status::NotSupported("readonly db cannot accept del");
  }
  if (core_->state_ >= kPrepared) {
    return Status::NotSupported("txn is already prepared, committed rollback");
  }
  if (core_->read_only_) {
    if (core_->ignore_prepare_) {
      return Status::NotSupported(
          "Transactions with ignore_prepare=true cannot perform updates");
    }
    return Status::NotSupported("Attempt to update in a read-only transaction");
  }

  const TxnKey txn_key(column_family->GetID(), key.ToString());
  Status s = CheckWriteConflict(txn_key);

  if (s.ok()) {
    written_keys_.emplace(std::move(txn_key));
    GetWriteBatch()->Delete(column_family, key);
    auto commit_ts = TOTransaction::isEnableTimestamp(key) ? core_->commit_ts_ : nots_ts;
    asof_commit_timestamps_.emplace_back(commit_ts);
  }
  return s;
}

Status TOTransactionImpl::Delete(const Slice& key) {
  return Delete(db_->DefaultColumnFamily(), key);
}

Iterator* TOTransactionImpl::GetIterator(ReadOptions& read_options) {
  return GetIterator(read_options, db_->DefaultColumnFamily());
}

Iterator* TOTransactionImpl::GetIterator(ReadOptions& read_options,
                      ColumnFamilyHandle* column_family) {
  if (core_->state_ >= kPrepared) {
    return nullptr;
  }

  read_options.timestamp = &core_->read_ts_slice_;

  invariant(core_->txn_snapshot);
  invariant(read_options.timestamp->size() == sizeof(RocksTimeStamp));
  read_options.snapshot = core_->txn_snapshot;
  Iterator* db_iter = db_->NewIterator(read_options, column_family);
  if (db_iter == nullptr) {
    return nullptr;
  }

  return txn_db_impl_->NewIteratorConsiderPrepare(core_, column_family,
                                                  db_iter);
}

Status TOTransactionImpl::CheckWriteConflict(const TxnKey& key) {
  return txn_db_impl_->CheckWriteConflict(key, GetID(), core_->read_ts_);
}

Status TOTransactionImpl::Commit(std::function<void()>* hook) {
  if (core_->state_ >= kCommitted) {
    return Status::InvalidArgument("txn already committed or rollback.");
  }
  
  invariant(asof_commit_timestamps_.size()
    == static_cast<size_t>(GetWriteBatch()->GetWriteBatch()->Count()));
  if (core_->commit_ts_set_) {
    for (size_t i = 0; i < asof_commit_timestamps_.size(); ++i) {
      if (asof_commit_timestamps_[i] == 0) {
        asof_commit_timestamps_[i] = core_->commit_ts_;
      }
    }
  }

  Status s;
  if (GetWriteBatch()->GetWriteBatch()->Count() != 0) {
    invariant(!txn_db_impl_->IsReadOnly());
    char ts_buf[sizeof(RocksTimeStamp)];
    size_t cnt = 0;
    Slice ts_slice(ts_buf, sizeof(RocksTimeStamp));
    const auto ts_sz_func = [&](uint32_t) {
      Encoder(ts_buf, sizeof(RocksTimeStamp)).put64(asof_commit_timestamps_[cnt++]);
      return sizeof(RocksTimeStamp);
    };
    GetWriteBatch()->GetWriteBatch()->UpdateTimestamps(ts_slice, ts_sz_func);
    invariant(cnt == asof_commit_timestamps_.size());
    // NOTE(xxxxxxxx): It's a simple modification for readonly transaction.
    // PutLogData will not increase Count. So, If in the future
    // PutLogData is added into TOTransactionDB, this shortcut should be redesigned.
    s = db_->Write(write_options_, GetWriteBatch()->GetWriteBatch());
  }
  if (hook) {
    (*hook)();
  }
  if (s.ok()) {
    // Change active txn set,
    // Move uncommitted keys to committed keys,
    // Clean data when the committed txn is activeTxnSet's header
    // TODO(xxxxxxxx): in fact, here we must not fail
    s = txn_db_impl_->CommitTransaction(core_, written_keys_);
  } else {
    s = Status::InvalidArgument("Transaction is fail for commit.");
  }

#ifdef NDEBUG
  // prepareHeap needs writeBatch for sanity check in debug mode.
  GetWriteBatch()->Clear();
#endif  // NDEBUG
  LOG(2) << "TOTDB txn id " << txn_id_ << " committed";
  return s;
}

Status TOTransactionImpl::Rollback() {
  if (core_->state_ >= kCommitted) {
    return Status::InvalidArgument("txn is already committed or rollback.");
  }

  // Change active txn set,
  // Clean uncommitted keys
  Status s = txn_db_impl_->RollbackTransaction(core_, written_keys_);

  GetWriteBatch()->Clear();

  LOG(2) << "TOTDB txn id " << txn_id_ << " rollbacked";
  return s;
}

Status TOTransactionImpl::SetName(const TransactionName& name) {
  name_ = name;
  return Status::OK();
}

TransactionID TOTransactionImpl::GetID() const {
  invariant(core_);
  return core_->txn_id_;
}

TOTransaction::TOTransactionState TOTransactionImpl::GetState() const {
  invariant(core_);
  return core_->state_;
}
}

#endif
