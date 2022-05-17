//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "third_party/s2/util/coding/coder.h"
#include "mongo/db/modules/rocks/src/totdb/totransaction_db_impl.h"
#include "mongo/db/modules/rocks/src/totdb/totransaction_prepare_iterator.h"
#include "mongo/util/log.h"
#include "mongo/bson/timestamp.h"

namespace rocksdb {

namespace {

using ATN = TOTransactionImpl::ActiveTxnNode;
size_t TxnKeySize(const TxnKey& key) {
  return sizeof(key.first) + key.second.size();
}

using BatchIteratorCB = std::function<void(uint32_t cf_id, const Slice& s)>;
class BatchIterator : public WriteBatch::Handler {
 public:
  BatchIterator(BatchIteratorCB cb) : cb_(std::move(cb)) {}

  Status PutCF(uint32_t column_family_id, const Slice& key,
               const Slice& value) override {
    (void)value;
    cb_(column_family_id, key);
    return Status::OK();
  }

  Status DeleteCF(uint32_t column_family_id, const Slice& key) override {
    cb_(column_family_id, key);
    return Status::OK();
  }

 private:
  BatchIteratorCB cb_;
};

}  // namespace

const TxnKey PrepareHeap::sentinal_ =
    std::make_pair(std::numeric_limits<uint32_t>::max(), "");

PrepareHeap::PrepareHeap() {
  // insert sentinal into map_ to simplify the reverse find logic
  map_.insert({sentinal_, {}});
}

std::shared_ptr<ATN> PrepareHeap::Find(
    const TOTransactionImpl::ActiveTxnNode* core, const TxnKey& key,
    TOTransaction::TOTransactionState* state) {
  std::shared_lock<std::shared_mutex> rl(mutex_);
  const auto it = map_.find(key);
  if (it == map_.end()) {
    return nullptr;
  }
  for (const auto& v : it->second) {
    if (v->txn_id_ == core->txn_id_) {
      *state = v->state_.load(std::memory_order_relaxed);
      return v;
    }
    if (v->prepare_ts_ <= core->read_ts_) {
      *state = v->state_.load(std::memory_order_relaxed);
      return v;
    }
  }
  return nullptr;
}

void PrepareHeap::Insert(const std::shared_ptr<ATN>& core) {
  int64_t cnt = 0;
  // NOTE: need to add PREPARING state to indicate the incomplete state ?
  auto iter_cb = [&](uint32_t cf_id, const Slice& key) {
    cnt++;
    const auto lookup_key =
        std::make_pair(cf_id, std::string(key.data(), key.size()-sizeof(RocksTimeStamp)));
    auto it = map_.find(lookup_key);
    if (it == map_.end()) {
      map_.insert({lookup_key, {core}});
    } else {
      invariant(it->second.size() > 0);
      if (it->second.front()->txn_id_ == core->txn_id_) {
        // NOTE(deyukong): there may be duplicated kvs in one write_batch
        // because during a txn, same key may be modified several times.
        // Though WriteBatchWithIndex sees the lastest one, WriteBatch.Iterate
        // sees them all.
      } else {
        invariant(it->second.front()->txn_id_ < core->txn_id_);
        auto state = it->second.front()->state_.load(std::memory_order_relaxed);
        (void)state;
        invariant(state == TOTransaction::TOTransactionState::kCommitted ||
               state == TOTransaction::TOTransactionState::kRollback);
        it->second.push_front(core);
      }
    }
  };
  BatchIterator it(iter_cb);
  {
    std::unique_lock<std::shared_mutex> wl(mutex_);
    auto status = core->write_batch_.GetWriteBatch()->Iterate(&it);
    (void)status;
    invariant(status.ok() && cnt == core->write_batch_.GetWriteBatch()->Count());
    core->state_.store(TOTransaction::TOTransactionState::kPrepared);
  }
}

uint32_t PrepareHeap::Remove(ATN* core) {
  uint32_t cnt = 0;
  // NOTE: need to add PREPARING state to indicate the incomplete state ?
  auto iter_cb = [&](uint32_t cf_id, const Slice& key) {
    const auto lookup_key =
        std::make_pair(cf_id, std::string(key.data(), key.size()-sizeof(RocksTimeStamp)));
    auto it = map_.find(lookup_key);
    if (it != map_.end()) {
      invariant(!it->second.empty() &&
             it->second.front()->txn_id_ == core->txn_id_);
      it->second.pop_front();
      cnt++;
      if (it->second.empty()) {
        map_.erase(it);
      }
    }
  };
  BatchIterator it(iter_cb);
  invariant(core->state_.load(std::memory_order_relaxed) ==
         TOTransaction::TOTransactionState::kPrepared);
  {
    std::unique_lock<std::shared_mutex> wl(mutex_);
    auto status = core->write_batch_.GetWriteBatch()->Iterate(&it);
    (void)status;
    invariant(status.ok());
  }
  return cnt;
}

void PrepareHeap::Purge(TransactionID oldest_txn_id, RocksTimeStamp oldest_ts) {
  (void)oldest_ts;
  std::unique_lock<std::shared_mutex> wl(mutex_);
  auto it = map_.begin();
  while (it->first != sentinal_) {
    auto& lst = it->second;
    auto lst_it = lst.begin();
    while (lst_it != lst.end()) {
      auto state = (*lst_it)->state_.load(std::memory_order_relaxed);
      invariant(state != TOTransaction::TOTransactionState::kRollback);
      if (state == TOTransaction::TOTransactionState::kCommitted &&
          (*lst_it)->commit_txn_id_ <=
              oldest_txn_id /* && lst_it->commit_ts_ < oldest_ts*/) {
        // committed-txns with commit_ts_ >= oldest_ts and commit_txn_id <=
        // oldest_txn_id are visible
        // to new transactions, this purge process is a slice different from
        // purging commit_map
        lst.erase(lst_it, lst.end());
        break;
      } else {
        invariant(state == TOTransaction::TOTransactionState::kCommitted ||
               state == TOTransaction::TOTransactionState::kPrepared);
        lst_it++;
      }
    }
    if (lst.empty()) {
      it = map_.erase(it);
    } else {
      it++;
    }
  }
}

Status TOTransactionDB::Open(const Options& options,
                     const TOTransactionDBOptions& txn_db_options,
                     const std::string& dbname, const std::string stable_ts_key, TOTransactionDB** dbptr) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  std::vector<ColumnFamilyDescriptor> trim_cfds;
  const bool trimHistory = false;
  Status s = Open(db_options, txn_db_options, dbname, column_families, &handles, trim_cfds, trimHistory, stable_ts_key, dbptr);
  if (s.ok()) {
    invariant(handles.size() == 1);
    // I can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }

  return s;
}

Status TOTransactionDB::Open(const DBOptions& db_options,
                     const TOTransactionDBOptions& txn_db_options,
                     const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& open_cfds,
                     std::vector<ColumnFamilyHandle*>* handles,
                     const std::vector<ColumnFamilyDescriptor>& trim_cfds,
                     const bool trimHistory,
                     const std::string stable_ts_key,
                     TOTransactionDB** dbptr) {
  Status s;
  for (const auto& cfd : open_cfds) {
    if (cfd.options.comparator == nullptr || cfd.options.comparator->timestamp_size() == 0) {
      return Status::InvalidArgument("invalid comparator");
    }
  }

  if (trimHistory) {
    invariant(trim_cfds.size() > 0);
    
    std::string trim_ts;
    // step1: get stableTs
    // step2: close db
    {
      std::vector<ColumnFamilyHandle*> tmp_cfhs;
      DB* tmp_db = nullptr;
      s = DB::Open(db_options, dbname, open_cfds, &tmp_cfhs, &tmp_db);
      if (!s.ok()) {
        return s;
      }

      char read_ts_buffer_[sizeof(RocksTimeStamp)];
      Encoder(read_ts_buffer_, sizeof(RocksTimeStamp)).put64(mongo::Timestamp::max().asULL());
      auto read_opt = rocksdb::ReadOptions();
      Slice readTs = rocksdb::Slice(read_ts_buffer_, sizeof(read_ts_buffer_));
      read_opt.timestamp = &readTs;

      s = tmp_db->Get(read_opt, stable_ts_key, &trim_ts);
      for (auto handle : tmp_cfhs) {
        auto tmp_s = tmp_db->DestroyColumnFamilyHandle(handle);
        assert(tmp_s.ok());
      }
      tmp_cfhs.clear();
      delete tmp_db;
      if (!s.ok() && !s.IsNotFound()) {
        return s;
      }
    }

    // step3: invoke rocksdb::OpenAndTrimHistory
    // step4: close db
    if(s.ok()) {
      LOG(0) << "##### TOTDB recover to stableTs " << rocksdb::Slice(trim_ts).ToString(true);
      std::vector<ColumnFamilyHandle*> tmp_cfhs;
      DB* tmp_db = nullptr;
      s = DB::OpenAndTrimHistory(db_options, dbname, trim_cfds, &tmp_cfhs, &tmp_db, trim_ts);
      if (!s.ok()) {
        return s;
      }
      for (auto handle : tmp_cfhs) {
        auto tmp_s = tmp_db->DestroyColumnFamilyHandle(handle);
        assert(tmp_s.ok());
      }
      tmp_cfhs.clear();
      delete tmp_db;
    }
  }

  LOG(0) << "##### TOTDB open on normal mode #####";
  DB* db = nullptr;
  s = DB::Open(db_options, dbname, open_cfds, handles, &db);
  if (s.ok()) {
    auto v = new TOTransactionDBImpl(db, txn_db_options, false, stable_ts_key);
    v->StartBackgroundCleanThread();
    *dbptr = v;
  }

  LOG(0) << "##### TOTDB open success #####";
  return s;
}

Status TOTransactionDBImpl::UnCommittedKeys::RemoveKeyInLock(
    const TxnKey& key, const size_t& stripe_num,
    std::atomic<int64_t>* mem_usage) {
  UnCommittedLockMapStripe* stripe = lock_map_stripes_.at(stripe_num);
  auto iter = stripe->uncommitted_keys_map_.find(key);
  invariant(iter != stripe->uncommitted_keys_map_.end());
  auto ccbytes = mem_usage->fetch_sub(TxnKeySize(key) + sizeof(iter->second),
                                      std::memory_order_relaxed);
  invariant(ccbytes >= 0);
  (void)ccbytes;
  stripe->uncommitted_keys_map_.erase(iter);
  return Status::OK();
}

Status TOTransactionDBImpl::UnCommittedKeys::CheckKeyAndAddInLock(
    const TxnKey& key, const TransactionID& txn_id, const size_t& stripe_num,
    uint64_t max_mem_usage, std::atomic<int64_t>* mem_usage) {
  UnCommittedLockMapStripe* stripe = lock_map_stripes_.at(stripe_num);

  auto iter = stripe->uncommitted_keys_map_.find(key);
  if (iter != stripe->uncommitted_keys_map_.end()) {
    // Check whether the key is modified by the same txn
    if (iter->second != txn_id) {
      LOG(2) << "TOTDB WriteConflict another txn id " << iter->second
             << " is modifying key " << rocksdb::Slice(key.second).ToString(true);
      return Status::Busy();
    } else {
      return Status::OK();
    }
  }
  size_t delta_size = TxnKeySize(key) + sizeof(txn_id);
  auto addedSize = mem_usage->load(std::memory_order_relaxed) + delta_size;
  if (addedSize > max_mem_usage) {
    LOG(2) << "TOTDB WriteConflict mem usage " << addedSize
           << " is greater than limit " << max_mem_usage;
    return Status::Busy();
  }
  mem_usage->fetch_add(delta_size, std::memory_order_relaxed);
  stripe->uncommitted_keys_map_.insert({key, txn_id});

  return Status::OK(); 
}

size_t TOTransactionDBImpl::UnCommittedKeys::CountInLock() const {
  size_t res = 0;
  for (size_t i = 0; i < lock_map_stripes_.size(); ++i) {
    res += lock_map_stripes_[i]->uncommitted_keys_map_.size();
  }
  return res;
}

Status TOTransactionDBImpl::CommittedKeys::AddKeyInLock(
    const TxnKey& key, const TransactionID& commit_txn_id,
    const RocksTimeStamp& prepare_ts, const RocksTimeStamp& commit_ts,
    const size_t& stripe_num, std::atomic<int64_t>* mem_usage) {
  CommittedLockMapStripe* stripe = lock_map_stripes_.at(stripe_num);
  if (stripe->committed_keys_map_.find(key) ==
      stripe->committed_keys_map_.end()) {
    mem_usage->fetch_add(TxnKeySize(key) + sizeof(prepare_ts) +
                         sizeof(commit_ts) + sizeof(commit_txn_id));
  }

  stripe->committed_keys_map_[key] =
      std::make_tuple(commit_txn_id, prepare_ts, commit_ts);

  return Status::OK();
}

Status TOTransactionDBImpl::CommittedKeys::CheckKeyInLock(
    const TxnKey& key, const TransactionID& txn_id,
    const RocksTimeStamp& timestamp, const size_t& stripe_num) {
  // padding to avoid false sharing
  CommittedLockMapStripe* stripe = lock_map_stripes_.at(stripe_num);

  auto iter = stripe->committed_keys_map_.find(key);
  if (iter != stripe->committed_keys_map_.end()) {
    // Check whether some txn commits the key after txn_id began
    const TransactionID& committed_txn_id = std::get<0>(iter->second);
    // const RocksTimeStamp& prepare_ts = std::get<1>(iter->second);
    const RocksTimeStamp& committed_ts = std::get<2>(iter->second);
    if (committed_txn_id > txn_id) {
      LOG(2) << "TOTDB WriteConflict a committed txn commit_id " << committed_txn_id
             << " greater than my txnid " << txn_id;
      return Status::Busy("SI conflict");
    }
    // Find the latest committed txn for this key and its commit ts
    if (committed_ts > timestamp) {
      LOG(2) << "TOTDB WriteConflict a committed txn commit_ts " << committed_ts
             << " greater than my read_ts " << timestamp;
      return Status::Busy("timestamp conflict");
    }
  }
  return Status::OK();
}

size_t TOTransactionDBImpl::CommittedKeys::CountInLock() const {
  size_t res = 0;
  for (size_t i = 0; i < lock_map_stripes_.size(); ++i) {
    res += lock_map_stripes_[i]->committed_keys_map_.size();
  }
  return res;
}

Status TOTransactionDBImpl::AddToActiveTxns(const std::shared_ptr<ATN>& active_txn) {
  active_txns_.insert({active_txn->txn_id_, active_txn});
  return Status::OK();
}

TOTransaction* TOTransactionDBImpl::BeginTransaction(const WriteOptions& write_options,
                                     const TOTransactionOptions& txn_options) {
  std::shared_ptr<ATN> newActiveTxnNode = nullptr;
  TOTransaction* newTransaction = nullptr;
  {
    std::lock_guard<std::mutex> lock(active_txns_mutex_);

    TOTxnOptions totxn_option;
    totxn_option.max_write_batch_size = txn_options.max_write_batch_size;
    newActiveTxnNode = std::shared_ptr<ATN>(new ATN);
    newTransaction = new TOTransactionImpl(this, write_options, totxn_option, newActiveTxnNode);

    newActiveTxnNode->txn_id_ = TOTransactionImpl::GenTxnID();
    newActiveTxnNode->txn_snapshot = GetRootDB()->GetSnapshot();
    newActiveTxnNode->timestamp_round_prepared_ =
        txn_options.timestamp_round_prepared;
    newActiveTxnNode->timestamp_round_read_ = txn_options.timestamp_round_read;
    newActiveTxnNode->read_only_ = txn_options.read_only;
    newActiveTxnNode->ignore_prepare_ = txn_options.ignore_prepare;
    // Add the transaction to active txns
    AddToActiveTxns(newActiveTxnNode);
  }
 
  LOG(2) << "TOTDB begin a txn id " << newActiveTxnNode->txn_id_
         << " snapshot " << newActiveTxnNode->txn_snapshot->GetSequenceNumber();
  return newTransaction;

}

Status TOTransactionDBImpl::CheckWriteConflict(const TxnKey& key,
                                               const TransactionID& txn_id,
                                               const RocksTimeStamp& readts) {
  //if first check the uc key and ck busy ,it will need remove uc key ,so we check commit key first
  auto stripe_num = GetStripe(key);
  invariant(keys_mutex_.size() > stripe_num);
  std::lock_guard<std::mutex> lock(*keys_mutex_[stripe_num]);
  // Check whether some txn commits the key after current txn started
  // Check whether the commit ts of latest committed txn for key is less than my read ts
  Status s = committed_keys_.CheckKeyInLock(key, txn_id, readts, stripe_num);
  if (!s.ok()) {
    LOG(2) << "TOTDB txn id " << txn_id
           << " key " << rocksdb::Slice(key.second).ToString(true) << " conflict ck";
    return s;
  }

  // Check whether the key is in uncommitted keys
  // if not, add the key to uncommitted keys
  s = uncommitted_keys_.CheckKeyAndAddInLock(
      key, txn_id, stripe_num,
      max_conflict_bytes_.load(std::memory_order_relaxed),
      &current_conflict_bytes_);
  if (!s.ok()) {
    LOG(2) << "TOTDB txn id " << txn_id
           << " key " << rocksdb::Slice(key.second).ToString(true) << " conflict uk";
    return s;
  }

  return Status::OK();
}

void TOTransactionDBImpl::CleanCommittedKeys() {
  TransactionID txn = 0;
  RocksTimeStamp ts = 0;
  
  while(clean_job_.IsRunning()) {
    // TEST_SYNC_POINT("TOTransactionDBImpl::CleanCommittedKeys::OnRunning");
    if (clean_job_.NeedToClean(&txn, &ts)) {
      uint64_t totalNum = 0;
      uint64_t removeKeys = 0;
      for (size_t i = 0; i < num_stripes_; i++) {
        std::unique_lock<std::mutex> lock(*keys_mutex_[i]);
        CommittedLockMapStripe* stripe = committed_keys_.lock_map_stripes_.at(i);
        totalNum += stripe->committed_keys_map_.size();
        auto map_iter = stripe->committed_keys_map_.begin();
        while (map_iter != stripe->committed_keys_map_.end()) {
          auto history = map_iter->second;
          const TransactionID& txn_id = std::get<0>(history);
          const RocksTimeStamp& prepare_ts = std::get<1>(history);
          const RocksTimeStamp& commit_ts = std::get<2>(history);
          if (txn_id <= txn && (commit_ts < ts || commit_ts == 0)) {
            auto ccbytes = current_conflict_bytes_.fetch_sub(
                TxnKeySize(map_iter->first) + sizeof(txn_id) +
                    sizeof(prepare_ts) + sizeof(commit_ts),
                std::memory_order_relaxed);
            invariant(ccbytes >= 0);
            (void)ccbytes;
            map_iter = stripe->committed_keys_map_.erase(map_iter);
            removeKeys++;
          } else {
            map_iter++;
          }
        }
      }
      prepare_heap_.Purge(txn, ts);
      clean_job_.FinishClean(txn, ts);
      if (totalNum < 10000 || removeKeys * 8 < totalNum) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  } 
  // End run clean thread
}

Status TOTransactionDBImpl::GetConsiderPrepare(
    const std::shared_ptr<ATN>& core, ReadOptions& options,
    ColumnFamilyHandle* column_family, const Slice& key, std::string* value) {
  RocksTimeStamp read_ts = core->read_ts_set_
                               ? core->read_ts_
                               : std::numeric_limits<RocksTimeStamp>::max();
  (void)read_ts;
  const auto lookup_key = std::make_pair(column_family->GetID(),
                                         std::string(key.data(), key.size()));
  TOTransaction::TOTransactionState state;
  std::shared_ptr<ATN> it = prepare_heap_.Find(core.get(), lookup_key, &state);
  // 1) key is not in prepare_heap_, read data from self and db
  if (it == nullptr) {
    return core->write_batch_.GetFromBatchAndDB(GetRootDB(), options,
                                                column_family, key, value);
  }

  // 2) myself should not appear in prepare_heap because not allowed to read
  // after prepared
  invariant(it->txn_id_ != core->txn_id_);
  invariant(it->prepare_ts_set_);
  invariant(state != TOTransaction::TOTransactionState::kRollback);

  // 3) if the txn in prepare_ts_set_ is actually prepared
  if (state == TOTransaction::TOTransactionState::kPrepared) {
    invariant(it->prepare_ts_set_);
    // when picking node from PrepareHeap, we've already considered ts
    // relationship.
    invariant(it->prepare_ts_ <= read_ts);
    if (core->ignore_prepare_) {
      SwapSnapshotGuard guard(&options, nullptr);
      return core->write_batch_.GetFromBatchAndDB(GetRootDB(), options,
                                                  column_family, key, value);
    }
    return PrepareConflict();
  }

  invariant(state == TOTransaction::TOTransactionState::kCommitted);
  SwapSnapshotGuard guard(&options, nullptr);
  return core->write_batch_.GetFromBatchAndDB(GetRootDB(), options,
                                              column_family, key, value);
}

Iterator* TOTransactionDBImpl::NewIteratorConsiderPrepare(
    const std::shared_ptr<ATN>& core, ColumnFamilyHandle* column_family,
    Iterator* db_iter) {
  std::unique_ptr<Iterator> base_writebatch(
      core->write_batch_.NewIteratorWithBase(column_family, db_iter));
  auto pmap_iter = std::unique_ptr<PrepareMapIterator>(
      new PrepareMapIterator(column_family, &prepare_heap_, core.get()));
  auto merge_iter =
      std::unique_ptr<PrepareMergingIterator>(new PrepareMergingIterator(
          std::move(base_writebatch), std::move(pmap_iter)));
  return new PrepareFilterIterator(GetRootDB(), column_family, core, std::move(merge_iter));
}

void TOTransactionDBImpl::StartBackgroundCleanThread() {
  clean_thread_ = std::thread([this] { CleanCommittedKeys(); });
}

void TOTransactionDBImpl::AdvanceTS(RocksTimeStamp* pMaxToCleanTs) {
  RocksTimeStamp max_to_clean_ts = 0;

  {
    std::shared_lock<std::shared_mutex> rl(ts_meta_mutex_);
    if (oldest_ts_ != nullptr) {
      max_to_clean_ts = *oldest_ts_;
    }
  }

  {
    std::shared_lock<std::shared_mutex> rl(read_ts_mutex_);
    for (auto it = read_q_.begin(); it != read_q_.end();) {
      if (it->second->state_.load() == TOTransaction::kStarted) {
        invariant(it->second->read_ts_set_);
        max_to_clean_ts = std::min(max_to_clean_ts, it->second->read_ts_);
        break;
      }
      it++;
    }
  }
  *pMaxToCleanTs = max_to_clean_ts;

  return;
}

Status TOTransactionDBImpl::AddReadQueue(const std::shared_ptr<ATN>& core,
                                         const RocksTimeStamp& ts) {
  RocksTimeStamp realTs = ts;
  std::shared_lock<std::shared_mutex> rl(ts_meta_mutex_);
  if (oldest_ts_ != nullptr) {
    if (realTs < *oldest_ts_) {
      if (!core->timestamp_round_read_) {
        return Status::InvalidArgument("read-ts smaller than oldest ts");
      }
      realTs = *oldest_ts_;
    } else {
      // otherwise, realTs >= *oldest_ts_
    }
  }

  // take care of the critical area, read_ts_mutex_ is within
  // ts_meta_mutex_
  std::unique_lock<std::shared_mutex> wl(read_ts_mutex_);
  invariant(!core->read_ts_set_);
  invariant(core->state_.load() == TOTransaction::kStarted);
  // we have to clean commited/aboarted txns, right?
  // we just start from the beginning and clean until the first active txn
  // This is only a strategy and has nothing to do with the correctness
  // You may design other strategies.
  for (auto it = read_q_.begin(); it != read_q_.end();) {
    if (it->second->state_.load() == TOTransaction::kStarted) {
      break;
    }
    // TODO: add walk stat
    it = read_q_.erase(it);
  }
  core->read_ts_set_ = true;
  core->read_ts_ = realTs;
  read_q_.insert({{realTs, core->txn_id_}, core});
  return Status::OK();
}

Status TOTransactionDBImpl::PublushTimeStamp(const std::shared_ptr<ATN>& core) {
  if (core->timestamp_published_) {
    return Status::OK();
  }

  RocksTimeStamp ts;
  if (core->commit_ts_set_) {
    /*
     * If we know for a fact that this is a prepared transaction and we only
     * have a commit timestamp, don't add to the durable queue. If we poll
     * all_durable after
     * setting the commit timestamp of a prepared transaction, that prepared
     * transaction
     * should NOT be visible.
     */
    if (core->state_.load(std::memory_order_relaxed) ==
        TOTransaction::kPrepared) {
      return Status::OK();
    }
    ts = core->commit_ts_;
  } else {
    // no reason to reach here
    invariant(0);
  }

  {
    std::unique_lock<std::shared_mutex> wl(commit_ts_mutex_);
    invariant(core->state_.load() == TOTransaction::kStarted ||
           core->state_.load() == TOTransaction::kPrepared);
    for (auto it = commit_q_.begin(); it != commit_q_.end();) {
      const auto state = it->second->state_.load(std::memory_order_relaxed);
      if (state == TOTransaction::kStarted ||
          state == TOTransaction::kPrepared) {
        break;
      }
      // TODO: add walk stat
      it = commit_q_.erase(it);
    }
    commit_q_.insert({{ts, core->txn_id_}, core});
    core->timestamp_published_ = true;
  }
  return Status::OK();
}

Status TOTransactionDBImpl::SetDurableTimeStamp(
    const std::shared_ptr<ATN>& core, const RocksTimeStamp& ts) {
  if (ts == 0) {
    return Status::NotSupported("not allowed to set durable-ts to 0");
  }
  if (core->state_.load(std::memory_order_relaxed) !=
      TOTransaction::kPrepared) {
    return Status::InvalidArgument(
        "durable timestamp should not be specified for non-prepared "
        "transaction");
  }
  if (!core->commit_ts_set_) {
    return Status::InvalidArgument(
        "commit timestamp is needed before the durable timestamp");
  }
  bool has_oldest = false;
  uint64_t tmp_oldest = 0;
  {
    std::shared_lock<std::shared_mutex> rl(ts_meta_mutex_);
    has_oldest = (oldest_ts_ != nullptr);
    tmp_oldest = has_oldest ? *oldest_ts_ : 0;
  }
  if (has_oldest && ts < tmp_oldest) {
    return Status::InvalidArgument(
        "durable timestamp is less than oldest timestamp");
  }
  if (ts < core->commit_ts_) {
    return Status::InvalidArgument(
        "durable timestamp is less than commit timestamp");
  }
  core->durable_ts_ = ts;
  core->durable_ts_set_ = true;
  return PublushTimeStamp(core);
}

Status TOTransactionDBImpl::SetCommitTimeStamp(const std::shared_ptr<ATN>& core,
                                               const RocksTimeStamp& ts_orig) {
  RocksTimeStamp ts = ts_orig;
  if (ts == 0) {
    return Status::NotSupported("not allowed to set committs to 0");
  }
  bool has_oldest = false;
  uint64_t tmp_oldest = 0;
  {
    std::shared_lock<std::shared_mutex> rl(ts_meta_mutex_);
    has_oldest = (oldest_ts_ != nullptr);
    tmp_oldest = has_oldest ? *oldest_ts_ : 0;
  }
  if (!core->prepare_ts_set_) {
    if (has_oldest && ts < tmp_oldest) {
      return Status::InvalidArgument("commit-ts smaller than oldest-ts");
    }
    if (core->commit_ts_set_ && ts < core->first_commit_ts_) {
      return Status::InvalidArgument("commit-ts smaller than first-commit-ts");
    }
  } else {
    if (core->prepare_ts_ > ts) {
      if (!core->timestamp_round_prepared_) {
        return Status::InvalidArgument("commit-ts smaller than prepare-ts");
      }
      ts = core->prepare_ts_;
    }
  }

  invariant(!core->durable_ts_set_ || core->durable_ts_ == core->commit_ts_);
  core->commit_ts_ = ts;
  if (!core->commit_ts_set_) {
    core->first_commit_ts_ = ts;
  }
  if (!core->durable_ts_set_) {
    core->durable_ts_ = ts;
  }
  core->commit_ts_set_ = true;
  return PublushTimeStamp(core);
}

Status TOTransactionDBImpl::TxnAssertAfterReads(
    const std::shared_ptr<ATN>& core, const char* op,
    const RocksTimeStamp& timestamp) {
  uint64_t walk_cnt = 0;
  std::shared_lock<std::shared_mutex> rl(read_ts_mutex_);
  for (auto it = read_q_.rbegin(); it != read_q_.rend(); it++) {
    walk_cnt++;
    // txns with ignore_prepare_ == true should not be considered
    if (it->second->state_.load() != TOTransaction::kStarted ||
        it->second->ignore_prepare_) {
      continue;
    }
    if (it->second->txn_id_ == core->txn_id_) {
      continue;
    }
    invariant(it->second->read_ts_set_);
    read_q_walk_len_sum_.fetch_add(read_q_.size(), std::memory_order_relaxed);
    read_q_walk_times_.fetch_add(walk_cnt, std::memory_order_relaxed);
    if (it->second->read_ts_ >= timestamp) {
      LOG(2) << op << " timestamp: " << timestamp
             << " must be greater than the latest active readts " << it->second->read_ts_;
      return Status::InvalidArgument(
          "commit/prepare ts must be greater than latest readts");
    } else {
      return Status::OK();
    }
  }

  return Status::OK();
}

Status TOTransactionDBImpl::PrepareTransaction(
    const std::shared_ptr<ATN>& core) {
  invariant(core->prepare_ts_set_);
  prepare_heap_.Insert(core);
  invariant(core->state_ == TOTransaction::TOTransactionState::kPrepared);
  return Status::OK();
}

Status TOTransactionDBImpl::SetPrepareTimeStamp(
    const std::shared_ptr<ATN>& core, const RocksTimeStamp& timestamp) {
  invariant(!core->prepare_ts_set_);
  auto s = TxnAssertAfterReads(core, "prepare", timestamp);
  if (!s.ok()) {
    return s;
  }

  RocksTimeStamp tmp_oldest = 0;
  {
    std::shared_lock<std::shared_mutex> rl(ts_meta_mutex_);
    if (oldest_ts_ != nullptr) {
      tmp_oldest = *oldest_ts_;
    }
  }

  // NOTE(deyukong): here wt did not set ts within ts_meta_mutex_, I just
  // followed
  if (timestamp < tmp_oldest) {
    if (core->timestamp_round_prepared_) {
      core->prepare_ts_ = tmp_oldest;
      core->prepare_ts_set_ = true;
      LOG(2) << "TOTDB round txn " << core->txn_id_ << " prepare_ts " << timestamp
             << " to oldest " << tmp_oldest;
      return Status::OK();
    } else {
      return Status::InvalidArgument(
          "prepareTs should not be less than oldest ts");
    }
  } else {
    core->prepare_ts_ = timestamp;
    core->prepare_ts_set_ = true;
    return Status::OK();
  }

  // void compiler complain, unreachable
  invariant(0);
  return Status::OK();
}

Status TOTransactionDBImpl::CommitTransaction(
    const std::shared_ptr<ATN>& core, const std::set<TxnKey>& written_keys) {
  TransactionID max_to_clean_txn_id = 0;
  RocksTimeStamp max_to_clean_ts = 0;
  RocksTimeStamp candidate_durable_timestamp = 0;
  RocksTimeStamp prev_durable_timestamp = 0;
  bool update_durable_ts = false;
  bool need_clean = false;
  LOG(2) << "TOTDB start to commit txn id " << core->txn_id_
         << " commit ts " << core->commit_ts_;
  // Update Active Txns
  auto state = core->state_.load(std::memory_order_relaxed);
  (void)state;
  invariant(state == TOTransaction::kStarted || state == TOTransaction::kPrepared);
  {
    std::lock_guard<std::mutex> lock(active_txns_mutex_);

    auto iter = active_txns_.find(core->txn_id_);
    invariant(iter != active_txns_.end());

    GetRootDB()->ReleaseSnapshot(iter->second->txn_snapshot);
    core->commit_txn_id_ = TOTransactionImpl::GenTxnID();
    iter->second->state_.store(TOTransaction::kCommitted);

    iter = active_txns_.erase(iter);

    if (core->commit_txn_id_ > committed_max_txnid_) {
      committed_max_txnid_ = core->commit_txn_id_;
    }
	
    if (iter == active_txns_.begin()) {
      need_clean = true;
      if (!active_txns_.empty()) {
        max_to_clean_txn_id = active_txns_.begin()->first - 1;
      } else {
        max_to_clean_txn_id = committed_max_txnid_;
      }
    }
  }
  
  //it's okey to publish commit_ts a little later
  if (core->durable_ts_set_) {
    candidate_durable_timestamp = core->durable_ts_;
  } else if (core->commit_ts_set_) {
    candidate_durable_timestamp = core->commit_ts_;
  }

  if (candidate_durable_timestamp != 0) {
    prev_durable_timestamp = committed_max_ts_.load(std::memory_order_relaxed);
    update_durable_ts = candidate_durable_timestamp > prev_durable_timestamp;
  }
  if (update_durable_ts) {
    while (candidate_durable_timestamp > prev_durable_timestamp) {
      update_max_commit_ts_times_.fetch_add(1, std::memory_order_relaxed);
      if (committed_max_ts_.compare_exchange_strong(
              prev_durable_timestamp, candidate_durable_timestamp)) {
        has_commit_ts_.store(true);
        break;
      }
      update_max_commit_ts_retries_.fetch_add(1, std::memory_order_relaxed);
      prev_durable_timestamp =
          committed_max_ts_.load(std::memory_order_relaxed);
    }
  } else {
    commit_without_ts_times_.fetch_add(1, std::memory_order_relaxed);
  }

  AdvanceTS(&max_to_clean_ts);

  // Move Uncommited keys for this txn to committed keys
  std::map<size_t, std::set<TxnKey>> stripe_keys_map;
  auto keys_iter = written_keys.begin();
  while (keys_iter != written_keys.end()) {
    auto stripe_num = GetStripe(*keys_iter); 
    if (stripe_keys_map.find(stripe_num) == stripe_keys_map.end()) {
      stripe_keys_map[stripe_num] = {};
    }
    stripe_keys_map[stripe_num].insert(std::move(*keys_iter));
    keys_iter++;
  }

  auto stripe_keys_iter = stripe_keys_map.begin();
  while (stripe_keys_iter != stripe_keys_map.end()) {
    std::lock_guard<std::mutex> lock(*keys_mutex_[stripe_keys_iter->first]);
    // the key in one txn insert to the CK with the max commit ts
    for (auto& key : stripe_keys_iter->second) {
      committed_keys_.AddKeyInLock(key, core->commit_txn_id_, core->prepare_ts_,
                                   core->commit_ts_, stripe_keys_iter->first,
                                   &current_conflict_bytes_);
      uncommitted_keys_.RemoveKeyInLock(key, stripe_keys_iter->first, &current_conflict_bytes_);
    }

    stripe_keys_iter++;
  }

  LOG(2) << "TOTDB end commit txn id " << core->txn_id_
         << " cid " << core->commit_txn_id_
         << " commit ts " << core->commit_ts_;
  // Clean committed keys
  if (need_clean) {
    // Clean committed keys async
    // Clean keys whose commited txnid <= max_to_clean_txn_id
    // and committed ts < max_to_clean_ts
    LOG(2) << "TOTDB going to clean txnid " << max_to_clean_txn_id
           << " ts " << max_to_clean_ts;
    clean_job_.SetCleanInfo(max_to_clean_txn_id, max_to_clean_ts);
  }

  txn_commits_.fetch_add(1, std::memory_order_relaxed);
  if (core->read_ts_set_) {
    read_with_ts_times_.fetch_add(1, std::memory_order_relaxed);
  } else {
    read_without_ts_times_.fetch_add(1, std::memory_order_relaxed);
  }

  return Status::OK();
}

Status TOTransactionDBImpl::RollbackTransaction(
    const std::shared_ptr<ATN>& core, const std::set<TxnKey>& written_keys) {
  LOG(2) << "TOTDB start to rollback txn id " << core->txn_id_;
  auto state = core->state_.load(std::memory_order_relaxed);
  invariant(state == TOTransaction::kStarted || state == TOTransaction::kPrepared);
  // clean prepare_heap_ before set state to keep the invariant that no
  // kRollback state in prepare_heap
  if (state == TOTransaction::kPrepared) {
    uint32_t remove_cnt = prepare_heap_.Remove(core.get());
    (void)remove_cnt;
    invariant(remove_cnt == written_keys.size());
  }
  // Remove txn for active txns
  bool need_clean = false;
  TransactionID max_to_clean_txn_id = 0;
  RocksTimeStamp max_to_clean_ts = 0;
  {
    std::lock_guard<std::mutex> lock(active_txns_mutex_);

    auto iter = active_txns_.find(core->txn_id_);
    invariant(iter != active_txns_.end());
    iter->second->state_.store(TOTransaction::kRollback);
    GetRootDB()->ReleaseSnapshot(iter->second->txn_snapshot);
    iter = active_txns_.erase(iter); 
    
    if (iter == active_txns_.begin()) {
      need_clean = true;
      if (!active_txns_.empty()) {
        max_to_clean_txn_id = active_txns_.begin()->first - 1;
      } else {
        max_to_clean_txn_id = committed_max_txnid_;
      }
    }
  }
  // Calculation the min clean ts between oldest and the read ts
  AdvanceTS(&max_to_clean_ts);

  // Remove written keys from uncommitted keys
  std::map<size_t, std::set<TxnKey>> stripe_keys_map;
  auto keys_iter = written_keys.begin();
  while (keys_iter != written_keys.end()) {
    auto stripe_num = GetStripe(*keys_iter); 
    if (stripe_keys_map.find(stripe_num) == stripe_keys_map.end()) {
      stripe_keys_map[stripe_num] = {};
    } 
    stripe_keys_map[stripe_num].insert(std::move(*keys_iter));

    keys_iter++;
  }

  auto stripe_keys_iter = stripe_keys_map.begin();
  while (stripe_keys_iter != stripe_keys_map.end()) {
    std::lock_guard<std::mutex> lock(*keys_mutex_[stripe_keys_iter->first]);

    for (auto & key : stripe_keys_iter->second) {  
      uncommitted_keys_.RemoveKeyInLock(key, stripe_keys_iter->first, &current_conflict_bytes_);
    }
    stripe_keys_iter++;
  }

  LOG(2) << "TOTDB end rollback txn id " << core->txn_id_;

  if (need_clean) {
    LOG(2) << "TOTDB going to clean txnid " << max_to_clean_txn_id
           << "ts " << max_to_clean_ts;
    clean_job_.SetCleanInfo(max_to_clean_txn_id, max_to_clean_ts);
  }

  txn_aborts_.fetch_add(1, std::memory_order_relaxed);
  if (core->read_ts_set_) {
    read_with_ts_times_.fetch_add(1, std::memory_order_relaxed);
  } else {
    read_without_ts_times_.fetch_add(1, std::memory_order_relaxed);
  }

  return Status::OK();
}

Status TOTransactionDBImpl::SetTimeStamp(const TimeStampType& ts_type,
                                         const RocksTimeStamp& ts, bool force) {
  if (ts_type == kCommitted) {
    // NOTE(deyukong): committed_max_ts_ is not protected by ts_meta_mutex_,
    // its change is by atomic cas. So here it's meaningless to Wlock
    // ts_meta_mutex_ it's app-level's duty to guarantee that when updating
    // kCommitted-timestamp, there is no paralllel running txns that will also
    // change it.
    if (force) {
      LOG(2) << "kCommittedTs force set from " << (has_commit_ts_.load() ? committed_max_ts_.load() : 0)
             << " to " << ts;
    }
    std::shared_lock<std::shared_mutex> rl(ts_meta_mutex_);
    if ((oldest_ts_ != nullptr && *oldest_ts_ > ts) && !force) {
      return Status::InvalidArgument(
          "kCommittedTs should not be less than kOldestTs");
    }
    committed_max_ts_.store(ts);
    has_commit_ts_.store(true);
    return Status::OK();
  }

  if (ts_type == kOldest) {
    // NOTE(deyukong): here we must take lock, every txn's readTs-setting
    // has to be in the same critical area within the set of kOldest
    uint64_t original = 0;
    {
      std::unique_lock<std::shared_mutex> wl(ts_meta_mutex_);
      if ((oldest_ts_ != nullptr && *oldest_ts_ > ts) && !force) {
        LOG(2) << "oldestTs can not travel back, oldest_ts from " << *oldest_ts_
               << " to " << ts
               << " keep oldest_ts " << *oldest_ts_;
        return Status::OK();
      }
      original = (oldest_ts_ == nullptr) ? 0 : *oldest_ts_;
      oldest_ts_.reset(new RocksTimeStamp(ts));
    }
    if (force) {
      LOG(2) << "kOldestTs force set from " << original << " to " << ts;
    }
    auto pin_ts = ts;
    {
      std::shared_lock<std::shared_mutex> rl(read_ts_mutex_);
      uint64_t walk_cnt = 0;
      for (auto it = read_q_.begin(); it != read_q_.end();) {
        if (it->second->state_.load() == TOTransaction::kStarted) {
          invariant(it->second->read_ts_set_);
          pin_ts = std::min(pin_ts, it->second->read_ts_);
          break;
        }
        it++;
        walk_cnt++;
      }
      read_q_walk_len_sum_.fetch_add(read_q_.size(), std::memory_order_relaxed);
      read_q_walk_times_.fetch_add(walk_cnt, std::memory_order_relaxed);
    }
    {
      char buf[sizeof(RocksTimeStamp)];
      Encoder(buf, sizeof(pin_ts)).put64(pin_ts);
      auto s = GetRootDB()->IncreaseFullHistoryTsLow(GetRootDB()->DefaultColumnFamily(), std::string(buf, sizeof(buf))); 
      if (!s.ok()) {
        return s;
      }
    }
    LOG(2) << "TOTDB set oldest ts type " << static_cast<int>(ts_type)
           << " value " << pin_ts;
    return Status::OK();
  }

  if (ts_type == kStable) {
    char val_buf[sizeof(RocksTimeStamp)];
    char ts_buf[sizeof(RocksTimeStamp)];
    Encoder(val_buf, sizeof(ts)).put64(ts);
    Encoder(ts_buf, sizeof(ts)).put64(0);
    auto write_ops = rocksdb::WriteOptions();
    write_ops.sync = true;
    auto s = Put(write_ops, DefaultColumnFamily(), stable_ts_key_, rocksdb::Slice(ts_buf, sizeof(ts_buf)),
                 rocksdb::Slice(val_buf, sizeof(val_buf)));
    if (!s.ok()) {
      return s;
    }
    LOG(2) << "TOTDB set stable ts type " << static_cast<int>(ts_type)
        << " value " << ts;
    return Status::OK();
  }

  return Status::InvalidArgument("invalid ts type");
}

Status TOTransactionDBImpl::QueryTimeStamp(const TimeStampType& ts_type, 
                                           RocksTimeStamp* timestamp) {
  if (ts_type == kAllCommitted) {
    if (!has_commit_ts_.load(/* seq_cst */)) {
      return Status::NotFound("not found");
    }
    auto tmp = committed_max_ts_.load(std::memory_order_relaxed);
    std::shared_lock<std::shared_mutex> rl(commit_ts_mutex_);
    uint64_t walk_cnt = 0;
    for (auto it = commit_q_.begin(); it != commit_q_.end(); ++it) {
      const auto state = it->second->state_.load(std::memory_order_relaxed);
      if (state != TOTransaction::kStarted &&
          state != TOTransaction::kPrepared) {
        walk_cnt++;
        continue;
      }
      invariant(it->second->commit_ts_set_);
      invariant(it->second->first_commit_ts_ > 0);
      invariant(it->second->commit_ts_ >= it->second->first_commit_ts_);
      tmp = std::min(tmp, it->first.first - 1);
      break;
    }
    commit_q_walk_len_sum_.fetch_add(commit_q_.size(), std::memory_order_relaxed);
    commit_q_walk_times_.fetch_add(walk_cnt, std::memory_order_relaxed);
    *timestamp = tmp;
    return Status::OK();
  }
  if (ts_type == kOldest) {
    // NOTE: query oldest is not a frequent thing, so I just
    // take the rlock
    std::shared_lock<std::shared_mutex> rl(ts_meta_mutex_);
    // todo
    std::string ts_holder;
    auto s = GetRootDB()->GetFullHistoryTsLow(GetRootDB()->DefaultColumnFamily(), &ts_holder);
    if (!s.ok()) {
      return s;
    }
    invariant(ts_holder.size() == sizeof(RocksTimeStamp));
    *timestamp = Decoder(ts_holder.data(), ts_holder.size()).get64();
    oldest_ts_.reset(new RocksTimeStamp(*timestamp));
    LOG(2) << "TOTDB query TS type " << static_cast<int>(ts_type) << " value " << *timestamp;
    return Status::OK();
  }
  if (ts_type == kStable) {
    std::string ts_holder;

    char read_ts_buffer_[sizeof(RocksTimeStamp)];
    Encoder(read_ts_buffer_, sizeof(RocksTimeStamp)).put64(mongo::Timestamp::max().asULL());
    auto read_opt = rocksdb::ReadOptions();
    Slice readTs = rocksdb::Slice(read_ts_buffer_, sizeof(read_ts_buffer_));
    read_opt.timestamp = &readTs;

    auto s = Get(read_opt, stable_ts_key_, &ts_holder);
    if (!s.ok()) {
      return s;
    }
    invariant(ts_holder.size() == sizeof(RocksTimeStamp));
    *timestamp = Decoder(ts_holder.data(), ts_holder.size()).get64();
    LOG(2) << "TOTDB query stable TS type " << static_cast<int>(ts_type) << " value " << *timestamp;
    return Status::OK();
  }
  return Status::InvalidArgument("invalid ts_type");
}

Status TOTransactionDBImpl::Stat(TOTransactionStat* stat) {
  if (stat == nullptr) {
    return Status::InvalidArgument("can not accept null as input");
  }
  memset(stat, 0, sizeof(TOTransactionStat));
  stat->max_conflict_bytes =
      max_conflict_bytes_.load(std::memory_order_relaxed);
  stat->cur_conflict_bytes = current_conflict_bytes_.load(std::memory_order_relaxed);
  {
    std::vector<std::unique_lock<std::mutex>> lks;
    for (size_t i = 0; i < num_stripes_; i++) {
      lks.emplace_back(std::unique_lock<std::mutex>(*keys_mutex_[i]));
    }
    stat->uk_num = uncommitted_keys_.CountInLock();
    stat->ck_num = committed_keys_.CountInLock();
  }
  {
    std::lock_guard<std::mutex> lock(active_txns_mutex_);
    stat->alive_txns_num = active_txns_.size();
  }
  {
    std::shared_lock<std::shared_mutex> rl(read_ts_mutex_);
    stat->read_q_num = read_q_.size();
    for (auto it = read_q_.begin(); it != read_q_.end(); it++) {
      invariant(it->second->read_ts_set_);
      if (it->second->state_.load(std::memory_order_relaxed) == TOTransaction::kStarted) {
        stat->min_read_ts = it->second->read_ts_;
        break;
      }
    }
  }
  {
    std::shared_lock<std::shared_mutex> rl(commit_ts_mutex_);
    stat->commit_q_num = commit_q_.size();
    for (auto it = commit_q_.begin(); it != commit_q_.end(); it++) {
      invariant(it->second->commit_ts_set_);
      if (it->second->state_.load(std::memory_order_relaxed) == TOTransaction::kStarted) {
        stat->min_uncommit_ts = it->second->commit_ts_;
        break;
      }
    }
  }
  {
    std::shared_lock<std::shared_mutex> rl(ts_meta_mutex_);
    stat->oldest_ts = oldest_ts_ == nullptr ? 0 : *oldest_ts_;
  }

  stat->max_commit_ts = has_commit_ts_.load() ? committed_max_ts_.load() : 0;
  stat->update_max_commit_ts_times = update_max_commit_ts_times_.load(std::memory_order_relaxed);
  stat->update_max_commit_ts_retries = update_max_commit_ts_retries_.load(std::memory_order_relaxed);
  stat->committed_max_txnid = committed_max_txnid_;
  stat->txn_commits = txn_commits_.load(std::memory_order_relaxed);
  stat->txn_aborts = txn_aborts_.load(std::memory_order_relaxed);
  stat->commit_without_ts_times = commit_without_ts_times_.load(std::memory_order_relaxed);
  stat->read_without_ts_times = read_without_ts_times_.load(std::memory_order_relaxed);
  stat->read_with_ts_times = read_with_ts_times_.load(std::memory_order_relaxed);
  stat->read_q_walk_len_sum = read_q_walk_len_sum_.load(std::memory_order_relaxed);
  stat->read_q_walk_times = read_q_walk_times_.load(std::memory_order_relaxed);
  stat->commit_q_walk_len_sum = commit_q_walk_len_sum_.load(std::memory_order_relaxed);
  stat->commit_q_walk_times = commit_q_walk_times_.load(std::memory_order_relaxed);
  return Status::OK();
}

Status TOTransactionDBImpl::BackgroundCleanJob::SetCleanInfo(const TransactionID& txn_id,
                                                             const RocksTimeStamp& time_stamp) {
  std::lock_guard<std::mutex> lock(thread_mutex_);
  txnid_ = txn_id;
  ts_ = time_stamp;
  return Status::OK();
}

bool TOTransactionDBImpl::BackgroundCleanJob::IsRunning() {
  std::lock_guard<std::mutex> lock(thread_mutex_);
  return thread_state_ == kRunning;
}

bool TOTransactionDBImpl::BackgroundCleanJob::NeedToClean(TransactionID* txn_id,
                                                          RocksTimeStamp* time_stamp) {
  std::lock_guard<std::mutex> lock(thread_mutex_);
  if (thread_state_ != kRunning) {
    return false;
  }
  *txn_id = txnid_;
  *time_stamp = ts_;
  return (txnid_ != 0);
}

void TOTransactionDBImpl::BackgroundCleanJob::FinishClean(const TransactionID& txn_id,
                                                          const RocksTimeStamp& time_stamp) {
  std::lock_guard<std::mutex> lock(thread_mutex_);
  if (txn_id == txnid_ && ts_ == time_stamp) {
    txnid_ = 0;
    ts_ = 0;
  }
}

void TOTransactionDBImpl::BackgroundCleanJob::StopThread() {
  std::lock_guard<std::mutex> lock(thread_mutex_);
  thread_state_ = kStopped;
}

std::unique_ptr<rocksdb::TOTransaction> TOTransactionDBImpl::makeTxn() {
    rocksdb::WriteOptions options;
    rocksdb::TOTransactionOptions txnOptions;
    return std::unique_ptr<rocksdb::TOTransaction>(BeginTransaction(options, txnOptions));
}

}  //  namespace rocksdb

#endif  // ROCKSDB_LITE
