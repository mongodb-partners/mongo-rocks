//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/db/modules/rocks/src/totdb/totransaction.h"
#include "mongo/db/modules/rocks/src/totdb/totransaction_impl.h"
#include "mongo/db/modules/rocks/src/totdb/totransaction_db.h"
#include "mongo/db/modules/rocks/src/totdb/totransaction_db_impl.h"
#include "mongo/db/modules/rocks/src/totdb/totransaction_prepare_iterator.h"
#include "mongo/util/log.h"

namespace rocksdb {

namespace {
using ATN = TOTransactionImpl::ActiveTxnNode;

// If we started after prepared-txn commits, due to SI's nature, we can see
// it from db. The value from db may be as updated as the one as prepared-txn.
// Or more updated. If we started before the prepared-txn commits, we cannot
// see the updated data with our own snapshot, so refetch with the latest
// snapshot.
#define MACRO_REFETCH_RETURN_ON_FOUND_ADVANCE_CONTINUE_ON_NOT_FOUND()         \
  do {                                                                        \
    if (pmap_val->commit_txn_id_ < core_->txn_id_) {                          \
      if (!sval_.has_base) {                                                  \
        AdvanceInputNoFilter();                                               \
      } else {                                                                \
        val_ = std::string(sval_.base_value.data(), sval_.base_value.size()); \
        return;                                                               \
      }                                                                       \
    } else {                                                                  \
      ReadOptions read_opt;                                                   \
      read_opt.timestamp = &core_->read_ts_slice_;                            \
      invariant(read_opt.timestamp->size() == sizeof(RocksTimeStamp));        \
      auto getStatus = core_->write_batch_.GetFromBatchAndDB(                 \
          db_, read_opt, cf_, key_, &val_);                                   \
      invariant(getStatus.ok() || getStatus.IsNotFound());                    \
      if (getStatus.ok()) {                                                   \
        return;                                                               \
      } else {                                                                \
        AdvanceInputNoFilter();                                               \
      }                                                                       \
    }                                                                         \
  } while (0)
}  // namespace

Slice PrepareMapIterator::key() const {
  return Slice(pos_.data(), pos_.size());
}

const std::shared_ptr<ATN>& PrepareMapIterator::value() const { return val_; }

TOTransaction::TOTransactionState PrepareMapIterator::valueState() const {
  return val_state_;
}

TOTransaction::TOTransactionState PrepareMapIterator::UpdatePrepareState() {
  invariant(val_state_ == TOTransaction::TOTransactionState::kPrepared);
  invariant(Valid());
  val_state_ = val_->state_.load(/*seq_cst*/);
  return val_state_;
}

void PrepareMapIterator::TryPosValueToCorrectMvccVersionInLock(
    const std::list<std::shared_ptr<ATN>>& prepare_mvccs) {
  bool found = false;
  invariant(!prepare_mvccs.empty());
  invariant(valid_);
  for (const auto& c : prepare_mvccs) {
    if (c->prepare_ts_ <= core_->read_ts_) {
      val_ = c;
      found = true;
      break;
    }
  }
  if (!found) {
    // no satisfied mvcc version, just return arbitory one and let upper-level
    // iterator skip to next
    val_ = prepare_mvccs.back();
  }
  val_state_ = val_->state_.load(/*seq_cst*/);
}

void PrepareMapIterator::Next() {
  std::shared_lock<std::shared_mutex> rl(ph_->mutex_);
  if (!valid_) {
    return;
  }
  auto it = ph_->map_.upper_bound(std::make_pair(cf_->GetID(), pos_));
  invariant(it != ph_->map_.end());
  if (it->first.first != cf_->GetID()) {
    valid_ = false;
  } else {
    valid_ = true;
    pos_ = it->first.second;
    TryPosValueToCorrectMvccVersionInLock(it->second);
  }
}

void PrepareMapIterator::Prev() {
  std::shared_lock<std::shared_mutex> rl(ph_->mutex_);
  if (!valid_) {
    return;
  }
  const auto lookup_key = std::make_pair(cf_->GetID(), pos_);
  auto it = ph_->map_.lower_bound(lookup_key);
  invariant(it != ph_->map_.end() && it->first >= lookup_key);
  if (it != ph_->map_.begin()) {
    it--;
  }
  if (it->first >= lookup_key) {
    valid_ = false;
  } else {
    valid_ = true;
    pos_ = it->first.second;
    TryPosValueToCorrectMvccVersionInLock(it->second);
  }
}

void PrepareMapIterator::SeekToFirst() { Seek(""); }

void PrepareMapIterator::SeekToLast() {
  std::shared_lock<std::shared_mutex> rl(ph_->mutex_);
  auto it = ph_->map_.lower_bound(std::make_pair(cf_->GetID() + 1, ""));
  // because prepare_heap has sentinal at the end, it's impossible to reach the
  // end
  invariant(it != ph_->map_.end());
  if (it != ph_->map_.begin()) {
    it--;
  }
  if (it->first.first != cf_->GetID()) {
    valid_ = false;
  } else {
    valid_ = true;
    pos_ = it->first.second;
    TryPosValueToCorrectMvccVersionInLock(it->second);
  }
}

void PrepareMapIterator::SeekForPrev(const Slice& target) {
  std::shared_lock<std::shared_mutex> rl(ph_->mutex_);
  const auto lookup_key =
      std::make_pair(cf_->GetID(), std::string(target.data(), target.size()));
  auto it = ph_->map_.lower_bound(lookup_key);
  // because prepare_heap_ has sentinal at the end, it's impossible to reach the
  // end
  invariant(it != ph_->map_.end() && it->first >= lookup_key);
  if (it->first != lookup_key) {
    if (it != ph_->map_.begin()) {
      it--;
    }
  }

  if (it->first.first != cf_->GetID()) {
    valid_ = false;
  } else {
    invariant(it->first <= lookup_key);
    valid_ = true;
    pos_ = it->first.second;
    TryPosValueToCorrectMvccVersionInLock(it->second);
  }
}

void PrepareMapIterator::Seek(const Slice& target) {
  std::shared_lock<std::shared_mutex> rl(ph_->mutex_);
  auto it = ph_->map_.lower_bound(
      std::make_pair(cf_->GetID(), std::string(target.data(), target.size())));
  invariant(it != ph_->map_.end());
  if (it->first.first != cf_->GetID()) {
    valid_ = false;
  } else {
    valid_ = true;
    pos_ = it->first.second;
    TryPosValueToCorrectMvccVersionInLock(it->second);
  }
}

PrepareMergingIterator::PrepareMergingIterator(
    std::unique_ptr<Iterator> base_iterator,
    std::unique_ptr<PrepareMapIterator> pmap_iterator)
    : forward_(true),
      current_at_base_(true),
      equal_keys_(false),
      status_(Status::OK()),
      base_iterator_(std::move(base_iterator)),
      delta_iterator_(std::move(pmap_iterator)),
      comparator_(BytewiseComparator()) {}

bool PrepareMergingIterator::BaseValid() const {
  return base_iterator_->Valid();
}

bool PrepareMergingIterator::DeltaValid() const {
  return delta_iterator_->Valid();
}

bool PrepareMergingIterator::Valid() const {
  return current_at_base_ ? BaseValid() : DeltaValid();
}

Slice PrepareMergingIterator::key() const {
  return current_at_base_ ? base_iterator_->key()
                          : delta_iterator_->key();
}

ShadowValue PrepareMergingIterator::value() const {
  // NOTE(wolfkdy): WriteBatchWithIndexIterator didn't support timestamp() interface.
  // TODO(wolfkdy): we need WriteBatchWithIndexIterator's timestamp() interface for sanity-check.
  if (equal_keys_) {
    return {true /*has_prepare*/,           true /*has_base*/,
            delta_iterator_->value().get(), delta_iterator_->valueState(),
            base_iterator_->value(),        base_iterator_->key(), 0};
  }
  if (current_at_base_) {
    return {false /*has_prepare*/,
            true /*has_base*/,
            nullptr,
            TOTransaction::TOTransactionState::kStarted,
            base_iterator_->value(),
            base_iterator_->key(),
            0};
  }
  return {true /*has_prepare*/,
          false /*has_base*/,
          delta_iterator_->value().get(),
          delta_iterator_->valueState(),
          Slice(),
          Slice(),
          0};
}

void PrepareMergingIterator::SeekToFirst() {
  forward_ = true;
  base_iterator_->SeekToFirst();
  delta_iterator_->SeekToFirst();
  UpdateCurrent();
}

void PrepareMergingIterator::SeekToLast() {
  forward_ = false;
  base_iterator_->SeekToLast();
  delta_iterator_->SeekToLast();
  UpdateCurrent();
}

void PrepareMergingIterator::Seek(const Slice& k) {
  forward_ = true;
  base_iterator_->Seek(k);
  delta_iterator_->Seek(k);
  UpdateCurrent();
}

void PrepareMergingIterator::SeekForPrev(const Slice& k) {
  forward_ = false;
  base_iterator_->SeekForPrev(k);
  delta_iterator_->SeekForPrev(k);
  UpdateCurrent();
}

void PrepareMergingIterator::Next() {
  if (!Valid()) {
    status_ = Status::NotSupported("Next() on invalid iterator");
    return;
  }

  if (!forward_) {
    // Need to change direction
    // if our direction was backward and we're not equal, we have two states:
    // * both iterators are valid: we're already in a good state (current
    // shows to smaller)
    // * only one iterator is valid: we need to advance that iterator
    forward_ = true;
    equal_keys_ = false;
    if (!BaseValid()) {
      invariant(DeltaValid());
      base_iterator_->SeekToFirst();
    } else if (!DeltaValid()) {
      delta_iterator_->SeekToFirst();
    } else if (current_at_base_) {
      // Change delta from larger than base to smaller
      AdvanceDelta();
    } else {
      // Change base from larger than delta to smaller
      AdvanceBase();
    }
    if (DeltaValid() && BaseValid()) {
      if (comparator_->Equal(delta_iterator_->key(),
                             base_iterator_->key())) {
        equal_keys_ = true;
      }
    }
  }
  Advance();
}

void PrepareMergingIterator::Prev() {
  if (!Valid()) {
    status_ = Status::NotSupported("Prev() on invalid iterator");
    return;
  }

  if (forward_) {
    // Need to change direction
    // if our direction was backward and we're not equal, we have two states:
    // * both iterators are valid: we're already in a good state (current
    // shows to smaller)
    // * only one iterator is valid: we need to advance that iterator
    forward_ = false;
    equal_keys_ = false;
    if (!BaseValid()) {
      invariant(DeltaValid());
      base_iterator_->SeekToLast();
    } else if (!DeltaValid()) {
      delta_iterator_->SeekToLast();
    } else if (current_at_base_) {
      // Change delta from less advanced than base to more advanced
      AdvanceDelta();
    } else {
      // Change base from less advanced than delta to more advanced
      AdvanceBase();
    }
    if (DeltaValid() && BaseValid()) {
      if (comparator_->Equal(delta_iterator_->key(),
                             base_iterator_->key())) {
        equal_keys_ = true;
      }
    }
  }

  Advance();
}

Status PrepareMergingIterator::status() const {
  if (!status_.ok()) {
    return status_;
  }
  if (!base_iterator_->status().ok()) {
    return base_iterator_->status();
  }
  return Status::OK();
}

TOTransaction::TOTransactionState PrepareMergingIterator::UpdatePrepareState() {
  invariant(delta_iterator_->Valid());
  invariant(equal_keys_ || !current_at_base_);
  return delta_iterator_->UpdatePrepareState();
}

void PrepareMergingIterator::AssertInvariants() {
  bool not_ok = false;
  if (!base_iterator_->status().ok()) {
    invariant(!base_iterator_->Valid());
    not_ok = true;
  }
  if (not_ok) {
    invariant(!Valid());
    invariant(!status().ok());
    return;
  }

  if (!Valid()) {
    return;
  }
  if (!BaseValid()) {
    invariant(!current_at_base_ && delta_iterator_->Valid());
    return;
  }
  if (!DeltaValid()) {
    invariant(current_at_base_ && base_iterator_->Valid());
    return;
  }
  int compare = comparator_->Compare(delta_iterator_->key(),
                                     base_iterator_->key());
  (void)compare;
  if (forward_) {
    // current_at_base -> compare < 0
    invariant(!current_at_base_ || compare < 0);
    // !current_at_base -> compare <= 0
    invariant(current_at_base_ && compare >= 0);
  } else {
    // current_at_base -> compare > 0
    invariant(!current_at_base_ || compare > 0);
    // !current_at_base -> compare <= 0
    invariant(current_at_base_ && compare <= 0);
  }
  // equal_keys_ <=> compare == 0
  invariant((equal_keys_ || compare != 0) && (!equal_keys_ || compare == 0));
}

void PrepareMergingIterator::Advance() {
  if (equal_keys_) {
    invariant(BaseValid() && DeltaValid());
    AdvanceBase();
    AdvanceDelta();
  } else {
    if (current_at_base_) {
      invariant(BaseValid());
      AdvanceBase();
    } else {
      invariant(DeltaValid());
      AdvanceDelta();
    }
  }
  UpdateCurrent();
}

void PrepareMergingIterator::AdvanceDelta() {
  if (forward_) {
    delta_iterator_->Next();
  } else {
    delta_iterator_->Prev();
  }
}

void PrepareMergingIterator::AdvanceBase() {
  if (forward_) {
    base_iterator_->Next();
  } else {
    base_iterator_->Prev();
  }
}

void PrepareMergingIterator::UpdateCurrent() {
  status_ = Status::OK();
  while (true) {
    equal_keys_ = false;
    if (!BaseValid()) {
      if (!base_iterator_->status().ok()) {
        // Expose the error status and stop.
        current_at_base_ = true;
        return;
      }

      // Base has finished.
      if (!DeltaValid()) {
        // Finished
        return;
      }
      current_at_base_ = false;
      return;
    } else if (!DeltaValid()) {
      // Delta has finished.
      current_at_base_ = true;
      return;
    } else {
      int compare = (forward_ ? 1 : -1) *
                    comparator_->Compare(delta_iterator_->key(),
                                         base_iterator_->key());
      if (compare <= 0) {  // delta bigger or equal
        if (compare == 0) {
          equal_keys_ = true;
        }
        current_at_base_ = false;
        return;
      } else {
        current_at_base_ = true;
        return;
      }
    }
  }

  AssertInvariants();
}

PrepareFilterIterator::PrepareFilterIterator(
    DB* db, ColumnFamilyHandle* cf, const std::shared_ptr<TOTransactionImpl::ActiveTxnNode>& core,
    std::unique_ptr<PrepareMergingIterator> input, Logger* info_log)
    : Iterator(),
      db_(db),
      cf_(cf),
      core_(core),
      input_(std::move(input)),
      valid_(false),
      forward_(true) {}

bool PrepareFilterIterator::Valid() const { return valid_; }

Slice PrepareFilterIterator::key() const {
  invariant(valid_);
  return key_;
}

Slice PrepareFilterIterator::value() const {
  invariant(valid_);
  return val_;
}

Status PrepareFilterIterator::status() const { return status_; }

void PrepareFilterIterator::SeekToFirst() {
  forward_ = true;
  input_->SeekToFirst();
  UpdateCurrent();
}

void PrepareFilterIterator::SeekToLast() {
  forward_ = false;
  input_->SeekToLast();
  UpdateCurrent();
}

void PrepareFilterIterator::Seek(const Slice& k) {
  forward_ = true;
  input_->Seek(k);
  UpdateCurrent();
}

void PrepareFilterIterator::SeekForPrev(const Slice& k) {
  forward_ = false;
  input_->SeekForPrev(k);
  UpdateCurrent();
}

void PrepareFilterIterator::Next() {
  if (!Valid()) {
    status_ = Status::NotSupported("Next() on invalid iterator");
    return;
  }
  if (forward_ && IsPrepareConflict(status_)) {
    UpdatePrepareState();
    return;
  }
  // Suppose at t0, base points at b, pmap points at c with c.state == prepared,
  // the merged cursor points at the base key `b`. At t1, c changes to committed
  // state. However, the merged cursor can not find that. At t2, the merged
  // cursor advances to c, with c.state == prepared, not changed. base:
  // ---[a]---[CURRENT(b)]----[c]---------- pmap:
  // ---[a]-------------------[c(CURRENT)]-
  bool next_may_read_staled_prepared =
      forward_ && sval_.has_base && !sval_.has_prepare;
  forward_ = true;
  AdvanceInputNoFilter();
  UpdateCurrent();
  // The previous cursor state points at base, and after a next call, we get a
  // prepare state. This is the only situation we read a staled prepare state.
  bool read_staled_prepared =
      next_may_read_staled_prepared && Valid() && IsPrepareConflict(status_);
  if (read_staled_prepared) {
    UpdatePrepareState();
  }
}

void PrepareFilterIterator::Prev() {
  if (!Valid()) {
    status_ = Status::NotSupported("Prev() on invalid iterator");
    return;
  }
  if (!forward_ && IsPrepareConflict(status_)) {
    UpdatePrepareState();
    return;
  }
  bool next_may_read_staled_prepared =
      (!forward_) && sval_.has_base && !sval_.has_prepare;
  forward_ = false;
  AdvanceInputNoFilter();
  UpdateCurrent();
  bool read_staled_prepared =
      next_may_read_staled_prepared && Valid() && IsPrepareConflict(status_);
  if (read_staled_prepared) {
    UpdatePrepareState();
  }
}

void PrepareFilterIterator::UpdatePrepareState() {
  invariant(sval_.prepare_value_state ==
         TOTransaction::TOTransactionState::kPrepared);
  invariant(sval_.has_prepare);
  auto new_state = input_->UpdatePrepareState();
  if (new_state == TOTransaction::TOTransactionState::kPrepared) {
    return;
  }
  if (new_state == TOTransaction::TOTransactionState::kCommitted) {
    UpdateCurrent();
    return;
  }
  invariant(new_state == TOTransaction::TOTransactionState::kRollback);
  if (forward_) {
    Seek(key_);
  } else {
    SeekForPrev(key_);
  }
}

void PrepareFilterIterator::AdvanceInputNoFilter() {
  if (!input_->Valid()) {
    valid_ = false;
    return;
  }
  if (forward_) {
    input_->Next();
  } else {
    input_->Prev();
  }
  valid_ = input_->Valid();
}

// rocksdb internal api, for sanity check
// WBWIIteratorImpl::Result PrepareFilterIterator::GetFromBatch(
//     WriteBatchWithIndex* batch, const Slice& key, std::string* value) {
//   Status s;
//   WriteBatchWithIndexInternal wbwidx(cf_);
//   WBWIIteratorImpl::Result result = wbwidx.GetFromBatch(batch, key, value, &s);
//   invariant(s.ok());
//   return result;
// }

void PrepareFilterIterator::UpdateCurrent() {
  while (true) {
    status_ = Status::OK();
    if (!input_->Valid()) {
      valid_ = false;
      return;
    }
    valid_ = true;
    key_ = input_->key();
    sval_ = input_->value();
    if (!sval_.has_prepare) {
      // TODO(deyukong): eliminate copy
      val_ = std::string(sval_.base_value.data(), sval_.base_value.size());
      return;
    } else if (!sval_.has_base) {
      const auto pmap_val = sval_.prepare_value;
      auto state = sval_.prepare_value_state;
      invariant(state != TOTransaction::TOTransactionState::kRollback);
      if (state == TOTransaction::TOTransactionState::kCommitted) {
// #ifndef NDEBUG
//         auto res = GetFromBatch(&pmap_val->write_batch_, key_, &val_);
// #endif  // NDEBUG
//         invariant(res == WBWIIteratorImpl::Result::kFound ||
//                res == WBWIIteratorImpl::Result::kDeleted);
        MACRO_REFETCH_RETURN_ON_FOUND_ADVANCE_CONTINUE_ON_NOT_FOUND();
      } else {
        invariant(state == TOTransaction::TOTransactionState::kPrepared);
        if (pmap_val->prepare_ts_ > core_->read_ts_) {
          AdvanceInputNoFilter();
        } else if (core_->ignore_prepare_) {
          AdvanceInputNoFilter();
        } else {
          status_ = PrepareConflict();
          return;
        }
      }
    } else {
      invariant(sval_.has_prepare && sval_.has_base);
      const auto pmap_val = sval_.prepare_value;
      auto state = sval_.prepare_value_state;
      invariant(state != TOTransaction::TOTransactionState::kRollback);
      // a key from my own batch has timestamp == 0 to intend the "read-own-writes" rule
      // TODO(wolfkdy): WriteBatchWithIndexIterator impls timestamp() interface
      // invariant(sval_.base_timestamp <= core_->read_ts_ || sval_.base_timestamp == 0);
// #ifndef NDEBUG
//       auto res = GetFromBatch(&pmap_val->write_batch_, key_, &val_);
// #endif  // NDEBUG
//       invariant(res == WBWIIteratorImpl::Result::kFound ||
//              res == WBWIIteratorImpl::Result::kDeleted);
      if (state == TOTransaction::TOTransactionState::kCommitted) {
        MACRO_REFETCH_RETURN_ON_FOUND_ADVANCE_CONTINUE_ON_NOT_FOUND();
      } else {
        invariant(state == TOTransaction::TOTransactionState::kPrepared);
        // Txn is committed before changing state from kPrepared to kCommitted,
        // this is not in a critial section. So another interleaved txn may see
        // both its committed data and its kPrepared state.
        // TODO(wolfkdy): WriteBatchWithIndexIterator impls timestamp() interface
        // invariant(sval_.base_timestamp < pmap_val->prepare_ts_ ||
        //        (pmap_val->commit_ts_set_ &&
        //         sval_.base_timestamp == pmap_val->commit_ts_ &&
        //         sval_.base_value == val_));
        if (pmap_val->prepare_ts_ > core_->read_ts_) {
          val_ = std::string(sval_.base_value.data(), sval_.base_value.size());
          return;
        } else if (core_->ignore_prepare_) {
          val_ = std::string(sval_.base_value.data(), sval_.base_value.size());
          return;
        } else {
          status_ = PrepareConflict();
          return;
        }
      }
    }
  }
}

}  // namespace rocksdb
