//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include "mongo/db/modules/rocks/src/totdb/totransaction_db_impl.h"
#include "mongo/db/modules/rocks/src/totdb/totransaction_impl.h"

namespace rocksdb {

// PrepareFilterIterator
//   |
//   --PrepareMergingIterator
//       |
//       -- PrepareMapIterator
//       |
//       -- BaseIterator
//            |
//            -- WriteBatchWithIndexIterator
//            |
//            -- Normal LsmTree Iterator
//                 |
//                 -- ......
//
// 1) `PrepareFilterIterator` checks prepare status of an input entry and
// decides
// to return, wait, or advance to the next record
// 2) PrepareMergingIterator arranges PrepareMapIterator and BaseIterator into
// total-order. if PrepareMapIterator and BaseIterator has the same key, they
// are both returned by `ShadowValue`, it is impossible that the same key
// comes from me(or WriteBatchWithIndexIterator) because the only operations
// after `prepare` is rollback or commit

class PrepareMapIterator {
 public:
  PrepareMapIterator(ColumnFamilyHandle* cf, PrepareHeap* ph,
                     TOTransactionImpl::ActiveTxnNode* core)
      : cf_(cf), ph_(ph), core_(core), valid_(false) {}

  bool Valid() const { return valid_; }

  void SeekToFirst();

  void SeekToLast();

  void Seek(const Slice& target);

  void SeekForPrev(const Slice& target);

  void Next();

  void Prev();

  Slice key() const;

  const std::shared_ptr<TOTransactionImpl::ActiveTxnNode>& value() const;

  TOTransaction::TOTransactionState valueState() const;

  TOTransaction::TOTransactionState UpdatePrepareState();

  PrepareHeap* getPrepareHeapMap() const;

 private:
  void TryPosValueToCorrectMvccVersionInLock(
      const std::list<std::shared_ptr<TOTransactionImpl::ActiveTxnNode>>&
          prepare_mvccs);

  ColumnFamilyHandle* cf_;  // not owned

  PrepareHeap* ph_;  // not owned

  TOTransactionImpl::ActiveTxnNode* core_;  // not owned

  bool forward_;

  bool valid_;

  std::string pos_;

  std::shared_ptr<TOTransactionImpl::ActiveTxnNode> val_;

  TOTransaction::TOTransactionState val_state_;
};

struct ShadowValue {
  bool has_prepare;
  bool has_base;
  TOTransactionImpl::ActiveTxnNode* prepare_value;
  TOTransaction::TOTransactionState prepare_value_state;
  Slice base_value;
  Slice base_key;
  RocksTimeStamp base_timestamp;
};

class PrepareMergingIterator {
 public:
  PrepareMergingIterator(std::unique_ptr<Iterator> base_iterator,
                         std::unique_ptr<PrepareMapIterator> pmap_iterator);

  bool Valid() const;

  Slice key() const;

  ShadowValue value() const;

  void SeekToFirst();

  void SeekToLast();

  void Seek(const Slice& k);

  void SeekForPrev(const Slice& k);

  void Next();

  void Prev();

  Status status() const;

  TOTransaction::TOTransactionState UpdatePrepareState();

 private:
  void Advance();

  void AdvanceDelta();

  void AdvanceBase();

  void AssertInvariants();

  void UpdateCurrent();

  bool BaseValid() const;

  bool DeltaValid() const;

  bool forward_;

  bool current_at_base_;

  bool equal_keys_;

  Status status_;

  std::unique_ptr<Iterator> base_iterator_;

  std::unique_ptr<PrepareMapIterator> delta_iterator_;

  const Comparator* comparator_;  // not owned
};

class PrepareFilterIterator : public Iterator {
 public:
  PrepareFilterIterator(DB* db, ColumnFamilyHandle* cf,
                        const std::shared_ptr<TOTransactionImpl::ActiveTxnNode>& core,
                        std::unique_ptr<PrepareMergingIterator> input,
                        Logger* info_log = nullptr);

  bool Valid() const final;

  Slice key() const final;

  Slice value() const final;

  void SeekToFirst() final;

  void SeekToLast() final;

  void Seek(const Slice& k) final;

  void SeekForPrev(const Slice& k) final;

  void Next() final;

  void Prev() final;

  Status status() const;

 private:
  // rocksdb internal api, for sanity check
  // WBWIIteratorImpl::Result GetFromBatch(WriteBatchWithIndex* batch,
  //                                       const Slice& key,
  //                                       std::string* val);

  void AdvanceInputNoFilter();

  void UpdateCurrent();

  void UpdatePrepareState();

  DB* db_;

  ColumnFamilyHandle* cf_;

  std::shared_ptr<TOTransactionImpl::ActiveTxnNode> core_;

  std::unique_ptr<PrepareMergingIterator> input_;

  Slice key_;

  std::string val_;

  ShadowValue sval_;

  bool valid_;

  bool forward_;

  Status status_;
};

}  //  namespace rocksdb

#endif  // ROCKSDB_LITE
