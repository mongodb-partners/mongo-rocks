#pragma once
#ifndef ROCKSDB_LITE

#include <functional>
#include <string>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"

namespace rocksdb {

class Iterator;
class TransactionDB;
class WriteBatchWithIndex;

using TransactionName = std::string;

using TransactionID = uint64_t;

//TimeStamp in rocksdb
using RocksTimeStamp = uint64_t;

//TimeStamp Ordering Transaction
class TOTransaction {
 public:
  virtual ~TOTransaction() {}

  // set prepare timestamp for transaction, if the application set the prepare
  // timestamp twice, an error will be returned
  virtual Status SetPrepareTimeStamp(const RocksTimeStamp& timestamp) = 0;

  virtual Status SetCommitTimeStamp(const RocksTimeStamp& timestamp) = 0;

  virtual Status SetDurableTimeStamp(const RocksTimeStamp& timestamp) = 0;

  // set read timestamp for transaction, if the application set the commit timestamp twice, an error will be returned
  virtual Status SetReadTimeStamp(const RocksTimeStamp& timestamp) = 0;

  virtual Status GetReadTimeStamp(RocksTimeStamp* timestamp) const = 0;

  virtual Status Prepare() = 0;

  virtual Status Commit(std::function<void()>* hook = nullptr) = 0;

  virtual Status Rollback() = 0;

  virtual Status Get(ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     std::string* value) = 0;

  virtual Status Get(ReadOptions& options, const Slice& key,
                     std::string* value) = 0;

  virtual Iterator* GetIterator(ReadOptions& read_options) = 0;

  virtual Iterator* GetIterator(ReadOptions& read_options,
                                ColumnFamilyHandle* column_family) = 0;

  virtual Status Put(ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value) = 0;
  virtual Status Put(const Slice& key, const Slice& value) = 0;

  virtual Status Delete(ColumnFamilyHandle* column_family, const Slice& key) = 0;
  virtual Status Delete(const Slice& key) = 0;

  virtual WriteBatchWithIndex* GetWriteBatch() = 0;

  virtual Status SetName(const TransactionName& name) = 0;

  virtual TransactionName GetName() const { return name_; }

  virtual TransactionID GetID() const { return 0; }

  enum TOTransactionState {
      kStarted = 0,
      kPrepared = 1,
      kCommitted = 2,
      kRollback = 3,
  };

  virtual TOTransactionState GetState() const = 0;

 protected:
  explicit TOTransaction(const DB* /*db*/) {}
  TOTransaction() {}

  TransactionName name_;
};

}  // namespace rocksdb

#endif

