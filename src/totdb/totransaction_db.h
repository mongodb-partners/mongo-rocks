#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include <utility>
#include <vector>
#include <iostream>

#include "mongo/db/modules/rocks/src/totdb/totransaction.h"
#include "mongo/util/str.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/stackable_db.h"
#include "third_party/s2/util/coding/coder.h"

namespace rocksdb {

//TimeStamp Ordering Transaction DB Options
#define DEFAULT_NUM_STRIPES 32

struct TOTransactionStat {
  size_t max_conflict_bytes;
  size_t cur_conflict_bytes;
  size_t uk_num;
  size_t ck_num;
  size_t alive_txns_num;
  size_t read_q_num;
  size_t commit_q_num;
  uint64_t oldest_ts;
  uint64_t min_read_ts;
  uint64_t max_commit_ts;
  uint64_t committed_max_txnid;
  uint64_t min_uncommit_ts;
  uint64_t update_max_commit_ts_times;
  uint64_t update_max_commit_ts_retries;
  uint64_t txn_commits;
  uint64_t txn_aborts;
  uint64_t commit_without_ts_times;
  uint64_t read_without_ts_times;
  uint64_t read_with_ts_times;
  uint64_t read_q_walk_len_sum;
  uint64_t read_q_walk_times;
  uint64_t commit_q_walk_len_sum;
  uint64_t commit_q_walk_times;
};

struct TOTransactionDBOptions {
  size_t num_stripes = DEFAULT_NUM_STRIPES;
  size_t max_conflict_check_bytes_size = 200 * 1024 * 1024;
  TOTransactionDBOptions(){};
  TOTransactionDBOptions(int max_conflict_check_bytes_size_mb)
      : max_conflict_check_bytes_size(max_conflict_check_bytes_size_mb * 1024 *
                                      1024) {}
};

enum TimeStampType {
  kOldest = 0,
  kStable = 1,  // kStable is not used
  kCommitted = 2,
  kAllCommitted = 3,
  kTimeStampMax,
};

//TimeStamp Ordering Transaction Options
struct TOTransactionOptions {
  size_t max_write_batch_size = 1000;
  // Whether or not to round up to the oldest timestamp when the read timestamp
  // is behind it.
  bool timestamp_round_read = false;
  // If true, The prepare timestamp will be rounded up to the oldest timestamp
  // if found to be
  // and the commit timestamp will be rounded up to the prepare timestamp if
  // found to be earlier
  // If false, Does not round up prepare and commit timestamp of a prepared
  // transaction.
  bool timestamp_round_prepared = false;

  bool read_only = false;

  bool ignore_prepare = false;
};

class TOComparator : public Comparator {
 public:
  TOComparator()
      : Comparator(sizeof(RocksTimeStamp)), cmp_without_ts_(BytewiseComparator()) {
  }

  static size_t TimestampSize() { return sizeof(RocksTimeStamp); }
  const char* Name() const override { return "TOComparator"; }

  void FindShortSuccessor(std::string*) const override {}

  void FindShortestSeparator(std::string*, const Slice&) const override {}

  int Compare(const Slice& a, const Slice& b) const override {
    int r = cmp_without_ts_->Compare(
      StripTimestampFromUserKey(a, timestamp_size()),
      StripTimestampFromUserKey(b, timestamp_size()));
    if (r != 0) {
      return r;
    }
    return -CompareTimestamp(
        Slice(a.data() + a.size() - timestamp_size(), timestamp_size()),
        Slice(b.data() + b.size() - timestamp_size(), timestamp_size()));
  }

  int CompareWithoutTimestamp(const Slice& a, bool a_has_ts, const Slice& b,
                              bool b_has_ts) const override {
    if (a_has_ts) {
      invariant(a.size() >= timestamp_size());
    }
    if (b_has_ts) {
      invariant(b.size() >= timestamp_size());
    }
    Slice lhs = a_has_ts ? StripTimestampFromUserKey(a, timestamp_size()) : a;
    Slice rhs = b_has_ts ? StripTimestampFromUserKey(b, timestamp_size()) : b;
    return cmp_without_ts_->Compare(lhs, rhs);
  }

  int CompareTimestamp(const Slice& ts1, const Slice& ts2) const override {
    invariant(ts1.data() && ts2.data());
    invariant(ts1.size() == sizeof(uint64_t));
    invariant(ts2.size() == sizeof(uint64_t));
    uint64_t ts1_data = Decoder(ts1.data(), ts1.size()).get64();
    uint64_t ts2_data = Decoder(ts2.data(), ts2.size()).get64();
    if (ts1_data < ts2_data) {
      return -1;
    } else if (ts1_data > ts2_data) {
      return 1;
    } else {
      return 0;
    }
  }

  static const Slice StripTimestampFromUserKey(const Slice& user_key, size_t ts_sz) {
    Slice ret = user_key;
    ret.remove_suffix(ts_sz);
    return ret;
  }

  private:
  const Comparator* cmp_without_ts_;
};

class TOTransactionDB : public StackableDB {
  public:
  static Status Open(const Options& options,
                     const TOTransactionDBOptions& txn_db_options,
                     const std::string& dbname, 
                     const std::string stableTsKey,
                     TOTransactionDB** dbptr);

  static Status Open(const DBOptions& db_options,
                     const TOTransactionDBOptions& txn_db_options,
                     const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& open_cfds,
                     std::vector<ColumnFamilyHandle*>* handles,
                     const std::vector<ColumnFamilyDescriptor>& trim_cfds,
                     const bool trimHistory,
                     const std::string stableTsKey,
                     TOTransactionDB** dbptr);
  virtual void SetMaxConflictBytes(uint64_t bytes) = 0;

  // The lifecycle of returned pointer should be managed by the application level
  virtual TOTransaction* BeginTransaction(
      const WriteOptions& write_options,
      const TOTransactionOptions& txn_options) = 0;

  virtual Status SetTimeStamp(const TimeStampType& ts_type,
                              const RocksTimeStamp& ts, bool force = false) = 0;

  virtual Status QueryTimeStamp(const TimeStampType& ts_type, RocksTimeStamp* timestamp) = 0;

  virtual Status Stat(TOTransactionStat* stat) = 0;
  //virtual Status Close();

  virtual std::unique_ptr<rocksdb::TOTransaction> makeTxn() = 0;

  protected:
	//std::shared_ptr<Logger> info_log_ = nullptr;	
  // To Create an ToTransactionDB, call Open()
  explicit TOTransactionDB(DB* db) : StackableDB(db) {}
};

}  // namespace rocksdb

#endif

