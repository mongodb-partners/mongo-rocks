MongoRocks r4.2.5
1) RocksDB layer bottommost compaction may be triggered frequently with no progress when enableMajorityReadConcern=true, TODO: add a issue somewhere
2) jstests/core/txns/commit_prepared_transaction_errors.js wont pass now because mongo-wt introduced the timestamped-safe unique index, which does dupkey check in wt-layer. mongoRocks does this in mongoRocks layer, which hangs into PrepareConflict error, while mongo-wt throws WriteConflict
3) src/mongo/db/storage/sorted_data_interface_test_dupkeycheck.cpp:TEST(SortedDataInterface, DupKeyCheckWithDuplicates) wont pass, because mongoRocks currently do not have timestamped-safe unique index

MongoRocks r4.0.3
1) RocksDB layer bottommost compaction may be triggered frequently with no progress when enableMajorityReadConcern=true, TODO: add a issue somewhere
