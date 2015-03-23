## RocksDB Storage Engine Module for MongoDB

To use this module, in the `mongo` repository directory do the following:

    mkdir -p src/mongo/db/modules/
    ln -sf ~/mongo-rocks src/mongo/db/modules/rocks

To build you will need to first install the RocksDB library, see `INSTALL.md`
at https://github.com/facebook/rocksdb for more information. If you install
in non-standard locations, you may need to add `--cpppath` and `--libpath`
options to the `scons` command line:

    scons --cpppath=/myrocksdb/include --libpath=/myrocksdb/lib

Start `mongod` using the `--storageEngine=rocksdb` option.

