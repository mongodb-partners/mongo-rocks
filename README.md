## RocksDB Storage Engine Module for MongoDB

### Stable Versions/Branches
+ v3.2
+ v3.4
+ v4.0.3
+ v4.2.5

### How to build
See BUILD.md

### More information
To use this module, it has to be linked from `mongo/src/mongo/db/modules`. The build system will automatically recognize it. In the `mongo` repository directory do the following:

    mkdir -p src/mongo/db/modules/
    ln -sf ~/mongo-rocks src/mongo/db/modules/rocks

To build you will need to first install the RocksDB library, see `INSTALL.md`
at https://github.com/facebook/rocksdb for more information. If you install
in non-standard locations, you may need to set `CPPPATH` and `LIBPATH`
environment variables:

    CPPPATH=/myrocksdb/include LIBPATH=/myrocksdb/lib scons

### Reach out
If you have any issues with MongoRocks, leave an issue on github's issue board.
