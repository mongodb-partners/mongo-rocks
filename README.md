![CI](https://github.com/mongodb-partners/mongo-rocks/workflows/CI/badge.svg?branch=master)
## RocksDB Storage Engine Module for MongoDB

**Important:** Last stable version of MongoRocks is 3.4 and 4.0(.3). MongoRocks 3.6 should compile, but are not yet ready to be used in production.

### TL;DR

#### MongoRocks3.2 and MongoRocks3.4
Execute this series of commands to compile MongoDB with RocksDB storage engine:

    # install compression libraries (zlib, bzip2, snappy):
    sudo apt-get install zlib1g-dev; sudo apt-get install libbz2-dev; sudo apt-get install libsnappy-dev
    # get rocksdb
    git clone https://github.com/facebook/rocksdb.git
    # compile rocksdb
    cd rocksdb; USE_RTTI=1 CFLAGS=-fPIC make static_lib; sudo INSTALL_PATH=/usr make install; cd ..
    # get mongo
    git clone https://github.com/mongodb/mongo.git
    # get mongorocks
    git clone https://github.com/mongodb-partners/mongo-rocks
    # add rocksdb module to mongo
    mkdir -p mongo/src/mongo/db/modules/
    ln -sf ~/mongo-rocks mongo/src/mongo/db/modules/rocks
    # compile mongo
    cd mongo; scons

#### MongoRocks4.0(.3)
To support HLC(hyper-logical-clock) and timestamp ordering transactions, RocksDB should provide necessary transaction APIs.
which can be found here: https://github.com/wolfkdy/rocksdb/tree/fb5.18_totdb. This branch is based on official RocksDB's
branch 5.18.3, there is a merge request for timestamp transactions into official RocksDB's repo, which can be found here:
https://github.com/facebook/rocksdb/pull/6407.

Execute this series of commands to compile MongoDB with RocksDB storage engine:

    # install compression libraries (zlib, bzip2, snappy):
    sudo apt-get install zlib1g-dev; sudo apt-get install libbz2-dev; sudo apt-get install libsnappy-dev
    # get rocksdb
    git clone https://github.com/wolfkdy/rocksdb.git
    git checkout fb5.18_totdb
    # compile rocksdb
    cd rocksdb; USE_RTTI=1 CFLAGS=-fPIC make static_lib; sudo INSTALL_PATH=/usr make install; cd ..
    # get mongo
    git clone https://github.com/mongodb/mongo.git
    git checkout tags/r4.0.3 -b branch_tags_4.0.3
    # get mongorocks
    git clone https://github.com/mongodb-partners/mongo-rocks
    git checkout v4.0.3
    # add rocksdb module to mongo
    mkdir -p mongo/src/mongo/db/modules/
    ln -sf ~/mongo-rocks mongo/src/mongo/db/modules/rocks
    # compile mongo
    cd mongo; scons



Start `mongod` using the `--storageEngine=rocksdb` option.

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

If you have any issues with MongoRocks, feel free to reach out to our Google Group https://groups.google.com/forum/#!forum/mongo-rocks
