## RocksDB Storage Engine Module for MongoDB

### TL;DR

Execute this series of commands to compile MongoDB with RocksDB storage engine:
    
    # install compression libraries (zlib, bzip2, snappy):
    sudo apt-get install zlib1g-dev; sudo apt-get install libbz2-dev; sudo apt-get install libsnappy-dev
    # get rocksdb
    git clone https://github.com/facebook/rocksdb.git
    # compile rocksdb
    cd rocksdb; make static_lib; sudo INSTALL_PATH=/usr make install; cd ..
    # get mongo
    git clone https://github.com/mongodb/mongo.git
    # get mongorocks
    git clone https://github.com/mongodb-partners/mongo-rocks
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
in non-standard locations, you may need to add `--cpppath` and `--libpath`
options to the `scons` command line:

    scons --cpppath=/myrocksdb/include --libpath=/myrocksdb/lib
    
### Reach out

If you have any issues with MongoRocks, feel free to reach out to our Google Group https://groups.google.com/forum/#!forum/mongo-rocks
