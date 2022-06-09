Execute this series of commands to compile MongoDB with RocksDB storage engine:
```
install compression libraries (zlib, bzip2, snappy):
sudo apt-get install zlib1g-dev; sudo apt-get install libbz2-dev; sudo apt-get install libsnappy-dev
# get rocksdb
git clone https://github.com/facebook/rocksdb.git
git checkout main
# compile rocksdb
cd rocksdb; USE_RTTI=1 CFLAGS=-fPIC make static_lib; sudo INSTALL_PATH=/usr make install; cd ..
# get mongo
git clone https://github.com/mongodb/mongo.git
git checkout tags/r4.2.5 -b branch_tags_4.2.5
# get mongorocks
git clone https://github.com/mongodb-partners/mongo-rocks
git checkout master
# add rocksdb module to mongo
mkdir -p mongo/src/mongo/db/modules/
ln -sf ~/mongo-rocks mongo/src/mongo/db/modules/rocks
# compile mongo
cd mongo; scons
```
Start `mongod` using the `--storageEngine=rocksdb` option.


