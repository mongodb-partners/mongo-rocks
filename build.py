
def configure(conf, env):
    print("Configuring rocks storage engine module")
    if not conf.CheckCXXHeader("rocksdb/db.h"):
        print("Could not find <rocksdb/db.h>, required for RocksDB storage engine build.")
        env.Exit(1)
