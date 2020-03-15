#!/bin/bash

ulimit -n 100000

for testcase in `cat resmokelist`
do
    echo "run suite: " $testcase
    python buildscripts/resmoke.py --storageEngine rocksdb --suite=$testcase --dbpathPrefix=/root/mongo/ci -j1 1>$testcase.log 2>&1
done
