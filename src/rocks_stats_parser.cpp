/*======
This file is part of Percona Server for MongoDB.

Copyright (c) 2006, 2017, Percona and/or its affiliates. All rights reserved.

    Percona Server for MongoDB is free software: you can redistribute
    it and/or modify it under the terms of the GNU Affero General
    Public License, version 3, as published by the Free Software
    Foundation.

    Percona Server for MongoDB is distributed in the hope that it will
    be useful, but WITHOUT ANY WARRANTY; without even the implied
    warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
    See the GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public
    License along with Percona Server for MongoDB.  If not, see
    <http://www.gnu.org/licenses/>.
======= */

#include <regex>

#include <rocksdb/version.h>

#include "rocks_stats_parser.h"

#define ROCKSDB_VERSION_NUM (((ROCKSDB_MAJOR * 100) + ROCKSDB_MINOR) * 100 + ROCKSDB_PATCH)

#if ROCKSDB_VERSION_NUM < 41305
#error "rocksdb versions before 4.13.5 are not supported"
#endif

namespace mongo {
    namespace {
        double toMegaBytes(double x, const std::string& suffix) {
            if (suffix == "MB")
                return x;
            if (suffix == "GB")
                return x*1024;
            if (suffix == "TB")
                return x*1024*1024;
            if (suffix == "KB")
                return x/1024;
            dassert(suffix == "B");
            return x/1024/1024;
        }

    }  // namespace


    RocksStatsParser::RocksStatsParser()
      : _bob(nullptr),
        _current_parser_fn(&RocksStatsParser::parseCompactionStatsHeader) {
    }

    RocksStatsParser::~RocksStatsParser() {
    }

    void RocksStatsParser::parse(const std::string& line) {
        (this->*_current_parser_fn)(line);
    }

    void RocksStatsParser::done(BSONObjBuilder& bob) {
        bob.append("compaction-stats", _compstats.obj());
        bob.append("db-stats", _dbstats.obj());
    }

    // "",
    // "** Compaction Stats [default] **",
    // Before 5.0.1:
    // "Level    Files   Size(MB) Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop"
    // Since 5.0.1 (commit 361010d447) (has typo with curly brackets)
    // "Level    Files   Size(MB} Score Read(GB}  Rn(GB} Rnp1(GB} Write(GB} Wnew(GB} Moved(GB} W-Amp Rd(MB/s} Wr(MB/s} Comp(sec} Comp(cnt} Avg(sec} KeyIn KeyDrop"
    // Since 5.4.5 (commit c50e3750dc):
    // "Level    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop"
    void RocksStatsParser::parseCompactionStatsHeader(const std::string& line) {
        if (line ==
#if ROCKSDB_VERSION_NUM < 50001
            "Level    Files   Size(MB) Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop"
#elif ROCKSDB_VERSION_NUM < 50405
            "Level    Files   Size(MB} Score Read(GB}  Rn(GB} Rnp1(GB} Write(GB} Wnew(GB} Moved(GB} W-Amp Rd(MB/s} Wr(MB/s} Comp(sec} Comp(cnt} Avg(sec} KeyIn KeyDrop"
#else
            "Level    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop"
#endif
            ) {
            _bob = &_compstats;
            _current_parser_fn = &RocksStatsParser::parseCompactionStatsSeparator;
        }
    }

    // Before 5.0.1:
    // "---------------------------------------------------------------------------------------------------------------------------------------------------------------------",
    // Since 5.0.1 (commit 361010d447):
    // "----------------------------------------------------------------------------------------------------------------------------------------------------------",
    void RocksStatsParser::parseCompactionStatsSeparator(const std::string& line) {
        if (line ==
            "--------------------------------------------------------------------"
            "-----------------------------------------------------------"
#if ROCKSDB_VERSION_NUM < 50001
            "--------------------------------------"
#else
            "---------------------------"
#endif
            ) {
            _current_parser_fn = &RocksStatsParser::parseCompactionStatsLX;
        }
    }

    // Before 5.4.5:
    // "  L0      4/4       0.01   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.4         0         1    0.007       0      0",
    // "  L7      7/4     423.33   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0         0         0    0.000       0      0",
    // " Sum     11/8     423.34   0.0      0.0     0.0      0.0       0.0      0.0       0.0   1.0      0.0      0.4         0         1    0.007       0      0",
    // " Int      0/0       0.00   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0         0         0    0.000       0      0",
    // Since 5.4.5 (commit c50e3750dc):
    // "  L0      4/4    0.01 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.4         0         1    0.007       0      0",
    // "  L7      7/4  423.33 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0         0         0    0.000       0      0",
    // " Sum     11/8  423.34 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   1.0      0.0      0.4         0         1    0.007       0      0",
    // " Int      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0         0         0    0.000       0      0",
    void RocksStatsParser::parseCompactionStatsLX(const std::string& line) {
        if (!_current_subobj) {
            _current_subobj.reset(new BSONObjBuilder(_bob->subobjStart("level-stats")));
        }
        static std::regex rex(
#if ROCKSDB_VERSION_NUM < 50405
            "\\s*(\\w+)\\s+(\\d+)/(\\d+)\\s+(\\d+\\.\\d+)\\s+(\\d+\\.\\d+)" // "  L7      7/4     423.33   0.0"
#else
            "\\s*(\\w+)\\s+(\\d+)/(\\d+)\\s+(\\d+\\.\\d+)\\s+(\\w+)\\s+(\\d+\\.\\d+)" // "  L7      7/4  423.33 KB   0.0"
#endif
            "\\s+(\\d+\\.\\d+)\\s+(\\d+\\.\\d+)\\s+(\\d+\\.\\d+)\\s+(\\d+\\.\\d+)" // "      0.0     0.0      0.0       0.0"
            "\\s+(\\d+\\.\\d+)\\s+(\\d+\\.\\d+)\\s+(\\d+\\.\\d+)\\s+(\\d+\\.\\d+)" // "      0.0       0.0   0.0      0.0"
            "\\s+(\\d+\\.\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+\\.\\d+)" // "      0.0         0         0    0.000"
            "\\s+(\\d+[KMG]?)\\s+(\\d+[KMG]?)" // "       0      0"
            );
        std::smatch mr;
        if (!std::regex_match(line, mr, rex)) {
            error(line);
            return;
        }
        // regex matched, parse individual values
        int i = 0;
        BSONObjBuilder lob(_current_subobj->subobjStart(mr[++i].str()));
        lob.append("num-files", std::stoi(mr[++i].str()));
        lob.append("being-compacted", std::stoi(mr[++i].str()));
#if ROCKSDB_VERSION_NUM < 50405
        lob.append("total-file-size-MB", std::stod(mr[++i].str()));
#else
        auto total_file_size = std::stod(mr[++i].str());
        lob.append("total-file-size-MB", toMegaBytes(total_file_size, mr[++i].str()));
#endif
        lob.append("score", std::stod(mr[++i].str()));
        lob.append("bytes-read-GB", std::stod(mr[++i].str()));
        lob.append("bytes-read-non-output-levels-GB", std::stod(mr[++i].str()));
        lob.append("bytes-read-output-levels-GB", std::stod(mr[++i].str()));
        lob.append("bytes-written-GB", std::stod(mr[++i].str()));
        lob.append("bytes-new-GB", std::stod(mr[++i].str()));
        lob.append("bytes-moved-GB", std::stod(mr[++i].str()));
        lob.append("w-amp", std::stod(mr[++i].str()));
        lob.append("bytes-read-MB-s", std::stod(mr[++i].str()));
        lob.append("bytes-written-MB-s", std::stod(mr[++i].str()));
        lob.append("compactions-sec", std::stod(mr[++i].str()));
        lob.append("compactions-cnt", std::stoi(mr[++i].str()));
        lob.append("compaction-avg-len-sec", std::stod(mr[++i].str()));
        lob.append("num-input-records", mr[++i].str());
        lob.append("num-dropped-records", mr[++i].str());
        lob.doneFast();

        if (mr[1].str() == "Int") {
            _current_subobj->doneFast();
            _current_subobj.reset();
            _current_parser_fn = &RocksStatsParser::parseCompactionStatsUptime;
        }
    }

    // "Uptime(secs): 8.8 total, 8.8 interval",
    void RocksStatsParser::parseCompactionStatsUptime(const std::string& line) {
        static std::regex rex("Uptime\\(secs\\): (\\d+\\.\\d+) total, (\\d+\\.\\d+) interval");
        std::smatch mr;
        if (!std::regex_match(line, mr, rex)) {
            error(line);
            return;
        }
        int i = 0;
        _bob->append("uptime-total-sec", std::stod(mr[++i].str()));
        _bob->append("uptime-interval-sec", std::stod(mr[++i].str()));
        _current_parser_fn = &RocksStatsParser::parseCompactionStatsFlush;
    }

    // "Flush(GB): cumulative 0.000, interval 0.000",
    void RocksStatsParser::parseCompactionStatsFlush(const std::string& line) {
        static std::regex rex("Flush\\(GB\\): cumulative (\\d+\\.\\d+), interval (\\d+\\.\\d+)");
        std::smatch mr;
        if (!std::regex_match(line, mr, rex)) {
            error(line);
            return;
        }
        int i = 0;
        _bob->append("flush-cumulative-GB", std::stod(mr[++i].str()));
        _bob->append("flush-interval-GB", std::stod(mr[++i].str()));
        _current_parser_fn = &RocksStatsParser::parseCompactionStatsAddFile;
    }

    // "AddFile(GB): cumulative 0.000, interval 0.000",
    void RocksStatsParser::parseCompactionStatsAddFile(const std::string& line) {
        static std::regex rex("AddFile\\(GB\\): cumulative (\\d+\\.\\d+), interval (\\d+\\.\\d+)");
        std::smatch mr;
        if (!std::regex_match(line, mr, rex)) {
            error(line);
            return;
        }
        int i = 0;
        _bob->append("addfile-cumulative-GB", std::stod(mr[++i].str()));
        _bob->append("addfile-interval-GB", std::stod(mr[++i].str()));
        _current_parser_fn = &RocksStatsParser::parseCompactionStatsAddFileCnt;
    }

    // "AddFile(Total Files): cumulative 0, interval 0",
    void RocksStatsParser::parseCompactionStatsAddFileCnt(const std::string& line) {
        static std::regex rex("AddFile\\(Total Files\\): cumulative (\\d+), interval (\\d+)");
        std::smatch mr;
        if (!std::regex_match(line, mr, rex)) {
            error(line);
            return;
        }
        int i = 0;
        _bob->append("addfile-cumulative-cnt", std::stoi(mr[++i].str()));
        _bob->append("addfile-interval-cnt", std::stoi(mr[++i].str()));
        _current_parser_fn = &RocksStatsParser::parseCompactionStatsAddFileL0Cnt;
    }

    // "AddFile(L0 Files): cumulative 0, interval 0",
    void RocksStatsParser::parseCompactionStatsAddFileL0Cnt(const std::string& line) {
        static std::regex rex("AddFile\\(L0 Files\\): cumulative (\\d+), interval (\\d+)");
        std::smatch mr;
        if (!std::regex_match(line, mr, rex)) {
            error(line);
            return;
        }
        int i = 0;
        _bob->append("addfile-cumulative-l0-cnt", std::stoi(mr[++i].str()));
        _bob->append("addfile-interval-l0-cnt", std::stoi(mr[++i].str()));
        _current_parser_fn = &RocksStatsParser::parseCompactionStatsAddFileKeyCnt;
    }

    // "AddFile(Keys): cumulative 0, interval 0",
    void RocksStatsParser::parseCompactionStatsAddFileKeyCnt(const std::string& line) {
        static std::regex rex("AddFile\\(Keys\\): cumulative (\\d+), interval (\\d+)");
        std::smatch mr;
        if (!std::regex_match(line, mr, rex)) {
            error(line);
            return;
        }
        int i = 0;
        _bob->append("addfile-cumulative-key-cnt", std::stoi(mr[++i].str()));
        _bob->append("addfile-interval-key-cnt", std::stoi(mr[++i].str()));
        _current_parser_fn = &RocksStatsParser::parseCompactionStatsCumulative;
    }

    // "Cumulative compaction: 0.00 GB write, 0.00 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.0 seconds",
    void RocksStatsParser::parseCompactionStatsCumulative(const std::string& line) {
        static std::regex rex("Cumulative compaction: "
                              "(\\d+\\.\\d+) GB write, (\\d+\\.\\d+) MB/s write, "
                              "(\\d+\\.\\d+) GB read, (\\d+\\.\\d+) MB/s read, (\\d+\\.\\d+) seconds");
        std::smatch mr;
        if (!std::regex_match(line, mr, rex)) {
            error(line);
            return;
        }
        int i = 0;
        _bob->append("cumulative-written-GB", std::stod(mr[++i].str()));
        _bob->append("cumulative-written-MB-s", std::stod(mr[++i].str()));
        _bob->append("cumulative-read-GB", std::stod(mr[++i].str()));
        _bob->append("cumulative-read-MB-s", std::stod(mr[++i].str()));
        _bob->append("cumulative-seconds", std::stod(mr[++i].str()));
        _current_parser_fn = &RocksStatsParser::parseCompactionStatsInterval;
    }

    // "Interval compaction: 0.00 GB write, 0.00 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.0 seconds",
    void RocksStatsParser::parseCompactionStatsInterval(const std::string& line) {
        static std::regex rex("Interval compaction: "
                              "(\\d+\\.\\d+) GB write, (\\d+\\.\\d+) MB/s write, "
                              "(\\d+\\.\\d+) GB read, (\\d+\\.\\d+) MB/s read, (\\d+\\.\\d+) seconds");
        std::smatch mr;
        if (!std::regex_match(line, mr, rex)) {
            error(line);
            return;
        }
        int i = 0;
        _bob->append("interval-written-GB", std::stod(mr[++i].str()));
        _bob->append("interval-written-MB-s", std::stod(mr[++i].str()));
        _bob->append("interval-read-GB", std::stod(mr[++i].str()));
        _bob->append("interval-read-MB-s", std::stod(mr[++i].str()));
        _bob->append("interval-seconds", std::stod(mr[++i].str()));
        _current_parser_fn = &RocksStatsParser::parseCompactionStatsStalls;
    }

    // "Stalls(count): 0 level0_slowdown, 0 level0_slowdown_with_compaction, 0 level0_numfiles, 0 level0_numfiles_with_compaction, 0 stop for pending_compaction_bytes, 0 slowdown for pending_compaction_bytes, 0 memtable_compaction, 0 memtable_slowdown, interval 0 total count",
    // "",
    void RocksStatsParser::parseCompactionStatsStalls(const std::string& line) {
        static std::regex rex("Stalls\\(count\\): "
                              "(\\d+) level0_slowdown, (\\d+) level0_slowdown_with_compaction, "
                              "(\\d+) level0_numfiles, (\\d+) level0_numfiles_with_compaction, "
                              "(\\d+) stop for pending_compaction_bytes, (\\d+) slowdown for pending_compaction_bytes, "
                              "(\\d+) memtable_compaction, (\\d+) memtable_slowdown, interval (\\d+) total count");
        std::smatch mr;
        if (!std::regex_match(line, mr, rex)) {
            error(line);
            return;
        }
        int i = 0;
        _bob->append("stalls-level0-slowdown", std::stoi(mr[++i].str()));
        _bob->append("stalls-level0-slowdown-with-compaction", std::stoi(mr[++i].str()));
        _bob->append("stalls-level0-numfiles", std::stoi(mr[++i].str()));
        _bob->append("stalls-level0-numfiles-with-compaction", std::stoi(mr[++i].str()));
        _bob->append("stalls-stop-for-pending-compaction-bytes", std::stoi(mr[++i].str()));
        _bob->append("stalls-slowdown-for-pending-compaction-bytes", std::stoi(mr[++i].str()));
        _bob->append("stalls-memtable-compaction", std::stoi(mr[++i].str()));
        _bob->append("stalls-memtable-slowdown", std::stoi(mr[++i].str()));
        _bob->append("stalls-interval-total-count", std::stoi(mr[++i].str()));
        _current_parser_fn = &RocksStatsParser::parseDBStatsHeader;
    }

    // "** DB Stats **",
    void RocksStatsParser::parseDBStatsHeader(const std::string& line) {
        if (line == "** DB Stats **") {
            _bob = &_dbstats;
            _current_parser_fn = &RocksStatsParser::parseDBStatsUptime;
        }
    }

    // "Uptime(secs): 2.3 total, 0.3 interval",
    void RocksStatsParser::parseDBStatsUptime(const std::string& line) {
        static std::regex rex("Uptime\\(secs\\): (\\d+\\.\\d+) total, (\\d+\\.\\d+) interval");
        std::smatch mr;
        if (!std::regex_match(line, mr, rex)) {
            error(line);
            return;
        }
        int i = 0;
        _bob->append("uptime-total-sec", std::stod(mr[++i].str()));
        _bob->append("uptime-interval-sec", std::stod(mr[++i].str()));
        _current_parser_fn = &RocksStatsParser::parseDBStatsCumulativeWrites;
    }

    // "Cumulative writes: 1 writes, 2 keys, 1 commit groups, 0.5 writes per commit group, ingest: 0.00 GB, 0.00 MB/s",
    void RocksStatsParser::parseDBStatsCumulativeWrites(const std::string& line) {
        static std::regex rex("Cumulative writes: "
                              "(\\d+[KMG]?) writes, (\\d+[KMG]?) keys, (\\d+[KMG]?) commit groups, "
                              "(\\d+\\.\\d+) writes per commit group, ingest: (\\d+\\.\\d+) GB, (\\d+\\.\\d+) MB/s");
        std::smatch mr;
        if (!std::regex_match(line, mr, rex)) {
            error(line);
            return;
        }
        int i = 0;
        _bob->append("cumulative-writes-cnt", mr[++i].str());
        _bob->append("cumulative-writes-keys", mr[++i].str());
        _bob->append("cumulative-writes-commit-groups", mr[++i].str());
        _bob->append("cumulative-writes-per-commit-group", std::stod(mr[++i].str()));
        _bob->append("cumulative-writes-ingest-GB", std::stod(mr[++i].str()));
        _bob->append("cumulative-writes-ingest-MB-s", std::stod(mr[++i].str()));
        _current_parser_fn = &RocksStatsParser::parseDBStatsCumulativeWAL;
    }

    // "Cumulative WAL: 1 writes, 0 syncs, 1.00 writes per sync, written: 0.00 GB, 0.00 MB/s",
    void RocksStatsParser::parseDBStatsCumulativeWAL(const std::string& line) {
        static std::regex rex("Cumulative WAL: "
                              "(\\d+[KMG]?) writes, (\\d+[KMG]?) syncs, "
                              "(\\d+\\.\\d+) writes per sync, written: (\\d+\\.\\d+) GB, (\\d+\\.\\d+) MB/s");
        std::smatch mr;
        if (!std::regex_match(line, mr, rex)) {
            error(line);
            return;
        }
        int i = 0;
        _bob->append("cumulative-WAL-writes", mr[++i].str());
        _bob->append("cumulative-WAL-syncs", mr[++i].str());
        _bob->append("cumulative-WAL-writes-per-sync", std::stod(mr[++i].str()));
        _bob->append("cumulative-WAL-written-GB", std::stod(mr[++i].str()));
        _bob->append("cumulative-WAL-written-MB-s", std::stod(mr[++i].str()));
        _current_parser_fn = &RocksStatsParser::parseDBStatsCumulativeStall;
    }

    // "Cumulative stall: 00:00:0.000 H:M:S, 0.0 percent",
    void RocksStatsParser::parseDBStatsCumulativeStall(const std::string& line) {
        static std::regex rex("Cumulative stall: (\\d+):(\\d+):(\\d+\\.\\d+) H:M:S, (\\d+\\.\\d+) percent");
        std::smatch mr;
        if (!std::regex_match(line, mr, rex)) {
            error(line);
            return;
        }
        int i = 0;
        double seconds = 3600.0 * std::stoi(mr[++i].str());
        seconds += 60.0 * std::stoi(mr[++i].str());
        seconds += std::stod(mr[++i].str());
        _bob->append("cumulative-stall-sec", seconds);
        _bob->append("cumulative-stall-percent", std::stod(mr[++i].str()));
        _current_parser_fn = &RocksStatsParser::parseDBStatsIntervalWrites;
    }

    // "Interval writes: 0 writes, 0 keys, 0 commit groups, 0.0 writes per commit group, ingest: 0.00 MB, 0.00 MB/s",
    void RocksStatsParser::parseDBStatsIntervalWrites(const std::string& line) {
        static std::regex rex("Interval writes: "
                              "(\\d+[KMG]?) writes, (\\d+[KMG]?) keys, (\\d+[KMG]?) commit groups, "
                              "(\\d+\\.\\d+) writes per commit group, ingest: (\\d+\\.\\d+) MB, (\\d+\\.\\d+) MB/s");
        std::smatch mr;
        if (!std::regex_match(line, mr, rex)) {
            error(line);
            return;
        }
        int i = 0;
        _bob->append("interval-writes-cnt", mr[++i].str());
        _bob->append("interval-writes-keys", mr[++i].str());
        _bob->append("interval-writes-commit-groups", mr[++i].str());
        _bob->append("interval-writes-per-commit-group", std::stod(mr[++i].str()));
        _bob->append("interval-writes-ingest-MB", std::stod(mr[++i].str()));
        _bob->append("interval-writes-ingest-MB-s", std::stod(mr[++i].str()));
        _current_parser_fn = &RocksStatsParser::parseDBStatsIntervalWAL;
    }

    // "Interval WAL: 0 writes, 0 syncs, 0.00 writes per sync, written: 0.00 MB, 0.00 MB/s",
    void RocksStatsParser::parseDBStatsIntervalWAL(const std::string& line) {
        static std::regex rex("Interval WAL: "
                              "(\\d+[KMG]?) writes, (\\d+[KMG]?) syncs, "
                              "(\\d+\\.\\d+) writes per sync, written: (\\d+\\.\\d+) MB, (\\d+\\.\\d+) MB/s");
        std::smatch mr;
        if (!std::regex_match(line, mr, rex)) {
            error(line);
            return;
        }
        int i = 0;
        _bob->append("interval-WAL-writes", mr[++i].str());
        _bob->append("interval-WAL-syncs", mr[++i].str());
        _bob->append("interval-WAL-writes-per-sync", std::stod(mr[++i].str()));
        _bob->append("interval-WAL-written-MB", std::stod(mr[++i].str()));
        _bob->append("interval-WAL-written-MB-s", std::stod(mr[++i].str()));
        _current_parser_fn = &RocksStatsParser::parseDBStatsIntervalStall;
    }

    // "Interval stall: 00:00:0.000 H:M:S, 0.0 percent",
    void RocksStatsParser::parseDBStatsIntervalStall(const std::string& line) {
        static std::regex rex("Interval stall: (\\d+):(\\d+):(\\d+\\.\\d+) H:M:S, (\\d+\\.\\d+) percent");
        std::smatch mr;
        if (!std::regex_match(line, mr, rex)) {
            error(line);
            return;
        }
        int i = 0;
        double seconds = 3600.0 * std::stoi(mr[++i].str());
        seconds += 60.0 * std::stoi(mr[++i].str());
        seconds += std::stod(mr[++i].str());
        _bob->append("interval-stall-sec", seconds);
        _bob->append("interval-stall-percent", std::stod(mr[++i].str()));
        // after this line there can be per level read latency histograms
        // but we decided not to parse them
        _current_parser_fn = &RocksStatsParser::noop;
    }

    void RocksStatsParser::error(const std::string& line) {
        if (_current_subobj) {
            _current_subobj->doneFast();
            _current_subobj.reset();
        }
        _bob->append("error", line);
        _current_parser_fn = &RocksStatsParser::noop;
    }

    void RocksStatsParser::noop(const std::string&) {}

} // namespace mongo
