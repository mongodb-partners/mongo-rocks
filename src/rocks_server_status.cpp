/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include <memory>
#include <regex>
#include <sstream>

#include "mongo/platform/basic.h"

#include "rocks_server_status.h"

#include "boost/scoped_ptr.hpp"

#include <rocksdb/db.h>
#include <rocksdb/statistics.h>

#include "mongo/base/checked_cast.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/scopeguard.h"

#include "rocks_recovery_unit.h"
#include "rocks_engine.h"
#include "rocks_transaction.h"

namespace mongo {
    using std::string;

    namespace {
        std::string PrettyPrintBytes(size_t bytes) {
            if (bytes < (16 << 10)) {
                return std::to_string(bytes) + "B";
            } else if (bytes < (16 << 20)) {
                return std::to_string(bytes >> 10) + "KB";
            } else if (bytes < (16LL << 30)) {
                return std::to_string(bytes >> 20) + "MB";
            } else {
                return std::to_string(bytes >> 30) + "GB";
            }
        }
    }  // namespace

    namespace {
        // parse string representation into more convenient structure
        class RocksStatsParser {
        public:
            RocksStatsParser()
              : _bob(nullptr),
                _current_parser_fn(&RocksStatsParser::parseCompactionStatsHeader) {
            }

            ~RocksStatsParser() {
            }

            void parse(const std::string& line) {
                (this->*_current_parser_fn)(line);
            }

            void done(BSONObjBuilder& bob) {
                bob.append("compaction-stats", _compstats.obj());
                bob.append("db-stats", _dbstats.obj());
            }

        private:
            // "",
            // "** Compaction Stats [default] **",
            // "Level    Files   Size(MB) Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop",
            void parseCompactionStatsHeader(const std::string& line) {
                if (line ==
                    "Level    Files   Size(MB) Score Read(GB)  Rn(GB) Rnp1(GB) "
                    "Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) "
                    "Comp(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop") {
                    _bob = &_compstats;
                    _current_parser_fn = &RocksStatsParser::parseCompactionStatsSeparator;
                }
            }

            // "---------------------------------------------------------------------------------------------------------------------------------------------------------------------",
            void parseCompactionStatsSeparator(const std::string& line) {
                if (line ==
                    "--------------------------------------------------------------------"
                    "-----------------------------------------------------------"
                    "--------------------------------------") {
                    _current_parser_fn = &RocksStatsParser::parseCompactionStatsLX;
                }
            }

            // "  L0      4/4       0.01   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.4         0         1    0.007       0      0",
            // "  L7      7/4     423.33   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0         0         0    0.000       0      0",
            // " Sum     11/8     423.34   0.0      0.0     0.0      0.0       0.0      0.0       0.0   1.0      0.0      0.4         0         1    0.007       0      0",
            // " Int      0/0       0.00   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0         0         0    0.000       0      0",
            void parseCompactionStatsLX(const std::string& line) {
                if (!_current_subobj) {
                    _current_subobj.reset(new BSONObjBuilder(_bob->subobjStart("level-stats")));
                }
                static std::regex rex(
                    "\\s*(\\w+)\\s+(\\d+)/(\\d+)\\s+(\\d+\\.\\d+)\\s+(\\d+\\.\\d+)" // "  L7      7/4     423.33   0.0"
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
                lob.append("total-file-size-MB", std::stod(mr[++i].str()));
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
            void parseCompactionStatsUptime(const std::string& line) {
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
            void parseCompactionStatsFlush(const std::string& line) {
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
            void parseCompactionStatsAddFile(const std::string& line) {
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
            void parseCompactionStatsAddFileCnt(const std::string& line) {
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
            void parseCompactionStatsAddFileL0Cnt(const std::string& line) {
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
            void parseCompactionStatsAddFileKeyCnt(const std::string& line) {
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
            void parseCompactionStatsCumulative(const std::string& line) {
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
            void parseCompactionStatsInterval(const std::string& line) {
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
            void parseCompactionStatsStalls(const std::string& line) {
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
            void parseDBStatsHeader(const std::string& line) {
                if (line == "** DB Stats **") {
                    _bob = &_dbstats;
                    _current_parser_fn = &RocksStatsParser::parseDBStatsUptime;
                }
            }

            // "Uptime(secs): 2.3 total, 0.3 interval",
            void parseDBStatsUptime(const std::string& line) {
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
            void parseDBStatsCumulativeWrites(const std::string& line) {
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
            void parseDBStatsCumulativeWAL(const std::string& line) {
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
            void parseDBStatsCumulativeStall(const std::string& line) {
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
            void parseDBStatsIntervalWrites(const std::string& line) {
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
            void parseDBStatsIntervalWAL(const std::string& line) {
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
            void parseDBStatsIntervalStall(const std::string& line) {
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

            void error(const std::string& line) {
                if (_current_subobj) {
                    _current_subobj->doneFast();
                    _current_subobj.reset();
                }
                _bob->append("error", line);
                _current_parser_fn = &RocksStatsParser::noop;
            }

            void noop(const std::string&) {}

            // compaction stats object builder
            BSONObjBuilder _compstats;
            // DB stats object builder
            BSONObjBuilder _dbstats;
            // convenience pointer to current object builder
            BSONObjBuilder* _bob;
            // current subobject builder
            std::unique_ptr<BSONObjBuilder> _current_subobj;
            // pointer to current parsing member function
            void (RocksStatsParser::*_current_parser_fn)(const std::string& line);
        };
    }  // namespace

    RocksServerStatusSection::RocksServerStatusSection(RocksEngine* engine)
        : ServerStatusSection("rocksdb"), _engine(engine) {}

    bool RocksServerStatusSection::includeByDefault() const { return true; }

    BSONObj RocksServerStatusSection::generateSection(OperationContext* txn,
                                                      const BSONElement& configElement) const {

        BSONObjBuilder bob;

        // if the second is true, that means that we pass the value through PrettyPrintBytes
        std::vector<std::pair<std::string, bool>> properties = {
            {"stats", false},
            {"num-immutable-mem-table", false},
            {"mem-table-flush-pending", false},
            {"compaction-pending", false},
            {"background-errors", false},
            {"cur-size-active-mem-table", true},
            {"cur-size-all-mem-tables", true},
            {"num-entries-active-mem-table", false},
            {"num-entries-imm-mem-tables", false},
            {"estimate-table-readers-mem", true},
            {"num-snapshots", false},
            {"oldest-snapshot-time", false},
            {"num-live-versions", false}};
        for (auto const& property : properties) {
            std::string statsString;
            if (!_engine->getDB()->GetProperty("rocksdb." + property.first, &statsString)) {
                statsString = "<error> unable to retrieve statistics";
                bob.append(property.first, statsString);
                continue;
            }
            if (property.first == "stats") {
                // special casing because we want to turn it into array
                BSONArrayBuilder a;
                RocksStatsParser rsp;
                std::stringstream ss(statsString);
                std::string line;
                while (std::getline(ss, line)) {
                    a.append(line);
                    rsp.parse(line);
                }
                bob.appendArray(property.first, a.arr());
                rsp.done(bob);
            } else if (property.second) {
                bob.append(property.first, PrettyPrintBytes(std::stoll(statsString)));
            } else {
                bob.append(property.first, statsString);
            }
        }
        bob.append("total-live-recovery-units", RocksRecoveryUnit::getTotalLiveRecoveryUnits());
        bob.append("block-cache-usage", PrettyPrintBytes(_engine->getBlockCacheUsage()));
        bob.append("transaction-engine-keys",
                   static_cast<long long>(_engine->getTransactionEngine()->numKeysTracked()));
        bob.append("transaction-engine-snapshots",
                   static_cast<long long>(_engine->getTransactionEngine()->numActiveSnapshots()));

        std::vector<rocksdb::ThreadStatus> threadList;
        auto s = rocksdb::Env::Default()->GetThreadList(&threadList);
        if (s.ok()) {
            BSONArrayBuilder threadStatus;
            for (auto& ts : threadList) {
                if (ts.operation_type == rocksdb::ThreadStatus::OP_UNKNOWN ||
                    ts.thread_type == rocksdb::ThreadStatus::USER) {
                    continue;
                }
                BSONObjBuilder threadObjBuilder;
                threadObjBuilder.append("type",
                                        rocksdb::ThreadStatus::GetOperationName(ts.operation_type));
                threadObjBuilder.append(
                    "time_elapsed", rocksdb::ThreadStatus::MicrosToString(ts.op_elapsed_micros));
                auto op_properties = rocksdb::ThreadStatus::InterpretOperationProperties(
                    ts.operation_type, ts.op_properties);

                const std::vector<std::pair<std::string, std::string>> properties(
                    {{"JobID", "job_id"},
                     {"BaseInputLevel", "input_level"},
                     {"OutputLevel", "output_level"},
                     {"IsManual", "manual"}});
                for (const auto& prop : properties) {
                    auto itr = op_properties.find(prop.first);
                    if (itr != op_properties.end()) {
                        threadObjBuilder.append(prop.second, static_cast<int>(itr->second));
                    }
                }

                const std::vector<std::pair<std::string, std::string>> byte_properties(
                    {{"BytesRead", "bytes_read"}, {"BytesWritten", "bytes_written"},
                     {"TotalInputBytes", "total_bytes"}});
                for (const auto& prop : byte_properties) {
                    auto itr = op_properties.find(prop.first);
                    if (itr != op_properties.end()) {
                        threadObjBuilder.append(prop.second,
                                                PrettyPrintBytes(static_cast<size_t>(itr->second)));
                    }
                }

                const std::vector<std::pair<std::string, std::string>> speed_properties(
                    {{"BytesRead", "read_throughput"}, {"BytesWritten", "write_throughput"}});
                for (const auto& prop : speed_properties) {
                    auto itr = op_properties.find(prop.first);
                    if (itr != op_properties.end()) {
                        size_t speed =
                            (itr->second * 1000 * 1000) / static_cast<size_t>(ts.op_elapsed_micros + 1);
                        threadObjBuilder.append(
                            prop.second, PrettyPrintBytes(static_cast<size_t>(speed)) + "/s");
                    }
                }

                threadStatus.append(threadObjBuilder.obj());
            }
            bob.appendArray("thread-status", threadStatus.arr());
        }

        // add counters
        auto stats = _engine->getStatistics();
        if (stats) {
            BSONObjBuilder countersObjBuilder;
            const std::map<rocksdb::Tickers, std::string> counterNameMap = {
                {rocksdb::NUMBER_KEYS_WRITTEN, "num-keys-written"},
                {rocksdb::NUMBER_KEYS_READ, "num-keys-read"},
                {rocksdb::NUMBER_DB_SEEK, "num-seeks"},
                {rocksdb::NUMBER_DB_NEXT, "num-forward-iterations"},
                {rocksdb::NUMBER_DB_PREV, "num-backward-iterations"},
                {rocksdb::BLOCK_CACHE_MISS, "block-cache-misses"},
                {rocksdb::BLOCK_CACHE_HIT, "block-cache-hits"},
                {rocksdb::BLOOM_FILTER_USEFUL, "bloom-filter-useful"},
                {rocksdb::BYTES_WRITTEN, "bytes-written"},
                {rocksdb::BYTES_READ, "bytes-read-point-lookup"},
                {rocksdb::ITER_BYTES_READ, "bytes-read-iteration"},
                {rocksdb::FLUSH_WRITE_BYTES, "flush-bytes-written"},
                {rocksdb::COMPACT_READ_BYTES, "compaction-bytes-read"},
                {rocksdb::COMPACT_WRITE_BYTES, "compaction-bytes-written"}
            };

            for (const auto& ticker: rocksdb::TickersNameMap) {
                invariant(ticker.second.compare(0, 8, "rocksdb.") == 0);
                std::string name(ticker.second, 8);
                auto alt = counterNameMap.find(ticker.first);
                if (alt != counterNameMap.end()) {
                    name = alt->second;
                }
                else {
                    std::replace(name.begin(), name.end(), '.', '-');
                }
                countersObjBuilder.append(name,
                    static_cast<long long>(stats->getTickerCount(ticker.first)));
            }

            bob.append("counters", countersObjBuilder.obj());
        }

        RocksEngine::appendGlobalStats(bob);

        return bob.obj();
    }

}  // namespace mongo

