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

#pragma once

#include <memory>
#include <string>

#include "mongo/bson/bsonobjbuilder.h"

namespace mongo {
    // parse string representation into more convenient structure
    class RocksStatsParser {
    public:
        RocksStatsParser();
        ~RocksStatsParser();

        void parse(const std::string& line);
        void done(BSONObjBuilder& bob);

    private:
    	// parsing functions
        void parseCompactionStatsHeader(const std::string& line);
        void parseCompactionStatsSeparator(const std::string& line);
        void parseCompactionStatsLX(const std::string& line);
        void parseCompactionStatsUptime(const std::string& line);
        void parseCompactionStatsFlush(const std::string& line);
        void parseCompactionStatsAddFile(const std::string& line);
        void parseCompactionStatsAddFileCnt(const std::string& line);
        void parseCompactionStatsAddFileL0Cnt(const std::string& line);
        void parseCompactionStatsAddFileKeyCnt(const std::string& line);
        void parseCompactionStatsCumulative(const std::string& line);
        void parseCompactionStatsInterval(const std::string& line);
        void parseCompactionStatsStalls(const std::string& line);
        void parseDBStatsHeader(const std::string& line);
        void parseDBStatsUptime(const std::string& line);
        void parseDBStatsCumulativeWrites(const std::string& line);
        void parseDBStatsCumulativeWAL(const std::string& line);
        void parseDBStatsCumulativeStall(const std::string& line);
        void parseDBStatsIntervalWrites(const std::string& line);
        void parseDBStatsIntervalWAL(const std::string& line);
        void parseDBStatsIntervalStall(const std::string& line);
        void error(const std::string& line);
        void noop(const std::string&);

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

} // namespace mongo
