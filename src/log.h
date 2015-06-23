//
// Created by Erik Muttersbach on 22/06/15.
//

#ifndef HDFS_BENCHMARK_LOG_H
#define HDFS_BENCHMARK_LOG_H

#include <cstring>

#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/trivial.hpp>

namespace logging = boost::log;

void initLogging() {
    logging::trivial::severity_level level = logging::trivial::info;
    const char *levelStr = getenv("LOG_LEVEL");
    if(levelStr != 0) {
        if(strcmp(levelStr, "DEBUG") == 0) {
            level = logging::trivial::debug;
        } else if(strcmp(levelStr, "WARNING") == 0) {
            level = logging::trivial::warning;
        }
    }

    logging::core::get()->set_filter(logging::trivial::severity >= level);
}

#endif //HDFS_BENCHMARK_LOG_H
