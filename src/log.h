//
// Created by Erik Muttersbach on 22/06/15.
//

#ifndef HDFS_BENCHMARK_LOG_H
#define HDFS_BENCHMARK_LOG_H

#include <boost/log/trivial.hpp>

void initLogging() {
    /*boost::log::core::get()->set_filter
            (
                    boost::log::trivial::severity >= boost::log::trivial::debug
            );*/
}

#endif //HDFS_BENCHMARK_LOG_H
