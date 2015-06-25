//
// Created by Erik Muttersbach on 20/05/15.
//

#ifndef HDFS_BENCHMARK_COMPARE_H
#define HDFS_BENCHMARK_COMPARE_H

#include "Block.h"

class Compare {
public:
    bool operator() (Block block1, Block block2);
};


#endif //HDFS_BENCHMARK_COMPARE_H
