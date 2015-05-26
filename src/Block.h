//
// Created by Erik Muttersbach on 20/05/15.
//

#ifndef HDFS_BENCHMARK_BLOCK_H
#define HDFS_BENCHMARK_BLOCK_H

#include <string>

using namespace std;

/**
 * Represents a block that has been downloaded
 */
class Block {
public:
    Block(uint32_t idx);
    Block(uint32_t idx, string host, void *data, size_t len);

    void *data;
    size_t len;
    string host;
    uint32_t idx;
};


#endif //HDFS_BENCHMARK_BLOCK_H
