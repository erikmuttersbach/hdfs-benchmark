//
// Created by Erik Muttersbach on 20/05/15.
//

#ifndef HDFS_BENCHMARK_BLOCK_H
#define HDFS_BENCHMARK_BLOCK_H

#include <set>
#include <string>

#include <hdfs/hdfs.h>

using namespace std;

/**
 * A block of a HDFS-resident file
 */
class Block {
public:
    Block(string path, uint32_t idx, string host, void *data, size_t len) :
            idx(idx), host(host), data(data), len(len), path(path) {
    }

    Block(string path, uint32_t idx, set<string> hosts) :
            idx(idx), hosts(hosts), path(path) {
    }

    ~Block() {

    }

    string path;
    set<string> hosts;
    uint32_t idx;
    void *data;
    size_t len;
    string host;
};


#endif //HDFS_BENCHMARK_BLOCK_H
