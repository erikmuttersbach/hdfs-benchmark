//
// Created by Erik Muttersbach on 20/05/15.
//

#ifndef HDFS_BENCHMARK_BLOCK_H
#define HDFS_BENCHMARK_BLOCK_H

#include <set>
#include <string>
#include <memory>

#include <stdlib.h>
#include <string.h>

#include <hdfs/hdfs.h>

using namespace std;

/**
 * A block of a HDFS-resident file
 */
class Block {
public:
    Block(hdfsFileInfo &fileInfo, uint32_t idx, set<string> hosts) :
            idx(idx), hosts(hosts), fileInfo(fileInfo) {
        this->fileInfo.mName = static_cast<char *>(malloc(strlen(fileInfo.mName)+1));
        strcpy(this->fileInfo.mName, fileInfo.mName);
    }

    ~Block() {

    }

    hdfsFileInfo fileInfo;
    set<string> hosts;
    uint32_t idx;
    shared_ptr<void> data;
    size_t len;
    string host;
};


#endif //HDFS_BENCHMARK_BLOCK_H
