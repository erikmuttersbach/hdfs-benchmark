#include <string.h>
#include <pthread.h>

#include <iostream>
#include <string>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <set>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <cassert>
#include <chrono>

#include <hdfs/hdfs.h>

#include "PriorityQueue.h"
#include "Block.h"
#include "Compare.h"
#include "options.h"
#include "expect.h"

using namespace std;

inline void useData(void *buffer, tSize len) __attribute__((__always_inline__));
inline void useData(void *buffer, tSize len) {
    uint64_t sum  = 0;
    for (size_t i = 0; i < len/sizeof(uint64_t); i++) {
        sum += *(((uint64_t*) buffer) + i);
    }
    assert(sum);
}

std::condition_variable cv;
std::mutex cvMutex;
std::mutex blocksMutex;
unordered_map<uint32_t, set<string>> blocks;
PriorityQueue<uint32_t, vector<uint32_t>, std::greater<uint32_t>> pendingBlocks;
PriorityQueue<Block, vector<Block>, Compare> loadedBlocks;

void _dumpLoadedBlocks() {
    //unique_lock<mutex> lock(blocksMutex);
    stringstream ss;
    for(auto& block : loadedBlocks) {
        ss << block.idx << " " << endl;
    }
    cout << ss.str() << endl;
}

void reader(hdfsFileInfo *fileInfo, string host, options_t options) {
    cout << "Thread " << host << " starting" << endl;

    pthread_setname_np((string("Thread-")+host).c_str());

    // Create Connection
    // TODO bake into function
    struct hdfsBuilder *hdfsBuilder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(hdfsBuilder, options.namenode);
    hdfsBuilderSetNameNodePort(hdfsBuilder, options.namenode_port);
    if (options.type == type_t::undefined || options.type == type_t::scr || options.type == type_t::zcr) {
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit", "true");
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.domain.socket.path", options.socket);
        // TODO Test
        //hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.domain.socket.data.traffic", "true");
        //hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit.streams.cache.size", "4000");
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit.skip.checksum",
                              options.skip_checksums > 0 ? "true" : "false");
    }

    hdfsFS fs = hdfsBuilderConnect(hdfsBuilder);
    EXPECT_NONZERO(fs, "hdfsBuilderConnect")

    hdfsFile file = hdfsOpenFile2(fs, host.c_str(), options.path, O_RDONLY, options.buffer_size, 0, 0);
    EXPECT_NONZERO(file, "hdfsOpenFile")

    while(true) {
        //std::unique_lock<std::mutex> l(m);

        uint32_t downloadBlockIdx = -1;
        {
            unique_lock<mutex> lock(blocksMutex);
            uint count = 0;
            for(auto& block : loadedBlocks) {
                if(block.host.compare(host) == 0) {
                    count++;
                }
            }

            if(count < 3) {
                for(auto it = pendingBlocks.begin(); it != pendingBlocks.end(); it++) {
                    if(blocks[*it].count(host)) {
                        downloadBlockIdx = *it;
                        pendingBlocks.remove(it);
                        break;
                    }
                }
            }
        }

        if(downloadBlockIdx == -1) {
            break;
        }

        // Download the block `downloadBlockIdx`
        cout << "Thread-" << host << " downloading " << downloadBlockIdx << endl;

        auto start = chrono::high_resolution_clock::now();

        int r = hdfsSeek(fs, file, fileInfo->mBlockSize*((uint64_t)downloadBlockIdx));
        EXPECT_NONNEGATIVE(r, "hdfsSeek")

        char *buffer = (char*)malloc(fileInfo->mBlockSize);
        tSize read = 0, totalRead = 0;
        do {
            read = hdfsRead(fs, file, buffer+totalRead, options.buffer_size);
            EXPECT_NONNEGATIVE(read, "hdfsRead")

            totalRead += read;
        } while (read > 0 && totalRead < fileInfo->mBlockSize);

        auto seconds = ((double)(chrono::duration_cast<chrono::milliseconds>(chrono::high_resolution_clock::now() - start)).count())/1000.0;
        cout << "Thread-" << host << " downloaded " << downloadBlockIdx << " (" << totalRead/(1024.0*1024.0) << " MB with " << ((double)totalRead/(1024.0*1024.0))/seconds << " MB/s)"<< endl;

        {
            unique_lock<mutex> lock(blocksMutex);
            loadedBlocks.push(Block(downloadBlockIdx, host, buffer, totalRead));
            //_dumpLoadedBlocks();
            cv.notify_one();
        }
    }

    hdfsCloseFile(fs, file);
    cout << "Thread " << host << " finished" << endl;
}

int main(int argc, char **argv) {
    // Parse Options
    auto options = parse_options(argc, argv);
    if (options.verbose) {
        cout << "Namenode:  " << options.namenode << ":" << options.namenode_port << endl;
        cout << "Socket:    " << options.socket << endl;
        cout << "File:      " << options.path << endl;
        cout << "Buffer:    " << options.buffer_size << endl;
        cout << "Checksums: " << (options.skip_checksums ? "false" : "true") << endl;
        cout << "Type:      " << options.type << endl;
    }

    // Create Connection
    // TODO bake into function
    struct hdfsBuilder *hdfsBuilder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(hdfsBuilder, options.namenode);
    hdfsBuilderSetNameNodePort(hdfsBuilder, options.namenode_port);
    if (options.type == type_t::undefined || options.type == type_t::scr || options.type == type_t::zcr) {
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit", "true");
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.domain.socket.path", options.socket);
        // TODO Test
        //hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.domain.socket.data.traffic", "true");
        //hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit.streams.cache.size", "4000");
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit.skip.checksum",
                              options.skip_checksums > 0 ? "true" : "false");
    }

    hdfsFS fs = hdfsBuilderConnect(hdfsBuilder);
    EXPECT_NONZERO(fs, "hdfsBuilderConnect")

    if (hdfsExists(fs, options.path) != 0) {
        printf("File %s does not exist\n", options.path);
        hdfsFreeBuilder(hdfsBuilder);
        exit(1);
    }

	auto start = chrono::high_resolution_clock::now();

    // Retrieve File Information and build queue and threads
    hdfsFileInfo *fileInfo = hdfsGetPathInfo(fs, options.path);

    char ***blockHosts = hdfsGetHosts(fs, options.path, 0, fileInfo->mSize);
    EXPECT_NONZERO(blockHosts, "hdfsGetHosts")

    set<string> hosts;
    for (uint block = 0; blockHosts[block]; block++) {
        pendingBlocks.push(block);

        for (uint j = 0; blockHosts[block][j]; j++) {
            if (blocks.count(block) == 0) {
                blocks[block] = set<string>();
            }
            blocks[block].insert(blockHosts[block][j]);

            hosts.insert(blockHosts[block][j]);
        }
    }

    if (options.verbose) {
        cout << "All Hosts: " << endl;
        for (auto it = hosts.begin(); it != hosts.end(); it++) {
            cout << "\t" << *it << endl;
        }

        cout << "All Blocks: " << endl;
        for (auto it = blocks.begin(); it != blocks.end(); it++) {
            cout << "\tBlock " << it->first << ":" << endl;
            for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++) {
                cout << "\t\t" << *it2 << endl;
            }
        }
    }

    // 3) Start Execution
	// Start Time

    std::thread consumer([&]() {
        uint32_t lastBlock = -1;
        while(true) {
            std::unique_lock<std::mutex> lock(blocksMutex);
            if(loadedBlocks.size() == 0 || loadedBlocks.peek().idx != lastBlock+1) {
                cv.wait(lock);
            }

			//cout << "Peek: " << loadedBlocks.peek().idx << endl;
            if(loadedBlocks.peek().idx == lastBlock+1) {
                auto block = loadedBlocks.pop();
                lastBlock = block.idx;

                //cout << "Using Data " << block.idx << endl;
                useData(block.data, block.len);

                if(block.idx+1 == blocks.size()) {
                    break;
                }
            }

        }
    });

    unordered_map<string, thread> threads;
    for (auto &host : hosts) {
        threads[host] = thread(reader, fileInfo, host, options);
    }

    for (auto &host : hosts) {
        threads[host].join();
    }

    consumer.join();

	auto seconds = ((double)(chrono::duration_cast<chrono::milliseconds>(chrono::high_resolution_clock::now() - start)).count())/1000.0;
        cout << "Downloaded " << fileInfo->mSize/(1024.0*1024.0) << " MB with " << ((double)fileInfo->mSize/(1024.0*1024.0))/seconds << " MB/s)"<< endl;

    // Clean Up
    hdfsDisconnect(fs);
    hdfsFreeBuilder(hdfsBuilder);
    hdfsFreeFileInfo(fileInfo, 1);

    return 0;
}
