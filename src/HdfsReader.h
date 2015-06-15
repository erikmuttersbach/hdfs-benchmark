//
// Created by Erik Muttersbach on 31/05/15.
//

#ifndef HDFS_BENCHMARK_HDFSREADER_H
#define HDFS_BENCHMARK_HDFSREADER_H

#include <string>
#include <exception>
#include <set>
#include <thread>
#include <mutex>
#include <iostream>
#include <unordered_map>

#include <hdfs/hdfs.h>

#include "Compare.h"
#include "Block.h"
#include "PriorityQueue.h"
#include "expect.h"

using namespace std;

class HdfsReader {
public:
    HdfsReader(string path, string namenode) : path(path), namenode(namenode) {

    }

    ~HdfsReader() {
        if(hdfsBuilder) {
            hdfsFreeBuilder(hdfsBuilder);
        }
        if(this->fileInfo) {
            hdfsFreeFileInfo(this->fileInfo, 1);
        }
    }

    template<typename Func>
    void read(Func func) {
        // Connect
        this->hdfsBuilder = hdfsNewBuilder();
        this->fs = connect(this->hdfsBuilder);

        // Retrieve File Information and build queue and threads
        fileInfo = hdfsGetPathInfo(fs, path.c_str());

        char ***blocksHosts = hdfsGetHosts(fs, path.c_str(), 0, fileInfo->mSize);
        EXPECT_NONZERO_EXC(blockHosts, "hdfsGetHosts")

        set<string> hosts;
        for (size_t block = 0; blocksHosts[block]; block++) {
            set<string> blockHosts;
            for (size_t j = 0; blocksHosts[block][j]; j++) {
                blockHosts.insert(blocksHosts[block][j]);
                hosts.insert(blocksHosts[block][j]);
            }

            pendingBlocks.push(Block(block, blockHosts));
        }
        size_t blockCount = pendingBlocks.size();

        /*if (options.verbose) {
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
        }*/

        // block consumer
        thread consumer([&]() {
            uint32_t lastBlock = -1;
            while(true) {
                unique_lock<mutex> lock(blocksMutex);
                if(loadedBlocks.size() == 0 || loadedBlocks.peek().idx != lastBlock+1) {
                    cv.wait(lock);
                }

                //cout << "Peek: " << loadedBlocks.peek().idx << endl;
                if(loadedBlocks.peek().idx == lastBlock+1) {
                    auto block = loadedBlocks.pop();
                    lastBlock = block.idx;

                    func(block);

                    if(block.idx+1 == blockCount) {
                        break;
                    }
                }

            }
        });

        // start block readers
        unordered_map<string, thread> threads;
        for (auto &host : hosts) {
            threads[host] = thread(&HdfsReader::reader, this, fileInfo, host);
        }

        // Wait for all threads to finish
        for (auto &host : hosts) {
            threads[host].join();
        }

        consumer.join();

        //auto seconds = ((double)(chrono::duration_cast<chrono::milliseconds>(chrono::high_resolution_clock::now() - start)).count())/1000.0;
        //cout << "Downloaded " << fileInfo->mSize/(1024.0*1024.0) << " MB with " << ((double)fileInfo->mSize/(1024.0*1024.0))/seconds << " MB/s)"<< endl;
    }

    void setSocket(string socket) {
        this->socket = socket;
    }

    void setBufferSize(size_t bufferSize) {
        this->bufferSize = bufferSize;
    }

    void setNamenodePort(int namenodePort) {
        this->namenodePort = namenodePort;
    }

    void setSkipChecksums(bool skipChecksums) {
        this->skipChecksums = skipChecksums;
    }

private:
    hdfsFS connect(struct hdfsBuilder *hdfsBuilder) {
        hdfsBuilderSetNameNode(hdfsBuilder, this->namenode.c_str());
        hdfsBuilderSetNameNodePort(hdfsBuilder, this->namenodePort);

        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit", "true");
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit.skip.checksum", this->skipChecksums ? "true" : "false");
        if(!this->socket.empty()) {
            hdfsBuilderConfSetStr(hdfsBuilder, "dfs.domain.socket.path", this->socket.c_str());
        }

        hdfsFS fs = hdfsBuilderConnect(hdfsBuilder);
        EXPECT_NONZERO_EXC(fs, "hdfsBuilderConnect")

        return fs;
    }

    void reader(hdfsFileInfo *fileInfo, string host) {
        cout << "Thread " << host << " starting" << endl;

        struct hdfsBuilder *hdfsBuilder = hdfsNewBuilder();
        hdfsFS fs = connect(hdfsBuilder);

        hdfsFile file = hdfsOpenFile2(fs, host.c_str(), this->path.c_str(), O_RDONLY, this->bufferSize, 0, 0);
        EXPECT_NONZERO_EXC(file, "hdfsOpenFile2")

        while(true) {
            uint32_t downloadBlockIdx = -1;
            {
                unique_lock<mutex> lock(blocksMutex);

                uint count = 0;
                for(auto& block : loadedBlocks) {
                    if(block.host.compare(host) == 0) {
                        count++;
                    }
                }

                if(count >= 3) {
                    continue;
                }

                for(auto it = pendingBlocks.begin(); it != pendingBlocks.end(); it++) {
                    if((*it).hosts.count(host) > 0) {
                        downloadBlockIdx = (*it).idx;
                        pendingBlocks.remove(it);
                        break;
                    }
                }

                if(downloadBlockIdx == -1) {
                    cout << "Thread-" << host << " did not find job " << pendingBlocks.size() << endl;
                    break;
                }

            }

            // Download the block `downloadBlockIdx`
            cout << "Thread-" << host << " downloading " << downloadBlockIdx << endl;

            auto start = chrono::high_resolution_clock::now();

            int r = hdfsSeek(fs, file, fileInfo->mBlockSize*((uint64_t)downloadBlockIdx));
            EXPECT_NONNEGATIVE(r, "hdfsSeek")

            char *buffer = (char*)malloc(fileInfo->mBlockSize);
            tSize read = 0, totalRead = 0;
            do {
                read = hdfsRead(fs, file, buffer+totalRead, this->bufferSize);
                EXPECT_NONNEGATIVE(read, "hdfsRead")

                totalRead += read;
            } while (read > 0 && totalRead < fileInfo->mBlockSize);

            auto seconds = ((double)(chrono::duration_cast<chrono::milliseconds>(chrono::high_resolution_clock::now() - start)).count())/1000.0;
            cout << "Thread-" << host << " downloaded " << downloadBlockIdx << " (" << totalRead/(1024.0*1024.0) << " MB with " << ((double)totalRead/(1024.0*1024.0))/seconds << " MB/s)"<< endl;

            {
                unique_lock<mutex> lock(blocksMutex);
                loadedBlocks.push(Block(downloadBlockIdx, host, buffer, totalRead));
                cv.notify_one();
            }
        }

        hdfsCloseFile(fs, file);
        cout << "Thread " << host << " finished" << endl;
    }

private:
    string path;
    string namenode;
    string socket;
    int namenodePort = 9000;
    size_t bufferSize = 4096;
    bool skipChecksums = false;

    struct hdfsBuilder *hdfsBuilder;
    hdfsFS fs;
    hdfsFile file;
    hdfsFileInfo *fileInfo;

    mutex blocksMutex;
    condition_variable cv;
    PriorityQueue<Block, vector<Block>, Compare> pendingBlocks;
    PriorityQueue<Block, vector<Block>, Compare> loadedBlocks;
};


#endif //HDFS_BENCHMARK_HDFSREADER_H
