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
#include <condition_variable>
#include <iostream>
#include <unordered_map>

#include <string.h>
#include <hdfs/hdfs.h>

#include "Compare.h"
#include "Block.h"
#include "PriorityQueue.h"
#include "expect.h"

using namespace std;

class HdfsReader {
public:
    HdfsReader(string namenode) : namenode(namenode) {

    }

    ~HdfsReader() {
        if(hdfsBuilder) {
            hdfsFreeBuilder(hdfsBuilder);
        }

        // TODO clean up
    }

    void connect() {
        this->hdfsBuilder = hdfsNewBuilder();
        this->fs = connect(this->hdfsBuilder);
    }

    vector<hdfsFileInfo> listDirectory(string path) {
        int entries;
        hdfsFileInfo *files = hdfsListDirectory(this->fs, path.c_str(), &entries);
        EXPECT_NONZERO_EXC(files, "hdfsListDirectory")

        vector<hdfsFileInfo> filesVector;
        for(int i=0; i<entries; i++) {
            filesVector.push_back(files[i]);
        }

        hdfsFreeFileInfo(files, entries);
        return filesVector;
    }

    void listDirectory(string path, function<void(hdfsFileInfo&)> func) {
        int entries;
        hdfsFileInfo *files = hdfsListDirectory(this->fs, path.c_str(), &entries);
        EXPECT_NONZERO_EXC(files, "hdfsListDirectory")

        for(unsigned i=0; i<entries; i++) {
            func(files[i]);
        }
    }

    bool isDirectory(string path) {
        hdfsFileInfo *fileInfo = hdfsGetPathInfo(fs, path.c_str());
        EXPECT_NONZERO_EXC(fileInfo, "hdfsGetPathInfo")

        return fileInfo->mKind == tObjectKind::kObjectKindDirectory;
    }

    /**
     * Read a file at `path` or all files in directory at `path`. For each
     * read file `func` will be called.
     *
     * TODO can only be called if A) connected, B) another read(...) is not in progress
     */
    void read(string path, function<void(Block&)> func = [](Block& b){}) {
        // Clean old run
        // TODO also in destructor
        this->pendingBlocks.clear();
        this->loadedBlocks.clear();
        this->blocks.clear();
        this->hosts.clear();

        // Check if path is pointing at a file or a directory
        vector<string> paths;
        if(this->isDirectory(path)) {
            this->listDirectory(path, [&paths](hdfsFileInfo &fileInfo) {
                if(fileInfo.mKind == tObjectKind::kObjectKindFile) {
                    paths.push_back(fileInfo.mName);
                }
            });
        } else {
            paths.push_back(path);
        }

        // Determine the set of hosts and the hosts of all files
        // to build up `pendingBlocks`
        for(auto &path : paths) {
            auto fileInfo = hdfsGetPathInfo(fs, path.c_str());
            EXPECT_NONZERO_EXC(fileInfo, "hdfsGetPathInfo")

            cout << fileInfo->mName << endl;

            char ***fileBlocksHosts = hdfsGetHosts(fs, path.c_str(), 0, fileInfo->mSize);
            EXPECT_NONZERO_EXC(fileBlocksHosts, "hdfsGetHosts")

            for (size_t blockIdx = 0; fileBlocksHosts[blockIdx]; blockIdx++) {
                if(blockIdx > 0) {
                    throw runtime_error("Multi-block files are not supported ("+path+")");
                }

                set<string> blockHosts;
                for (size_t hostIdx = 0; fileBlocksHosts[blockIdx][hostIdx]; hostIdx++) {
                    char *host = fileBlocksHosts[blockIdx][hostIdx];
                    blockHosts.insert(host);
                    hosts.insert(host);
                }

                pendingBlocks.push(Block(fileInfo->mName, blockIdx, blockHosts));
            }
        }

        size_t blockCount = pendingBlocks.size();

        cout << "PENDING BLOCKS: " << endl;
        for(Block &block : pendingBlocks) {
            cout << block.path << endl;
        }

        /*

        // Allocate memory for the whole file
        if(this->buffer) {
            free(this->buffer);
        }
        this->buffer = static_cast<char*>(malloc(this->fileInfo->mSize));
        EXPECT_NONZERO_EXC(this->buffer, "malloc")

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

                    //this->blocks[block.idx] = block;
                    if(func) {
                        func(block);
                    }

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

        //TODO free
         */
    }

    /*tOffset getFileSize() {
        return this->fileInfo->mSize;
    }

    tOffset getBlockSize() {
        return this->fileInfo->mBlockSize;
    }*/

    /*void *getBuffer() {
        return this->buffer;
    }*/

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

        if(!this->socket.empty()) {
			hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit", "true");
        	hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit.skip.checksum", this->skipChecksums ? "true" : "false");
            hdfsBuilderConfSetStr(hdfsBuilder, "dfs.domain.socket.path", this->socket.c_str());
        } else {
			hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit", "false");
		}

        hdfsFS fs = hdfsBuilderConnect(hdfsBuilder);
        EXPECT_NONZERO_EXC(fs, "hdfsBuilderConnect")

        return fs;
    }

    /*void reader(hdfsFileInfo *fileInfo, string host) {
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
                    cout << "Thread-" << host << " did not find job (" << pendingBlocks.size() << " pending)" << endl;
                    break;
                }

            }

            // Download the block `downloadBlockIdx`
            cout << "Thread-" << host << " downloading " << downloadBlockIdx << endl;

            auto start = chrono::high_resolution_clock::now();

            tOffset offset = fileInfo->mBlockSize*((uint64_t)downloadBlockIdx);
            int r = hdfsSeek(fs, file, offset);
            EXPECT_NONNEGATIVE(r, "hdfsSeek")

            tSize read = 0, totalRead = 0;
            do {
                read = hdfsRead(fs, file, this->buffer+offset+totalRead, this->fileInfo->mBlockSize);
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
    }*/

private:
    //string path;
    string namenode;
    string socket;
    int namenodePort = 9000;
    size_t bufferSize = 4096;
    bool skipChecksums = false;

    struct hdfsBuilder *hdfsBuilder;
    hdfsFS fs;
    //hdfsFile file;
    //hdfsFileInfo *fileInfo;

    mutex blocksMutex;
    condition_variable cv;

    // Buffer for the whole read file
    //char *buffer = 0;

    // All Hosts
    set<string> hosts;

    // Blocks to be downloaded
    PriorityQueue<Block, vector<Block>, Compare> pendingBlocks;

    // Blocks to be processed
    PriorityQueue<Block, vector<Block>, Compare> loadedBlocks;

    // Ordered list of all blocks downloaded
    vector<Block> blocks;
};


#endif //HDFS_BENCHMARK_HDFSREADER_H
