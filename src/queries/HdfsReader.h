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
#include <algorithm>

#include <boost/log/trivial.hpp>
#include <boost/thread.hpp>
#include <boost/atomic/atomic.hpp>

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

    HdfsReader(string namenode, int port, string socket) : namenode(namenode), namenodePort(port), socket(socket) {

    }

    ~HdfsReader() {
        if (hdfsBuilder) {
            hdfsFreeBuilder(hdfsBuilder);
        }
        if (this->fs) {
            hdfsDisconnect(this->fs);
        }
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
        for (int i = 0; i < entries; i++) {
            filesVector.push_back(files[i]);
        }

        hdfsFreeFileInfo(files, entries);
        return filesVector;
    }

    void listDirectory(string path, function<void(hdfsFileInfo &)> func) {
        int entries;
        hdfsFileInfo *files = hdfsListDirectory(this->fs, path.c_str(), &entries);
        EXPECT_NONZERO_EXC(files, "hdfsListDirectory")

        for (unsigned i = 0; i < entries; i++) {
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
    void read(string path,
              function<void(vector<string> &paths)> initFunc = [](vector<string> &paths) { },
              function<void(Block &)> func = [](Block b) { },
              unsigned consumerCount = 1) {
        this->reset();

        // Check if path is pointing at a file or a directory
        vector<string> paths;
        if (this->isDirectory(path)) {
            this->listDirectory(path, [&paths](hdfsFileInfo &fileInfo) {
                if (fileInfo.mKind == tObjectKind::kObjectKindFile) {
                    paths.push_back(fileInfo.mName);
                }
            });
        } else {
            paths.push_back(path);
        }

        if (initFunc) {
            initFunc(paths);
        }

        // Determine the set of hosts and the hosts of all files
        // to build up `pendingBlocks`
        for (auto &path : paths) {
            auto fileInfo = hdfsGetPathInfo(fs, path.c_str());
            EXPECT_NONZERO_EXC(fileInfo, "hdfsGetPathInfo")

            char ***fileBlocksHosts = hdfsGetHosts(fs, path.c_str(), 0, fileInfo->mSize);
            EXPECT_NONZERO_EXC(fileBlocksHosts, "hdfsGetHosts")

            for (size_t blockIdx = 0; fileBlocksHosts[blockIdx]; blockIdx++) {
                set<string> blockHosts;
                for (size_t hostIdx = 0; fileBlocksHosts[blockIdx][hostIdx]; hostIdx++) {
                    char *host = fileBlocksHosts[blockIdx][hostIdx];
                    blockHosts.insert(host);
                    hosts.insert(host);
                }

                pendingBlocks.push(Block(*fileInfo, blockIdx, blockHosts));
            }
        }

        BOOST_LOG_TRIVIAL(debug) << "Downloading " << pendingBlocks.size() << "blocks";

        size_t blockCount = pendingBlocks.size();
        boost::atomic<unsigned> consumedBlocks(0);

        // block consumers
        boost::thread_group consumers;
        for (unsigned int i = 0; i < consumerCount; i++) {
            consumers.create_thread([i, &blockCount, &consumedBlocks, this, &func]() {
                //uint32_t lastBlock = -1;

                while (true) {
                    // A) old impl. guarantees blocks arrive in order
                    /*unique_lock<mutex> lock(blocksMutex);
                    if(loadedBlocks.size() == 0 || loadedBlocks.peek().idx != lastBlock+1) {
                        cv.wait(lock);
                    }

                    if(loadedBlocks.peek().idx == lastBlock+1) {
                        auto block = loadedBlocks.pop();
                        lastBlock = block.idx;

                        if(func) {
                            func(block);
                        }

                        if(block.idx+1 == blockCount) {
                            break;
                        }
                    }*/

                    // B) doesnt guarantee order
                    if (blockCount == consumedBlocks) {
                        break;
                    }

                    Block *block = 0;
                    {
                        unique_lock<mutex> lock(blocksMutex);
                        if (loadedBlocks.size() == 0) {
                            BOOST_LOG_TRIVIAL(debug) << "Thread-" << i << " sleeping";
                            cv.wait(lock);
                            BOOST_LOG_TRIVIAL(debug) << "Thread-" << i << " woken up";
                        }

                        if (blockCount == consumedBlocks) {
                            break;
                        } else if (loadedBlocks.size() > 0) {
                            block = new Block(loadedBlocks.pop());
                            consumedBlocks++;
                        }
                    }

                    if (func && block != 0) {
                        func(*block);
                        BOOST_LOG_TRIVIAL(debug) << "Thread-" << i << " finished work";
                    } else if (block == 0) {
                        BOOST_LOG_TRIVIAL(debug) << "Thread-" << i << " found block == 0";
                    }


                    delete block;
                }
                BOOST_LOG_TRIVIAL(debug) << "Thread-" << i << " finishing";
            });
        }

        // start block readers
        unordered_map<string, thread> threads;
        for (auto &host : hosts) {
            threads[host] = thread(&HdfsReader::reader, this, host);
        }

        // Wait for all threads to finish
        for (auto &host : hosts) {
            threads[host].join();
        }

        {
            unique_lock<mutex> lock(blocksMutex);
            cv.notify_all();
        }

        consumers.join_all();

        //auto seconds = ((double)(chrono::duration_cast<chrono::milliseconds>(chrono::high_resolution_clock::now() - start)).count())/1000.0;
        //cout << "Downloaded " << fileInfo->mSize/(1024.0*1024.0) << " MB with " << ((double)fileInfo->mSize/(1024.0*1024.0))/seconds << " MB/s)"<< endl;
    }

    void reset() {
        this->pendingBlocks.clear();
        this->loadedBlocks.clear();
        this->blocks.clear();
        this->hosts.clear();
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

        if (!this->socket.empty()) {
            hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit", "true");
            hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit.skip.checksum",
                                  this->skipChecksums ? "true" : "false");
            hdfsBuilderConfSetStr(hdfsBuilder, "dfs.domain.socket.path", this->socket.c_str());
        } else {
            hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit", "false");
        }

        hdfsFS fs = hdfsBuilderConnect(hdfsBuilder);
        EXPECT_NONZERO_EXC(fs, "hdfsBuilderConnect")

        return fs;
    }

    void reader(string host) {
        BOOST_LOG_TRIVIAL(debug) << "Thread-" << host << " starting";

        struct hdfsBuilder *hdfsBuilder = hdfsNewBuilder();
        hdfsFS fs = connect(hdfsBuilder);

        //hdfsFile file = hdfsOpenFile2(fs, host.c_str(), this->path.c_str(), O_RDONLY, this->bufferSize, 0, 0);
        //EXPECT_NONZERO_EXC(file, "hdfsOpenFile2")

        while (true) {
            shared_ptr<Block> downloadBlock = nullptr;
            {
                unique_lock<mutex> lock(blocksMutex);

                uint count = 0;
                for (auto &block : loadedBlocks) {
                    if (block.host.compare(host) == 0) {
                        count++;
                    }
                }

                if (count >= 3) {
                    continue;
                }

                for (auto it = pendingBlocks.begin(); it != pendingBlocks.end(); it++) {
                    if ((*it).hosts.count(host) > 0) {
                        downloadBlock = shared_ptr<Block>(new Block(*it));
                        pendingBlocks.remove(it);
                        break;
                    }
                }

                if (downloadBlock == NULL) {
                    BOOST_LOG_TRIVIAL(debug) << "Thread-" << host << " did not find job (" << pendingBlocks.size() <<
                                             " pending)";
                    break;
                }

            }

            // Download the block `downloadBlockIdx`
            BOOST_LOG_TRIVIAL(debug) << "Thread-" << host << " downloading " << downloadBlock->fileInfo.mName << "(" <<
                                     downloadBlock->idx << ")";

            auto start = chrono::high_resolution_clock::now();

            hdfsFile file = hdfsOpenFile2(fs, host.c_str(), downloadBlock->fileInfo.mName, O_RDONLY, this->bufferSize,
                                          0, 0);
            EXPECT_NONZERO_EXC(file, "hdfsOpenFile2")

            downloadBlock->host = host;
            downloadBlock->len =
                    downloadBlock->fileInfo.mSize - downloadBlock->idx * downloadBlock->fileInfo.mBlockSize;
            downloadBlock->data = shared_ptr<void>(malloc(downloadBlock->len), free);

            tOffset offset = downloadBlock->fileInfo.mBlockSize * ((uint64_t) downloadBlock->idx);
            int r = hdfsSeek(fs, file, offset);
            EXPECT_NONNEGATIVE(r, "hdfsSeek")

            tSize read = 0, totalRead = 0;
            do {
                read = hdfsRead(fs, file, static_cast<char *>(downloadBlock->data.get()) + totalRead,
                                downloadBlock->len - totalRead);
                EXPECT_NONNEGATIVE(read, "hdfsRead")

                totalRead += read;
            } while (read > 0 && totalRead < downloadBlock->len);

            assert(totalRead == downloadBlock->len);

            auto seconds = ((double) (chrono::duration_cast<chrono::milliseconds>(
                    chrono::high_resolution_clock::now() - start)).count()) / 1000.0;
            BOOST_LOG_TRIVIAL(debug) << "Thread-" << host << " downloaded " << downloadBlock->fileInfo.mName << " (" <<
                                     downloadBlock->idx << ", " <<
                                     totalRead / (1024.0 * 1024.0) << " MB with " <<
                                     ((double) totalRead / (1024.0 * 1024.0)) / seconds << " MB/s)";
            // TODO Waiting for this takes ages ... measure
            {
                unique_lock<mutex> lock(blocksMutex);
                loadedBlocks.push(Block(*downloadBlock.get()));
                cv.notify_one();
            }

            hdfsCloseFile(fs, file);
        }

        BOOST_LOG_TRIVIAL(debug) << "Thread-" << host << " finished";
    }

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
