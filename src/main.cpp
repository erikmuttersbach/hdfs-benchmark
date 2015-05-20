#include <string.h>

#include <iostream>
#include <unordered_map>
#include <vector>
#include <set>

#include <hdfs/hdfs.h>

#include "options.h"
#include "expect.h"

using namespace std;

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

    // Retrieve File Information and build queue and threads
    hdfsFileInfo *fileInfo = hdfsGetPathInfo(fs, options.path);

    char ***blockHosts = hdfsGetHosts(fs, options.path, 0, fileInfo->mSize);
    EXPECT_NONZERO(blockHosts, "hdfsGetHosts")

    set<string> hosts;
    unordered_map<uint32_t, vector<string>> blocks;
    for (uint block = 0; blockHosts[block]; block++) {
        //vector<string> blockHosts;
        for (uint j = 0; blockHosts[block][j]; j++) {
            if (blocks.count(block) == 0) {
                blocks[block] = vector<string>();
            }
            blocks[block].push_back(blockHosts[block][j]);

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
    

    // Clean Up
    hdfsDisconnect(fs);
    hdfsFreeBuilder(hdfsBuilder);

    return 0;
}
