#include <iostream>
#include <string>

#include "../queries/HdfsReader.h"
#include "../queries/log.h"

using namespace std;

int main(int argc, char **argv) {
    initLogging();
    if (argc != 1+4) {
        cout << "Usage: " << argv[0] << " NAMENODE SOCKET FILE ORDERED?" << endl;
        exit(1);
    }

    string namenode = argv[1];
    string socket = strcmp(argv[2], "0") == 0 ? "" : argv[2];
    string path = argv[3];
    bool orderPreserving = strcmp(argv[4], "true") == 0 ? true : false;

    auto start = std::chrono::high_resolution_clock::now();

    HdfsReader hdfsReader(namenode, 9000, socket);
    hdfsReader.connect();
    hdfsReader.setOrderPreserving(orderPreserving);

    auto start2 = std::chrono::high_resolution_clock::now();

    size_t len = 0;
    hdfsReader.read(path, nullptr, [&](Block &block) {
        len += block.len;
        BOOST_LOG_TRIVIAL(debug) << "Read block " << block.idx;
    });

    auto stop = std::chrono::high_resolution_clock::now();
    double d_sec = ((double)std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count())/1000.0;
    double d_sec2 = ((double)std::chrono::duration_cast<std::chrono::milliseconds>(stop - start2).count())/1000.0;

    cout << ((double)len)/(1024.*1024.)/d_sec << " " << ((double)len)/(1024.*1024.)/d_sec2 << endl;
}
