//
// Created by Erik Muttersbach on 17/06/15.
//

#ifndef HDFS_BENCHMARK_QUERY_H
#define HDFS_BENCHMARK_QUERY_H

#include <vector>
#include <string>

#include "HdfsReader.h"
#include "ParquetFile.h"

class Query {
public:
    Query(int argc, char **argv) {
        // Check program arguments
        if(argc < 3) {
            cout << "Usage: " << argv[0] << " NAMENODE FILE1 [FILE2] [FILE3] ..." << endl;
            exit(1);
        }

        this->hdfsReader = new HdfsReader(argv[1]);
        this->hdfsReader->connect();

        // Determine the parquet files to load
        if(argc == 3) {
            if(this->hdfsReader->isDirectory(argv[2])) {
                for(hdfsFileInfo fileInfo : this->hdfsReader->listDirectory(argv[2])) {
                    if(fileInfo.mKind == tObjectKind::kObjectKindFile) {
                        paths.push_back(fileInfo.mName);
                    }
                }
            }
        } else {
            for(int i=2; i<argc; i++) {
                paths.push_back(argv[i]);
            }
        };

        cout << "Using files: " << endl;
        for(string p : paths) {
            cout << p << endl;
        }
    }

    ~Query() {
        delete this->hdfsReader;
    }

    void run() {
        // Load the data
        for(string &path : paths) {
            // Read the file at `path` and construct a parquet file
            this->hdfsReader->read(path);
            ParquetFile parquetFile(static_cast<uint8_t*>(this->hdfsReader->getBuffer()), this->hdfsReader->getFileSize());

            // Let a concrete implementation load the data
            this->readFile(parquetFile);
        }

        // Execute the actual query
        this->execute();
    }

protected:
    virtual void readFile(ParquetFile file) = 0;
    virtual void execute() = 0;

protected:
    HdfsReader *hdfsReader;
    vector<string> paths;
};


#endif //HDFS_BENCHMARK_QUERY_H
