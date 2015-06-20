//
// Created by Erik Muttersbach on 18/06/15.
//

#ifndef HDFS_BENCHMARK_TABLE_H
#define HDFS_BENCHMARK_TABLE_H

#include <vector>
#include <string>
#include <functional>

#include "HdfsReader.h"
#include "ParquetFile.h"
#include "sha256.h"

using namespace std;

class Table {
public:
    /**
     * Create a new `Table` which will read data from the file(s) at `paths`.
     * If `paths.size() == 1` and the path is a directory, all files from that
     * directory will be read.
     */
    Table(HdfsReader &hdfsReader, vector<string> paths) : hdfsReader(hdfsReader), paths(paths) {
        // Check if we have to read the files from a directory
        if(this->paths.size() == 1) {
            if(this->hdfsReader.isDirectory(this->paths[0].c_str())) {
                string dir = this->paths[0].c_str();
                this->paths.clear();
                for(hdfsFileInfo fileInfo : this->hdfsReader.listDirectory(dir)) {
                    if(fileInfo.mKind == tObjectKind::kObjectKindFile) {
                        this->paths.push_back(fileInfo.mName);
                    }
                }
            }
        }
    }

    void printSchema() {
        this->hdfsReader.read(this->paths[0]);
		ParquetFile parquetFile(static_cast<uint8_t*>(this->hdfsReader.getBuffer()), this->hdfsReader.getFileSize());
        parquetFile.printSchema();
    }

    /**
     * Start reading the table.
     * `readChunk(...)` will be called for each file.
     */
    void read(function<void(ParquetFile &parquetFile)> readChunk) {
        for(string &path : this->paths) {
			cout << "Reading " << path << " (" << this->hdfsReader.getFileSize() << ")" << endl;
            this->hdfsReader.read(path);
			SHA256 sha256;
            cout << "SHA-256: " << sha256(this->hdfsReader.getBuffer(), this->hdfsReader.getFileSize()) << endl;
			ParquetFile parquetFile(static_cast<uint8_t*>(this->hdfsReader.getBuffer()), this->hdfsReader.getFileSize());
            parquetFile.printSchema();
            readChunk(parquetFile);
        }
    }

private:
    HdfsReader &hdfsReader;
    vector<string> paths;
};


#endif //HDFS_BENCHMARK_TABLE_H
