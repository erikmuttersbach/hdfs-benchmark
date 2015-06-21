#include <iostream>
#include <iomanip>

#include "HdfsReader.h"
#include "ParquetFile.h"

using namespace std;

int main(int argc, char **argv) {
    if(argc != 3) {
        cout << "Usage: " << argv[0] << " NAMENODE PATH" << endl;
    }
    HdfsReader reader(argv[1]);
    reader.connect();
    reader.read(argv[2]);

    ParquetFile file(static_cast<const uint8_t*>(reader.getBuffer()), reader.getFileSize());
    file.printSchema();

    vector<ParquetFile> files;
    cout << "|";
    vector<bool> p(file.getFileMetaData().schema.size()-1);
    for(unsigned i=1; i<file.getFileMetaData().schema.size(); i++) {
        auto &schemaElement = file.getFileMetaData().schema[i];
        cout << " " << setw(15) << schemaElement.name << " |";

        files.push_back(ParquetFile(static_cast<const uint8_t*>(reader.getBuffer()), reader.getFileSize()));
    }
    cout << endl;
/*
    vector<bool> p(file.getFileMetaData().schema.size()-1);
    for(unsigned i=1; i<file.getFileMetaData().schema.size(); i++) {
        auto &schemaElement = file.getFileMetaData().schema[i];
        cout << " " << setw(15) << schemaElement.name << " |";
        p[i-1] = false;
    }
    cout << endl;*/

    for(unsigned ri=0; ri<file.getRowGroups().size(); ri++) {
        vector<benchmark::ColumnChunk::Reader> columnReaders;
        for(unsigned ci=0; ci<file.getRowGroups()[ri].getNumberOfColumns(); ci++) {
            columnReaders.push_back(files[ci].getRowGroups()[ri].getColumn(ci).getReader());
        }
/*
        for(unsigned i=0; i<100; i++) {
            cout << "|";
            for(auto &columnReader : columnReaders) {
                if(p[columnReader.getIdx()]) {
                    assert(columnReader.hasNext());
                    cout << " " << setw(15) << columnReader.readString() << " |";
                } else {
                    cout << " " << setw(15) << " NO " << " |";
                }
            }
            cout << endl;
        }*/
    }

    return 0;
}
