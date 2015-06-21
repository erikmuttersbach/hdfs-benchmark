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

    cout << "|";
    vector<bool> p(file.getFileMetaData()->schema.size()-1);
    for(unsigned i=1; i<file.getFileMetaData()->schema.size(); i++) {
        auto &schemaElement = file.getFileMetaData()->schema[i];
        cout << " " << setw(15) << schemaElement.name << " |";
        p[i-1] = false;
    }
    cout << endl;

    for(unsigned i=0; i<16; i++) {
        p[i] = true;
    }

    for(auto &rowGroup : file.getRowGroups()) {
        vector<benchmark::ColumnChunk> columnChunks;
        vector<benchmark::ColumnChunk::Reader> columnReaders;
        for(auto &col : rowGroup.allColumns()) {
            if(p[columnReaders.size()]) {
                columnReaders.push_back(move(col.getReader()));
            } else {
                columnReaders.push_back(benchmark::ColumnChunk::Reader(&col));
            }
        }

        for(unsigned i=0; i<10; i++) {
            cout << "|";
            for(auto &columnReader : columnReaders) {
                if(p[columnReader.getIdx()]) {
                    try {
                        assert(columnReader.hasNext());
                        cout << " " << setw(15) << columnReader.readString() << " |";
                    } catch(exception &e) {
                        p[columnReader.getIdx()] = false;
                        cout << " " << setw(15) << " EXC " << " |";
                    }
                } else {
                    cout << " " << setw(15) << " NO " << " |";
                }
            }
            cout << endl;
        }
    }

    return 0;
}
