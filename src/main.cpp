#include <iostream>
#include <iomanip>

#include "HdfsReader.h"
#include "ParquetFile.h"

using namespace std;

int main(int argc, char **argv) {
    if (argc != 3) {
        cout << "Usage: " << argv[0] << " NAMENODE PATH" << endl;
    }
    HdfsReader reader(argv[1]);
    reader.connect();
    reader.read(argv[2]);

    ParquetFile file(static_cast<const uint8_t *>(reader.getBuffer()), reader.getFileSize());
    file.printSchema();

    cout << "|";
    for (unsigned i = 1; i < file.getFileMetaData().schema.size(); i++) {
        auto &schemaElement = file.getFileMetaData().schema[i];
        cout << " " << setw(15) << schemaElement.name << " |";
    }
    cout << endl;


    for (auto &rowGroup : file.getRowGroups()) {
        vector<benchmark::ColumnChunk::Reader> columnReaders;
        for (auto &col : rowGroup.allColumns()) {
            columnReaders.push_back(move(col.getReader()));
        }

        for (unsigned i = 0; i < 100; i++) {
            cout << "|";
            for (auto &columnReader : columnReaders) {

                assert(columnReader.hasNext());
                cout << " " << setw(15) << columnReader.readString() << " |";

            }
            cout << endl;
        }
    }

    return 0;
}
