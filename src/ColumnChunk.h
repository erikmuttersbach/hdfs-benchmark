//
// Created by Erik Muttersbach on 20/06/15.
//

#ifndef HDFS_BENCHMARK_COLUMN_H
#define HDFS_BENCHMARK_COLUMN_H

#include <parquet/parquet.h>

using namespace std;
using namespace parquet_cpp;

class ParquetFile;
class RowGroup;

namespace benchmark {
    class ColumnChunk {
    public:
        ColumnChunk(ParquetFile *parquetFile, RowGroup *rowGroup, parquet::ColumnChunk &columnChunk, unsigned int idx) :
                columnChunk(columnChunk), parquetFile(parquetFile), rowGroup(rowGroup), idx(idx) {

        }

        class Reader {
        public:
            Reader(ColumnChunk *p, parquet::ColumnChunk &columnChunk);

            bool hasNext() {
                return this->columnReader->HasNext();
            }

            template<typename TR>
            TR read();

            ~Reader() {
                delete input;
                delete columnReader;
            }

        private:
            InMemoryInputStream *input;
            ColumnReader *columnReader;
        };

        ColumnChunk::Reader getReader() {
            return Reader(this, this->columnChunk);
        }

    private:
        parquet::ColumnChunk &columnChunk;
        ParquetFile *parquetFile;
        RowGroup *rowGroup;
        unsigned int idx;
    };
};

using namespace benchmark;


#endif //HDFS_BENCHMARK_COLUMN_H
