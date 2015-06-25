//
// Created by Erik Muttersbach on 20/06/15.
//

#ifndef HDFS_BENCHMARK_COLUMN_H
#define HDFS_BENCHMARK_COLUMN_H

#include <parquet/parquet.h>

using namespace std;
using namespace parquet_cpp;

class ParquetFile;

namespace benchmark {
    class RowGroup;

    class ColumnChunk {
    public:
        ColumnChunk(ParquetFile *parquetFile, RowGroup *rowGroup, parquet::ColumnChunk &columnChunk, unsigned int idx) :
                columnChunk(columnChunk), parquetFile(parquetFile), rowGroup(rowGroup), idx(idx) {

        }

        class Reader {
        public:
            Reader(ColumnChunk *p) : p(p) {

            }
            Reader(ColumnChunk *p, parquet::ColumnChunk &columnChunk);
            Reader(const Reader&& r) : input(r.input), columnReader(r.columnReader), p(r.p) {

            }

            bool hasNext() {
                return this->columnReader->HasNext();
            }

            template<typename TR>
            TR read();

            string readString(size_t readString = 15);

            unsigned getIdx() {
                return this->p->idx;
            }

            /*~Reader() {
                if(this->input) {
                    delete input;
                }
                if(this->columnReader) {
                    delete columnReader;
                }
            }*/

        private:
            ColumnChunk *p = 0;
            InMemoryInputStream *input = 0;
            ColumnReader *columnReader = 0;
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
