//
// Created by Erik Muttersbach on 20/06/15.
//

#ifndef HDFS_BENCHMARK_ROWGROUP_H
#define HDFS_BENCHMARK_ROWGROUP_H

#include <vector>

#include <parquet/parquet.h>

#include "ColumnChunk.h"

class ParquetFile;

using namespace std;

class RowGroup {
public:
    RowGroup(ParquetFile *parquetFile, parquet::RowGroup &rowGroup) : rowGroup(rowGroup), parquetFile(parquetFile) {

    }

    ColumnChunk getColumn(unsigned int col) {
        return ColumnChunk(parquetFile, this, this->rowGroup.columns[col], col);
    }

private:
    parquet::RowGroup rowGroup;
    ParquetFile *parquetFile;
};


#endif //HDFS_BENCHMARK_ROWGROUP_H
