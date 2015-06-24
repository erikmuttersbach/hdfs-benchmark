//
// Created by Erik Muttersbach on 20/06/15.
//

#include "RowGroup.h"

#include "ParquetFile.h"

namespace benchmark {
    vector<benchmark::ColumnChunk>& RowGroup::allColumns() {
        static vector<ColumnChunk> cols;
        if (cols.size() == 0) {
            for (unsigned i = 0; i < parquetFile->getFileMetaData()->schema.size()-1; i++) {
                cols.push_back(move(getColumn(i)));
            }
        }
        return cols;
    }
}
