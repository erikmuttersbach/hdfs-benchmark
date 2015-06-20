//
// Created by Erik Muttersbach on 20/06/15.
//

#include "ColumnChunk.h"

#include "ParquetFile.h"

benchmark::ColumnChunk::Reader::Reader(ColumnChunk *p, parquet::ColumnChunk &columnChunk) {
    size_t columnStart = columnChunk.meta_data.data_page_offset;
    if (columnChunk.meta_data.__isset.dictionary_page_offset) {
        if (columnStart > columnChunk.meta_data.dictionary_page_offset) {
            columnStart = columnChunk.meta_data.dictionary_page_offset;
        }
    }

    const uint8_t *columnBuffer = p->parquetFile->getBuffer() + columnStart;
    this->input = new InMemoryInputStream(columnBuffer, columnChunk.meta_data.total_compressed_size);
    this->columnReader = new ColumnReader(&columnChunk.meta_data, &p->parquetFile->getFileMetaData().schema[p->idx + 1], input);
}

template<>
string benchmark::ColumnChunk::Reader::read() {
    int defLevel, repLevel;
    ByteArray byteArray = this->columnReader->GetByteArray(&defLevel, &repLevel);
    return string(reinterpret_cast<const char*>(byteArray.ptr), byteArray.len);
}

template<>
double benchmark::ColumnChunk::Reader::read() {
    int defLevel, repLevel;
    return this->columnReader->GetDouble(&defLevel, &repLevel);
}

template<>
ByteArray benchmark::ColumnChunk::Reader::read() {
    int defLevel, repLevel;
    return this->columnReader->GetByteArray(&defLevel, &repLevel);
}
