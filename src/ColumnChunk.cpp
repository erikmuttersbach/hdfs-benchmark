//
// Created by Erik Muttersbach on 20/06/15.
//

#include <sstream>

#include <stdint.h>

#include "ColumnChunk.h"

#include "ParquetFile.h"

namespace benchmark {

    ColumnChunk::Reader::Reader(ColumnChunk *p, parquet::ColumnChunk &columnChunk) : p(p) {
        size_t columnStart = columnChunk.meta_data.data_page_offset;
        if (columnChunk.meta_data.__isset.dictionary_page_offset) {
            if (columnStart > columnChunk.meta_data.dictionary_page_offset) {
                columnStart = columnChunk.meta_data.dictionary_page_offset;
            }
        }

        const uint8_t *columnBuffer = p->parquetFile->getBuffer() + columnStart;
        this->input = new InMemoryInputStream(columnBuffer, columnChunk.meta_data.total_compressed_size);
        this->columnReader = new ColumnReader(&columnChunk.meta_data,
                                              &p->parquetFile->getFileMetaData()->schema[p->idx + 1], input);
    }

    template<>
    string ColumnChunk::Reader::read() {
        int defLevel, repLevel;
        ByteArray byteArray = this->columnReader->GetByteArray(&defLevel, &repLevel);
        assert(defLevel >= repLevel);
        return string(reinterpret_cast<const char *>(byteArray.ptr), byteArray.len);
    }

    template<>
    ByteArray ColumnChunk::Reader::read() {
        int defLevel, repLevel;
        auto val = this->columnReader->GetByteArray(&defLevel, &repLevel);
        assert(defLevel >= repLevel);
        return val;
    }

    template<>
    double ColumnChunk::Reader::read() {
        int defLevel, repLevel;
        auto val = this->columnReader->GetDouble(&defLevel, &repLevel);
        assert(defLevel >= repLevel);
        return val;
    }

    template<>
    float ColumnChunk::Reader::read() {
        int defLevel, repLevel;
        auto val = this->columnReader->GetFloat(&defLevel, &repLevel);
        assert(defLevel >= repLevel);
        return val;
    }

    template<>
    int32_t ColumnChunk::Reader::read() {
        int defLevel, repLevel;
        auto val = this->columnReader->GetInt32(&defLevel, &repLevel);
        assert(defLevel >= repLevel);
        return val;
    }

    template<>
    int64_t ColumnChunk::Reader::read() {
        int defLevel, repLevel;
        int64_t val = this->columnReader->GetInt64(&defLevel, &repLevel);
        assert(defLevel >= repLevel);
        return val;
    }

    template<>
    bool ColumnChunk::Reader::read() {
        int defLevel, repLevel;
        auto val = this->columnReader->GetBool(&defLevel, &repLevel);
        return val;
    }


    string ColumnChunk::Reader::readString(size_t length) {
        stringstream ss;
        auto &schemaElement = this->p->parquetFile->getFileMetaData()->schema[this->p->idx+1];
        switch(schemaElement.type) {
            case Type::BOOLEAN:
                ss << this->read<bool>(); break;
            case Type::INT32:
                ss << this->read<int32_t>(); break;
            case Type::INT64:
                ss << this->read<int64_t>(); break;
            case Type::INT96:
                ss << "INT96 UNSUPPORTED"; break;
            case Type::FLOAT:
                ss << this->read<float>(); break;
            case Type::DOUBLE:
                ss << this->read<double>(); break;
            case Type::BYTE_ARRAY:
                ss << this->read<string>(); break;
            case Type::FIXED_LEN_BYTE_ARRAY:
                ss << "FIXED_LEN_BYTE_ARRAY UNSUPPORTED"; break;
        }
        string s = ss.str();
        return s.substr(0, length);
    }
}
