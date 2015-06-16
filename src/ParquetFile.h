//
// Created by Erik Muttersbach on 16/06/15.
//

#ifndef HDFS_BENCHMARK_PARQUETREADER_H
#define HDFS_BENCHMARK_PARQUETREADER_H

#include <vector>
#include <functional>
#include <type_traits>
#include <typeinfo>

#include <parquet/parquet.h>

using namespace parquet;
using namespace parquet_cpp;
using namespace std;

// footer size = 4 byte constant + 4 byte metadata len
const uint32_t FOOTER_SIZE = 8;
const uint8_t PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};

class ParquetFile {
public:
    /**
     * Constructs a new instance of `ParquetReader` and parses the files meta data.
     */
    ParquetFile(const uint8_t *buffer, size_t bufferLength) : buffer(buffer), bufferLength(bufferLength) {
        this->readMetaData();
    }

    template<typename T, typename TR = T>
    vector<TR> readColumn(unsigned int i, function<TR(T)> func = [](T t){return t;}) {
        vector<TR> data;
        for(auto &rowGroup : this->fileMetaData.row_groups) {
            ColumnChunk column = rowGroup.columns[i];
            size_t columnStart = column.meta_data.data_page_offset;
            if (column.meta_data.__isset.dictionary_page_offset) {
                if (columnStart > column.meta_data.dictionary_page_offset) {
                    columnStart = column.meta_data.dictionary_page_offset;
                }
            }

            const uint8_t *columnBuffer = this->buffer + columnStart;
            InMemoryInputStream input(columnBuffer, column.meta_data.total_compressed_size);
            ColumnReader reader(&column.meta_data, &this->fileMetaData.schema[i + 1], &input);

            int repetitionLevel = 0, defintionLevel = 0;
            while (reader.HasNext()) {
                /*if (std::is_same<T, string>::value) {
                    auto byteArray = reader.GetByteArray(&repetitionLevel, &defintionLevel);
                    //data.push_back(func(string((char*)byteArray.ptr, byteArray.len)));
                } else if (std::is_same<T, int32_t >::value) {
                    int32_t val = reader.GetInt32(&repetitionLevel, &defintionLevel);
                    //data.push_back(func(val));
                } else {
                    throw runtime_error("type for columns is not supported");
                }*/
                auto value = this->readValue<T>(reader, &repetitionLevel, &defintionLevel);
                data.push_back(func(value));
            }
        }

        return data;
    }

    // explicit_specialization.cpp
    template<typename T>
    T readValue(ColumnReader &reader, int *repetitionLevel, int *defintionLevel) {
        throw runtime_error(string("Type ")+typeid(T).name()+" is not supported");
    };

    FileMetaData getFileMetaData() {
        return this->fileMetaData;
    }

    void printInfo() {
        cout << "Schema: " << endl;
        int k=-1;
        for(auto &i : this->fileMetaData.schema) {
            cout << k++ << ": " << i.name << " (";
            switch(i.type) {
                case Type::BOOLEAN:
                    cout << "BOOLEAN"; break;
                case Type::INT32:
                    cout << "INT32"; break;
                case Type::INT64:
                    cout << "INT64"; break;
                case Type::INT96:
                    cout << "INT96"; break;
                case Type::FLOAT:
                    cout << "FLOAT"; break;
                case Type::DOUBLE:
                    cout << "DOUBLE"; break;
                case Type::BYTE_ARRAY:
                    cout << "BYTE_ARRAY"; break;
                case Type::FIXED_LEN_BYTE_ARRAY:
                    cout << "FIXED_LEN_BYTE_ARRAY"; break;
            }
            cout << ")" << endl;
        }

        cout << "#Rows: " << this->fileMetaData.num_rows << endl;
    }

private:
    void readMetaData() {
        if (this->bufferLength < FOOTER_SIZE) {
            throw runtime_error("Invalid Parquet file: Corrupt footer");
        }

        const uint8_t *footer = this->buffer+(this->bufferLength-FOOTER_SIZE);

        if (memcmp(footer+4, PARQUET_MAGIC, 4) != 0) {
            throw runtime_error("Invalid Parquet file: Corrupt footer");
        }

        uint32_t metadataLength = *reinterpret_cast<const uint32_t*>(footer);
        if (FOOTER_SIZE + metadataLength > this->bufferLength) {
            throw runtime_error("Invalid parquet file. File is less than file metadata size.");
        }

        const uint8_t *metadata = this->buffer+(this->bufferLength-FOOTER_SIZE-metadataLength);

        DeserializeThriftMsg(metadata, &metadataLength, &this->fileMetaData);
    }

private:
    const uint8_t *buffer;
    size_t bufferLength;

    FileMetaData fileMetaData;
};



#endif //HDFS_BENCHMARK_PARQUETREADER_H
