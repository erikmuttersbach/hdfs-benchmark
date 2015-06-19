//
// Created by Erik Muttersbach on 16/06/15.
//

#include "ParquetFile.h"

template<> string ParquetFile::readValue(ColumnReader &reader, int *repetitionLevel, int *defintionLevel)
{
    auto byteArray = reader.GetByteArray(repetitionLevel, defintionLevel);
    return string((char*)byteArray.ptr, byteArray.len);
}

template<> ByteArray ParquetFile::readValue(ColumnReader &reader, int *repetitionLevel, int *defintionLevel)
{
    return reader.GetByteArray(repetitionLevel, defintionLevel);
}

template<> double ParquetFile::readValue(ColumnReader &reader, int *repetitionLevel, int *defintionLevel)
{
    return reader.GetDouble(repetitionLevel, defintionLevel);
}

template<> int32_t ParquetFile::readValue(ColumnReader &reader, int *repetitionLevel, int *defintionLevel)
{
    return (int32_t)reader.GetInt32(repetitionLevel, defintionLevel);
}

template<> int64_t ParquetFile::readValue(ColumnReader &reader, int *repetitionLevel, int *defintionLevel)
{
    return (int64_t)reader.GetInt64(repetitionLevel, defintionLevel);
}
