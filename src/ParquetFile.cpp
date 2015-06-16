//
// Created by Erik Muttersbach on 16/06/15.
//

#include "ParquetFile.h"

template<> string ParquetFile::readValue(ColumnReader &reader, int *repetitionLevel, int *defintionLevel)
{
    auto byteArray = reader.GetByteArray(repetitionLevel, defintionLevel);
    return string((char*)byteArray.ptr, byteArray.len);
}

template<> double ParquetFile::readValue(ColumnReader &reader, int *repetitionLevel, int *defintionLevel)
{
    return reader.GetDouble(repetitionLevel, defintionLevel);
}
