#include <iostream>
#include <string>
#include <functional>

#include <parquet/parquet.h>
#include <vector>

#include "HdfsReader.h"
#include "ParquetFile.h"

int main(int argc, char **argv) {
    if(argc != 3) {
        cout << "Usage: " << argv[0] << " FILE NAMENODE" << endl;
        exit(1);
    }

    HdfsReader reader(argv[1], argv[2]);
    reader.connect();
    reader.read(std::function<void(Block)>());

    ParquetFile parquetFile(static_cast<uint8_t*>(reader.getBuffer()), reader.getFileSize());
    parquetFile.printInfo();

    // Read Data
    vector<char> returnflag, linestatus;
    vector<double> quantity, extendedprice, discount, tax;
    vector<unsigned> date;

    auto list = parquetFile.readColumn<string>(11);
    for(auto &s : list) {
        //cout << s << endl;
    }



    /*while (parseLine(in,line,elements)) {
        auto d=parseDate(elements[10]);
        char returnFlag=elements[8].first[0],lineStatus=elements[9].first[0];
        returnflag.push_back(returnFlag); linestatus.push_back(lineStatus);
        quantity.push_back(parseDouble(elements[4])); extendedprice.push_back(parseDouble(elements[5])); discount.push_back(parseDouble(elements[6])); tax.push_back(parseDouble(elements[7]));
        date.push_back(d);
    }*/

    return 0;
}
