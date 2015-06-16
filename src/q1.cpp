#include <iostream>

#include <parquet/parquet.h>
#include <vector>

#include "HdfsReader.h"

int main(int argc, char **argv) {
    if(argc != 3) {
        cout << "Usage: " << argv[0] << " FILE NAMENODE" << endl;
        exit(1);
    }

    HdfsReader reader(argv[1], argv[2]);
    reader.connect();
    reader.read([](Block block){
        cout << "Loaded Block " << block.idx << endl;
    });

    

    //parquet_cpp::DeserializeThriftMsg(0, 0, 0);

    return 0;
}
