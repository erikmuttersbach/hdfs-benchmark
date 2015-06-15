#include <iostream>

#include "HdfsReader.h"

int main() {
    HdfsReader reader("/tpch/100/customer/customer.tbl", "scyper11");
    reader.read([](Block block){
        cout << "Using Block " << block.idx << endl;
    });

    return 0;
}
