#include <iostream>

#include "HdfsReader.h"

int main() {
    HdfsReader reader("/tmp/10M", "192.168.77.150");
    reader.read([](Block block){
        cout << "Using Block " << block.idx << endl;
    });

    return 0;
}