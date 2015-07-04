#include <iostream>

#include "HdfsReader.h"
#include "ParquetFile.h"
#include "log.h"

struct P_brand {
    char data[10];
};
struct P_container {
    char data[10];
};

struct LineitemMatch {
    int32_t partkey;
    double quantity;
    double extendedprice;
};

int main(int argc, char **argv) {
    initLogging();
    if (argc != 1+5) {
        cout << "Usage: " << argv[0] << " #THREADS NAMENODE SOCKET LINEITEM-PATH PART-PATH" << endl;
        exit(1);
    }

    const unsigned threadCount = atoi(argv[1]);
    string namenode = argv[2];
    string socket = (strcmp(argv[3], "-") == 0 ? "" : argv[3]);
    string lineitemPath = argv[4];
    string partPath = argv[5];

    HdfsReader hdfsReader(namenode, 9000, socket);
    hdfsReader.connect();

    auto start = std::chrono::high_resolution_clock::now();

    // Reading part
    static const char *brand = "Brand#23";
    static const char *container = "MED BOX\0";
    const uint64_t brandPattern1 = *reinterpret_cast<const uint64_t *>(brand);
    const uint16_t brandPattern2 = 0;
    const uint64_t containerPattern1 = *reinterpret_cast<const uint64_t *>(container);
    const uint16_t containerPattern2 = 0;

    // We assume we know from table stats that the largest part key is 20000000,
    // instead of using a hash index we use a part
    vector<bool> partMatches;
    partMatches.resize(20000000);

    hdfsReader.read(partPath, [&](vector<string> &paths) {

    }, [&](Block block) {
        ParquetFile file(static_cast<const uint8_t *>(block.data.get()), block.fileInfo.mSize);
        for (auto &rowGroup : file.getRowGroups()) {
            auto partkeyColumn = rowGroup.getColumn(0).getReader();
            auto brandColumn = rowGroup.getColumn(3).getReader();
            auto containerColumn = rowGroup.getColumn(6).getReader();

            P_container container;
            P_brand brand;

            while (partkeyColumn.hasNext()) {
                assert(brandColumn.hasNext() && containerColumn.hasNext());

                auto partkey = partkeyColumn.read<int32_t>();
                auto brandByteArray = brandColumn.read<ByteArray>();
                auto containerByteArray = containerColumn.read<ByteArray>();

                memset(brand.data, 0, 10);
                memcpy(brand.data, brandByteArray.ptr, MIN(brandByteArray.len, 10));

                memset(container.data, 0, 10);
                memcpy(container.data, containerByteArray.ptr, MIN(containerByteArray.len, 10));

                uint64_t b1 = *reinterpret_cast<const uint64_t *>(brand.data);
                uint16_t b2 = *reinterpret_cast<const uint16_t *>(brand.data + 8);
                uint64_t c1 = *reinterpret_cast<const uint64_t *>(container.data);
                uint64_t c2 = *reinterpret_cast<const uint16_t *>(container.data + 8);
                if ((b1 == brandPattern1) && (b2 == brandPattern2) && (c1 == containerPattern1) &&
                    (c2 == containerPattern2)) {
                    partMatches[partkey] = true;
                }
            }
        }
    }, 4);

    // Read lineitem
    vector<LineitemMatch> matched;
    mutex matchedMutex; // TODO optimizable
    hdfsReader.read(lineitemPath, [&](vector<string> &paths) {

    }, [&](Block block) {
        ParquetFile file(static_cast<const uint8_t *>(block.data.get()), block.fileInfo.mSize);
        for (auto &rowGroup : file.getRowGroups()) {
            auto partkeyColumn = rowGroup.getColumn(1).getReader();
            auto quantityColumn = rowGroup.getColumn(4).getReader();
            auto extendedpriceColumn = rowGroup.getColumn(5).getReader();

            while (partkeyColumn.hasNext()) {
                assert(quantityColumn.hasNext() && extendedpriceColumn.hasNext());

                auto partkey = partkeyColumn.read<int32_t>();
                auto quantity = quantityColumn.read<double>();
                auto extendedprice = extendedpriceColumn.read<double>();

                if (partMatches[partkey]) {
                    matchedMutex.lock();
                    matched.push_back({partkey, quantity, extendedprice});
                    matchedMutex.unlock();
                }
            }
        }
    });

    sort(matched.begin(), matched.end(), [](const LineitemMatch &a, const LineitemMatch &b) {
        return a.partkey < b.partkey;
    });

    double sum = 0;
    for (unsigned index = 0, limit = matched.size(); index != limit;) {
        unsigned partkey = matched[index].partkey;
        unsigned end = index;
        double avgQuantity = 0;
        while (true) {
            if ((end == limit) || (matched[end].partkey != partkey)) break;
            avgQuantity += matched[end].quantity;
            ++end;
        }
        avgQuantity = 0.2 * avgQuantity / (end - index);
        for (; index != end; ++index) {
            if (matched[index].quantity < avgQuantity)
                sum += matched[index].extendedprice;
        }

        index = end;
    }

    double result = sum / 7.0;

    auto stop = std::chrono::high_resolution_clock::now();
    cout << result << endl;
    cout << "duration " << std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count() << "ms" << endl;

    return 0;
}
