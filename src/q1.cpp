#include <iostream>
#include <future>

#include <vector>
#include <iomanip>

#include "HdfsReader.h"
#include "ParquetFile.h"
#include "log.h"
#include "sha256.h"

static void print(const char *header, double *values) {
    cout << header;
    for (unsigned index = 0; index != 8; ++index) cout << fixed << setprecision(2) << " " << values[index];
    cout << endl;
}

int main(int argc, char **argv) {
    initLogging();
    if (argc != 4) {
        cout << "Usage: " << argv[0] << " #THREADS NAMENODE LINEITEM-PATH" << endl;
        exit(1);
    }

    const unsigned threadCount = atoi(argv[1]);

    HdfsReader hdfsReader(argv[2]);
    hdfsReader.connect();

    // Intermediate and result data structures
    struct Group {
        double sum1 = 0, sum2 = 0, sum3 = 0, sum4 = 0, sum5 = 0, count = 0;
    };
    Group groups[4];
    memset(groups, 4, sizeof(Group));
    double results[4 * 8];
    unsigned groupIds[4] = {('A' << 8) | 'F', ('N' << 8) | 'F', ('N' << 8) | 'O', ('R' << 8) | 'F'};
    std::mutex groupsMutex;

    auto start = std::chrono::high_resolution_clock::now();

    vector<vector<Group>> _groups;
    boost::atomic<unsigned> idxCounter(0);

    // Start Reading the directory of parquet files, process the files as
    // they are available
    hdfsReader.read(argv[3], [&](vector<string> &paths){
        _groups.resize(paths.size());
    },[&](Block block) {
        ParquetFile file(static_cast<const uint8_t *>(block.data.get()), block.fileInfo.mSize);

        unsigned idx = idxCounter++;
        _groups[idx].resize(file.getFileMetaData()->num_rows);

        for (auto &rowGroup : file.getRowGroups()) {
            auto quantityColumn = rowGroup.getColumn(4).getReader();
            auto extendedpriceColumn = rowGroup.getColumn(5).getReader();
            auto discountColumn = rowGroup.getColumn(6).getReader();
            auto taxColumn = rowGroup.getColumn(7).getReader();
            auto returnflagColumn = rowGroup.getColumn(8).getReader();
            auto linestatusColumn = rowGroup.getColumn(9).getReader();
            auto dateColumn = rowGroup.getColumn(10).getReader();

            while (dateColumn.hasNext()) {
                assert(returnflagColumn.hasNext() && linestatusColumn.hasNext() &&
                       extendedpriceColumn.hasNext() && discountColumn.hasNext() &&
                       taxColumn.hasNext() && quantityColumn.hasNext());

                string dateStr = dateColumn.read < string > ();
                int a = 0, b = 0, c = 0;
                sscanf(reinterpret_cast<const char *>(dateStr.c_str()), "%d-%d-%d", &a, &b, &c);
                unsigned date = (a * 10000) + (b * 100) + c;

                char returnflag = returnflagColumn.read < ByteArray > ().ptr[0];
                char linestatus = linestatusColumn.read < ByteArray > ().ptr[0];
                double quantity = quantityColumn.read < double > ();
                double extendedprice = extendedpriceColumn.read < double > ();
                double discount = discountColumn.read < double > ();
                double tax = taxColumn.read < double > ();

                if (date > 19980811) {
                    continue;
                }

                int f = ((returnflag << 8) | linestatus);
                int groupId = -1;
                for (unsigned i = 0; i < 4; i++) {
                    if (f == groupIds[i]) {
                        groupId = i;
                        break;
                    }
                }
                assert(groupId >= 0 && groupId < 4);
                Group &s = _groups[idx][groupId];
                double v1 = extendedprice * (1.0 - discount);
                double v2 = v1 * (1.0 + tax);
                s.sum1 += quantity;
                s.sum2 += extendedprice;
                s.sum3 += v1;
                s.sum4 += v2;
                s.sum5 += discount;
                s.count++;
            }
        }

        /*groupsMutex.lock();
        for (unsigned i = 0; i < 4; i++) {
            groups[i].sum1 += _groups[i].sum1;
            groups[i].sum2 += _groups[i].sum2;
            groups[i].sum3 += _groups[i].sum3;
            groups[i].sum4 += _groups[i].sum4;
            groups[i].sum5 += _groups[i].sum5;
            groups[i].count += _groups[i].count;
        }
        groupsMutex.unlock();*/
    }, threadCount);

    for(vector<Group> &g : _groups) {
        for (unsigned i = 0; i < 4; i++) {
            groups[i].sum1 += g[i].sum1;
            groups[i].sum2 += g[i].sum2;
            groups[i].sum3 += g[i].sum3;
            groups[i].sum4 += g[i].sum4;
            groups[i].sum5 += g[i].sum5;
            groups[i].count += g[i].count;
        }
    }

    for (unsigned index = 0; index != 4; ++index) {
        double count = groups[index].count;
        results[8 * index + 0] = groups[index].sum1;
        results[8 * index + 1] = groups[index].sum2;
        results[8 * index + 2] = groups[index].sum3;
        results[8 * index + 3] = groups[index].sum4;
        results[8 * index + 4] = groups[index].sum1 / count;
        results[8 * index + 5] = groups[index].sum2 / count;
        results[8 * index + 6] = groups[index].sum5 / count;
        results[8 * index + 7] = count;
    }

    auto stop = std::chrono::high_resolution_clock::now();

    print("A F", results + 0);
    print("N F", results + 8);
    print("N O", results + 16);
    print("R F", results + 24);

    cout << "duration " << std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count() << "ms" << endl;

    return 0;
}
