#include <iostream>
#include <string>
#include <functional>

#include <parquet/parquet.h>
#include <vector>
#include <iomanip>
#include <hdfs/hdfs.h>

#include "HdfsReader.h"
#include "ParquetFile.h"
#include "Table.h"

static void print(const char *header, double *values) {
    cerr << header;
    for (unsigned index = 0; index != 8; ++index) cerr << fixed << setprecision(2) << " " << values[index];
    cerr << endl;
}

int main(int argc, char **argv) {
    HdfsReader hdfsReader(argv[1]);
    hdfsReader.connect();

    struct Sums {
        double sum1, sum2, sum3, sum4, sum5, count;
    };
    Sums sums[65536] = {{0, 0, 0, 0, 0, 0}};
    double results[4 * 8];
    unsigned long count = 0;

    auto start=std::chrono::high_resolution_clock::now();

    Table part(hdfsReader, vector<string>{argv[2]});
    part.printSchema();
    part.read([&sums, &count](ParquetFile &parquetFile) {
        for (auto &rowGroup : parquetFile.getRowGroups()) {
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

                count++;

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

                Sums &s = sums[((returnflag << 8) | linestatus)];
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
    });


    unsigned groups[4] = {('A' << 8) | 'F', ('N' << 8) | 'F', ('N' << 8) | 'O', ('R' << 8) | 'F'};
    for (unsigned index = 0; index != 4; ++index) {
        double count = sums[groups[index]].count;
        results[8 * index + 0] = sums[groups[index]].sum1;
        results[8 * index + 1] = sums[groups[index]].sum2;
        results[8 * index + 2] = sums[groups[index]].sum3;
        results[8 * index + 3] = sums[groups[index]].sum4;
        results[8 * index + 4] = sums[groups[index]].sum1 / count;
        results[8 * index + 5] = sums[groups[index]].sum2 / count;
        results[8 * index + 6] = sums[groups[index]].sum5 / count;
        results[8 * index + 7] = count;
    }

    auto stop=std::chrono::high_resolution_clock::now();

    print("A F", results + 0);
    print("N F", results + 8);
    print("N O", results + 16);
    print("R F", results + 24);

    cout << "duration " << std::chrono::duration_cast<std::chrono::milliseconds>(stop-start).count() << "ms" << endl;

    return 0;
}
