#include <iostream>
#include <string>
#include <functional>

#include <parquet/parquet.h>
#include <vector>
#include <iomanip>
#include <hdfs/hdfs.h>

#include "HdfsReader.h"
#include "Table.h"

#define CONCAT(v1, v2) v1.insert(v1.end(), v2.begin(), v2.end());

int main(int argc, char **argv) {
    HdfsReader hdfsReader(argv[1]);
    hdfsReader.connect();

    // Reading part
    vector<P_type> p_type;
    vector<unsigned> p_partkey;

    Table part(hdfsReader, vector<string>{"/tpch1_parquet.db/part"});
    part.printSchema();
    part.read([&p_partkey, &p_type](ParquetFile &parquetFile) {
        vector<unsigned> _p_partkey = parquetFile.readColumn<int32_t, unsigned>(0, [](int32_t v) {
            return static_cast<unsigned>(v);
        });
        vector<P_type> _p_type = parquetFile.readColumn<ByteArray, P_type>(4, [](ByteArray v) {
            P_type p;
            memset(p.data, 0, sizeof(p.data));
            memcpy(p.data, v.ptr, MIN(v.len, 25));
            return p;
        });

        p_partkey.insert(p_partkey.end(), _p_partkey.begin(), _p_partkey.end());
        p_type.insert(p_type.end(), _p_type.begin(), _p_type.end());
    });

    // Reading lineitem
    vector<double> l_extendedprice, l_discount;
    vector<unsigned> l_shipdate, l_partkey;

    Table lineitem(hdfsReader, vector<string>{"/tpch1_parquet.db/lineitem"});
    lineitem.printSchema();
    lineitem.read([&l_partkey, &l_extendedprice, &l_discount, &l_shipdate](ParquetFile &parquetFile) {
        auto _l_partkey = parquetFile.readColumn<int32_t, unsigned>(1, [](int32_t v) {
            return static_cast<unsigned>(v);
        });
        auto _l_extendedprice = parquetFile.readColumn<double>(5);
        auto _l_discount = parquetFile.readColumn<double>(6);
        auto _l_shipdate = parquetFile.readColumn<string, unsigned>(10, [](string v) {
            int a = 0, b = 0, c = 0;
            sscanf(v.data(), "%d-%d-%d", &a, &b, &c);
            return (a * 10000) + (b * 100) + c;
        });

        CONCAT(l_partkey, _l_partkey)
        CONCAT(l_extendedprice, _l_extendedprice)
        CONCAT(l_discount, _l_discount)
        CONCAT(l_shipdate, _l_shipdate)
    });

    cout << "constructing primary key index on part..." << endl;
    HashIndexLinearProbing<uint64_t> p_partkeyIndex(p_partkey.size());
    for (unsigned long tid = 0; tid != p_partkey.size(); ++tid) {
        auto entry = p_partkeyIndex.insert(p_partkey[tid]);
        entry->value = tid;
    }
    for (unsigned index = 0; index != 5; ++index) {
        cerr << "executing query" << endl;
        {
            auto start = std::chrono::high_resolution_clock::now();
            double result = 0;
            query(p_type, l_partkey, l_extendedprice, l_discount, l_shipdate, p_partkeyIndex, result);
            auto stop = std::chrono::high_resolution_clock::now();
            cerr << fixed << setprecision(2) << result << endl;
            cerr << "duration " << std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count() <<
            "ms" << endl;
        }
    }

    return 0;
}
