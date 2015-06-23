#include <iostream>
#include <string>
#include <functional>

#include <parquet/parquet.h>
#include <vector>
#include <iomanip>
#include <hdfs/hdfs.h>
#include <boost/log/trivial.hpp>

#include "HdfsReader.h"
#include "ParquetFile.h"
#include "log.h"

#define CONCAT(v1, v2) v1.insert(v1.end(), v2.begin(), v2.end());

struct P_type {
    char data[25];
};

namespace {
    class Hash {
    public:
        uint64_t operator()(uint64_t key) {
#ifdef __SSE4_2__
      // Use the crc32 function of the processor, if available
      return _mm_crc32_u64(0xab1353a8c4190def,key);
#else
            // Otherwise, use Murmurhash
            const uint64_t m = 0xab1353a8c4190def;
            const int r = 47;
            uint64_t h = 0x8445d61a4e774912 ^(8 * m);
            key *= m;
            key ^= key >> r;
            key *= m;
            h ^= key;
            h *= m;
            h ^= h >> r;
            h *= m;
            h ^= h >> r;
            return h | (1ull << 63);
#endif
        }
    };

    class IdentityHashing {
    public:
        uint64_t operator()(uint64_t key) {
            return key;
        }
    };

    template<class V, typename Hash=Hash>
    class HashIndexLinearProbing
        // A simple hash index using linear probing for collision handling
    {
    public:
        struct Entry {
            uint64_t key;
            V value;
        };
        Entry **hashTable;
        uint64_t size;
        Entry *entries;
        uint64_t fill;

    private:
        uint64_t mask;
        Hash hashFunction;

    public:
        explicit HashIndexLinearProbing(uint64_t elements);

        HashIndexLinearProbing(const HashIndexLinearProbing &) = delete;

        HashIndexLinearProbing &operator=(const HashIndexLinearProbing &) = delete;

        ~HashIndexLinearProbing();

        Entry *insert(uint64_t key);

        Entry *lookup(uint64_t key);
    };

    template<class V, typename Hash>
    HashIndexLinearProbing<V, Hash>::HashIndexLinearProbing(uint64_t elements)
            : fill(0)
    // Constructor
    {
        uint64_t bits = ceil(log2(elements)) + 1;
        size = 1ull << (bits);
        mask = size - 1;
        hashTable = static_cast<Entry **>(malloc(size * sizeof(Entry * )));
        memset(hashTable, 0, size * sizeof(Entry * ));
        entries = static_cast<Entry *>(malloc(size * sizeof(Entry)));
    }

    template<class V, typename Hash>
    HashIndexLinearProbing<V, Hash>::~HashIndexLinearProbing()
    // Destructor
    {
        free(hashTable);
        free(entries);
    }

    template<class V, typename Hash>
    typename HashIndexLinearProbing<V, Hash>::Entry *HashIndexLinearProbing<V, Hash>::insert(uint64_t key)
    // Insert a <key,value> pair
    {
        uint64_t pos = hashFunction(key) & mask;
        while (hashTable[pos] != 0) {
            if (hashTable[pos]->key == key) {
                return hashTable[pos];
            }
            pos = (pos + 1) & mask;
        }
        Entry *entry = entries + (fill++);
        entry->key = key;
        hashTable[pos] = entry;
        return entry;
    }

    template<class V, typename Hash>
    typename HashIndexLinearProbing<V, Hash>::Entry *HashIndexLinearProbing<V, Hash>::lookup(uint64_t key)
    // Lookup a value for a given key
    {
        uint64_t pos = hashFunction(key) & mask;
        while (hashTable[pos] != 0) {
            if (hashTable[pos]->key == key)
                return hashTable[pos];
            pos = (pos + 1) & mask;
        }
        return nullptr;
    }
}

#define HL(H, L)     ((uint64_t)(((uint64_t)H << 32) + L))
#define H(X)        (X >> 32)
#define L(X)        (X & 0x00000000FFFFFFFF)

// q14, assumes statistics are known
int main(int argc, char **argv) {
    initLogging();
    if (argc != 5) {
        cout << "Usage: " << argv[0] << " #THREADS NAMENODE LINEITEM-PATH PART-PATH" << endl;
        exit(1);
    }

    const unsigned threadCount = atoi(argv[1]);

    HdfsReader hdfsReader(argv[2]);
    hdfsReader.connect();

    auto start = std::chrono::high_resolution_clock::now();

    // as determined by
    // SELECT COUNT(*) as count1, 100*year(l_shipdate)+month(l_shipdate) as `year_month` FROM lineitem GROUP BY year_month ORDER BY count1 DESC;
    HashIndexLinearProbing<vector<uint64_t>> l_partkeyIndex(7738727);

    vector<vector<double>> l_extendedprice, l_discount;
    vector<vector<unsigned>> l_shipdate;

    mutex partkeyIndexMutex;

    boost::atomic <uint32_t> idx1Counter(0);

    // Read lineitem and build up the hash index
    hdfsReader.read(argv[3], [&](vector<string> &paths) {
        l_extendedprice.resize(paths.size());
        l_discount.resize(paths.size());
        l_shipdate.resize(paths.size());
    }, [&](Block block) {
        ParquetFile file(static_cast<const uint8_t *>(block.data.get()), block.fileInfo.mSize);

        uint32_t idx1 = idx1Counter++;
        uint32_t idx2 = 0;
        l_extendedprice[idx1].resize(file.getFileMetaData()->num_rows);
        l_discount[idx1].resize(file.getFileMetaData()->num_rows);
        l_shipdate[idx1].resize(file.getFileMetaData()->num_rows);

        for (auto &rowGroup : file.getRowGroups()) {
            auto partkeyColumn = rowGroup.getColumn(1).getReader();
            auto extendedpriceColumn = rowGroup.getColumn(5).getReader();
            auto discountColumn = rowGroup.getColumn(6).getReader();
            auto shipdateColumn = rowGroup.getColumn(10).getReader();

            while (partkeyColumn.hasNext()) {
                assert(shipdateColumn.hasNext() && extendedpriceColumn.hasNext() && discountColumn.hasNext());

                string shipdateByteArray = shipdateColumn.read < string > ();
                int a = 0, b = 0, c = 0;
                sscanf(reinterpret_cast<const char *>(shipdateByteArray.c_str()), "%d-%d-%d", &a, &b, &c);
                unsigned shipdate = (a * 10000) + (b * 100) + c;

                auto partkey = partkeyColumn.read < int32_t > ();
                auto extendedprice = extendedpriceColumn.read < double > ();
                auto discount = discountColumn.read < double > ();

                if (shipdate < 19950901 || shipdate >= 19951001) {
                    continue;
                }

                l_extendedprice[idx1][idx2] = extendedprice;
                l_discount[idx1][idx2] = discount;
                l_shipdate[idx1][idx2] = shipdate;

                partkeyIndexMutex.lock();
                auto entry = l_partkeyIndex.insert(partkey);
                entry->value.push_back(HL(idx1, idx2));
                partkeyIndexMutex.unlock();

                idx2++;
            }
        }
    }, threadCount);

    // Read part
    // TODO parallelize
    static const char *promo = "PROMO";
    uint32_t promoPattern1 = *reinterpret_cast<const uint32_t *>(promo);
    uint8_t promoPattern2 = *reinterpret_cast<const uint8_t *>(promo + 4);

    vector<double> dividend, divisor;
    boost::atomic<unsigned> idxCounter(0);

    auto start2 = std::chrono::high_resolution_clock::now();
    hdfsReader.read(argv[4], [&](vector<string> &paths) {
        dividend.resize(paths.size());
        divisor.resize(paths.size());
    }, [&](Block block) {
        ParquetFile file(static_cast<const uint8_t *>(block.data.get()), block.fileInfo.mSize);

        unsigned idx = idxCounter++;
        divisor[idx] = 0;
        dividend[idx] = 0;

        for (auto &rowGroup : file.getRowGroups()) {
            auto partkeyColumn = rowGroup.getColumn(0).getReader();
            auto typeColumn = rowGroup.getColumn(4).getReader();

            while (partkeyColumn.hasNext()) {
                assert(typeColumn.hasNext());

                auto partkey = partkeyColumn.read < int32_t > ();
                auto typeByteArray = typeColumn.read < ByteArray > ();

                auto entry = l_partkeyIndex.lookup(partkey);
                if (entry == 0) {
                    continue;
                }

                P_type type;
                memset(type.data, 0, 25);
                memcpy(type.data, typeByteArray.ptr, MIN(25, typeByteArray.len));
                uint32_t t1 = *reinterpret_cast<const uint32_t *>(type.data);
                uint8_t t2 = *reinterpret_cast<const uint8_t *>(type.data + 4);

                for (uint64_t tidx : entry->value) {
                    int32_t tidx1 = H(tidx), tidx2 = L(tidx);
                    double a = l_extendedprice[tidx1][tidx2] * (1 - l_discount[tidx1][tidx2]);
                    if ((t1 == promoPattern1) && (t2 == promoPattern2)) {
                        dividend[idx] += a;
                    }
                    divisor[idx] += a;
                }
            }
        }
    }, 4);
    double dividendSum = 0, divisorSum = 0;
    for(unsigned i=0; i<dividend.size(); i++) {
        dividendSum += dividend[i];
        divisorSum += divisor[i];
    }
    double result = 100 * (dividendSum / divisorSum);

    auto stop = std::chrono::high_resolution_clock::now();
    auto stop2 = std::chrono::high_resolution_clock::now();

    cout << result << endl;

    cout << "duration " << std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count() << "ms" << endl;
    //cout << "duration " << std::chrono::duration_cast<std::chrono::milliseconds>(stop2 - start2).count() << "ms" <<
    endl;

    return 0;
}
