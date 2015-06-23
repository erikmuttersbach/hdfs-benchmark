#include <iostream>
#include <string>
#include <functional>

#include <parquet/parquet.h>
#include <vector>
#include <iomanip>
#include <hdfs/hdfs.h>

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
            if(hashTable[pos]->key == key) {
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

static uint64_t rdtsc()
// Cycle counter on x86
{
#if defined(__x86_64__) && defined(__GNUC__)
    uint32_t hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return static_cast<uint64_t>(lo) | (static_cast<uint64_t>(hi) << 32);
#else
   return 0;
#endif
}

static void query(const vector<P_type> &p_type, const vector<unsigned> &l_partkey,
                  const vector<double> &l_extendedprice, const vector<double> &l_discount,
                  const vector<unsigned> &l_shipdate, HashIndexLinearProbing<uint64_t> &p_partkeyIndex,
                  double &result) {
    static const char *promo = "PROMO";
    uint32_t promoPattern1 = *reinterpret_cast<const uint32_t *>(promo);
    uint8_t promoPattern2 = *reinterpret_cast<const uint8_t *>(promo + 4);

    auto start = rdtsc();
    double dividend = 0, divisor = 0;
    for (unsigned long tid = 0, limit = l_partkey.size(); tid != limit; ++tid) {
        if ((l_shipdate[tid] < 19940301) || (l_shipdate[tid] >= 19940401)) continue;
#ifdef DIRECTINDEX
      uint64_t pTid=l_partkey[tid]-1;
      uint32_t t1=*reinterpret_cast<const uint32_t*>(p_type[pTid].data);
      uint8_t t2=*reinterpret_cast<const uint8_t*>(p_type[pTid].data+4);
#else
        auto entry = p_partkeyIndex.lookup(l_partkey[tid]);
        uint32_t t1 = *reinterpret_cast<const uint32_t *>(p_type[entry->value].data);
        uint8_t t2 = *reinterpret_cast<const uint8_t *>(p_type[entry->value].data + 4);
#endif
        double a = l_extendedprice[tid] * (1 - l_discount[tid]);
        if ((t1 == promoPattern1) && (t2 == promoPattern2))
            dividend += a;
        divisor += a;
    }
    auto stop = rdtsc();
    auto diff = stop - start;
    cerr << diff << " cycles, " << (static_cast<double>(diff) / l_partkey.size()) << " cycles per lineitem tuple" <<
    endl;
    result = 100 * (dividend / divisor);
}

#define HL(H,L)     ((uint64_t)(((uint64_t)H << 32) + L))
#define H(X)        (X >> 32)
#define L(X)        (X & 0x00000000FFFFFFFF)

int main(int argc, char **argv) {
    initLogging();
    if (argc != 4) {
        cout << "Usage: " << argv[0] << " NAMENODE LINEITEM-PATH PART-PATH" << endl;
        exit(1);
    }

    HdfsReader hdfsReader(argv[1]);
    hdfsReader.connect();

    auto start = std::chrono::high_resolution_clock::now();

    //
    HashIndexLinearProbing<vector<uint64_t>> l_partkeyIndex(80000);

    vector<vector<double>> l_extendedprice, l_discount;
    vector<vector<unsigned>> l_shipdate;

    mutex lineitemMutex, partkeyIndexMutex;

    // Read lineitem and build up the hash index
    hdfsReader.read(argv[2], [&](Block block) {
        ParquetFile file(static_cast<const uint8_t *>(block.data.get()), block.fileInfo.mSize);

        lineitemMutex.lock();
        l_extendedprice.push_back(vector<double>());
        l_discount.push_back(vector<double>());
        l_shipdate.push_back(vector<unsigned>());

        auto &_l_extendedprice = l_extendedprice.back();
        auto &_l_discount = l_discount.back();
        auto &_l_shipdate = l_shipdate.back();

        uint32_t idx1 = l_extendedprice.size()-1;
        lineitemMutex.unlock();

        for (auto &rowGroup : file.getRowGroups()) {
            auto partkeyColumn = rowGroup.getColumn(1).getReader();
            auto extendedpriceColumn = rowGroup.getColumn(5).getReader();
            auto discountColumn = rowGroup.getColumn(6).getReader();
            auto shipdateColumn = rowGroup.getColumn(10).getReader();

            while (partkeyColumn.hasNext()) {
                assert(shipdateColumn.hasNext() && extendedpriceColumn.hasNext() && discountColumn.hasNext());

                ByteArray shipdateByteArray = shipdateColumn.read < ByteArray > ();
                int a = 0, b = 0, c = 0;
                sscanf(reinterpret_cast<const char *>(shipdateByteArray.ptr), "%d-%d-%d", &a, &b, &c);
                unsigned shipdate = (a * 10000) + (b * 100) + c;

                auto partkey = partkeyColumn.read < int32_t > ();
                auto extendedprice = extendedpriceColumn.read < double > ();
                auto discount = discountColumn.read < double > ();

                // TODO correct?
                if (shipdate < 19940301 || shipdate >= 19940401) {
                    continue;
                }

                _l_extendedprice.push_back(extendedprice);
                _l_discount.push_back(discount);
                _l_shipdate.push_back(shipdate);

                partkeyIndexMutex.lock();

                    auto entry = l_partkeyIndex.insert(partkey);
                
                entry->value.push_back(HL(idx1, l_shipdate.size()-1));
                partkeyIndexMutex.unlock();
            }
        }
    }, thread::hardware_concurrency());

    // Read part
    hdfsReader.read(argv[2], [&](Block block) {
        ParquetFile file(static_cast<const uint8_t *>(block.data.get()), block.fileInfo.mSize);
    });

    auto stop = std::chrono::high_resolution_clock::now();
    cout << "duration " << std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count() << "ms" << endl;

    return 0;
}
