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
    return static_cast<uint64_t>(lo)|(static_cast<uint64_t>(hi)<<32);
#else
   return 0;
#endif
}

static void query(const vector<P_type>& p_type,const vector<unsigned>& l_partkey,const vector<double>& l_extendedprice,const vector<double>& l_discount,const vector<unsigned>& l_shipdate,HashIndexLinearProbing<uint64_t>& p_partkeyIndex,double& result)
{
    static const char* promo="PROMO";
    uint32_t promoPattern1=*reinterpret_cast<const uint32_t*>(promo);
    uint8_t promoPattern2=*reinterpret_cast<const uint8_t*>(promo+4);

    auto start=rdtsc();
    double dividend=0,divisor=0;
    for (unsigned long tid=0,limit=l_partkey.size();tid!=limit;++tid) {
        if ((l_shipdate[tid]<19940301)||(l_shipdate[tid]>=19940401)) continue;
#ifdef DIRECTINDEX
      uint64_t pTid=l_partkey[tid]-1;
      uint32_t t1=*reinterpret_cast<const uint32_t*>(p_type[pTid].data);
      uint8_t t2=*reinterpret_cast<const uint8_t*>(p_type[pTid].data+4);
#else
        auto entry=p_partkeyIndex.lookup(l_partkey[tid]);
        uint32_t t1=*reinterpret_cast<const uint32_t*>(p_type[entry->value].data);
        uint8_t t2=*reinterpret_cast<const uint8_t*>(p_type[entry->value].data+4);
#endif
        double a=l_extendedprice[tid]*(1-l_discount[tid]);
        if ((t1==promoPattern1)&&(t2==promoPattern2))
            dividend+=a;
        divisor+=a;
    }
    auto stop=rdtsc();
    auto diff=stop-start;
    cerr << diff << " cycles, " << (static_cast<double>(diff)/l_partkey.size()) << " cycles per lineitem tuple" << endl;
    result=100*(dividend/divisor);
}

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
