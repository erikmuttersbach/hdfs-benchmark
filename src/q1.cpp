#include <iostream>
#include <string>
#include <functional>

#include <parquet/parquet.h>
#include <vector>
#include <iomanip>

#include "HdfsReader.h"
#include "ParquetFile.h"

static void print(const char* header,double* values)
{
    cerr << header;
    for (unsigned index=0;index!=8;++index) cerr << fixed << setprecision(2) << " " << values[index];
    cerr << endl;
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

static void aggregate(const vector<char>& returnflag,const vector<char>& linestatus,const vector<double>& quantity,const vector<double>& extendedprice,const vector<double>& discount,const vector<double>& tax,const vector<unsigned>& date,double* results) __attribute__((noinline));
static void aggregate(const vector<char>& returnflag,const vector<char>& linestatus,const vector<double>& quantity,const vector<double>& extendedprice,const vector<double>& discount,const vector<double>& tax,const vector<unsigned>& date,double* results)
{
    const char* returnflagc=returnflag.data(),*linestatusc=linestatus.data();
    const double* quantityc=quantity.data(),*extendedpricec=extendedprice.data(),*discountc=discount.data(),*taxc=tax.data();
    const unsigned* datec=date.data();
    struct Sums { double sum1,sum2,sum3,sum4,sum5,count; };
    Sums sums[65536]={{0,0,0,0,0,0}};
    auto start=rdtsc();
    for (unsigned long tid=0,limit=quantity.size();tid!=limit;++tid) {
        if (datec[tid]>19980811) continue; // using a branch is actually faster than choosing an invalid group instead. The condition is false 97%
        char returnflag=returnflagc[tid],linestatus=linestatusc[tid];
        Sums& s=sums[((returnflag<<8)|linestatus)];
        double quantity=quantityc[tid];
        double extendedprice=extendedpricec[tid];
        double discount=discountc[tid];
        double tax=taxc[tid];
        double v1=extendedprice*(1.0-discount);
        double v2=v1*(1.0+tax);
        s.sum1+=quantity;
        s.sum2+=extendedprice;
        s.sum3+=v1;
        s.sum4+=v2;
        s.sum5+=discount;
        s.count++;
    }
    auto stop=rdtsc();
    auto diff=stop-start;
    cerr << diff << " cycles, " << (static_cast<double>(diff)/quantity.size()) << " cycles per iteration" << endl;
    unsigned groups[4]={('A'<<8)|'F',('N'<<8)|'F',('N'<<8)|'O',('R'<<8)|'F'};
    for (unsigned index=0;index!=4;++index) {
        double count=sums[groups[index]].count;
        results[8*index+0]=sums[groups[index]].sum1;
        results[8*index+1]=sums[groups[index]].sum2;
        results[8*index+2]=sums[groups[index]].sum3;
        results[8*index+3]=sums[groups[index]].sum4;
        results[8*index+4]=sums[groups[index]].sum1/count;
        results[8*index+5]=sums[groups[index]].sum2/count;
        results[8*index+6]=sums[groups[index]].sum5/count;
        results[8*index+7]=count;
    }
}

int main(int argc, char **argv) {
    if(argc != 3) {
        cout << "Usage: " << argv[0] << " FILE NAMENODE" << endl;
        exit(1);
    }

    // A
    /*
    HdfsReader reader(argv[1], argv[2]);
    reader.connect();
    reader.read(std::function<void(Block)>());

    ParquetFile parquetFile(static_cast<uint8_t*>(reader.getBuffer()), reader.getFileSize());
    parquetFile.printInfo();

    // Read Columns
    auto returnflag = parquetFile.readColumn<string, char>(8, [](string v){return v.data()[0];});
    auto linestatus = parquetFile.readColumn<string, char>(9, [](string v){return v.data()[0];});
    auto quantity = parquetFile.readColumn<double>(4);
    auto extendedprice = parquetFile.readColumn<double>(5);
    auto discount = parquetFile.readColumn<double>(6);
    auto tax = parquetFile.readColumn<double>(7);
    auto date = parquetFile.readColumn<string, unsigned>(10, [](string v){
        int a=0,b=0,c=0;
        sscanf(v.data(),"%d-%d-%d",&a,&b,&c);
        return (a*10000)+(b*100)+c;
    });*/

    // B
    vector<string> paths = {
            "/tpch/1_parquet/lineitem/6e498418984728bf-4a439c45d1415caf_1214301924_data.0.parq",
            "/tpch/1_parquet/lineitem/6e498418984728bf-4a439c45d1415cb0_1013092888_data.0.parq",
            "/tpch/1_parquet/lineitem/6e498418984728bf-4a439c45d1415cb1_1013092888_data.0.parq",
            "/tpch/1_parquet/lineitem/6e498418984728bf-4a439c45d1415cb2_644054718_data.0.parq",
            "/tpch/1_parquet/lineitem/6e498418984728bf-4a439c45d1415cb3_644054718_data.0.parq"
    };

    vector<char> returnflag,linestatus;
    vector<double> quantity,extendedprice,discount,tax;
    vector<unsigned> date;

    for(string &path : paths) {
        HdfsReader reader(path, argv[2]);
        reader.connect();
        reader.read(std::function<void(Block)>());

        ParquetFile parquetFile(static_cast<uint8_t*>(reader.getBuffer()), reader.getFileSize());

        auto _returnflag = parquetFile.readColumn<string, char>(8, [](string v){return v.data()[0];});
        auto _linestatus = parquetFile.readColumn<string, char>(9, [](string v){return v.data()[0];});
        auto _quantity = parquetFile.readColumn<double>(4);
        auto _extendedprice = parquetFile.readColumn<double>(5);
        auto _discount = parquetFile.readColumn<double>(6);
        auto _tax = parquetFile.readColumn<double>(7);
        auto _date = parquetFile.readColumn<string, unsigned>(10, [](string v){
            int a=0,b=0,c=0;
            sscanf(v.data(),"%d-%d-%d",&a,&b,&c);
            return (a*10000)+(b*100)+c;
        });

        returnflag.insert(returnflag.end(), _returnflag.begin(), _returnflag.end());
        linestatus.insert(linestatus.end(), _linestatus.begin(), _linestatus.end());
        quantity.insert(quantity.end(), _quantity.begin(), _quantity.end());
        extendedprice.insert(extendedprice.end(), _extendedprice.begin(), _extendedprice.end());
        discount.insert(discount.end(), _discount.begin(), _discount.end());
        tax.insert(tax.end(), _tax.begin(), _tax.end());
        date.insert(date.end(), _date.begin(), _date.end());
    }

    /*for(int i=0; i<20; i++) {
        cout << quantity[i] << endl;
    }*/

    // Aggregate
    for (unsigned index=0;index!=5;++index) {
        {
            auto start=std::chrono::high_resolution_clock::now();
            double results[4*8];
            aggregate(returnflag,linestatus,quantity,extendedprice,discount,tax,date,results);
            auto stop=std::chrono::high_resolution_clock::now();
            print("A F",results+0);
            print("N F",results+8);
            print("N O",results+16);
            print("R F",results+24);
            cerr << "duration " << std::chrono::duration_cast<std::chrono::milliseconds>(stop-start).count() << "ms" << endl;
        }
    }

    return 0;
}
