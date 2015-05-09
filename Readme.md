# HDFS Library Benchmark

This benchmark evaluates the performance of the native JNI-based libhdfs and the native implementation libhdfs3 (https://github.com/PivotalRD/libhdfs3).

## Compiling

`$ cd hdfs-benchmark`
`$ mkdir build`
`$ cd build`
`$ cmake ..`

Running `$ cmake ..` without arguments usually finds the Hadoop-provided libhdfs, if Hadoop was installed in `/usr/local/hadoop` 
or with Clouderas Hadoop distribution (CDH). In order to link against libhdfs3 specify the library and include dir like so:

`$ cmake -DLIBHDFS_LIBRARY=... -DLIBHDFS_INCLUDE_DIR=... ..`

## Running the benchmarks

### Nativer Lib
If you run the benchmark as root, e.g. with sudo, the file system cache will be cleared automatically. If you prefer to run the benchmark without root rights, run the following commands to drop the file system cache:  

`$ sudo sync`  
`$ sudo echo 3 | sudo tee /proc/sys/vm/drop_caches`

### Measuring disk read and write speed with dd

`$ dd if=/dev/zero of=tempfile bs=1M count=1024 conv=fdatasync,notrunc`
`$ echo 3 | sudo tee /proc/sys/vm/drop_caches`
`$ dd if=tempfile of=/dev/null bs=1M count=1024 conv=fdatasync,notrunc`
