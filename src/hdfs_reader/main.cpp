#include <iostream>
#include <cassert>

#include <string.h>
#ifdef HAS_LIBHDFS
#include <hdfs.h>
#else
#include <hdfs/hdfs.h>
#endif

#include "options.h"

using namespace std;

// On Mac OS X clock_gettime is not available
#ifdef __MACH__
#include <mach/mach_time.h>

#define CLOCK_MONOTONIC 0
int clock_gettime(int clk_id, struct timespec *t){
    mach_timebase_info_data_t timebase;
    mach_timebase_info(&timebase);
    uint64_t time;
    time = mach_absolute_time();
    double nseconds = ((double)time * (double)timebase.numer)/((double)timebase.denom);
    double seconds = ((double)time * (double)timebase.numer)/((double)timebase.denom * 1e9);
    t->tv_sec = seconds;
    t->tv_nsec = nseconds;
    return 0;
}
#endif

#define EXPECT_NONZERO(r, func) if(r==NULL) { \
                                    fprintf(stderr, "%s failed: %s\n", func, strerror(errno)); \
                                    exit(1); \
                                }

#define EXPECT_NONNEGATIVE(r, func) if(r < 0) { \
                                    fprintf(stderr, "%s failed: %s\n", func, strerror(errno)); \
                                    exit(1); \
                                }

timespec timespec_diff(timespec start, timespec end) {
    timespec temp;
    if ((end.tv_nsec - start.tv_nsec) < 0) {
        temp.tv_sec = end.tv_sec - start.tv_sec - 1;
        temp.tv_nsec = 1000000000 + end.tv_nsec - start.tv_nsec;
    } else {
        temp.tv_sec = end.tv_sec - start.tv_sec;
        temp.tv_nsec = end.tv_nsec - start.tv_nsec;
    }
    return temp;
}



inline void useData(void *buffer, tSize len) __attribute__((__always_inline__));
inline void useData(void *buffer, tSize len) {
    uint64_t sum  = 0;
    for (size_t i = 0; i < len/sizeof(uint64_t); i++) {
        sum += *(((uint64_t*) buffer) + i);
    }
    assert(sum);
}


bool readHdfsZcr(options_t &options, hdfsFS fs, hdfsFile file, hdfsFileInfo *fileInfo) {
#ifdef HAS_LIBHDFS
    struct hadoopRzOptions *rzOptions;
    struct hadoopRzBuffer *rzBuffer;

    // Initialize zcr
    rzOptions = hadoopRzOptionsAlloc();
    EXPECT_NONZERO(rzOptions, "hadoopRzOptionsAlloc")

    hadoopRzOptionsSetSkipChecksum(rzOptions, options.skip_checksums);
    hadoopRzOptionsSetByteBufferPool(rzOptions, ELASTIC_BYTE_BUFFER_POOL_CLASS);

    size_t total_read = 0, read = 0;
    do {
        rzBuffer = hadoopReadZero(file, rzOptions, options.buffer_size);
        if (rzBuffer != NULL) {
            const void *data = hadoopRzBufferGet(rzBuffer);
            read = hadoopRzBufferLength(rzBuffer);
            total_read += read;

            if (read > 0) {
                useData((void *) data, read);
            }

            hadoopRzBufferFree(file, rzBuffer);
        }
        else if (errno == EOPNOTSUPP) {
            printf("zcr not supported\n");
            return false;
        }
        else {
            EXPECT_NONZERO(rzBuffer, "hadoopReadZero");
        }
    } while (read > 0);

    if (total_read != fileInfo[0].mSize) {
        fprintf(stderr, "Failed to zero-copy read file to full size\n");
        exit(1);
    }

    hadoopRzOptionsFree(rzOptions);

    if(options.verbose) {
        cout << "Performed ZCR" << endl;
    }
    return true;
#else
    cout << "ZCR not supported (link with libhdfs)" << endl;
    return false;
#endif
}


bool readHdfsStandard(options_t &options, hdfsFS fs, hdfsFile file, hdfsFileInfo *fileInfo) {
    char *buffer = (char *) malloc(sizeof(char) * options.buffer_size);
    tSize total_read = 0, read = 0;
    do {
        read = hdfsRead(fs, file, buffer, options.buffer_size);

        if (read > 0) {
            useData(buffer, read);
        }

        total_read += read;
    } while (read > 0);

    if (total_read == 0) {
        fprintf(stderr, "Failed to read any byte from the file\n");
        exit(1);
    }

    free(buffer);

    if(options.verbose) {
        cout << "Performed Standard/SCR" << endl;
    }
    return true;
}

int main(int argc, char *argv[]) {
    options_t options = parse_options(argc, argv);

    if(options.verbose) {
        cout << "Namenode:  " << options.namenode << ":" << options.namenode_port << endl;
        cout << "Socket:    " << options.socket << endl;
        cout << "File:      " << options.path << endl;
        cout << "Buffer:    " << options.buffer_size << endl;
        cout << "Checksums: " << (options.skip_checksums ? "false" : "true") << endl;
        cout << "Type:      " << options.type << endl;
    }

    struct hdfsBuilder *hdfsBuilder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(hdfsBuilder, options.namenode);
    hdfsBuilderSetNameNodePort(hdfsBuilder, options.namenode_port);
    if(options.type == type_t::undefined || options.type == type_t::scr || options.type == type_t::zcr) {
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit", "true");
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.domain.socket.path", options.socket);
        // TODO Test
        //hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.domain.socket.data.traffic", "true");
        //hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit.streams.cache.size", "4000");
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit.skip.checksum", options.skip_checksums > 0 ? "true" : "false");
    } else {
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit", "false");
    }

    // Connect
    hdfsFS fs = hdfsBuilderConnect(hdfsBuilder);
	EXPECT_NONZERO(fs, "hdfsBuilderConnect")

    struct timespec start, end, start2, end2;

    tOffset fileSize = 0;

    // Check if the file exists
    if (hdfsExists(fs, options.path) != 0) {
        printf("File %s does not exist\n", options.path);

    } else {
        hdfsFileInfo *fileInfo = hdfsGetPathInfo(fs, options.path);
        fileSize = fileInfo[0].mSize;

        #ifdef HAS_LIBHDFS
		if(options.verbose) {
	        char ***hosts = hdfsGetHosts(fs, options.path, 0, fileInfo->mSize);
    	    EXPECT_NONZERO(hosts, "hdfsGetHosts")
        	
			uint i=0;
			for(i=0; hosts[i]; i++) {
				for(uint j=0; hosts[i][j]; j++) 
					cout << "Block["<< i << "][" << j << "]: " << hosts[i][j] << endl;
       	 	}
			cout << "Reading " << i << " blocks" << endl;
		}
		#endif

        clock_gettime(CLOCK_MONOTONIC, &start);

        // Open and read the file
        hdfsFile file = hdfsOpenFile(fs, options.path, O_RDONLY, options.buffer_size, 0, 0);
        EXPECT_NONZERO(file, "hdfsOpenFile")

        clock_gettime(CLOCK_MONOTONIC, &start2);

        if(options.type == type_t::undefined) {
            if (!readHdfsZcr(options, fs, file, fileInfo)) {
                cout << "Falling back to standard read" << endl;
                readHdfsStandard(options, fs, file, fileInfo);
            }
        } else if(options.type == type_t::zcr) {
            readHdfsZcr(options, fs, file, fileInfo);
        } else {
            readHdfsStandard(options, fs, file, fileInfo);
        }

        clock_gettime(CLOCK_MONOTONIC, &end2);

        // Get Statistics
#ifdef HAS_LIBHDFS
        if(options.verbose) {
            struct hdfsReadStatistics *stats;
            hdfsFileGetReadStatistics(file, &stats);
            printf("Statistics:\n\tTotal: %lu\n\tLocal: %lu\n\tShort Circuit: %lu\n\tZero Copy Read: %lu\n",
                   stats->totalBytesRead, stats->totalLocalBytesRead, stats->totalShortCircuitBytesRead,
                   stats->totalZeroCopyBytesRead);
            hdfsFileFreeReadStatistics(stats);
        }
#endif

        hdfsFreeFileInfo(fileInfo, 1);
        hdfsCloseFile(fs, file);
    }

    clock_gettime(CLOCK_MONOTONIC, &end);
    struct timespec d = timespec_diff(start, end);
    struct timespec d2 = timespec_diff(start2, end2);
    double speed = (((double) fileSize) / ((double) d.tv_sec + d.tv_nsec / 1000000000.0)) / (1024.0 * 1024.0);
    double speed2 = (((double) fileSize) / ((double) d2.tv_sec + d2.tv_nsec / 1000000000.0)) / (1024.0 * 1024.0);

    if(options.verbose) {
        printf("Read %f MB with %lfMB/s (%lfMB/s)\n", ((double) fileSize) / (1024.0 * 1024.0), speed, speed2);
    } else {
        printf("%f\n", speed);
    }

    hdfsDisconnect(fs);
    //hdfsFreeBuilder(hdfsBuilder);

    return 0;
}
