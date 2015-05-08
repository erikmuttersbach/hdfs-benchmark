#include <iostream>
#include <getopt.h>
#include <errno.h>
#include <string.h>
#include <hdfs.h>

using namespace std;

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

typedef enum {
    undefined = 0, standard, sc, zcr
} type_t;

struct {
    const char *path = NULL;
    size_t buffer_size = 4096;
    int verbose = false;
    int skip_checksums = false;
    type_t type = type_t::undefined;
} options;

void print_usage() {
    printf("hdfs_benchmark -f FILE [-b BUFFER_SIZE] [-t TYPE]\n"
                   "  -f, --file        File to read\n"
                   "  -b, --buffer      Buffer size, defaults to 4096\n"
                   "  -t, --type        One of standard, sc, zcr\n"
                   "  -v, --verbose     Verbose output, e.g. statistics, formatted speed\n");
}

void parse_options(int argc, char *argv[]) {

    static struct option options_config[] = {
            //{"force-hdfs-standard", no_argument, &options.force_hdfs_standard_read, 1},
            //{"use-hdfs-pread", no_argument, &options.use_hdfs_pread, 1},
/*#ifdef __linux__
            {"advise-sequential",   no_argument, &options.advise_sequential, 1},
            {"advise-willneed",     no_argument, &options.advise_willneed, 1},
            {"use-readahead",       no_argument, &options.use_readahead, 1},
            {"use-ioprio",          no_argument, &options.use_ioprio, 1},
#endif*/
            {"file",   required_argument, 0,                'f'},
            {"buffer", optional_argument, 0,                'b'},
            {"help",   optional_argument, 0,                'h'},
            {"type",   optional_argument, 0,                't'},
            {"skip-checksums",     no_argument, &options.skip_checksums, 1},
            {"v",      no_argument,       &options.verbose, 'v'},
            {0, 0,                        0,                0}
    };

    int c = 0;
    while (c >= 0) {
        int option_index;
        c = getopt_long(argc, argv, "f:b:t:", options_config, &option_index);

        switch (c) {
            case 'f':
                options.path = optarg;
                break;
            case 'b':
                options.buffer_size = atoi(optarg);
                break;
            case 'h':
                print_usage();
                exit(1);
            case 't':
                if(strcmp(optarg, "standard") == 0) {
                    options.type = type_t::standard;
                } else  if(strcmp(optarg, "sc") == 0) {
                    options.type = type_t::sc;
                } else  if(strcmp(optarg, "zcr") == 0) {
                    options.type = type_t::zcr;
                } else {
                    printf("%s is not a valid type\n", optarg);
                    exit(1);
                }
                break;
            default:
                break;
        }
    }

    if (options.path == NULL) {
        print_usage();
        exit(1);
    }
}

void useData(void *buffer, tSize len) {
    volatile uint64_t sum = 0;
    for (size_t i = 0; i < len; i++) {
        sum += *((char *) buffer + i);
    }
}

bool readHdfsZcr(hdfsFS fs, hdfsFile file, hdfsFileInfo *fileInfo) {
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

    return true;
}

bool readHdfsStandard(hdfsFS fs, hdfsFile file, hdfsFileInfo *fileInfo) {
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

    return true;
}

int main(int argc, char *argv[]) {
    parse_options(argc, argv);

    struct hdfsBuilder *hdfsBuilder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(hdfsBuilder, "localhost");
    hdfsBuilderSetNameNodePort(hdfsBuilder, 9000);
    if(options.type == type_t::undefined || options.type == type_t::sc) {
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit", "true");
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket");
        // TODO Test
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.domain.socket.data.traffic", "true");
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit.streams.cache.size", "4000");
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit.skip.checksum", options.skip_checksums ? "true" : "false");
    }

    // Connect
    hdfsFS fs = hdfsBuilderConnect(hdfsBuilder);

    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);

    tOffset fileSize = 0;

    // Read File
    if (hdfsExists(fs, options.path) != 0) {
        printf("File %s does not exist\n", options.path);

    } else {
        hdfsFileInfo *fileInfo = hdfsGetPathInfo(fs, options.path);
        fileSize = fileInfo[0].mSize;

        hdfsFile file = hdfsOpenFile(fs, options.path, O_RDONLY, options.buffer_size, 0, 0);
        EXPECT_NONZERO(file, "hdfsOpenFile")

        if(options.type == type_t::undefined) {
            if (!readHdfsZcr(fs, file, fileInfo)) {
                readHdfsStandard(fs, file, fileInfo);
            }
        } else if(options.type == type_t::zcr) {
            readHdfsZcr(fs, file, fileInfo);
        } else {
            readHdfsStandard(fs, file, fileInfo);
        }

        // Get Statistics
        struct hdfsReadStatistics *stats;
        hdfsFileGetReadStatistics(file, &stats);
        printf("Statistics:\n\tTotal: %lu\n\tLocal: %lu\n\tShort Circuit: %lu\n\tZero Copy Read: %lu\n",
               stats->totalBytesRead, stats->totalLocalBytesRead, stats->totalShortCircuitBytesRead,
               stats->totalZeroCopyBytesRead);

        hdfsFileFreeReadStatistics(stats);
        hdfsFreeFileInfo(fileInfo, 1);
        hdfsCloseFile(fs, file);
    }

    clock_gettime(CLOCK_MONOTONIC, &end);
    struct timespec d = timespec_diff(start, end);
    double speed = (((double) fileSize) / ((double) d.tv_sec + d.tv_nsec / 1000000000.0)) / (1024.0 * 1024.0);

    printf("Read %f MB with %lfMiB/s\n", ((double) fileSize) / (1024.0 * 1024.0), speed);

    hdfsDisconnect(fs);
    //hdfsFreeBuilder(hdfsBuilder);

    return 0;
}