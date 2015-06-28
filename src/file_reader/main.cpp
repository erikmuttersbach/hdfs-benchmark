#include <iostream>
#include <cassert>
#include <iomanip>
#include <chrono>

#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <getopt.h>

using namespace std;

#define EXPECT_NONZERO(r, func) if(r==NULL) { \
                                    fprintf(stderr, "%s failed: %s\n", func, strerror(errno)); \
                                    exit(1); \
                                }

#define EXPECT_NONNEGATIVE(r, func) if(r < 0) { \
                                    fprintf(stderr, "%s failed: %s\n", func, strerror(errno)); \
                                    exit(1); \
                                }

inline void use_data(void *buffer, size_t len) __attribute__((__always_inline__));
inline void use_data(void *buffer, size_t len) {
    uint64_t sum  = 0;
    for (size_t i = 0; i < len/sizeof(uint64_t); i++) {
        sum += *(((uint64_t*) buffer) + i);
    }
    assert(sum);
}

enum benchmark_t {
    none = 0, file_mmap, file_read
};

static const char *benchmark_s[] = {
        "none", "file_mmap", "file_read"
};

struct {
    const char *path = "/tmp/1000M";
    size_t buffer_size = 4096;

    int advise_willneed = false;
    int advise_sequential = false;
    int use_readahead = false;
    int use_ioprio = false;

    bool verbose = false;

    benchmark_t benchmark = none;
} options;

void print_usage(int argc, char *argv[]) {
    printf("Usage: %s -t file_read|file_mmap\n", argv[0]);
    printf("\t-f FILE\n");
    printf("\t-b BUFFER_SIZE\n");
    printf("\t-t BENCHMARK_TYPE\n");
    printf("\t-v                 Verbose output \n");
    printf("\t--advise-sequential\n");
    printf("\t--advise-willneed\n");
    printf("\t--use-readahead\n");
    printf("\t--use-ioprio\n");
}

void parse_options(int argc, char *argv[]) {
    static struct option options_config[] = {
#ifdef __linux__
            {"advise-sequential",   no_argument, &options.advise_sequential, 1},
            {"advise-willneed",     no_argument, &options.advise_willneed, 1},
            {"use-readahead",       no_argument, &options.use_readahead, 1},
            {"use-ioprio",          no_argument, &options.use_ioprio, 1},
#endif
            {"file",    optional_argument, 0, 'f'},
            {"buffer",  optional_argument, 0, 'b'},
            {"type",    required_argument, 0, 't'},
            {"verbose", optional_argument, 0, 'v'},
            {0, 0, 0, 0}
    };

    int c = 0;
    while(c >= 0) {
        int option_index;
        c = getopt_long(argc, argv, "f:b:t:v", options_config, &option_index);

        switch(c) {
            case 'f':
                options.path = optarg;
                break;
            case 'b':
                options.buffer_size = atoi(optarg);
                break;
            case 'v':
                options.verbose = true;
                break;
            case 't':
                if(strcmp(optarg, "file_mmap") == 0) {
                    options.benchmark = benchmark_t::file_mmap;
                } else if(strcmp(optarg, "file_read") == 0) {
                    options.benchmark = benchmark_t::file_read;
                } else {
                    printf("%s is not a valid benchmark. Options are: file_mmap, file_read\n", optarg);
                    exit(1);
                }
                break;
            default:
                break;
        }
    }

    if(options.benchmark == benchmark_t::none) {
        print_usage(argc, argv);
        printf("Please select a benchmark type\n");
        exit(1);
    }

    if(options.advise_willneed && options.advise_sequential) {
        printf("Options --advise-willneed and --advise-sequential are exclusive\n");
        exit(1);
    }
}

size_t read_file() {
    FILE *file = fopen(options.path, "r");
    EXPECT_NONZERO(file, "fopen");

#ifdef __linux__
    struct stat file_stat;
    stat(options.path, &file_stat);

    if(options.advise_willneed || options.advise_sequential) {
        posix_fadvise(fileno(file), 0, file_stat.st_size, options.advise_sequential ? POSIX_FADV_SEQUENTIAL : POSIX_FADV_WILLNEED);
    }

    if(options.use_readahead) {
        readahead(fileno(file), 0, file_stat.st_size);
    }

    if(options.use_ioprio) {
        syscall(SYS_ioprio_set, getpid(), 1, 1);
    }
#endif

    char *buffer = (char *) malloc(sizeof(char) * options.buffer_size);
    size_t total_read = 0, read = 0;
    do {
        read = fread(buffer, sizeof(char), options.buffer_size, file);
        if (read > 0) {
            use_data(buffer, read);
        }

        total_read += read;
    } while (read > 0);

    if (total_read == 0) {
        fprintf(stderr, "Failed to read any byte from the file\n");
        exit(1);
    }

    free(buffer);
    fclose(file);

    return file_stat.st_size;
}

size_t read_file_mmap() {
    int fd = open(options.path, O_RDONLY);
    EXPECT_NONNEGATIVE(fd, "open");

    struct stat file_stat;
    stat(options.path, &file_stat);

    void *data = mmap(NULL, file_stat.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if(MAP_FAILED == data) {
        fprintf(stderr, "mmap failed: %s\n", strerror(errno));
        exit(1);
    }

#ifdef __linux__
    if(options.advise_willneed || options.advise_sequential) {
        posix_madvise(data, file_stat.st_size, options.advise_sequential ? POSIX_MADV_SEQUENTIAL : POSIX_MADV_WILLNEED);
    }

    if(options.use_ioprio) {
        syscall(SYS_ioprio_set, getpid(), 1, 1);
    }
#endif

    use_data(data, file_stat.st_size);

    munmap(data, file_stat.st_size);
    close(fd);

    return file_stat.st_size;
}

int main(int argc, char *argv[]) {
    parse_options(argc, argv);

    if(options.verbose) {
        cout << "File:      " << options.path << endl;
        cout << "Buffer:    " << options.buffer_size << endl;
        cout << "Type:      " << benchmark_s[options.benchmark] << endl;
        cout << "advise-sequential: " << options.advise_sequential << endl;
        cout << "advise-willneed:   " << options.advise_willneed << endl;
        cout << "use-readahead:     " << options.use_readahead << endl;
        cout << "use-ioprio:        " << options.use_ioprio << endl;
    }

    auto start = std::chrono::high_resolution_clock::now();

    size_t len = 0;
    if(options.benchmark == file_mmap) {
        len = read_file_mmap();
    } else {
        len = read_file();
    }

    auto stop = std::chrono::high_resolution_clock::now();
    double sec = ((double)std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count())/1000.0;

    cout << ((double)len)/(1024.*1024.)/sec << "" << endl;


    return 0;
}
