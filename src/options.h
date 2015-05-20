#include <getopt.h>

typedef enum {
    undefined = 0, standard, scr, zcr
} type_t;

typedef struct {
    const char *path = NULL;
    const char *socket = "/var/lib/hadoop-hdfs/dn_socket";
    const char *namenode = "localhost";
    int namenode_port = 9000;
    size_t buffer_size = 4096;
    int verbose = false;
    int skip_checksums = false;
    int sample = false;
    type_t type = type_t::undefined;
} options_t;

void print_usage() {
    printf("hdfs_benchmark -f FILE [-b BUFFER_SIZE] [-t TYPE] [-n NAMENODE] [-p NAMENODE_PORT]\n"
                   "  -f, --file           File to read\n"
                   "  -b, --buffer         Buffer size, defaults to 4096\n"
                   "  -t, --type           One of standard, sc, zcr\n"
                   "  -n, --namenode       Namenode hostname, default: localhost\n"
                   "  -p, --namenode-port  Namenode port, default: 9000\n"
                   "  -v, --verbose        Verbose output, e.g. statistics, formatted speed\n"
                   "  -x, --sample         Sample the copy speed every 1s\n");
}

options_t parse_options(int argc, char *argv[]) {
    options_t options;

    static struct option options_config[] = {
            {"file",   required_argument, 0,                'f'},
            {"buffer", optional_argument, 0,                'b'},
            {"help",   optional_argument, 0,                'h'},
            {"type",   optional_argument, 0,                't'},
            {"socket", optional_argument, 0,                's'},
            {"namenode", optional_argument, 0,              'n'},
            {"namenode-port", optional_argument, 0,         'p'},
            {"verbose",      no_argument,       &options.verbose, 'v'},
            {"sample",      no_argument,       &options.sample, 'x'},
            {"skip-checksums",     no_argument, &options.skip_checksums, 1},

            {0, 0,                        0,                0}
    };

    int c = 0;
    while (c >= 0) {
        int option_index;
        c = getopt_long(argc, argv, "f:b:n:p:t:s:v", options_config, &option_index);

        switch (c) {
            case 'v':
                options.verbose = true;
                break;
            case 'f':
                options.path = optarg;
                break;
            case 'b':
                options.buffer_size = atoi(optarg);
                break;
            case 'n':
                options.namenode = optarg;
                break;
            case 'p':
                options.namenode_port = atoi(optarg);
                break;
            case 'h':
                print_usage();
                exit(1);
            case 's':
                options.socket = optarg;
                break;
            case 'x':
                options.sample = true;
                break;
            case 't':
                if(strcmp(optarg, "standard") == 0) {
                    options.type = type_t::standard;
                } else  if(strcmp(optarg, "scr") == 0) {
                    options.type = type_t::scr;
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

    return options;
}