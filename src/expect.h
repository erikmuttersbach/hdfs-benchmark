#define EXPECT_NONZERO(r, func) if(r==NULL) { \
                                    fprintf(stderr, "%s failed: %s\n", func, strerror(errno)); \
                                    exit(1); \
                                }

#define EXPECT_NONNEGATIVE(r, func) if(r < 0) { \
                                    fprintf(stderr, "%s failed: %s\n", func, strerror(errno)); \
                                    exit(1); \
                                }

