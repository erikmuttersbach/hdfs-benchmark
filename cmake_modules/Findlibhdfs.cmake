if (NOT LIBHDFS_FOUND)
    include(FindPackageHandleStandardArgs)

    find_path(LIBHDFS_INCLUDE_DIR hdfs.h PATHS /usr/local/include /usr/include /usr/local/hadoop/include/)
    find_library(LIBHDFS_LIBRARY hdfs PATHS /usr/local/hadoop/lib/native)

    find_package_handle_standard_args(LIBHDFS DEFAULT_MSG LIBHDFS_LIBRARY LIBHDFS_INCLUDE_DIR)

    if (LIBHDFS_FOUND)
        add_definitions(-DLIBHDFS_FOUND)
    endif (LIBHDFS_FOUND)
endif()