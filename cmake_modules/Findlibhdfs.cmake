if (NOT LIBHDFS_FOUND)
    include(FindPackageHandleStandardArgs)

    find_path(LIBHDFS_INCLUDE_DIR hdfs/hdfs.h PATHS /usr/local/include /usr/include /usr/local/hadoop/include/)
    find_library(LIBHDFS_LIBRARY hdfs PATHS /usr/local/hadoop/lib/native /usr/lib/hadoop/lib/native/)
    if(NOT LIBHDFS_LIBRARY)
        find_path(LIBHDFS_INCLUDE_DIR hdfs/hdfs.h /usr/local/include)
        find_library(LIBHDFS_LIBRARY hdfs3 /usr/local/lib)
    endif()

    find_package_handle_standard_args(LIBHDFS DEFAULT_MSG LIBHDFS_LIBRARY LIBHDFS_INCLUDE_DIR)

    message(STATUS "libhdfs:")
    message(STATUS "  LIBHDFS_INCLUDE_DIR=${LIBHDFS_INCLUDE_DIR}")
    message(STATUS "  LIBHDFS_LIBRARY=${LIBHDFS_LIBRARY}")

    if(LIBHDFS_LIBRARY MATCHES "libhdfs3")
        message(STATUS "  using libhdfs3")
        add_definitions(-DHAS_LIBHDFS3=1)
    else()
        message(STATUS "  using Hadoop libhdfs")
        add_definitions(-DHAS_LIBHDFS=1)
    endif()

    if (LIBHDFS_FOUND)
        add_definitions(-DLIBHDFS_FOUND)
    endif (LIBHDFS_FOUND)
endif()