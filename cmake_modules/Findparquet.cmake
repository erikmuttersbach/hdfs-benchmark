if (NOT PARQUET_FOUND)
    include(FindPackageHandleStandardArgs)

    find_path(PARQUET_INCLUDE_DIR parquet/parquet.h PATHS /usr/local/include /usr/include)
    find_library(PARQUET_LIBRARY Parquet PATHS /usr/local/lib /usr/lib)

    find_package_handle_standard_args(PARQUET DEFAULT_MSG PARQUET_LIBRARY PARQUET_INCLUDE_DIR)

    if (PARQUET_FOUND)
        add_definitions(-DPARQUET_FOUND)
    endif (PARQUET_FOUND)
endif()