if (NOT THRIFT_FOUND)
    include(FindPackageHandleStandardArgs)

    find_path(THRIFT_INCLUDE_DIR thrift/Thrift.h PATHS /usr/local/include /usr/include ${THRIFT_HOME}/include)
    find_library(THRIFT_LIBRARY thrift PATHS /usr/lib /usr/local/lib ${THRIFT_HOME}/lib)

    find_package_handle_standard_args(THRIFT DEFAULT_MSG THRIFT_LIBRARY THRIFT_INCLUDE_DIR)
endif()
