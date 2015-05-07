if (NOT LIBHADOOP_FOUND)
    include(FindPackageHandleStandardArgs)

    find_library(LIBHADOOP_LIBRARY hadoop PATHS /usr/local/hadoop/lib/native /usr/lib/hadoop/lib/native/)

    find_package_handle_standard_args(LIBHADOOP DEFAULT_MSG LIBHADOOP_LIBRARY)
endif()