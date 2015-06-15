set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -msse -msse2")
set(CMAKE_C_FLAGS_RELEASE "-O3 -march=native -msse -msse2")

#set(CMAKE_BUILD_TYPE Release)

set(SOURCE_FILES Block.cpp Compare.cpp)

add_executable(hdfs_benchmark main.cpp ${SOURCE_FILES})
add_executable(q1 q1.cpp ${SOURCE_FILES})

find_package (libhdfs REQUIRED)
if (LIBHDFS_FOUND)
    include_directories(${LIBHDFS_INCLUDE_DIR})
    target_link_libraries(hdfs_benchmark ${LIBHDFS_LIBRARY})
    target_link_libraries(q1 ${LIBHDFS_LIBRARY})
endif (LIBHDFS_FOUND)

find_package(parquet REQUIRED)

# LINKER
target_link_libraries(hdfs_benchmark uuid pthread)
target_link_libraries(q1 uuid pthread)
