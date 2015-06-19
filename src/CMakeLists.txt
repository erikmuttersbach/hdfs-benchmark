set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -msse -msse2")
set(CMAKE_C_FLAGS_RELEASE "-O3 -march=native -msse -msse2")

#set(CMAKE_BUILD_TYPE Release)

set(SOURCE_FILES Block.cpp Compare.cpp ParquetFile.cpp)

add_executable(hdfs_benchmark main.cpp ${SOURCE_FILES})
add_executable(q1 q1.cpp ${SOURCE_FILES})
add_executable(q14 q14.cpp ${SOURCE_FILES})

find_package(libhdfs REQUIRED)
find_package(parquet REQUIRED)

include_directories(${LIBHDFS_INCLUDE_DIR} ${PARQUET_INCLUDE_DIRS})

set(LIBRARIES ${LIBHDFS_LIBRARY} ${PARQUET_LIBRARIES} thrift)
target_link_libraries(hdfs_benchmark ${LIBRARIES})
target_link_libraries(q1 ${LIBRARIES})
target_link_libraries(q14 ${LIBRARIES})

# LINKER
target_link_libraries(hdfs_benchmark uuid pthread)
target_link_libraries(q1 uuid pthread)
