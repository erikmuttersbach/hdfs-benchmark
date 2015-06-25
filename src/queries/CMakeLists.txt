set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(CMAKE_CXX_FLAGS_RELEASE "-g -O3 -march=native -msse -msse2")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fno-inline-functions")
set(CMAKE_C_FLAGS_RELEASE "-g -O3 -march=native -msse -msse2")

set(SOURCE_FILES Block.cpp Compare.cpp ParquetFile.cpp ColumnChunk.cpp sha256.cpp RowGroup.cpp)

add_executable(q1 q1.cpp ${SOURCE_FILES})
add_executable(q14 q14.cpp ${SOURCE_FILES})
add_executable(q17 q17.cpp ${SOURCE_FILES})

find_package(libhdfs REQUIRED)
find_package(parquet REQUIRED)
find_package(thrift REQUIRED)
find_package(Boost REQUIRED COMPONENTS thread log log_setup system)

add_definitions(-DBOOST_LOG_DYN_LINK=1)

include_directories(${LIBHDFS_INCLUDE_DIR} ${PARQUET_INCLUDE_DIRS} ${THRIFT_INCLUDE_DIR})

set(LIBRARIES ${LIBHDFS_LIBRARY} ${PARQUET_LIBRARIES} ${THRIFT_LIBRARY} ${Boost_LIBRARIES} uuid pthread)
target_link_libraries(q1 ${LIBRARIES})
target_link_libraries(q14 ${LIBRARIES})
target_link_libraries(q17 ${LIBRARIES})
