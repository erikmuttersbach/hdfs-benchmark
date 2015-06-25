set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(CMAKE_CXX_FLAGS_RELEASE "-g -O3 -march=native -msse -msse2")
set(CMAKE_C_FLAGS_RELEASE "-g -O3 -march=native -msse -msse2")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fno-inline-functions")
set(CMAKE_C_FLAGS_DEBUG "-g -O0 -fno-inline-functions")

add_executable(hdfs_reader main.cpp)

find_package(libhdfs REQUIRED)

include_directories(${LIBHDFS_INCLUDE_DIR})
target_link_libraries(hdfs_reader ${LIBHDFS_LIBRARY} uuid pthread)


