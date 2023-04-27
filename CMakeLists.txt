cmake_minimum_required(VERSION 3.16)

project (MapReduce)

# 设置C++标准为 C++ 11
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -W -Wall -Wextra -g")

include_directories(include)

#produce dynamic lib
add_library(mrFunc SHARED src/utility/map_reduceFun.cpp)

# master
add_executable(master  src/master.cpp src/utility/logRAII.cpp)
target_link_libraries(master log4cpp pthread zmq)

# worker
add_executable(worker  src/worker.cpp src/utility/logRAII.cpp)
target_link_libraries(worker log4cpp pthread dl zmq )

# exe file output dir
SET (EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin) 

# library output path
set(LIBRARY_OUTPUT_PATH  ${PROJECT_SOURCE_DIR}/lib)