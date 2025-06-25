cmake_minimum_required(VERSION 2.8.12...3.29)
project(example-c++)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_BUILD_TYPE Release)

include_directories(../../../src/include)
include_directories(../../../third_party/nlohmann)
include_directories(../../../third_party/emhash)
link_directories(../../../release/src)


file(GLOB CPP_FILES ../microbenchmark/*.cpp )
foreach(A_CPP_FILE ${CPP_FILES})
    get_filename_component(EXECUTABLE_NAME ${A_CPP_FILE} NAME_WE)
    add_executable(${EXECUTABLE_NAME} ${A_CPP_FILE})
    target_link_libraries(${EXECUTABLE_NAME} duckdb)
endforeach(A_CPP_FILE ${CPP_FILES})

