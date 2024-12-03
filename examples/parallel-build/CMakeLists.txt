cmake_minimum_required(VERSION 2.8.12...3.29)
project(benchmark)

set(CMAKE_CXX_STANDARD 11)

include_directories(/opt/intel/oneapi/vtune/latest/include)
link_libraries(/opt/intel/oneapi/vtune/latest/lib64/libittnotify.a)

include_directories(../duckdb/src/include)
link_directories(../duckdb/build/release/src)

add_compile_options(-g)

add_executable(benchmark main.cpp)
target_link_libraries(benchmark duckdb)
