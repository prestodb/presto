cmake_minimum_required(VERSION 3.0.0)
cmake_policy(SET CMP0063 NEW)

project(TPCHExtension)

include_directories(dbgen/include)
include_directories(include)
include_directories(..)
add_subdirectory(dbgen)

add_library(tpch_extension STATIC tpch-extension.cpp)
target_link_libraries(tpch_extension dbgen)
