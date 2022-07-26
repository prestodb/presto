cmake_minimum_required(VERSION 3.0.0)
cmake_policy(SET CMP0063 NEW)

project(dbgen CXX C)

include_directories(include)

add_definitions(-DDBNAME=dss -DMAC -DORACLE -DTPCH)

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set_source_files_properties(
    build.cpp dbgen.cpp permute.cpp
    PROPERTIES COMPILE_FLAGS -Wno-writable-strings)
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set_source_files_properties(
    build.cpp dbgen.cpp permute.cpp
    PROPERTIES COMPILE_FLAGS -Wno-write-strings)
endif()

add_library(
  dbgen STATIC
  dbgen.cpp
  dbgen_gunk.cpp
  bm_utils.cpp
  build.cpp
  permute.cpp
  rnd.cpp
  rng64.cpp
  speed_seed.cpp
  text.cpp)
