cmake_minimum_required(VERSION 3.10)

# set the project name
project(PrestoCpp)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -mavx2 -mfma -mavx -mf16c -masm=intel -mlzcnt")

set(Boost_USE_MULTITHREADED TRUE)
find_package(Boost 1.72.0 REQUIRED program_options context filesystem regex thread system date_time atomic)
include_directories(${Boost_INCLUDE_DIRS})

find_package(GTest REQUIRED)
find_package(gflags COMPONENTS shared)
find_library(GLOG glog)
find_library(FOLLY folly)
include_directories(${GTEST_INCLUDE_DIRS})

find_package(Arrow REQUIRED)
include_directories(${ARROW_INCLUDE_DIRS})

find_package(double-conversion REQUIRED)

find_library(FMT fmt)

find_package(folly REQUIRED)
include_directories(${FOLLY_INCLUDE_DIRS})

include_directories(third_party)
include_directories(src)

include(CTest)              # include after project() but before add_subdirectory()

add_subdirectory(scripts)
add_subdirectory(src)
