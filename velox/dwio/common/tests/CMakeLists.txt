# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(utils)
add_library(velox_dwio_faulty_file_sink FaultyFileSink.cpp)
target_link_libraries(
  velox_dwio_faulty_file_sink velox_file_test_utils velox_dwio_common)

# There is an issue with the VTT symbol for the InlineExecutor from folly when
# building on Linux with Clang15. It is not created and results in a SEGV when
# attempting to construct the InlineExecutor. nm output shows no address for the
# "v VTT for folly::InlineExecutor" symbol while for a debug build it shows
# "000000000185d3e0 V VTT for folly::InlineExecutor" symbol. It seems to have
# been optimized away by Clang15 and may be some kind of bug. Changing the
# optimization level to 0 results in proper creation/linkage and successful
# execution of the test. Review if this is still necessary when upgrading Clang.
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set_property(
    SOURCE ParallelForTest.cpp
    APPEND
    PROPERTY COMPILE_OPTIONS -O0)
endif()

add_executable(
  velox_dwio_common_test
  BitConcatenationTest.cpp
  BitPackDecoderTest.cpp
  ChainedBufferTests.cpp
  ColumnSelectorTests.cpp
  DataBufferTests.cpp
  DecoderUtilTest.cpp
  ExecutorBarrierTest.cpp
  OnDemandUnitLoaderTests.cpp
  LocalFileSinkTest.cpp
  MemorySinkTest.cpp
  LoggedExceptionTest.cpp
  MeasureTimeTests.cpp
  ParallelForTest.cpp
  RangeTests.cpp
  ReadFileInputStreamTests.cpp
  ReaderTest.cpp
  RetryTests.cpp
  TestBufferedInput.cpp
  ThrottlerTest.cpp
  TypeTests.cpp
  UnitLoaderToolsTests.cpp
  WriterTest.cpp
  OptionsTests.cpp)
add_test(velox_dwio_common_test velox_dwio_common_test)
target_link_libraries(
  velox_dwio_common_test
  velox_dwio_common_test_utils
  velox_temp_path
  velox_vector_test_lib
  Boost::regex
  velox_link_libs
  Folly::folly
  ${TEST_LINK_LIBS}
  GTest::gtest
  GTest::gtest_main
  GTest::gmock
  glog::glog
  fmt::fmt
  protobuf::libprotobuf)

if(VELOX_ENABLE_BENCHMARKS)
  add_executable(velox_dwio_common_data_buffer_benchmark
                 DataBufferBenchmark.cpp)

  target_link_libraries(
    velox_dwio_common_data_buffer_benchmark
    velox_dwio_common
    velox_memory
    velox_dwio_common_exception
    Folly::folly
    Folly::follybenchmark)

  add_executable(velox_dwio_common_int_decoder_benchmark
                 IntDecoderBenchmark.cpp)
  target_link_libraries(
    velox_dwio_common_int_decoder_benchmark
    velox_dwio_common_exception
    velox_exception
    velox_dwio_dwrf_common
    Folly::folly
    Folly::follybenchmark)

  if(VELOX_ENABLE_ARROW)
    add_subdirectory(Lemire/FastPFor)
    add_executable(velox_dwio_common_bitpack_decoder_benchmark
                   BitPackDecoderBenchmark.cpp)

    target_compile_options(velox_dwio_common_bitpack_decoder_benchmark
                           PRIVATE -Wno-deprecated-declarations)

    target_link_libraries(
      velox_dwio_common_bitpack_decoder_benchmark
      velox_dwio_common
      arrow
      velox_fastpforlib
      duckdb_static
      Folly::folly
      Folly::follybenchmark)
  endif()
endif()
