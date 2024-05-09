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
  gflags::gflags
  gtest
  gtest_main
  gmock
  glog::glog
  fmt::fmt)

add_executable(velox_dwio_common_data_buffer_benchmark DataBufferBenchmark.cpp)

target_link_libraries(
  velox_dwio_common_data_buffer_benchmark velox_dwio_common velox_memory
  velox_dwio_common_exception Folly::folly ${FOLLY_BENCHMARK})

add_executable(velox_dwio_common_int_decoder_benchmark IntDecoderBenchmark.cpp)
target_link_libraries(
  velox_dwio_common_int_decoder_benchmark velox_dwio_common_exception
  velox_exception velox_dwio_dwrf_common Folly::folly ${FOLLY_BENCHMARK})

if(VELOX_ENABLE_ARROW AND VELOX_ENABLE_BENCHMARKS)
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
    ${FOLLY_BENCHMARK})
endif()
