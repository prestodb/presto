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

add_executable(velox_dwio_parquet_page_reader_test ParquetPageReaderTest.cpp)
add_test(
  NAME velox_dwio_parquet_page_reader_test
  COMMAND velox_dwio_parquet_page_reader_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(
  velox_dwio_parquet_page_reader_test
  velox_dwio_native_parquet_reader
  velox_link_libs
  ${TEST_LINK_LIBS}
)

add_executable(velox_parquet_e2e_filter_test E2EFilterTest.cpp)
add_test(velox_parquet_e2e_filter_test velox_parquet_e2e_filter_test)
target_link_libraries(
  velox_parquet_e2e_filter_test
  velox_dwio_common_test_utils
  velox_dwio_parquet_writer
  velox_dwio_native_parquet_reader
  lz4::lz4
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS}
)

add_library(velox_dwio_parquet_reader_benchmark_lib ParquetReaderBenchmark.cpp)
target_link_libraries(
  velox_dwio_parquet_reader_benchmark_lib
  velox_dwio_parquet_reader
  velox_dwio_parquet_writer
  velox_exec_test_lib
  velox_exec
  velox_hive_connector
  ${TEST_LINK_LIBS}
  Folly::follybenchmark
  Folly::folly
)

if(VELOX_ENABLE_BENCHMARKS)
  add_executable(velox_dwio_parquet_reader_benchmark ParquetReaderBenchmarkMain.cpp)
  target_link_libraries(velox_dwio_parquet_reader_benchmark velox_dwio_parquet_reader_benchmark_lib)
endif()

add_executable(
  velox_dwio_parquet_reader_test
  ParquetReaderTest.cpp
  ParquetReaderBenchmarkTest.cpp
  BloomFilterTest.cpp
)
add_test(
  NAME velox_dwio_parquet_reader_test
  COMMAND velox_dwio_parquet_reader_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(
  velox_dwio_parquet_reader_test
  velox_dwio_parquet_common
  velox_dwio_parquet_reader_benchmark_lib
  velox_link_libs
)

add_executable(velox_dwio_parquet_structure_decoder_test NestedStructureDecoderTest.cpp)
add_test(
  NAME velox_dwio_parquet_structure_decoder_test
  COMMAND velox_dwio_parquet_structure_decoder_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(
  velox_dwio_parquet_structure_decoder_test
  velox_dwio_native_parquet_reader
  velox_link_libs
  ${TEST_LINK_LIBS}
)

if(VELOX_ENABLE_BENCHMARKS)
  add_executable(velox_dwio_parquet_structure_decoder_benchmark NestedStructureDecoderBenchmark.cpp)
  target_link_libraries(
    velox_dwio_parquet_structure_decoder_benchmark
    velox_dwio_native_parquet_reader
    Folly::folly
    Folly::follybenchmark
  )
endif()

add_executable(velox_dwio_parquet_table_scan_test ParquetTableScanTest.cpp)
add_test(
  NAME velox_dwio_parquet_table_scan_test
  COMMAND velox_dwio_parquet_table_scan_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(
  velox_dwio_parquet_table_scan_test
  velox_dwio_parquet_reader
  velox_dwio_parquet_writer
  velox_exec_test_lib
  velox_exec
  velox_hive_connector
  velox_link_libs
  velox_type_tz
  ${TEST_LINK_LIBS}
)

if(${VELOX_ENABLE_ARROW})
  add_executable(velox_dwio_parquet_rlebp_decoder_test RleBpDecoderTest.cpp)
  add_test(
    NAME velox_dwio_parquet_rlebp_decoder_test
    COMMAND velox_dwio_parquet_rlebp_decoder_test
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  )
  target_link_libraries(
    velox_dwio_parquet_rlebp_decoder_test
    velox_dwio_native_parquet_reader
    arrow
    velox_link_libs
    ${TEST_LINK_LIBS}
  )
endif()
