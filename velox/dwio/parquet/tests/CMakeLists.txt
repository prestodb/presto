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

set(
  TEST_LINK_LIBS
  velox_dwio_common_test_utils
  velox_vector_test_lib
  velox_exec_test_lib
  velox_dwio_parquet_reader
  velox_dwio_parquet_writer
  velox_temp_path
  GTest::gtest
  GTest::gtest_main
  GTest::gmock
  gflags::gflags
  glog::glog
)

add_subdirectory(common)
add_subdirectory(reader)
add_subdirectory(thrift)
add_subdirectory(writer)

add_executable(velox_dwio_parquet_tpch_test ParquetTpchTest.cpp)
add_test(
  NAME velox_dwio_parquet_tpch_test
  COMMAND velox_dwio_parquet_tpch_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(
  velox_dwio_parquet_tpch_test
  velox_tpch_connector
  velox_aggregates
  velox_tpch_gen
  ${TEST_LINK_LIBS}
)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/examples DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
