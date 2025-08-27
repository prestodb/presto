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

add_executable(velox_s3config_test S3ConfigTest.cpp)
add_test(velox_s3config_test velox_s3config_test)
target_link_libraries(
  velox_s3config_test
  velox_common_config
  velox_s3fs
  GTest::gtest
  GTest::gtest_main
)

add_executable(velox_s3file_test S3FileSystemTest.cpp S3UtilTest.cpp)
add_test(velox_s3file_test velox_s3file_test)
target_link_libraries(
  velox_s3file_test
  velox_file
  velox_s3fs
  velox_core
  velox_exec_test_lib
  velox_dwio_common_exception
  velox_exec
  GTest::gtest
  GTest::gtest_main
)

add_executable(velox_s3registration_test S3FileSystemRegistrationTest.cpp)
add_test(velox_s3registration_test velox_s3registration_test)
target_link_libraries(
  velox_s3registration_test
  velox_file
  velox_s3fs
  velox_core
  velox_exec_test_lib
  velox_dwio_parquet_reader
  velox_dwio_common_exception
  velox_exec
  GTest::gtest
  GTest::gtest_main
)

add_executable(velox_s3finalize_test S3FileSystemFinalizeTest.cpp)
add_test(velox_s3finalize_test velox_s3finalize_test)
target_link_libraries(
  velox_s3finalize_test
  velox_s3fs
  velox_file
  velox_core
  GTest::gtest
  GTest::gtest_main
)

add_executable(velox_s3insert_test S3InsertTest.cpp)
add_test(velox_s3insert_test velox_s3insert_test)
target_link_libraries(
  velox_s3insert_test
  velox_file
  velox_s3fs
  velox_core
  velox_exec_test_lib
  velox_dwio_parquet_writer
  velox_dwio_parquet_reader
  velox_dwio_common_exception
  velox_exec
  GTest::gtest
  GTest::gtest_main
)

add_executable(velox_s3read_test S3ReadTest.cpp)
add_test(
  NAME velox_s3read_test
  COMMAND velox_s3read_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(
  velox_s3read_test
  velox_file
  velox_s3fs
  velox_core
  velox_exec_test_lib
  velox_dwio_parquet_reader
  velox_dwio_common_exception
  velox_exec
  GTest::gtest
  GTest::gtest_main
)

add_executable(velox_s3metrics_test S3FileSystemMetricsTest.cpp)
add_test(velox_s3metrics_test velox_s3metrics_test)
target_link_libraries(
  velox_s3metrics_test
  velox_s3fs
  velox_exec_test_lib
  GTest::gtest
  GTest::gtest_main
)

add_executable(velox_s3multiendpoints_test S3MultipleEndpointsTest.cpp)
add_test(velox_s3multiendpoints_test velox_s3multiendpoints_test)
target_link_libraries(
  velox_s3multiendpoints_test
  velox_file
  velox_s3fs
  velox_core
  velox_exec_test_lib
  velox_dwio_parquet_reader
  velox_dwio_parquet_writer
  velox_dwio_common_exception
  velox_exec
  GTest::gtest
  GTest::gtest_main
)
