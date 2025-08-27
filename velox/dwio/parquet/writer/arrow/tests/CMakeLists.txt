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

add_executable(
  velox_dwio_arrow_parquet_writer_test
  BloomFilterTest.cpp
  ColumnReaderTest.cpp
  ColumnWriterTest.cpp
  EncodingTest.cpp
  FileDeserializeTest.cpp
  FileSerializeTest.cpp
  MetadataTest.cpp
  PageIndexTest.cpp
  PropertiesTest.cpp
  SchemaTest.cpp
  StatisticsTest.cpp
  TypesTest.cpp
)

add_test(velox_dwio_arrow_parquet_writer_test velox_dwio_arrow_parquet_writer_test)

target_link_libraries(
  velox_dwio_arrow_parquet_writer_test
  velox_dwio_arrow_parquet_writer_test_lib
  GTest::gmock
  GTest::gtest
  GTest::gtest_main
  arrow
  arrow_testing
  velox_dwio_native_parquet_reader
  velox_temp_path
)

add_library(
  velox_dwio_arrow_parquet_writer_test_lib
  BloomFilter.cpp
  BloomFilterReader.cpp
  ColumnReader.cpp
  ColumnScanner.cpp
  FileReader.cpp
  TestUtil.cpp
  XxHasher.cpp
)

target_link_libraries(
  velox_dwio_arrow_parquet_writer_test_lib
  arrow
  velox_dwio_arrow_parquet_writer_lib
  GTest::gtest
)
