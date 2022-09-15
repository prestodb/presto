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

add_library(velox_temp_path TempFilePath.cpp TempDirectoryPath.cpp)

target_link_libraries(velox_temp_path velox_exception)

add_library(
  velox_exec_test_lib
  AssertQueryBuilder.cpp
  Cursor.cpp
  HiveConnectorTestBase.cpp
  OperatorTestBase.cpp
  PlanBuilder.cpp
  QueryAssertions.cpp
  SumNonPODAggregate.cpp
  TpchQueryBuilder.cpp)

target_link_libraries(
  velox_exec_test_lib
  velox_temp_path
  velox_core
  velox_exception
  velox_expression
  velox_parse_parser
  velox_duckdb_conversion
  velox_dwio_common
  velox_dwio_type_fbhive
  velox_dwio_dwrf_reader
  velox_dwio_dwrf_writer
  velox_dwio_common_test_utils
  velox_hive_connector
  velox_tpch_connector
  velox_presto_serializer
  velox_functions_prestosql)
