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

add_library(velox_cudf_exec_test_lib CudfHiveConnectorTestBase.cpp CudfPlanBuilder.cpp)

set_target_properties(velox_cudf_exec_test_lib PROPERTIES CUDA_ARCHITECTURES native)

target_link_libraries(
  velox_cudf_exec_test_lib
  velox_vector_test_lib
  velox_temp_path
  velox_cursor
  cudf::cudf
  velox_cudf_exec
  velox_cudf_vector
  velox_core
  velox_exception
  velox_expression
  velox_parse_parser
  velox_duckdb_conversion
  velox_file_test_utils
  velox_cudf_hive_connector
  velox_aggregates
)
