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

pyvelox_add_module(
  legacy
  MODULE
  legacy/complex.cpp
  legacy/conversion.cpp
  legacy/pyvelox.cpp
  legacy/serde.cpp
  legacy/signatures.cpp)

target_link_libraries(
  legacy
  PRIVATE
    velox_type
    velox_vector
    velox_core
    velox_exec
    velox_parse_parser
    velox_functions_prestosql
    velox_functions_spark
    velox_aggregates
    velox_functions_spark_aggregates)

if(VELOX_PYTHON_LEGACY_ONLY)
  # Only build the legacy module for CI
  return()
endif()

add_library(velox_py_init init/PyInit.cpp)

target_link_libraries(
  velox_py_init
  PRIVATE velox_memory)
# This should not be necessary but the xxhash header is not part of any target
target_include_directories(velox_py_init
                           PUBLIC ${PROJECT_SOURCE_DIR}/velox/external/xxhash)

# pyvelox.type library:
add_library(velox_py_type_lib type/PyType.cpp)
target_link_libraries(
  velox_py_type_lib velox_type pybind11::module)

pyvelox_add_module(type MODULE type/type.cpp)
target_link_libraries(
  type
  PRIVATE velox_py_type_lib)

# pyvelox.file library:
add_library(velox_py_file_lib file/PyFile.cpp)
target_link_libraries(
  velox_py_file_lib velox_dwio_common velox_py_init pybind11::module)

pyvelox_add_module(file MODULE file/file.cpp)
target_link_libraries(
  file
  PRIVATE velox_py_file_lib)

# pyvelox.vector library:
add_library(velox_py_vector_lib vector/PyVector.cpp)
target_link_libraries(
  velox_py_vector_lib velox_vector pybind11::module)

pyvelox_add_module(vector MODULE vector/vector.cpp)
target_link_libraries(
  vector
  PRIVATE velox_py_vector_lib)

# pyvelox.plan_builder library:
add_library(velox_py_plan_builder_lib plan_builder/PyPlanBuilder.cpp)
target_include_directories(velox_py_plan_builder_lib
                           PUBLIC ${PROJECT_SOURCE_DIR}/velox/external/xxhash)
target_link_libraries(
  velox_py_plan_builder_lib
  velox_py_type_lib
  velox_py_vector_lib
  velox_py_init
  velox_vector
  velox_core
  velox_cursor
  velox_hive_connector
  velox_aggregates
  velox_functions_prestosql
  velox_parse_expression
  velox_exec_test_lib
  velox_tpch_connector
  velox_dwio_dwrf_reader
  velox_dwio_dwrf_common
  velox_dwio_dwrf_writer
  Folly::folly
  pybind11::module)

pyvelox_add_module(plan_builder MODULE plan_builder/plan_builder.cpp)
target_link_libraries(
  plan_builder
  PRIVATE velox_py_plan_builder_lib)

# pyvelox.runner library:
add_library(velox_py_local_runner_lib runner/PyLocalRunner.cpp
                                      runner/PyConnectors.cpp)
target_include_directories(velox_py_local_runner_lib
                           PUBLIC ${PROJECT_SOURCE_DIR}/velox/external/xxhash)
target_link_libraries(
  velox_py_local_runner_lib
  velox_py_type_lib
  velox_py_vector_lib
  velox_py_init
  velox_vector
  velox_core
  velox_cursor
  velox_hive_connector
  velox_exec_test_lib
  velox_dwio_dwrf_reader
  velox_dwio_dwrf_common
  velox_dwio_dwrf_writer
  pybind11::module)

pyvelox_add_module(runner MODULE runner/runner.cpp)
target_link_libraries(
  runner
  PRIVATE velox_py_local_runner_lib)

# pyarrow has to be installed
find_package(pyarrow REQUIRED)

# 'arrow' target already exists
pyvelox_add_module(arrow_module MODULE arrow/arrow.cpp)
set_target_properties(
  arrow_module
  PROPERTIES OUTPUT_NAME "arrow")

# This ensures the dynamic linker can find libarrow* dependencies at runtime
# from the installed pyarrow package without requiring LD_LIBRARY_PATH to be set
get_rpath_origin(_origin)
set_property(
  TARGET arrow_module
  PROPERTY INSTALL_RPATH "${_origin}/../pyarrow"
  APPEND)

target_link_libraries(
  arrow_module
  PRIVATE
    velox_py_init
    velox_py_vector_lib
    velox_arrow_bridge
    pyarrow::dev
    pybind11::module)
