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

if(VELOX_BUILD_PYTHON_PACKAGE)
  message("Creating pyvelox module")
  # Define our Python module:
  pybind11_add_module(
    pyvelox
    MODULE
    complex.cpp
    conversion.cpp
    pyvelox.cpp
    serde.cpp
    signatures.cpp)

  target_link_libraries(
    pyvelox
    PRIVATE velox_type
            velox_vector
            velox_core
            velox_exec
            velox_functions_prestosql
            velox_parse_parser
            velox_functions_prestosql
            velox_functions_spark)

  target_include_directories(pyvelox SYSTEM
                             PRIVATE ${CMAKE_CURRENT_LIST_DIR}/..)
  target_compile_definitions(pyvelox PRIVATE -DCREATE_PYVELOX_MODULE)
  install(TARGETS pyvelox LIBRARY DESTINATION .)
endif()
