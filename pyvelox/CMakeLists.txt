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
  include_directories(SYSTEM ${CMAKE_SOURCE_DIR})
  add_definitions(-DCREATE_PYVELOX_MODULE -DVELOX_DISABLE_GOOGLETEST)
  # Define our Python module:
  pybind11_add_module(
    pyvelox
    MODULE
    pyvelox.cpp
    serde.cpp
    signatures.cpp
    complex.cpp
    conversion.cpp)
  # Link with Velox:
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

  install(TARGETS pyvelox LIBRARY DESTINATION .)
else()
  # Torcharrow will not use pyvelox as an extension module for compatibility
  # reasons.
  message("Creating pyvelox library")
  add_library(pyvelox pyvelox.cpp pyvelox.h)
  target_link_libraries(pyvelox velox_type pybind11::module)
endif()
