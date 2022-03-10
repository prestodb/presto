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
add_library(velox_function_registry FunctionRegistry.cpp)

target_link_libraries(velox_function_registry velox_expression velox_type
                      velox_core velox_exception velox_exec)

add_subdirectory(lib)
if(${VELOX_ENABLE_PRESTO_FUNCTIONS})
  add_subdirectory(prestosql)
endif()

if(${VELOX_ENABLE_SPARK_FUNCTIONS})
  add_subdirectory(sparksql)
endif()

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
