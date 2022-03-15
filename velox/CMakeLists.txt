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

# VELOX BASE
add_subdirectory(buffer)
add_subdirectory(common)
add_subdirectory(core)
add_subdirectory(serializers)
add_subdirectory(type)
add_subdirectory(vector)
add_subdirectory(row)
add_subdirectory(flag_definitions)
add_subdirectory(external/date)
add_subdirectory(external/md5)
#

# examples depend on expression
if(${VELOX_ENABLE_EXAMPLES} AND ${VELOX_ENABLE_EXPRESSION})
  add_subdirectory(examples)
endif()

if(${VELOX_ENABLE_BENCHMARKS} OR ${VELOX_ENABLE_BENCHMARKS_BASIC})
  add_subdirectory(benchmarks)
endif()

if(${VELOX_ENABLE_EXPRESSION})
  add_subdirectory(expression)
endif()

if(${VELOX_ENABLE_PARSE})
  add_subdirectory(parse)
endif()

# hive connector depends on dwio
if(${VELOX_ENABLE_HIVE_CONNECTOR})
  add_subdirectory(dwio)
endif()

add_subdirectory(functions) # depends on md5 (postgresql)
add_subdirectory(connectors)

# exec depends on codegen
if(${VELOX_ENABLE_EXEC})
  add_subdirectory(exec)
  add_subdirectory(codegen)
endif()

if(${VELOX_ENABLE_DUCKDB})
  add_subdirectory(duckdb)
  add_subdirectory(external/duckdb)
endif()

if(${VELOX_CODEGEN_SUPPORT})
  add_subdirectory(experimental/codegen)
endif()

# substrait converter
if(${VELOX_ENABLE_SUBSTRAIT})
  add_subdirectory(substrait)
endif()
