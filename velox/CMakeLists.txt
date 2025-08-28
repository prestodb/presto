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
include_directories(external/xxhash)
add_subdirectory(buffer)
add_subdirectory(common)
add_subdirectory(core)
add_subdirectory(serializers)
add_subdirectory(type)
add_subdirectory(vector)
add_subdirectory(row)
add_subdirectory(flag_definitions)
add_subdirectory(external/date)
add_subdirectory(external/tzdb)
add_subdirectory(external/md5)
add_subdirectory(external/hdfs)
#

# examples depend on expression
if(${VELOX_ENABLE_EXAMPLES} AND ${VELOX_ENABLE_EXPRESSION})
  add_subdirectory(examples)
endif()

add_subdirectory(benchmarks)

if(${VELOX_ENABLE_EXPRESSION})
  add_subdirectory(expression)
endif()

if(${VELOX_ENABLE_PARSE})
  add_subdirectory(parse)
endif()

# hive connector depends on dwio
if(${VELOX_BUILD_MINIMAL_WITH_DWIO} OR ${VELOX_ENABLE_HIVE_CONNECTOR})
  add_subdirectory(dwio)
endif()

if(${VELOX_ENABLE_TPCH_CONNECTOR})
  add_subdirectory(tpch/gen)
endif()

if(${VELOX_ENABLE_TPCDS_CONNECTOR})
  add_subdirectory(tpcds/gen)
endif()

add_subdirectory(functions) # depends on md5 (postgresql)
add_subdirectory(connectors)

if(${VELOX_ENABLE_EXEC})
  add_subdirectory(exec)
endif()

if(${VELOX_ENABLE_DUCKDB})
  add_subdirectory(duckdb)
endif()

if(VELOX_ENABLE_WAVE OR VELOX_ENABLE_CUDF)
  add_subdirectory(experimental/gpu)
  if(${VELOX_ENABLE_CUDF})
    add_subdirectory(experimental/cudf)
  endif()
  if(${VELOX_ENABLE_WAVE})
    add_subdirectory(experimental/wave)
    add_subdirectory(external/jitify)
  endif()
endif()

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tool)
endif()

if(VELOX_BUILD_PYTHON_PACKAGE)
  add_subdirectory(python)
endif()
