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
add_library(
  velox_functions_spark
  ArraySort.cpp
  CompareFunctionsNullSafe.cpp
  Hash.cpp
  In.cpp
  LeastGreatest.cpp
  Map.cpp
  RegexFunctions.cpp
  Register.cpp
  RegisterArithmetic.cpp
  RegisterCompare.cpp
  Size.cpp
  SplitFunctions.cpp
  String.cpp
  Subscript.cpp)

target_link_libraries(
  velox_functions_spark velox_functions_lib velox_functions_prestosql_impl
  velox_is_null_functions velox_functions_util ${FOLLY_WITH_DEPENDENCIES})

set_property(TARGET velox_functions_spark PROPERTY JOB_POOL_COMPILE
                                                   high_memory_pool)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
  add_subdirectory(aggregates)
endif()

if(${VELOX_ENABLE_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()
