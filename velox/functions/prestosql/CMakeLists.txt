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
add_subdirectory(json)
add_subdirectory(types)
add_subdirectory(window)

add_library(
  velox_functions_prestosql_impl OBJECT
  ArrayConstructor.cpp
  ArrayContains.cpp
  ArrayDistinct.cpp
  ArrayDuplicates.cpp
  ArrayIntersectExcept.cpp
  ArrayPosition.cpp
  ArraySort.cpp
  ArraySum.cpp
  Comparisons.cpp
  DecimalArithmetic.cpp
  ElementAt.cpp
  FilterFunctions.cpp
  FromUnixTime.cpp
  GreatestLeast.cpp
  InPredicate.cpp
  Map.cpp
  MapConcat.cpp
  MapEntries.cpp
  MapKeysAndValues.cpp
  Not.cpp
  Reduce.cpp
  Reverse.cpp
  RowFunction.cpp
  Slice.cpp
  Split.cpp
  StringFunctions.cpp
  Subscript.cpp
  ToUtf8.cpp
  Transform.cpp
  TransformKeys.cpp
  TransformValues.cpp
  URLFunctions.cpp
  VectorArithmetic.cpp
  WidthBucketArray.cpp
  Zip.cpp)

target_link_libraries(
  velox_functions_prestosql_impl
  velox_common_hyperloglog
  velox_functions_json
  velox_functions_lib
  velox_expression
  md5
  velox_type_tz
  velox_presto_types)

set_property(TARGET velox_functions_prestosql_impl PROPERTY JOB_POOL_COMPILE
                                                            high_memory_pool)

if(${VELOX_ENABLE_AGGREGATES})
  add_subdirectory(aggregates)
endif()

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
  add_subdirectory(benchmarks)
  add_subdirectory(coverage)
endif()

add_subdirectory(registration)
