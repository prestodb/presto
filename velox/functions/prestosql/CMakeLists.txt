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

add_library(
  velox_functions_prestosql OBJECT
  ArrayConstructor.cpp
  ArrayContains.cpp
  ArrayDistinct.cpp
  ArrayIntersectExcept.cpp
  ArrayMinMax.cpp
  Cardinality.cpp
  Coalesce.cpp
  CoreFunctions.cpp
  ElementAt.cpp
  FilterFunctions.cpp
  FromUnixTime.cpp
  InPredicate.cpp
  IsNull.cpp
  Length.cpp
  Map.cpp
  MapConcat.cpp
  MapEntries.cpp
  MapKeysAndValues.cpp
  Not.cpp
  Reduce.cpp
  RegisterArithmetic.cpp
  RegisterCheckedArithmetic.cpp
  RegisterComparisons.cpp
  Reverse.cpp
  RowFunction.cpp
  Split.cpp
  StringFunctions.cpp
  Subscript.cpp
  ToUtf8.cpp
  Transform.cpp
  VectorArithmetic.cpp
  VectorFunctions.cpp
  WidthBucketArray.cpp)

target_link_libraries(
  velox_functions_prestosql
  velox_exec
  velox_functions_json
  velox_functions_lib
  velox_expression
  md5
  velox_type_tz)

set_property(TARGET velox_functions_prestosql PROPERTY JOB_POOL_COMPILE
                                                       high_memory_pool)

add_subdirectory(tests)
add_subdirectory(benchmarks)
add_subdirectory(coverage)
