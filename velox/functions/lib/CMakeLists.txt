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

velox_add_library(velox_is_null_functions IsNull.cpp)

velox_link_libraries(velox_is_null_functions velox_expression)

velox_add_library(velox_functions_util LambdaFunctionUtil.cpp
                  RowsTranslationUtil.cpp)

velox_link_libraries(velox_functions_util velox_vector velox_common_base)

velox_add_library(velox_functions_lib_date_time_formatter DateTimeFormatter.cpp
                  DateTimeFormatterBuilder.cpp)

velox_link_libraries(velox_functions_lib_date_time_formatter velox_type_tz)

velox_add_library(
  velox_functions_lib
  ArrayShuffle.cpp
  CheckDuplicateKeys.cpp
  CheckNestedNulls.cpp
  KllSketch.cpp
  MapConcat.cpp
  Re2Functions.cpp
  Repeat.cpp
  StringEncodingUtils.cpp
  SubscriptUtil.cpp
  TimeUtils.cpp)

velox_link_libraries(
  velox_functions_lib
  velox_functions_util
  velox_vector
  velox_type_tz
  re2::re2
  Folly::folly)

add_subdirectory(aggregates)
add_subdirectory(string)
add_subdirectory(window)
if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${VELOX_ENABLE_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()
