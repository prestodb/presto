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
velox_add_library(
  velox_presto_types
  BingTileRegistration.cpp
  BingTileType.cpp
  GeometryRegistration.cpp
  HyperLogLogRegistration.cpp
  IPAddressRegistration.cpp
  IPPrefixRegistration.cpp
  JsonCastOperator.cpp
  JsonRegistration.cpp
  TDigestRegistration.cpp
  TimestampWithTimeZoneRegistration.cpp
  UuidRegistration.cpp)

velox_link_libraries(
  velox_presto_types
  velox_type
  velox_memory
  velox_expression
  velox_functions_util
  velox_functions_json
  velox_functions_lib_date_time_formatter
  velox_constrained_input_generators
  velox_presto_types_fuzzer_utils)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()

add_subdirectory(fuzzer_utils)
