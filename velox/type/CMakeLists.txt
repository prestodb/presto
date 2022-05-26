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
if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
add_subdirectory(tz)

add_library(
  velox_type
  StringView.h
  Type.cpp
  Variant.cpp
  Timestamp.cpp
  TimestampConversion.cpp
  Date.cpp
  Filter.cpp
  LongDecimal.cpp
  Subfield.cpp
  Tokenizer.cpp)

target_link_libraries(
  velox_type
  velox_type_tz
  velox_common_base
  velox_encode
  velox_exception
  velox_serialization
  velox_external_date
  ${Boost_REGEX_LIBRARIES}
  ${FOLLY_WITH_DEPENDENCIES})
