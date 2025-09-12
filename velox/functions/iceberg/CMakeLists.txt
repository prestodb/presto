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

velox_add_library(velox_functions_iceberg_hash Murmur3Hash32.cpp)

velox_link_libraries(velox_functions_iceberg_hash velox_common_base velox_functions_util)

velox_add_library(velox_functions_iceberg BucketFunction.cpp Register.cpp Truncate.cpp)

velox_link_libraries(
  velox_functions_iceberg
  velox_functions_iceberg_hash
  velox_functions_lib
  Folly::folly
)

if(NOT VELOX_MONO_LIBRARY)
  set_property(TARGET velox_functions_iceberg PROPERTY JOB_POOL_COMPILE high_memory_pool)
endif()

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
