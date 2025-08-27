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

velox_add_library(velox_common_fuzzer_util Utils.cpp)

velox_link_libraries(velox_common_fuzzer_util velox_type velox_exception)

velox_add_library(velox_constrained_input_generators ConstrainedGenerators.cpp)

velox_link_libraries(
  velox_constrained_input_generators
  Folly::folly
  velox_type
  velox_common_fuzzer_util
  velox_functions_prestosql
)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
