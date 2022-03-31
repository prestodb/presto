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

if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  add_executable(velox_codegen_compiler_utils_test compiler_utils_test.cpp
                                                   macOSDefinitions.cpp)
else()
  add_executable(velox_codegen_compiler_utils_test compiler_utils_test.cpp
                                                   circleCIDefinitions.cpp)
endif()

add_test(velox_codegen_compiler_utils_test velox_codegen_compiler_utils_test)
add_dependencies(velox_codegen_compiler_utils_test velox_codegen_compiler_utils
                 velox_transform_utils velox_codegen_proto velox_exception)
target_link_libraries(
  velox_codegen_compiler_utils_test
  velox_codegen_compiler_utils
  velox_transform_utils
  velox_codegen_proto
  velox_exception
  ${Boost_FILESYSTEM_LIBRARIES}
  gtest
  gtest_main
  ${FOLLY_WITH_DEPENDENCIES}
  ${DOUBLE_CONVERSION}
  ${GFLAGS_LIBRARIES}
  glog::glog
  ${FMT})
