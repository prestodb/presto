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

add_executable(velox_codegen_external_process_test external_process_test.cpp)
add_test(velox_codegen_external_process_test
         velox_codegen_external_process_test)
add_dependencies(velox_codegen_external_process_test
                 velox_codegen_external_process)
target_link_libraries(
  velox_codegen_external_process_test
  gtest
  gtest_main
  ${FOLLY_WITH_DEPENDENCIES}
  ${Boost_FILESYSTEM_LIBRARIES}
  ${DOUBLE_CONVERSION}
  ${GFLAGS_LIBRARIES}
  glog::glog
  ${FMT})
