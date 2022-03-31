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

add_executable(velox_transform_utils_test TestUtils.cpp ../ranges_utils.h)

add_test(velox_transform_utils_test velox_transform_utils_test)
# target_compile_features(velox_transform_utils_test INTERFACE cxx_std_20)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(velox_transform_utils_test
                         PUBLIC -fconcepts-diagnostics-depth=6)
endif()

target_link_libraries(
  velox_transform_utils_test
  velox_transform_utils
  ${ANTLR4_RUNTIME}
  ${Boost_ATOMIC_LIBRARIES}
  ${Boost_CONTEXT_LIBRARIES}
  ${Boost_DATE_TIME_LIBRARIES}
  ${Boost_FILESYSTEM_LIBRARIES}
  ${Boost_PROGRAM_OPTIONS_LIBRARIES}
  ${Boost_REGEX_LIBRARIES}
  ${Boost_THREAD_LIBRARIES}
  ${Boost_SYSTEM_LIBRARIES}
  ${gflags_LIBRARIES}
  gtest
  gtest_main
  ${FOLLY_WITH_DEPENDENCIES}
  ${DOUBLE_CONVERSION}
  ${GFLAGS_LIBRARIES}
  glog::glog
  ${FMT}
  dl)
