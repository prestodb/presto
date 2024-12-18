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

velox_add_library(velox_test_util ScopedTestTime.cpp TestValue.cpp
                  RandomSeed.cpp)

velox_link_libraries(
  velox_test_util
  PUBLIC velox_exception
  PRIVATE glog::glog Folly::folly)

if(${VELOX_BUILD_TESTING})
  velox_add_library(velox_test_output_matcher OutputMatcher.cpp)
  velox_link_libraries(velox_test_output_matcher PUBLIC Folly::folly
                                                        GTest::gtest re2::re2)

  add_subdirectory(tests)
endif()
