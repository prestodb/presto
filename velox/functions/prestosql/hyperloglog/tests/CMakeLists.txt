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

add_executable(velox_aggregates_hyperloglog_test DenseHllTest.cpp
                                                 SparseHllTest.cpp)

add_test(NAME velox_aggregates_hyperloglog_test
         COMMAND velox_aggregates_hyperloglog_test)

target_link_libraries(
  velox_aggregates_hyperloglog_test velox_functions_hyperloglog velox_encode
  ${gflags_LIBRARIES} ${GTEST_BOTH_LIBRARIES})
