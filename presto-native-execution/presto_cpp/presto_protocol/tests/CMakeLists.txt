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

add_executable(
  presto_protocol_test
  Base64Test.cpp
  CallExpressionTest.cpp
  ConstantExpressionTest.cpp
  DataSizeTest.cpp
  DomainTest.cpp
  DurationTest.cpp
  LifespanTest.cpp
  MapWithIntegerKeysTest.cpp
  OptionalTest.cpp
  RowExpressionTest.cpp
  TaskUpdateRequestTest.cpp
  TupleDomainTest.cpp
  TypeErrorTest.cpp
  VariableReferenceExpressionTest.cpp
  PlanFragmentTest.cpp)
add_test(
  NAME presto_protocol_test
  COMMAND presto_protocol_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
add_dependencies(presto_protocol_test presto_protocol)

target_link_libraries(
  presto_protocol_test
  gtest
  gtest_main
  $<TARGET_OBJECTS:presto_protocol>
  velox_type
  velox_encode
  velox_exception
  velox_vector
  Boost::filesystem
  ${RE2}
  ${FOLLY_LIBRARIES}
  ${DOUBLE_CONVERSION}
  ${GLOG}
  ${GFLAGS_LIBRARIES}
  pthread)
