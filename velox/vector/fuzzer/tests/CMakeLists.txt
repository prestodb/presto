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
add_executable(velox_vector_fuzzer_test VectorFuzzerTest.cpp)

add_test(velox_vector_fuzzer_test velox_vector_fuzzer_test)

target_link_libraries(
  velox_vector_fuzzer_test
  velox_vector_fuzzer
  velox_presto_types
  velox_constrained_input_generators
  velox_memory
  GTest::gtest
  GTest::gtest_main
  GTest::gmock
)

add_executable(velox_constrained_vector_generator_test ConstrainedVectorGeneratorTest.cpp)

add_test(
  NAME velox_constrained_vector_generator_test
  COMMAND velox_constrained_vector_generator_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
  velox_constrained_vector_generator_test
  velox_constrained_vector_generator
  velox_vector_test_lib
  GTest::gtest
  GTest::gtest_main
  GTest::gmock
)
