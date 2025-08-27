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

add_library(velox_vector_fuzzer_util Utils.cpp)

target_link_libraries(velox_vector_fuzzer_util velox_vector velox_common_fuzzer_util)

add_library(velox_vector_fuzzer GeneratorSpec.cpp VectorFuzzer.cpp)

target_link_libraries(
  velox_vector_fuzzer
  velox_type
  velox_vector
  velox_vector_fuzzer_util
  velox_common_fuzzer_util
  velox_presto_types
  velox_constrained_vector_generator
)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  target_compile_options(velox_vector_fuzzer PRIVATE -Wno-deprecated-declarations)
endif()

add_library(velox_constrained_vector_generator ConstrainedVectorGenerator.cpp)

target_link_libraries(
  velox_constrained_vector_generator
  velox_vector
  velox_expression
  velox_constrained_input_generators
  velox_vector_fuzzer_util
)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
