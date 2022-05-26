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

add_library(
  velox_vector
  BaseVector.cpp
  ComplexVector.cpp
  ConstantVector.cpp
  DecodedVector.cpp
  FlatVector.cpp
  LazyVector.cpp
  SelectivityVector.cpp
  SequenceVector.cpp
  VectorEncoding.cpp
  VectorStream.cpp)

target_link_libraries(velox_vector velox_encode velox_memory velox_time
                      velox_type velox_buffer)

add_subdirectory(arrow)
add_subdirectory(fuzzer)

if(${VELOX_BUILD_TESTING} OR ${VELOX_ENABLE_BENCHMARKS_BASIC})
  add_subdirectory(tests)
  add_subdirectory(benchmarks)
endif()
