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
add_library(velox_vector_test_lib VectorMaker.cpp VectorTestBase.cpp)

target_link_libraries(velox_vector_test_lib velox_vector)

add_executable(
  velox_vector_test
  VectorMakerTest.cpp
  VectorTest.cpp
  VectorEstimateFlatSizeTest.cpp
  VectorPrepareForReuseTest.cpp
  DecodedVectorTest.cpp
  SelectivityVectorTest.cpp
  EnsureWritableVectorTest.cpp
  MayHaveNullsRecursiveTest.cpp)

add_test(velox_vector_test velox_vector_test)

target_link_libraries(
  velox_vector_test
  velox_vector_test_lib
  velox_buffer
  velox_vector
  velox_serialization
  velox_memory
  velox_presto_serializer
  ${Boost_ATOMIC_LIBRARIES}
  ${Boost_CONTEXT_LIBRARIES}
  ${Boost_DATE_TIME_LIBRARIES}
  ${Boost_FILESYSTEM_LIBRARIES}
  ${Boost_PROGRAM_OPTIONS_LIBRARIES}
  ${Boost_REGEX_LIBRARIES}
  ${Boost_THREAD_LIBRARIES}
  ${Boost_SYSTEM_LIBRARIES}
  gtest
  gtest_main
  ${FOLLY_WITH_DEPENDENCIES}
  ${DOUBLE_CONVERSION}
  ${gflags_LIBRARIES}
  glog::glog
  ${FMT}
  dl)

add_executable(simple_vector_test SimpleVectorTest.cpp ToStringUtilityTest.cpp)

add_test(simple_vector_test simple_vector_test)

target_link_libraries(
  simple_vector_test
  velox_vector
  velox_buffer
  velox_type
  velox_vector_test_lib
  gtest
  gtest_main
  ${FOLLY_WITH_DEPENDENCIES}
  ${gflags_LIBRARIES}
  glog::glog
  ${FMT})

add_executable(bias_vector_test BiasVectorTest.cpp)

add_test(bias_vector_test bias_vector_test)

target_link_libraries(
  bias_vector_test
  velox_vector
  Boost::filesystem
  gtest
  gtest_main
  ${FOLLY_WITH_DEPENDENCIES}
  ${gflags_LIBRARIES}
  glog::glog
  ${FMT})
