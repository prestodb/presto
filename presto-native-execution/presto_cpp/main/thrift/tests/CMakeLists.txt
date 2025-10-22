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
  thrift_io_test
  ThriftIOTest.cpp)

add_test(
  NAME thrift_io_test
  COMMAND thrift_io_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_include_directories(thrift_io_test PRIVATE
  ${presto_thrift_INCLUDES}
  ${presto_native_INCLUDES}
  ${THRIFT_INCLUDES}
  ${GLOG_INCLUDE_DIR})

target_link_libraries(
  thrift_io_test
  presto_thrift_extra
  presto_thrift-cpp2
  presto_native-cpp2
  ${THRIFTCPP2}
  ${THRIFT_PROTOCOL}
  ${THRIFT_METADATA}
  ${THRIFT_CORE}
  ${THRIFT_TRANSPORT}
  ${FOLLY_WITH_DEPENDENCIES}
  ${RE2}
  GTest::gmock
  GTest::gtest
  GTest::gtest_main)

set_property(TARGET thrift_io_test PROPERTY JOB_POOL_LINK
                                             presto_link_job_pool)
