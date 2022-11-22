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

find_program(THRIFT1 thrift1)
find_library(THRIFTCPP2 thriftcpp2)
find_library(THRIFT_CORE thrift-core)
find_library(THRIFT_PROTOCOL thriftprotocol)
find_library(THRIFT_METADATA thriftmetadata)
find_library(THRIFT_TRANSPORT transport)
find_path(THRIFT_INCLUDES thrift/lib/cpp2/gen/module_data_h.h PATH_SUFFIXES include REQUIRED)

include(ThriftLibrary.cmake)

thrift_library(
  presto_thrift
  PrestoThrift
  cpp2
  ""
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}/presto_cpp/main/thrift
  "..")



target_link_libraries(presto_thrift-cpp2 ${THRIFT_PROTOCOL} ${THRIFT_METADATA}
                      ${THRIFT_CORE} ${THRIFT_TRANSPORT})
set(presto_thrift_INCLUDES ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(presto_thrift-cpp2 PUBLIC ${presto_thrift_INCLUDES})
target_include_directories(presto_thrift-cpp2-obj PUBLIC ${THRIFT_INCLUDES})

add_library(presto_thrift_extra ProtocolToThrift.cpp)
target_link_libraries(presto_thrift_extra)
target_include_directories(presto_thrift_extra PUBLIC ${presto_thrift_INCLUDES} ${THRIFT_INCLUDES})
add_dependencies(presto_thrift_extra presto_thrift-cpp2)
