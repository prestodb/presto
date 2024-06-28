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

# Set up Proto
file(
  GLOB PROTO_FILES
  RELATIVE ${PROJECT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/*.proto)
foreach(PROTO ${PROTO_FILES})
  get_filename_component(PROTO_DIR ${PROTO} DIRECTORY)
  get_filename_component(PROTO_NAME ${PROTO} NAME_WE)
  list(APPEND PROTO_SRCS
       "${PROJECT_BINARY_DIR}/${PROTO_DIR}/${PROTO_NAME}.pb.cc")
  list(APPEND PROTO_HDRS
       "${PROJECT_BINARY_DIR}/${PROTO_DIR}/${PROTO_NAME}.pb.h")
  list(APPEND PROTO_FILES_FULL
       "${PROJECT_SOURCE_DIR}/${PROTO_DIR}/${PROTO_NAME}.proto")
endforeach()
set(PROTO_OUTPUT_FILES ${PROTO_HDRS} ${PROTO_SRCS})
set_source_files_properties(${PROTO_OUTPUT_FILES} PROPERTIES GENERATED TRUE)

# Ensure that the option --proto_path is not given an empty argument
foreach(PROTO_PATH ${CMAKE_SOURCE_DIR} ${Protobuf_INCLUDE_DIRS})
  list(APPEND PROTO_PATH_ARGS --proto_path=${PROTO_PATH})
endforeach()

add_custom_command(
  OUTPUT ${PROTO_OUTPUT_FILES}
  COMMAND protobuf::protoc ${PROTO_PATH_ARGS} --cpp_out ${CMAKE_BINARY_DIR}
          ${PROTO_FILES_FULL}
  DEPENDS protobuf::protoc
  COMMENT "Running PROTO compiler"
  VERBATIM)
add_custom_target(dwio_proto ALL DEPENDS ${PROTO_OUTPUT_FILES})

add_library(velox_dwio_dwrf_proto ${PROTO_HDRS} ${PROTO_SRCS})

# Access generated proto file with.
#
# #include "velox/dwio/dwrf/proto/dwrf_proto.pb.h"
target_link_libraries(velox_dwio_dwrf_proto protobuf::libprotobuf)
target_include_directories(velox_dwio_dwrf_proto PUBLIC ${PROJECT_BINARY_DIR})
