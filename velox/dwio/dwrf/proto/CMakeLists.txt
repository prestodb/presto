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

protobuf_generate_cpp(VELOX_DWIO_DWRF_PROTO_SRCS VELOX_DWIO_DWRF_PROTO_HDRS
                      dwrf_proto.proto)

protobuf_generate_cpp(VELOX_DWIO_ORC_PROTO_SRCS VELOX_DWIO_ORC_PROTO_HDRS
                      orc_proto.proto)

add_library(
  velox_dwio_dwrf_proto
  ${VELOX_DWIO_DWRF_PROTO_SRCS} ${VELOX_DWIO_DWRF_PROTO_HDRS}
  ${VELOX_DWIO_ORC_PROTO_SRCS} ${VELOX_DWIO_ORC_PROTO_HDRS})

# Access generated proto file with.
#
# #include "velox/dwio/dwrf/proto/dwrf_proto.pb.h"
target_link_libraries(velox_dwio_dwrf_proto protobuf::libprotobuf)
target_include_directories(velox_dwio_dwrf_proto
                           PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/../../../../)
