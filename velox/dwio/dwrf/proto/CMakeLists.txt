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

add_custom_command(
  OUTPUT dwrf_proto.pb.cc dwrf_proto.pb.h
  COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR} && ${PROTOBUF_PROTOC_EXECUTABLE}
          dwrf_proto.proto --cpp_out=${CMAKE_CURRENT_BINARY_DIR})

add_library(velox_dwio_dwrf_proto dwrf_proto.pb.cc)

# Access generated proto file with.
#
# #include "velox/dwio/dwrf/proto/dwrf_proto.pb.h"
target_link_libraries(velox_dwio_dwrf_proto ${Protobuf_LIBRARIES})
target_include_directories(velox_dwio_dwrf_proto
                           PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/../../../../)
