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

velox_add_library(
  velox_query_trace_exec
  QueryMetadataWriter.cpp
  QueryTraceConfig.cpp
  QueryDataWriter.cpp
  QueryTraceUtil.cpp)

velox_link_libraries(
  velox_query_trace_exec
  PRIVATE
    velox_common_io
    velox_file
    velox_core
    velox_vector
    velox_connector
    velox_common_base
    velox_presto_serializer)

velox_add_library(velox_query_trace_retrieve QueryDataReader.cpp
                  QueryMetadataReader.cpp)

velox_link_libraries(
  velox_query_trace_retrieve
  velox_common_io
  velox_file
  velox_core
  velox_vector
  velox_connector
  velox_common_base
  velox_hive_connector)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(test)
endif()
