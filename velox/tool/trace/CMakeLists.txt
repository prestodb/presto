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

velox_add_library(query_trace_replayer_base QueryTraceReplayer.cpp)
velox_link_libraries(
  query_trace_replayer_base
  velox_query_trace_retrieve
  velox_aggregates
  velox_type
  velox_vector
  velox_vector_test_lib
  velox_exec
  velox_exec_test_lib
  velox_tpch_connector
  velox_memory
  Folly::folly
  glog::glog
  gflags::gflags)

add_executable(query_replayer QueryReplayer.cpp)

target_link_libraries(
  query_replayer query_trace_replayer_base)
