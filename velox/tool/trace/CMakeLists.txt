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
  velox_query_trace_replayer_base
  AggregationReplayer.cpp
  FilterProjectReplayer.cpp
  HashJoinReplayer.cpp
  OperatorReplayerBase.cpp
  PartitionedOutputReplayer.cpp
  TableScanReplayer.cpp
  TableWriterReplayer.cpp
  TraceReplayRunner.cpp
  TraceReplayTaskRunner.cpp)

target_link_libraries(
  velox_query_trace_replayer_base
  velox_aggregates
  velox_type
  velox_vector
  velox_vector_test_lib
  velox_exec
  velox_exec_test_lib
  velox_dwio_parquet_reader
  velox_dwio_parquet_writer
  velox_hive_connector
  velox_tpch_connector
  velox_memory
  velox_hdfs
  velox_gcs
  velox_abfs
  velox_s3fs
  Folly::folly
  glog::glog
  gflags::gflags)

add_executable(velox_query_replayer TraceReplayerMain.cpp)

target_link_libraries(
  velox_query_replayer
  velox_query_trace_replayer_base
  velox_exec
  velox_exec_test_lib
  velox_tpch_connector)

add_library(velox_trace_file_tool_base TraceFileToolRunner.cpp)

target_link_libraries(
  velox_trace_file_tool_base
  velox_exec
  velox_type
  velox_vector
  velox_exec
  velox_hive_connector
  Folly::folly
  glog::glog
  gflags::gflags)

add_executable(velox_trace_file_tool TraceFileToolMain.cpp)

target_link_libraries(
  velox_trace_file_tool velox_trace_file_tool_base)

add_subdirectory(tests)
