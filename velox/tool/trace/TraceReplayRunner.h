/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <utility>
#include "velox/common/file/FileSystems.h"
#include "velox/tool/trace/OperatorReplayerBase.h"

#include <folly/executors/IOThreadPoolExecutor.h>
#include "velox/exec/TaskTraceReader.h"

DECLARE_string(root_dir);
DECLARE_bool(summary);
DECLARE_bool(short_summary);
DECLARE_string(query_id);
DECLARE_string(task_id);
DECLARE_string(node_id);
DECLARE_int32(driver_id);
DECLARE_string(driver_ids);
DECLARE_string(table_writer_output_dir);
DECLARE_double(hive_connector_executor_hw_multiplier);
DECLARE_int32(shuffle_serialization_format);
DECLARE_uint64(query_memory_capacity_mb);
DECLARE_double(driver_cpu_executor_hw_multiplier);
DECLARE_string(memory_arbitrator_type);
DECLARE_bool(copy_results);
DECLARE_string(function_prefix);

namespace facebook::velox::tool::trace {

/// The trace replay runner. It is configured through a set of gflags passed
/// from replayer tool command line.
class TraceReplayRunner {
 public:
  TraceReplayRunner();
  virtual ~TraceReplayRunner() = default;

  /// Initializes the trace replay runner by setting the velox runtime
  /// environment for the trace replay. It is invoked before run().
  virtual void init();

  /// Runs the trace replay with a set of gflags passed from replayer tool.
  virtual void run();

 protected:
  std::unique_ptr<tool::trace::OperatorReplayerBase> createReplayer() const;

  const std::unique_ptr<folly::CPUThreadPoolExecutor> cpuExecutor_;
  const std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
  std::shared_ptr<filesystems::FileSystem> fs_;
  std::unique_ptr<exec::trace::TaskTraceMetadataReader>
      taskTraceMetadataReader_;
  core::PlanNodePtr tracePlanNode_;
};

} // namespace facebook::velox::tool::trace
