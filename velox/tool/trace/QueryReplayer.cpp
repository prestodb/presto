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

#include <folly/executors/IOThreadPoolExecutor.h>
#include <gflags/gflags.h>

#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/storage_adapters/abfs/RegisterAbfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/gcs/RegisterGCSFileSystem.h"
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/tool/trace/AggregationReplayer.h"
#include "velox/tool/trace/OperatorReplayerBase.h"
#include "velox/tool/trace/TableWriterReplayer.h"
#include "velox/type/Type.h"

DEFINE_bool(usage, false, "Show the usage");
DEFINE_string(root, "", "Root dir of the query tracing");
DEFINE_bool(summary, false, "Show the summary of the tracing");
DEFINE_bool(short_summary, false, "Only show number of tasks and task ids");
DEFINE_string(
    task_id,
    "",
    "Specify the target task id, if empty, show the summary of all the traced query task.");
DEFINE_string(node_id, "", "Specify the target node id.");
DEFINE_int32(pipeline_id, 0, "Specify the target pipeline id.");
DEFINE_string(operator_type, "", "Specify the target operator type.");
DEFINE_string(
    table_writer_output_dir,
    "",
    "Specify output directory of TableWriter.");
DEFINE_double(
    hiveConnectorExecutorHwMultiplier,
    2.0,
    "Hardware multipler for hive connector.");

using namespace facebook::velox;

namespace {
void init() {
  memory::initializeMemoryManager({});
  filesystems::registerLocalFileSystem();
  filesystems::registerS3FileSystem();
  filesystems::registerHdfsFileSystem();
  filesystems::registerGCSFileSystem();
  filesystems::abfs::registerAbfsFileSystem();
  Type::registerSerDe();
  core::PlanNode::registerSerDe();
  core::ITypedExpr::registerSerDe();
  exec::registerPartitionFunctionSerDe();
  connector::hive::HiveTableHandle::registerSerDe();
  connector::hive::LocationHandle::registerSerDe();
  connector::hive::HiveColumnHandle::registerSerDe();
  connector::hive::HiveInsertTableHandle::registerSerDe();
  if (!isRegisteredVectorSerde()) {
    serializer::presto::PrestoVectorSerde::registerVectorSerde();
  }
  // TODO: make it configurable.
  const auto ioExecutor = std::make_unique<folly::IOThreadPoolExecutor>(
      std::thread::hardware_concurrency() *
      FLAGS_hiveConnectorExecutorHwMultiplier);
  const auto hiveConnector =
      connector::getConnectorFactory("hive")->newConnector(
          "test-hive",
          std::make_shared<config::ConfigBase>(
              std::unordered_map<std::string, std::string>()),
          ioExecutor.get());
  connector::registerConnector(hiveConnector);
}

std::unique_ptr<tool::trace::OperatorReplayerBase> createReplayer(
    const std::string& operatorType) {
  std::unique_ptr<tool::trace::OperatorReplayerBase> replayer = nullptr;
  if (operatorType == "TableWriter") {
    replayer = std::make_unique<tool::trace::TableWriterReplayer>(
        FLAGS_root,
        FLAGS_task_id,
        FLAGS_node_id,
        FLAGS_pipeline_id,
        FLAGS_operator_type,
        FLAGS_table_writer_output_dir);
  } else if (operatorType == "Aggregation") {
    replayer = std::make_unique<tool::trace::AggregationReplayer>(
        FLAGS_root,
        FLAGS_task_id,
        FLAGS_node_id,
        FLAGS_pipeline_id,
        FLAGS_operator_type);
  } else {
    VELOX_FAIL("Unsupported opeartor type: {}", FLAGS_operator_type);
  }
  VELOX_USER_CHECK_NOT_NULL(replayer);
  return replayer;
}
} // namespace

int main(int argc, char** argv) {
  if (argc == 1) {
    LOG(ERROR) << "\n" << tool::trace::OperatorReplayerBase::usage();
    return 1;
  }

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_usage) {
    LOG(INFO) << "\n" << tool::trace::OperatorReplayerBase::usage();
    return 0;
  }

  if (FLAGS_root.empty()) {
    LOG(ERROR) << "Root dir is not provided!\n"
               << tool::trace::OperatorReplayerBase::usage();
    return 1;
  }

  init();
  if (FLAGS_summary || FLAGS_short_summary) {
    tool::trace::OperatorReplayerBase::printSummary(
        FLAGS_root, FLAGS_task_id, FLAGS_short_summary);
    return 0;
  }

  const auto replayer = createReplayer(FLAGS_operator_type);
  VELOX_USER_CHECK_NOT_NULL(
      replayer, "Unsupported opeartor type: {}", FLAGS_operator_type);

  replayer->run();

  return 0;
}
