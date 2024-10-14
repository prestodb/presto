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
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/storage_adapters/abfs/RegisterAbfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/gcs/RegisterGCSFileSystem.h"
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/exec/QueryTraceUtil.h"
#include "velox/tool/trace/AggregationReplayer.h"
#include "velox/tool/trace/OperatorReplayerBase.h"
#include "velox/tool/trace/PartitionedOutputReplayer.h"
#include "velox/tool/trace/TableWriterReplayer.h"
#include "velox/type/Type.h"

DEFINE_string(
    root_dir,
    "",
    "Root directory where the replayer is reading the traced data, it must be "
    "set");
DEFINE_bool(
    summary,
    false,
    "Show the summary of the tracing including number of tasks and task ids. "
    "It also print the query metadata including query configs, connectors "
    "properties, and query plan in JSON format.");
DEFINE_bool(short_summary, false, "Only show number of tasks and task ids");
DEFINE_string(
    task_id,
    "",
    "Specify the target task id, if empty, show the summary of all the traced "
    "query task.");
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
  common::Filter::registerSerDe();
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
  connector::registerConnectorFactory(
      std::make_shared<connector::hive::HiveConnectorFactory>());
  const auto hiveConnector =
      connector::getConnectorFactory("hive")->newConnector(
          "test-hive",
          std::make_shared<config::ConfigBase>(
              std::unordered_map<std::string, std::string>()),
          ioExecutor.get());
  connector::registerConnector(hiveConnector);
}

std::unique_ptr<tool::trace::OperatorReplayerBase> createReplayer() {
  std::unique_ptr<tool::trace::OperatorReplayerBase> replayer;
  if (FLAGS_operator_type == "TableWriter") {
    replayer = std::make_unique<tool::trace::TableWriterReplayer>(
        FLAGS_root_dir,
        FLAGS_task_id,
        FLAGS_node_id,
        FLAGS_pipeline_id,
        FLAGS_operator_type,
        FLAGS_table_writer_output_dir);
  } else if (FLAGS_operator_type == "Aggregation") {
    replayer = std::make_unique<tool::trace::AggregationReplayer>(
        FLAGS_root_dir,
        FLAGS_task_id,
        FLAGS_node_id,
        FLAGS_pipeline_id,
        FLAGS_operator_type);
  } else if (FLAGS_operator_type == "PartitionedOutput") {
    replayer = std::make_unique<tool::trace::PartitionedOutputReplayer>(
        FLAGS_root_dir,
        FLAGS_task_id,
        FLAGS_node_id,
        FLAGS_pipeline_id,
        FLAGS_operator_type);
  } else {
    VELOX_UNSUPPORTED("Unsupported operator type: {}", FLAGS_operator_type);
  }
  VELOX_USER_CHECK_NOT_NULL(replayer);
  return replayer;
}

void printSummary(
    const std::string& rootDir,
    const std::string& taskId,
    bool shortSummary) {
  const auto fs = filesystems::getFileSystem(rootDir, nullptr);
  const auto taskIds = exec::trace::getTaskIds(rootDir, fs);
  if (taskIds.empty()) {
    LOG(ERROR) << "No traced query task under " << rootDir;
    return;
  }

  std::ostringstream summary;
  summary << "\n++++++Query trace summary++++++\n";
  summary << "Number of tasks: " << taskIds.size() << "\n";
  summary << "Task ids: " << folly::join(",", taskIds);

  if (shortSummary) {
    LOG(INFO) << summary.str();
    return;
  }

  const auto summaryTaskIds =
      taskId.empty() ? taskIds : std::vector<std::string>{taskId};
  for (const auto& taskId : summaryTaskIds) {
    summary << "\n++++++Query configs and plan of task " << taskId
            << ":++++++\n";
    const auto traceTaskDir = fmt::format("{}/{}", rootDir, taskId);
    const auto queryMetaFile = fmt::format(
        "{}/{}",
        traceTaskDir,
        exec::trace::QueryTraceTraits::kQueryMetaFileName);
    const auto metaObj = exec::trace::getMetadata(queryMetaFile, fs);
    const auto& configObj =
        metaObj[exec::trace::QueryTraceTraits::kQueryConfigKey];
    summary << "++++++Query configs++++++\n";
    summary << folly::toJson(configObj) << "\n";
    summary << "++++++Query plan++++++\n";
    const auto queryPlan = ISerializable::deserialize<core::PlanNode>(
        metaObj[exec::trace::QueryTraceTraits::kPlanNodeKey],
        memory::MemoryManager::getInstance()->tracePool());
    summary << queryPlan->toString(true, true);
  }
  LOG(INFO) << summary.str();
}
} // namespace

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (argc == 1) {
    gflags::ShowUsageWithFlags(argv[0]);
    return -1;
  }
  if (FLAGS_root_dir.empty()) {
    gflags::SetUsageMessage("--root_dir must be provided.");
    gflags::ShowUsageWithFlags(argv[0]);
    return -1;
  }

  try {
    init();
    if (FLAGS_summary || FLAGS_short_summary) {
      printSummary(FLAGS_root_dir, FLAGS_task_id, FLAGS_short_summary);
      return 0;
    }
    createReplayer()->run();
  } catch (const VeloxException& e) {
    LOG(ERROR) << e.what();
    return -1;
  }

  return 0;
}
