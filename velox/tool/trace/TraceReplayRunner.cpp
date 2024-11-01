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

#include "velox/tool/trace/TraceReplayRunner.h"

#include <gflags/gflags.h>

#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/storage_adapters/abfs/RegisterAbfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/gcs/RegisterGCSFileSystem.h"
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/core/PlanNode.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/OperatorTraceReader.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/exec/TaskTraceReader.h"
#include "velox/exec/TraceUtil.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/tool/trace/AggregationReplayer.h"
#include "velox/tool/trace/FilterProjectReplayer.h"
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
DEFINE_string(query_id, "", "Specify the target query id which must be set");
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

namespace facebook::velox::tool::trace {
namespace {

std::unique_ptr<tool::trace::OperatorReplayerBase> createReplayer() {
  std::unique_ptr<tool::trace::OperatorReplayerBase> replayer;
  if (FLAGS_operator_type == "TableWriter") {
    VELOX_USER_CHECK(
        !FLAGS_table_writer_output_dir.empty(),
        "--table_writer_output_dir is required");
    replayer = std::make_unique<tool::trace::TableWriterReplayer>(
        FLAGS_root_dir,
        FLAGS_query_id,
        FLAGS_task_id,
        FLAGS_node_id,
        FLAGS_pipeline_id,
        FLAGS_operator_type,
        FLAGS_table_writer_output_dir);
  } else if (FLAGS_operator_type == "Aggregation") {
    replayer = std::make_unique<tool::trace::AggregationReplayer>(
        FLAGS_root_dir,
        FLAGS_query_id,
        FLAGS_task_id,
        FLAGS_node_id,
        FLAGS_pipeline_id,
        FLAGS_operator_type);
  } else if (FLAGS_operator_type == "PartitionedOutput") {
    replayer = std::make_unique<tool::trace::PartitionedOutputReplayer>(
        FLAGS_root_dir,
        FLAGS_query_id,
        FLAGS_task_id,
        FLAGS_node_id,
        FLAGS_pipeline_id,
        FLAGS_operator_type);
  } else if (FLAGS_operator_type == "FilterProject") {
    replayer = std::make_unique<tool::trace::FilterProjectReplayer>(
        FLAGS_root_dir,
        FLAGS_query_id,
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

void printTaskMetadata(
    const std::string& taskTraceDir,
    memory::MemoryPool* pool,
    std::ostringstream& oss) {
  auto taskMetaReader = std::make_unique<exec::trace::TaskTraceMetadataReader>(
      taskTraceDir, pool);
  std::unordered_map<std::string, std::string> queryConfigs;
  std::unordered_map<std::string, std::unordered_map<std::string, std::string>>
      connectorProperties;
  core::PlanNodePtr queryPlan;
  taskMetaReader->read(queryConfigs, connectorProperties, queryPlan);

  oss << "\n++++++Query configs++++++\n";
  for (const auto& queryConfigEntry : queryConfigs) {
    oss << "\t" << queryConfigEntry.first << ": " << queryConfigEntry.second
        << "\n";
  }
  oss << "\n++++++Connector configs++++++\n";
  for (const auto& connectorPropertyEntry : connectorProperties) {
    oss << connectorPropertyEntry.first << "\n";
    for (const auto& propertyEntry : connectorPropertyEntry.second) {
      oss << "\t" << propertyEntry.first << ": " << propertyEntry.second
          << "\n";
    }
  }
  oss << "\n++++++Task query plan++++++\n";
  oss << queryPlan->toString(true, true);
}

void printTaskTraceSummary(
    const std::string& traceDir,
    const std::string& queryId,
    const std::string& taskId,
    const std::string& nodeId,
    uint32_t pipelineId,
    memory::MemoryPool* pool,
    std::ostringstream& oss) {
  auto fs = filesystems::getFileSystem(traceDir, nullptr);
  const auto taskTraceDir =
      exec::trace::getTaskTraceDirectory(traceDir, queryId, taskId);

  const std::vector<uint32_t> driverIds = exec::trace::listDriverIds(
      exec::trace::getNodeTraceDirectory(taskTraceDir, nodeId), pipelineId, fs);
  oss << "\n++++++Task " << taskId << "++++++\n";
  for (const auto& driverId : driverIds) {
    const auto opTraceDir = exec::trace::getOpTraceDirectory(
        taskTraceDir, nodeId, pipelineId, driverId);
    const auto opTraceSummary =
        exec::trace::OperatorTraceSummaryReader(
            exec::trace::getOpTraceDirectory(
                taskTraceDir, nodeId, pipelineId, driverId),
            pool)
            .read();
    oss << driverId << " driver, " << opTraceSummary.toString() << "\n";
  }
}

void printSummary(
    const std::string& rootDir,
    const std::string& queryId,
    const std::string& taskId,
    bool shortSummary,
    memory::MemoryPool* pool) {
  const std::string queryDir =
      exec::trace::getQueryTraceDirectory(rootDir, queryId);
  const auto fs = filesystems::getFileSystem(queryDir, nullptr);
  const auto taskIds = exec::trace::getTaskIds(rootDir, queryId, fs);
  VELOX_USER_CHECK(!taskIds.empty(), "No task found under {}", rootDir);

  std::ostringstream summary;
  summary << "\n++++++Query trace summary++++++\n";
  summary << "Number of tasks: " << taskIds.size() << "\n";
  if (shortSummary) {
    summary << "Task ids: " << folly::join("\n", taskIds);
    LOG(INFO) << summary.str();
    return;
  }

  const auto summaryTaskIds =
      taskId.empty() ? taskIds : std::vector<std::string>{taskId};
  printTaskMetadata(
      exec::trace::getTaskTraceDirectory(rootDir, queryId, summaryTaskIds[0]),
      pool,
      summary);
  summary << "\n++++++Task Summaries++++++\n";
  for (const auto& taskId : summaryTaskIds) {
    printTaskTraceSummary(
        rootDir,
        queryId,
        taskId,
        FLAGS_node_id,
        FLAGS_pipeline_id,
        pool,
        summary);
  }
  LOG(INFO) << summary.str();
}
} // namespace

TraceReplayRunner::TraceReplayRunner()
    : ioExecutor_(std::make_unique<folly::IOThreadPoolExecutor>(
          std::thread::hardware_concurrency() *
              FLAGS_hiveConnectorExecutorHwMultiplier,
          std::make_shared<folly::NamedThreadFactory>(
              "TraceReplayIoConnector"))) {}

void TraceReplayRunner::init() {
  memory::initializeMemoryManager({});
  filesystems::registerLocalFileSystem();
  filesystems::registerS3FileSystem();
  filesystems::registerHdfsFileSystem();
  filesystems::registerGCSFileSystem();
  filesystems::abfs::registerAbfsFileSystem();

  dwio::common::registerFileSinks();
  dwrf::registerDwrfReaderFactory();
  dwrf::registerDwrfWriterFactory();
  parquet::registerParquetReaderFactory();
  parquet::registerParquetWriterFactory();

  core::PlanNode::registerSerDe();
  core::ITypedExpr::registerSerDe();
  common::Filter::registerSerDe();
  Type::registerSerDe();
  exec::registerPartitionFunctionSerDe();
  if (!isRegisteredVectorSerde()) {
    serializer::presto::PrestoVectorSerde::registerVectorSerde();
  }
  connector::hive::HiveTableHandle::registerSerDe();
  connector::hive::LocationHandle::registerSerDe();
  connector::hive::HiveColumnHandle::registerSerDe();
  connector::hive::HiveInsertTableHandle::registerSerDe();
  connector::hive::HiveConnectorSplit::registerSerDe();
  connector::hive::registerHivePartitionFunctionSerDe();
  connector::hive::HiveBucketProperty::registerSerDe();

  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();

  connector::registerConnectorFactory(
      std::make_shared<connector::hive::HiveConnectorFactory>());
  const auto hiveConnector =
      connector::getConnectorFactory("hive")->newConnector(
          "test-hive",
          std::make_shared<config::ConfigBase>(
              std::unordered_map<std::string, std::string>()),
          ioExecutor_.get());
  connector::registerConnector(hiveConnector);
}

void TraceReplayRunner::run() {
  VELOX_USER_CHECK(!FLAGS_root_dir.empty(), "--root_dir must be provided");
  VELOX_USER_CHECK(!FLAGS_query_id.empty(), "--query_id must be provided");
  VELOX_USER_CHECK(!FLAGS_node_id.empty(), "--node_id must be provided");

  if (FLAGS_summary || FLAGS_short_summary) {
    auto pool = memory::memoryManager()->addLeafPool("replayer");
    printSummary(
        FLAGS_root_dir,
        FLAGS_query_id,
        FLAGS_task_id,
        FLAGS_short_summary,
        pool.get());
    return;
  }

  VELOX_USER_CHECK(
      !FLAGS_operator_type.empty(), "--operator_type must be provided");
  createReplayer()->run();
}
} // namespace facebook::velox::tool::trace
