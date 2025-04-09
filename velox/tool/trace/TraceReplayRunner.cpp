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
#include "velox/connectors/hive/storage_adapters/gcs/RegisterGcsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/core/PlanNode.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/exec/OperatorTraceReader.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/exec/TaskTraceReader.h"
#include "velox/exec/TraceUtil.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/CompactRowSerializer.h"
#include "velox/serializers/UnsafeRowSerializer.h"
#include "velox/tool/trace/AggregationReplayer.h"
#include "velox/tool/trace/FilterProjectReplayer.h"
#include "velox/tool/trace/HashJoinReplayer.h"
#include "velox/tool/trace/OperatorReplayerBase.h"
#include "velox/tool/trace/PartitionedOutputReplayer.h"
#include "velox/tool/trace/TableScanReplayer.h"
#include "velox/tool/trace/TableWriterReplayer.h"
#include "velox/type/Type.h"

#ifdef VELOX_ENABLE_PARQUET
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#endif

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
DEFINE_string(driver_ids, "", "A comma-separated list of target driver ids");
DEFINE_string(
    table_writer_output_dir,
    "",
    "Specify output directory of TableWriter.");
DEFINE_double(
    hive_connector_executor_hw_multiplier,
    2.0,
    "Hardware multipler for hive connector.");
DEFINE_double(
    driver_cpu_executor_hw_multiplier,
    2.0,
    "Hardware multipler for driver cpu executor.");
DEFINE_int32(
    shuffle_serialization_format,
    0,
    "Specify the shuffle serialization format, 0: presto columnar, 1: compact row, 2: spark unsafe row.");
DEFINE_string(
    memory_arbitrator_type,
    "shared",
    "Specify the memory arbitrator type.");
DEFINE_uint64(
    query_memory_capacity_mb,
    0,
    "Specify the query memory capacity limit in GB. If it is zero, then there is no limit.");
DEFINE_bool(copy_results, false, "Copy the replaying results.");
DEFINE_string(
    function_prefix,
    "",
    "Prefix for the scalar and aggregate functions.");

namespace facebook::velox::tool::trace {
namespace {
VectorSerde::Kind getVectorSerdeKind() {
  switch (FLAGS_shuffle_serialization_format) {
    case 0:
      return VectorSerde::Kind::kPresto;
    case 1:
      return VectorSerde::Kind::kCompactRow;
    case 2:
      return VectorSerde::Kind::kUnsafeRow;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported shuffle serialization format: {}",
          static_cast<int>(FLAGS_shuffle_serialization_format));
  }
}

void printTaskMetadata(
    const std::string& taskTraceDir,
    memory::MemoryPool* pool,
    std::ostringstream& oss) {
  auto taskMetaReader = std::make_unique<exec::trace::TaskTraceMetadataReader>(
      taskTraceDir, pool);
  const auto queryConfigs = taskMetaReader->queryConfigs();
  const auto connectorProperties = taskMetaReader->connectorProperties();
  const auto queryPlan = taskMetaReader->queryPlan();

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

void printPipelineTraceSummary(
    const std::string& taskTraceDir,
    const std::string& nodeId,
    uint32_t pipelineId,
    uint32_t driverId,
    memory::MemoryPool* pool,
    std::ostringstream& oss) {
  const auto opTraceDir = exec::trace::getOpTraceDirectory(
      taskTraceDir, nodeId, pipelineId, driverId);
  const auto opTraceSummary =
      exec::trace::OperatorTraceSummaryReader(
          exec::trace::getOpTraceDirectory(
              taskTraceDir, nodeId, pipelineId, driverId),
          pool)
          .read();
  oss << "driver " << driverId << ": " << opTraceSummary.toString() << "\n";
}

void printTaskTraceSummary(
    const std::string& traceDir,
    const std::string& queryId,
    const std::string& taskId,
    const std::string& nodeId,
    memory::MemoryPool* pool,
    std::ostringstream& oss) {
  auto fs = filesystems::getFileSystem(traceDir, nullptr);
  const auto taskTraceDir =
      exec::trace::getTaskTraceDirectory(traceDir, queryId, taskId);
  const auto pipelineIds = exec::trace::listPipelineIds(
      exec::trace::getNodeTraceDirectory(taskTraceDir, nodeId), fs);

  oss << "\n++++++Task " << taskId << "++++++\n";
  for (const auto pipelineId : pipelineIds) {
    oss << "\n++++++Pipeline " << pipelineId << "++++++\n";
    const auto driverIds = exec::trace::listDriverIds(
        exec::trace::getNodeTraceDirectory(taskTraceDir, nodeId),
        pipelineId,
        fs);
    for (const auto& driverId : driverIds) {
      printPipelineTraceSummary(
          taskTraceDir, nodeId, pipelineId, driverId, pool, oss);
    }
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
        rootDir, queryId, taskId, FLAGS_node_id, pool, summary);
  }
  LOG(INFO) << summary.str();
}
} // namespace

TraceReplayRunner::TraceReplayRunner()
    : cpuExecutor_(std::make_unique<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency() *
              FLAGS_driver_cpu_executor_hw_multiplier,
          std::make_shared<folly::NamedThreadFactory>(
              "TraceReplayCpuConnector"))),
      ioExecutor_(std::make_unique<folly::IOThreadPoolExecutor>(
          std::thread::hardware_concurrency() *
              FLAGS_hive_connector_executor_hw_multiplier,
          std::make_shared<folly::NamedThreadFactory>(
              "TraceReplayIoConnector"))) {}

void TraceReplayRunner::init() {
  VELOX_USER_CHECK(!FLAGS_root_dir.empty(), "--root_dir must be provided");
  VELOX_USER_CHECK(!FLAGS_query_id.empty(), "--query_id must be provided");
  VELOX_USER_CHECK(!FLAGS_node_id.empty(), "--node_id must be provided");

  if (!memory::MemoryManager::testInstance()) {
    memory::MemoryManagerOptions options;
    options.arbitratorKind = FLAGS_memory_arbitrator_type;
    memory::initializeMemoryManager({});
  }
  filesystems::registerLocalFileSystem();
  filesystems::registerS3FileSystem();
  filesystems::registerHdfsFileSystem();
  filesystems::registerGcsFileSystem();
  filesystems::registerAbfsFileSystem();

  dwio::common::registerFileSinks();
  dwrf::registerDwrfReaderFactory();
  dwrf::registerDwrfWriterFactory();

#ifdef VELOX_ENABLE_PARQUET
  parquet::registerParquetReaderFactory();
  parquet::registerParquetWriterFactory();
#endif

  core::PlanNode::registerSerDe();
  core::ITypedExpr::registerSerDe();
  common::Filter::registerSerDe();
  Type::registerSerDe();
  exec::registerPartitionFunctionSerDe();
  if (!isRegisteredVectorSerde()) {
    serializer::presto::PrestoVectorSerde::registerVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
    serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kCompactRow)) {
    serializer::CompactRowVectorSerde::registerNamedVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kUnsafeRow)) {
    serializer::spark::UnsafeRowVectorSerde::registerNamedVectorSerde();
  }
  connector::hive::HiveTableHandle::registerSerDe();
  connector::hive::LocationHandle::registerSerDe();
  connector::hive::HiveColumnHandle::registerSerDe();
  connector::hive::HiveInsertTableHandle::registerSerDe();
  connector::hive::HiveInsertFileNameGenerator::registerSerDe();
  connector::hive::HiveConnectorSplit::registerSerDe();
  connector::hive::registerHivePartitionFunctionSerDe();
  connector::hive::HiveBucketProperty::registerSerDe();

  functions::prestosql::registerAllScalarFunctions(FLAGS_function_prefix);
  aggregate::prestosql::registerAllAggregateFunctions(FLAGS_function_prefix);
  parse::registerTypeResolver();

  if (!facebook::velox::connector::hasConnectorFactory("hive")) {
    connector::registerConnectorFactory(
        std::make_shared<connector::hive::HiveConnectorFactory>());
  }

  fs_ = filesystems::getFileSystem(FLAGS_root_dir, nullptr);
  const auto taskTraceDir = exec::trace::getTaskTraceDirectory(
      FLAGS_root_dir, FLAGS_query_id, FLAGS_task_id);
  taskTraceMetadataReader_ =
      std::make_unique<exec::trace::TaskTraceMetadataReader>(
          taskTraceDir, memory::MemoryManager::getInstance()->tracePool());
}

std::unique_ptr<tool::trace::OperatorReplayerBase>
TraceReplayRunner::createReplayer() const {
  std::unique_ptr<tool::trace::OperatorReplayerBase> replayer;
  const auto taskTraceDir = exec::trace::getTaskTraceDirectory(
      FLAGS_root_dir, FLAGS_query_id, FLAGS_task_id);
  const auto traceNodeName = taskTraceMetadataReader_->nodeName(FLAGS_node_id);
  const auto queryCapacityBytes = (1ULL * FLAGS_query_memory_capacity_mb) << 20;
  if (traceNodeName == "TableWrite") {
    VELOX_USER_CHECK(
        !FLAGS_table_writer_output_dir.empty(),
        "--table_writer_output_dir is required");
    replayer = std::make_unique<tool::trace::TableWriterReplayer>(
        FLAGS_root_dir,
        FLAGS_query_id,
        FLAGS_task_id,
        FLAGS_node_id,
        traceNodeName,
        FLAGS_driver_ids,
        queryCapacityBytes,
        cpuExecutor_.get(),
        FLAGS_table_writer_output_dir);
  } else if (traceNodeName == "Aggregation") {
    replayer = std::make_unique<tool::trace::AggregationReplayer>(
        FLAGS_root_dir,
        FLAGS_query_id,
        FLAGS_task_id,
        FLAGS_node_id,
        traceNodeName,
        FLAGS_driver_ids,
        queryCapacityBytes,
        cpuExecutor_.get());
  } else if (traceNodeName == "PartitionedOutput") {
    replayer = std::make_unique<tool::trace::PartitionedOutputReplayer>(
        FLAGS_root_dir,
        FLAGS_query_id,
        FLAGS_task_id,
        FLAGS_node_id,
        getVectorSerdeKind(),
        traceNodeName,
        FLAGS_driver_ids,
        queryCapacityBytes,
        cpuExecutor_.get());
  } else if (traceNodeName == "TableScan") {
    const auto connectorId =
        taskTraceMetadataReader_->connectorId(FLAGS_node_id);
    if (const auto& collectors = connector::getAllConnectors();
        collectors.find(connectorId) == collectors.end()) {
      const auto hiveConnector =
          connector::getConnectorFactory("hive")->newConnector(
              connectorId,
              std::make_shared<config::ConfigBase>(
                  std::unordered_map<std::string, std::string>()),
              ioExecutor_.get());
      connector::registerConnector(hiveConnector);
    }
    replayer = std::make_unique<tool::trace::TableScanReplayer>(
        FLAGS_root_dir,
        FLAGS_query_id,
        FLAGS_task_id,
        FLAGS_node_id,
        traceNodeName,
        FLAGS_driver_ids,
        queryCapacityBytes,
        cpuExecutor_.get());
  } else if (traceNodeName == "Filter" || traceNodeName == "Project") {
    replayer = std::make_unique<tool::trace::FilterProjectReplayer>(
        FLAGS_root_dir,
        FLAGS_query_id,
        FLAGS_task_id,
        FLAGS_node_id,
        traceNodeName,
        FLAGS_driver_ids,
        queryCapacityBytes,
        cpuExecutor_.get());
  } else if (traceNodeName == "HashJoin") {
    replayer = std::make_unique<tool::trace::HashJoinReplayer>(
        FLAGS_root_dir,
        FLAGS_query_id,
        FLAGS_task_id,
        FLAGS_node_id,
        traceNodeName,
        FLAGS_driver_ids,
        queryCapacityBytes,
        cpuExecutor_.get());
  } else {
    VELOX_UNSUPPORTED("Unsupported operator type: {}", traceNodeName);
  }
  VELOX_USER_CHECK_NOT_NULL(replayer);
  return replayer;
}

void TraceReplayRunner::run() {
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
  VELOX_USER_CHECK(!FLAGS_task_id.empty(), "--task_id must be provided");
  createReplayer()->run(FLAGS_copy_results);
}
} // namespace facebook::velox::tool::trace
