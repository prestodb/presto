/*
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
#include "presto_cpp/main/TaskManager.h"
#include <folly/executors/ThreadedExecutor.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "folly/experimental/EventCount.h"
#include "presto_cpp/main/PrestoExchangeSource.h"
#include "presto_cpp/main/TaskResource.h"
#include "presto_cpp/main/common/tests/MutableConfigs.h"
#include "presto_cpp/main/connectors/PrestoToVeloxConnector.h"
#include "presto_cpp/main/tests/HttpServerWrapper.h"
#include "velox/common/base/Fs.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/exec/Values.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/type/Type.h"

DECLARE_int32(old_task_ms);
DECLARE_bool(velox_memory_leak_check_enabled);

static const std::string kHiveConnectorId = "test-hive";

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::presto {

namespace {

// Repeatedly calls for cleanOldTasks() for a while to ensure that we overcome a
// potential race condition where we call cleanOldTasks() before some Tasks are
// ready to be cleaned.
void waitForAllOldTasksToBeCleaned(
    TaskManager* taskManager,
    uint64_t maxWaitUs) {
  taskManager->cleanOldTasks();

  uint64_t waitUs = 0;
  while (taskManager->getNumTasks() > 0) {
    constexpr uint64_t kWaitInternalUs = 1'000;
    std::this_thread::sleep_for(std::chrono::microseconds(kWaitInternalUs));
    waitUs += kWaitInternalUs;
    taskManager->cleanOldTasks();
    if (waitUs >= maxWaitUs) {
      break;
    }
  }
}

// Generates task ID in Presto-compatible format.
class TaskIdGenerator {
 public:
  TaskIdGenerator(std::string queryId) : queryId_{std::move(queryId)} {}

  protocol::TaskId makeTaskId(int32_t stage, int32_t task) {
    return fmt::format("{}.{}.0.{}.0", queryId_, stage, task);
  }

 private:
  const std::string queryId_;
};

int64_t sumOpSpillBytes(
    const std::string& opType,
    const protocol::TaskInfo& taskInfo) {
  int64_t sum = 0;
  const auto& taskStats = taskInfo.stats;
  for (const auto& pipelineStats : taskStats.pipelines) {
    for (const auto& opStats : pipelineStats.operatorSummaries) {
      if (opStats.operatorType != opType) {
        continue;
      }
      sum += opStats.spilledDataSizeInBytes;
    }
  }
  return sum;
}

class Cursor {
 public:
  Cursor(
      TaskManager* taskManager,
      const protocol::TaskId& taskId,
      const RowTypePtr& rowType,
      velox::VectorSerde::Kind serdeKind,
      memory::MemoryPool* pool)
      : pool_(pool),
        taskManager_(taskManager),
        taskId_(taskId),
        rowType_(rowType),
        serdeKind_(serdeKind) {}

  std::optional<std::vector<RowVectorPtr>> next() {
    if (atEnd_) {
      return std::nullopt;
    }

    auto maxSize = protocol::DataSize("32MB");
    auto maxWait = protocol::Duration("2s");

    auto resultRequestState = http::CallbackRequestHandlerState::create();
    auto results =
        taskManager_
            ->getResults(
                taskId_, 0, sequence_, maxSize, maxWait, resultRequestState)
            .getVia(folly::EventBaseManager::get()->getEventBase());

    std::vector<RowVectorPtr> vectors;
    if (results->data && results->data->length()) {
      vectors = deserialize(results->data.get());
    }

    if (results->complete) {
      taskManager_->abortResults(taskId_, 0);
      atEnd_ = true;
    } else {
      taskManager_->acknowledgeResults(taskId_, 0, results->nextSequence);
      sequence_ = results->nextSequence;
    }
    return vectors;
  }

 private:
  std::vector<RowVectorPtr> deserialize(folly::IOBuf* buffer) {
    std::vector<ByteRange> byteRanges;
    for (auto& range : *buffer) {
      byteRanges.emplace_back(ByteRange{
          const_cast<uint8_t*>(range.data()),
          static_cast<int32_t>(range.size()),
          0});
    }

    const auto input =
        std::make_unique<BufferInputStream>(std::move(byteRanges));
    auto* serde = velox::getNamedVectorSerde(serdeKind_);
    std::vector<RowVectorPtr> vectors;
    while (!input->atEnd()) {
      RowVectorPtr vector;
      VectorStreamGroup::read(
          input.get(), pool_, rowType_, serde, &vector, nullptr);
      vectors.emplace_back(vector);
    }
    return vectors;
  }

  memory::MemoryPool* const pool_;
  TaskManager* const taskManager_;
  const protocol::TaskId taskId_;
  const RowTypePtr rowType_;
  const velox::VectorSerde::Kind serdeKind_;
  bool atEnd_{false};
  uint64_t sequence_{0};
};

void setAggregationSpillConfig(
    std::map<std::string, std::string>& queryConfigs) {
  queryConfigs.emplace(core::QueryConfig::kSpillEnabled, "true");
  queryConfigs.emplace(core::QueryConfig::kAggregationSpillEnabled, "true");
}

class TaskManagerTest : public exec::test::OperatorTestBase,
                        public testing::WithParamInterface<VectorSerde::Kind> {
 public:
  static std::vector<VectorSerde::Kind> getTestParams() {
    const std::vector<VectorSerde::Kind> kinds(
        {VectorSerde::Kind::kPresto,
         VectorSerde::Kind::kCompactRow,
         VectorSerde::Kind::kUnsafeRow});
    return kinds;
  }

  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
    filesystems::registerLocalFileSystem();
    if (!connector::hasConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)) {
      connector::registerConnectorFactory(
          std::make_shared<connector::hive::HiveConnectorFactory>());
    }
    test::setupMutableSystemConfig();
    SystemConfig::instance()->setValue(
        std::string(SystemConfig::kMemoryArbitratorKind), "SHARED");
    ASSERT_EQ(SystemConfig::instance()->memoryArbitratorKind(), "SHARED");
    common::testutil::TestValue::enable();
  }

 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    dwrf::registerDwrfWriterFactory();
    dwrf::registerDwrfReaderFactory();
    exec::ExchangeSource::registerFactory(
        [cpuExecutor = exchangeCpuExecutor_,
         ioExecutor = exchangeIoExecutor_,
         connPool = connPool_](
            const std::string& taskId,
            int destination,
            std::shared_ptr<exec::ExchangeQueue> queue,
            memory::MemoryPool* pool) {
          return PrestoExchangeSource::create(
              taskId,
              destination,
              queue,
              pool,
              cpuExecutor.get(),
              ioExecutor.get(),
              connPool.get(),
              nullptr);
        });

    registerPrestoToVeloxConnector(std::make_unique<HivePrestoToVeloxConnector>(
        connector::hive::HiveConnectorFactory::kHiveConnectorName));
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(
                kHiveConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()));
    connector::registerConnector(hiveConnector);

    rowType_ = ROW({"c0", "c1"}, {INTEGER(), VARCHAR()});

    taskManager_ = std::make_unique<TaskManager>(
        driverExecutor_.get(), httpSrvCpuExecutor_.get(), nullptr);

    auto validator = std::make_shared<facebook::presto::VeloxPlanValidator>();
    taskResource_ = std::make_unique<TaskResource>(
        pool_.get(),
        httpSrvCpuExecutor_.get(),
        validator.get(),
        *taskManager_.get());

    auto httpServer = std::make_unique<http::HttpServer>(
        httpSrvIOExecutor_,
        std::make_unique<http::HttpConfig>(
            folly::SocketAddress("127.0.0.1", 0)));
    taskResource_->registerUris(*httpServer.get());

    httpServerWrapper_ =
        std::make_unique<facebook::presto::test::HttpServerWrapper>(
            std::move(httpServer));
    auto serverAddress = httpServerWrapper_->start().get();

    taskManager_->setBaseUri(fmt::format(
        "http://{}:{}",
        serverAddress.getAddressStr(),
        serverAddress.getPort()));
    writerFactory_ =
        dwio::common::getWriterFactory(dwio::common::FileFormat::DWRF);
  }

  void TearDown() override {
    if (httpServerWrapper_) {
      httpServerWrapper_->stop();
    }
    connector::unregisterConnector(kHiveConnectorId);
    unregisterPrestoToVeloxConnector(
        connector::hive::HiveConnectorFactory::kHiveConnectorName);
    dwrf::unregisterDwrfWriterFactory();
    dwrf::unregisterDwrfReaderFactory();
    taskManager_.reset();
    OperatorTestBase::TearDown();
  }

  std::vector<RowVectorPtr> makeVectors(int count, int rowsPerVector) {
    std::vector<RowVectorPtr> vectors;
    for (int i = 0; i < count; ++i) {
      auto vector = std::dynamic_pointer_cast<RowVector>(
          facebook::velox::test::BatchMaker::createBatch(
              rowType_, rowsPerVector, *pool_));
      vectors.emplace_back(vector);
    }
    return vectors;
  }

  void writeToFile(const std::string& filePath, RowVectorPtr vector) {
    writeToFile(filePath, std::vector{vector});
  }

  void writeToFile(
      const std::string& filePath,
      const std::vector<RowVectorPtr>& vectors) {
    auto options = writerFactory_->createWriterOptions();
    options->schema = rowType_;
    options->memoryPool = rootPool_.get();
    auto sink = std::make_unique<dwio::common::LocalFileSink>(
        filePath, dwio::common::FileSink::Options{});
    auto writer =
        writerFactory_->createWriter(std::move(sink), std::move(options));

    for (size_t i = 0; i < vectors.size(); ++i) {
      writer->write(vectors[i]);
    }
    writer->close();
  }

  std::vector<std::string> makeFilePaths(
      const std::shared_ptr<TempDirectoryPath>& dirPath,
      int count) {
    std::vector<std::string> filePaths;
    filePaths.reserve(count);
    for (int i = 0; i < count; ++i) {
      filePaths.emplace_back(fmt::format("{}/{}", dirPath->getPath(), i));
    }
    return filePaths;
  }

  protocol::ScheduledSplit makeSplit(
      const std::string& filePath,
      long sequenceId) {
    auto hiveSplit = std::make_shared<protocol::hive::HiveSplit>();
    hiveSplit->fileSplit.path = filePath;
    hiveSplit->storage.storageFormat.inputFormat =
        "com.facebook.hive.orc.OrcInputFormat";
    hiveSplit->fileSplit.start = 0;
    hiveSplit->fileSplit.length = fs::file_size(filePath);

    protocol::ScheduledSplit split;
    split.split.connectorId = kHiveConnectorId;
    split.split.connectorSplit = hiveSplit;
    split.sequenceId = sequenceId;
    return split;
  }

  /// Checks if any of the specified tasks has failed and returns that task's
  /// status. Returns nullptr if no task has failed.
  std::unique_ptr<protocol::TaskStatus> checkForFailedTasks(
      const std::vector<std::string>& allTaskIds) {
    for (const auto& taskId : allTaskIds) {
      auto state = http::CallbackRequestHandlerState::create();
      auto status =
          taskManager_
              ->getTaskStatus(
                  taskId, std::nullopt, protocol::Duration("0s"), state)
              .getVia(folly::EventBaseManager::get()->getEventBase());
      if (status->state == protocol::TaskState::FAILED) {
        return status;
      }
    }
    return nullptr;
  }

  struct ResultsOrFailure {
    std::vector<RowVectorPtr> results;
    std::unique_ptr<protocol::TaskStatus> status;
  };

  ResultsOrFailure fetchAllResults(
      const protocol::TaskId& taskId,
      const RowTypePtr& resultType,
      const std::vector<std::string>& allTaskIds) {
    Cursor cursor(
        taskManager_.get(), taskId, resultType, GetParam(), pool_.get());
    std::vector<RowVectorPtr> vectors;
    for (;;) {
      auto moreVectors = cursor.next();
      if (!moreVectors.has_value()) {
        break;
      }

      if (!moreVectors.value().empty()) {
        vectors.insert(
            vectors.end(),
            moreVectors.value().begin(),
            moreVectors.value().end());
      } else {
        // Check if any of the tasks failed.
        auto status = checkForFailedTasks(allTaskIds);
        if (status) {
          return {vectors, std::move(status)};
        }
      }
    }

    return {vectors, nullptr};
  }

  void assertResults(
      const protocol::TaskId& taskId,
      const RowTypePtr& resultType,
      const std::string& duckDbSql) {
    auto resultOrFailure = fetchAllResults(taskId, resultType, {taskId});
    ASSERT_EQ(resultOrFailure.status, nullptr);
    exec::test::assertResults(
        resultOrFailure.results, resultType, duckDbSql, duckDbQueryRunner_);
  }

  std::unique_ptr<protocol::TaskInfo> createOutputTask(
      const std::vector<std::string>& taskUris,
      const RowTypePtr& outputType,
      long& splitSequenceId,
      protocol::TaskId outputTaskId = "output.0.0.1.0") {
    auto planFragment =
        exec::test::PlanBuilder()
            .exchange(outputType, GetParam())
            .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam())
            .planFragment();

    protocol::TaskUpdateRequest updateRequest;
    updateRequest.sources.push_back(
        makeRemoteSource("0", taskUris, 0, splitSequenceId));
    return createOrUpdateTask(outputTaskId, updateRequest, planFragment);
  }

  protocol::TaskSource makeSource(
      const protocol::PlanNodeId& sourceId,
      const std::vector<std::string>& filePaths,
      bool noMoreSplits,
      long& splitSequenceId) {
    protocol::TaskSource source;
    source.planNodeId = sourceId;
    for (auto& filePath : filePaths) {
      source.splits.emplace_back(makeSplit(filePath, splitSequenceId++));
    }
    source.noMoreSplits = noMoreSplits;
    return source;
  }

  protocol::TaskSource makeRemoteSource(
      const protocol::PlanNodeId& sourceId,
      const std::vector<std::string>& remoteTaskUris,
      int32_t destination,
      long& splitSequenceId) {
    protocol::TaskSource source;
    source.planNodeId = sourceId;
    for (const auto& taskUri : remoteTaskUris) {
      const auto location = fmt::format("{}/results/{}", taskUri, destination);
      source.splits.emplace_back(makeRemoteSplit(location, splitSequenceId++));
    }
    source.noMoreSplits = true;
    return source;
  }

  // Version with auto-incremented sequence id.
  protocol::TaskSource makeSource(
      const protocol::PlanNodeId& sourceId,
      const std::vector<std::string>& filePaths,
      bool noMoreSplits) {
    return makeSource(sourceId, filePaths, noMoreSplits, splitSequenceId_);
  }

  protocol::ScheduledSplit makeRemoteSplit(
      const std::string& location,
      long sequenceId) {
    auto remoteSplit = std::make_shared<protocol::RemoteSplit>();
    remoteSplit->location.location = location;

    protocol::ScheduledSplit split;
    split.split.connectorId = "system";
    split.split.connectorSplit = remoteSplit;
    split.sequenceId = sequenceId;
    return split;
  }

  // Runs select c0 % 5, count(*) from t group by 1
  // Setup 3 stages:
  //  - scan + partial agg
  //  - final agg
  //  - output
  std::pair<int64_t, int64_t> testCountAggregation(
      const protocol::QueryId& queryId,
      const std::vector<std::string>& filePaths,
      const std::map<std::string, std::string>& queryConfigStrings = {},
      bool expectTaskFailure = false,
      bool expectSpill = false) {
    std::vector<std::string> allTaskIds;

    const int numPartitions = 3;
    // Create scan + partial agg tasks
    auto partialAggPlanFragment =
        exec::test::PlanBuilder()
            .tableScan(rowType_)
            .project({"c0 % 5"})
            .partialAggregation({"p0"}, {"count(1)"})
            .partitionedOutput({"p0"}, numPartitions, {"p0", "a0"}, GetParam())
            .planFragment();

    TaskIdGenerator taskIdGenerator(queryId);

    std::vector<std::string> partialAggTasks;
    long splitSequenceId{0};
    for (int i = 0; i < filePaths.size(); ++i) {
      const auto taskId = taskIdGenerator.makeTaskId(0, i);
      allTaskIds.emplace_back(taskId);

      protocol::TaskUpdateRequest updateRequest;
      updateRequest.sources.push_back(
          makeSource("0", {filePaths[i]}, true, splitSequenceId));
      updateRequest.session.systemProperties = queryConfigStrings;
      auto taskInfo =
          createOrUpdateTask(taskId, updateRequest, partialAggPlanFragment);
      partialAggTasks.emplace_back(taskInfo->taskStatus.self);
    }

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    // Create final-agg tasks
    auto finalAggBuilder = exec::test::PlanBuilder(planNodeIdGenerator);
    auto finalAggPlanFragment =
        finalAggBuilder
            .localPartition(
                {"p0"},
                {exec::test::PlanBuilder(planNodeIdGenerator)
                     .exchange(
                         partialAggPlanFragment.planNode->outputType(),
                         GetParam())
                     .planNode()})
            .finalAggregation({"p0"}, {"count(a0)"}, {{BIGINT()}})
            .partitionedOutput({}, 1, {"p0", "a0"}, GetParam())
            .planFragment();

    std::vector<std::string> finalAggTasks;
    std::vector<protocol::TaskId> finalAggTaskIds;
    for (int i = 0; i < numPartitions; ++i) {
      const auto finalAggTaskId = taskIdGenerator.makeTaskId(1, i);

      allTaskIds.emplace_back(finalAggTaskId);
      finalAggTaskIds.emplace_back(finalAggTaskId);

      protocol::TaskUpdateRequest updateRequest;
      updateRequest.sources.push_back(
          makeRemoteSource("0", partialAggTasks, i, splitSequenceId));
      updateRequest.session.systemProperties = queryConfigStrings;
      auto taskInfo = createOrUpdateTask(
          finalAggTaskId, updateRequest, finalAggPlanFragment);

      finalAggTasks.emplace_back(taskInfo->taskStatus.self);
    }

    auto finalOutputType = finalAggPlanFragment.planNode->outputType();
    auto outputTaskInfo = createOutputTask(
        finalAggTasks,
        finalOutputType,
        splitSequenceId,
        fmt::format("{}.2.0.0.0", queryId));
    allTaskIds.emplace_back(outputTaskInfo->taskId);
    if (!expectTaskFailure) {
      assertResults(
          outputTaskInfo->taskId,
          finalOutputType,
          "SELECT c0 % 5, count(*) FROM tmp GROUP BY 1");
    } else {
      auto resultsOrFailures =
          fetchAllResults(outputTaskInfo->taskId, finalOutputType, allTaskIds);
      EXPECT_TRUE(resultsOrFailures.status != nullptr);
      EXPECT_EQ(resultsOrFailures.status->state, protocol::TaskState::FAILED);
      for (const auto& taskId : allTaskIds) {
        taskManager_->deleteTask(taskId, true, true);
      }
    }

    for (const auto& finalAggTaskId : finalAggTaskIds) {
      auto cbState = std::make_shared<http::CallbackRequestHandlerState>();
      auto taskInfo =
          taskManager_
              ->getTaskInfo(
                  finalAggTaskId, false, std::nullopt, std::nullopt, cbState)
              .get();
      const int64_t spilledBytes = sumOpSpillBytes("Aggregation", *taskInfo);
      if (expectSpill && injectedSpillCount() > 0) {
        EXPECT_GT(spilledBytes, 0);
      } else {
        EXPECT_EQ(spilledBytes, 0);
      }
    }

    auto cbState = std::make_shared<http::CallbackRequestHandlerState>();
    auto taskInfo = taskManager_
                        ->getTaskInfo(
                            outputTaskInfo->taskId,
                            false,
                            std::nullopt,
                            std::nullopt,
                            cbState)
                        .get();

    return std::make_pair(
        taskInfo->stats.peakUserMemoryInBytes,
        taskInfo->stats.peakTotalMemoryInBytes);
  }

  // Setup the temporary spilling directory and initialize the system config
  // file (in the same temporary directory) to contain the spilling path
  // setting.
  static std::shared_ptr<exec::test::TempDirectoryPath> setupSpillPath() {
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto sysConfigFilePath =
        fmt::format("{}/config.properties", spillDirectory->getPath());
    auto fileSystem = filesystems::getFileSystem(sysConfigFilePath, nullptr);
    auto sysConfigFile = fileSystem->openFileForWrite(sysConfigFilePath);
    SystemConfig::instance()->setValue(
        std::string(SystemConfig::kSpillerSpillPath),
        spillDirectory->getPath());
    sysConfigFile->close();

    auto nodeConfigFilePath =
        fmt::format("{}/node.properties", spillDirectory->getPath());
    auto nodeConfigFile = fileSystem->openFileForWrite(nodeConfigFilePath);
    nodeConfigFile->append(fmt::format(
        "{}={}\n{}={}",
        NodeConfig::kNodeInternalAddress,
        "192.16.7.66",
        NodeConfig::kNodeId,
        "12"));
    nodeConfigFile->close();
    NodeConfig::instance()->initialize(nodeConfigFilePath);

    return spillDirectory;
  }

  std::shared_ptr<exec::Task> createDummyExecTask(
      const std::string& taskId,
      const core::PlanFragment& planFragment) {
    auto queryCtx =
        taskManager_->getQueryContextManager()->findOrCreateQueryCtx(
            taskId, {});
    return exec::Task::create(
        taskId,
        planFragment,
        0,
        std::move(queryCtx),
        exec::Task::ExecutionMode::kParallel);
  }

  std::unique_ptr<protocol::TaskInfo> createOrUpdateTask(
      const protocol::TaskId& taskId,
      const protocol::TaskUpdateRequest& updateRequest,
      const core::PlanFragment& planFragment,
      bool summarize = true) {
    auto queryCtx =
        taskManager_->getQueryContextManager()->findOrCreateQueryCtx(
            taskId, updateRequest);
    return taskManager_->createOrUpdateTask(
        taskId, updateRequest, planFragment, summarize, std::move(queryCtx), 0);
  }

  RowTypePtr rowType_;
  exec::test::DuckDbQueryRunner duckDbQueryRunner_;
  std::unique_ptr<TaskManager> taskManager_;
  std::unique_ptr<TaskResource> taskResource_;
  std::unique_ptr<facebook::presto::test::HttpServerWrapper> httpServerWrapper_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> exchangeCpuExecutor_ =
      std::make_shared<folly::CPUThreadPoolExecutor>(1);
  std::shared_ptr<folly::IOThreadPoolExecutor> exchangeIoExecutor_ =
      std::make_shared<folly::IOThreadPoolExecutor>(10);
  std::shared_ptr<folly::CPUThreadPoolExecutor> driverExecutor_ =
      std::make_shared<folly::CPUThreadPoolExecutor>(
          4,
          std::make_shared<folly::NamedThreadFactory>("Driver"));
  std::shared_ptr<folly::CPUThreadPoolExecutor> httpSrvCpuExecutor_ =
      std::make_shared<folly::CPUThreadPoolExecutor>(
          4,
          std::make_shared<folly::NamedThreadFactory>("HTTPSrvCpu"));
  std::shared_ptr<folly::IOThreadPoolExecutor> httpSrvIOExecutor_ =
      std::make_shared<folly::IOThreadPoolExecutor>(
          8,
          std::make_shared<folly::NamedThreadFactory>("HTTPSrvIO"));
  long splitSequenceId_{0};
  std::shared_ptr<http::HttpClientConnectionPool> connPool_ =
      std::make_shared<http::HttpClientConnectionPool>();
  std::shared_ptr<dwio::common::WriterFactory> writerFactory_;
};

// Runs "select * from t where c0 % 5 = 0" query.
// Creates one task and provides all splits at once.
TEST_P(TaskManagerTest, tableScanAllSplitsAtOnce) {
  const auto tableDir = exec::test::TempDirectoryPath::create();
  auto filePaths = makeFilePaths(tableDir, 5);
  auto vectors = makeVectors(filePaths.size(), 1'000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i], vectors[i]);
  }
  duckDbQueryRunner_.createTable("tmp", vectors);

  auto planFragment = exec::test::PlanBuilder()
                          .tableScan(rowType_)
                          .filter("c0 % 5 = 0")
                          .partitionedOutput({}, 1, {"c0", "c1"}, GetParam())
                          .planFragment();

  long splitSequenceId{0};

  protocol::TaskId taskId = "scan.0.0.1.0";

  protocol::TaskUpdateRequest updateRequest;
  updateRequest.sources.push_back(
      makeSource("0", filePaths, true, splitSequenceId));
  auto taskInfo = createOrUpdateTask(taskId, updateRequest, planFragment);

  ASSERT_GE(taskInfo->stats.queuedTimeInNanos, 0);
  assertResults(taskId, rowType_, "SELECT * FROM tmp WHERE c0 % 5 = 0");
}

TEST_P(TaskManagerTest, fecthFromFinishedTask) {
  const auto tableDir = exec::test::TempDirectoryPath::create();
  auto filePaths = makeFilePaths(tableDir, 5);
  auto vectors = makeVectors(filePaths.size(), 1'000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i], vectors[i]);
  }
  duckDbQueryRunner_.createTable("tmp", vectors);

  auto planFragment = exec::test::PlanBuilder()
                          .tableScan(rowType_)
                          .filter("c0 % 5 = 0")
                          .partitionedOutputArbitrary({"c0", "c1"}, GetParam())
                          .planFragment();

  const protocol::TaskId taskId = "scan.0.0.1.0";
  long splitSequenceId{0};
  protocol::TaskUpdateRequest updateRequest;
  updateRequest.sources.push_back(
      makeSource("0", filePaths, true, splitSequenceId));
  const auto taskInfo = createOrUpdateTask(taskId, updateRequest, planFragment);

  const protocol::Duration longWait("300s");
  const auto maxSize = protocol::DataSize("32MB");
  auto resultRequestState = http::CallbackRequestHandlerState::create();
  const auto destination = 0;
  auto consumeCompleted = false;
  // Keep consuming destination 0 until completed.
  while (!consumeCompleted) {
    auto results =
        taskManager_
            ->getResults(
                taskId, destination, 0, maxSize, longWait, resultRequestState)
            .getVia(folly::EventBaseManager::get()->getEventBase());
    consumeCompleted = results->complete;
  }
  // Close destination 0 and triggers the task closure.
  taskManager_->abortResults(taskId, destination);
  auto prestoTask = taskManager_->tasks().at(taskId);
  ASSERT_TRUE(waitForTaskStateChange(
      prestoTask->task.get(), TaskState::kFinished, 3'000'000));

  const auto newDestination = 1;
  // Fetch destination 1 from the closed task.
  auto newResult = taskManager_->getResults(
      taskId, newDestination, 0, maxSize, longWait, resultRequestState);
  // It should get an immediate complete signal instead of waiting for timeout.
  ASSERT_TRUE(newResult.isReady());
  ASSERT_TRUE(newResult.value()->complete);
}

DEBUG_ONLY_TEST_P(TaskManagerTest, fecthFromArbitraryOutput) {
  // Block output until the first fetch destination becomes inactive.
  folly::EventCount outputWait;
  std::atomic<bool> outputWaitFlag{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(const velox::exec::Values*)>(
          [&](const velox::exec::Values* values) {
            outputWait.await([&]() { return outputWaitFlag.load(); });
          }));

  const std::vector<RowVectorPtr> batches = makeVectors(1, 1'000);
  auto planFragment = exec::test::PlanBuilder()
                          .values(batches)
                          .partitionedOutputArbitrary({"c0", "c1"}, GetParam())
                          .planFragment();
  const protocol::TaskId taskId = "source.0.0.1.0";
  const auto taskInfo = createOrUpdateTask(taskId, {}, planFragment);

  const protocol::Duration longWait("10s");
  const auto maxSize = protocol::DataSize("1024MB");
  auto expiredRequestState = http::CallbackRequestHandlerState::create();
  // Consume from destination 0 to simulate the case that the http request has
  // expired while destination has notify setup.
  auto expiredResultWait = taskManager_->getResults(
      taskId, 0, 0, maxSize, protocol::Duration("1s"), expiredRequestState);
  // Reset the http request to simulate the case that it has expired.
  expiredRequestState.reset();

  // Unblock output.
  outputWaitFlag = true;
  outputWait.notifyAll();

  // Consuming from destination 1 and expect get result.
  auto requestState = http::CallbackRequestHandlerState::create();
  const auto result =
      taskManager_
          ->getResults(
              taskId, 1, 0, maxSize, protocol::Duration("10s"), requestState)
          .getVia(folly::EventBaseManager::get()->getEventBase());
  ASSERT_FALSE(result->complete);
  ASSERT_FALSE(result->data->empty());
  ASSERT_EQ(result->sequence, 0);
  ASSERT_EQ(result->nextSequence, 1);

  // Check the expired result hasn't fetched any data after timeout.
  const auto expriredResult =
      std::move(expiredResultWait)
          .getVia(folly::EventBaseManager::get()->getEventBase());
  ASSERT_FALSE(expriredResult->complete);
  ASSERT_TRUE(expriredResult->data->empty());
  ASSERT_EQ(expriredResult->sequence, 0);
  ASSERT_EQ(expriredResult->nextSequence, 0);

  // Close destinations and triggers the task closure.
  taskManager_->abortResults(taskId, 0);
  taskManager_->abortResults(taskId, 1);

  auto prestoTask = taskManager_->tasks().at(taskId);
  ASSERT_TRUE(waitForTaskStateChange(
      prestoTask->task.get(), TaskState::kFinished, 3'000'000));
}

TEST_P(TaskManagerTest, taskCleanupWithPendingResultData) {
  // Trigger old task cleanup immediately.
  taskManager_->setOldTaskCleanUpMs(0);

  const auto tableDir = exec::test::TempDirectoryPath::create();
  auto filePaths = makeFilePaths(tableDir, 5);
  auto vectors = makeVectors(filePaths.size(), 1'000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i], vectors[i]);
  }

  auto planFragment = exec::test::PlanBuilder()
                          .tableScan(rowType_)
                          .filter("c0 % 5 = 0")
                          .partitionedOutput({}, 1, {"c0", "c1"}, GetParam())
                          .planFragment();

  long splitSequenceId{0};
  auto source = makeSource("0", filePaths, true, splitSequenceId);

  const protocol::TaskId taskId = "scan.0.0.1.0";
  protocol::TaskUpdateRequest updateRequest;
  updateRequest.sources.push_back(source);
  const auto taskInfo = createOrUpdateTask(taskId, updateRequest, planFragment);

  const protocol::Duration longWait("300s");
  const auto maxSize = protocol::DataSize("32MB");
  auto resultRequestState = http::CallbackRequestHandlerState::create();
  auto results =
      taskManager_
          ->getResults(taskId, 0, 0, maxSize, longWait, resultRequestState)
          .getVia(folly::EventBaseManager::get()->getEventBase());

  std::exception e;
  taskManager_->createOrUpdateErrorTask(
      taskId, std::make_exception_ptr(e), true, 0);
  taskManager_->deleteTask(taskId, true, true);
  for (int i = 0; i < 10; ++i) {
    // 'results' holds a reference on the presto task which prevents the old
    // task cleanup.
    const auto numCleanupTasks = taskManager_->cleanOldTasks();
    ASSERT_EQ(numCleanupTasks, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  // Clear the results which releases the reference on the presto tasks.
  results.reset();
  // Wait until the task gets cleanup.
  for (;;) {
    // 'results' holds a reference on the presto task which prevents the old
    // task cleanup.
    const auto numCleanupTasks = taskManager_->cleanOldTasks();
    if (numCleanupTasks == 1) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

// Tests a grouped execution task with no splits and task queuing enabled.
TEST_P(TaskManagerTest, queuedEmptyGroupedExecutionTask) {
  SystemConfig::instance()->setValue(
      std::string(SystemConfig::kWorkerOverloadedTaskQueuingEnabled), "true");

  core::PlanNodeId scanNodeId;
  auto planFragment = exec::test::PlanBuilder()
                          .tableScan(rowType_)
                          .capturePlanNodeId(scanNodeId)
                          .filter("c0 % 5 = 1")
                          .partitionedOutput({}, 1, {"c0", "c1"}, GetParam())
                          .planFragment();
  planFragment.executionStrategy = core::ExecutionStrategy::kGrouped;
  planFragment.groupedExecutionLeafNodeIds.emplace(scanNodeId);
  planFragment.numSplitGroups = 2;

  // Tell task manager that server is overloaded and create a task with no
  // splits and 'no more splits' message.
  taskManager_->setServerOverloaded(true);

  const protocol::TaskId taskId = "scanQueuedTask.0.0.1.0";
  protocol::TaskUpdateRequest updateRequest;
  long splitSequenceId{0};
  updateRequest.sources.push_back(
      makeSource(scanNodeId, {}, true, splitSequenceId));

  createOrUpdateTask(taskId, updateRequest, planFragment);

  // Check that the task is not started.
  auto prestoTask = taskManager_->tasks().at(taskId);
  ASSERT_FALSE(prestoTask->taskStarted);

  // Mark server no longer overloaded and trigger the task start.
  taskManager_->setServerOverloaded(false);
  taskManager_->maybeStartNextQueuedTask();

  // Check that the task is started.
  ASSERT_TRUE(prestoTask->taskStarted);

  // Fetch the results and see that we have finished the task.
  auto results = fetchAllResults(taskId, rowType_, {taskId});
  ASSERT_EQ(results.results.size(), 0);
  auto execTask = prestoTask->task;
  if (execTask) {
    ASSERT_EQ(execTask->state(), TaskState::kFinished);
  }
}

// Runs "select * from t where c0 % 5 = 1" query.
// Creates one task and provides splits one at a time.
TEST_P(TaskManagerTest, tableScanOneSplitAtATime) {
  const auto tableDir = exec::test::TempDirectoryPath::create();
  auto filePaths = makeFilePaths(tableDir, 5);
  auto vectors = makeVectors(filePaths.size(), 1'000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i], vectors[i]);
  }
  duckDbQueryRunner_.createTable("tmp", vectors);

  auto planFragment = exec::test::PlanBuilder()
                          .tableScan(rowType_)
                          .filter("c0 % 5 = 1")
                          .partitionedOutput({}, 1, {"c0", "c1"}, GetParam())
                          .planFragment();

  protocol::TaskId taskId = "scan.0.0.1.0";
  auto taskInfo = createOrUpdateTask(taskId, {}, planFragment);

  long splitSequenceId{0};
  for (auto& filePath : filePaths) {
    auto source = makeSource("0", {filePath}, false, splitSequenceId);

    protocol::TaskUpdateRequest updateRequest;
    updateRequest.sources.push_back(source);
    taskManager_->createOrUpdateTask(
        taskId, updateRequest, {}, true, nullptr, 0);
  }

  protocol::TaskUpdateRequest updateRequest;
  updateRequest.sources.push_back(makeSource("0", {}, true, splitSequenceId));
  taskManager_->createOrUpdateTask(taskId, updateRequest, {}, true, nullptr, 0);

  assertResults(taskId, rowType_, "SELECT * FROM tmp WHERE c0 % 5 = 1");
}

// Runs 2-stage tableScan: (1) multiple table scan tasks; (2) single output task
TEST_P(TaskManagerTest, tableScanMultipleTasks) {
  const auto tableDir = exec::test::TempDirectoryPath::create();
  auto filePaths = makeFilePaths(tableDir, 5);
  auto vectors = makeVectors(filePaths.size(), 1'000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i], vectors[i]);
  }
  duckDbQueryRunner_.createTable("tmp", vectors);

  auto planFragment = exec::test::PlanBuilder()
                          .tableScan(rowType_)
                          .filter("c0 % 5 = 1")
                          .partitionedOutput({}, 1, {"c0", "c1"}, GetParam())
                          .planFragment();

  TaskIdGenerator taskIdGenerator("scan");

  std::vector<std::string> tasks;
  long splitSequenceId{0};
  for (int i = 0; i < filePaths.size(); i++) {
    const auto taskId = taskIdGenerator.makeTaskId(0, i);
    auto source = makeSource("0", {filePaths[i]}, true, splitSequenceId);
    protocol::TaskUpdateRequest updateRequest;
    updateRequest.sources.push_back(source);
    auto taskInfo = createOrUpdateTask(taskId, updateRequest, planFragment);
    tasks.emplace_back(taskInfo->taskStatus.self);
  }

  auto outputTaskInfo = createOutputTask(
      tasks, planFragment.planNode->outputType(), splitSequenceId);
  assertResults(
      outputTaskInfo->taskId, rowType_, "SELECT * FROM tmp WHERE c0 % 5 = 1");
}

TEST_P(TaskManagerTest, countAggregation) {
  const auto tableDir = exec::test::TempDirectoryPath::create();
  auto filePaths = makeFilePaths(tableDir, 5);
  auto vectors = makeVectors(filePaths.size(), 1'000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i], vectors[i]);
  }
  duckDbQueryRunner_.createTable("tmp", vectors);

  testCountAggregation("test_count_aggr", filePaths);
}

// Run distributed sort query that has 2 stages. First stage runs multiple
// tasks with partial sort. Second stage runs single task with merge exchange.
TEST_P(TaskManagerTest, distributedSort) {
  const auto tableDir = exec::test::TempDirectoryPath::create();
  auto filePaths = makeFilePaths(tableDir, 5);
  auto vectors = makeVectors(filePaths.size(), 1'000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i], vectors[i]);
  }
  duckDbQueryRunner_.createTable("tmp", vectors);

  // Create partial sort tasks.
  auto partialSortPlan =
      exec::test::PlanBuilder()
          .tableScan(rowType_)
          .orderBy({"c0"}, true)
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam())
          .planFragment();

  TaskIdGenerator taskIdGenerator("distributed-sort");

  std::vector<std::string> partialSortUris;
  long splitSequenceId{0};
  for (int i = 0; i < filePaths.size(); ++i) {
    protocol::TaskId taskId = taskIdGenerator.makeTaskId(1, i);

    protocol::TaskUpdateRequest updateRequest;
    updateRequest.sources.push_back(
        makeSource("0", {filePaths[i]}, true, splitSequenceId));

    auto taskInfo = createOrUpdateTask(taskId, updateRequest, partialSortPlan);
    partialSortUris.emplace_back(taskInfo->taskStatus.self);
  }

  // Create final sort task.
  auto finalSortPlan =
      exec::test::PlanBuilder()
          .mergeExchange(rowType_, {"c0"}, GetParam())
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam())
          .planFragment();

  protocol::TaskUpdateRequest updateRequest;
  updateRequest.sources.push_back(
      makeRemoteSource("0", partialSortUris, 0, splitSequenceId));
  const std::string finalTaskId = taskIdGenerator.makeTaskId(0, 0);
  createOrUpdateTask(finalTaskId, updateRequest, finalSortPlan);
  assertResults(finalTaskId, rowType_, "SELECT * FROM tmp ORDER BY c0");

  auto infoRequestState = http::CallbackRequestHandlerState::create();
  auto statusRequestState = http::CallbackRequestHandlerState::create();
  auto resultRequestState = http::CallbackRequestHandlerState::create();

  const auto finalPrestoTaskStats =
      taskManager_
          ->getTaskInfo(
              finalTaskId, false, std::nullopt, std::nullopt, infoRequestState)
          .get()
          ->stats;
  const auto finalPrestoTaskStatus =
      taskManager_
          ->getTaskStatus(
              finalTaskId, std::nullopt, std::nullopt, statusRequestState)
          .wait()
          .get();
  ASSERT_EQ(
      finalPrestoTaskStatus->peakNodeTotalMemoryReservationInBytes,
      finalPrestoTaskStats.peakNodeTotalMemoryInBytes);
  // Since we have more than one tasks in a query, then the task peak memory
  // usage might be lower than the node one.
  ASSERT_LE(
      finalPrestoTaskStats.peakTotalMemoryInBytes,
      finalPrestoTaskStats.peakNodeTotalMemoryInBytes);
  ASSERT_EQ(
      finalPrestoTaskStats.peakTotalMemoryInBytes,
      finalPrestoTaskStats.peakUserMemoryInBytes);
}

TEST_P(TaskManagerTest, outOfQueryUserMemory) {
  const auto tableDir = exec::test::TempDirectoryPath::create();
  auto filePaths = makeFilePaths(tableDir, 5);
  auto vectors = makeVectors(filePaths.size(), 1'000);
  for (auto i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i], vectors[i]);
  }
  duckDbQueryRunner_.createTable("tmp", vectors);

  testCountAggregation("cold", filePaths);

  auto [peakUser, peakTotal] = testCountAggregation("initial", filePaths);
  SystemConfig::instance()->setValue(
      std::string(SystemConfig::kQueryMaxMemoryPerNode),
      fmt::format("{}B", peakUser - 1));
  testCountAggregation("max-memory", filePaths, {}, true);

  // Verify the query is successful with some limits.
  SystemConfig::instance()->setValue(
      std::string(SystemConfig::kQueryMaxMemoryPerNode), "20GB");
  testCountAggregation("test-count-aggr", filePaths);

  // Wait a little to allow for futures to complete.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  auto tasks = taskManager_->tasks();

  for (const auto& [taskId, prestoTask] : tasks) {
    const auto& task = prestoTask->task;
    ASSERT_LE(1, task.use_count()) << taskId;
  }
}

// Tests whether the returned futures timeout.
TEST_P(TaskManagerTest, outOfOrderRequests) {
  auto eventBase = folly::EventBaseManager::get()->getEventBase();
  // 5 minute wait to ensure we don't timeout the out-of-order requests.
  auto longWait = protocol::Duration("300s");
  auto shortWait = std::chrono::seconds(1);

  const auto tableDir = exec::test::TempDirectoryPath::create();
  auto filePaths = makeFilePaths(tableDir, 5);
  auto vectors = makeVectors(filePaths.size(), 1000);
  duckDbQueryRunner_.createTable("tmp", vectors);

  auto planFragment = exec::test::PlanBuilder()
                          .values(vectors)
                          .filter("c0 % 5 = 0")
                          .partitionedOutput({}, 1, {"c0", "c1"}, GetParam())
                          .planFragment();

  protocol::TaskId taskId = "scan.0.0.1.0";
  protocol::TaskState currentState{};
  auto maxSize = protocol::DataSize("32MB");

  auto infoRequestState = http::CallbackRequestHandlerState::create();
  auto statusRequestState = http::CallbackRequestHandlerState::create();
  auto resultRequestState = http::CallbackRequestHandlerState::create();

  // Out of order requests.
  auto taskInfo = taskManager_->getTaskInfo(
      taskId, false, currentState, longWait, infoRequestState);
  auto taskStatus = taskManager_->getTaskStatus(
      taskId, currentState, longWait, statusRequestState);
  auto results = taskManager_->getResults(
      taskId, 0, 0, maxSize, longWait, resultRequestState);

  // Create the task.
  createOrUpdateTask(taskId, {}, planFragment);

  EXPECT_NO_THROW(std::move(taskInfo).within(shortWait).getVia(eventBase));
  EXPECT_NO_THROW(std::move(taskStatus).within(shortWait).getVia(eventBase));
  EXPECT_NO_THROW(std::move(results).within(shortWait).getVia(eventBase));

  assertResults(taskId, rowType_, "SELECT * FROM tmp WHERE c0 % 5 = 0");
}

// Tests whether the returned futures timeout.
TEST_P(TaskManagerTest, timeoutOutOfOrderRequests) {
  auto eventBase = folly::EventBaseManager::get()->getEventBase();
  auto shortWait = protocol::Duration("100ms");
  auto longWait = std::chrono::seconds(5);
  auto maxSize = protocol::DataSize("32MB");

  protocol::TaskId taskId = "test.0.0.1.0";
  protocol::TaskState currentState{};

  auto infoRequestState = http::CallbackRequestHandlerState::create();
  auto statusRequestState = http::CallbackRequestHandlerState::create();
  auto resultRequestState = http::CallbackRequestHandlerState::create();

  // Verify that getTaskInfo's future times out, but not within(longWait).
  EXPECT_NO_THROW(
      taskManager_
          ->getTaskInfo(
              taskId, false, currentState, shortWait, infoRequestState)
          .within(longWait)
          .getVia(eventBase));

  // Verify that getStatusInfo's future times out, but not within(longWait).
  EXPECT_NO_THROW(
      taskManager_
          ->getTaskStatus(taskId, currentState, shortWait, statusRequestState)
          .via(eventBase)
          .within(longWait)
          .getVia(eventBase));

  // Verify that getResults' future times out, but not within(longWait).
  EXPECT_NO_THROW(
      taskManager_
          ->getResults(taskId, 0, 0, maxSize, shortWait, resultRequestState)
          .via(eventBase)
          .within(longWait)
          .getVia(eventBase));
}

TEST_P(TaskManagerTest, getBaseSpillDirectory) {
  taskManager_->setBaseSpillDirectory("dummy");
  EXPECT_EQ("dummy", taskManager_->getBaseSpillDirectory());
}

TEST_P(TaskManagerTest, aggregationSpill) {
  // NOTE: we need to write more than one batches to each file (source split) to
  // trigger spill.
  const int numBatchesPerFile = 5;
  const int numFiles = 5;
  const auto tableDir = exec::test::TempDirectoryPath::create();
  auto filePaths = makeFilePaths(tableDir, numFiles);
  std::vector<std::vector<RowVectorPtr>> batches(numFiles);
  for (int i = 0; i < numFiles; ++i) {
    batches[i] = makeVectors(numBatchesPerFile, 1'000);
  }
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < filePaths.size(); ++i) {
    writeToFile(filePaths[i], batches[i]);
    std::move(
        batches[i].begin(), batches[i].end(), std::back_inserter(vectors));
  }
  duckDbQueryRunner_.createTable("tmp", vectors);

  // Keep bumping up queryId per each test iteration to generate a new query id.
  int queryId = 0;
  for (const bool doSpill : {false, true}) {
    SCOPED_TRACE(fmt::format("doSpill: {}", doSpill));
    std::shared_ptr<exec::test::TempDirectoryPath> spillDirectory;
    std::map<std::string, std::string> queryConfigs;

    int32_t spillPct{0};
    if (doSpill) {
      spillDirectory = TaskManagerTest::setupSpillPath();
      taskManager_->setBaseSpillDirectory(spillDirectory->getPath());
      spillPct = 100;
      queryConfigs.emplace(core::QueryConfig::kSpillEnabled, "true");
      queryConfigs.emplace(core::QueryConfig::kAggregationSpillEnabled, "true");
    }

    TestScopedSpillInjection scopedSpillInjection(spillPct);
    testCountAggregation(
        fmt::format("aggregationSpill:{}", queryId++),
        filePaths,
        queryConfigs,
        false,
        doSpill);
  }
}

TEST_P(TaskManagerTest, buildTaskSpillDirectoryPath) {
  EXPECT_EQ(
      std::make_tuple(
          "fs::/base/presto_native/192.168.10.2_19/2022-12-20/20221220-Q/Task1/",
          "fs::/base/presto_native/192.168.10.2_19/2022-12-20/"),
      TaskManager::buildTaskSpillDirectoryPath(
          "fs::/base", "192.168.10.2", "19", "20221220-Q", "Task1", true));
  EXPECT_EQ(
      std::make_tuple(
          "fsx::/root/presto_native/192.16.10.2_sample_node_id/1970-01-01/Q100/Task22/",
          "fsx::/root/presto_native/192.16.10.2_sample_node_id/1970-01-01/"),
      TaskManager::buildTaskSpillDirectoryPath(
          "fsx::/root",
          "192.16.10.2",
          "sample_node_id",
          "Q100",
          "Task22",
          true));

  EXPECT_EQ(
      std::make_tuple(
          "fs::/base/presto_native/2022-12-20/20221220-Q/Task1/",
          "fs::/base/presto_native/2022-12-20/"),
      TaskManager::buildTaskSpillDirectoryPath(
          "fs::/base", "192.168.10.2", "19", "20221220-Q", "Task1", false));
  EXPECT_EQ(
      std::make_tuple(
          "fsx::/root/presto_native/1970-01-01/Q100/Task22/",
          "fsx::/root/presto_native/1970-01-01/"),
      TaskManager::buildTaskSpillDirectoryPath(
          "fsx::/root",
          "192.16.10.2",
          "sample_node_id",
          "Q100",
          "Task22",
          false));
}

TEST_P(TaskManagerTest, getDataOnAbortedTask) {
  // Simulate scenario where Driver encountered a VeloxException and terminated
  // a task, which removes the entry in BufferManager. The main taskmanager
  // tries to process the resultRequest and calls getData() which must return
  // false. The resultRequest must be marked incomplete.
  auto planFragment = exec::test::PlanBuilder()
                          .tableScan(rowType_)
                          .filter("c0 % 5 = 0")
                          .partitionedOutput({}, 1, {"c0", "c1"}, GetParam())
                          .planFragment();

  int token = 123;
  auto scanTaskId = "scan.0.0.1.0";
  bool promiseFulfilled = false;
  auto prestoTask = std::make_shared<PrestoTask>(scanTaskId, "1");
  auto [promise, f] = folly::makePromiseContract<std::unique_ptr<Result>>();
  folly::ThreadedExecutor executor;
  folly::Future<std::unique_ptr<Result>> semiFuture =
      std::move(f).via(&executor);
  // Future is invoked when a value is set on the promise.
  auto future =
      std::move(semiFuture)
          .thenValue([&promiseFulfilled,
                      token](std::unique_ptr<Result> result) {
            ASSERT_EQ(result->complete, false);
            ASSERT_EQ(
                result->data->capacity(), folly::IOBuf::create(0)->capacity());
            ASSERT_EQ(result->sequence, token);
            promiseFulfilled = true;
          });
  auto promiseHolder = std::make_shared<PromiseHolder<std::unique_ptr<Result>>>(
      std::move(promise));
  auto requestState = http::CallbackRequestHandlerState::create();
  auto request = std::make_unique<ResultRequest>(
      folly::to_weak_ptr(promiseHolder),
      folly::to_weak_ptr(requestState),
      scanTaskId,
      0,
      token,
      protocol::DataSize("32MB"));
  prestoTask->resultRequests.insert({0, std::move(request)});
  prestoTask->task = createDummyExecTask(scanTaskId, planFragment);
  taskManager_->getDataForResultRequests(prestoTask->resultRequests);
  std::move(future).get();
  ASSERT_EQ(prestoTask->info.nodeId, "1");
  ASSERT_TRUE(promiseFulfilled);
}

TEST_P(TaskManagerTest, getResultsFromFailedTask) {
  const protocol::TaskId taskId = "error-task.0.0.0.0";
  std::exception e;
  taskManager_->createOrUpdateErrorTask(
      taskId, std::make_exception_ptr(e), true, 0);

  // We expect to get empty results, rather than an exception.
  const uint64_t startTimeUs = velox::getCurrentTimeMicro();
  auto results = taskManager_
                     ->getResults(
                         taskId,
                         0,
                         0,
                         protocol::DataSize("32MB"),
                         protocol::Duration("1s"),
                         http::CallbackRequestHandlerState::create())
                     .get();
  const uint64_t finishTimeUs = velox::getCurrentTimeMicro();

  {
    // ensure response is returned with a delay
    using namespace std::chrono;
    ASSERT_GE(
        finishTimeUs - startTimeUs,
        duration_cast<microseconds>(milliseconds{500}).count());
  }

  ASSERT_FALSE(results->complete);
  ASSERT_EQ(results->data->capacity(), 0);
}

TEST_P(TaskManagerTest, getResultsFromAbortedTask) {
  const protocol::TaskId taskId = "aborted-task.0.0.0.0";
  // deleting a non existing task creates an aborted task
  taskManager_->deleteTask(taskId, true, true);

  // We expect to get empty results, rather than an exception.
  const uint64_t startTimeUs = velox::getCurrentTimeMicro();
  auto results = taskManager_
                     ->getResults(
                         taskId,
                         0,
                         0,
                         protocol::DataSize("32MB"),
                         protocol::Duration("1s"),
                         http::CallbackRequestHandlerState::create())
                     .get();
  const uint64_t finishTimeUs = velox::getCurrentTimeMicro();

  {
    // ensure response is returned with a delay
    using namespace std::chrono;
    ASSERT_GE(
        finishTimeUs - startTimeUs,
        duration_cast<microseconds>(milliseconds{500}).count());
  }

  ASSERT_FALSE(results->complete);
  ASSERT_EQ(results->data->capacity(), 0);
}

TEST_P(TaskManagerTest, testCumulativeMemory) {
  const std::vector<RowVectorPtr> batches = makeVectors(4, 128);
  const auto planFragment =
      exec::test::PlanBuilder()
          .values(batches)
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam())
          .planFragment();
  auto queryCtx = core::QueryCtx::create(driverExecutor_.get());
  const protocol::TaskId taskId = "scan.0.0.1.0";
  auto veloxTask = Task::create(
      taskId,
      std::move(planFragment),
      0,
      std::move(queryCtx),
      Task::ExecutionMode::kParallel);

  const uint64_t startTimeMs = velox::getCurrentTimeMs();
  auto prestoTask = std::make_unique<PrestoTask>(taskId, "fakeId");
  prestoTask->task = veloxTask;
  veloxTask->start(1);
  prestoTask->taskStarted = true;

  auto outputBufferManager = OutputBufferManager::getInstanceRef();
  ASSERT_TRUE(outputBufferManager != nullptr);
  // Wait until the task has produced all the output buffers so its memory usage
  // stay constant to ease test.
  for (;;) {
    auto outputBufferStats = outputBufferManager->stats(taskId).value();
    if (outputBufferStats.noMoreData) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  const auto memoryUsage = veloxTask->queryCtx()->pool()->usedBytes();
  ASSERT_GT(memoryUsage, 0);

  const uint64_t lastTimeMs = velox::getCurrentTimeMs();
  protocol::TaskInfo prestoTaskInfo = prestoTask->updateInfo(true);
  ASSERT_EQ(prestoTaskInfo.stats.userMemoryReservationInBytes, memoryUsage);
  ASSERT_EQ(prestoTaskInfo.stats.systemMemoryReservationInBytes, 0);
  const auto lastCumulativeTotalMemory =
      prestoTaskInfo.stats.cumulativeTotalMemory;
  ASSERT_GT(lastCumulativeTotalMemory, 0);

  // The initial reported cumulative memory must be less than the expected value
  // below.
  ASSERT_LE(
      lastCumulativeTotalMemory, memoryUsage * (lastTimeMs - startTimeMs));
  // Presto native doesn't differentiate user and system memory.
  ASSERT_EQ(
      lastCumulativeTotalMemory, prestoTaskInfo.stats.cumulativeUserMemory);

  // Wait for 1 second to check the updated cumulative memory usage is within
  // bound.
  std::this_thread::sleep_for(std::chrono::milliseconds(1'000));

  prestoTaskInfo = prestoTask->updateInfo(true);
  // Wait a bit to avoid the timing related flakiness in test check below.
  std::this_thread::sleep_for(std::chrono::milliseconds(1));
  const uint64_t currentTimeMs = velox::getCurrentTimeMs();
  // There won't be any task memory usage change as we don't consume any output
  // buffers.
  ASSERT_EQ(memoryUsage, prestoTaskInfo.stats.userMemoryReservationInBytes);
  ASSERT_EQ(prestoTaskInfo.stats.systemMemoryReservationInBytes, 0);
  ASSERT_LE(
      prestoTaskInfo.stats.cumulativeTotalMemory,
      lastCumulativeTotalMemory + memoryUsage * (currentTimeMs - lastTimeMs));
  ASSERT_LT(
      lastCumulativeTotalMemory, prestoTaskInfo.stats.cumulativeTotalMemory);
  ASSERT_EQ(
      prestoTaskInfo.stats.cumulativeTotalMemory,
      prestoTaskInfo.stats.cumulativeUserMemory);

  veloxTask->requestAbort();
  const auto taskStatus = prestoTask->updateStatus();
  const auto taskStats = prestoTask->updateInfo(true).stats;
  ASSERT_EQ(
      taskStatus.peakNodeTotalMemoryReservationInBytes,
      taskStats.peakNodeTotalMemoryInBytes);
  // Since we have only one task in a query, then the task peak memory usage
  // will be the same as the node one.
  ASSERT_EQ(
      taskStats.peakTotalMemoryInBytes, taskStats.peakNodeTotalMemoryInBytes);
  ASSERT_EQ(taskStats.peakTotalMemoryInBytes, taskStats.peakUserMemoryInBytes);
  prestoTask.reset();
  veloxTask.reset();
  velox::exec::test::waitForAllTasksToBeDeleted(3'000'000);
}

TEST_P(TaskManagerTest, checkBatchSplits) {
  const auto taskId = "test.1.2.3.4";

  core::PlanNodeId probeId;
  core::PlanNodeId buildId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto planFragment =
      exec::test::PlanBuilder(planNodeIdGenerator)
          .tableScan(rowType_)
          .capturePlanNodeId(probeId)
          .hashJoin(
              {"c0"},
              {"u_c0"},
              exec::test::PlanBuilder(planNodeIdGenerator)
                  .tableScan(rowType_)
                  .capturePlanNodeId(buildId)
                  .project({"c0 as u_c0", "c1 as u_c1"})
                  .planNode(),
              "",
              {"u_c0", "u_c1"})
          .singleAggregation({}, {"count(1)"})
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam())
          .planFragment();

  // No splits.
  auto queryCtx =
      taskManager_->getQueryContextManager()->findOrCreateQueryCtx(taskId, {});
  VELOX_ASSERT_THROW(
      taskManager_->createOrUpdateBatchTask(
          taskId, {}, planFragment, true, queryCtx, 0),
      "Expected all splits and no-more-splits message for all plan nodes");

  // Splits for scan node on the probe side.
  protocol::BatchTaskUpdateRequest batchRequest;
  batchRequest.taskUpdateRequest.sources.push_back(
      makeSource(probeId, {}, true));
  VELOX_ASSERT_THROW(
      taskManager_->createOrUpdateBatchTask(
          taskId, batchRequest, planFragment, true, queryCtx, 0),
      "Expected all splits and no-more-splits message for all plan nodes: " +
          buildId);

  // Splits for scan nodes on both probe and build sides, but missing
  // no-more-splits message for build side.
  batchRequest.taskUpdateRequest.sources.push_back(
      makeSource(buildId, {}, false));
  VELOX_ASSERT_THROW(
      taskManager_->createOrUpdateBatchTask(
          taskId, batchRequest, planFragment, true, queryCtx, 0),
      "Expected no-more-splits message for plan node " + buildId);

  // All splits.
  batchRequest.taskUpdateRequest.sources.back().noMoreSplits = true;
  ASSERT_NO_THROW(taskManager_->createOrUpdateBatchTask(
      taskId, batchRequest, planFragment, true, queryCtx, 0));
  auto resultOrFailure = fetchAllResults(taskId, ROW({BIGINT()}), {});
  ASSERT_EQ(resultOrFailure.status, nullptr);
}

TEST_P(TaskManagerTest, buildSpillDirectoryFailure) {
  // Cleanup old tasks between test iterations.
  taskManager_->setOldTaskCleanUpMs(0);
  for (bool buildSpillDirectoryFailure : {false}) {
    SCOPED_TRACE(fmt::format(
        "buildSpillDirectoryFailure: {}", buildSpillDirectoryFailure));
    auto spillDir = setupSpillPath();

    std::vector<RowVectorPtr> batches = makeVectors(1, 1'000);

    if (buildSpillDirectoryFailure) {
      // Set bad formatted spill path.
      taskManager_->setBaseSpillDirectory(
          fmt::format("/etc/{}", spillDir->getPath()));
    } else {
      taskManager_->setBaseSpillDirectory(spillDir->getPath());
    }

    std::map<std::string, std::string> queryConfigs;
    setAggregationSpillConfig(queryConfigs);

    auto planFragment = exec::test::PlanBuilder()
                            .values(batches)
                            .singleAggregation({"c0"}, {"count(c1)"}, {})
                            .planFragment();
    const protocol::TaskId taskId = "test.0.0.0.0";
    protocol::TaskUpdateRequest updateRequest;
    updateRequest.session.systemProperties = queryConfigs;
    // Create task will fail if the spilling directory setup fails.
    if (buildSpillDirectoryFailure) {
      VELOX_ASSERT_THROW(
          createOrUpdateTask(taskId, updateRequest, planFragment), "Mkdir");
    } else {
      createOrUpdateTask(taskId, updateRequest, planFragment);
      auto taskMap = taskManager_->tasks();
      ASSERT_EQ(taskMap.size(), 1);
      auto* veloxTask = taskMap.begin()->second->task.get();
      ASSERT_TRUE(veloxTask != nullptr);
      ASSERT_FALSE(veloxTask->spillDirectory().empty());
    }

    taskManager_->deleteTask(taskId, true, true);
    if (!buildSpillDirectoryFailure) {
      auto taskMap = taskManager_->tasks();
      ASSERT_EQ(taskMap.size(), 1);
      auto* veloxTask = taskMap.begin()->second->task.get();
      ASSERT_TRUE(veloxTask != nullptr);
      while (veloxTask->numFinishedDrivers() != veloxTask->numTotalDrivers()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }
    waitForAllOldTasksToBeCleaned(taskManager_.get(), 3'000'000);
    velox::exec::test::waitForAllTasksToBeDeleted(3'000'000);
    ASSERT_TRUE(taskManager_->tasks().empty());
  }
}

TEST_P(TaskManagerTest, summarize) {
  const auto tableDir = exec::test::TempDirectoryPath::create();
  auto filePaths = makeFilePaths(tableDir, 5);
  auto vectors = makeVectors(filePaths.size(), 1'000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i], vectors[i]);
  }
  duckDbQueryRunner_.createTable("tmp", vectors);

  auto planFragment = exec::test::PlanBuilder()
                          .tableScan(rowType_)
                          .filter("c0 % 5 = 1")
                          .partitionedOutput({}, 1, {"c0", "c1"}, GetParam())
                          .planFragment();

  protocol::TaskId taskId = "scan.0.0.1.0";
  auto taskInfo = createOrUpdateTask(taskId, {}, planFragment, false);
  // pipeline stats available when summarize set to false
  ASSERT_GT(taskInfo->stats.pipelines.size(), 0);

  long splitSequenceId{0};
  bool summarize = false;
  for (auto& filePath : filePaths) {
    auto source = makeSource("0", {filePath}, false, splitSequenceId);
    summarize = !summarize;
    protocol::TaskUpdateRequest updateRequest;
    updateRequest.sources.push_back(source);
    taskInfo = taskManager_->createOrUpdateTask(
        taskId, updateRequest, {}, summarize, nullptr, 0);
    if (summarize) {
      // no pipeline stats when summarize set to true
      ASSERT_EQ(taskInfo->stats.pipelines.size(), 0);
    } else {
      // pipeline stats available when summarize set to false
      ASSERT_GT(taskInfo->stats.pipelines.size(), 0);
    }
  }

  auto callback = std::make_shared<http::CallbackRequestHandlerState>();
  taskInfo =
      taskManager_
          ->getTaskInfo(taskId, false, std::nullopt, std::nullopt, callback)
          .get();
  // pipeline stats available when summarize set to false
  ASSERT_GT(taskInfo->stats.pipelines.size(), 0);

  taskInfo =
      taskManager_
          ->getTaskInfo(taskId, true, std::nullopt, std::nullopt, callback)
          .get();
  // no pipeline stats when summarize set to true
  ASSERT_EQ(taskInfo->stats.pipelines.size(), 0);

  protocol::TaskUpdateRequest updateRequest;
  updateRequest.sources.push_back(makeSource("0", {}, true, splitSequenceId));
  taskInfo = taskManager_->createOrUpdateTask(
      taskId, updateRequest, {}, true, nullptr, 0);
  // no pipeline stats when summarize set to true
  ASSERT_EQ(taskInfo->stats.pipelines.size(), 0);

  assertResults(taskId, rowType_, "SELECT * FROM tmp WHERE c0 % 5 = 1");

  taskInfo =
      taskManager_
          ->getTaskInfo(taskId, true, std::nullopt, std::nullopt, callback)
          .get();
  // pipeline stats available when summarize set to false and task is finished
  ASSERT_GT(taskInfo->stats.pipelines.size(), 0);

  taskInfo = taskManager_->deleteTask(taskId, true, true);
  // pipeline stats available when summarize set to false and task is finished
  ASSERT_GT(taskInfo->stats.pipelines.size(), 0);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    TaskManagerTest,
    TaskManagerTest,
    testing::ValuesIn(TaskManagerTest::getTestParams()));
} // namespace
} // namespace facebook::presto
