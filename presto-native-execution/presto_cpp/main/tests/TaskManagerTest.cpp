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
#include <gtest/gtest.h>
#include "presto_cpp/main/PrestoExchangeSource.h"
#include "presto_cpp/main/TaskResource.h"
#include "presto_cpp/main/tests/HttpServerWrapper.h"
#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/exec/tests/utils/TempFilePath.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Type.h"

using namespace facebook::velox;
using namespace facebook::presto;

namespace {
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
      sum += opStats.spilledDataSize.getValue(protocol::DataUnit::BYTE);
    }
  }
  return sum;
}
} // namespace

class Cursor {
 public:
  Cursor(
      TaskManager* taskManager,
      const protocol::TaskId& taskId,
      const RowTypePtr& rowType,
      memory::MemoryPool* pool)
      : pool_(pool),
        taskManager_(taskManager),
        taskId_(taskId),
        rowType_(rowType) {}

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
          const_cast<uint8_t*>(range.data()), (int32_t)range.size(), 0});
    }

    ByteStream input;
    input.resetInput(std::move(byteRanges));

    std::vector<RowVectorPtr> vectors;
    while (!input.atEnd()) {
      RowVectorPtr vector;
      VectorStreamGroup::read(&input, pool_, rowType_, &vector);
      vectors.emplace_back(vector);
    }
    return vectors;
  }

  memory::MemoryPool* pool_;
  memory::MappedMemory* mappedMemory_ = memory::MappedMemory::getInstance();
  TaskManager* taskManager_;
  const protocol::TaskId taskId_;
  RowTypePtr rowType_;
  bool atEnd_ = false;
  uint64_t sequence_ = 0;
};

static const uint64_t kGB = 1024 * 1024 * 1024ULL;

class TaskManagerTest : public testing::Test {
 protected:
  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();
    exec::ExchangeSource::registerFactory(
        PrestoExchangeSource::createExchangeSource);
    if (!isRegisteredVectorSerde()) {
      serializer::presto::PrestoVectorSerde::registerVectorSerde();
    };
    filesystems::registerLocalFileSystem();

    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(
                facebook::velox::exec::test::kHiveConnectorId, nullptr);
    connector::registerConnector(hiveConnector);
    dwrf::registerDwrfReaderFactory();

    pool_ = memory::getDefaultScopedMemoryPool();
    rowType_ = ROW({"c0", "c1"}, {INTEGER(), VARCHAR()});

    taskManager_ = std::make_unique<TaskManager>();
    taskResource_ = std::make_unique<TaskResource>(*taskManager_.get());

    auto httpServer = std::make_unique<http::HttpServer>(
        folly::SocketAddress("127.0.0.1", 0));
    taskResource_->registerUris(*httpServer.get());

    httpServerWrapper_ =
        std::make_unique<facebook::presto::test::HttpServerWrapper>(
            std::move(httpServer));
    auto serverAddress = httpServerWrapper_->start().get();

    taskManager_->setBaseUri(fmt::format(
        "http://{}:{}",
        serverAddress.getAddressStr(),
        serverAddress.getPort()));
  }

  void TearDown() override {
    if (httpServerWrapper_) {
      httpServerWrapper_->stop();
    }
    connector::unregisterConnector(
        facebook::velox::exec::test::kHiveConnectorId);
    dwrf::unregisterDwrfReaderFactory();
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
    dwrf::WriterOptions options;
    options.config = std::make_shared<facebook::velox::dwrf::Config>();
    options.schema = rowType_;
    auto sink = std::make_unique<dwio::common::FileSink>(
        filePath, dwio::common::MetricsLog::voidLog());
    dwrf::Writer writer{options, std::move(sink), *pool_};

    for (size_t i = 0; i < vectors.size(); ++i) {
      writer.write(vectors[i]);
    }
    writer.close();
  }

  std::vector<std::shared_ptr<exec::test::TempFilePath>> makeFilePaths(
      int count) {
    std::vector<std::shared_ptr<exec::test::TempFilePath>> filePaths;
    filePaths.reserve(count);
    for (int i = 0; i < count; ++i) {
      filePaths.emplace_back(exec::test::TempFilePath::create());
    }
    return filePaths;
  }

  protocol::ScheduledSplit makeSplit(
      const std::string& filePath,
      long sequenceId) {
    auto hiveSplit = std::make_shared<protocol::HiveSplit>();
    hiveSplit->path = filePath;
    hiveSplit->storage.storageFormat.inputFormat =
        "com.facebook.hive.orc.OrcInputFormat";
    hiveSplit->start = 0;
    hiveSplit->length = fs::file_size(filePath);

    protocol::ScheduledSplit split;
    split.split.connectorId = facebook::velox::exec::test::kHiveConnectorId;
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
    Cursor cursor(taskManager_.get(), taskId, resultType, pool_.get());
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
      protocol::TaskId outputTaskId = "output.0.0.1") {
    std::vector<std::string> locations;
    for (auto& taskUri : taskUris) {
      locations.emplace_back(fmt::format("{}/results/0", taskUri));
    }

    auto planFragment = exec::test::PlanBuilder()
                            .exchange(outputType)
                            .partitionedOutput({}, 1)
                            .planFragment();

    return taskManager_->createOrUpdateTask(
        outputTaskId,
        planFragment,
        {makeRemoteSource("0", locations, true, splitSequenceId)},
        {},
        {},
        {});
  }

  protocol::TaskSource makeSource(
      const protocol::PlanNodeId& sourceId,
      const std::vector<std::shared_ptr<exec::test::TempFilePath>>& filePaths,
      bool noMoreSplits,
      long& splitSequenceId) {
    protocol::TaskSource source;
    source.planNodeId = sourceId;
    for (auto& filePath : filePaths) {
      source.splits.emplace_back(makeSplit(filePath->path, splitSequenceId++));
    }
    source.noMoreSplits = noMoreSplits;
    return source;
  }

  protocol::TaskSource makeRemoteSource(
      const protocol::PlanNodeId& sourceId,
      const std::vector<std::string>& locations,
      bool noMoreSplits,
      long& splitSequenceId) {
    protocol::TaskSource source;
    source.planNodeId = sourceId;
    for (auto& location : locations) {
      source.splits.emplace_back(makeRemoteSplit(location, splitSequenceId++));
    }
    source.noMoreSplits = noMoreSplits;
    return source;
  }

  // Version with auto-incremented sequence id.
  protocol::TaskSource makeSource(
      const protocol::PlanNodeId& sourceId,
      const std::vector<std::shared_ptr<exec::test::TempFilePath>>& filePaths,
      bool noMoreSplits) {
    return makeSource(sourceId, filePaths, noMoreSplits, splitSequenceId_);
  }

  // Version with auto-incremented sequence id.
  protocol::TaskSource makeRemoteSource(
      const protocol::PlanNodeId& sourceId,
      const std::vector<std::string>& locations,
      bool noMoreSplits) {
    return makeRemoteSource(
        sourceId, locations, noMoreSplits, splitSequenceId_);
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
      const std::vector<std::shared_ptr<exec::test::TempFilePath>>& filePaths,
      const std::unordered_map<std::string, std::string>& queryConfigStrings =
          {},
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
            .partitionedOutput({"p0"}, numPartitions, {"p0", "a0"})
            .planFragment();

    std::vector<std::string> partialAggTasks;
    long splitSequenceId{0};
    for (int i = 0; i < filePaths.size(); ++i) {
      protocol::TaskId taskId = fmt::format("{}.0.0.{}", queryId, i);
      allTaskIds.emplace_back(taskId);
      auto source = makeSource("0", {filePaths[i]}, true, splitSequenceId);
      auto taskQueryConfig = queryConfigStrings;
      auto taskInfo = taskManager_->createOrUpdateTask(
          taskId,
          partialAggPlanFragment,
          {source},
          {},
          std::move(taskQueryConfig),
          {});
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
                     .exchange(partialAggPlanFragment.planNode->outputType())
                     .planNode()})
            .finalAggregation({"p0"}, {"count(a0)"}, {BIGINT()})
            .partitionedOutput({}, 1, {"p0", "a0"})
            .planFragment();

    std::vector<std::string> finalAggTasks;
    std::vector<protocol::TaskId> finalAggTaskIds;
    for (int i = 0; i < numPartitions; ++i) {
      protocol::TaskId finalAggTaskId = fmt::format("{}.1.0.{}", queryId, i);
      allTaskIds.emplace_back(finalAggTaskId);
      finalAggTaskIds.emplace_back(finalAggTaskId);

      std::vector<std::string> locations;
      for (auto& taskUri : partialAggTasks) {
        locations.emplace_back(fmt::format("{}/results/{}", taskUri, i));
      }

      auto taskQueryConfig = queryConfigStrings;
      auto taskInfo = taskManager_->createOrUpdateTask(
          finalAggTaskId,
          finalAggPlanFragment,
          {makeRemoteSource("0", locations, true, splitSequenceId)},
          {},
          std::move(taskQueryConfig),
          {});

      finalAggTasks.emplace_back(taskInfo->taskStatus.self);
    }

    auto finalOutputType = finalAggPlanFragment.planNode->outputType();
    auto outputTaskInfo = createOutputTask(
        finalAggTasks,
        finalOutputType,
        splitSequenceId,
        fmt::format("{}.2.0.0", queryId));
    allTaskIds.emplace_back(outputTaskInfo->taskId);
    if (!expectTaskFailure) {
      assertResults(
          outputTaskInfo->taskId,
          finalOutputType,
          "SELECT c0 % 5, count(*) FROM tmp GROUP BY 1");
    } else {
      auto resultsOrFailures =
          fetchAllResults(outputTaskInfo->taskId, finalOutputType, allTaskIds);
      EXPECT_EQ(resultsOrFailures.status->state, protocol::TaskState::FAILED);
      for (const auto& taskId : allTaskIds) {
        taskManager_->deleteTask(taskId, true);
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
      if (expectSpill) {
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

  void setMemoryLimits(uint64_t userMax, uint64_t totalMax) {
    taskManager_->getQueryContextManager()->overrideProperties(
        QueryContextManager::kQueryMaxMemoryPerNode,
        fmt::format("{}B", userMax));
    taskManager_->getQueryContextManager()->overrideProperties(
        QueryContextManager::kQueryMaxTotalMemoryPerNode,
        fmt::format("{}B", totalMax));
  }

  std::unique_ptr<memory::MemoryPool> pool_;
  RowTypePtr rowType_;
  exec::test::DuckDbQueryRunner duckDbQueryRunner_;
  std::unique_ptr<TaskManager> taskManager_;
  std::unique_ptr<TaskResource> taskResource_;
  std::unique_ptr<facebook::presto::test::HttpServerWrapper> httpServerWrapper_;
  long splitSequenceId_{0};
};

// Runs "select * from t where c0 % 5 = 0" query.
// Creates one task and provides all splits at once.
TEST_F(TaskManagerTest, tableScanAllSplitsAtOnce) {
  auto filePaths = makeFilePaths(5);
  auto vectors = makeVectors(filePaths.size(), 1'000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }
  duckDbQueryRunner_.createTable("tmp", vectors);

  auto planFragment = exec::test::PlanBuilder()
                          .tableScan(rowType_)
                          .filter("c0 % 5 = 0")
                          .partitionedOutput({}, 1, {"c0", "c1"})
                          .planFragment();

  long splitSequenceId{0};
  auto source = makeSource("0", filePaths, true, splitSequenceId);

  protocol::TaskId taskId = "scan.0.0.1";
  auto taskInfo = taskManager_->createOrUpdateTask(
      taskId, planFragment, {source}, {}, {}, {});

  assertResults(taskId, rowType_, "SELECT * FROM tmp WHERE c0 % 5 = 0");
}

// Runs "select * from t where c0 % 5 = 1" query.
// Creates one task and provides splits one at a time.
TEST_F(TaskManagerTest, tableScanOneSplitAtATime) {
  auto filePaths = makeFilePaths(5);
  auto vectors = makeVectors(filePaths.size(), 1'000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }
  duckDbQueryRunner_.createTable("tmp", vectors);

  auto planFragment = exec::test::PlanBuilder()
                          .tableScan(rowType_)
                          .filter("c0 % 5 = 1")
                          .partitionedOutput({}, 1, {"c0", "c1"})
                          .planFragment();

  protocol::TaskId taskId = "scan.0.0.1";
  auto taskInfo =
      taskManager_->createOrUpdateTask(taskId, planFragment, {}, {}, {}, {});

  long splitSequenceId{0};
  for (auto& filePath : filePaths) {
    auto source = makeSource("0", {filePath}, false, splitSequenceId);
    taskManager_->createOrUpdateTask(taskId, {}, {source}, {}, {}, {});
  }

  taskManager_->createOrUpdateTask(
      taskId, {}, {makeSource("0", {}, true, splitSequenceId)}, {}, {}, {});

  assertResults(taskId, rowType_, "SELECT * FROM tmp WHERE c0 % 5 = 1");
}

// Runs 2-stage tableScan: (1) multiple table scan tasks; (2) single output task
TEST_F(TaskManagerTest, tableScanMultipleTasks) {
  auto filePaths = makeFilePaths(5);
  auto vectors = makeVectors(filePaths.size(), 1'000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }
  duckDbQueryRunner_.createTable("tmp", vectors);

  auto planFragment = exec::test::PlanBuilder()
                          .tableScan(rowType_)
                          .filter("c0 % 5 = 1")
                          .partitionedOutput({}, 1, {"c0", "c1"})
                          .planFragment();

  std::vector<std::string> tasks;
  long splitSequenceId{0};
  for (int i = 0; i < filePaths.size(); i++) {
    protocol::TaskId taskId = fmt::format("scan.0.0.{}", i);
    auto source = makeSource("0", {filePaths[i]}, true, splitSequenceId);
    auto taskInfo = taskManager_->createOrUpdateTask(
        taskId, planFragment, {source}, {}, {}, {});
    tasks.emplace_back(taskInfo->taskStatus.self);
  }

  auto outputTaskInfo = createOutputTask(
      tasks, planFragment.planNode->outputType(), splitSequenceId);
  assertResults(
      outputTaskInfo->taskId, rowType_, "SELECT * FROM tmp WHERE c0 % 5 = 1");
}

// Create a task to scan an empty (invalid) ORC file. Ensure that the error
// propagates via getTaskStatus().
TEST_F(TaskManagerTest, emptyFile) {
  auto filePaths = makeFilePaths(1);
  auto planFragment = exec::test::PlanBuilder()
                          .tableScan(rowType_)
                          .partitionedOutput({}, 1, {"c0", "c1"})
                          .planFragment();

  // Create task to scan an empty file.
  auto source = makeSource("0", filePaths, true);
  protocol::TaskId taskId = "scan.0.0.1";
  auto taskInfo = taskManager_->createOrUpdateTask(
      taskId, planFragment, {source}, {}, {}, {});

  protocol::Duration longWait("300s");
  auto statusRequestState = http::CallbackRequestHandlerState::create();

  // Keep polling until we see the error. Try for 5 times, sleep 100ms.
  for (size_t i = 0;; i++) {
    if (i > 5) {
      FAIL() << "Didn't get a failure after " << i << " tries";
    }

    auto taskStatus =
        taskManager_->getTaskStatus(taskId, {}, longWait, statusRequestState)
            .get();
    if (not taskStatus->failures.empty()) {
      EXPECT_EQ(1, taskStatus->failures.size());
      const auto& failure = taskStatus->failures.front();
      EXPECT_EQ("fileLength_ > 0 ORC file is empty", failure.message);
      EXPECT_EQ("VeloxException", failure.type);
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

TEST_F(TaskManagerTest, countAggregation) {
  auto filePaths = makeFilePaths(5);
  auto vectors = makeVectors(filePaths.size(), 1'000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }
  duckDbQueryRunner_.createTable("tmp", vectors);

  testCountAggregation("test_count_aggr", filePaths);
}

TEST_F(TaskManagerTest, outOfQueryUserMemory) {
  auto filePaths = makeFilePaths(5);
  auto vectors = makeVectors(filePaths.size(), 1'000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }
  duckDbQueryRunner_.createTable("tmp", vectors);

  testCountAggregation("cold", filePaths);

  auto [peakUser, peakTotal] = testCountAggregation("initial", filePaths);

  setMemoryLimits(peakUser - 1, 20 * kGB);
  testCountAggregation("max-memory", filePaths, {}, true);

  // Verify query.max-total-memory-per-node is respected.
  setMemoryLimits(20 * kGB, peakTotal - 1);
  testCountAggregation("max-total-memory", filePaths, {}, true);

  // Verify the query is successful with some limits.
  setMemoryLimits(20 * kGB, 20 * kGB);
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
TEST_F(TaskManagerTest, outOfOrderRequests) {
  auto eventBase = folly::EventBaseManager::get()->getEventBase();
  // 5 minute wait to ensure we don't timeout the out-of-order requests.
  auto longWait = protocol::Duration("300s");
  auto shortWait = std::chrono::seconds(1);

  auto filePaths = makeFilePaths(5);
  auto vectors = makeVectors(filePaths.size(), 1000);
  duckDbQueryRunner_.createTable("tmp", vectors);

  auto planFragment = exec::test::PlanBuilder()
                          .values(vectors)
                          .filter("c0 % 5 = 0")
                          .partitionedOutput({}, 1, {"c0", "c1"})
                          .planFragment();

  protocol::TaskId taskId = "scan.0.0.1";
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
  taskManager_->createOrUpdateTask(taskId, planFragment, {}, {}, {}, {});

  EXPECT_NO_THROW(std::move(taskInfo).within(shortWait).getVia(eventBase));
  EXPECT_NO_THROW(std::move(taskStatus).within(shortWait).getVia(eventBase));
  EXPECT_NO_THROW(std::move(results).within(shortWait).getVia(eventBase));

  assertResults(taskId, rowType_, "SELECT * FROM tmp WHERE c0 % 5 = 0");
}

// Tests whether the returned futures timeout.
TEST_F(TaskManagerTest, timeoutOutOfOrderRequests) {
  auto eventBase = folly::EventBaseManager::get()->getEventBase();
  auto shortWait = protocol::Duration("100ms");
  auto longWait = std::chrono::seconds(5);
  auto maxSize = protocol::DataSize("32MB");

  protocol::TaskId taskId = "test.0.0.1";
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

TEST_F(TaskManagerTest, aggregationSpilll) {
  // NOTE: we need to write more than one batches to each file (source split) to
  // trigger spill.
  const int numBatchesPerFile = 5;
  const int numFiles = 5;
  auto filePaths = makeFilePaths(numFiles);
  std::vector<std::vector<RowVectorPtr>> batches(numFiles);
  for (int i = 0; i < numFiles; ++i) {
    batches[i] = makeVectors(numBatchesPerFile, 1'000);
  }
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < filePaths.size(); ++i) {
    writeToFile(filePaths[i]->path, batches[i]);
    std::move(
        batches[i].begin(), batches[i].end(), std::back_inserter(vectors));
  }
  duckDbQueryRunner_.createTable("tmp", vectors);

  // Keep bumping up query id per each test iteration to generate a new query
  // id.
  int queryId = 0;
  for (const bool doSpill : {true, true}) {
    SCOPED_TRACE(fmt::format("doSpill: {}", doSpill));
    auto tempDirectory = exec::test::TempDirectoryPath::create();
    std::unordered_map<std::string, std::string> queryConfigs;
    if (doSpill) {
      queryConfigs.emplace(core::QueryConfig::kSpillPath, tempDirectory->path);
      queryConfigs.emplace(core::QueryConfig::kTestingSpillPct, "100");
    }
    testCountAggregation(
        fmt::format("aggregationSpilll:{}", queryId++),
        filePaths,
        queryConfigs,
        false,
        doSpill);
  }
}

// TODO: add disk spilling test for order by and hash join later.
