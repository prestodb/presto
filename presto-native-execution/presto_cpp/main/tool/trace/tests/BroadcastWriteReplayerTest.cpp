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
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 */
#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "presto_cpp/main/operators/BroadcastWrite.h"
#include "presto_cpp/main/tool/trace/BroadcastWriteReplayer.h"
#include "velox/exec/OperatorTraceReader.h"
#include "velox/exec/TraceUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::tool::trace;
using namespace facebook::presto;
using namespace facebook::presto::operators;

namespace {

// Mock BroadcastFileWriter that captures writes instead of writing to disk
class MockBroadcastFileWriter {
 public:
  MockBroadcastFileWriter(
      const std::string& filePath,
      uint64_t maxBroadcastBytes,
      uint64_t writeBufferSize,
      std::unique_ptr<VectorSerde::Options> serdeOptions,
      memory::MemoryPool* pool)
      : filePath_(filePath),
        maxBroadcastBytes_(maxBroadcastBytes),
        pool_(pool) {
    LOG(INFO) << "MockBroadcastFileWriter created with path: " << filePath;
  }

  void write(const RowVectorPtr& rowVector) {
    capturedData_.push_back(rowVector);
    numRows_ += rowVector->size();
  }

  void noMoreData() {
    finalized_ = true;
  }

  RowVectorPtr fileStats() {
    // Return a dummy stats vector (BroadcastWrite returns file metadata)
    return nullptr;
  }

  // Test accessors
  const std::string& getFilePath() const {
    return filePath_;
  }
  const std::vector<RowVectorPtr>& getCapturedData() const {
    return capturedData_;
  }
  uint64_t getNumRows() const {
    return numRows_;
  }
  bool isFinalized() const {
    return finalized_;
  }

 private:
  std::string filePath_;
  uint64_t maxBroadcastBytes_;
  memory::MemoryPool* pool_;
  std::vector<RowVectorPtr> capturedData_;
  uint64_t numRows_{0};
  bool finalized_{false};
};

// Global registry to store mock writers for test verification
static std::vector<std::shared_ptr<MockBroadcastFileWriter>> g_mockWriters;
static std::mutex g_mockWritersMutex;

void clearMockWriters() {
  std::lock_guard<std::mutex> lock(g_mockWritersMutex);
  g_mockWriters.clear();
}

std::vector<std::shared_ptr<MockBroadcastFileWriter>> getMockWriters() {
  std::lock_guard<std::mutex> lock(g_mockWritersMutex);
  return g_mockWriters;
}

void registerMockWriter(std::shared_ptr<MockBroadcastFileWriter> writer) {
  std::lock_guard<std::mutex> lock(g_mockWritersMutex);
  g_mockWriters.push_back(writer);
}

// Test version of BroadcastWriteOperator that uses MockBroadcastFileWriter
class TestBroadcastWriteOperator : public Operator {
 public:
  TestBroadcastWriteOperator(
      int32_t operatorId,
      DriverCtx* ctx,
      const std::shared_ptr<const BroadcastWriteNode>& planNode)
      : Operator(
            ctx,
            planNode->outputType(),
            operatorId,
            planNode->id(),
            "BroadcastWrite"),
        serdeRowType_{planNode->serdeRowType()},
        serdeChannels_(calculateOutputChannels(
            planNode->inputType(),
            planNode->serdeRowType(),
            planNode->serdeRowType())),
        maxBroadcastBytes_(planNode->maxBroadcastBytes()) {
    const auto& basePath = planNode->basePath();
    VELOX_CHECK(!basePath.empty(), "Base path for broadcast files is empty!");

    // Create mock writer and register it for verification
    mockWriter_ = std::make_shared<MockBroadcastFileWriter>(
        fmt::format("{}/file_broadcast_test", basePath),
        planNode->maxBroadcastBytes(),
        8 << 20,
        nullptr,
        operatorCtx_->pool());

    registerMockWriter(mockWriter_);
  }

  bool needsInput() const override {
    return true;
  }

  void addInput(RowVectorPtr input) override {
    RowVectorPtr reorderedInput = nullptr;
    if (serdeRowType_->size() > 0 && serdeChannels_.empty()) {
      reorderedInput = std::move(input);
    } else {
      std::vector<VectorPtr> outputColumns;
      outputColumns.reserve(serdeChannels_.size());
      for (auto i : serdeChannels_) {
        outputColumns.push_back(input->childAt(i));
      }

      reorderedInput = std::make_shared<RowVector>(
          input->pool(),
          serdeRowType_,
          nullptr /*nulls*/,
          input->size(),
          outputColumns);
    }

    mockWriter_->write(reorderedInput);
    auto lockedStats = stats_.wlock();
    lockedStats->addOutputVector(
        reorderedInput->estimateFlatSize(), reorderedInput->size());
  }

  void noMoreInput() override {
    Operator::noMoreInput();
    mockWriter_->noMoreData();
  }

  RowVectorPtr getOutput() override {
    if (!noMoreInput_ || finished_) {
      return nullptr;
    }

    finished_ = true;
    return mockWriter_->fileStats();
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

 private:
  const RowTypePtr serdeRowType_;
  const std::vector<column_index_t> serdeChannels_;
  const uint64_t maxBroadcastBytes_;
  std::shared_ptr<MockBroadcastFileWriter> mockWriter_;
  bool finished_{false};
};

// Translator for TestBroadcastWriteOperator
class TestBroadcastWriteTranslator : public Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<Operator> toOperator(
      DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    if (auto broadcastWriteNode =
            std::dynamic_pointer_cast<const BroadcastWriteNode>(node)) {
      return std::make_unique<TestBroadcastWriteOperator>(
          id, ctx, broadcastWriteNode);
    }
    return nullptr;
  }
};

class MockBroadcastWriteOperator : public Operator {
 public:
  MockBroadcastWriteOperator(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const BroadcastWriteNode>& broadcastWriteNode)
      : Operator(
            driverCtx,
            broadcastWriteNode->outputType(),
            operatorId,
            broadcastWriteNode->id(),
            "MockBroadcastWrite") {}

  void addInput(RowVectorPtr /* unused */) override {}

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void noMoreInput() override {
    Operator::noMoreInput();
  }

  BlockingReason isBlocked(ContinueFuture* /* future */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_;
  }
};

class MockBroadcastWriteTranslator : public Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<Operator> toOperator(
      DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    if (auto broadcastWriteNode =
            std::dynamic_pointer_cast<const BroadcastWriteNode>(node)) {
      return std::make_unique<MockBroadcastWriteOperator>(
          id, ctx, broadcastWriteNode);
    }
    return nullptr;
  }
};

} // namespace

class BroadcastWriteReplayerTest : public HiveConnectorTestBase {
 protected:
  static void SetUpTestCase() {
    exec::test::HiveConnectorTestBase::SetUpTestCase();
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    filesystems::registerLocalFileSystem();
    facebook::velox::exec::trace::registerDummySourceSerDe();
    if (!isRegisteredVectorSerde()) {
      serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }
    Type::registerSerDe();
    common::Filter::registerSerDe();
    core::PlanNode::registerSerDe();
    exec::trace::registerDummySourceSerDe();
    core::ITypedExpr::registerSerDe();
    DeserializationWithContextRegistryForSharedPtr().Register(
        "BroadcastWriteNode", operators::BroadcastWriteNode::create);
    exec::trace::registerTraceNodeFactory(
        "MockBroadcastWrite",
        [](const core::PlanNode* traceNode,
           const core::PlanNodeId& nodeId) -> core::PlanNodePtr {
          if (const auto* broadcastWriteNode =
                  dynamic_cast<const operators::BroadcastWriteNode*>(
                      traceNode)) {
            return std::make_shared<operators::BroadcastWriteNode>(
                nodeId,
                broadcastWriteNode->basePath(),
                broadcastWriteNode->maxBroadcastBytes(),
                broadcastWriteNode->serdeRowType(),
                std::make_shared<exec::trace::DummySourceNode>(
                    broadcastWriteNode->sources().front()->outputType()));
          }
          return nullptr;
        });
  }

  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(
        std::thread::hardware_concurrency());
    // Clear mock writers from any previous test
    clearMockWriters();
  }

  void TearDown() override {
    // Clear mock writers BEFORE parent TearDown destroys memory pools
    clearMockWriters();
    HiveConnectorTestBase::TearDown();
  }

  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;

  // Helper function to create deterministic test vectors
  std::vector<RowVectorPtr> makeDeterministicVectors(
      int numVectors,
      int rowsPerVector) {
    std::vector<RowVectorPtr> testVectors;
    testVectors.reserve(numVectors);
    for (int vectorIdx = 0; vectorIdx < numVectors; ++vectorIdx) {
      testVectors.push_back(makeRowVector({
          makeFlatVector<int32_t>(
              rowsPerVector,
              [vectorIdx](vector_size_t row) { return vectorIdx * 100 + row; }),
          makeFlatVector<std::string>(
              rowsPerVector,
              [vectorIdx](vector_size_t row) {
                return fmt::format("row_{}_{}", vectorIdx, row);
              }),
      }));
    }
    return testVectors;
  }
};

TEST_F(BroadcastWriteReplayerTest, basic) {
  auto traceDirPath = exec::test::TempDirectoryPath::create();
  const std::string traceRoot = traceDirPath->getPath();

  // Register mock operator for trace phase
  auto mockTranslator = std::make_unique<MockBroadcastWriteTranslator>();
  exec::Operator::registerOperator(std::move(mockTranslator));

  const auto inputData = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
      makeFlatVector<std::string>({"a", "b", "c", "d", "e"}),
  });

  const auto outputType = ROW({"c0", "c1"}, {INTEGER(), VARCHAR()});

  // Create broadcast write plan with mock operator
  // Use a temp directory as the ORIGINAL basePath (simulates path)
  auto originalBasePathDir = exec::test::TempDirectoryPath::create();
  const std::string originalBasePath = originalBasePathDir->getPath();
  const uint64_t maxBroadcastBytes = 1024 * 1024;

  std::string broadcastWriteNodeId;
  auto plan =
      PlanBuilder()
          .values({inputData})
          .addNode([&](const std::string& id, core::PlanNodePtr input) {
            broadcastWriteNodeId = id;
            return std::make_shared<BroadcastWriteNode>(
                id, originalBasePath, maxBroadcastBytes, outputType, input);
          })
          .planNode();

  // Run the trace phase with mock operator
  std::shared_ptr<Task> task;
  auto traceResult =
      AssertQueryBuilder(duckDbQueryRunner_)
          .plan(plan)
          .config(core::QueryConfig::kQueryTraceEnabled, true)
          .config(core::QueryConfig::kQueryTraceDir, traceRoot)
          .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
          .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
          .config(core::QueryConfig::kQueryTraceNodeId, broadcastWriteNodeId)
          .copyResults(pool(), task);

  // Check that trace metadata was created
  const auto taskTraceDir = exec::trace::getTaskTraceDirectory(
      traceRoot, task->queryCtx()->queryId(), task->taskId());
  const auto opTraceDir = exec::trace::getOpTraceDirectory(
      taskTraceDir, broadcastWriteNodeId, /*pipelineId=*/0, /*driverId=*/0);
  const auto summary =
      exec::trace::OperatorTraceSummaryReader(opTraceDir, pool()).read();
  ASSERT_EQ(summary.opType, "MockBroadcastWrite");
  ASSERT_GT(summary.inputRows, 0);

  // Run replay with TEST operator that captures writes
  exec::Operator::unregisterAllOperators();
  exec::Operator::registerOperator(
      std::make_unique<TestBroadcastWriteTranslator>());

  const std::string driverIds = "0";
  const uint64_t queryCapacity = 1024 * 1024;

  // Create a SEPARATE temporary directory for replay output
  auto replayOutputDirPath = exec::test::TempDirectoryPath::create();
  const std::string replayOutputDir = replayOutputDirPath->getPath();

  // Ensure the two directories are different
  ASSERT_NE(originalBasePath, replayOutputDir);

  // Clear any previous mock writers before replay
  clearMockWriters();

  BroadcastWriteReplayer replayer(
      traceRoot,
      task->queryCtx()->queryId(),
      task->taskId(),
      broadcastWriteNodeId,
      "BroadcastWrite",
      driverIds,
      queryCapacity,
      executor_.get(),
      replayOutputDir);

  auto replayResult = replayer.run();
  ASSERT_NE(replayResult, nullptr);

  // VERIFY PATH REDIRECTION VIA MOCK CAPTURES:
  auto mockWriters = getMockWriters();
  ASSERT_FALSE(mockWriters.empty())
      << "No mock writers were created during replay";

  for (const auto& mockWriter : mockWriters) {
    // 1. Verify the mock was created with replayOutputDir (not
    // originalBasePath)
    const auto& capturedPath = mockWriter->getFilePath();
    EXPECT_TRUE(capturedPath.find(replayOutputDir) != std::string::npos)
        << "Mock writer path should contain replayOutputDir. Got: "
        << capturedPath << ", expected to contain: " << replayOutputDir;

    EXPECT_TRUE(capturedPath.find(originalBasePath) == std::string::npos)
        << "Mock writer path should NOT contain originalBasePath. Got: "
        << capturedPath << ", should not contain: " << originalBasePath;

    // 2. Verify data was actually written to the mock
    EXPECT_GT(mockWriter->getNumRows(), 0)
        << "Mock writer should have captured rows";
    EXPECT_TRUE(mockWriter->isFinalized())
        << "Mock writer should be finalized (noMoreData called)";

    const auto& capturedData = mockWriter->getCapturedData();
    EXPECT_FALSE(capturedData.empty())
        << "Mock writer should have captured data vectors";

    LOG(INFO) << "✓ Mock writer verified: " << capturedPath << " with "
              << mockWriter->getNumRows() << " rows in " << capturedData.size()
              << " batches";
  }
}

TEST_F(BroadcastWriteReplayerTest, multipleDrivers) {
  const uint32_t numDrivers = 4;
  const uint32_t numVectorsPerDriver = 10;
  const uint32_t numRowsPerVector = 100;

  auto traceDirPath = exec::test::TempDirectoryPath::create();
  const std::string traceRoot = traceDirPath->getPath();

  // Register mock operator for trace phase
  auto mockTranslator = std::make_unique<MockBroadcastWriteTranslator>();
  exec::Operator::registerOperator(std::move(mockTranslator));

  // Create deterministic test data
  const auto testVectors =
      makeDeterministicVectors(numVectorsPerDriver, numRowsPerVector);

  const auto outputType = ROW({"c0", "c1"}, {INTEGER(), VARCHAR()});

  // Create broadcast write plan with mock operator
  auto originalBasePathDir = exec::test::TempDirectoryPath::create();
  const std::string originalBasePath = originalBasePathDir->getPath();
  const uint64_t maxBroadcastBytes = 1024 * 1024;

  std::string broadcastWriteNodeId;
  auto plan =
      PlanBuilder()
          .values(testVectors)
          .addNode([&](std::string id, core::PlanNodePtr input) {
            broadcastWriteNodeId = id;
            return std::make_shared<BroadcastWriteNode>(
                id, originalBasePath, maxBroadcastBytes, outputType, input);
          })
          .planNode();

  // PHASE 1: Run trace phase with multiple drivers
  std::shared_ptr<Task> task;
  auto traceResult =
      AssertQueryBuilder(duckDbQueryRunner_)
          .plan(plan)
          .maxDrivers(numDrivers)
          .config(core::QueryConfig::kQueryTraceEnabled, true)
          .config(core::QueryConfig::kQueryTraceDir, traceRoot)
          .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
          .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
          .config(core::QueryConfig::kQueryTraceNodeId, broadcastWriteNodeId)
          .copyResults(pool(), task);

  // VERIFY TRACE METADATA: Check trace summaries across drivers that processed
  // data
  const auto taskTraceDir = exec::trace::getTaskTraceDirectory(
      traceRoot, task->queryCtx()->queryId(), task->taskId());
  const auto expectedTracedRows = numVectorsPerDriver * numRowsPerVector;
  uint32_t totalTracedRows = 0;

  // Collect driver IDs that actually have trace data
  std::vector<std::string> validDriverIds;

  // Not all drivers may have processed data - only check the ones that did
  for (auto driverId = 0; driverId < numDrivers; ++driverId) {
    const auto opTraceDir = exec::trace::getOpTraceDirectory(
        taskTraceDir,
        broadcastWriteNodeId,
        /*pipelineId=*/0,
        driverId);

    // Check if this driver has a trace (it may not if it didn't process any
    // data)
    auto fs = filesystems::getFileSystem(opTraceDir, nullptr);
    if (!fs->exists(opTraceDir)) {
      continue;
    }

    validDriverIds.push_back(std::to_string(driverId));

    const auto summary =
        exec::trace::OperatorTraceSummaryReader(opTraceDir, pool()).read();
    ASSERT_EQ(summary.opType, "MockBroadcastWrite");
    totalTracedRows += summary.inputRows;
  }

  // Total traced rows should equal expected (data was distributed across
  // drivers)
  ASSERT_EQ(totalTracedRows, expectedTracedRows)
      << "Total traced rows across all drivers should match expected";
  ASSERT_NE(traceResult, nullptr);

  // Ensure we found driver with trace data
  ASSERT_FALSE(validDriverIds.empty())
      << "At least one driver should have trace data";

  // Run replay with TEST operator that captures writes
  exec::Operator::unregisterAllOperators();
  exec::Operator::registerOperator(
      std::make_unique<TestBroadcastWriteTranslator>());

  // Create a separate temporary directory for replay output
  auto replayOutputDirPath = exec::test::TempDirectoryPath::create();
  const std::string replayOutputDir = replayOutputDirPath->getPath();

  // Replay only the drivers that have trace data
  const std::string driverIds = folly::join(",", validDriverIds);
  LOG(INFO) << "Replaying drivers: " << driverIds;
  const uint64_t queryCapacity = 1024 * 1024;

  // Clear any previous mock writers before replay
  clearMockWriters();

  BroadcastWriteReplayer replayer(
      traceRoot,
      task->queryCtx()->queryId(),
      task->taskId(),
      broadcastWriteNodeId,
      "BroadcastWrite",
      driverIds,
      queryCapacity,
      executor_.get(),
      replayOutputDir);

  auto replayResult = replayer.run();
  ASSERT_NE(replayResult, nullptr);

  // Verify path redirection via mock captures
  auto mockWriters = getMockWriters();
  ASSERT_FALSE(mockWriters.empty())
      << "No mock writers were created during replay";

  uint64_t totalCapturedRows = 0;
  for (const auto& mockWriter : mockWriters) {
    // Verify each mock was created with replayOutputDir (not
    // originalBasePath)
    const auto& capturedPath = mockWriter->getFilePath();
    EXPECT_TRUE(capturedPath.find(replayOutputDir) != std::string::npos)
        << "Mock writer path should contain replayOutputDir. Got: "
        << capturedPath << ", expected to contain: " << replayOutputDir;

    EXPECT_TRUE(capturedPath.find(originalBasePath) == std::string::npos)
        << "Mock writer path should NOT contain originalBasePath. Got: "
        << capturedPath << ", should not contain: " << originalBasePath;

    // 2. Verify data was actually written
    EXPECT_GT(mockWriter->getNumRows(), 0)
        << "Mock writer should have captured rows";
    EXPECT_TRUE(mockWriter->isFinalized())
        << "Mock writer should be finalized (noMoreData called)";

    totalCapturedRows += mockWriter->getNumRows();

    LOG(INFO) << "✓ Mock writer verified: " << capturedPath << " with "
              << mockWriter->getNumRows() << " rows in "
              << mockWriter->getCapturedData().size() << " batches";
  }

  // 3. Verify that total rows across all drivers matches expected
  EXPECT_EQ(totalCapturedRows, expectedTracedRows)
      << "Total captured rows across all mock writers should match expected";

  LOG(INFO) << "✓ Multiple drivers test passed: " << mockWriters.size()
            << " mock writers captured " << totalCapturedRows << " total rows";
}
