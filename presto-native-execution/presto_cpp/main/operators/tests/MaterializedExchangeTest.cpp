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
#include <folly/Uri.h>
#include <folly/init/Init.h>

#include <boost/range/algorithm/find_if.hpp>

#include "presto_cpp/main/operators/LocalShuffle.h"
#include "presto_cpp/main/operators/MaterializedExchange.h"
#include "presto_cpp/main/operators/MaterializedOutput.h"
#include "presto_cpp/main/operators/MaterializedOutputBuffer.h"
#include "presto_cpp/main/operators/ShuffleExchangeSource.h"
#include "presto_cpp/main/operators/ShuffleRead.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/ExchangeClient.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::common::testutil;
using namespace facebook::presto;
using namespace facebook::presto::operators;
using namespace ::testing;

namespace facebook::presto::operators::test {
namespace {

std::string makeTaskId(
    const std::string& prefix,
    int num,
    const std::string& shuffleInfo = "") {
  auto url = fmt::format("batch://{}-{}", prefix, num);
  if (shuffleInfo.empty()) {
    return url;
  }
  return url + "?shuffleInfo=" + shuffleInfo;
}

void registerExchangeSource(const std::string& shuffleName) {
  exec::ExchangeSource::factories().clear();
  exec::ExchangeSource::registerFactory(
      [shuffleName](
          const std::string& taskId,
          int destination,
          const std::shared_ptr<exec::ExchangeQueue>& queue,
          memory::MemoryPool* pool) -> std::shared_ptr<exec::ExchangeSource> {
        if (!taskId.starts_with("batch://")) {
          return nullptr;
        }
        auto uri = folly::Uri(taskId);
        auto queryParams = uri.getQueryParams();
        auto it = boost::range::find_if(queryParams, [](const auto& pair) {
          return pair.first == "shuffleInfo";
        });
        EXPECT_NE(it, queryParams.end())
            << "No shuffle read info provided in taskId: " << taskId;

        return std::make_shared<ShuffleExchangeSource>(
            taskId,
            destination,
            queue,
            ShuffleInterfaceFactory::factory(shuffleName)
                ->createReader(it->second, destination, pool),
            pool);
      });
}

std::string localShuffleWriteInfo(
    const std::string& rootPath,
    uint32_t numPartitions) {
  return LocalShuffleWriteInfo{
      .rootPath = rootPath,
      .queryId = "query_id",
      .numPartitions = numPartitions,
      .shuffleId = 0,
      .sortedShuffle = false}
      .serialize();
}

std::string localShuffleReadInfo(
    const std::string& rootPath,
    uint32_t partition) {
  return LocalShuffleReadInfo{
      .rootPath = rootPath,
      .queryId = "query_id",
      .partitionIds = {fmt::format("shuffle_0_0_{}", partition)},
      .sortedShuffle = false}
      .serialize();
}

} // namespace

class MaterializedExchangeTest : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    exec::test::OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();

    exec::Operator::registerOperator(
        std::make_unique<MaterializedOutputTranslator>());
    exec::Operator::registerOperator(
        std::make_unique<MaterializedExchangeTranslator>());
    exec::Operator::registerOperator(std::make_unique<ShuffleReadTranslator>());

    shuffleName_ = std::string(LocalPersistentShuffleFactory::kShuffleName);
    exec::ExchangeSource::factories().clear();
    ShuffleInterfaceFactory::registerFactory(
        shuffleName_, std::make_unique<LocalPersistentShuffleFactory>());
    registerExchangeSource(shuffleName_);

    tempDir_ = exec::test::TempDirectoryPath::create();
  }

  void TearDown() override {
    exec::test::waitForAllTasksToBeDeleted();
    exec::ExchangeSource::factories().clear();
    exec::test::OperatorTestBase::TearDown();
  }

  std::shared_ptr<exec::Task> makeTask(
      const std::string& taskId,
      core::PlanNodePtr planNode,
      int destination) {
    auto queryCtx =
        core::QueryCtx::create(executor_.get(), core::QueryConfig({}));
    core::PlanFragment planFragment{planNode};
    return exec::Task::create(
        taskId,
        std::move(planFragment),
        destination,
        std::move(queryCtx),
        exec::Task::ExecutionMode::kParallel);
  }

  RowVectorPtr copyResultVector(const RowVectorPtr& result) {
    auto vector = std::static_pointer_cast<RowVector>(
        BaseVector::create(result->type(), result->size(), pool()));
    vector->copy(result.get(), 0, 0, result->size());
    VELOX_CHECK_EQ(vector->size(), result->size());
    return vector;
  }

  /// Run MaterializedOutput: Values -> MaterializedOutputNode.
  /// Returns expected output = input replicated numDrivers times.
  std::vector<RowVectorPtr> runExchangeWrite(
      const std::vector<RowVectorPtr>& data,
      int numPartitions,
      int numDrivers) {
    auto dataType = asRowType(data[0]->type());
    auto writeInfoStr =
        localShuffleWriteInfo(tempDir_->getPath(), numPartitions);
    auto writeInfo = LocalShuffleWriteInfo::deserialize(writeInfoStr);

    // Create writer directly with a small per-partition buffer to avoid OOM
    // in unit tests (the factory default of 256MB is too large for tests with
    // many partitions).
    constexpr uint64_t kMaxBytesPerPartition = 1 << 20; // 1MB
    auto writer = std::make_shared<LocalShuffleWriter>(
        writeInfo.rootPath,
        writeInfo.queryId,
        writeInfo.shuffleId,
        writeInfo.numPartitions,
        kMaxBytesPerPartition,
        writeInfo.sortedShuffle,
        pool());

    // Create a writer memory pool for MaterializedOutputBuffer, scoped under
    // the test's root pool to avoid consuming shared arbitrator capacity.
    auto writerPool = rootPool_->addLeafChild("writerPool");

    // Create the shared MaterializedOutputBuffer.
    constexpr size_t kMaxBufferedBytes = 1 << 20; // 1MB
    auto buffer = std::make_shared<MaterializedOutputBuffer>(
        numPartitions, writer, writerPool, kMaxBufferedBytes);

    // Build partition key expressions on column 0.
    std::vector<core::TypedExprPtr> keys{
        std::make_shared<core::FieldAccessTypedExpr>(
            dataType->childAt(0), dataType->nameOf(0))};

    // Create HashPartitionFunctionSpec on column 0.
    auto partitionFunctionSpec =
        std::make_shared<exec::HashPartitionFunctionSpec>(
            dataType, std::vector<column_index_t>{0});

    // Build plan: Values -> MaterializedOutputNode.
    auto valuesNode = exec::test::PlanBuilder().values(data, true).planNode();
    auto exchangeWriteNode = std::make_shared<MaterializedOutputNode>(
        "exchangeWrite",
        keys,
        numPartitions,
        dataType,
        partitionFunctionSpec,
        valuesNode,
        buffer);

    auto taskId = makeTaskId("write", 0);
    auto task = makeTask(taskId, exchangeWriteNode, 0);
    task->start(numDrivers);

    EXPECT_TRUE(exec::test::waitForTaskCompletion(task.get(), 10'000'000));

    // Build expected output: each driver processes the full input.
    std::vector<RowVectorPtr> expected;
    for (const auto& input : data) {
      for (int i = 0; i < numDrivers; ++i) {
        expected.push_back(input);
      }
    }
    return expected;
  }

  /// Read data from shuffle for all partitions using MaterializedExchangeNode.
  /// Returns all output vectors across all partitions.
  std::vector<RowVectorPtr> runExchangeRead(
      int numPartitions,
      const RowTypePtr& dataType) {
    std::vector<RowVectorPtr> outputVectors;

    for (int partition = 0; partition < numPartitions; ++partition) {
      auto readInfo = localShuffleReadInfo(tempDir_->getPath(), partition);

      // Build plan: MaterializedExchangeNode.
      auto plan =
          exec::test::PlanBuilder()
              .addNode(
                  [&dataType](
                      core::PlanNodeId nodeId,
                      core::PlanNodePtr /* source */) -> core::PlanNodePtr {
                    return std::make_shared<MaterializedExchangeNode>(
                        nodeId, dataType);
                  })
              .planNode();

      exec::CursorParameters params;
      params.planNode = plan;
      params.destination = partition;

      auto [taskCursor, results] =
          exec::test::readCursor(params, [&](exec::TaskCursor* taskCursor) {
            if (taskCursor->noMoreSplits()) {
              return;
            }

            auto& task = taskCursor->task();
            auto remoteSplit = std::make_shared<exec::RemoteConnectorSplit>(
                makeTaskId("read", 0, readInfo));
            task->addSplit("0", exec::Split{remoteSplit});
            task->noMoreSplits("0");
            taskCursor->setNoMoreSplits();
          });

      for (const auto& result : results) {
        auto copied = copyResultVector(result);
        outputVectors.push_back(copied);
      }
    }

    return outputVectors;
  }

  /// Clean up shuffle files in the temp directory.
  void cleanupDirectory(const std::string& rootPath) {
    auto fileSystem = filesystems::getFileSystem(rootPath, nullptr);
    auto files = fileSystem->list(rootPath);
    for (auto& file : files) {
      fileSystem->remove(file);
    }
  }

  std::string shuffleName_;
  std::shared_ptr<exec::test::TempDirectoryPath> tempDir_;
};

TEST_F(MaterializedExchangeTest, basicEndToEnd) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<std::string>({"a", "bb", "ccc", "dddd", "eeeee", "f"}),
  });

  const int numPartitions = 4;
  const int numDrivers = 2;

  auto expected = runExchangeWrite({data}, numPartitions, numDrivers);
  auto actual = runExchangeRead(numPartitions, asRowType(data->type()));

  exec::test::assertEqualResults(expected, actual);
  cleanupDirectory(tempDir_->getPath());
}

TEST_F(MaterializedExchangeTest, largeDataEndToEnd) {
  const int numRows = 10000;

  auto data = makeRowVector({
      makeFlatVector<int32_t>(numRows, [](auto row) { return row; }),
      makeFlatVector<int64_t>(numRows, [](auto row) { return row * 100; }),
      makeFlatVector<std::string>(
          numRows, [](auto row) { return fmt::format("str_{}", row); }),
      makeFlatVector<double>(numRows, [](auto row) { return row * 1.5; }),
  });

  const int numPartitions = 8;
  const int numDrivers = 4;

  auto expected = runExchangeWrite({data}, numPartitions, numDrivers);
  auto actual = runExchangeRead(numPartitions, asRowType(data->type()));

  exec::test::assertEqualResults(expected, actual);
  cleanupDirectory(tempDir_->getPath());
}

TEST_F(MaterializedExchangeTest, singlePartition) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({10, 20, 30}),
      makeFlatVector<std::string>({"x", "y", "z"}),
  });

  const int numPartitions = 1;
  const int numDrivers = 1;

  auto expected = runExchangeWrite({data}, numPartitions, numDrivers);
  auto actual = runExchangeRead(numPartitions, asRowType(data->type()));

  exec::test::assertEqualResults(expected, actual);
  cleanupDirectory(tempDir_->getPath());
}

TEST_F(MaterializedExchangeTest, manyPartitions) {
  const int numRows = 500;

  auto data = makeRowVector({
      makeFlatVector<int32_t>(numRows, [](auto row) { return row; }),
      makeFlatVector<int64_t>(numRows, [](auto row) { return row * 7; }),
  });

  const int numPartitions = 16;
  const int numDrivers = 1;

  auto expected = runExchangeWrite({data}, numPartitions, numDrivers);
  auto actual = runExchangeRead(numPartitions, asRowType(data->type()));

  exec::test::assertEqualResults(expected, actual);
  cleanupDirectory(tempDir_->getPath());
}

TEST_F(MaterializedExchangeTest, emptyInput) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(0, [](auto row) { return row; }),
      makeFlatVector<std::string>(0, [](auto /*row*/) { return ""; }),
  });

  const int numPartitions = 4;
  const int numDrivers = 1;

  auto expected = runExchangeWrite({data}, numPartitions, numDrivers);
  auto actual = runExchangeRead(numPartitions, asRowType(data->type()));

  // Both expected and actual should be empty.
  ASSERT_TRUE(expected.empty() || expected[0]->size() == 0);
  ASSERT_TRUE(actual.empty());
  cleanupDirectory(tempDir_->getPath());
}

// Verify that buffered RowGroups are tracked through the pool. With a
// constrained pool, enqueuing too much data without draining should fail.
TEST_F(MaterializedExchangeTest, bufferMemoryTracking) {
  const int32_t numPartitions = 100;
  // 2MB pool — enough for ~200 enqueues of 10KB but not all 100 partitions
  // if drain doesn't happen (100 × 10KB × multiple rounds).
  constexpr int64_t kPoolCapacity = 2L * 1024 * 1024;

  auto rootPool =
      memory::memoryManager()->addRootPool("bufferMemoryTest", kPoolCapacity);
  auto bufferPool = rootPool->addLeafChild("buffer");

  auto shuffleDir = exec::test::TempDirectoryPath::create();
  auto writeInfo = localShuffleWriteInfo(shuffleDir->getPath(), numPartitions);
  auto writer = ShuffleInterfaceFactory::factory(shuffleName_)
                    ->createWriter(writeInfo, pool());

  auto buffer = std::make_shared<MaterializedOutputBuffer>(
      numPartitions,
      std::shared_ptr<ShuffleWriter>(std::move(writer)),
      std::move(bufferPool),
      /*maxBufferedBytes=*/100L * 1024 * 1024,
      /*partitionDrainThreshold=*/50L * 1024);

  // Enqueue 10KB per partition across many rounds. Pool has 2MB, drain
  // threshold is 50KB so partitions won't drain until 50KB accumulated.
  // After ~200 enqueues (200 × 10KB = 2MB) the pool should OOM.
  bool threw = false;
  int32_t totalEnqueued = 0;
  for (int32_t round = 0; round < 10 && !threw; ++round) {
    for (int32_t p = 0; p < numPartitions && !threw; ++p) {
      try {
        auto iobuf = buffer->allocateTrackedIOBuf(10 * 1024);
        std::memset(iobuf->writableData(), 'x', 10 * 1024);
        iobuf->append(10 * 1024);
        velox::ContinueFuture future;
        buffer->enqueue(p, std::move(iobuf), &future);
        ++totalEnqueued;
      } catch (const velox::VeloxRuntimeError&) {
        threw = true;
      }
    }
  }
  EXPECT_TRUE(threw) << "Expected OOM but enqueued all " << totalEnqueued;
  EXPECT_GT(totalEnqueued, 0) << "Should have enqueued some data";

  // Pool should show tracked usage from the successful enqueues.
  EXPECT_GT(buffer->pool()->usedBytes(), 0);
  LOG(INFO) << "Enqueued " << totalEnqueued << " times before OOM, "
            << "pool used: "
            << velox::succinctBytes(buffer->pool()->usedBytes());

  buffer->abort();
  cleanupDirectory(shuffleDir->getPath());
}

} // namespace facebook::presto::operators::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  // folly::Init MUST run before any folly::Singleton (e.g. Timekeeper) is
  // touched. Without this, gtest_main's default main() lets SetUpTestCase
  // hit the Timekeeper singleton via PeriodicStatsReporter and abort the
  // process under OSS CMake builds.
  folly::Init follyInit{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
