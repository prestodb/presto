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
#include "folly/init/Init.h"

#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/main/operators/LocalShuffle.h"
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/operators/ShuffleExchangeSource.h"
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "presto_cpp/main/operators/ShuffleWrite.h"
#include "presto_cpp/main/operators/tests/PlanBuilder.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/ExchangeClient.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/row/CompactRow.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::common::testutil;
using namespace facebook::presto;
using namespace facebook::presto::operators;
using namespace ::testing;

namespace facebook::presto::operators::test {

namespace {

static const uint64_t kFakeBackgroundCpuTimeMs = 123;

struct TestShuffleInfo {
  uint32_t numPartitions;
  uint32_t maxBytesPerPartition;

  static TestShuffleInfo deserializeShuffleInfo(const std::string& info) {
    auto jsonReadInfo = nlohmann::json::parse(info);
    TestShuffleInfo shuffleInfo;
    jsonReadInfo.at("numPartitions").get_to(shuffleInfo.numPartitions);
    jsonReadInfo.at("maxBytesPerPartition")
        .get_to(shuffleInfo.maxBytesPerPartition);
    return shuffleInfo;
  }
};

std::vector<int> getSortOrder(const std::vector<std::string>& keys) {
  std::vector<int> indices(keys.size());
  std::iota(indices.begin(), indices.end(), 0);
  std::sort(indices.begin(), indices.end(), [&keys](int a, int b) {
    return compareKeys(keys[a], keys[b]) == std::strong_ordering::less;
  });
  return indices;
}

class TestShuffleWriter : public ShuffleWriter {
 public:
  TestShuffleWriter(
      memory::MemoryPool* pool,
      uint32_t numPartitions,
      uint32_t maxBytesPerPartition,
      uint32_t maxKeyBytes = 1024) // 1KB
      : pool_(pool),
        numPartitions_(numPartitions),
        maxBytesPerPartition_(maxBytesPerPartition),
        maxKeyBytes_(maxKeyBytes),
        inProgressSizes_(numPartitions, 0),
        readyPartitions_(
            std::make_shared<
                std::vector<std::vector<std::unique_ptr<ReadBatch>>>>()),
        serializedSortKeys_(
            std::make_shared<std::vector<std::vector<std::string>>>()) {
    inProgressPartitions_.resize(numPartitions_);
    readyPartitions_->resize(numPartitions_);
    serializedSortKeys_->resize(numPartitions_);
  }

  void initialize(velox::memory::MemoryPool* pool) {
    if (pool_ == nullptr) {
      pool_ = pool;
    }
  }

  void collect(int32_t partition, std::string_view key, std::string_view data)
      override {
    TestValue::adjust(
        "facebook::presto::operators::test::TestShuffleWriter::collect", this);

    auto& readBatch = inProgressPartitions_[partition];
    TRowSize rowSize = data.size();
    auto size = sizeof(TRowSize) + rowSize;

    // Check if there is enough space in the buffer.
    if (readBatch &&
        inProgressSizes_[partition] + size >= maxBytesPerPartition_) {
      readBatch->data->setSize(inProgressSizes_[partition]);
      (*readyPartitions_)[partition].emplace_back(std::move(readBatch));
      inProgressPartitions_[partition].reset();
    }

    // Allocate buffer if needed.
    if (readBatch == nullptr) {
      auto buffer = AlignedBuffer::allocate<char>(maxBytesPerPartition_, pool_);
      VELOX_CHECK_NOT_NULL(buffer);
      readBatch = std::make_unique<ReadBatch>(
          std::vector<std::string_view>{}, std::move(buffer));
      inProgressPartitions_[partition] = std::move(readBatch);
      inProgressSizes_[partition] = 0;
    }

    // Copy data.
    auto offset = inProgressSizes_[partition];
    auto* rawBuffer = readBatch->data->asMutable<char>() + offset;

    *(TRowSize*)(rawBuffer) = folly::Endian::big(rowSize);
    ::memcpy(rawBuffer + sizeof(TRowSize), data.data(), rowSize);
    readBatch->rows.push_back(
        std::string_view(rawBuffer + sizeof(TRowSize), rowSize));
    inProgressSizes_[partition] += size;

    if (!key.empty()) {
      serializedSortKeys_->at(partition).emplace_back(key);
    }
  }

  void noMoreData(bool success) override {
    VELOX_CHECK(success, "Unexpected error");
    // Flush in-progress buffers.
    for (auto i = 0; i < numPartitions_; ++i) {
      if (inProgressSizes_[i] > 0) {
        auto& readBatch = inProgressPartitions_[i];
        readBatch->data->setSize(inProgressSizes_[i]);
        (*readyPartitions_)[i].emplace_back(std::move(readBatch));
        inProgressPartitions_[i].reset();
      }
    }
  }

  folly::F14FastMap<std::string, int64_t> stats() const override {
    return {
        {"test-shuffle.write", 1002},
        {exec::ExchangeClient::kBackgroundCpuTimeMs, kFakeBackgroundCpuTimeMs}};
  }

  std::shared_ptr<std::vector<std::vector<std::unique_ptr<ReadBatch>>>>&
  readyPartitions() {
    return readyPartitions_;
  }

  std::shared_ptr<std::vector<std::vector<std::string>>>& serializedSortKeys() {
    return serializedSortKeys_;
  }

  static void reset() {
    getInstance().reset();
  }

  /// Maintains a single shuffle write interface for testing purpose.
  static std::shared_ptr<TestShuffleWriter>& getInstance() {
    static std::shared_ptr<TestShuffleWriter> instance_;
    return instance_;
  }

  static std::shared_ptr<TestShuffleWriter> createWriter(
      const std::string& serializedShuffleInfo,
      velox::memory::MemoryPool* pool) {
    std::shared_ptr<TestShuffleWriter>& instance = getInstance();
    if (instance) {
      return instance;
    }
    TestShuffleInfo writeInfo =
        TestShuffleInfo::deserializeShuffleInfo(serializedShuffleInfo);
    // We need only one instance for the shuffle since it's in-memory.
    instance = std::make_shared<TestShuffleWriter>(
        pool, writeInfo.numPartitions, writeInfo.maxBytesPerPartition);
    return instance;
  }

 private:
  memory::MemoryPool* pool_{nullptr};
  const uint32_t numPartitions_;
  const uint32_t maxBytesPerPartition_;
  const uint32_t maxKeyBytes_;

  /// Indexed by partition number. Each element represents currently being
  /// accumulated buffer by shuffler for a certain partition. Internal layout:
  /// | row-size | ..row-payload.. | row-size | ..row-payload.. | ..
  std::vector<std::unique_ptr<ReadBatch>> inProgressPartitions_;

  /// Tracks the total size of each in-progress partition in
  /// inProgressPartitions_
  std::vector<size_t> inProgressSizes_;
  std::shared_ptr<std::vector<std::vector<std::unique_ptr<ReadBatch>>>>
      readyPartitions_;
  std::shared_ptr<std::vector<std::vector<std::string>>> serializedSortKeys_;
};

class TestShuffleReader : public ShuffleReader {
 public:
  TestShuffleReader(
      const int32_t partition,
      const std::shared_ptr<
          std::vector<std::vector<std::unique_ptr<ReadBatch>>>>&
          readyPartitions)
      : partition_(partition), readyPartitions_(readyPartitions) {}

  folly::SemiFuture<std::vector<std::unique_ptr<ReadBatch>>> next(
      uint64_t maxBytes) override {
    VELOX_CHECK_GT(maxBytes, 0, "maxBytes must be greater than 0");
    TestValue::adjust(
        "facebook::presto::operators::test::TestShuffleReader::next", this);
    std::vector<std::unique_ptr<ReadBatch>> result;
    auto& partitionBatches = (*readyPartitions_)[partition_];

    if (!partitionBatches.empty()) {
      result.push_back(std::move(partitionBatches.back()));
      partitionBatches.pop_back();
    }

    for (size_t totalBytes = 0;
         totalBytes < maxBytes && !partitionBatches.empty();) {
      result.push_back(std::move(partitionBatches.back()));
      totalBytes += result.back()->data->size();
      partitionBatches.pop_back();
    }

    return folly::makeSemiFuture(std::move(result));
  }

  void noMoreData(bool success) override {
    VELOX_CHECK(success, "Unexpected error");
  }

  folly::F14FastMap<std::string, int64_t> stats() const override {
    return {
        {"test-shuffle.read", 1032},
        {exec::ExchangeClient::kBackgroundCpuTimeMs, kFakeBackgroundCpuTimeMs}};
  }

 private:
  const int32_t partition_;
  const std::shared_ptr<std::vector<std::vector<std::unique_ptr<ReadBatch>>>>&
      readyPartitions_;
};

class TestShuffleFactory : public ShuffleInterfaceFactory {
 public:
  static constexpr std::string_view kShuffleName = "test-shuffle";

  std::shared_ptr<ShuffleReader> createReader(
      const std::string& /* serializedShuffleInfo */,
      const int partition,
      velox::memory::MemoryPool* pool) override {
    return std::make_shared<TestShuffleReader>(
        partition, TestShuffleWriter::getInstance()->readyPartitions());
  }

  std::shared_ptr<ShuffleWriter> createWriter(
      const std::string& serializedShuffleInfo,
      velox::memory::MemoryPool* pool) override {
    return TestShuffleWriter::createWriter(serializedShuffleInfo, pool);
  }
};

void registerExchangeSource(const std::string& shuffleName) {
  exec::ExchangeSource::factories().clear();
  exec::ExchangeSource::registerFactory(
      [shuffleName](
          const std::string& taskId,
          int destination,
          const std::shared_ptr<exec::ExchangeQueue>& queue,
          memory::MemoryPool* pool) -> std::shared_ptr<exec::ExchangeSource> {
        if (strncmp(taskId.c_str(), "batch://", 8) == 0) {
          auto uri = folly::Uri(taskId);
          for (auto& pair : uri.getQueryParams()) {
            if (pair.first == "shuffleInfo") {
              return std::make_shared<ShuffleExchangeSource>(
                  taskId,
                  destination,
                  queue,
                  ShuffleInterfaceFactory::factory(shuffleName)
                      ->createReader(pair.second, destination, pool),
                  pool);
            }
          }
          VELOX_USER_FAIL(
              "No shuffle read info provided in taskId. taskId: {}", taskId);
        }
        return nullptr;
      });
}
} // namespace

class ShuffleTest : public exec::test::OperatorTestBase {
 public:
  std::string testShuffleInfo(
      uint32_t numPartitions,
      uint32_t maxBytesPerPartition) {
    static constexpr std::string_view kTemplate =
        "{{\n"
        "  \"numPartitions\": {},\n"
        "  \"maxBytesPerPartition\": {}\n"
        "}}";
    return fmt::format(kTemplate, numPartitions, maxBytesPerPartition);
  }

  std::string localShuffleWriteInfo(
      const std::string& rootPath,
      uint32_t numPartitions) {
    static constexpr std::string_view kTemplate =
        "{{\n"
        "  \"rootPath\": \"{}\",\n"
        "  \"queryId\": \"query_id\",\n"
        "  \"shuffleId\": 0,\n"
        "  \"numPartitions\": {}\n"
        "}}";
    return fmt::format(kTemplate, rootPath, numPartitions);
  }

  std::string localShuffleReadInfo(
      const std::string& rootPath,
      uint32_t numPartitions,
      uint32_t partition) {
    static constexpr std::string_view kTemplate =
        "{{\n"
        "  \"rootPath\": \"{}\",\n"
        "  \"queryId\": \"query_id\",\n"
        "  \"partitionIds\": [ \"shuffle_0_0_{}\" ]\n"
        "}}";
    return fmt::format(kTemplate, rootPath, partition, numPartitions);
  }

 protected:
  void SetUp() override {
    exec::test::OperatorTestBase::SetUp();
    velox::filesystems::registerLocalFileSystem();
    ShuffleInterfaceFactory::registerFactory(
        std::string(TestShuffleFactory::kShuffleName),
        std::make_unique<TestShuffleFactory>());
    ShuffleInterfaceFactory::registerFactory(
        std::string(LocalPersistentShuffleFactory::kShuffleName),
        std::make_unique<LocalPersistentShuffleFactory>());
    exec::Operator::registerOperator(
        std::make_unique<PartitionAndSerializeTranslator>());
    exec::Operator::registerOperator(
        std::make_unique<ShuffleWriteTranslator>());
  }

  void TearDown() override {
    TestShuffleWriter::reset();
    exec::test::waitForAllTasksToBeDeleted();
    exec::test::OperatorTestBase::TearDown();
  }

  static std::string makeTaskId(
      const std::string& prefix,
      int num,
      const std::string& shuffleInfo = "") {
    auto url = fmt::format("batch://{}-{}", prefix, num);
    if (shuffleInfo.empty()) {
      return url;
    }
    return url + "?shuffleInfo=" + shuffleInfo;
  }

  std::shared_ptr<exec::Task> makeTask(
      const std::string& taskId,
      core::PlanNodePtr planNode,
      int destination,
      core::QueryConfig&& queryConfig) {
    auto queryCtx =
        core::QueryCtx::create(executor_.get(), std::move(queryConfig));
    core::PlanFragment planFragment{planNode};
    return exec::Task::create(
        taskId,
        std::move(planFragment),
        destination,
        std::move(queryCtx),
        exec::Task::ExecutionMode::kParallel);
  }

  RowVectorPtr deserialize(
      const RowVectorPtr& serializedResult,
      const RowTypePtr& rowType) {
    auto serializedData =
        serializedResult->childAt(2)->as<FlatVector<StringView>>();
    auto* rawValues = serializedData->rawValues();

    std::vector<std::string_view> rows;
    rows.reserve(serializedData->size());
    for (auto i = 0; i < serializedData->size(); ++i) {
      const auto& serializedRow = rawValues[i];
      rows.push_back(
          std::string_view(serializedRow.data(), serializedRow.size()));
    }

    return std::dynamic_pointer_cast<RowVector>(
        row::CompactRow::deserialize(rows, rowType, pool()));
  }

  RowVectorPtr copyResultVector(const RowVectorPtr& result) {
    auto vector = std::static_pointer_cast<RowVector>(
        BaseVector::create(result->type(), result->size(), pool()));
    vector->copy(result.get(), 0, 0, result->size());
    VELOX_CHECK_EQ(vector->size(), result->size());
    return vector;
  }

  void testPartitionAndSerialize(
      const RowVectorPtr& data,
      const VectorPtr& expectedReplicate = nullptr) {
    const bool replicateNullsAndAny = expectedReplicate != nullptr;
    auto plan =
        exec::test::PlanBuilder()
            .values({data})
            .addNode(addPartitionAndSerializeNode(7, replicateNullsAndAny))
            .planNode();

    auto results = exec::test::AssertQueryBuilder(plan).copyResults(pool());

    // Verify partition values are in [0, 7) range.
    auto partitions = results->childAt(0)->as<SimpleVector<int32_t>>();
    for (auto i = 0; i < results->size(); ++i) {
      ASSERT_GE(partitions->valueAt(i), 0);
      ASSERT_LT(partitions->valueAt(i), 7);
    }

    // Verify 'data'.
    auto deserialized = deserialize(results, asRowType(data->type()));
    velox::test::assertEqualVectors(data, deserialized);

    // Verify 'replicate' flags.
    if (replicateNullsAndAny) {
      velox::test::assertEqualVectors(results->childAt(3), expectedReplicate);
    }
  }

  void testPartitionAndSerialize(
      const core::PlanNodePtr& plan,
      const RowVectorPtr& expected,
      const exec::CursorParameters params,
      const std::optional<uint32_t> expectedOutputCount = std::nullopt) {
    auto [taskCursor, serializedResults] = exec::test::readCursor(params);

    RowVectorPtr result =
        BaseVector::create<RowVector>(expected->type(), 0, pool());
    for (auto& serializedResult : serializedResults) {
      // Verify that serialized data can be deserialized successfully into the
      // original data.
      auto deserialized =
          deserialize(serializedResult, asRowType(expected->type()));
      if (deserialized != nullptr) {
        result->append(deserialized.get());
      }
    }
    velox::test::assertEqualVectors(expected, result);
    if (expectedOutputCount) {
      ASSERT_EQ(expectedOutputCount.value(), serializedResults.size());
    }
  }

  void testPartitionAndSerialize(
      const core::PlanNodePtr& plan,
      const RowVectorPtr& expected) {
    exec::CursorParameters params;
    params.planNode = plan;
    params.maxDrivers = 2;
    testPartitionAndSerialize(plan, expected, params);
  }

  std::pair<std::unique_ptr<exec::TaskCursor>, std::vector<RowVectorPtr>>
  runShuffleReadTask(
      const exec::CursorParameters& params,
      const std::string& shuffleInfo) {
    return exec::test::readCursor(params, [&](exec::TaskCursor* taskCursor) {
      if (taskCursor->noMoreSplits()) {
        return;
      }

      auto& task = taskCursor->task();
      auto remoteSplit = std::make_shared<exec::RemoteConnectorSplit>(
          makeTaskId("read", 0, shuffleInfo));
      task->addSplit("0", exec::Split{remoteSplit});
      task->noMoreSplits("0");
      taskCursor->setNoMoreSplits();
    });
  }

  void runShuffleTest(
      const std::string& shuffleName,
      const std::string& serializedShuffleWriteInfo,
      const std::function<std::string(uint32_t partition)>&
          serializedShuffleReadInfo,
      bool replicateNullsAndAny,
      size_t numPartitions,
      size_t numMapDrivers,
      const std::vector<RowVectorPtr>& data,
      uint64_t backgroundCpuTimeNanos = 0,
      const std::optional<std::vector<velox::core::SortOrder>>& sortOrders =
          std::nullopt,
      const std::optional<std::vector<velox::core::FieldAccessTypedExprPtr>>&
          fields = std::nullopt,
      const std::optional<std::vector<std::vector<int>>>& expectedOrdering = {},
      core::QueryConfig&& queryConfig = core::QueryConfig({})) {
    // Register new shuffle related operators.
    exec::Operator::registerOperator(
        std::make_unique<PartitionAndSerializeTranslator>());
    exec::Operator::registerOperator(
        std::make_unique<ShuffleWriteTranslator>());
    exec::Operator::registerOperator(std::make_unique<ShuffleReadTranslator>());

    // Flatten the inputs to avoid issues assertEqualResults referred here:
    // https://github.com/facebookincubator/velox/issues/2859
    auto dataType = asRowType(data[0]->type());

    auto writerPlan =
        exec::test::PlanBuilder()
            .values(data, true)
            .addNode(addPartitionAndSerializeNode(
                numPartitions, replicateNullsAndAny, {}, sortOrders, fields))
            .localPartition(std::vector<std::string>{})
            .addNode(addShuffleWriteNode(
                numPartitions, shuffleName, serializedShuffleWriteInfo))
            .planNode();

    auto writerTaskId = makeTaskId("leaf", 0);
    auto writerTask =
        makeTask(writerTaskId, writerPlan, 0, std::move(queryConfig));
    writerTask->start(numMapDrivers);

    ASSERT_TRUE(exec::test::waitForTaskCompletion(writerTask.get(), 5'000'000));

    // Verify that shuffle stats got propagated to the ShuffleWrite operator.
    const auto shuffleWriteOperatorStats =
        writerTask->taskStats().pipelineStats[0].operatorStats.back();
    // Test if background CPU stats have been folded into finishTiming for
    // ShuffleWrite
    ASSERT_EQ(
        backgroundCpuTimeNanos > 0 ? 1 : 0,
        shuffleWriteOperatorStats.backgroundTiming.count);
    ASSERT_EQ(
        backgroundCpuTimeNanos,
        shuffleWriteOperatorStats.backgroundTiming.cpuNanos);
    const auto shuffleRuntimeStats = shuffleWriteOperatorStats.runtimeStats;
    ASSERT_EQ(
        1, shuffleRuntimeStats.count(fmt::format("{}.write", shuffleName)));

    // NOTE: each map driver processes the input once.
    std::vector<RowVectorPtr> expectedOutputVectors;
    for (const auto& input : data) {
      for (int i = 0; i < numMapDrivers; i++) {
        expectedOutputVectors.push_back(input);
      }
    }
    std::vector<RowVectorPtr> outputVectors;
    folly::F14FastSet<int32_t> emptyPartitions;
    folly::F14FastMap<int32_t, int32_t> numNulls;
    // Create and run multiple downstream tasks, one per partition, to read data
    // from shuffle.
    for (auto partition = 0; partition < numPartitions; ++partition) {
      auto plan = exec::test::PlanBuilder()
                      .addNode(addShuffleReadNode(dataType))
                      .project(dataType->names())
                      .planNode();

      exec::CursorParameters params;
      params.planNode = plan;
      params.destination = partition;

      auto [taskCursor, results] =
          runShuffleReadTask(params, serializedShuffleReadInfo(partition));

      // Verify that shuffle stats got propagated to the Exchange operator.
      const auto shuffleReadOperatorStats =
          taskCursor->task()->taskStats().pipelineStats[0].operatorStats[0];
      // Test if background CPU stats have been folded into finishTiming for
      // ShuffleRead
      ASSERT_EQ(
          backgroundCpuTimeNanos > 0 ? 1 : 0,
          shuffleReadOperatorStats.backgroundTiming.count);
      ASSERT_EQ(
          backgroundCpuTimeNanos,
          shuffleReadOperatorStats.backgroundTiming.cpuNanos);
      const auto exchangeStats = shuffleReadOperatorStats.runtimeStats;
      ASSERT_EQ(1, exchangeStats.count(fmt::format("{}.read", shuffleName)));

      vector_size_t numResults = 0;
      for (const auto& result : results) {
        outputVectors.push_back(copyResultVector(result));
        numResults += result->size();
        numNulls[partition] += countNulls(result->childAt(0));
      }
      if (numResults == 0) {
        emptyPartitions.insert(partition);
      }
    }

    if (replicateNullsAndAny) {
      EXPECT_EQ(emptyPartitions.size(), 0);
      ASSERT_EQ(numPartitions, numNulls.size());

      vector_size_t expectedNullCount = 0;
      for (const auto& input : data) {
        expectedNullCount += countNulls(input->childAt(0));
      }
      expectedNullCount *= numMapDrivers;

      for (auto i = 0; i < numPartitions; ++i) {
        ASSERT_TRUE(numNulls.contains(i));
        ASSERT_EQ(expectedNullCount, numNulls[i]);
      }

      // TODO: Add assertContainResults for the remaining elements
    } else {
      velox::exec::test::assertEqualResults(
          expectedOutputVectors, outputVectors);
    }

    auto shuffleWriter = TestShuffleWriter::getInstance();
    if (shuffleWriter) {
      const auto serializedSortKeys = shuffleWriter->serializedSortKeys();
      if (sortOrders && fields) {
        for (auto i = 0; i < numPartitions; ++i) {
          const auto resultSortingOrder =
              getSortOrder((*serializedSortKeys)[i]);
          EXPECT_EQ(expectedOrdering.value()[i], resultSortingOrder);
        }
      } else {
        for (auto i = 0; i < numPartitions; ++i) {
          EXPECT_TRUE((*serializedSortKeys)[i].empty());
        }
      }
    } else {
      // Sorted shuffle is not supported with local shuffle.
      EXPECT_FALSE(sortOrders && fields);
    }
  }

  static vector_size_t countNulls(const VectorPtr& vector) {
    vector_size_t numNulls = 0;
    for (auto i = 0; i < vector->size(); ++i) {
      if (vector->isNullAt(i)) {
        ++numNulls;
      }
    }
    return numNulls;
  }

  void fuzzerTest(bool replicateNullsAndAny, size_t numPartitions) {
    // For unit testing, these numbers are set to relatively small values.
    // For stress testing, the following parameters and the fuzzer vector,
    // string and container sizes can be bumped up.
    size_t numMapDrivers = 1;
    size_t numInputVectors = 5;
    size_t numIterations = 5;

    // Set up the fuzzer parameters.
    VectorFuzzer::Options opts;
    opts.vectorSize = 1000;
    opts.nullRatio = 0.1;
    opts.dictionaryHasNulls = false;
    opts.stringVariableLength = true;

    // UnsafeRows use microseconds to store timestamp.
    opts.timestampPrecision =
        VectorFuzzer::Options::TimestampPrecision::kMicroSeconds;
    opts.stringLength = 100;
    opts.containerLength = 10;

    // For the time being, we are not including any MAP or more than three level
    // nested data structures given the limitations of the fuzzer and
    // assertEqualResults:
    // Limitations of assertEqualResults:
    // https://github.com/facebookincubator/velox/issues/2859
    auto rowType = ROW({
        {"c0", INTEGER()},
        {"c1", TINYINT()},
        {"c2", INTEGER()},
        {"c3", BIGINT()},
        {"c4", INTEGER()},
        {"c5", TIMESTAMP()},
        {"c6", REAL()},
        {"c7", TINYINT()},
        {"c8", DOUBLE()},
        {"c9", VARCHAR()},
        {"c10", ROW({VARCHAR(), INTEGER(), TIMESTAMP()})},
        {"c11", INTEGER()},
        {"c12", REAL()},
        {"c13", ARRAY(INTEGER())},
        {"c14", ARRAY(TINYINT())},
        {"c15", ROW({INTEGER(), VARCHAR(), ARRAY(INTEGER())})},
        {"c16", MAP(TINYINT(), REAL())},
    });

    auto rootDirectory = velox::exec::test::TempDirectoryPath::create();
    auto rootPath = rootDirectory->getPath();
    const std::string shuffleWriteInfo =
        localShuffleWriteInfo(rootPath, numPartitions);

    for (int it = 0; it < numIterations; it++) {
      auto seed = folly::Random::rand32();

      SCOPED_TRACE(
          fmt::format(
              "Iteration {}, numPartitions {}, replicateNullsAndAny {}, seed {}",
              it,
              numPartitions,
              replicateNullsAndAny,
              seed));

      VectorFuzzer fuzzer(opts, pool_.get(), seed);
      std::vector<RowVectorPtr> inputVectors;
      // Create input vectors.
      for (size_t i = 0; i < numInputVectors; ++i) {
        auto input = fuzzer.fuzzInputRow(rowType);
        inputVectors.push_back(input);
      }
      velox::exec::ExchangeSource::factories().clear();
      registerExchangeSource(
          std::string(LocalPersistentShuffleFactory::kShuffleName));
      runShuffleTest(
          std::string(LocalPersistentShuffleFactory::kShuffleName),
          shuffleWriteInfo,
          [&](auto partition) {
            return localShuffleReadInfo(rootPath, numPartitions, partition);
          },
          replicateNullsAndAny,
          numPartitions,
          numMapDrivers,
          inputVectors);
      cleanupDirectory(rootPath);
    }
  }

  void partitionAndSerializeWithThresholds(
      vector_size_t outputRowLimit,
      size_t outputSizeLimit,
      vector_size_t inputRows,
      size_t expectedOutputCount,
      bool sorted = false) {
    VectorFuzzer::Options opts;
    opts.vectorSize = 10;
    opts.nullRatio = 0;
    opts.dictionaryHasNulls = false;
    opts.stringLength = 10000;
    opts.containerLength = 10000;
    opts.stringVariableLength = false;
    opts.containerVariableLength = false;
    auto seed = folly::Random::rand32();
    VectorFuzzer fuzzer(opts, pool_.get(), seed);
    // Create a deeply nested row, such that each row exceeds the output batch
    // limit.
    RowVectorPtr data;
    std::optional<std::vector<velox::core::SortOrder>> ordering = std::nullopt;
    std::optional<
        std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>>>
        fields = std::nullopt;

    if (sorted) {
      data = makeRowVector(
          // value
          {fuzzer.fuzzConstant(INTEGER(), inputRows),
           // sort key
           fuzzer.fuzzRow(
               {fuzzer.fuzzConstant(VARCHAR(), 100),
                fuzzer.fuzzArray(
                    fuzzer.fuzzArray(fuzzer.fuzzFlat(DOUBLE()), 100), 100)},
               inputRows)});
      ordering = {velox::core::SortOrder(velox::core::kAscNullsFirst)};
      fields =
          std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>>{
              std::make_shared<const velox::core::FieldAccessTypedExpr>(
                  INTEGER(), "c1")};
    } else {
      data = makeRowVector({fuzzer.fuzzMap(
          fuzzer.fuzzConstant(VARCHAR(), 100),
          fuzzer.fuzzArray(
              fuzzer.fuzzArray(fuzzer.fuzzFlat(DOUBLE()), 100), 100),
          inputRows)});
    }

    auto plan = exec::test::PlanBuilder()
                    .values({data}, false)
                    .addNode(addPartitionAndSerializeNode(
                        2, true, {"c0"}, ordering, fields))
                    .planNode();

    auto properties = std::unordered_map<std::string, std::string>{
        {core::QueryConfig::kPreferredOutputBatchBytes,
         std::to_string(outputSizeLimit)},
        {core::QueryConfig::kPreferredOutputBatchRows,
         std::to_string(outputRowLimit)}};

    auto queryCtx =
        core::QueryCtx::create(executor_.get(), core::QueryConfig(properties));
    auto params = exec::CursorParameters();
    params.planNode = plan;
    params.queryCtx = queryCtx;

    auto expected = makeRowVector({data->childAt(0)});
    testPartitionAndSerialize(plan, expected, params, expectedOutputCount);
  }

  void cleanupDirectory(const std::string& rootPath) {
    auto fileSystem = velox::filesystems::getFileSystem(rootPath, nullptr);
    auto files = fileSystem->list(rootPath);
    for (auto& file : files) {
      fileSystem->remove(file);
    }
  }

  void runPartitionAndSerializeSerdeTest(
      const RowVectorPtr& data,
      size_t numPartitions,
      const std::optional<std::vector<std::string>>& serdeLayout =
          std::nullopt) {
    TestShuffleWriter::reset();

    auto shuffleInfo = testShuffleInfo(numPartitions, 1 << 20);
    TestShuffleWriter::createWriter(shuffleInfo, pool());

    auto plan = exec::test::PlanBuilder()
                    .values({data}, true)
                    .addNode(addPartitionAndSerializeNode(
                        numPartitions,
                        false,
                        serdeLayout.value_or(std::vector<std::string>{})))
                    .localPartition(std::vector<std::string>{})
                    .addNode(addShuffleWriteNode(
                        numPartitions,
                        std::string(TestShuffleFactory::kShuffleName),
                        shuffleInfo))
                    .planNode();

    exec::CursorParameters params;
    params.planNode = plan;
    params.maxDrivers = 1;

    auto [taskCursor, results] = exec::test::readCursor(params);
    ASSERT_EQ(results.size(), 0);

    auto shuffleWriter = TestShuffleWriter::getInstance();
    ASSERT_NE(shuffleWriter, nullptr);

    auto readyPartitions = shuffleWriter->readyPartitions();
    ASSERT_NE(readyPartitions, nullptr);

    size_t totalRows = 0;
    for (size_t partitionIdx = 0; partitionIdx < numPartitions;
         ++partitionIdx) {
      for (const auto& batch : (*readyPartitions)[partitionIdx]) {
        totalRows += batch->rows.size();
      }
    }
    ASSERT_EQ(totalRows, data->size());

    auto expectedType = serdeLayout.has_value()
        ? createSerdeLayoutType(asRowType(data->type()), serdeLayout.value())
        : asRowType(data->type());

    std::vector<RowVectorPtr> deserializedData;
    for (size_t partitionIdx = 0; partitionIdx < numPartitions;
         ++partitionIdx) {
      for (const auto& batch : (*readyPartitions)[partitionIdx]) {
        auto deserialized = std::dynamic_pointer_cast<RowVector>(
            row::CompactRow::deserialize(batch->rows, expectedType, pool()));
        if (deserialized != nullptr && deserialized->size() > 0) {
          deserializedData.push_back(deserialized);
        }
      }
    }

    auto expected = serdeLayout.has_value()
        ? reorderColumns(data, serdeLayout.value())
        : data;
    velox::exec::test::assertEqualResults({expected}, deserializedData);
  }

 private:
  RowTypePtr createSerdeLayoutType(
      const RowTypePtr& originalType,
      const std::vector<std::string>& layout) {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    for (const auto& name : layout) {
      auto idx = originalType->getChildIdx(name);
      names.push_back(name);
      types.push_back(originalType->childAt(idx));
    }
    return ROW(std::move(names), std::move(types));
  }

  RowVectorPtr reorderColumns(
      const RowVectorPtr& data,
      const std::vector<std::string>& newLayout) {
    auto rowType = asRowType(data->type());
    std::vector<VectorPtr> columns;
    for (const auto& name : newLayout) {
      columns.push_back(data->childAt(rowType->getChildIdx(name)));
    }
    return makeRowVector(columns);
  }
};

TEST_F(ShuffleTest, operators) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4}),
      makeFlatVector<int64_t>({10, 20, 30, 40}),
  });
  auto info = testShuffleInfo(4, 1 << 20 /* 1MB */);
  auto plan = exec::test::PlanBuilder()
                  .values({data}, true)
                  .addNode(addPartitionAndSerializeNode(4, false))
                  .localPartition(std::vector<std::string>{})
                  .addNode(addShuffleWriteNode(
                      4, std::string(TestShuffleFactory::kShuffleName), info))
                  .planNode();

  exec::CursorParameters params;
  params.planNode = plan;
  params.maxDrivers = 2;

  auto [taskCursor, serializedResults] = exec::test::readCursor(params);
  ASSERT_EQ(serializedResults.size(), 0);
  TestShuffleWriter::reset();
}

DEBUG_ONLY_TEST_F(ShuffleTest, shuffleWriterExceptions) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4}),
      makeFlatVector<int64_t>({10, 20, 30, 40}),
  });
  auto info = testShuffleInfo(4, 1 << 20 /* 1MB */);

  SCOPED_TESTVALUE_SET(
      "facebook::presto::operators::test::TestShuffleWriter::collect",
      std::function<void(TestShuffleWriter*)>(
          [&](TestShuffleWriter* /*writer*/) {
            // Trigger a std::bad_function_call exception.
            std::function<bool()> nullFunction = nullptr;
            VELOX_CHECK(nullFunction());
          }));

  exec::CursorParameters params;
  params.planNode =
      exec::test::PlanBuilder()
          .values({data})
          .addNode(addPartitionAndSerializeNode(4, false))
          .addNode(addShuffleWriteNode(
              4, std::string(TestShuffleFactory::kShuffleName), info))
          .planNode();

  VELOX_ASSERT_THROW(
      exec::test::readCursor(params), "ShuffleWriter::collect failed");
}

DEBUG_ONLY_TEST_F(ShuffleTest, shuffleReaderExceptions) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4}),
      makeFlatVector<int64_t>({10, 20, 30, 40}),
  });

  auto info = testShuffleInfo(4, 1 << 20 /* 1MB */);
  TestShuffleWriter::createWriter(info, pool());

  exec::CursorParameters params;
  params.planNode =
      exec::test::PlanBuilder()
          .values({data})
          .addNode(addPartitionAndSerializeNode(2, false))
          .addNode(addShuffleWriteNode(
              2, std::string(TestShuffleFactory::kShuffleName), info))
          .planNode();

  ASSERT_NO_THROW(exec::test::readCursor(params));

  std::function<void(TestShuffleReader*)> injectFailure =
      [&](TestShuffleReader* /*reader*/) {
        // Trigger a std::bad_function_call exception.
        std::function<bool()> nullFunction = nullptr;
        VELOX_CHECK(nullFunction());
      };

  exec::Operator::registerOperator(std::make_unique<ShuffleReadTranslator>());
  registerExchangeSource(std::string(TestShuffleFactory::kShuffleName));
  params.planNode = exec::test::PlanBuilder()
                        .addNode(addShuffleReadNode(asRowType(data->type())))
                        .planNode();

  {
    SCOPED_TESTVALUE_SET(
        "facebook::presto::operators::test::TestShuffleReader::next",
        injectFailure);

    VELOX_ASSERT_THROW(
        runShuffleReadTask(params, info), "ShuffleReader::next failed");
  }
}

TEST_F(ShuffleTest, endToEnd) {
  size_t numPartitions = 5;
  size_t numMapDrivers = 2;

  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });

  // Make sure all previously registered exchange factory are gone.
  velox::exec::ExchangeSource::factories().clear();
  auto shuffleInfo = testShuffleInfo(numPartitions, 1 << 20);
  TestShuffleWriter::createWriter(shuffleInfo, pool());
  registerExchangeSource(std::string(TestShuffleFactory::kShuffleName));
  runShuffleTest(
      std::string(TestShuffleFactory::kShuffleName),
      shuffleInfo,
      [&](auto /*partition*/) { return shuffleInfo; },
      false,
      numPartitions,
      numMapDrivers,
      {data},
      kFakeBackgroundCpuTimeMs * Timestamp::kNanosecondsInMillisecond);
}

TEST_F(ShuffleTest, endToEndWithSortedShuffle) {
  size_t numPartitions = 2;
  size_t numMapDrivers = 1;

  auto batch1 = makeRowVector({
      makeFlatVector<int32_t>({0, 0, 1, 1, 1, 1}), // partition key
      makeFlatVector<int64_t>({30, 10, 20, 50, 40, 60}), // sorting column
  });

  auto batch2 = makeRowVector({
      makeFlatVector<int32_t>({0, 0, 1}), // partition key
      makeFlatVector<int64_t>({70, 80, 90}), // sorting column
  });

  auto expectedSortingOrder = {
      std::vector<int>{1, 0, 2, 3}, // partition key 0
      std::vector<int>{0, 2, 1, 3, 4}, // partition key 1
  };

  auto ordering = {velox::core::SortOrder(velox::core::kAscNullsFirst)};
  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.push_back(
      std::make_shared<const velox::core::FieldAccessTypedExpr>(
          velox::BIGINT(), fmt::format("c{}", 1)));

  // Make sure all previously registered exchange factory are gone.
  velox::exec::ExchangeSource::factories().clear();
  std::string shuffleInfo = testShuffleInfo(numPartitions, 1 << 20);
  TestShuffleWriter::createWriter(shuffleInfo, pool());
  registerExchangeSource(std::string(TestShuffleFactory::kShuffleName));
  runShuffleTest(
      std::string(TestShuffleFactory::kShuffleName),
      shuffleInfo,
      [&](auto /*partition*/) { return shuffleInfo; },
      false,
      numPartitions,
      numMapDrivers,
      {batch1, batch2},
      kFakeBackgroundCpuTimeMs * Timestamp::kNanosecondsInMillisecond,
      ordering,
      fields,
      expectedSortingOrder);
}

TEST_F(ShuffleTest, endToEndWithSortedShuffleRowLimit) {
  size_t numPartitions = 3;
  size_t numMapDrivers = 1;

  auto data = makeRowVector({
      makeFlatVector<int32_t>({0, 0, 1, 1, 1, 1, 2, 2, 2}), // partition key
      makeFlatVector<StringView>(
          {"key3",
           "key1",
           "key22",
           "key55",
           "key44",
           "key66",
           "key111",
           "key222",
           "key333"}) // sorting column
  });

  auto expectedSortingOrder = {
      std::vector<int>{1, 0}, // partition key 0
      std::vector<int>{0, 2, 1, 3}, // partition key 1
      std::vector<int>{0, 1, 2} // partition key 2
  };

  auto ordering = {velox::core::SortOrder(velox::core::kAscNullsFirst)};
  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.push_back(
      std::make_shared<const velox::core::FieldAccessTypedExpr>(
          velox::VARCHAR(), fmt::format("c{}", 1)));

  // Make sure all previously registered exchange factory are gone.
  velox::exec::ExchangeSource::factories().clear();
  std::string shuffleInfo = testShuffleInfo(numPartitions, 1 << 20);
  TestShuffleWriter::createWriter(shuffleInfo, pool());
  registerExchangeSource(std::string(TestShuffleFactory::kShuffleName));

  auto properties = std::unordered_map<std::string, std::string>{
      {core::QueryConfig::kPreferredOutputBatchBytes,
       std::to_string(1'000'000'000)},
      {core::QueryConfig::kPreferredOutputBatchRows, std::to_string(3)}};

  auto queryConfig = core::QueryConfig(properties);

  runShuffleTest(
      std::string(TestShuffleFactory::kShuffleName),
      shuffleInfo,
      [&](auto /*partition*/) { return shuffleInfo; },
      false,
      numPartitions,
      numMapDrivers,
      {data},
      kFakeBackgroundCpuTimeMs * Timestamp::kNanosecondsInMillisecond,
      ordering,
      fields,
      expectedSortingOrder,
      std::move(queryConfig));
}

TEST_F(ShuffleTest, endToEndWithReplicateNullAndAny) {
  size_t numPartitions = 9;
  size_t numMapDrivers = 2;

  auto data = makeRowVector({
      makeNullableFlatVector<int32_t>({1, 2, 3, 4, 5, 6, std::nullopt}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60, 70}),
  });

  // Make sure all previously registered exchange factory are gone.
  velox::exec::ExchangeSource::factories().clear();
  const std::string shuffleInfo = testShuffleInfo(numPartitions, 1 << 20);
  TestShuffleWriter::createWriter(shuffleInfo, pool());
  registerExchangeSource(std::string(TestShuffleFactory::kShuffleName));
  runShuffleTest(
      std::string(TestShuffleFactory::kShuffleName),
      shuffleInfo,
      [&](auto /*partition*/) { return shuffleInfo; },
      true,
      numPartitions,
      numMapDrivers,
      {data},
      kFakeBackgroundCpuTimeMs * Timestamp::kNanosecondsInMillisecond);
}

TEST_F(ShuffleTest, replicateNullsAndAny) {
  // No nulls. Expect to replicate first row.
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4}),
      makeFlatVector<int64_t>({10, 20, 30, 40}),
  });

  testPartitionAndSerialize(
      data, makeFlatVector<bool>({true, false, false, false}));

  // Nulls. Expect to replicate rows with nulls.
  data = makeRowVector({
      makeNullableFlatVector<int32_t>({1, 2, std::nullopt, std::nullopt, 5}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
  });

  testPartitionAndSerialize(
      data, makeFlatVector<bool>({false, false, true, true, false}));

  // Null in the first row.
  data = makeRowVector({
      makeNullableFlatVector<int32_t>(
          {std::nullopt, 2, std::nullopt, std::nullopt, 5}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
  });

  testPartitionAndSerialize(
      data, makeFlatVector<bool>({true, false, true, true, false}));
}

TEST_F(ShuffleTest, persistentShuffleDeser) {
  std::string serializedWriteInfo =
      "{\n"
      "  \"rootPath\": \"abc\",\n"
      "  \"queryId\": \"query_id\",\n"
      "  \"shuffleId\": 1,\n"
      "  \"numPartitions\": 11\n"
      "}";
  LocalShuffleWriteInfo shuffleWriteInfo =
      LocalShuffleWriteInfo::deserialize(serializedWriteInfo);
  EXPECT_EQ(shuffleWriteInfo.rootPath, "abc");
  EXPECT_EQ(shuffleWriteInfo.queryId, "query_id");
  EXPECT_EQ(shuffleWriteInfo.shuffleId, 1);
  EXPECT_EQ(shuffleWriteInfo.numPartitions, 11);

  serializedWriteInfo =
      "{\n"
      "  \"rootPath\": \"efg\",\n"
      "  \"queryId\": \"query_id\",\n"
      "  \"shuffleId\": 1,\n"
      "  \"numPartitions\": 12\n"
      "}";
  shuffleWriteInfo = LocalShuffleWriteInfo::deserialize(serializedWriteInfo);
  EXPECT_EQ(shuffleWriteInfo.rootPath, "efg");
  EXPECT_EQ(shuffleWriteInfo.queryId, "query_id");
  EXPECT_EQ(shuffleWriteInfo.shuffleId, 1);
  EXPECT_EQ(shuffleWriteInfo.numPartitions, 12);

  std::string serializedReadInfo =
      "{\n"
      "  \"rootPath\": \"abc\",\n"
      "  \"queryId\": \"query_id\",\n"
      "  \"partitionIds\": [ \"shuffle1\" ]\n"
      "}";
  std::vector<std::string> partitionIds{"shuffle1"};
  LocalShuffleReadInfo shuffleReadInfo =
      LocalShuffleReadInfo::deserialize(serializedReadInfo);
  EXPECT_EQ(shuffleReadInfo.rootPath, "abc");
  EXPECT_EQ(shuffleReadInfo.queryId, "query_id");
  EXPECT_EQ(shuffleReadInfo.partitionIds, partitionIds);

  std::string badSerializedInfo =
      "{\n"
      "  \"rootpath\": \"efg\",\n"
      "  \"queryId\": \"query_id\",\n"
      "  \"shuffleId\": 1\n"
      "}";
  EXPECT_THROW(
      LocalShuffleWriteInfo::deserialize(badSerializedInfo),
      nlohmann::detail::out_of_range);

  badSerializedInfo =
      "{\n"
      "  \"rootPath\": \"abc\",\n"
      "  \"queryId\": \"query_id\",\n"
      "  \"shuffleId\": \"hey-wrong-type\"\n"
      "}";
  EXPECT_THROW(
      LocalShuffleWriteInfo::deserialize(badSerializedInfo),
      nlohmann::detail::type_error);
}

TEST_F(ShuffleTest, persistentShuffle) {
  uint32_t numPartitions = 1;
  uint32_t numMapDrivers = 1;

  auto rootDirectory = velox::exec::test::TempDirectoryPath::create();
  auto rootPath = rootDirectory->getPath();

  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });

  // Make sure all previously registered exchange factory are gone.
  velox::exec::ExchangeSource::factories().clear();
  const std::string shuffleWriteInfo =
      localShuffleWriteInfo(rootPath, numPartitions);
  registerExchangeSource(
      std::string(LocalPersistentShuffleFactory::kShuffleName));
  runShuffleTest(
      std::string(LocalPersistentShuffleFactory::kShuffleName),
      shuffleWriteInfo,
      [&](auto partition) {
        return localShuffleReadInfo(rootPath, numPartitions, partition);
      },
      false,
      numPartitions,
      numMapDrivers,
      {data});
  cleanupDirectory(rootPath);
}

TEST_F(ShuffleTest, persistentShuffleBatch) {
  const uint32_t numPartitions = 1;
  const uint32_t partition = 0;

  const size_t numRows{20};
  std::vector<std::string> values{numRows};
  std::vector<std::string_view> views{numRows};
  const size_t rowSize{64};
  for (auto i = 0; i < numRows; ++i) {
    values[i] = std::string(rowSize, 'a' + i % 26);
    views[i] = values[i];
  }

  struct {
    uint64_t maxBytes;
    int expectedOutputCalls;
    bool sortedShuffle;

    std::string debugString() const {
      return fmt::format(
          "maxBytes: {}, expectedOutputCalls: {}, sortedShuffle: {}",
          maxBytes,
          expectedOutputCalls,
          sortedShuffle);
    }
  } testSettings[] = {
      {.maxBytes = 1, .expectedOutputCalls = numRows, .sortedShuffle = false},
      {.maxBytes = 100, .expectedOutputCalls = numRows, .sortedShuffle = false},
      {.maxBytes = 1 << 25, .expectedOutputCalls = 1, .sortedShuffle = false},
      {.maxBytes = 1, .expectedOutputCalls = numRows, .sortedShuffle = true},
      {.maxBytes = 100, .expectedOutputCalls = numRows, .sortedShuffle = true},
      {.maxBytes = 1 << 25, .expectedOutputCalls = 1, .sortedShuffle = true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto tempRootDir = velox::exec::test::TempDirectoryPath::create();
    auto testRootPath = tempRootDir->getPath();

    LocalShuffleWriteInfo writeInfo = LocalShuffleWriteInfo::deserialize(
        localShuffleWriteInfo(testRootPath, numPartitions));

    auto writer = std::make_shared<LocalShuffleWriter>(
        writeInfo.rootPath,
        writeInfo.queryId,
        writeInfo.shuffleId,
        writeInfo.numPartitions,
        /*maxBytesPerPartition=*/1,
        testData.sortedShuffle,
        pool());

    for (auto i = 0; i < numRows; ++i) {
      writer->collect(
          partition,
          testData.sortedShuffle ? std::string_view(values[i].data(), 8)
                                 : std::string_view{},
          views[i]);
    }
    writer->noMoreData(true);

    // Create reader
    LocalShuffleReadInfo readInfo = LocalShuffleReadInfo::deserialize(
        localShuffleReadInfo(testRootPath, numPartitions, partition));

    auto reader = std::make_shared<LocalShuffleReader>(
        readInfo.rootPath,
        readInfo.queryId,
        readInfo.partitionIds,
        testData.sortedShuffle,
        pool());
    reader->initialize();

    int numOutputCalls{0};
    int numBatches{0};
    int totalRows{0};
    // Read all batches
    while (true) {
      auto batches = reader->next(testData.maxBytes)
                         .via(folly::getGlobalCPUExecutor())
                         .get();
      if (batches.empty()) {
        break;
      }

      ++numOutputCalls;
      numBatches += batches.size();
      for (const auto& batch : batches) {
        totalRows += batch->rows.size();
      }
    }

    reader->noMoreData(true);

    // Verify we read all rows.
    ASSERT_EQ(totalRows, numRows);
    // ASSERT_EQ(numBatches, numRows);
    //  Verify number of output batches.
    ASSERT_EQ(numOutputCalls, testData.expectedOutputCalls);
    cleanupDirectory(testRootPath);
  }
}

TEST_F(ShuffleTest, persistentShuffleFuzz) {
  fuzzerTest(false, 1);
  fuzzerTest(false, 3);
  fuzzerTest(false, 7);
}

TEST_F(ShuffleTest, persistentShuffleFuzzWithReplicateNullsAndAny) {
  fuzzerTest(true, 1);
  fuzzerTest(true, 3);
  fuzzerTest(true, 7);
}

TEST_F(ShuffleTest, partitionAndSerializeOutputByteLimit) {
  partitionAndSerializeWithThresholds(10'000, 1, 10, 10);
}

TEST_F(ShuffleTest, partitionAndSerializeOutputRowLimit) {
  partitionAndSerializeWithThresholds(5, 1'000'000'000, 10, 2);
}

TEST_F(ShuffleTest, partitionAndSerializeOutputRowLimitWithSort) {
  partitionAndSerializeWithThresholds(5, 1'000'000'000, 10, 2, true);
}

TEST_F(ShuffleTest, partitionAndSerializeOutputByteLimitWithSort) {
  partitionAndSerializeWithThresholds(10'000, 100, 10, 10, true);
}

TEST_F(ShuffleTest, partitionAndSerializeNoLimit) {
  partitionAndSerializeWithThresholds(1'000, 1'000'000'000, 5, 1);
}

TEST_F(ShuffleTest, partitionAndSerializeBothLimited) {
  partitionAndSerializeWithThresholds(1, 1'000'000, 5, 5);
}

TEST_F(ShuffleTest, partitionAndSerializeOperator) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data}, false)
                  .addNode(addPartitionAndSerializeNode(4, false))
                  .planNode();

  testPartitionAndSerialize(plan, data);
}

TEST_F(ShuffleTest, partitionAndSerializeWithLargeInput) {
  auto data = makeRowVector(
      {makeFlatVector<int32_t>(20'000, [](auto row) { return row; })});

  auto plan = exec::test::PlanBuilder()
                  .values({data}, false)
                  .addNode(addPartitionAndSerializeNode(1, true))
                  .planNode();

  testPartitionAndSerialize(plan, data);
}

// Test the write path of sorted shuffle by directly reading files from disk.
// This test and parseShuffleRows only for testing the correctness of the
// LocalShuffleWriter::collect()
TEST_F(ShuffleTest, localShuffleWriterSortedOutput) {
  const uint32_t numPartitions = 2;
  auto rootDirectory = velox::exec::test::TempDirectoryPath::create();
  const auto& rootPath = rootDirectory->getPath();
  const std::vector<std::string> keys = {
      "key3", "key1", "key5", "key2", "key4", "key6"};
  const std::vector<std::string> values = {
      "data3", "data1", "data5", "data2", "data4", "data6"};
  const std::vector<std::vector<std::pair<std::string, std::string>>> expected =
      {{{"key3", "data3"}, {"key4", "data4"}, {"key5", "data5"}},
       {{"key1", "data1"}, {"key2", "data2"}, {"key6", "data6"}}};

  LocalShuffleWriteInfo writeInfo = LocalShuffleWriteInfo::deserialize(
      localShuffleWriteInfo(rootPath, numPartitions));
  auto writer = std::make_shared<LocalShuffleWriter>(
      writeInfo.rootPath,
      writeInfo.queryId,
      writeInfo.shuffleId,
      writeInfo.numPartitions,
      /*maxBytesPerPartition=*/1024,
      /*sortedShuffle=*/true,
      pool());
  for (size_t i = 0; i < keys.size(); ++i) {
    writer->collect(i % numPartitions, keys[i], values[i]);
  }
  writer->noMoreData(true);

  auto fs = velox::filesystems::getFileSystem(rootPath, nullptr);
  std::map<int, std::vector<std::string>> partitionFiles;
  for (const auto& file : fs->list(rootPath)) {
    const auto pos = file.find("_shuffle_0_0_") + 13;
    partitionFiles[std::stoi(file.substr(pos, 1))].push_back(file);
  }

  for (int partition = 0; partition < numPartitions; ++partition) {
    std::vector<std::pair<std::string, std::string>> actual;
    for (const auto& filename : partitionFiles[partition]) {
      auto file = fs->openFileForRead(filename);
      auto buffer = AlignedBuffer::allocate<char>(file->size(), pool(), 0);
      file->pread(0, file->size(), buffer->asMutable<void>());
      auto parsedRows =
          testingExtractRowMetadata(buffer->as<char>(), file->size(), true);

      for (const auto& row : parsedRows) {
        const char* data = buffer->as<char>();
        const char* keyPtr = data + row.rowStart + (sizeof(uint32_t) * 2);
        const char* dataPtr = keyPtr + row.keySize;
        actual.emplace_back(
            std::string(keyPtr, row.keySize),
            std::string(dataPtr, row.dataSize));
      }
    }
    EXPECT_EQ(actual, expected[partition]);
  }

  cleanupDirectory(rootPath);
}

TEST_F(ShuffleTest, partitionAndSerializeWithDifferentColumnOrder) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data}, false)
                  .addNode(addPartitionAndSerializeNode(4, false, {"c1", "c0"}))
                  .planNode();

  auto expected = makeRowVector({data->childAt(1), data->childAt(0)});
  testPartitionAndSerialize(plan, expected);

  // Duplicate some columns.
  plan =
      exec::test::PlanBuilder()
          .values({data}, false)
          .addNode(addPartitionAndSerializeNode(4, false, {"c1", "c0", "c1"}))
          .planNode();

  expected =
      makeRowVector({data->childAt(1), data->childAt(0), data->childAt(1)});
  testPartitionAndSerialize(plan, expected);

  // Remove one column.
  plan = exec::test::PlanBuilder()
             .values({data}, false)
             .addNode(addPartitionAndSerializeNode(4, false, {"c1"}))
             .planNode();

  expected = makeRowVector({data->childAt(1)});
  testPartitionAndSerialize(plan, expected);
}

TEST_F(ShuffleTest, partitionAndSerializeOperatorWhenSinglePartition) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data}, false)
                  .addNode(addPartitionAndSerializeNode(1, false))
                  .planNode();

  testPartitionAndSerialize(plan, data);
}

TEST_F(ShuffleTest, shuffleWriterToString) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data}, true)
                  .addNode(addPartitionAndSerializeNode(4, false))
                  .localPartition(std::vector<std::string>{})
                  .addNode(addShuffleWriteNode(
                      4,
                      std::string(TestShuffleFactory::kShuffleName),
                      testShuffleInfo(10, 10)))
                  .planNode();

  ASSERT_EQ(plan->toString(false, false), "-- ShuffleWrite[3]\n");
  ASSERT_EQ(
      plan->toString(true, false),
      "-- ShuffleWrite[3][4, test-shuffle]"
      " -> partition:INTEGER, key:VARBINARY, data:VARBINARY\n");
}

TEST_F(ShuffleTest, partitionAndSerializeToString) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data}, true)
                  .addNode(addPartitionAndSerializeNode(4, false))
                  .planNode();

  ASSERT_EQ(plan->toString(false, false), "-- PartitionAndSerialize[1]\n");
  ASSERT_EQ(
      plan->toString(true, false),
      "-- PartitionAndSerialize[1][(c0) 4 HASH(c0) ROW<c0:INTEGER,c1:BIGINT>]"
      " -> partition:INTEGER, key:VARBINARY, data:VARBINARY\n");

  plan = exec::test::PlanBuilder()
             .values({data}, true)
             .addNode(addPartitionAndSerializeNode(4, true))
             .planNode();

  ASSERT_EQ(plan->toString(false, false), "-- PartitionAndSerialize[1]\n");
  ASSERT_EQ(
      plan->toString(true, false),
      "-- PartitionAndSerialize[1][(c0) 4 HASH(c0) ROW<c0:INTEGER,c1:BIGINT>]"
      " -> partition:INTEGER, key:VARBINARY, data:VARBINARY, replicate:BOOLEAN\n");
}

class DummyShuffleInterfaceFactory : public ShuffleInterfaceFactory {
 public:
  std::shared_ptr<ShuffleReader> createReader(
      const std::string& serializedShuffleInfo,
      const int32_t partition,
      velox::memory::MemoryPool* pool) override {
    return nullptr;
  }
  std::shared_ptr<ShuffleWriter> createWriter(
      const std::string& serializedShuffleInfo,
      velox::memory::MemoryPool* pool) override {
    return nullptr;
  }
};

TEST_F(ShuffleTest, shuffleInterfaceRegistration) {
  const std::string kShuffleName = "dummy-shuffle";
  EXPECT_TRUE(
      ShuffleInterfaceFactory::registerFactory(
          kShuffleName, std::make_unique<DummyShuffleInterfaceFactory>()));
  EXPECT_NO_THROW(ShuffleInterfaceFactory::factory(kShuffleName));
  EXPECT_FALSE(
      ShuffleInterfaceFactory::registerFactory(
          kShuffleName, std::make_unique<DummyShuffleInterfaceFactory>()));
}

TEST_F(ShuffleTest, shuffleReadRuntimeStats) {
  const size_t numPartitions = 1;
  const size_t numMapDrivers = 1;

  exec::Operator::registerOperator(std::make_unique<ShuffleWriteTranslator>());
  exec::Operator::registerOperator(std::make_unique<ShuffleReadTranslator>());
  velox::exec::ExchangeSource::factories().clear();
  registerExchangeSource(std::string(TestShuffleFactory::kShuffleName));

  const auto dataType = ROW({
      {"c0", INTEGER()},
      {"c1", BIGINT()},
  });

  VectorFuzzer::Options opts;
  opts.vectorSize = 100;
  opts.nullRatio = 0.1;
  opts.stringLength = 50;
  VectorFuzzer fuzzer(opts, pool_.get());

  struct {
    int numBatches;

    std::string debugString() const {
      return fmt::format("numBatches: {}", numBatches);
    }
  } testSettings[] = {{1}, {3}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    std::vector<RowVectorPtr> inputVectors;
    inputVectors.reserve(testData.numBatches);
    for (int i = 0; i < testData.numBatches; ++i) {
      inputVectors.push_back(fuzzer.fuzzInputRow(dataType));
    }

    auto shuffleInfo = testShuffleInfo(numPartitions, 1 << 20);
    TestShuffleWriter::createWriter(shuffleInfo, pool());

    auto writerPlan =
        exec::test::PlanBuilder()
            .values(inputVectors, true)
            .addNode(addPartitionAndSerializeNode(numPartitions, false))
            .localPartition(std::vector<std::string>{})
            .addNode(addShuffleWriteNode(
                numPartitions,
                std::string(TestShuffleFactory::kShuffleName),
                shuffleInfo))
            .planNode();

    auto writerTaskId = makeTaskId("leaf", 0);
    auto writerTask =
        makeTask(writerTaskId, writerPlan, 0, core::QueryConfig({}));
    writerTask->start(numMapDrivers);

    ASSERT_TRUE(exec::test::waitForTaskCompletion(writerTask.get(), 5'000'000));

    auto plan = exec::test::PlanBuilder()
                    .addNode(addShuffleReadNode(dataType))
                    .project(dataType->names())
                    .planNode();

    exec::CursorParameters params;
    params.planNode = plan;
    params.destination = 0;
    params.maxDrivers = 1;

    auto [taskCursor, results] = runShuffleReadTask(params, shuffleInfo);
    ASSERT_TRUE(
        exec::test::waitForTaskCompletion(taskCursor->task().get(), 5'000'000));

    const auto operatorStats =
        taskCursor->task()->taskStats().pipelineStats[0].operatorStats[0];
    const auto& runtimeStats = operatorStats.runtimeStats;

    ASSERT_EQ(runtimeStats.count(ShuffleRead::kShuffleDecodeTime), 1);
    const auto& decodeTimeStat =
        runtimeStats.at(ShuffleRead::kShuffleDecodeTime);
    ASSERT_GT(decodeTimeStat.count, 0);
    ASSERT_GT(decodeTimeStat.sum, 0);
    ASSERT_EQ(velox::RuntimeCounter::Unit::kNanos, decodeTimeStat.unit);

    ASSERT_EQ(runtimeStats.count(ShuffleRead::kShufflePagesPerInputBatch), 1);
    const auto& batchesPerReadStat =
        runtimeStats.at(ShuffleRead::kShufflePagesPerInputBatch);
    ASSERT_EQ(velox::RuntimeCounter::Unit::kNone, batchesPerReadStat.unit);
    ASSERT_GT(batchesPerReadStat.count, 0);
    ASSERT_GT(batchesPerReadStat.sum, 0);

    ASSERT_EQ(runtimeStats.count(ShuffleRead::kShuffleInputBatches), 1);
    const auto& numBatchesStat =
        runtimeStats.at(ShuffleRead::kShuffleInputBatches);
    ASSERT_GT(numBatchesStat.count, 0);
    ASSERT_GT(numBatchesStat.sum, 0);
    ASSERT_EQ(velox::RuntimeCounter::Unit::kNone, numBatchesStat.unit);
  }
}

TEST_F(ShuffleTest, partitionAndSerializeEndToEnd) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });
  runPartitionAndSerializeSerdeTest(data, 4);

  data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4}),
      makeFlatVector<int64_t>({10, 20, 30, 40}),
      makeFlatVector<std::string>({"a", "b", "c", "d"}),
  });

  runPartitionAndSerializeSerdeTest(data, 2, {{"c2", "c0"}});
}

TEST_F(ShuffleTest, persistentShuffleSortedEndToEnd) {
  const uint32_t numPartitions = 1;
  const uint32_t partition = 0;

  struct TestConfig {
    size_t maxBytesPerPartition;
    size_t numRows;
    uint64_t readMaxBytes;
    size_t minDataSize;
    size_t maxDataSize;
    std::string debugString() const {
      return fmt::format(
          "maxBytesPerPartition:{}, rows:{}, readMax:{}, dataSize:{}-{}",
          maxBytesPerPartition,
          numRows,
          readMaxBytes,
          minDataSize,
          maxDataSize);
    }
  } testSettings[] = {
      {.maxBytesPerPartition = 1024,
       .numRows = 1,
       .readMaxBytes = 1024,
       .minDataSize = 10,
       .maxDataSize = 50},
      {.maxBytesPerPartition = 1024,
       .numRows = 10,
       .readMaxBytes = 1024 * 1024,
       .minDataSize = 50,
       .maxDataSize = 200},
      {.maxBytesPerPartition = 500,
       .numRows = 20,
       .readMaxBytes = 1024 * 1024,
       .minDataSize = 50,
       .maxDataSize = 150},
      {.maxBytesPerPartition = 1024,
       .numRows = 50,
       .readMaxBytes = 8192,
       .minDataSize = 100,
       .maxDataSize = 400},
      {.maxBytesPerPartition = 2048,
       .numRows = 100,
       .readMaxBytes = 1024 * 1024,
       .minDataSize = 200,
       .maxDataSize = 1000},
  };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto tempRootDir = velox::exec::test::TempDirectoryPath::create();
    const auto testRootPath = tempRootDir->getPath();

    LocalShuffleWriteInfo writeInfo = LocalShuffleWriteInfo::deserialize(
        localShuffleWriteInfo(testRootPath, numPartitions));

    auto writer = std::make_shared<LocalShuffleWriter>(
        writeInfo.rootPath,
        writeInfo.queryId,
        writeInfo.shuffleId,
        writeInfo.numPartitions,
        testData.maxBytesPerPartition,
        /*sortedShuffle=*/true,
        pool());

    folly::Random::DefaultGenerator rng;
    rng.seed(1);
    std::vector<int32_t> randomKeys;
    randomKeys.reserve(testData.numRows);
    std::vector<std::string> dataValues;
    dataValues.reserve(testData.numRows);

    for (size_t i = 0; i < testData.numRows; ++i) {
      randomKeys.push_back(static_cast<int32_t>(folly::Random::rand32(rng)));

      const size_t sizeRange = testData.maxDataSize - testData.minDataSize;
      const size_t dataSize = testData.minDataSize +
          (sizeRange > 0 ? folly::Random::rand32(rng) % sizeRange : 0);

      // Create data with index marker at the end for verification
      std::string data(dataSize, static_cast<char>('a' + (i % 26)));
      data.append(fmt::format("_idx{:04d}", i));
      dataValues.push_back(std::move(data));
    }
    for (size_t i = 0; i < randomKeys.size(); ++i) {
      int32_t keyBigEndian = folly::Endian::big(randomKeys[i]);
      std::string_view keyBytes(
          reinterpret_cast<const char*>(&keyBigEndian), kUint32Size);
      writer->collect(partition, keyBytes, dataValues[i]);
    }
    writer->noMoreData(true);

    LocalShuffleReadInfo readInfo = LocalShuffleReadInfo::deserialize(
        localShuffleReadInfo(testRootPath, numPartitions, partition));

    auto reader = std::make_shared<LocalShuffleReader>(
        readInfo.rootPath,
        readInfo.queryId,
        readInfo.partitionIds,
        /*sortedShuffle=*/true,
        pool());
    reader->initialize();

    size_t count = 0;
    std::vector<std::string> readDataValues;

    while (true) {
      auto batches = reader->next(testData.readMaxBytes)
                         .via(folly::getGlobalCPUExecutor())
                         .get();
      if (batches.empty()) {
        break;
      }

      for (const auto& batch : batches) {
        for (const auto& row : batch->rows) {
          const char* rowData = row.data();
          const TRowSize dataSize =
              folly::Endian::big(*reinterpret_cast<const TRowSize*>(rowData));
          readDataValues.emplace_back(rowData + kUint32Size, dataSize);
          ++count;
        }
      }
    }

    EXPECT_EQ(randomKeys.size(), count);

    // Get the sorted order of original keys using getSortOrder
    std::vector<std::string> keys;
    keys.reserve(randomKeys.size());
    for (const auto& key : randomKeys) {
      int32_t keyBigEndian = folly::Endian::big(key);
      keys.emplace_back(
          reinterpret_cast<const char*>(&keyBigEndian), sizeof(int32_t));
    }
    auto sortedOrder = getSortOrder(keys);

    // Verify data appears in sorted key order
    for (size_t i = 0; i < readDataValues.size(); ++i) {
      // Extract original index from data value (format: [chars]_idx0000)
      const std::string& dataValue = readDataValues[i];
      size_t idxPos = dataValue.find("_idx");
      ASSERT_NE(idxPos, std::string::npos)
          << "Data value at position " << i << " missing '_idx' marker: '"
          << dataValue << "'";

      size_t originalIdx = std::stoul(dataValue.substr(idxPos + 4));

      // The data at position i should correspond to the key at sortedOrder[i]
      EXPECT_EQ(originalIdx, sortedOrder[i])
          << "Data at position " << i << " should correspond to key at index "
          << sortedOrder[i] << " but corresponds to index " << originalIdx;
    }
    reader->noMoreData(true);
    cleanupDirectory(testRootPath);
  }
}

} // namespace facebook::presto::operators::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv};
  return RUN_ALL_TESTS();
}
