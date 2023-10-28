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
#include "presto_cpp/main/operators/LocalPersistentShuffle.h"
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "presto_cpp/main/operators/ShuffleWrite.h"
#include "presto_cpp/main/operators/UnsafeRowExchangeSource.h"
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

class TestShuffleWriter : public ShuffleWriter {
 public:
  TestShuffleWriter(
      memory::MemoryPool* pool,
      uint32_t numPartitions,
      uint32_t maxBytesPerPartition)
      : pool_(pool),
        numPartitions_(numPartitions),
        maxBytesPerPartition_(maxBytesPerPartition),
        inProgressSizes_(numPartitions, 0),
        readyPartitions_(
            std::make_shared<std::vector<std::vector<BufferPtr>>>()) {
    inProgressPartitions_.resize(numPartitions_);
    readyPartitions_->resize(numPartitions_);
  }

  void initialize(velox::memory::MemoryPool* pool) {
    if (pool_ == nullptr) {
      pool_ = pool;
    }
  }

  void collect(int32_t partition, std::string_view data) override {
    using TRowSize = uint32_t;

    TestValue::adjust(
        "facebook::presto::operators::test::TestShuffleWriter::collect", this);

    auto& buffer = inProgressPartitions_[partition];
    TRowSize rowSize = data.size();
    auto size = sizeof(TRowSize) + rowSize;

    // Check if there is enough space in the buffer.
    if (buffer && inProgressSizes_[partition] + size >= maxBytesPerPartition_) {
      buffer->setSize(inProgressSizes_[partition]);
      (*readyPartitions_)[partition].emplace_back(std::move(buffer));
      inProgressPartitions_[partition].reset();
    }

    // Allocate buffer if needed.
    if (buffer == nullptr) {
      buffer = AlignedBuffer::allocate<char>(maxBytesPerPartition_, pool_);
      assert(buffer != nullptr);
      inProgressPartitions_[partition] = buffer;
      inProgressSizes_[partition] = 0;
    }

    // Copy data.
    auto offset = inProgressSizes_[partition];
    auto rawBuffer = buffer->asMutable<char>() + offset;

    *(TRowSize*)(rawBuffer) = folly::Endian::big(rowSize);
    ::memcpy(rawBuffer + sizeof(TRowSize), data.data(), rowSize);

    inProgressSizes_[partition] += size;
  }

  void noMoreData(bool success) override {
    VELOX_CHECK(success, "Unexpected error")
    // Flush in-progress buffers.
    for (auto i = 0; i < numPartitions_; ++i) {
      if (inProgressSizes_[i] > 0) {
        auto& buffer = inProgressPartitions_[i];
        buffer->setSize(inProgressSizes_[i]);
        (*readyPartitions_)[i].emplace_back(std::move(buffer));
        inProgressPartitions_[i].reset();
      }
    }
  }

  folly::F14FastMap<std::string, int64_t> stats() const override {
    return {
        {"test-shuffle.write", 1002},
        {exec::ExchangeClient::kBackgroundCpuTimeMs, kFakeBackgroundCpuTimeMs}};
  }

  std::shared_ptr<std::vector<std::vector<BufferPtr>>>& readyPartitions() {
    return readyPartitions_;
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
      velox::memory::MemoryPool* FOLLY_NONNULL pool) {
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

  /// Indexed by partition number. Each element represents currently being
  /// accumulated buffer by shuffler for a certain partition. Internal layout:
  /// | row-size | ..row-payload.. | row-size | ..row-payload.. | ..
  std::vector<BufferPtr> inProgressPartitions_;

  /// Tracks the total size of each in-progress partition in
  /// inProgressPartitions_
  std::vector<size_t> inProgressSizes_;
  std::shared_ptr<std::vector<std::vector<BufferPtr>>> readyPartitions_;
};

class TestShuffleReader : public ShuffleReader {
 public:
  TestShuffleReader(
      const int32_t partition,
      const std::shared_ptr<std::vector<std::vector<BufferPtr>>>&
          readyPartitions)
      : partition_(partition), readyPartitions_(readyPartitions) {}

  bool hasNext() override {
    TestValue::adjust(
        "facebook::presto::operators::test::TestShuffleReader::hasNext", this);
    return !(*readyPartitions_)[partition_].empty();
  }

  BufferPtr next() override {
    TestValue::adjust(
        "facebook::presto::operators::test::TestShuffleReader::next", this);
    VELOX_CHECK(!(*readyPartitions_)[partition_].empty());

    auto buffer = (*readyPartitions_)[partition_].back();
    (*readyPartitions_)[partition_].pop_back();
    return buffer;
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
  int32_t partition_;
  const std::shared_ptr<std::vector<std::vector<BufferPtr>>>& readyPartitions_;
};

class TestShuffleFactory : public ShuffleInterfaceFactory {
 public:
  static constexpr std::string_view kShuffleName = "test-shuffle";

  std::shared_ptr<ShuffleReader> createReader(
      const std::string& /* serializedShuffleInfo */,
      const int partition,
      velox::memory::MemoryPool* FOLLY_NONNULL pool) override {
    return std::make_shared<TestShuffleReader>(
        partition, TestShuffleWriter::getInstance()->readyPartitions());
  }

  std::shared_ptr<ShuffleWriter> createWriter(
      const std::string& serializedShuffleInfo,
      velox::memory::MemoryPool* FOLLY_NONNULL pool) override {
    return TestShuffleWriter::createWriter(serializedShuffleInfo, pool);
  }
};

void registerExchangeSource(const std::string& shuffleName) {
  exec::ExchangeSource::factories().clear();
  exec::ExchangeSource::registerFactory(
      [shuffleName](
          const std::string& taskId,
          int destination,
          std::shared_ptr<exec::ExchangeQueue> queue,
          memory::MemoryPool* FOLLY_NONNULL pool)
          -> std::unique_ptr<exec::ExchangeSource> {
        if (strncmp(taskId.c_str(), "batch://", 8) == 0) {
          auto uri = folly::Uri(taskId);
          for (auto& pair : uri.getQueryParams()) {
            if (pair.first == "shuffleInfo") {
              return std::make_unique<UnsafeRowExchangeSource>(
                  taskId,
                  destination,
                  std::move(queue),
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

class UnsafeRowShuffleTest : public exec::test::OperatorTestBase {
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
      int destination) {
    auto queryCtx = std::make_shared<core::QueryCtx>(
        executor_.get(), core::QueryConfig({}));
    core::PlanFragment planFragment{planNode};
    return exec::Task::create(
        taskId, std::move(planFragment), destination, std::move(queryCtx));
  }

  RowVectorPtr deserialize(
      const RowVectorPtr& serializedResult,
      const RowTypePtr& rowType) {
    auto serializedData =
        serializedResult->childAt(1)->as<FlatVector<StringView>>();
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
      velox::test::assertEqualVectors(results->childAt(2), expectedReplicate);
    }
  }

  void testPartitionAndSerialize(
      const core::PlanNodePtr& plan,
      const RowVectorPtr& expected,
      const exec::test::CursorParameters params,
      const std::optional<uint32_t> expectedOutputCount = std::nullopt) {
    auto [taskCursor, serializedResults] =
        readCursor(params, [](auto /*task*/) {});

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
    exec::test::CursorParameters params;
    params.planNode = plan;
    params.maxDrivers = 2;
    testPartitionAndSerialize(plan, expected, params);
  }

  std::pair<std::unique_ptr<exec::test::TaskCursor>, std::vector<RowVectorPtr>>
  runShuffleReadTask(
      const exec::test::CursorParameters& params,
      const std::string& shuffleInfo) {
    bool noMoreSplits = false;
    return readCursor(params, [&](auto* task) {
      if (noMoreSplits) {
        return;
      }

      auto remoteSplit = std::make_shared<exec::RemoteConnectorSplit>(
          makeTaskId("read", 0, shuffleInfo));

      task->addSplit("0", exec::Split{remoteSplit});
      task->noMoreSplits("0");
      noMoreSplits = true;
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
      uint64_t backgroundCpuTimeNanos = 0) {
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
                numPartitions, replicateNullsAndAny))
            .localPartition(std::vector<std::string>{})
            .addNode(addShuffleWriteNode(
                numPartitions, shuffleName, serializedShuffleWriteInfo))
            .planNode();

    auto writerTaskId = makeTaskId("leaf", 0);
    auto writerTask = makeTask(writerTaskId, writerPlan, 0);
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

      exec::test::CursorParameters params;
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
    opts.containerHasNulls = false;
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
    // Fuzzer issues with null-key maps:
    // https://github.com/facebookincubator/velox/issues/2848
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
    });

    // Create a local file system storage based shuffle.
    velox::filesystems::registerLocalFileSystem();
    auto rootDirectory = velox::exec::test::TempDirectoryPath::create();
    auto rootPath = rootDirectory->path;
    const std::string shuffleWriteInfo =
        localShuffleWriteInfo(rootPath, numPartitions);

    for (int it = 0; it < numIterations; it++) {
      auto seed = folly::Random::rand32();

      SCOPED_TRACE(fmt::format(
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
      size_t expectedOutputCount) {
    VectorFuzzer::Options opts;
    opts.vectorSize = 10;
    opts.nullRatio = 0;
    opts.dictionaryHasNulls = false;
    opts.containerHasNulls = false;
    opts.stringLength = 10000;
    opts.containerLength = 10000;
    opts.stringVariableLength = false;
    opts.containerVariableLength = false;
    auto seed = folly::Random::rand32();
    VectorFuzzer fuzzer(opts, pool_.get(), seed);
    // Create a deeply nested row, such that each row exceeds the output batch
    // limit.
    auto data = makeRowVector({fuzzer.fuzzMap(
        fuzzer.fuzzConstant(VARCHAR(), 100),
        fuzzer.fuzzArray(fuzzer.fuzzArray(fuzzer.fuzzFlat(DOUBLE()), 100), 100),
        inputRows)});

    auto plan = exec::test::PlanBuilder()
                    .values({data}, false)
                    .addNode(addPartitionAndSerializeNode(2, true))
                    .planNode();

    auto properties = std::unordered_map<std::string, std::string>{
        {core::QueryConfig::kPreferredOutputBatchBytes,
         std::to_string(outputSizeLimit)},
        {core::QueryConfig::kPreferredOutputBatchRows,
         std::to_string(outputRowLimit)}};

    auto queryCtx = std::make_shared<core::QueryCtx>(
        executor_.get(), core::QueryConfig(properties));
    auto params = exec::test::CursorParameters();
    params.planNode = plan;
    params.queryCtx = queryCtx;

    testPartitionAndSerialize(plan, data, params, expectedOutputCount);
  }

  void cleanupDirectory(const std::string& rootPath) {
    auto fileSystem = velox::filesystems::getFileSystem(rootPath, nullptr);
    auto files = fileSystem->list(rootPath);
    for (auto& file : files) {
      fileSystem->remove(file);
    }
  }
};

TEST_F(UnsafeRowShuffleTest, operators) {
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

  exec::test::CursorParameters params;
  params.planNode = plan;
  params.maxDrivers = 2;

  auto [taskCursor, serializedResults] =
      readCursor(params, [](auto /*task*/) {});
  ASSERT_EQ(serializedResults.size(), 0);
  TestShuffleWriter::reset();
}

TEST_F(UnsafeRowShuffleTest, shuffleWriterExceptions) {
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

  exec::test::CursorParameters params;
  params.planNode =
      exec::test::PlanBuilder()
          .values({data})
          .addNode(addPartitionAndSerializeNode(4, false))
          .addNode(addShuffleWriteNode(
              4, std::string(TestShuffleFactory::kShuffleName), info))
          .planNode();

  VELOX_ASSERT_THROW(
      readCursor(params, [](auto /*task*/) {}),
      "ShuffleWriter::collect failed");

  TestShuffleWriter::reset();
}

TEST_F(UnsafeRowShuffleTest, shuffleReaderExceptions) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4}),
      makeFlatVector<int64_t>({10, 20, 30, 40}),
  });

  auto info = testShuffleInfo(4, 1 << 20 /* 1MB */);
  TestShuffleWriter::createWriter(info, pool());

  exec::test::CursorParameters params;
  params.planNode =
      exec::test::PlanBuilder()
          .values({data})
          .addNode(addPartitionAndSerializeNode(2, false))
          .addNode(addShuffleWriteNode(
              2, std::string(TestShuffleFactory::kShuffleName), info))
          .planNode();

  ASSERT_NO_THROW(readCursor(params, [](auto /*task*/) {}));

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
        "facebook::presto::operators::test::TestShuffleReader::hasNext",
        injectFailure);

    VELOX_ASSERT_THROW(
        runShuffleReadTask(params, info), "ShuffleReader::hasNext failed");
  }

  {
    SCOPED_TESTVALUE_SET(
        "facebook::presto::operators::test::TestShuffleReader::next",
        injectFailure);

    VELOX_ASSERT_THROW(
        runShuffleReadTask(params, info), "ShuffleReader::next failed");
  }

  TestShuffleWriter::reset();
}

TEST_F(UnsafeRowShuffleTest, endToEnd) {
  size_t numPartitions = 5;
  size_t numMapDrivers = 2;

  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
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
      false,
      numPartitions,
      numMapDrivers,
      {data},
      kFakeBackgroundCpuTimeMs * Timestamp::kNanosecondsInMillisecond);
  TestShuffleWriter::reset();
}

TEST_F(UnsafeRowShuffleTest, endToEndWithReplicateNullAndAny) {
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
  TestShuffleWriter::reset();
}

TEST_F(UnsafeRowShuffleTest, replicateNullsAndAny) {
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

TEST_F(UnsafeRowShuffleTest, persistentShuffleDeser) {
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

TEST_F(UnsafeRowShuffleTest, persistentShuffle) {
  uint32_t numPartitions = 1;
  uint32_t numMapDrivers = 1;

  // Create a local file system storage based shuffle.
  velox::filesystems::registerLocalFileSystem();
  auto rootDirectory = velox::exec::test::TempDirectoryPath::create();
  auto rootPath = rootDirectory->path;

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

TEST_F(UnsafeRowShuffleTest, persistentShuffleFuzz) {
  fuzzerTest(false, 1);
  fuzzerTest(false, 3);
  fuzzerTest(false, 7);
}

TEST_F(UnsafeRowShuffleTest, persistentShuffleFuzzWithReplicateNullsAndAny) {
  fuzzerTest(true, 1);
  fuzzerTest(true, 3);
  fuzzerTest(true, 7);
}

TEST_F(UnsafeRowShuffleTest, partitionAndSerializeOutputByteLimit) {
  partitionAndSerializeWithThresholds(10'000, 1, 10, 10);
}

TEST_F(UnsafeRowShuffleTest, partitionAndSerializeOutputRowLimit) {
  partitionAndSerializeWithThresholds(5, 1'000'000'000, 10, 2);
}

TEST_F(UnsafeRowShuffleTest, partitionAndSerializeNoLimit) {
  partitionAndSerializeWithThresholds(1'000, 1'000'000'000, 5, 1);
}

TEST_F(UnsafeRowShuffleTest, partitionAndSerializeBothLimited) {
  partitionAndSerializeWithThresholds(1, 1'000'000, 5, 5);
}

TEST_F(UnsafeRowShuffleTest, partitionAndSerializeOperator) {
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

TEST_F(UnsafeRowShuffleTest, partitionAndSerializeWithLargeInput) {
  auto data = makeRowVector(
      {makeFlatVector<int32_t>(20'000, [](auto row) { return row; })});

  auto plan = exec::test::PlanBuilder()
                  .values({data}, false)
                  .addNode(addPartitionAndSerializeNode(1, true))
                  .planNode();

  testPartitionAndSerialize(plan, data);
}

TEST_F(UnsafeRowShuffleTest, partitionAndSerializeWithDifferentColumnOrder) {
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

TEST_F(UnsafeRowShuffleTest, partitionAndSerializeOperatorWhenSinglePartition) {
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

TEST_F(UnsafeRowShuffleTest, shuffleWriterToString) {
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

  ASSERT_EQ(plan->toString(false, false), "-- ShuffleWrite\n");
  ASSERT_EQ(
      plan->toString(true, false),
      "-- ShuffleWrite[4, test-shuffle] -> partition:INTEGER, data:VARBINARY\n");
}

TEST_F(UnsafeRowShuffleTest, partitionAndSerializeToString) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data}, true)
                  .addNode(addPartitionAndSerializeNode(4, false))
                  .planNode();

  ASSERT_EQ(plan->toString(false, false), "-- PartitionAndSerialize\n");
  ASSERT_EQ(
      plan->toString(true, false),
      "-- PartitionAndSerialize[(c0) 4 HASH(c0) ROW<c0:INTEGER,c1:BIGINT>] -> partition:INTEGER, data:VARBINARY\n");

  plan = exec::test::PlanBuilder()
             .values({data}, true)
             .addNode(addPartitionAndSerializeNode(4, true))
             .planNode();

  ASSERT_EQ(plan->toString(false, false), "-- PartitionAndSerialize\n");
  ASSERT_EQ(
      plan->toString(true, false),
      "-- PartitionAndSerialize[(c0) 4 HASH(c0) ROW<c0:INTEGER,c1:BIGINT>] -> partition:INTEGER, data:VARBINARY, replicate:BOOLEAN\n");
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

TEST_F(UnsafeRowShuffleTest, shuffleInterfaceRegistration) {
  const std::string kShuffleName = "dummy-shuffle";
  EXPECT_TRUE(ShuffleInterfaceFactory::registerFactory(
      kShuffleName, std::make_unique<DummyShuffleInterfaceFactory>()));
  EXPECT_NO_THROW(ShuffleInterfaceFactory::factory(kShuffleName));
  EXPECT_FALSE(ShuffleInterfaceFactory::registerFactory(
      kShuffleName, std::make_unique<DummyShuffleInterfaceFactory>()));
}
} // namespace facebook::presto::operators::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
