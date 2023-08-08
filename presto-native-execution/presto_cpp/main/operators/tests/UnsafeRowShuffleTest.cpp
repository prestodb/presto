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
#include "presto_cpp/external/json/json.hpp"
#include "presto_cpp/main/operators/LocalPersistentShuffle.h"
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "presto_cpp/main/operators/ShuffleWrite.h"
#include "presto_cpp/main/operators/UnsafeRowExchangeSource.h"
#include "presto_cpp/main/operators/tests/PlanBuilder.h"
#include "velox/connectors/hive/HivePartitionFunction.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/row/UnsafeRowDeserializers.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::presto;
using namespace facebook::presto::operators;

namespace facebook::presto::operators::test {

namespace {

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
    return {{"test-shuffle.write", 1002}};
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

class HivePartitionFunctionSpec : public core::PartitionFunctionSpec {
 public:
  HivePartitionFunctionSpec(const std::vector<column_index_t>& keys)
      : keys_{keys} {}

  std::unique_ptr<core::PartitionFunction> create(
      int numPartitions) const override {
    return std::make_unique<connector::hive::HivePartitionFunction>(
        numPartitions, std::vector<int>(numPartitions), keys_);
  }

  std::string toString() const override {
    return fmt::format("HIVE({})", folly::join(", ", keys_));
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "HivePartitionFunctionSpec";
    obj["keys"] = ISerializable::serialize(keys_);
    return obj;
  }

  static core::PartitionFunctionSpecPtr deserialize(
      const folly::dynamic& obj,
      void* /*context*/) {
    return std::make_shared<HivePartitionFunctionSpec>(
        ISerializable::deserialize<std::vector<column_index_t>>(obj["keys"]));
  }

 private:
  const std::vector<column_index_t> keys_;
};

class TestShuffleReader : public ShuffleReader {
 public:
  TestShuffleReader(
      const int32_t partition,
      const std::shared_ptr<std::vector<std::vector<BufferPtr>>>&
          readyPartitions)
      : partition_(partition), readyPartitions_(readyPartitions) {}

  bool hasNext() override {
    return !(*readyPartitions_)[partition_].empty();
  }

  BufferPtr next(bool success) override {
    VELOX_CHECK(success, "Unexpected error")
    VELOX_CHECK(!(*readyPartitions_)[partition_].empty());

    auto buffer = (*readyPartitions_)[partition_].back();
    (*readyPartitions_)[partition_].pop_back();
    return buffer;
  }

  folly::F14FastMap<std::string, int64_t> stats() const override {
    return {{"test-shuffle.read", 1032}};
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
  static constexpr std::string_view kTestShuffleInfoFormat =
      "{{\n"
      "  \"numPartitions\": {},\n"
      "  \"maxBytesPerPartition\": {}\n"
      "}}";

  static constexpr std::string_view kLocalShuffleWriteInfoFormat =
      "{{\n"
      "  \"rootPath\": \"{}\",\n"
      "  \"queryId\": \"query_id\",\n"
      "  \"shuffleId\": 0,\n"
      "  \"numPartitions\": {}\n"
      "}}";

  static constexpr std::string_view kLocalShuffleReadInfoFormat =
      "{{\n"
      "  \"rootPath\": \"{}\",\n"
      "  \"queryId\": \"query_id\",\n"
      "  \"partitionIds\": [ \"shuffle_0_0_0\" ],\n"
      "  \"numPartitions\": {}\n"
      "}}";

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
        executor_.get(), std::make_shared<velox::core::MemConfig>());
    core::PlanFragment planFragment{planNode};
    return exec::Task::create(
        taskId, std::move(planFragment), destination, std::move(queryCtx));
  }

  void addRemoteSplits(
      exec::Task* task,
      const std::vector<std::string>& remoteTaskIds) {
    for (auto& taskId : remoteTaskIds) {
      auto split =
          exec::Split(std::make_shared<exec::RemoteConnectorSplit>(taskId), -1);
      task->addSplit("0", std::move(split));
    }
    task->noMoreSplits("0");
  }

  RowVectorPtr deserialize(
      const RowVectorPtr& serializedResult,
      const RowTypePtr& rowType) {
    auto serializedData =
        serializedResult->childAt(1)->as<FlatVector<StringView>>();

    std::vector<std::optional<std::string_view>> rows;
    rows.reserve(serializedData->size());
    for (auto i = 0; i < serializedData->size(); ++i) {
      auto serializedRow = serializedData->valueAt(i);
      rows.push_back(
          std::string_view(serializedRow.data(), serializedRow.size()));
    }

    return std::dynamic_pointer_cast<RowVector>(
        row::UnsafeRowDeserializer::deserialize(rows, rowType, pool()));
  }

  RowVectorPtr copyResultVector(const RowVectorPtr& result) {
    auto vector = std::static_pointer_cast<RowVector>(
        BaseVector::create(result->type(), result->size(), pool()));
    vector->copy(result.get(), 0, 0, result->size());
    VELOX_CHECK_EQ(vector->size(), result->size());
    return vector;
  }

  void runShuffleTest(
      const std::string& shuffleName,
      const std::string& serializedShuffleWriteInfo,
      const std::string& serializedShuffleReadInfo,
      size_t numPartitions,
      size_t numMapDrivers,
      const std::vector<RowVectorPtr>& data) {
    // Register new shuffle related operators.
    exec::Operator::registerOperator(
        std::make_unique<PartitionAndSerializeTranslator>());
    exec::Operator::registerOperator(
        std::make_unique<ShuffleWriteTranslator>());
    exec::Operator::registerOperator(std::make_unique<ShuffleReadTranslator>());

    // Flatten the inputs to avoid issues assertEqualResults referred here:
    // https://github.com/facebookincubator/velox/issues/2859
    auto dataType = asRowType(data[0]->type());
    std::vector<RowVectorPtr> flattenInputs;
    // Create and run single leaf task to partition data and write it to
    // shuffle.
    for (auto& input : data) {
      flattenInputs.push_back(vectorMaker_.flatten<RowVector>(input));
    }

    auto writerPlan = exec::test::PlanBuilder()
                          .values(flattenInputs, true)
                          .addNode(addPartitionAndSerializeNode(numPartitions))
                          .localPartition({})
                          .addNode(addShuffleWriteNode(
                              shuffleName, serializedShuffleWriteInfo))
                          .planNode();

    auto writerTaskId = makeTaskId("leaf", 0);
    auto writerTask = makeTask(writerTaskId, writerPlan, 0);
    exec::Task::start(writerTask, numMapDrivers);

    ASSERT_TRUE(exec::test::waitForTaskCompletion(writerTask.get(), 3'000'000));

    // Verify that shuffle stats got propagated to the ShuffleWrite operator.
    auto shuffleStats = writerTask->taskStats()
                            .pipelineStats[0]
                            .operatorStats.back()
                            .runtimeStats;
    ASSERT_EQ(1, shuffleStats.count(fmt::format("{}.write", shuffleName)));

    // NOTE: each map driver processes the input once.
    std::vector<RowVectorPtr> expectedOutputVectors;
    for (auto& input : flattenInputs) {
      for (int i = 0; i < numMapDrivers; i++) {
        expectedOutputVectors.push_back(input);
      }
    }
    std::vector<RowVectorPtr> outputVectors;
    // Create and run multiple downstream tasks, one per partition, to read data
    // from shuffle.
    for (auto i = 0; i < numPartitions; ++i) {
      auto plan = exec::test::PlanBuilder()
                      .addNode(addShuffleReadNode(dataType))
                      .project(dataType->names())
                      .planNode();

      exec::test::CursorParameters params;
      params.planNode = plan;
      params.destination = i;

      bool noMoreSplits = false;
      auto [taskCursor, results] = readCursor(params, [&](auto* task) {
        if (noMoreSplits) {
          return;
        }
        addRemoteSplits(
            task, {makeTaskId("read", 0, serializedShuffleReadInfo)});
        noMoreSplits = true;
      });

      // Verify that shuffle stats got propagated to the Exchange operator.
      auto exchangeStats = taskCursor->task()
                               ->taskStats()
                               .pipelineStats[0]
                               .operatorStats[0]
                               .runtimeStats;
      ASSERT_EQ(1, exchangeStats.count(fmt::format("{}.read", shuffleName)));

      for (auto& resultVector : results) {
        auto vector = copyResultVector(resultVector);
        outputVectors.push_back(vector);
      }
    }
    velox::exec::test::assertEqualResults(expectedOutputVectors, outputVectors);
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
  auto info = fmt::format(kTestShuffleInfoFormat, 4, 1 << 20 /* 1MB */);
  auto plan = exec::test::PlanBuilder()
                  .values({data}, true)
                  .addNode(addPartitionAndSerializeNode(4))
                  .localPartition({})
                  .addNode(addShuffleWriteNode(
                      std::string(TestShuffleFactory::kShuffleName), info))
                  .planNode();

  exec::test::CursorParameters params;
  params.planNode = plan;
  params.maxDrivers = 2;

  auto [taskCursor, serializedResults] =
      readCursor(params, [](auto /*task*/) {});
  ASSERT_EQ(serializedResults.size(), 0);
  TestShuffleWriter::reset();
}

TEST_F(UnsafeRowShuffleTest, endToEnd) {
  size_t numPartitions = 5;
  size_t numMapDrivers = 2;

  auto data = vectorMaker_.rowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });

  // Make sure all previously registered exchange factory are gone.
  velox::exec::ExchangeSource::factories().clear();
  const std::string kShuffleInfo =
      fmt::format(kTestShuffleInfoFormat, numPartitions, 1 << 20);
  TestShuffleWriter::createWriter(kShuffleInfo, pool());
  registerExchangeSource(std::string(TestShuffleFactory::kShuffleName));
  runShuffleTest(
      std::string(TestShuffleFactory::kShuffleName),
      kShuffleInfo,
      kShuffleInfo,
      numPartitions,
      numMapDrivers,
      {data});
  TestShuffleWriter::reset();
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
      "  \"partitionIds\": [ \"shuffle1\" ],\n"
      "  \"numPartitions\": 11\n"
      "}";
  std::vector<std::string> partitionIds{"shuffle1"};
  LocalShuffleReadInfo shuffleReadInfo =
      LocalShuffleReadInfo::deserialize(serializedReadInfo);
  EXPECT_EQ(shuffleReadInfo.rootPath, "abc");
  EXPECT_EQ(shuffleReadInfo.queryId, "query_id");
  EXPECT_EQ(shuffleReadInfo.partitionIds, partitionIds);
  EXPECT_EQ(shuffleReadInfo.numPartitions, 11);

  std::string badSerializedInfo =
      "{\n"
      "  \"rootpath\": \"efg\",\n"
      "  \"queryId\": \"query_id\",\n"
      "  \"shuffleId\": 1,\n"
      "  \"numpartitions\": 12\n"
      "}";
  EXPECT_THROW(
      LocalShuffleWriteInfo::deserialize(badSerializedInfo),
      nlohmann::detail::out_of_range);

  badSerializedInfo =
      "{\n"
      "  \"rootPath\": \"abc\",\n"
      "  \"queryId\": \"query_id\",\n"
      "  \"shuffleId\": 1,\n"
      "  \"numPartitions\": \"hey-wrong-type\"\n"
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

  auto data = vectorMaker_.rowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });

  // Make sure all previously registered exchange factory are gone.
  velox::exec::ExchangeSource::factories().clear();
  const std::string kShuffleWriteInfo =
      fmt::format(kLocalShuffleWriteInfoFormat, rootPath, numPartitions);
  const std::string kShuffleReadInfo =
      fmt::format(kLocalShuffleReadInfoFormat, rootPath, numPartitions);
  const std::string kShuffleName =
      std::string(LocalPersistentShuffleFactory::kShuffleName);
  registerExchangeSource(
      std::string(LocalPersistentShuffleFactory::kShuffleName));
  runShuffleTest(
      std::string(LocalPersistentShuffleFactory::kShuffleName),
      kShuffleWriteInfo,
      kShuffleReadInfo,
      numPartitions,
      numMapDrivers,
      {data});
  cleanupDirectory(rootPath);
}

TEST_F(UnsafeRowShuffleTest, persistentShuffleFuzz) {
  // For unit testing, these numbers are set to relatively small values.
  // For stress testing, the following parameters and the fuzzer vector,
  // string and container sizes can be bumped up.
  size_t numPartitions = 1;
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
  auto rowType = ROW(
      {{"c0", INTEGER()},
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
       {"c15", ROW({INTEGER(), VARCHAR(), ARRAY(INTEGER())})}});

  // Create a local file system storage based shuffle.
  velox::filesystems::registerLocalFileSystem();
  auto rootDirectory = velox::exec::test::TempDirectoryPath::create();
  auto rootPath = rootDirectory->path;
  const std::string shuffleWriteInfo =
      fmt::format(kLocalShuffleWriteInfoFormat, rootPath, numPartitions);
  const std::string shuffleReadInfo =
      fmt::format(kLocalShuffleReadInfoFormat, rootPath, numPartitions);
  for (int it = 0; it < numIterations; it++) {
    auto seed = folly::Random::rand32();
    VectorFuzzer fuzzer(opts, pool_.get(), seed);
    std::vector<RowVectorPtr> inputVectors;
    // Create input vectors.
    for (size_t i = 0; i < numInputVectors; ++i) {
      auto input = fuzzer.fuzzRow(rowType);
      inputVectors.push_back(input);
    }
    velox::exec::ExchangeSource::factories().clear();
    registerExchangeSource(
        std::string(LocalPersistentShuffleFactory::kShuffleName));
    runShuffleTest(
        std::string(LocalPersistentShuffleFactory::kShuffleName),
        shuffleWriteInfo,
        shuffleReadInfo,
        numPartitions,
        numMapDrivers,
        inputVectors);
    cleanupDirectory(rootPath);
  }
}

TEST_F(UnsafeRowShuffleTest, partitionAndSerializeOperator) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data}, true)
                  .addNode(addPartitionAndSerializeNode(4))
                  .planNode();

  exec::test::CursorParameters params;
  params.planNode = plan;
  params.maxDrivers = 2;

  auto [taskCursor, serializedResults] =
      readCursor(params, [](auto /*task*/) {});
  EXPECT_EQ(serializedResults.size(), 2);

  for (auto& serializedResult : serializedResults) {
    // Verify that serialized data can be deserialized successfully into the
    // original data.
    auto deserialized = deserialize(serializedResult, asRowType(data->type()));
    velox::test::assertEqualVectors(data, deserialized);
  }
}

TEST_F(UnsafeRowShuffleTest, partitionAndSerializeOperatorWhenSinglePartition) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data}, true)
                  .addNode(addPartitionAndSerializeNode(1))
                  .planNode();

  exec::test::CursorParameters params;
  params.planNode = plan;
  params.maxDrivers = 2;

  auto [taskCursor, serializedResults] =
      readCursor(params, [](auto /*task*/) {});
  EXPECT_EQ(serializedResults.size(), 2);

  for (auto& serializedResult : serializedResults) {
    // Verify that serialized data can be deserialized successfully into the
    // original data.
    auto deserialized = deserialize(serializedResult, asRowType(data->type()));
    velox::test::assertEqualVectors(data, deserialized);
  }
}

TEST_F(UnsafeRowShuffleTest, shuffleWriterToString) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data}, true)
                  .addNode(addPartitionAndSerializeNode(4))
                  .localPartition({})
                  .addNode(addShuffleWriteNode(
                      std::string(TestShuffleFactory::kShuffleName),
                      fmt::format(kTestShuffleInfoFormat, 10, 10)))
                  .planNode();

  ASSERT_EQ(plan->toString(false, false), "-- ShuffleWrite\n");
  ASSERT_EQ(
      plan->toString(true, false),
      "-- ShuffleWrite[] -> p:INTEGER, d:VARBINARY\n");
}

TEST_F(UnsafeRowShuffleTest, partitionAndSerializeToString) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data}, true)
                  .addNode(addPartitionAndSerializeNode(4))
                  .planNode();

  ASSERT_EQ(plan->toString(false, false), "-- PartitionAndSerialize\n");
  // TODO Add a check for plan->toString(true, false)
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
