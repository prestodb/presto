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
#include "folly/init/Init.h"
#include "presto_cpp/external/json/json.hpp"
#include "presto_cpp/main/operators/LocalPersistentShuffle.h"
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/operators/ShuffleWrite.h"
#include "presto_cpp/main/operators/UnsafeRowExchangeSource.h"
#include "velox/connectors/hive/HivePartitionFunction.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/expression/VectorFunction.h"
#include "velox/serializers/UnsafeRowSerializer.h"
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

class TestShuffle : public ShuffleReader, public ShuffleWriter {
 public:
  static constexpr std::string_view kShuffleName = "test-shuffle";

  TestShuffle(
      memory::MemoryPool* pool,
      uint32_t numPartitions,
      uint32_t maxBytesPerPartition)
      : pool_{pool},
        numPartitions_{numPartitions},
        maxBytesPerPartition_{maxBytesPerPartition},
        inProgressSizes_(numPartitions, 0) {
    inProgressPartitions_.resize(numPartitions_);
    readyPartitions_.resize(numPartitions_);
  }

  void initialize(velox::memory::MemoryPool* pool) {
    if (pool_ == nullptr) {
      pool_ = pool;
    }
  }

  void collect(int32_t partition, std::string_view data) {
    auto& buffer = inProgressPartitions_[partition];

    // Check if there is enough space in the buffer.
    if (buffer &&
        inProgressSizes_[partition] + data.size() + sizeof(size_t) >=
            maxBytesPerPartition_) {
      buffer->setSize(inProgressSizes_[partition]);
      readyPartitions_[partition].emplace_back(std::move(buffer));
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
    auto rawBuffer = buffer->asMutable<char>();
    auto offset = inProgressSizes_[partition];

    *(size_t*)(rawBuffer + offset) = data.size();

    offset += sizeof(size_t);
    ::memcpy(rawBuffer + offset, data.data(), data.size());

    inProgressSizes_[partition] += sizeof(size_t) + data.size();
  }

  void noMoreData(bool success) {
    VELOX_CHECK(success, "Unexpected error")
    // Flush in-progress buffers.
    for (auto i = 0; i < numPartitions_; ++i) {
      if (inProgressSizes_[i] > 0) {
        auto& buffer = inProgressPartitions_[i];
        buffer->setSize(inProgressSizes_[i]);
        readyPartitions_[i].emplace_back(std::move(buffer));
        inProgressPartitions_[i].reset();
      }
    }
  }

  bool hasNext(int32_t partition) {
    return !readyPartitions_[partition].empty();
  }

  BufferPtr next(int32_t partition, bool success) {
    VELOX_CHECK(success, "Unexpected error")
    VELOX_CHECK(!readyPartitions_[partition].empty());

    auto buffer = readyPartitions_[partition].back();
    readyPartitions_[partition].pop_back();
    return buffer;
  }

  static void reset() {
    getInstance().reset();
  }

  /// Maintains a single shuffle interface for testing purpose.
  static std::shared_ptr<TestShuffle>& getInstance() {
    static std::shared_ptr<TestShuffle> instance_;
    return instance_;
  }

  static std::shared_ptr<TestShuffle> create(
      const std::string& serializedShuffleInfo,
      velox::memory::MemoryPool* FOLLY_NONNULL pool) {
    std::shared_ptr<TestShuffle>& instance = getInstance();
    if (instance) {
      return instance;
    }
    TestShuffleInfo writeInfo =
        TestShuffleInfo::deserializeShuffleInfo(serializedShuffleInfo);
    // We need only one instance for the shuffle since it's in-memory.
    instance = std::make_shared<TestShuffle>(
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
  std::vector<std::vector<BufferPtr>> readyPartitions_;
};

class TestShuffleFactory : public ShuffleInterfaceFactory {
 public:
  std::shared_ptr<ShuffleReader> createReader(
      const std::string& serializedShuffleInfo,
      velox::memory::MemoryPool* FOLLY_NONNULL pool) override {
    return TestShuffle::create(serializedShuffleInfo, pool);
  }

  std::shared_ptr<ShuffleWriter> createWriter(
      const std::string& serializedShuffleInfo,
      velox::memory::MemoryPool* FOLLY_NONNULL pool) override {
    return TestShuffle::create(serializedShuffleInfo, pool);
  }
};

void registerExchangeSource(const std::shared_ptr<ShuffleReader>& shuffle) {
  exec::ExchangeSource::registerFactory(
      [shuffle](
          const std::string& taskId,
          int destination,
          std::shared_ptr<exec::ExchangeQueue> queue,
          memory::MemoryPool* FOLLY_NONNULL pool)
          -> std::unique_ptr<exec::ExchangeSource> {
        if (strncmp(taskId.c_str(), "batch://", 8) == 0) {
          return std::make_unique<UnsafeRowExchangeSource>(
              taskId, destination, std::move(queue), shuffle, pool);
        }
        return nullptr;
      });
}

auto addPartitionAndSerializeNode(uint32_t numPartitions) {
  return [numPartitions](
             core::PlanNodeId nodeId,
             core::PlanNodePtr source) -> core::PlanNodePtr {
    std::vector<core::TypedExprPtr> keys;
    keys.push_back(
        std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"));
    auto outputType = source->outputType();
    return std::make_shared<PartitionAndSerializeNode>(
        nodeId,
        keys,
        numPartitions,
        ROW({"p", "d"}, {INTEGER(), VARBINARY()}),
        std::move(source),
        [outputType, keys](int numPartitions) {
          auto keyChannels = exec::toChannels(outputType, keys);
          return std::make_unique<connector::hive::HivePartitionFunction>(
              numPartitions, std::vector<int>(numPartitions), keyChannels);
        });
  };
}

auto addShuffleWriteNode(
    const std::string& shuffleName,
    const std::string& serializedWriteInfo) {
  return [&shuffleName, &serializedWriteInfo](
             core::PlanNodeId nodeId,
             core::PlanNodePtr source) -> core::PlanNodePtr {
    return std::make_shared<ShuffleWriteNode>(
        nodeId, shuffleName, serializedWriteInfo, std::move(source));
  };
}
} // namespace

class UnsafeRowShuffleTest : public exec::test::OperatorTestBase {
 public:
  static constexpr std::string_view kTestShuffleInfoFormat =
      "{{\n"
      "  \"numPartitions\": {},\n"
      "  \"maxBytesPerPartition\": {}\n"
      "}}";

  static constexpr std::string_view kLocalShuffleInfoFormat =
      "{{\n"
      "  \"rootPath\": \"{}\",\n"
      "  \"numPartitions\": {}\n"
      "}}";

 protected:
  void registerVectorSerde() override {
    serializer::spark::UnsafeRowVectorSerde::registerVectorSerde();
    ShuffleInterfaceFactory::registerFactory(
        std::string(TestShuffle::kShuffleName),
        std::make_unique<TestShuffleFactory>());
    ShuffleInterfaceFactory::registerFactory(
        std::string(LocalPersistentShuffle::kShuffleName),
        std::make_unique<LocalPersistentShuffleFactory>());
  }

  static std::string makeTaskId(const std::string& prefix, int num) {
    return fmt::format("batch://{}-{}", prefix, num);
  }

  std::shared_ptr<exec::Task> makeTask(
      const std::string& taskId,
      core::PlanNodePtr planNode,
      int destination) {
    auto queryCtx = std::make_shared<core::QueryCtx>(executor_.get());
    core::PlanFragment planFragment{planNode};
    return std::make_shared<exec::Task>(
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

    // Serialize data into a single block.

    // Calculate total size.
    size_t totalSize = 0;
    for (auto i = 0; i < serializedData->size(); ++i) {
      totalSize += serializedData->valueAt(i).size();
    }

    // Allocate the block. Add an extra sizeof(size_t) bytes for each row to
    // hold row size.
    BufferPtr buffer = AlignedBuffer::allocate<char>(
        totalSize + sizeof(size_t) * serializedData->size(), pool());
    auto rawBuffer = buffer->asMutable<char>();

    // Copy data.
    size_t offset = 0;
    for (auto i = 0; i < serializedData->size(); ++i) {
      auto value = serializedData->valueAt(i);

      *(size_t*)(rawBuffer + offset) = value.size();
      offset += sizeof(size_t);

      memcpy(rawBuffer + offset, value.data(), value.size());
      offset += value.size();
    }

    // Deserialize the block.
    return deserialize(buffer, rowType);
  }

  RowVectorPtr deserialize(BufferPtr& serialized, const RowTypePtr& rowType) {
    auto serializer =
        std::make_unique<serializer::spark::UnsafeRowVectorSerde>();

    ByteRange byteRange = {
        serialized->asMutable<uint8_t>(), (int32_t)serialized->size(), 0};

    auto input = std::make_unique<ByteStream>();
    input->resetInput({byteRange});

    RowVectorPtr result;
    serializer->deserialize(input.get(), pool(), rowType, &result, nullptr);
    return result;
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
      const std::string& serializedShuffleInfo,
      size_t numPartitions,
      size_t numMapDrivers,
      const std::vector<RowVectorPtr>& data) {
    // Register new shuffle related operators.
    exec::Operator::registerOperator(
        std::make_unique<PartitionAndSerializeTranslator>());
    exec::Operator::registerOperator(
        std::make_unique<ShuffleWriteTranslator>());

    // Flatten the inputs to avoid issues assertEqualResults referred here:
    // https://github.com/facebookincubator/velox/issues/2859
    auto dataType = asRowType(data[0]->type());
    std::vector<RowVectorPtr> flattenInputs;
    // Create and run single leaf task to partition data and write it to
    // shuffle.
    for (auto& input : data) {
      flattenInputs.push_back(vectorMaker_.flatten<RowVector>(input));
    }

    auto leafPlan =
        exec::test::PlanBuilder()
            .values(flattenInputs, true)
            .addNode(addPartitionAndSerializeNode(numPartitions))
            .localPartition({})
            .addNode(addShuffleWriteNode(shuffleName, serializedShuffleInfo))
            .planNode();

    auto leafTaskId = makeTaskId("leaf", 0);
    auto leafTask = makeTask(leafTaskId, leafPlan, 0);
    exec::Task::start(leafTask, numMapDrivers);

    ASSERT_TRUE(exec::test::waitForTaskCompletion(leafTask.get(), 3'000'000));

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
                      .exchange(dataType)
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
        addRemoteSplits(task, {leafTaskId});
        noMoreSplits = true;
      });

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
  exec::Operator::registerOperator(
      std::make_unique<PartitionAndSerializeTranslator>());
  exec::Operator::registerOperator(std::make_unique<ShuffleWriteTranslator>());

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
                      std::string(TestShuffle::kShuffleName), info))
                  .planNode();

  exec::test::CursorParameters params;
  params.planNode = plan;
  params.maxDrivers = 2;

  auto [taskCursor, serializedResults] =
      readCursor(params, [](auto /*task*/) {});
  ASSERT_EQ(serializedResults.size(), 0);
  TestShuffle::reset();
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
  const std::string shuffleInfo =
      fmt::format(kTestShuffleInfoFormat, numPartitions, 1 << 20);
  TestShuffle::create(shuffleInfo, pool());
  registerExchangeSource(TestShuffle::getInstance());
  runShuffleTest(
      std::string(TestShuffle::kShuffleName),
      shuffleInfo,
      numPartitions,
      numMapDrivers,
      {data});
  TestShuffle::reset();
}

TEST_F(UnsafeRowShuffleTest, persistentShuffleDeser) {
  std::string serializedInfo =
      "{\n"
      "  \"rootPath\": \"abc\",\n"
      "  \"numPartitions\": 11\n"
      "}";
  LocalShuffleInfo shuffleInfo = LocalShuffleInfo::deserialize(serializedInfo);
  EXPECT_EQ(shuffleInfo.rootPath, "abc");
  EXPECT_EQ(shuffleInfo.numPartitions, 11);

  serializedInfo =
      "{\n"
      "  \"rootPath\": \"efg\",\n"
      "  \"numPartitions\": 12\n"
      "}";
  shuffleInfo = LocalShuffleInfo::deserialize(serializedInfo);
  EXPECT_EQ(shuffleInfo.rootPath, "efg");
  EXPECT_EQ(shuffleInfo.numPartitions, 12);

  std::string badSerializedInfo =
      "{\n"
      "  \"rootpath\": \"efg\",\n"
      "  \"numpartitions\": 12\n"
      "}";
  EXPECT_THROW(
      LocalShuffleInfo::deserialize(badSerializedInfo),
      nlohmann::detail::out_of_range);

  badSerializedInfo =
      "{\n"
      "  \"rootPath\": \"abc\",\n"
      "  \"numPartitions\": \"hey-wrong-type\"\n"
      "}";
  EXPECT_THROW(
      LocalShuffleInfo::deserialize(badSerializedInfo),
      nlohmann::detail::type_error);
}

TEST_F(UnsafeRowShuffleTest, persistentShuffle) {
  uint32_t numPartitions = 5;
  uint32_t numMapDrivers = 2;

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
  const std::string shuffleInfo =
      fmt::format(kLocalShuffleInfoFormat, rootPath, numPartitions);
  auto localShuffle = std::make_shared<LocalPersistentShuffle>(
      rootPath, numPartitions, 1 << 20 /* 1MB */, pool());
  registerExchangeSource(localShuffle);
  runShuffleTest(
      std::string(LocalPersistentShuffle::kShuffleName),
      shuffleInfo,
      numPartitions,
      numMapDrivers,
      {data});
  cleanupDirectory(rootPath);
}

TEST_F(UnsafeRowShuffleTest, persistentShuffleFuzz) {
  // For unit testing, these numbers are set to relatively small values.
  // For stress testing, the following parameters and the fuzzer vector,
  // string and container sizes can be bumped up.
  size_t numPartitions = 5;
  size_t numMapDrivers = 2;
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
  opts.useMicrosecondPrecisionTimestamp = true;
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
  const std::string shuffleInfo =
      fmt::format(kLocalShuffleInfoFormat, rootPath, numPartitions);
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
    auto localShuffle = std::make_shared<LocalPersistentShuffle>(
        rootPath, numPartitions, 1 << 15, pool());
    registerExchangeSource(localShuffle);
    runShuffleTest(
        std::string(LocalPersistentShuffle::kShuffleName),
        shuffleInfo,
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
  ASSERT_EQ(serializedResults.size(), 2);

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
                      std::string(TestShuffle::kShuffleName),
                      fmt::format(kTestShuffleInfoFormat, 10, 10)))
                  .planNode();

  ASSERT_EQ(
      plan->toString(true, false),
      "-- ShuffleWrite[] -> p:INTEGER, d:VARBINARY\n");
  ASSERT_EQ(
      plan->toString(true, true),
      "-- ShuffleWrite[] -> p:INTEGER, d:VARBINARY\n"
      ""
      "  -- LocalPartition[GATHER] -> p:INTEGER, d:VARBINARY\n"
      "    -- PartitionAndSerialize[(c0) 4] -> p:INTEGER, d:VARBINARY\n"
      "      -- Values[1000 rows in 1 vectors] -> c0:INTEGER, c1:BIGINT\n");
  ASSERT_EQ(plan->toString(false, false), "-- ShuffleWrite\n");
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

  ASSERT_EQ(
      plan->toString(true, false),
      "-- PartitionAndSerialize[(c0) 4] -> p:INTEGER, d:VARBINARY\n");
  ASSERT_EQ(
      plan->toString(true, true),
      "-- PartitionAndSerialize[(c0) 4] -> p:INTEGER, d:VARBINARY\n"
      "  -- Values[1000 rows in 1 vectors] -> c0:INTEGER, c1:BIGINT\n");
  ASSERT_EQ(plan->toString(false, false), "-- PartitionAndSerialize\n");
}

class DummyShuffleInterfaceFactory : public ShuffleInterfaceFactory {
 public:
  std::shared_ptr<ShuffleReader> createReader(
      const std::string& serializedShuffleInfo,
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
