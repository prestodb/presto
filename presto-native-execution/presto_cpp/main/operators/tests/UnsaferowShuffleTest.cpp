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

class TestShuffle : public ShuffleInterface {
 public:
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
    readyForRead_ = true;
  }

  bool hasNext(int32_t partition) const {
    return !readyPartitions_[partition].empty();
  }

  BufferPtr next(int32_t partition, bool success) {
    VELOX_CHECK(success, "Unexpected error")
    VELOX_CHECK(!readyPartitions_[partition].empty());

    auto buffer = readyPartitions_[partition].back();
    readyPartitions_[partition].pop_back();
    return buffer;
  }

  bool readyForRead() const {
    return readyForRead_;
  }

 private:
  memory::MemoryPool* pool_{nullptr};
  bool readyForRead_ = false;
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

void registerExchangeSource(const std::shared_ptr<ShuffleInterface>& shuffle) {
  exec::ExchangeSource::registerFactory(
      [shuffle](
          const std::string& taskId,
          int destination,
          std::shared_ptr<exec::ExchangeQueue> queue,
          memory::MemoryPool* FOLLY_NONNULL pool)
          -> std::unique_ptr<exec::ExchangeSource> {
        if (strncmp(taskId.c_str(), "spark://", 8) == 0) {
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

auto addShuffleWriteNode(const std::shared_ptr<ShuffleInterface>& shuffle) {
  return [shuffle](
             core::PlanNodeId nodeId,
             core::PlanNodePtr source) -> core::PlanNodePtr {
    return std::make_shared<ShuffleWriteNode>(
        nodeId, shuffle, std::move(source));
  };
}
} // namespace

class UnsafeRowShuffleTest : public exec::test::OperatorTestBase {
 protected:
  void registerVectorSerde() override {
    serializer::spark::UnsafeRowVectorSerde::registerVectorSerde();
  }

  static std::string makeTaskId(const std::string& prefix, int num) {
    return fmt::format("spark://{}-{}", prefix, num);
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
      const std::shared_ptr<ShuffleInterface>& shuffle,
      size_t numPartitions,
      size_t numMapDrivers,
      const std::vector<RowVectorPtr>& data) {
    // Register new shuffle related operators.
    exec::Operator::registerOperator(
        std::make_unique<PartitionAndSerializeTranslator>());
    exec::Operator::registerOperator(
        std::make_unique<ShuffleWriteTranslator>());

    // Make sure all previously registered exchange factory are gone.
    velox::exec::ExchangeSource::factories().clear();
    registerExchangeSource(shuffle);

    // Flatten the inputs to avoid issues assertEqualResults referred here:
    // https://github.com/facebookincubator/velox/issues/2859
    auto dataType = asRowType(data[0]->type());
    std::vector<RowVectorPtr> flattenInputs;
    // Create and run single leaf task to partition data and write it to
    // shuffle.
    for (auto& input : data) {
      flattenInputs.push_back(vectorMaker_.flatten<RowVector>(input));
    }

    auto leafPlan = exec::test::PlanBuilder()
                        .values(flattenInputs, true)
                        .addNode(addPartitionAndSerializeNode(numPartitions))
                        .localPartition({})
                        .addNode(addShuffleWriteNode(shuffle))
                        .planNode();

    auto leafTaskId = makeTaskId("leaf", 0);
    auto leafTask = makeTask(leafTaskId, leafPlan, 0);
    exec::Task::start(leafTask, numMapDrivers);

    ASSERT_TRUE(exec::test::waitForTaskCompletion(leafTask.get(), 3'000'000));
    ASSERT_TRUE(shuffle->readyForRead());

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
      ASSERT_FALSE(shuffle->hasNext(i)) << i;
      for (auto& resultVector : results) {
        auto vector = copyResultVector(resultVector);
        outputVectors.push_back(vector);
      }
    }
    velox::exec::test::assertEqualResults(expectedOutputVectors, outputVectors);
  }
};

TEST_F(UnsafeRowShuffleTest, operators) {
  exec::Operator::registerOperator(
      std::make_unique<PartitionAndSerializeTranslator>());
  exec::Operator::registerOperator(std::make_unique<ShuffleWriteTranslator>());

  auto shuffle = std::make_shared<TestShuffle>(pool(), 4, 1 << 20 /* 1MB */);

  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4}),
      makeFlatVector<int64_t>({10, 20, 30, 40}),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data}, true)
                  .addNode(addPartitionAndSerializeNode(4))
                  .localPartition({})
                  .addNode(addShuffleWriteNode(shuffle))
                  .planNode();

  exec::test::CursorParameters params;
  params.planNode = plan;
  params.maxDrivers = 2;

  auto [taskCursor, serializedResults] =
      readCursor(params, [](auto /*task*/) {});
  ASSERT_EQ(serializedResults.size(), 0);
}

TEST_F(UnsafeRowShuffleTest, endToEnd) {
  size_t numPartitions = 5;
  size_t numMapDrivers = 2;

  auto shuffle =
      std::make_shared<TestShuffle>(pool(), numPartitions, 1 << 20 /* 1MB */);
  auto data = vectorMaker_.rowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });
  runShuffleTest(shuffle, numPartitions, numMapDrivers, {data});
}

TEST_F(UnsafeRowShuffleTest, persistentShuffleDeser) {
  std::string serializedInfo =
      "{\n"
      "  \"rootPath\": \"abc\",\n"
      "  \"numPartitions\": 11\n"
      "}";
  LocalShuffleInfo shuffleInfo =
      LocalShuffleInfo::deserialize(serializedInfo);
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
  size_t numPartitions = 5;
  size_t numMapDrivers = 2;

  // Create a local file system storage based shuffle.
  velox::filesystems::registerLocalFileSystem();
  auto rootDirectory = velox::exec::test::TempDirectoryPath::create();
  auto rootPath = rootDirectory->path;

  // Initialize persistent shuffle.
  auto shuffle = std::make_shared<LocalPersistentShuffle>(
      rootPath, pool(), numPartitions, 1 << 20 /* 1MB */);

  auto data = vectorMaker_.rowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });
  runShuffleTest(shuffle, numPartitions, numMapDrivers, {data});
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
  auto shuffle = std::make_shared<LocalPersistentShuffle>(
      rootPath, pool(), numPartitions, 1 << 15);
  for (int it = 0; it < numIterations; it++) {
    shuffle->reset(pool(), numPartitions, rootPath);

    auto seed = folly::Random::rand32();
    VectorFuzzer fuzzer(opts, pool_.get(), seed);
    std::vector<RowVectorPtr> inputVectors;
    // Create input vectors.
    for (size_t i = 0; i < numInputVectors; ++i) {
      auto input = fuzzer.fuzzRow(rowType);
      inputVectors.push_back(input);
    }
    runShuffleTest(shuffle, numPartitions, numMapDrivers, inputVectors);
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
                  .addNode(addShuffleWriteNode(nullptr))
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

TEST_F(UnsafeRowShuffleTest, shuffleInterfaceRegistration) {
  const std::string kShuffleName = "dummy-shuffle";
  auto dummyShuffleInterface =
      [](const std::string& /* serializedShuffleInfo */,
         ShuffleInterface::Type /* type */,
         velox::memory::MemoryPool* /* pool */) { return nullptr; };
  EXPECT_TRUE(
      ShuffleInterface::registerFactory(kShuffleName, dummyShuffleInterface));
  EXPECT_NO_THROW(ShuffleInterface::factory(kShuffleName));
  EXPECT_FALSE(
      ShuffleInterface::registerFactory(kShuffleName, dummyShuffleInterface));
}
} // namespace facebook::presto::operators::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
