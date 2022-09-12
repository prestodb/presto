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
#include "folly/init/Init.h"
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/operators/ShuffleWrite.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/VectorFunction.h"
#include "velox/serializers/UnsafeRowSerializer.h"

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
    if (!buffer) {
      buffer = AlignedBuffer::allocate<char>(maxBytesPerPartition_, pool_);
      inProgressSizes_[partition] = 0;
    }

    // Copy data.
    auto rawBuffer = buffer->asMutable<char>();
    auto offset = inProgressSizes_[partition];

    *(size_t*)(rawBuffer + offset) = data.size();

    offset += sizeof(size_t);
    memcpy(rawBuffer + offset, data.data(), data.size());

    inProgressSizes_[partition] += sizeof(size_t) + data.size();
  }

  void noMoreData(bool success) {
    VELOX_CHECK(success, "Unexpected error")
    for (auto i = 0; i < numPartitions_; ++i) {
      if (inProgressSizes_[i] > 0) {
        auto& buffer = inProgressPartitions_[i];
        buffer->setSize(inProgressSizes_[i]);
        readyPartitions_[i].emplace_back(std::move(buffer));
        inProgressPartitions_[i].reset();
      }
    }
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

 private:
  memory::MemoryPool* pool_;
  const uint32_t numPartitions_;
  const uint32_t maxBytesPerPartition_;
  std::vector<BufferPtr> inProgressPartitions_;
  std::vector<size_t> inProgressSizes_;
  std::vector<std::vector<BufferPtr>> readyPartitions_;
};

auto addPartitionAndSerializeNode(int numPartitions) {
  return [numPartitions](
             core::PlanNodeId nodeId,
             core::PlanNodePtr source) -> core::PlanNodePtr {
    auto outputType = ROW({"p", "d"}, {INTEGER(), VARBINARY()});

    std::vector<core::TypedExprPtr> keys;
    keys.push_back(
        std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"));

    return std::make_shared<PartitionAndSerializeNode>(
        nodeId, keys, numPartitions, outputType, std::move(source));
  };
}

auto addShuffleWriteNode(ShuffleInterface* shuffle) {
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

  static std::string makeTaskId(const std::string& prefix, int num) {
    return fmt::format("spark://{}-{}", prefix, num);
  }

  std::shared_ptr<exec::Task> makeTask(
      const std::string& taskId,
      core::PlanNodePtr planNode,
      int destination) {
    auto queryCtx =
        core::QueryCtx::createForTest(std::make_shared<core::MemConfig>());
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

};

TEST_F(UnsafeRowShuffleTest, operators) {
  exec::Operator::registerOperator(
      std::make_unique<PartitionAndSerializeTranslator>());
  exec::Operator::registerOperator(std::make_unique<ShuffleWriteTranslator>());

  TestShuffle shuffle(pool(), 4, 1 << 20 /* 1MB */);

  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4}),
      makeFlatVector<int64_t>({10, 20, 30, 40}),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data}, true)
                  .addNode(addPartitionAndSerializeNode(4))
                  .localPartition({})
                  .addNode(addShuffleWriteNode(&shuffle))
                  .planNode();

  exec::test::CursorParameters params;
  params.planNode = plan;
  params.maxDrivers = 2;

  auto [taskCursor, serializedResults] =
      readCursor(params, [](auto /*task*/) {});
  ASSERT_EQ(serializedResults.size(), 0);
}

TEST_F(UnsafeRowShuffleTest, partitionAndSerializeOperator) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  auto plan =
      exec::test::PlanBuilder()
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

TEST_F(UnsafeRowShuffleTest, ShuffleWriterToString) {
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
      "-- ShuffleWrite[] -> p:INTEGER, d:VARBINARY\n"""
      "  -- LocalPartition[GATHER] -> p:INTEGER, d:VARBINARY\n"
      "    -- PartitionAndSerialize[(c0) 4] -> p:INTEGER, d:VARBINARY\n"
      "      -- Values[1000 rows in 1 vectors] -> c0:INTEGER, c1:BIGINT\n");
  ASSERT_EQ(
      plan->toString(false, false),
      "-- ShuffleWrite\n");
}

TEST_F(UnsafeRowShuffleTest, partitionAndSerializeToString) {
  auto data = vectorMaker_.rowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  auto plan =
      exec::test::PlanBuilder()
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
  ASSERT_EQ(
      plan->toString(false, false),
      "-- PartitionAndSerialize\n");
}
} // namespace facebook::presto::operators::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
