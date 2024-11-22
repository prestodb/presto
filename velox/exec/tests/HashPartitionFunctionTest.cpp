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

#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::velox::exec;

class HashPartitionFunctionTest : public velox::test::VectorTestBase,
                                  public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(HashPartitionFunctionTest, function) {
  const int numRows = 10'000;
  auto vector = makeRowVector(
      {makeFlatVector<int32_t>(numRows, [](auto row) { return row * 100 / 3; }),
       makeFlatVector<int32_t>(numRows, [](auto row) { return row * 128; })});
  auto rowType = asRowType(vector->type());

  // The test case the two hash partition functions having the same config.
  {
    std::vector<uint32_t> partitionsWithoutBits(numRows);
    HashPartitionFunction functionWithoutBits(false, 4, rowType, {0});
    functionWithoutBits.partition(*vector, partitionsWithoutBits);
    EXPECT_EQ(4, functionWithoutBits.numPartitions());

    std::vector<uint32_t> partitionsWithBits(numRows);
    HashPartitionFunction functionWithBits(HashBitRange{0, 2}, rowType, {0});
    functionWithBits.partition(*vector, partitionsWithBits);
    EXPECT_EQ(partitionsWithoutBits, partitionsWithBits);
    EXPECT_EQ(4, functionWithBits.numPartitions());
  }

  // The test case the local and remote hash functions have different
  // partitions.
  {
    std::vector<uint32_t> partitionsWithoutBits(numRows);
    HashPartitionFunction functionWithoutBits(true, 4, rowType, {0});
    functionWithoutBits.partition(*vector, partitionsWithoutBits);
    EXPECT_EQ(4, functionWithoutBits.numPartitions());

    std::vector<uint32_t> partitionsWithBits(numRows);
    HashPartitionFunction functionWithBits(HashBitRange{0, 2}, rowType, {0});
    functionWithBits.partition(*vector, partitionsWithBits);
    EXPECT_NE(partitionsWithoutBits, partitionsWithBits);
    EXPECT_EQ(4, functionWithBits.numPartitions());
  }

  // The test case the two hash partition functions with different configs.
  {
    std::vector<uint32_t> partitionsWithoutBits(numRows);
    HashPartitionFunction functionWithoutBits(false, 4, rowType, {0});
    functionWithoutBits.partition(*vector, partitionsWithoutBits);
    EXPECT_EQ(4, functionWithoutBits.numPartitions());

    std::vector<uint32_t> partitionsWithBits(numRows);
    HashPartitionFunction functionWithBits(HashBitRange{0, 3}, rowType, {0});
    functionWithBits.partition(*vector, partitionsWithBits);
    EXPECT_NE(partitionsWithoutBits, partitionsWithBits);
    EXPECT_EQ(8, functionWithBits.numPartitions());
  }

  // The test case the two hash partition functions with different configs.
  {
    std::vector<uint32_t> partitionsWithoutBits(numRows);
    HashPartitionFunction functionWithoutBits(true, 4, rowType, {0});
    functionWithoutBits.partition(*vector, partitionsWithoutBits);
    EXPECT_EQ(4, functionWithoutBits.numPartitions());

    std::vector<uint32_t> partitionsWithBits(numRows);
    HashPartitionFunction functionWithBits(HashBitRange{29, 31}, rowType, {0});
    functionWithBits.partition(*vector, partitionsWithBits);
    EXPECT_NE(partitionsWithoutBits, partitionsWithBits);
    EXPECT_EQ(4, functionWithBits.numPartitions());
  }

  // The test case the two hash partition functions with different configs.
  {
    std::vector<uint32_t> partitionsWithBits1(numRows);
    HashPartitionFunction functionWithBits1(HashBitRange{40, 42}, rowType, {0});
    functionWithBits1.partition(*vector, partitionsWithBits1);
    EXPECT_EQ(4, functionWithBits1.numPartitions());

    std::vector<uint32_t> partitionsWithBits2(numRows);
    HashPartitionFunction functionWithBits2(HashBitRange{29, 31}, rowType, {0});
    functionWithBits2.partition(*vector, partitionsWithBits2);
    EXPECT_NE(partitionsWithBits1, partitionsWithBits2);
    EXPECT_EQ(4, functionWithBits2.numPartitions());
  }

  // The test case the two hash partition functions with different configs.
  {
    std::vector<uint32_t> partitionsWithBits1(numRows);
    HashPartitionFunction functionWithBits1(HashBitRange{20, 31}, rowType, {0});
    functionWithBits1.partition(*vector, partitionsWithBits1);
    EXPECT_EQ(1 << 11, functionWithBits1.numPartitions());

    std::vector<uint32_t> partitionsWithBits2(numRows);
    HashPartitionFunction functionWithBits2(HashBitRange{29, 31}, rowType, {0});
    functionWithBits2.partition(*vector, partitionsWithBits2);
    EXPECT_NE(partitionsWithBits1, partitionsWithBits2);
    EXPECT_EQ(4, functionWithBits2.numPartitions());
  }

  // The test case the two hash partition functions having the same config.
  {
    std::vector<uint32_t> partitionsWithBits1(numRows);
    HashPartitionFunction functionWithBits1(HashBitRange{29, 31}, rowType, {0});
    functionWithBits1.partition(*vector, partitionsWithBits1);
    EXPECT_EQ(4, functionWithBits1.numPartitions());

    std::vector<uint32_t> partitionsWithBits2(numRows);
    HashPartitionFunction functionWithBits2(HashBitRange{29, 31}, rowType, {0});
    functionWithBits2.partition(*vector, partitionsWithBits2);
    EXPECT_EQ(partitionsWithBits1, partitionsWithBits2);
    EXPECT_EQ(4, functionWithBits2.numPartitions());
  }
}

TEST_F(HashPartitionFunctionTest, skew) {
  const int numRows = 10'000;
  auto input = makeRowVector(
      {makeFlatVector<int32_t>(numRows, [](auto row) { return row; })});
  auto rowType = asRowType(input->type());

  const auto hashSpec = std::make_unique<exec::HashPartitionFunctionSpec>(
      rowType, std::vector<column_index_t>{0});
  const int numRemotePartitions{8};
  auto remoteHashFunction =
      hashSpec->create(numRemotePartitions, /*localExchange=*/false);
  std::vector<uint32_t> remotePartitions(numRows);
  remoteHashFunction->partition(*input, remotePartitions);

  std::vector<BufferPtr> partitionIndicesVector(numRemotePartitions);
  std::vector<vector_size_t*> rawPartitionIndicesVector(numRemotePartitions);
  std::vector<vector_size_t> partitionSizeVectors(numRemotePartitions, 0);
  for (int i = 0; i < numRemotePartitions; ++i) {
    partitionIndicesVector[i] =
        AlignedBuffer::allocate<vector_size_t>(numRows, pool_.get());
    rawPartitionIndicesVector[i] =
        partitionIndicesVector[i]->asMutable<vector_size_t>();
  }
  for (int row = 0; row < numRows; ++row) {
    ASSERT_LT(remotePartitions[row], numRemotePartitions);
    const int partition = remotePartitions[row];
    rawPartitionIndicesVector[partition][partitionSizeVectors[partition]++] =
        row;
  }
  std::vector<VectorPtr> partitionedInputs;
  for (int partition = 0; partition < numRemotePartitions; ++partition) {
    partitionedInputs.push_back(wrap(
        partitionSizeVectors[partition],
        partitionIndicesVector[partition],
        input));
  }

  // Checks that the bad hash partition function (using remote hash function
  // with local hash partition count) map all the local input rows to one local
  // partition.
  const int numLocalPartitions{4};
  auto badLocalHashFunction =
      hashSpec->create(numLocalPartitions, /*localExchange=*/false);
  for (int partition = 0; partition < numRemotePartitions; ++partition) {
    const auto localPartitionSize = partitionSizeVectors[partition];
    std::vector<uint32_t> localPartitions(localPartitionSize);
    badLocalHashFunction->partition(
        *static_cast<RowVector*>(partitionedInputs[partition].get()),
        localPartitions);
    std::unordered_set<int> localPartitionSet;
    for (int row = 1; row < localPartitionSize; ++row) {
      localPartitionSet.insert(localPartitions[row]);
    }
    ASSERT_EQ(localPartitionSet.size(), 1) << partition;
  }

  // Verifies that the local hash partition function evenly distributes the
  // local input rows.
  auto localHashFunction =
      hashSpec->create(numLocalPartitions, /*localExchange=*/true);
  for (int partition = 0; partition < numRemotePartitions; ++partition) {
    const auto localPartitionSize = partitionSizeVectors[partition];
    std::vector<uint32_t> localPartitions(localPartitionSize);
    localHashFunction->partition(
        *static_cast<RowVector*>(partitionedInputs[partition].get()),
        localPartitions);
    std::unordered_set<int> localPartitionSet;
    for (int row = 1; row < localPartitionSize; ++row) {
      localPartitionSet.insert(localPartitions[row]);
    }
    ASSERT_EQ(localPartitionSet.size(), numLocalPartitions) << partition;
  }
}

TEST_F(HashPartitionFunctionTest, spec) {
  Type::registerSerDe();
  core::ITypedExpr::registerSerDe();

  RowTypePtr inputType(
      ROW({"c0", "c1", "c2", "c3", "c4"},
          {BIGINT(), SMALLINT(), INTEGER(), BIGINT(), VARCHAR()}));

  // The test case with 1 constValues.
  {
    auto hashSpec = std::make_unique<exec::HashPartitionFunctionSpec>(
        inputType,
        std::vector<column_index_t>{0, kConstantChannel, 2, 3, 4},
        std::vector<VectorPtr>{makeConstant(123, 1)});
    ASSERT_EQ(R"(HASH(c0, "123", c2, c3, c4))", hashSpec->toString());

    auto serialized = hashSpec->serialize();
    ASSERT_EQ(serialized["constants"].size(), 1);

    auto copy = HashPartitionFunctionSpec::deserialize(serialized, pool());
    ASSERT_EQ(hashSpec->toString(), copy->toString());
  }

  // The test case with 0 constValues.
  {
    auto hashSpec = std::make_unique<exec::HashPartitionFunctionSpec>(
        inputType,
        std::vector<column_index_t>{0, 1, 2, 3, 4},
        std::vector<VectorPtr>{});
    ASSERT_EQ("HASH(c0, c1, c2, c3, c4)", hashSpec->toString());

    auto serialized = hashSpec->serialize();
    ASSERT_EQ(serialized["constants"].size(), 0);

    auto copy = HashPartitionFunctionSpec::deserialize(serialized, pool());
    ASSERT_EQ(hashSpec->toString(), copy->toString());
  }
}

TEST_F(HashPartitionFunctionTest, noKeyAndBitRange) {
  for (bool localExchange : {false, true}) {
    SCOPED_TRACE(fmt::format("localExchange: {}", localExchange));
    auto vector = makeRowVector({makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6})});
    auto rowType = asRowType(vector->type());
    const auto numRows{vector->size()};
    HashPartitionFunction function(localExchange, 4, rowType, {}, {});

    std::vector<uint32_t> partitions(numRows);
    const auto singlePartition = function.partition(*vector, partitions);
    ASSERT_TRUE(singlePartition.has_value());
    EXPECT_EQ(singlePartition.value(), 0u);
  }
}
