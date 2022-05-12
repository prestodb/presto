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
#include "velox/connectors/hive/HivePartitionFunction.h"
#include "gtest/gtest.h"
#include "velox/vector/tests/VectorMaker.h"

using namespace facebook::velox;

class HivePartitionFunctionTest : public ::testing::Test {
 protected:
  void assertPartitions(
      const VectorPtr& vector,
      int bucketCount,
      const std::vector<uint32_t>& expectedPartitions) {
    auto rowVector = vm_.rowVector({vector});

    auto size = rowVector->size();

    std::vector<int> bucketToPartition(bucketCount);
    std::iota(bucketToPartition.begin(), bucketToPartition.end(), 0);
    std::vector<ChannelIndex> keyChannels;
    keyChannels.emplace_back(0);
    connector::hive::HivePartitionFunction partitionFunction(
        bucketCount, bucketToPartition, keyChannels);

    std::vector<uint32_t> partitions(size);
    partitionFunction.partition(*rowVector, partitions);
    for (auto i = 0; i < size; ++i) {
      EXPECT_EQ(expectedPartitions[i], partitions[i])
          << "at " << i << ": " << vector->toString(i);
    }
  }

  // Run one function with 3 columns (two of which have the same values in each
  // row) and another with two constant channels replacing the last two columns.
  // Partitions should be the same.
  void assertPartitionsWithConstChannel(
      const VectorPtr& vector,
      int bucketCount) {
    auto column2 = vm_.flatVector<int64_t>(
        vector->size(), [](auto /*row*/) { return 13; });
    auto column3 = vm_.flatVector<int64_t>(
        vector->size(), [](auto /*row*/) { return 97; });
    auto rowVector = vm_.rowVector({vector, column2, column3});

    auto size = rowVector->size();
    std::vector<VectorPtr> constValues{
        vm_.flatVector<int64_t>({13}), vm_.flatVector<int64_t>({97})};

    std::vector<int> bucketToPartition(bucketCount);
    std::iota(bucketToPartition.begin(), bucketToPartition.end(), 0);

    std::vector<ChannelIndex> keyChannelsNorm{0, 1, 2};
    connector::hive::HivePartitionFunction partitionFunctionNorm(
        bucketCount, bucketToPartition, keyChannelsNorm);

    std::vector<ChannelIndex> keyChannelsConst{
        0, kConstantChannel, kConstantChannel};
    connector::hive::HivePartitionFunction partitionFunctionConst(
        bucketCount, bucketToPartition, keyChannelsConst, constValues);

    std::vector<uint32_t> partitionsNorm(size);
    partitionFunctionNorm.partition(*rowVector, partitionsNorm);
    std::vector<uint32_t> partitionsConst(size);
    partitionFunctionConst.partition(*rowVector, partitionsConst);
    EXPECT_EQ(partitionsNorm, partitionsConst);
  }

  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  test::VectorMaker vm_{pool_.get()};
};

TEST_F(HivePartitionFunctionTest, int64) {
  auto values = vm_.flatVectorNullable<int64_t>(
      {std::nullopt,
       300'000'000'000,
       std::numeric_limits<int64_t>::min(),
       std::numeric_limits<int64_t>::max()});

  assertPartitions(values, 1, {0, 0, 0, 0});
  assertPartitions(values, 2, {0, 1, 0, 0});
  assertPartitions(values, 500, {0, 497, 0, 0});
  assertPartitions(values, 997, {0, 852, 0, 0});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, string) {
  // TODO Fix flatVectorNullable to set stringBuffers.
  std::vector<std::optional<std::string>> values = {
      std::nullopt, "", "test string", "\u5f3a\u5927\u7684Presto\u5f15\u64ce"};
  auto vector = vm_.flatVectorNullable(values);

  assertPartitions(vector, 1, {0, 0, 0, 0});
  assertPartitions(vector, 2, {0, 0, 1, 0});
  assertPartitions(vector, 500, {0, 0, 211, 454});
  assertPartitions(vector, 997, {0, 0, 894, 831});

  assertPartitionsWithConstChannel(vector, 1);
  assertPartitionsWithConstChannel(vector, 2);
  assertPartitionsWithConstChannel(vector, 500);
  assertPartitionsWithConstChannel(vector, 997);
}

TEST_F(HivePartitionFunctionTest, bool) {
  auto values =
      vm_.flatVectorNullable<bool>({std::nullopt, true, false, false, true});

  assertPartitions(values, 1, {0, 0, 0, 0, 0});
  assertPartitions(values, 2, {0, 1, 0, 0, 1});
  assertPartitions(values, 500, {0, 1, 0, 0, 1});
  assertPartitions(values, 997, {0, 1, 0, 0, 1});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}
