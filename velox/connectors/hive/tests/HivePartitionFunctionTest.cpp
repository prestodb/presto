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

TEST_F(HivePartitionFunctionTest, bigint) {
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

TEST_F(HivePartitionFunctionTest, varchar) {
  auto values = vm_.flatVectorNullable<std::string>(
      {std::nullopt,
       "",
       "test string",
       "\u5f3a\u5927\u7684Presto\u5f15\u64ce"});

  assertPartitions(values, 1, {0, 0, 0, 0});
  assertPartitions(values, 2, {0, 0, 1, 0});
  assertPartitions(values, 500, {0, 0, 211, 454});
  assertPartitions(values, 997, {0, 0, 894, 831});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, boolean) {
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

TEST_F(HivePartitionFunctionTest, tinyint) {
  auto values = vm_.flatVectorNullable<int8_t>(
      {std::nullopt,
       64,
       std::numeric_limits<int8_t>::min(),
       std::numeric_limits<int8_t>::max()});

  assertPartitions(values, 1, {0, 0, 0, 0});
  assertPartitions(values, 2, {0, 0, 0, 1});
  assertPartitions(values, 500, {0, 64, 20, 127});
  assertPartitions(values, 997, {0, 64, 355, 127});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, smallint) {
  auto values = vm_.flatVectorNullable<int16_t>(
      {std::nullopt,
       30'000,
       std::numeric_limits<int16_t>::min(),
       std::numeric_limits<int16_t>::max()});

  assertPartitions(values, 1, {0, 0, 0, 0});
  assertPartitions(values, 2, {0, 0, 0, 1});
  assertPartitions(values, 500, {0, 0, 380, 267});
  assertPartitions(values, 997, {0, 90, 616, 863});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, integer) {
  auto values = vm_.flatVectorNullable<int32_t>(
      {std::nullopt,
       2'000'000'000,
       std::numeric_limits<int32_t>::min(),
       std::numeric_limits<int32_t>::max()});

  assertPartitions(values, 1, {0, 0, 0, 0});
  assertPartitions(values, 2, {0, 0, 0, 1});
  assertPartitions(values, 500, {0, 0, 0, 147});
  assertPartitions(values, 997, {0, 54, 0, 482});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, real) {
  auto values = vm_.flatVectorNullable<float>(
      {std::nullopt,
       2'000'000'000.0,
       std::numeric_limits<float>::lowest(),
       std::numeric_limits<float>::max()});

  assertPartitions(values, 1, {0, 0, 0, 0});
  assertPartitions(values, 2, {0, 0, 1, 1});
  assertPartitions(values, 500, {0, 348, 39, 39});
  assertPartitions(values, 997, {0, 544, 632, 632});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, double) {
  auto values = vm_.flatVectorNullable<double>(
      {std::nullopt,
       300'000'000'000.5,
       std::numeric_limits<double>::lowest(),
       std::numeric_limits<double>::max()});

  assertPartitions(values, 1, {0, 0, 0, 0});
  assertPartitions(values, 2, {0, 1, 0, 0});
  assertPartitions(values, 500, {0, 349, 76, 76});
  assertPartitions(values, 997, {0, 63, 729, 729});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, timestamp) {
  auto values = vm_.flatVectorNullable<Timestamp>(
      {std::nullopt,
       Timestamp(100'000, 900'000),
       Timestamp(
           std::numeric_limits<int64_t>::min(),
           std::numeric_limits<uint64_t>::min()),
       Timestamp(
           std::numeric_limits<int64_t>::max(),
           std::numeric_limits<uint64_t>::max())});

  assertPartitions(values, 1, {0, 0, 0, 0});
  assertPartitions(values, 2, {0, 0, 0, 0});
  assertPartitions(values, 500, {0, 284, 0, 0});
  assertPartitions(values, 997, {0, 514, 0, 0});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, date) {
  auto values = vm_.flatVectorNullable<Date>(
      {std::nullopt,
       Date(2'000'000'000),
       Date(std::numeric_limits<int32_t>::min()),
       Date(std::numeric_limits<int32_t>::max())});

  assertPartitions(values, 1, {0, 0, 0, 0});
  assertPartitions(values, 2, {0, 0, 0, 1});
  assertPartitions(values, 500, {0, 0, 0, 147});
  assertPartitions(values, 997, {0, 54, 0, 482});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}
