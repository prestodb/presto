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
}
