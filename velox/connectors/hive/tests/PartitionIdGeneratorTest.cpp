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

#include "velox/connectors/hive/PartitionIdGenerator.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include "gtest/gtest.h"

namespace facebook::velox::connector::hive {

class PartitionIdGeneratorTest : public ::testing::Test,
                                 public test::VectorTestBase {};

TEST_F(PartitionIdGeneratorTest, consecutiveIds) {
  auto numPartitions = 100;

  PartitionIdGenerator idGenerator(ROW({VARCHAR()}), {0}, 100);

  auto input = makeRowVector(
      {makeFlatVector<StringView>(numPartitions * 3, [&](auto row) {
        return StringView(Date(18000 + row % numPartitions).toString());
      })});

  raw_vector<uint64_t> ids;
  idGenerator.run(input, ids);

  // distinctIds contains 100 ids in the range of [1, 100] that are consecutive.
  std::unordered_set<uint64_t> distinctIds(ids.begin(), ids.end());
  EXPECT_EQ(distinctIds.size(), numPartitions);
  EXPECT_EQ(*std::min_element(distinctIds.begin(), distinctIds.end()), 1);
  EXPECT_EQ(
      *std::max_element(distinctIds.begin(), distinctIds.end()), numPartitions);
}

TEST_F(PartitionIdGeneratorTest, stableIds) {
  PartitionIdGenerator idGenerator(ROW({BIGINT()}), {0}, 100);

  auto numPartitions = 40;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(numPartitions, [](auto row) { return row; }),
  });

  auto otherNumPartitions = 60;
  auto otherInput = makeRowVector({
      makeFlatVector<int64_t>(
          otherNumPartitions, [](auto row) { return row * 1000; }),
  });

  raw_vector<uint64_t> firstTimeIds;
  raw_vector<uint64_t> secondTimeIds;
  raw_vector<uint64_t> otherIds;
  idGenerator.run(input, firstTimeIds);
  idGenerator.run(otherInput, otherIds);
  idGenerator.run(input, secondTimeIds);

  EXPECT_TRUE(std::equal(
      firstTimeIds.begin(), firstTimeIds.end(), secondTimeIds.begin()));
}

TEST_F(PartitionIdGeneratorTest, maxPartitionId) {
  PartitionIdGenerator idGenerator(ROW({BIGINT()}), {0}, 100);

  auto firstMaxPartitionValue = 10;
  auto firstInput = makeRowVector({
      makeFlatVector<int64_t>(
          1000, [&](auto row) { return row % firstMaxPartitionValue; }),
  });

  auto secondMaxPartitionValue = firstMaxPartitionValue - 1;
  auto secondInput = makeRowVector({
      makeFlatVector<int64_t>(
          1000, [&](auto row) { return row % secondMaxPartitionValue; }),
  });

  raw_vector<uint64_t> firstIds;
  raw_vector<uint64_t> secondIds;
  idGenerator.run(firstInput, firstIds);
  idGenerator.run(secondInput, secondIds);

  EXPECT_EQ(idGenerator.recentMaxPartitionId(), secondMaxPartitionValue);
}

TEST_F(PartitionIdGeneratorTest, limitOfPartitionNumber) {
  auto maxPartitions = 100;

  PartitionIdGenerator idGenerator(ROW({INTEGER()}), {0}, maxPartitions);

  auto input = makeRowVector({
      makeFlatVector<int32_t>(maxPartitions + 1, [](auto row) { return row; }),
  });

  raw_vector<uint64_t> ids;

  VELOX_ASSERT_THROW(
      idGenerator.run(input, ids), "Exceeded limit of distinct partitions.");
}

} // namespace facebook::velox::connector::hive
