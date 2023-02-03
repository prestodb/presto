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

TEST_F(PartitionIdGeneratorTest, consecutiveIdsSingleKey) {
  auto numPartitions = 100;

  PartitionIdGenerator idGenerator(ROW({VARCHAR()}), {0}, 100, pool());

  auto input = makeRowVector(
      {makeFlatVector<StringView>(numPartitions * 3, [&](auto row) {
        return StringView(Date(18000 + row % numPartitions).toString());
      })});

  raw_vector<uint64_t> ids;
  idGenerator.run(input, ids);

  // distinctIds contains 100 ids in the range of [1, 100] that are consecutive.
  std::unordered_set<uint64_t> distinctIds(ids.begin(), ids.end());
  EXPECT_EQ(distinctIds.size(), numPartitions);
  EXPECT_EQ(*std::min_element(distinctIds.begin(), distinctIds.end()), 0);
  EXPECT_EQ(
      *std::max_element(distinctIds.begin(), distinctIds.end()),
      numPartitions - 1);
}

TEST_F(PartitionIdGeneratorTest, consecutiveIdsMultipleKeys) {
  PartitionIdGenerator idGenerator(
      ROW({VARCHAR(), INTEGER()}), {0, 1}, 100, pool());

  auto input = makeRowVector({
      makeFlatVector<StringView>(
          1'000,
          [&](auto row) {
            return StringView(Date(18000 + row % 5).toString());
          }),
      makeFlatVector<int32_t>(1'000, [&](auto row) { return row % 17; }),
  });

  raw_vector<uint64_t> ids;
  idGenerator.run(input, ids);

  // distinctIds contains 85 ids in the range of [0, 84].
  auto numPartitions = 5 * 17;

  std::unordered_set<uint64_t> distinctIds(ids.begin(), ids.end());
  EXPECT_EQ(distinctIds.size(), numPartitions);
  EXPECT_EQ(*std::min_element(distinctIds.begin(), distinctIds.end()), 0);
  EXPECT_EQ(
      *std::max_element(distinctIds.begin(), distinctIds.end()),
      numPartitions - 1);
}

TEST_F(PartitionIdGeneratorTest, stableIdsSingleKey) {
  PartitionIdGenerator idGenerator(ROW({BIGINT()}), {0}, 100, pool());

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

  for (auto i = 0; i < input->size(); ++i) {
    EXPECT_EQ(firstTimeIds[i], secondTimeIds[i]) << "at " << i;
  }
}

TEST_F(PartitionIdGeneratorTest, stableIdsMultipleKeys) {
  PartitionIdGenerator idGenerator(
      ROW({BIGINT(), VARCHAR(), INTEGER()}), {1, 2}, 100, pool());

  const vector_size_t size = 1'000;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      makeFlatVector<StringView>(
          size,
          [](auto row) {
            return StringView(Date(18000 + row % 3).toString());
          }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
  });

  raw_vector<uint64_t> firstTimeIds;
  idGenerator.run(input, firstTimeIds);

  auto moreInput = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      makeFlatVector<StringView>(
          size,
          [](auto row) {
            return StringView(Date(18000 + row % 5).toString());
          }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 17; }),
  });

  raw_vector<uint64_t> moreIds;
  idGenerator.run(moreInput, moreIds);

  raw_vector<uint64_t> secondTimeIds;
  idGenerator.run(input, secondTimeIds);

  for (auto i = 0; i < input->size(); ++i) {
    EXPECT_EQ(firstTimeIds[i], secondTimeIds[i]) << "at " << i;
  }
}

TEST_F(PartitionIdGeneratorTest, numPartitions) {
  PartitionIdGenerator idGenerator(ROW({BIGINT()}), {0}, 100, pool());

  // First run to process partition 0,..,9. Total num of partitions processed by
  // far is 10.
  auto firstInput = makeRowVector({
      makeFlatVector<int64_t>(1000, [&](auto row) { return row % 10; }),
  });

  raw_vector<uint64_t> ids;
  idGenerator.run(firstInput, ids);
  EXPECT_EQ(idGenerator.numPartitions(), 10);

  // Second run to process partition 10,...,19. Total number of partitions
  // processed by far is 20.
  auto secondInput = makeRowVector({
      makeFlatVector<int64_t>(1000, [&](auto row) { return row % 10 + 10; }),
  });

  idGenerator.run(secondInput, ids);
  EXPECT_EQ(idGenerator.numPartitions(), 20);

  // Third run to process partition 0,...,9. Total number of partitions
  // processed by far is 20.
  auto thirdInput = makeRowVector({
      makeFlatVector<int64_t>(1000, [&](auto row) { return row % 10; }),
  });

  idGenerator.run(secondInput, ids);
  EXPECT_EQ(idGenerator.numPartitions(), 20);
}

TEST_F(PartitionIdGeneratorTest, limitOfPartitionNumber) {
  auto maxPartitions = 100;

  PartitionIdGenerator idGenerator(
      ROW({INTEGER()}), {0}, maxPartitions, pool());

  auto input = makeRowVector({
      makeFlatVector<int32_t>(maxPartitions + 1, [](auto row) { return row; }),
  });

  raw_vector<uint64_t> ids;

  VELOX_ASSERT_THROW(
      idGenerator.run(input, ids),
      fmt::format("Exceeded limit of {} distinct partitions.", maxPartitions));
}

} // namespace facebook::velox::connector::hive
