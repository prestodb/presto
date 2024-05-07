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
                                 public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(PartitionIdGeneratorTest, consecutiveIdsSingleKey) {
  auto numPartitions = 100;

  PartitionIdGenerator idGenerator(ROW({VARCHAR()}), {0}, 100, pool(), true);

  auto input = makeRowVector(
      {makeFlatVector<StringView>(numPartitions * 3, [&](auto row) {
        return StringView::makeInline(
            DATE()->toString(18000 + row % numPartitions));
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
      ROW({VARCHAR(), INTEGER()}), {0, 1}, 100, pool(), true);

  auto input = makeRowVector({
      makeFlatVector<StringView>(
          1'000,
          [&](auto row) {
            return StringView::makeInline(DATE()->toString(18000 + row % 5));
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

TEST_F(PartitionIdGeneratorTest, multipleBoolKeys) {
  PartitionIdGenerator idGenerator(
      ROW({BOOLEAN(), BOOLEAN()}), {0, 1}, 100, pool(), true);

  auto input = makeRowVector({
      makeFlatVector<bool>(
          1'000, [](vector_size_t row) { return row < 50; }, nullEvery(7)),
      makeFlatVector<bool>(
          1'000,
          [](vector_size_t row) { return (row % 2) == 0; },
          nullEvery(3)),
  });

  raw_vector<uint64_t> ids;
  idGenerator.run(input, ids);

  // distinctIds contains 9 ids.
  const auto numPartitions = 9;

  std::unordered_set<uint64_t> distinctIds(ids.begin(), ids.end());
  EXPECT_EQ(distinctIds.size(), numPartitions);
  EXPECT_EQ(*std::min_element(distinctIds.begin(), distinctIds.end()), 0);
  EXPECT_EQ(
      *std::max_element(distinctIds.begin(), distinctIds.end()),
      numPartitions - 1);
}

TEST_F(PartitionIdGeneratorTest, stableIdsSingleKey) {
  PartitionIdGenerator idGenerator(ROW({BIGINT()}), {0}, 100, pool(), true);

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
      ROW({BIGINT(), VARCHAR(), INTEGER()}), {1, 2}, 100, pool(), true);

  const vector_size_t size = 1'000;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      makeFlatVector<StringView>(
          size,
          [](auto row) {
            return StringView::makeInline(DATE()->toString(18000 + row % 3));
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
            return StringView::makeInline(DATE()->toString(18000 + row % 5));
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

TEST_F(PartitionIdGeneratorTest, partitionKeysCaseSensitive) {
  PartitionIdGenerator idGenerator(
      ROW({"cc0", "Cc1"}, {BIGINT(), VARCHAR()}), {1}, 100, pool(), false);

  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<std::string>({"apple", "orange", "apple"}),
  });

  raw_vector<uint64_t> firstTimeIds;
  idGenerator.run(input, firstTimeIds);
  EXPECT_EQ("Cc1=apple", idGenerator.partitionName(0));
  EXPECT_EQ("Cc1=orange", idGenerator.partitionName(1));
}

TEST_F(PartitionIdGeneratorTest, numPartitions) {
  PartitionIdGenerator idGenerator(ROW({BIGINT()}), {0}, 100, pool(), true);

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
      ROW({INTEGER()}), {0}, maxPartitions, pool(), true);

  auto input = makeRowVector({
      makeFlatVector<int32_t>(maxPartitions + 1, [](auto row) { return row; }),
  });

  raw_vector<uint64_t> ids;

  VELOX_ASSERT_THROW(
      idGenerator.run(input, ids),
      fmt::format("Exceeded limit of {} distinct partitions.", maxPartitions));
}

TEST_F(PartitionIdGeneratorTest, supportedPartitionKeyTypes) {
  // Test on supported key types.
  {
    PartitionIdGenerator idGenerator(
        ROW({
            VARCHAR(),
            BOOLEAN(),
            VARBINARY(),
            TINYINT(),
            SMALLINT(),
            INTEGER(),
            BIGINT(),
        }),
        {0, 1, 2, 3, 4, 5, 6},
        100,
        pool(),
        true);

    auto input = makeRowVector({
        makeNullableFlatVector<StringView>(
            {"Left", std::nullopt, "Right"}, VARCHAR()),
        makeNullableFlatVector<bool>({true, false, std::nullopt}),
        makeFlatVector<StringView>(
            {"proton", "neutron", "electron"}, VARBINARY()),
        makeNullableFlatVector<int8_t>({1, 2, std::nullopt}),
        makeNullableFlatVector<int16_t>({1, 2, std::nullopt}),
        makeNullableFlatVector<int32_t>({1, std::nullopt, 2}),
        makeNullableFlatVector<int64_t>({std::nullopt, 1, 2}),
    });

    raw_vector<uint64_t> ids;
    idGenerator.run(input, ids);

    EXPECT_TRUE(ids[0] == 0);
    EXPECT_TRUE(ids[1] == 1);
    EXPECT_TRUE(ids[2] == 2);
  }

  // Test unsupported partition key types.
  {
    auto input = makeRowVector({
        makeConstant<float>(1.0, 1),
        makeConstant<double>(1.0, 1),
        makeConstant<Timestamp>(Timestamp::fromMillis(1639426440000), 1),
        makeArrayVector<int32_t>({{1, 2, 3}}),
        makeMapVector<int16_t, int16_t>({{{1, 2}}}),
    });

    for (column_index_t i = 1; i < input->childrenSize(); i++) {
      VELOX_ASSERT_THROW(
          PartitionIdGenerator(
              asRowType(input->type()), {i}, 100, pool(), true),
          fmt::format(
              "Unsupported partition type: {}.",
              input->childAt(i)->type()->toString()));
    }
  }
}

} // namespace facebook::velox::connector::hive
