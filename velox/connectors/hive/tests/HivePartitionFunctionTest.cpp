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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

class HivePartitionFunctionTest : public ::testing::Test,
                                  public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void assertPartitions(
      const VectorPtr& vector,
      int bucketCount,
      const std::vector<uint32_t>& expectedPartitions) {
    auto rowVector = makeRowVector({vector});

    auto size = rowVector->size();

    std::vector<int> bucketToPartition(bucketCount);
    std::iota(bucketToPartition.begin(), bucketToPartition.end(), 0);
    std::vector<column_index_t> keyChannels;
    keyChannels.emplace_back(0);
    connector::hive::HivePartitionFunction partitionFunction(
        bucketCount, bucketToPartition, keyChannels);
    std::vector<uint32_t> partitions(size);
    partitionFunction.partition(*rowVector, partitions);
    for (auto i = 0; i < size; ++i) {
      EXPECT_EQ(expectedPartitions[i], partitions[i])
          << "at " << i << ": " << vector->toString(i);
    }

    // Retry the same with nested dictionaries where indices  for some
    // non-referenced rows are bad.
    auto innerIndices = makeIndicesInReverse(size);
    auto outerIndices = makeIndices(size, [](auto i) { return i; });
    outerIndices->asMutable<vector_size_t>()[size - 1] =
        std::numeric_limits<int32_t>().max();
    auto dictValues = wrapInDictionary(
        outerIndices, size - 1, wrapInDictionary(innerIndices, vector));
    rowVector = makeRowVector({dictValues});
    partitionFunction.partition(*rowVector, partitions);
    for (auto i = 0; i < size - 1; ++i) {
      EXPECT_EQ(expectedPartitions[size - 1 - i], partitions[i])
          << "at " << i << ": " << vector->toString(i);
    }
  }

  // Run one function with 3 columns (two of which have the same values in each
  // row) and another with two constant channels replacing the last two columns.
  // Partitions should be the same.
  void assertPartitionsWithConstChannel(
      const VectorPtr& vector,
      int bucketCount) {
    auto column2 = makeFlatVector<int64_t>(
        vector->size(), [](auto /*row*/) { return 13; });
    auto column3 = makeFlatVector<int64_t>(
        vector->size(), [](auto /*row*/) { return 97; });
    auto rowVector = makeRowVector({vector, column2, column3});

    auto size = rowVector->size();
    std::vector<VectorPtr> constValues{
        // Must declare the literal as std::vector, else function
        // resolves to makeFlatVector(size_t).
        makeFlatVector<int64_t>(std::vector<int64_t>{13}),
        makeFlatVector<int64_t>(std::vector<int64_t>{97})};

    std::vector<int> bucketToPartition(bucketCount);
    std::iota(bucketToPartition.begin(), bucketToPartition.end(), 0);
    std::vector<column_index_t> keyChannelsNorm{0, 1, 2};
    connector::hive::HivePartitionFunction partitionFunctionNorm(
        bucketCount, bucketToPartition, keyChannelsNorm);

    std::vector<column_index_t> keyChannelsConst{
        0, kConstantChannel, kConstantChannel};
    connector::hive::HivePartitionFunction partitionFunctionConst(
        bucketCount, bucketToPartition, keyChannelsConst, constValues);

    std::vector<uint32_t> partitionsNorm(size);
    partitionFunctionNorm.partition(*rowVector, partitionsNorm);
    std::vector<uint32_t> partitionsConst(size);
    partitionFunctionConst.partition(*rowVector, partitionsConst);
    EXPECT_EQ(partitionsNorm, partitionsConst);
  }
};

TEST_F(HivePartitionFunctionTest, bigint) {
  auto values = makeNullableFlatVector<int64_t>(
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
  auto values = makeNullableFlatVector<std::string>(
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
      makeNullableFlatVector<bool>({std::nullopt, true, false, false, true});

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
  auto values = makeNullableFlatVector<int8_t>(
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
  auto values = makeNullableFlatVector<int16_t>(
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
  auto values = makeNullableFlatVector<int32_t>(
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
  auto values = makeNullableFlatVector<float>(
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
  auto values = makeNullableFlatVector<double>(
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
  auto values = makeNullableFlatVector<Timestamp>(
      {std::nullopt,
       Timestamp(100'000, 900'000),
       Timestamp(Timestamp::kMinSeconds, std::numeric_limits<uint64_t>::min()),
       Timestamp(Timestamp::kMaxSeconds, Timestamp::kMaxNanos)});

  assertPartitions(values, 1, {0, 0, 0, 0});
  assertPartitions(values, 2, {0, 0, 0, 0});
  assertPartitions(values, 500, {0, 284, 122, 450});
  assertPartitions(values, 997, {0, 514, 404, 733});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, date) {
  auto values = makeNullableFlatVector<int32_t>(
      {std::nullopt,
       2'000'000'000,
       std::numeric_limits<int32_t>::min(),
       std::numeric_limits<int32_t>::max()},
      DATE());

  assertPartitions(values, 1, {0, 0, 0, 0});
  assertPartitions(values, 2, {0, 0, 0, 1});
  assertPartitions(values, 500, {0, 0, 0, 147});
  assertPartitions(values, 997, {0, 54, 0, 482});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, array) {
  auto values = makeNullableArrayVector<int32_t>(
      std::vector<std::optional<std::vector<std::optional<int32_t>>>>{
          std::nullopt,
          std::vector<std::optional<int32_t>>{},
          std::vector<std::optional<int32_t>>{std::nullopt},
          std::vector<std::optional<int32_t>>{1},
          std::vector<std::optional<int32_t>>{1, 2, 3}});

  assertPartitions(values, 1, {0, 0, 0, 0, 0});
  assertPartitions(values, 2, {0, 0, 0, 1, 0});
  assertPartitions(values, 500, {0, 0, 0, 1, 26});
  assertPartitions(values, 997, {0, 0, 0, 1, 29});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, arrayElementsEncoded) {
  // Dictionary encode the elements.
  auto elements = makeFlatVector<int32_t>(
      10, [](auto row) { return row; }, [](auto row) { return row % 4 == 0; });
  auto indices = makeIndicesInReverse(elements->size());
  auto encodedElements = wrapInDictionary(indices, elements);

  vector_size_t size = 5;
  BufferPtr offsetsBuffer = allocateOffsets(size, pool_.get());
  BufferPtr sizesBuffer = allocateSizes(size, pool_.get());
  BufferPtr nullsBuffer =
      AlignedBuffer::allocate<bool>(size, pool_.get(), bits::kNotNull);
  ;
  auto rawOffsets = offsetsBuffer->asMutable<vector_size_t>();
  auto rawSizes = sizesBuffer->asMutable<vector_size_t>();
  auto rawNulls = nullsBuffer->asMutable<uint64_t>();

  // Make the elements have gaps.
  // Set the values in position 2 to be invalid since that Array should be null.
  std::vector<vector_size_t> offsets{
      0, 2, std::numeric_limits<int32_t>().max(), 4, 8};
  std::vector<vector_size_t> sizes{
      2, 1, std::numeric_limits<int32_t>().max(), 3, 2};
  memcpy(rawOffsets, offsets.data(), size * sizeof(vector_size_t));
  memcpy(rawSizes, sizes.data(), size * sizeof(vector_size_t));

  bits::setNull(rawNulls, 2);

  auto values = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(elements->type()),
      nullsBuffer,
      size,
      offsetsBuffer,
      sizesBuffer,
      encodedElements);

  assertPartitions(values, 1, {0, 0, 0, 0, 0});
  assertPartitions(values, 2, {1, 1, 0, 0, 1});
  assertPartitions(values, 500, {279, 7, 0, 308, 31});
  assertPartitions(values, 997, {279, 7, 0, 820, 31});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, nestedArrays) {
  auto innerArrays = makeNullableArrayVector<int32_t>(
      std::vector<std::optional<std::vector<std::optional<int32_t>>>>{
          std::vector<std::optional<int32_t>>{1, 2, 3},
          std::vector<std::optional<int32_t>>{4, 5},
          std::vector<std::optional<int32_t>>{6, 7, 8},
          std::nullopt,
          std::vector<std::optional<int32_t>>{9},
          std::vector<std::optional<int32_t>>{10, std::nullopt, 11}});
  auto values = makeArrayVector({0, 2, 2, 3}, innerArrays, {1});

  assertPartitions(values, 1, {0, 0, 0, 0});
  assertPartitions(values, 2, {1, 0, 1, 0});
  assertPartitions(values, 500, {435, 0, 491, 400});
  assertPartitions(values, 997, {31, 0, 9, 927});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, map) {
  auto values = makeNullableMapVector<std::string, int32_t>(
      std::vector<std::optional<
          std::vector<std::pair<std::string, std::optional<int32_t>>>>>{
          std::nullopt,
          std::vector<std::pair<std::string, std::optional<int32_t>>>{},
          std::vector<std::pair<std::string, std::optional<int32_t>>>{
              {"a", std::nullopt}},
          std::vector<std::pair<std::string, std::optional<int32_t>>>{{"b", 1}},
          std::vector<std::pair<std::string, std::optional<int32_t>>>{
              {"x", 1}, {"y", 2}, {"z", 3}}});

  assertPartitions(values, 1, {0, 0, 0, 0, 0});
  assertPartitions(values, 2, {0, 0, 1, 1, 1});
  assertPartitions(values, 500, {0, 0, 97, 99, 365});
  assertPartitions(values, 997, {0, 0, 97, 99, 365});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, mapEntriesEncoded) {
  vector_size_t elementsSize = 10;

  // Dictionary encode the keys and values.
  auto mapKeys = makeFlatVector<std::string>(
      elementsSize, [](auto row) { return fmt::format("key_{}", row); });
  // Produces indices: [0, 3, 6, 9, 2, 5, 8, 1, 4, 7]
  auto keyIndices =
      makeIndices(elementsSize, [](auto row) { return (row * 3) % 10; });
  auto encodedKeys = wrapInDictionary(keyIndices, mapKeys);
  auto mapValues = makeFlatVector<int32_t>(
      elementsSize,
      [](auto row) { return row; },
      [](auto row) { return row % 4 == 0; });
  auto valueIndices = makeIndicesInReverse(elementsSize);
  auto encodedValues = wrapInDictionary(valueIndices, mapValues);

  vector_size_t size = 5;
  BufferPtr offsetsBuffer = allocateOffsets(size, pool_.get());
  BufferPtr sizesBuffer = allocateSizes(size, pool_.get());
  BufferPtr nullsBuffer =
      AlignedBuffer::allocate<bool>(size, pool_.get(), bits::kNotNull);
  ;
  auto rawOffsets = offsetsBuffer->asMutable<vector_size_t>();
  auto rawSizes = sizesBuffer->asMutable<vector_size_t>();
  auto rawNulls = nullsBuffer->asMutable<uint64_t>();

  // Make the elements have gaps.
  // Set the values in position 2 to be invalid since that Map should be null.
  std::vector<vector_size_t> offsets{
      0, 2, std::numeric_limits<int32_t>().max(), 4, 8};
  std::vector<vector_size_t> sizes{
      2, 1, std::numeric_limits<int32_t>().max(), 3, 2};
  memcpy(rawOffsets, offsets.data(), size * sizeof(vector_size_t));
  memcpy(rawSizes, sizes.data(), size * sizeof(vector_size_t));

  bits::setNull(rawNulls, 2);

  auto values = std::make_shared<MapVector>(
      pool_.get(),
      MAP(mapKeys->type(), mapValues->type()),
      nullsBuffer,
      size,
      offsetsBuffer,
      sizesBuffer,
      encodedKeys,
      encodedValues);

  assertPartitions(values, 1, {0, 0, 0, 0, 0});
  assertPartitions(values, 2, {0, 1, 0, 1, 0});
  assertPartitions(values, 500, {336, 413, 0, 259, 336});
  assertPartitions(values, 997, {345, 666, 0, 24, 345});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, nestedMaps) {
  auto innerMaps = makeNullableMapVector<int32_t, float>(
      std::vector<
          std::optional<std::vector<std::pair<int32_t, std::optional<float>>>>>{
          std::vector<std::pair<int32_t, std::optional<float>>>{
              {1, -1.0}, {2, -2.0}, {3, -3.0}},
          std::vector<std::pair<int32_t, std::optional<float>>>{
              {4, -4.0}, {5, -5.0}},
          std::vector<std::pair<int32_t, std::optional<float>>>{
              {6, -6.0}, {7, -7.0}, {8, -8.0}},
          std::nullopt,
          std::vector<std::pair<int32_t, std::optional<float>>>{{9, -9.0}},
          std::vector<std::pair<int32_t, std::optional<float>>>{
              {10, -10.0}, {12, std::nullopt}, {11, -11.0}}});
  auto keys = makeFlatVector<std::string>({"a", "b", "c", "d", "e", "f"});
  auto values = makeMapVector({0, 2, 2, 3}, keys, innerMaps, {1});

  assertPartitions(values, 1, {0, 0, 0, 0});
  assertPartitions(values, 2, {0, 0, 0, 1});
  assertPartitions(values, 500, {98, 0, 134, 207});
  assertPartitions(values, 997, {189, 0, 569, 505});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, row) {
  auto col1 = makeNullableFlatVector<int32_t>({std::nullopt, std::nullopt, 1});
  auto col2 = makeNullableFlatVector<float>({std::nullopt, std::nullopt, 1.0});
  auto col3 =
      makeNullableFlatVector<std::string>({std::nullopt, std::nullopt, "a"});
  auto values =
      makeRowVector({col1, col2, col3}, [](auto row) { return row == 0; });

  assertPartitions(values, 1, {0, 0, 0});
  assertPartitions(values, 2, {0, 0, 0});
  assertPartitions(values, 500, {0, 0, 34});
  assertPartitions(values, 997, {0, 0, 466});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, rowFieldsEncoded) {
  vector_size_t size = 5;

  // Dictionary encode the fields.
  auto col1 = makeFlatVector<int32_t>(
      size, [](auto row) { return row; }, [](auto row) { return row == 2; });
  // Produces indices: [0, 2, 4, 1, 3]
  auto col1Indices = makeIndices(size, [](auto row) { return (row * 2) % 5; });
  auto encodedCol1 = wrapInDictionary(col1Indices, col1);
  auto col2 = makeFlatVector<float>(
      size,
      [](auto row) { return row + 10.0; },
      [](auto row) { return row == 4; });
  // Produces keys: [0, 3, 1, 4, 2]
  auto col2Indices = makeIndices(size, [](auto row) { return (row * 3) % 5; });
  auto encodedCol2 = wrapInDictionary(col2Indices, col2);
  auto col3 = makeFlatVector<std::string>(
      size, [](auto row) { return fmt::format("{}", row + 20); });
  auto col3Indices = makeIndicesInReverse(size);
  auto encodedCol3 = wrapInDictionary(col3Indices, col3);

  BufferPtr nullsBuffer =
      AlignedBuffer::allocate<bool>(size, pool_.get(), bits::kNotNull);
  ;
  auto rawNulls = nullsBuffer->asMutable<uint64_t>();

  bits::setNull(rawNulls, 2);

  // Produces rows that look like:
  // {col1: 0, col2: 10.0, col3: "24"}
  // {col1: NULL, col2: 13.0, col3: "23"}
  // NULL
  // {col1: 1, col2: NULL, col3: "21"}
  // {col1: 3, col2: 12.0, col3: "20"}
  auto values = std::make_shared<RowVector>(
      pool_.get(),
      ROW({col1->type(), col2->type(), col3->type()}),
      nullsBuffer,
      size,
      std::vector<VectorPtr>{encodedCol1, encodedCol2, encodedCol3});

  assertPartitions(values, 1, {0, 0, 0, 0, 0});
  assertPartitions(values, 2, {0, 1, 0, 0, 1});
  assertPartitions(values, 500, {334, 401, 0, 60, 425});
  assertPartitions(values, 997, {354, 354, 0, 566, 575});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, nestedRows) {
  auto innerCol1 = makeNullableFlatVector<int32_t>(
      {std::nullopt, std::nullopt, 1, std::nullopt, 3});
  auto innerCol2 = makeNullableFlatVector<float>(
      {2.0, std::nullopt, 1.0, std::nullopt, 3.0});
  auto innerRow =
      makeRowVector({innerCol1, innerCol2}, [](auto row) { return row == 1; });
  auto outerCol = makeNullableFlatVector<std::string>(
      {"a", "b", std::nullopt, std::nullopt, "c"});
  auto values =
      makeRowVector({outerCol, innerRow}, [](auto row) { return row == 3; });

  assertPartitions(values, 1, {0, 0, 0, 0, 0});
  assertPartitions(values, 2, {1, 0, 1, 0, 0});
  assertPartitions(values, 500, {331, 38, 247, 0, 290});
  assertPartitions(values, 997, {756, 47, 921, 0, 836});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}

TEST_F(HivePartitionFunctionTest, spec) {
  Type::registerSerDe();
  core::ITypedExpr::registerSerDe();
  const int bucketCount = 14;
  // Build round-robin mapping for testing below.
  const std::vector<int> bucketToPartition = {
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3};
  ASSERT_EQ(bucketToPartition.size(), bucketCount);
  // std::iota(bucketToPartition.begin(), bucketToPartition.end(), 0);

  // The test case with 1 constValues.
  {
    auto hiveSpec =
        std::make_unique<connector::hive::HivePartitionFunctionSpec>(
            bucketCount,
            bucketToPartition,
            std::vector<column_index_t>{
                0, 1, kConstantChannel, 3, kConstantChannel},
            std::vector<VectorPtr>{makeConstant(123, 1), makeConstant(17, 1)});
    ASSERT_EQ(
        hiveSpec->toString(), "HIVE((0, 1, \"123\", 3, \"17\") buckets: 14)");

    auto serialized = hiveSpec->serialize();
    ASSERT_EQ(serialized["constants"].size(), 2);

    auto copy = connector::hive::HivePartitionFunctionSpec::deserialize(
        serialized, pool());
    ASSERT_EQ(hiveSpec->toString(), copy->toString());
  }

  // The test case with 0 constValues.
  {
    auto hiveSpec =
        std::make_unique<connector::hive::HivePartitionFunctionSpec>(
            bucketCount,
            bucketToPartition,
            std::vector<column_index_t>{0, 1, 2, 3, 4},
            std::vector<VectorPtr>{});
    ASSERT_EQ(hiveSpec->toString(), "HIVE((0, 1, 2, 3, 4) buckets: 14)");

    auto serialized = hiveSpec->serialize();
    ASSERT_EQ(serialized["constants"].size(), 0);

    auto copy = connector::hive::HivePartitionFunctionSpec::deserialize(
        serialized, pool());
    ASSERT_EQ(hiveSpec->toString(), copy->toString());
  }

  // The test case without bucket to partition map.
  {
    auto hiveSpecWithoutPartitionMap =
        std::make_unique<connector::hive::HivePartitionFunctionSpec>(
            bucketCount,
            std::vector<column_index_t>{0, 1, 2},
            std::vector<VectorPtr>{});
    ASSERT_EQ(
        hiveSpecWithoutPartitionMap->toString(), "HIVE((0, 1, 2) buckets: 14)");
    {
      auto serialized = hiveSpecWithoutPartitionMap->serialize();

      auto copy = connector::hive::HivePartitionFunctionSpec::deserialize(
          serialized, pool());
      ASSERT_EQ(hiveSpecWithoutPartitionMap->toString(), copy->toString());
    }
    auto hiveFunctionWithoutPartitionMap =
        hiveSpecWithoutPartitionMap->create(10);

    auto hiveSpecWithPartitionMap =
        std::make_unique<connector::hive::HivePartitionFunctionSpec>(
            bucketCount,
            bucketToPartition,
            std::vector<column_index_t>{0, 1, 2},
            std::vector<VectorPtr>{});
    ASSERT_EQ(
        hiveSpecWithoutPartitionMap->toString(), "HIVE((0, 1, 2) buckets: 14)");
    {
      auto serialized = hiveSpecWithoutPartitionMap->serialize();

      auto copy = connector::hive::HivePartitionFunctionSpec::deserialize(
          serialized, pool());
      ASSERT_EQ(hiveSpecWithPartitionMap->toString(), copy->toString());
    }
    auto hiveFunctionWithPartitionMap = hiveSpecWithPartitionMap->create(10);

    // Test two functions generates the same result.
    auto rowType =
        ROW({"c0", "c1", "c2", "c3", "c4"},
            {INTEGER(), VARCHAR(), BIGINT(), TINYINT(), TIMESTAMP()});
    const int vectorSize = 1000;
    VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
    std::vector<uint32_t> partitionIdsWithMap;
    partitionIdsWithMap.reserve(vectorSize);
    std::vector<uint32_t> partitionIdsWithoutMap;
    partitionIdsWithoutMap.reserve(vectorSize);
    for (int i = 0; i < 5; ++i) {
      auto vector = fuzzer.fuzzRow(rowType);
      hiveFunctionWithPartitionMap->partition(*vector, partitionIdsWithMap);
      hiveFunctionWithoutPartitionMap->partition(
          *vector, partitionIdsWithoutMap);
      for (int j = 0; j < vectorSize; ++j) {
        ASSERT_EQ(partitionIdsWithMap[j], partitionIdsWithoutMap[j]) << j;
      }
    }
  }
}

TEST_F(HivePartitionFunctionTest, function) {
  Type::registerSerDe();
  core::ITypedExpr::registerSerDe();
  const int bucketCount = 10;
  // Build an identical bucket to partition map for testing below.
  const std::vector<int> bucketToPartition = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  auto hiveFunctionWithoutPartitionMap =
      std::make_unique<connector::hive::HivePartitionFunction>(
          bucketCount,
          std::vector<column_index_t>{0, 1, 2},
          std::vector<VectorPtr>{});

  auto hiveFunctionWithPartitionMap =
      std::make_unique<connector::hive::HivePartitionFunction>(
          bucketCount,
          bucketToPartition,
          std::vector<column_index_t>{0, 1, 2},
          std::vector<VectorPtr>{});
  // Test two functions generates the same result.
  auto rowType =
      ROW({"c0", "c1", "c2", "c3", "c4"},
          {INTEGER(), VARCHAR(), BIGINT(), TINYINT(), TIMESTAMP()});
  const int vectorSize = 1000;
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  std::vector<uint32_t> partitionIdsWithMap;
  partitionIdsWithMap.reserve(vectorSize);
  std::vector<uint32_t> partitionIdsWithoutMap;
  partitionIdsWithoutMap.reserve(vectorSize);
  for (int i = 0; i < 5; ++i) {
    auto vector = fuzzer.fuzzRow(rowType);
    hiveFunctionWithPartitionMap->partition(*vector, partitionIdsWithMap);
    hiveFunctionWithoutPartitionMap->partition(*vector, partitionIdsWithoutMap);
    for (int j = 0; j < vectorSize; ++j) {
      ASSERT_EQ(partitionIdsWithMap[j], partitionIdsWithoutMap[j]) << j;
    }
  }
}

TEST_F(HivePartitionFunctionTest, unknown) {
  auto values = makeAllNullFlatVector<UnknownValue>(4);

  assertPartitions(values, 1, {0, 0, 0, 0});
  assertPartitions(values, 2, {0, 0, 0, 0});
  assertPartitions(values, 500, {0, 0, 0, 0});
  assertPartitions(values, 997, {0, 0, 0, 0});

  assertPartitionsWithConstChannel(values, 1);
  assertPartitionsWithConstChannel(values, 2);
  assertPartitionsWithConstChannel(values, 500);
  assertPartitionsWithConstChannel(values, 997);
}
