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

#include <gtest/gtest.h>

#include "velox/exec/PrefixSort.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"

namespace facebook::velox::exec::prefixsort::test {
namespace {

class PrefixSortTest : public exec::test::OperatorTestBase {
 protected:
  std::vector<char*, memory::StlAllocator<char*>>
  storeRows(int numRows, const RowVectorPtr& sortedRows, RowContainer* data);

  static constexpr CompareFlags kAsc{
      true,
      true,
      false,
      CompareFlags::NullHandlingMode::kNullAsValue};

  static constexpr CompareFlags kDesc{
      true,
      false,
      false,
      CompareFlags::NullHandlingMode::kNullAsValue};

  void testPrefixSort(
      const std::vector<CompareFlags>& compareFlags,
      const RowVectorPtr& data) {
    const auto numRows = data->size();
    const auto expectedResult =
        generateExpectedResult(compareFlags, numRows, data);

    const auto rowType = asRowType(data->type());

    // Store data in a RowContainer.
    const std::vector<TypePtr> keyTypes{
        rowType->children().begin(),
        rowType->children().begin() + compareFlags.size()};
    const std::vector<TypePtr> payloadTypes{
        rowType->children().begin() + compareFlags.size(),
        rowType->children().end()};

    RowContainer rowContainer(keyTypes, payloadTypes, pool_.get());
    auto rows = storeRows(numRows, data, &rowContainer);
    const std::shared_ptr<memory::MemoryPool> sortPool =
        rootPool_->addLeafChild("prefixsort");
    const auto maxBytes = PrefixSort::maxRequiredBytes(
        &rowContainer,
        compareFlags,
        common::PrefixSortConfig{
            1024,
            // Set threshold to 0 to enable prefix-sort in small dataset.
            0,
            12},
        sortPool.get());
    const auto beforeBytes = sortPool->peakBytes();
    ASSERT_EQ(sortPool->peakBytes(), 0);
    // Use PrefixSort to sort rows.
    PrefixSort::sort(
        &rowContainer,
        compareFlags,
        common::PrefixSortConfig{
            1024,
            // Set threshold to 0 to enable prefix-sort in small dataset.
            0,
            12},
        sortPool.get(),
        rows);
    ASSERT_GE(maxBytes, sortPool->peakBytes() - beforeBytes);

    // Extract data from the RowContainer in order.
    const RowVectorPtr actual =
        BaseVector::create<RowVector>(rowType, numRows, pool_.get());
    for (int column = 0; column < compareFlags.size(); ++column) {
      rowContainer.extractColumn(
          rows.data(), numRows, column, actual->childAt(column));
    }

    velox::test::assertEqualVectors(actual, expectedResult);
  }

 private:
  // Use std::sort to generate expected result.
  const RowVectorPtr generateExpectedResult(
      const std::vector<CompareFlags>& compareFlags,
      int numRows,
      const RowVectorPtr& sortedRows);
};

std::vector<char*, memory::StlAllocator<char*>> PrefixSortTest::storeRows(
    int numRows,
    const RowVectorPtr& sortedRows,
    RowContainer* data) {
  std::vector<char*, memory::StlAllocator<char*>> rows(*pool());
  SelectivityVector allRows(numRows);
  rows.resize(numRows);
  for (int row = 0; row < numRows; ++row) {
    rows[row] = data->newRow();
  }
  for (int column = 0; column < sortedRows->childrenSize(); ++column) {
    DecodedVector decoded(*sortedRows->childAt(column), allRows);
    for (int i = 0; i < numRows; ++i) {
      char* row = rows[i];
      data->store(decoded, i, row, column);
    }
  }
  return rows;
}

const RowVectorPtr PrefixSortTest::generateExpectedResult(
    const std::vector<CompareFlags>& compareFlags,
    int numRows,
    const RowVectorPtr& sortedRows) {
  const auto rowType = asRowType(sortedRows->type());
  const int numKeys = compareFlags.size();
  RowContainer rowContainer(rowType->children(), pool_.get());
  auto rows = storeRows(numRows, sortedRows, &rowContainer);

  std::sort(
      rows.begin(), rows.end(), [&](const char* leftRow, const char* rightRow) {
        for (auto i = 0; i < numKeys; ++i) {
          if (auto result =
                  rowContainer.compare(leftRow, rightRow, i, compareFlags[i])) {
            return result < 0;
          }
        }
        return false;
      });

  const RowVectorPtr result =
      BaseVector::create<RowVector>(rowType, numRows, pool_.get());
  for (int column = 0; column < compareFlags.size(); ++column) {
    rowContainer.extractColumn(
        rows.data(), numRows, column, result->childAt(column));
  }
  return result;
}

TEST_F(PrefixSortTest, singleKey) {
  // Vectors without nulls.
  const std::vector<VectorPtr> testData = {
      makeFlatVector<int64_t>({5, 4, 3, 2, 1}),
      makeFlatVector<int32_t>({5, 4, 3, 2, 1}),
      makeFlatVector<int16_t>({5, 4, 3, 2, 1}),
      makeFlatVector<int128_t>(
          {5,
           HugeInt::parse("1234567"),
           HugeInt::parse("12345678901234567890"),
           HugeInt::parse("12345679"),
           HugeInt::parse("-12345678901234567890")}),
      makeFlatVector<float>({5.5, 4.4, 3.3, 2.2, 1.1}),
      makeFlatVector<double>({5.5, 4.4, 3.3, 2.2, 1.1}),
      makeFlatVector<Timestamp>(
          {Timestamp(5, 5),
           Timestamp(4, 4),
           Timestamp(3, 3),
           Timestamp(2, 2),
           Timestamp(1, 1)}),
      makeFlatVector<std::string_view>({"eee", "ddd", "ccc", "bbb", "aaa"}),
      makeFlatVector<std::string_view>({"e", "dddddd", "bbb", "ddd", "aaa"}),
      makeFlatVector<std::string_view>(
          {"ddd_not_inline",
           "aaa_is_not_inline",
           "aaa_is_not_inline_a",
           "ddd_is_not_inline",
           "aaa_is_not_inline_b"}),
      makeNullableFlatVector<std::string_view>(
          {"\u7231",
           "\u671B\u5E0C\u2014\u5FF5\u4FE1",
           "\u671B\u5E0C",
           "\u7231\u2014",
           "\u4FE1 \u7231"},
          VARBINARY())};
  for (int i = 0; i < testData.size(); ++i) {
    const auto data = makeRowVector({testData[i]});

    testPrefixSort({kAsc}, data);
    testPrefixSort({kDesc}, data);
  }
}

TEST_F(PrefixSortTest, singleKeyWithNulls) {
  // Vectors with nulls.
  const std::vector<VectorPtr> testData = {
      makeNullableFlatVector<int64_t>({5, 4, std::nullopt, 2, 1}),
      makeNullableFlatVector<int32_t>({5, 4, std::nullopt, 2, 1}),
      makeNullableFlatVector<int16_t>({5, 4, std::nullopt, 2, 1}),
      makeNullableFlatVector<int128_t>(
          {5,
           HugeInt::parse("1234567"),
           std::nullopt,
           HugeInt::parse("12345679"),
           HugeInt::parse("-12345678901234567890")}),
      makeNullableFlatVector<float>({5.5, 4.4, std::nullopt, 2.2, 1.1}),
      makeNullableFlatVector<double>({5.5, 4.4, std::nullopt, 2.2, 1.1}),
      makeNullableFlatVector<Timestamp>(
          {Timestamp(5, 5),
           Timestamp(4, 4),
           std::nullopt,
           Timestamp(2, 2),
           Timestamp(1, 1)}),
      makeNullableFlatVector<std::string_view>(
          {"eee", "ddd", std::nullopt, "bbb", "aaa"}),
      makeNullableFlatVector<std::string_view>(
          {"ee", "aaa", std::nullopt, "d", "aaaaaaaaaaaaaa"}),
      makeNullableFlatVector<std::string_view>(
          {"aaa_is_not_inline",
           "aaa_is_not_inline_2",
           std::nullopt,
           "aaa_is_not_inline_1",
           "aaaaaaaaaaaaaa"}),
      makeNullableFlatVector<std::string_view>(
          {"\u7231",
           "\u671B\u5E0C\u2014\u5FF5\u4FE1",
           std::nullopt,
           "\u7231\u2014",
           "\u4FE1 \u7231"},
          VARBINARY())};

  for (int i = 0; i < testData.size(); ++i) {
    const auto data = makeRowVector({testData[i]});

    testPrefixSort({kAsc}, data);
    testPrefixSort({kDesc}, data);
  }
}

TEST_F(PrefixSortTest, multipleKeys) {
  // Test all keys normalized : bigint, integer
  {
    const auto data = makeRowVector({
        makeNullableFlatVector<int64_t>({5, 2, std::nullopt, 2, 1}),
        makeNullableFlatVector<int32_t>({5, 4, std::nullopt, 2, 1}),
    });

    testPrefixSort({kAsc, kAsc}, data);
    testPrefixSort({kDesc, kDesc}, data);
  }

  // Test keys with semi-normalized : bigint, varchar
  {
    const auto data = makeRowVector({
        makeNullableFlatVector<int64_t>({5, 2, std::nullopt, 2, 1}),
        makeNullableFlatVector<std::string_view>(
            {"eee", "aaa", std::nullopt, "bbb", "aaaa"}),
    });

    testPrefixSort({kAsc, kAsc}, data);
    testPrefixSort({kDesc, kDesc}, data);
  }
}

TEST_F(PrefixSortTest, fuzz) {
  std::vector<TypePtr> keyTypes = {
      INTEGER(),
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      BIGINT(),
      DECIMAL(12, 2),
      DECIMAL(25, 6),
      REAL(),
      DOUBLE(),
      TIMESTAMP(),
      VARCHAR(),
      VARBINARY()};

  auto runFuzzTest = [&](double nullRatio) {
    for (const auto& type : keyTypes) {
      SCOPED_TRACE(fmt::format("{}", type->toString()));
      VectorFuzzer fuzzer(
          {.vectorSize = 10'240, .nullRatio = nullRatio}, pool());
      RowVectorPtr data = fuzzer.fuzzRow(ROW({type}));

      testPrefixSort({kAsc}, data);
      testPrefixSort({kDesc}, data);
    }
  };

  runFuzzTest(0.1);
  runFuzzTest(0.0);
}

TEST_F(PrefixSortTest, fuzzMulti) {
  std::vector<TypePtr> keyTypes = {
      INTEGER(),
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      BIGINT(),
      DECIMAL(12, 2),
      DECIMAL(25, 6),
      REAL(),
      DOUBLE(),
      TIMESTAMP(),
      VARCHAR(),
      VARBINARY()};
  auto runFuzzTest = [&](double nullRatio) {
    VectorFuzzer fuzzer({.vectorSize = 10'240, .nullRatio = nullRatio}, pool());

    for (auto i = 0; i < 20; ++i) {
      auto type1 = fuzzer.randType(keyTypes, 0);
      auto type2 = fuzzer.randType(keyTypes, 0);

      SCOPED_TRACE(fmt::format("{}, {}", type1->toString(), type2->toString()));
      auto data = fuzzer.fuzzRow(ROW({type1, type2, VARCHAR()}));

      testPrefixSort({kAsc, kAsc}, data);
      testPrefixSort({kDesc, kDesc}, data);
    }
  };

  runFuzzTest(0.1);
  runFuzzTest(0.0);
}

TEST_F(PrefixSortTest, checkMaxNormalizedKeySizeForMultipleKeys) {
  // Test the normalizedKeySize doesn't exceed the MaxNormalizedKeySize.
  // The normalizedKeySize for BIGINT should be 8 + 1.
  std::vector<TypePtr> keyTypes = {BIGINT(), BIGINT()};
  std::vector<CompareFlags> compareFlags = {kAsc, kDesc};
  std::vector<std::optional<uint32_t>> maxStringLengths = {
      std::nullopt, std::nullopt};
  std::vector<bool> columnHasNulls = {true, true};
  auto sortLayout = PrefixSortLayout::generate(
      keyTypes, columnHasNulls, compareFlags, 8, 9, maxStringLengths);
  ASSERT_FALSE(sortLayout.hasNormalizedKeys);

  auto sortLayoutOneKey = PrefixSortLayout::generate(
      keyTypes, columnHasNulls, compareFlags, 9, 9, maxStringLengths);
  ASSERT_TRUE(sortLayoutOneKey.hasNormalizedKeys);
  ASSERT_TRUE(sortLayoutOneKey.hasNonNormalizedKey);
  ASSERT_EQ(sortLayoutOneKey.prefixOffsets.size(), 1);
  ASSERT_EQ(sortLayoutOneKey.prefixOffsets[0], 0);

  auto sortLayoutOneKey1 = PrefixSortLayout::generate(
      keyTypes, columnHasNulls, compareFlags, 17, 12, maxStringLengths);
  ASSERT_TRUE(sortLayoutOneKey1.hasNormalizedKeys);
  ASSERT_TRUE(sortLayoutOneKey1.hasNonNormalizedKey);
  ASSERT_EQ(sortLayoutOneKey1.prefixOffsets.size(), 1);
  ASSERT_EQ(sortLayoutOneKey1.prefixOffsets[0], 0);

  auto sortLayoutTwoKeys = PrefixSortLayout::generate(
      keyTypes, columnHasNulls, compareFlags, 18, 12, maxStringLengths);
  ASSERT_TRUE(sortLayoutTwoKeys.hasNormalizedKeys);
  ASSERT_FALSE(sortLayoutTwoKeys.hasNonNormalizedKey);
  ASSERT_FALSE(
      sortLayoutTwoKeys.nonPrefixSortStartIndex <
      sortLayoutTwoKeys.numNormalizedKeys);
  ASSERT_EQ(sortLayoutTwoKeys.prefixOffsets.size(), 2);
  ASSERT_EQ(sortLayoutTwoKeys.prefixOffsets[0], 0);
  ASSERT_EQ(sortLayoutTwoKeys.prefixOffsets[1], 9);
  ASSERT_TRUE(sortLayoutTwoKeys.normalizedKeyHasNullByte[0]);
  ASSERT_TRUE(sortLayoutTwoKeys.normalizedKeyHasNullByte[1]);

  columnHasNulls = {false, false};
  auto sortLayoutTwoKeysNoNulls = PrefixSortLayout::generate(
      keyTypes, columnHasNulls, compareFlags, 18, 12, maxStringLengths);
  ASSERT_TRUE(sortLayoutTwoKeysNoNulls.hasNormalizedKeys);
  ASSERT_FALSE(sortLayoutTwoKeysNoNulls.hasNonNormalizedKey);
  ASSERT_EQ(sortLayoutTwoKeysNoNulls.prefixOffsets.size(), 2);
  ASSERT_EQ(sortLayoutTwoKeysNoNulls.prefixOffsets[0], 0);
  ASSERT_EQ(sortLayoutTwoKeysNoNulls.prefixOffsets[1], 8);
  ASSERT_FALSE(sortLayoutTwoKeysNoNulls.normalizedKeyHasNullByte[0]);
  ASSERT_FALSE(sortLayoutTwoKeysNoNulls.normalizedKeyHasNullByte[1]);
}

TEST_F(PrefixSortTest, optimizeSortKeysOrder) {
  struct {
    RowTypePtr inputType;
    std::vector<column_index_t> keyChannels;
    std::vector<column_index_t> expectedSortedKeyChannels;

    std::string debugString() const {
      return fmt::format(
          "inputType {}, keyChannels {}, expectedSortedKeyChannels {}",
          inputType->toString(),
          fmt::join(keyChannels, ":"),
          fmt::join(expectedSortedKeyChannels, ":"));
    }
  } testSettings[] = {
      {ROW({BIGINT(), BIGINT()}), {0, 1}, {0, 1}},
      {ROW({BIGINT(), BIGINT()}), {1, 0}, {1, 0}},
      {ROW({BIGINT(), BIGINT(), BIGINT()}), {1, 0}, {1, 0}},
      {ROW({BIGINT(), BIGINT(), BIGINT()}), {1, 0}, {1, 0}},
      {ROW({BIGINT(), SMALLINT(), BIGINT()}), {0, 1}, {1, 0}},
      {ROW({BIGINT(), SMALLINT(), VARCHAR()}), {0, 1, 2}, {1, 0, 2}},
      {ROW({TINYINT(), BIGINT(), VARCHAR(), TINYINT(), INTEGER(), VARCHAR()}),
       {2, 1, 0, 4, 5, 3},
       {4, 1, 2, 5, 0, 3}},
      {ROW({INTEGER(), BIGINT(), VARCHAR(), TINYINT(), INTEGER(), VARCHAR()}),
       {5, 4, 3, 2, 1, 0},
       {4, 0, 1, 5, 2, 3}}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    std::vector<IdentityProjection> projections;
    for (auto i = 0; i < testData.keyChannels.size(); ++i) {
      projections.emplace_back(testData.keyChannels[i], i);
    }
    PrefixSortLayout::optimizeSortKeysOrder(testData.inputType, projections);
    std::unordered_set<column_index_t> outputChannelSet;
    for (auto i = 0; i < projections.size(); ++i) {
      ASSERT_EQ(
          projections[i].inputChannel, testData.expectedSortedKeyChannels[i]);
      ASSERT_EQ(
          testData.keyChannels[projections[i].outputChannel],
          projections[i].inputChannel);
    }
  }
}

TEST_F(PrefixSortTest, makeSortLayoutForString) {
  std::vector<TypePtr> keyTypes = {VARCHAR(), BIGINT()};
  std::vector<CompareFlags> compareFlags = {kAsc, kDesc};
  std::vector<std::optional<uint32_t>> maxStringLengths = {9, std::nullopt};
  std::vector<bool> columnHasNulls = {true, true};

  auto sortLayoutOneKey = PrefixSortLayout::generate(
      keyTypes, columnHasNulls, compareFlags, 24, 8, maxStringLengths);
  ASSERT_TRUE(sortLayoutOneKey.hasNormalizedKeys);
  ASSERT_TRUE(sortLayoutOneKey.hasNonNormalizedKey);
  ASSERT_TRUE(
      sortLayoutOneKey.nonPrefixSortStartIndex <
      sortLayoutOneKey.numNormalizedKeys);
  ASSERT_EQ(sortLayoutOneKey.encodeSizes.size(), 1);
  ASSERT_EQ(sortLayoutOneKey.encodeSizes[0], 9);

  auto sortLayoutTwoCompleteKeys = PrefixSortLayout::generate(
      keyTypes, columnHasNulls, compareFlags, 26, 9, maxStringLengths);
  ASSERT_TRUE(sortLayoutTwoCompleteKeys.hasNormalizedKeys);
  ASSERT_FALSE(sortLayoutTwoCompleteKeys.hasNonNormalizedKey);
  ASSERT_TRUE(
      sortLayoutTwoCompleteKeys.nonPrefixSortStartIndex ==
      sortLayoutTwoCompleteKeys.numNormalizedKeys);
  ASSERT_EQ(sortLayoutTwoCompleteKeys.encodeSizes.size(), 2);
  ASSERT_EQ(sortLayoutTwoCompleteKeys.encodeSizes[0], 10);
  ASSERT_EQ(sortLayoutTwoCompleteKeys.encodeSizes[1], 9);

  // The last key type is VARBINARY, indicating that only partial data is
  // encoded in the prefix.
  std::vector<TypePtr> keyTypes1 = {BIGINT(), VARBINARY()};
  maxStringLengths = {std::nullopt, 9};
  auto sortLayoutTwoKeys = PrefixSortLayout::generate(
      keyTypes1, columnHasNulls, compareFlags, 26, 8, maxStringLengths);
  ASSERT_TRUE(sortLayoutTwoKeys.hasNormalizedKeys);
  ASSERT_FALSE(sortLayoutTwoKeys.hasNonNormalizedKey);
  ASSERT_TRUE(
      sortLayoutTwoKeys.nonPrefixSortStartIndex <
      sortLayoutTwoKeys.numNormalizedKeys);
  ASSERT_EQ(sortLayoutTwoKeys.encodeSizes.size(), 2);
  ASSERT_EQ(sortLayoutTwoKeys.encodeSizes[0], 9);
  ASSERT_EQ(sortLayoutTwoKeys.encodeSizes[1], 9);
}

} // namespace
} // namespace facebook::velox::exec::prefixsort::test
