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
  std::vector<char*>
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
    std::vector<char*> rows = storeRows(numRows, data, &rowContainer);

    // Use PrefixSort to sort rows.
    PrefixSort::sort(
        rows,
        pool_.get(),
        &rowContainer,
        compareFlags,
        common::PrefixSortConfig{
            1024,
            // Set threshold to 0 to enable prefix-sort in small dataset.
            0});

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

std::vector<char*> PrefixSortTest::storeRows(
    int numRows,
    const RowVectorPtr& sortedRows,
    RowContainer* data) {
  std::vector<char*> rows;
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
  std::vector<char*> rows = storeRows(numRows, sortedRows, &rowContainer);

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
  const int numRows = 5;
  const int columnsSize = 7;

  // Vectors without nulls.
  const std::vector<VectorPtr> testData = {
      makeFlatVector<int64_t>({5, 4, 3, 2, 1}),
      makeFlatVector<int32_t>({5, 4, 3, 2, 1}),
      makeFlatVector<int16_t>({5, 4, 3, 2, 1}),
      makeFlatVector<float>({5.5, 4.4, 3.3, 2.2, 1.1}),
      makeFlatVector<double>({5.5, 4.4, 3.3, 2.2, 1.1}),
      makeFlatVector<Timestamp>(
          {Timestamp(5, 5),
           Timestamp(4, 4),
           Timestamp(3, 3),
           Timestamp(2, 2),
           Timestamp(1, 1)}),
      makeFlatVector<std::string_view>({"eee", "ddd", "ccc", "bbb", "aaa"})};
  for (int i = 5; i < columnsSize; ++i) {
    const auto data = makeRowVector({testData[i]});

    testPrefixSort({kAsc}, data);
    testPrefixSort({kDesc}, data);
  }
}

TEST_F(PrefixSortTest, singleKeyWithNulls) {
  const int numRows = 5;
  const int columnsSize = 7;

  Timestamp ts = {5, 5};
  // Vectors with nulls.
  const std::vector<VectorPtr> testData = {
      makeNullableFlatVector<int64_t>({5, 4, std::nullopt, 2, 1}),
      makeNullableFlatVector<int32_t>({5, 4, std::nullopt, 2, 1}),
      makeNullableFlatVector<int16_t>({5, 4, std::nullopt, 2, 1}),
      makeNullableFlatVector<float>({5.5, 4.4, std::nullopt, 2.2, 1.1}),
      makeNullableFlatVector<double>({5.5, 4.4, std::nullopt, 2.2, 1.1}),
      makeNullableFlatVector<Timestamp>(
          {Timestamp(5, 5),
           Timestamp(4, 4),
           std::nullopt,
           Timestamp(2, 2),
           Timestamp(1, 1)}),
      makeNullableFlatVector<std::string_view>(
          {"eee", "ddd", std::nullopt, "bbb", "aaa"})};

  for (int i = 5; i < columnsSize; ++i) {
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
            {"eee", "ddd", std::nullopt, "bbb", "aaa"}),
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
      HUGEINT(),
      REAL(),
      DOUBLE(),
      TIMESTAMP(),
      VARCHAR(),
      VARBINARY()};
  for (const auto& type : keyTypes) {
    SCOPED_TRACE(fmt::format("{}", type->toString()));
    VectorFuzzer fuzzer({.vectorSize = 10'240, .nullRatio = 0.1}, pool());
    RowVectorPtr data = fuzzer.fuzzRow(ROW({type}));

    testPrefixSort({kAsc}, data);
    testPrefixSort({kDesc}, data);
  }
}

TEST_F(PrefixSortTest, fuzzMulti) {
  std::vector<TypePtr> keyTypes = {
      INTEGER(),
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      BIGINT(),
      HUGEINT(),
      REAL(),
      DOUBLE(),
      TIMESTAMP(),
      VARCHAR(),
      VARBINARY()};

  VectorFuzzer fuzzer({.vectorSize = 10'240, .nullRatio = 0.1}, pool());

  for (auto i = 0; i < 20; ++i) {
    auto type1 = fuzzer.randType(keyTypes, 0);
    auto type2 = fuzzer.randType(keyTypes, 0);

    SCOPED_TRACE(fmt::format("{}, {}", type1->toString(), type2->toString()));
    auto data = fuzzer.fuzzRow(ROW({type1, type2, VARCHAR()}));

    testPrefixSort({kAsc, kAsc}, data);
    testPrefixSort({kDesc, kDesc}, data);
  }
}
} // namespace
} // namespace facebook::velox::exec::prefixsort::test
