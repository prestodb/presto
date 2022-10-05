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
#include "velox/exec/RowContainer.h"
#include <gtest/gtest.h>
#include <algorithm>
#include <array>
#include <random>
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/ContainerRowSerde.h"
#include "velox/exec/VectorHasher.h"
#include "velox/exec/tests/utils/RowContainerTestBase.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

class RowContainerTest : public exec::test::RowContainerTestBase {
 protected:
  void testExtractColumn(
      RowContainer& container,
      const std::vector<char*>& rows,
      int column,
      const VectorPtr& expected) {
    // This set of tests uses all variations of the extractColumn API to copy
    // column value for all rows in the container. Run these with offset 0
    // and offset to skip the first 1/3 rows.
    testExtractColumnAllRows(container, rows, column, expected, 0);
    testExtractColumnAllRows(
        container, rows, column, expected, rows.size() / 3);
    // This set of tests use extractColumn with an out-of-order pattern
    // and repetitions. Run these with offset 0 and offset to skip the first
    // 1/3 rows.
    testOutOfOrderRowNumbers(container, rows, column, expected, 0);
    testOutOfOrderRowNumbers(
        container, rows, column, expected, rows.size() / 3);

    testRepeatedRowNumbers(container, rows, column, expected);
    testNegativeRowNumbers(container, rows, column, expected);
  }

  void testExtractColumnAllRows(
      RowContainer& container,
      const std::vector<char*>& rows,
      int column,
      const VectorPtr& expected,
      vector_size_t offset) {
    auto size = rows.size();

    auto testEqualVectors = [](const VectorPtr& lhs,
                               const VectorPtr& rhs,
                               vector_size_t lhsIndex,
                               vector_size_t rhsIndex) {
      for (auto i = 0; i < lhs->size() - lhsIndex; i++) {
        EXPECT_TRUE(lhs->equalValueAt(rhs.get(), lhsIndex + i, rhsIndex + i));
      }
    };

    auto testBasic = [&]() {
      // Test the extractColumn API that didn't use the offset parameter and
      // copied to the start of the result vector.
      auto result = BaseVector::create(expected->type(), size, pool_.get());
      container.extractColumn(rows.data(), size, column, result);
      assertEqualVectors(expected, result);
    };

    auto testBasicWithOffset = [&]() {
      // Test extractColumn from offset.
      auto result = BaseVector::create(expected->type(), size, pool_.get());
      container.extractColumn(
          rows.data(), size - offset, column, offset, result);
      testEqualVectors(result, expected, offset, 0);
    };

    auto testRowNumbers = [&]() {
      auto result = BaseVector::create(expected->type(), size, pool_.get());

      std::vector<vector_size_t> rowNumbers;
      rowNumbers.resize(size - offset);
      for (int i = 0; i < size - offset; i++) {
        rowNumbers[i] = i + offset;
      }
      folly::Range<const vector_size_t*> rowNumbersRange =
          folly::Range(rowNumbers.data(), size - offset);
      container.extractColumn(
          rows.data(), rowNumbersRange, column, offset, result);
      testEqualVectors(result, expected, offset, offset);
    };

    // Test using extractColumn API.
    testBasic();
    testBasicWithOffset();
    // Test using extractColumn (with rowNumbers) API.
    testRowNumbers();
  }

  void testOutOfOrderRowNumbers(
      RowContainer& container,
      const std::vector<char*>& rows,
      int column,
      const VectorPtr& expected,
      vector_size_t offset) {
    auto size = rows.size();

    // In this test the extractColumn API is operated on an out-of-order
    // input (either the base input data or the rowNumbers). The new ordering
    // has null in even positions and retains odd positions from the original
    // input.

    // This is a helper method for result verification for the out of order
    // results.
    auto verifyResults = [&](const VectorPtr& result, int offset = 0) {
      EXPECT_EQ(size, result->size());
      for (vector_size_t i = offset; i < size; ++i) {
        if (i % 2 == 0) {
          EXPECT_TRUE(result->isNullAt(i)) << "at " << i;
        } else {
          EXPECT_TRUE(expected->equalValueAt(result.get(), i, i))
              << "at " << i << ": expected " << expected->toString(i)
              << ", got " << result->toString();
        }
      }
    };

    auto testBasic = [&]() {
      // Construct an input source with null in even
      // positions and that retains the original rows in
      // odd positions. Copy its rows
      std::vector<char*> input(size, nullptr);
      for (auto i = 1; i < size; i += 2) {
        input[i] = rows[i];
      }

      // Test the extractColumn API that didn't use the offset parameter
      // and copies to the start of the result vector.
      auto result = BaseVector::create(expected->type(), size, pool_.get());
      container.extractColumn(input.data(), size, column, result);
      verifyResults(result);

      // Test extractColumn from offset.
      container.extractColumn(
          input.data() + offset, size - offset, column, offset, result);
      verifyResults(result, offset);
    };

    auto testRowNumbers = [&]() {
      // Setup input rows vector so that the first row is null.
      std::vector<char*> input(rows);
      input[0] = nullptr;

      // The rowNumbersBuffer has values like 0, 1, 0, 3, 0, 5, 0, 7, etc
      // This tests the case of column extraction where the rowNumber values
      // are repeated and are also out of order.
      std::vector<vector_size_t> rowNumbers;
      auto rowNumbersSize = size - offset;
      rowNumbers.resize(rowNumbersSize);

      for (int i = 0; i < rowNumbersSize; i++) {
        if ((i + offset) % 2 == 0) {
          rowNumbers[i] = 0;
        } else {
          rowNumbers[i] = i + offset;
        }
      }

      auto result = BaseVector::create(expected->type(), size, pool_.get());
      folly::Range<const vector_size_t*> rowNumbersRange =
          folly::Range(rowNumbers.data(), rowNumbersSize);
      container.extractColumn(
          input.data(), rowNumbersRange, column, offset, result);
      verifyResults(result, offset);
    };

    testBasic();
    testRowNumbers();
  }

  void testRepeatedRowNumbers(
      RowContainer& container,
      const std::vector<char*>& rows,
      int column,
      const VectorPtr& expected) {
    auto size = rows.size();
    // Copy the rows so that we rotate over the values at positions 0, 1, 2.
    // This is for testing copying repeated row numbers.
    std::vector<vector_size_t> rowNumbers(size, 0);
    rowNumbers.resize(size);
    for (int i = 0; i < size; i++) {
      rowNumbers[i] = i % 3;
    }
    auto rowNumbersRange = folly::Range(rowNumbers.data(), size);

    auto result = BaseVector::create(expected->type(), size, pool_.get());
    container.extractColumn(rows.data(), rowNumbersRange, column, 0, result);

    EXPECT_EQ(size, result->size());
    for (vector_size_t i = 0; i < size; ++i) {
      EXPECT_TRUE(result->equalValueAt(expected.get(), i, i % 3))
          << "at " << i << ": expected " << expected->toString(i % 3)
          << ", got " << result->toString(i);
    }
  }

  void testNegativeRowNumbers(
      RowContainer& container,
      const std::vector<char*>& rows,
      int column,
      const VectorPtr& expected) {
    auto size = rows.size();
    // Negative row numbers result in nullptr being copied to that position.
    // Alternate between nulls at odd positions and a copy of the values
    // at even positions.
    std::vector<vector_size_t> rowNumbers;
    rowNumbers.resize(size);
    for (int i = 0; i < size; i++) {
      if (i % 2) {
        rowNumbers[i] = -1 * i;
      } else {
        rowNumbers[i] = i;
      }
    }
    auto rowNumbersRange = folly::Range(rowNumbers.data(), size);

    auto result = BaseVector::create(expected->type(), size, pool_.get());
    container.extractColumn(rows.data(), rowNumbersRange, column, 0, result);

    EXPECT_EQ(size, result->size());
    EXPECT_TRUE(result->equalValueAt(expected.get(), 0, 0))
        << "at 0 expected " << expected->toString(0) << ", got "
        << result->toString(0);
    for (vector_size_t i = 1; i < size; ++i) {
      if (i % 2) {
        EXPECT_TRUE(result->isNullAt(i))
            << "at " << i << "expected null, got " << result->toString(i);
      } else {
        EXPECT_TRUE(result->equalValueAt(expected.get(), i, i))
            << "at " << i << ": expected " << expected->toString(i) << ", got "
            << result->toString(i);
      }
    }
  }

  void checkSizes(std::vector<char*>& rows, RowContainer& data) {
    int64_t sum = 0;
    for (auto row : rows) {
      sum += data.rowSize(row) - data.fixedRowSize();
    }
    auto usage = data.stringAllocator().cumulativeBytes();
    EXPECT_EQ(usage, sum);
  }

  // Stores the input vector in Row Container, extracts it and compares. Returns
  // the container.
  std::unique_ptr<RowContainer> roundTrip(const VectorPtr& input) {
    // Create row container.
    std::vector<TypePtr> types{input->type()};

    // Store the vector in the rowContainer.
    auto rowContainer = std::make_unique<RowContainer>(types, mappedMemory_);
    auto size = input->size();
    SelectivityVector allRows(size);
    std::vector<char*> rows(size);
    DecodedVector decoded(*input, allRows);
    for (size_t row = 0; row < size; ++row) {
      rows[row] = rowContainer->newRow();
      rowContainer->store(decoded, row, rows[row], 0);
    }

    testExtractColumn(*rowContainer, rows, 0, input);
    return rowContainer;
  }

  template <typename T>
  void testCompareFloats(TypePtr type, bool ascending, bool nullsFirst) {
    // NOTE: set 'isJoinBuild' to true to enable nullable sort key in test.
    auto rowContainer = makeRowContainer({type}, {type}, false);
    auto lowest = std::numeric_limits<T>::lowest();
    auto min = std::numeric_limits<T>::min();
    auto max = std::numeric_limits<T>::max();
    auto nan = std::numeric_limits<T>::quiet_NaN();
    auto inf = std::numeric_limits<T>::infinity();

    facebook::velox::test::VectorMaker vectorMaker{pool_.get()};
    VectorPtr values = vectorMaker.flatVectorNullable<T>(
        {std::nullopt, max, inf, 0.0, nan, lowest, min});
    int numRows = values->size();

    SelectivityVector allRows(numRows);
    DecodedVector decoded(*values, allRows);
    std::vector<char*> rows(numRows);
    std::vector<std::pair<int, char*>> indexedRows(numRows);
    for (size_t i = 0; i < numRows; ++i) {
      rows[i] = rowContainer->newRow();
      rowContainer->store(decoded, i, rows[i], 0);
      indexedRows[i] = std::make_pair(i, rows[i]);
    }

    std::vector<std::optional<T>> expectedOrder = {
        std::nullopt, lowest, 0.0, min, max, inf, nan};
    if (!ascending) {
      if (nullsFirst) {
        std::reverse(expectedOrder.begin() + 1, expectedOrder.end());
      } else {
        std::reverse(expectedOrder.begin(), expectedOrder.end());
      }
    } else {
      if (!nullsFirst) {
        for (int i = 0; i < expectedOrder.size() - 1; ++i) {
          expectedOrder[i] = expectedOrder[i + 1];
        }
        expectedOrder[expectedOrder.size() - 1] = std::nullopt;
      }
    }
    VectorPtr expected = vectorMaker.flatVectorNullable<T>(expectedOrder);
    {
      // Verify compare method with two rows as input
      std::sort(rows.begin(), rows.end(), [&](const char* l, const char* r) {
        return rowContainer->compare(l, r, 0, {nullsFirst, ascending}) < 0;
      });
      VectorPtr result = BaseVector::create(type, numRows, pool_.get());
      rowContainer->extractColumn(rows.data(), numRows, 0, result);
      assertEqualVectors(expected, result);
    }

    // Verify compare method with row and decoded vector as input
    {
      std::sort(
          indexedRows.begin(),
          indexedRows.end(),
          [&](const std::pair<int, char*>& l, const std::pair<int, char*>& r) {
            return rowContainer->compare(
                       l.second,
                       rowContainer->columnAt(0),
                       decoded,
                       r.first,
                       {nullsFirst, ascending}) < 0;
          });
      std::vector<std::optional<T>> sorted;
      for (const auto& irow : indexedRows) {
        if (decoded.isNullAt(irow.first)) {
          sorted.push_back(std::nullopt);
        } else {
          sorted.push_back(decoded.valueAt<T>(irow.first));
        }
      }
      VectorPtr result = vectorMaker.template flatVectorNullable<T>(sorted);
      assertEqualVectors(expected, result);
    }

    // Verify compareRows method with row as input.
    {
      std::sort(
          indexedRows.begin(),
          indexedRows.end(),
          [&](const std::pair<int, char*>& l, const std::pair<int, char*>& r) {
            return rowContainer->compareRows(
                       l.second, r.second, {{nullsFirst, ascending}}) < 0;
          });
      std::vector<std::optional<T>> sorted;
      for (const auto& irow : indexedRows) {
        if (decoded.isNullAt(irow.first)) {
          sorted.push_back(std::nullopt);
        } else {
          sorted.push_back(decoded.valueAt<T>(irow.first));
        }
      }
      VectorPtr result = vectorMaker.template flatVectorNullable<T>(sorted);
      assertEqualVectors(expected, result);
    }
    // Verify compareRows method with default compare flags.
    if (ascending && nullsFirst) {
      std::sort(
          indexedRows.begin(),
          indexedRows.end(),
          [&](const std::pair<int, char*>& l, const std::pair<int, char*>& r) {
            return rowContainer->compareRows(l.second, r.second) < 0;
          });
      std::vector<std::optional<T>> sorted;
      for (const auto& irow : indexedRows) {
        if (decoded.isNullAt(irow.first)) {
          sorted.push_back(std::nullopt);
        } else {
          sorted.push_back(decoded.valueAt<T>(irow.first));
        }
      }
      VectorPtr result = vectorMaker.template flatVectorNullable<T>(sorted);
      assertEqualVectors(expected, result);
    }
  }
};

namespace {
// Ensures that 'column is non-null by copying a non-null value over nulls.
void makeNonNull(RowVectorPtr batch, int32_t column) {
  auto values = batch->childAt(column);
  std::vector<vector_size_t> nonNulls;
  for (auto i = 0; i < values->size(); ++i) {
    if (!values->isNullAt(i)) {
      nonNulls.push_back(i);
    }
  }
  VELOX_CHECK(!nonNulls.empty(), "Whole column must not be null");
  vector_size_t nonNull = 0;
  for (auto i = 0; i < values->size(); ++i) {
    if (values->isNullAt(i)) {
      values->copy(values.get(), i, nonNulls[nonNull++ % nonNulls.size()], 1);
    }
  }
}

// Makes a string with deterministic distinct content.
void makeLargeString(int32_t approxSize, std::string& string) {
  string.reserve(approxSize);
  while (string.size() < string.capacity() - 20) {
    auto size = string.size();
    auto number =
        fmt::format("{}", (size + 1 + approxSize) * 0xc6a4a7935bd1e995L);
    string.resize(size + number.size());
    memcpy(&string[size], &number[0], number.size());
  }
}
} // namespace

namespace {
static int32_t sign(int32_t n) {
  return n < 0 ? -1 : n == 0 ? 0 : 1;
}
} // namespace

TEST_F(RowContainerTest, storeExtractArrayOfVarchar) {
  // Make a string vector with two rows each having 2 elements.
  // Here it is important, that one of the 1st row's elements has more than 12
  // characters (14 and 13 in this case), since 12 is the number of chars being
  // inlined in the String View.
  facebook::velox::test::VectorMaker vectorMaker{pool_.get()};
  std::vector<std::string> strings{
      "aaaaaaaaaaaaaa", "bbbbbbbbbbbbb", "cccccccc", "ddddd"};
  std::vector<std::vector<StringView>> elements(2);
  elements[0].emplace_back(strings[0]);
  elements[0].emplace_back(strings[1]);
  elements[1].emplace_back(strings[2]);
  elements[1].emplace_back(strings[3]);
  auto input = vectorMaker.arrayVector<StringView>(elements);
  roundTrip(input);
}

TEST_F(RowContainerTest, types) {
  constexpr int32_t kNumRows = 100;
  auto batch = makeDataset(
      ROW(
          {{"bool_val", BOOLEAN()},
           {"tiny_val", TINYINT()},
           {"small_val", SMALLINT()},
           {"int_val", INTEGER()},
           {"long_val", BIGINT()},
           {"float_val", REAL()},
           {"double_val", DOUBLE()},
           {"string_val", VARCHAR()},
           {"array_val", ARRAY(VARCHAR())},
           {"struct_val",
            ROW({{"s_int", INTEGER()}, {"s_array", ARRAY(REAL())}})},
           {"map_val",
            MAP(VARCHAR(),
                MAP(BIGINT(),
                    ROW({{"s2_int", INTEGER()}, {"s2_string", VARCHAR()}})))},

           {"bool_val2", BOOLEAN()},
           {"tiny_val2", TINYINT()},
           {"small_val2", SMALLINT()},
           {"int_val2", INTEGER()},
           {"long_val2", BIGINT()},
           {"float_val2", REAL()},
           {"double_val2", DOUBLE()},
           {"string_val2", VARCHAR()},
           {"array_val2", ARRAY(VARCHAR())},
           {"struct_val2",
            ROW({{"s_int", INTEGER()}, {"s_array", ARRAY(REAL())}})},
           {"map_val2",
            MAP(VARCHAR(),
                MAP(BIGINT(),
                    ROW({{"s2_int", INTEGER()}, {"s2_string", VARCHAR()}})))}}),

      kNumRows,
      [](RowVectorPtr rows) {
        auto strings =
            rows->childAt(
                    rows->type()->as<TypeKind::ROW>().getChildIdx("string_val"))
                ->as<FlatVector<StringView>>();
        for (auto i = 0; i < strings->size(); i += 11) {
          std::string chars;
          makeLargeString(i * 10000, chars);
          strings->set(i, StringView(chars));
        }
      });
  const auto& types = batch->type()->as<TypeKind::ROW>().children();
  std::vector<TypePtr> keys;
  // We have the same types twice, once as non-null keys, next as
  // nullable non-keys.
  keys.insert(keys.begin(), types.begin(), types.begin() + types.size() / 2);
  std::vector<TypePtr> dependents;
  dependents.insert(
      dependents.begin(), types.begin() + types.size() / 2, types.end());
  auto data = makeRowContainer(keys, dependents);

  EXPECT_GT(data->nextOffset(), 0);
  EXPECT_GT(data->probedFlagOffset(), 0);
  for (int i = 0; i < kNumRows; ++i) {
    auto row = data->newRow();
    if (i < kNumRows / 2) {
      RowContainer::normalizedKey(row) = i;
    } else {
      data->disableNormalizedKeys();
    }
  }
  EXPECT_EQ(kNumRows, data->numRows());
  std::vector<char*> rows(kNumRows);
  RowContainerIterator iter;

  EXPECT_EQ(data->listRows(&iter, kNumRows, rows.data()), kNumRows);
  EXPECT_EQ(data->listRows(&iter, kNumRows, rows.data()), 0);

  checkSizes(rows, *data);
  SelectivityVector allRows(kNumRows);
  for (auto column = 0; column < batch->childrenSize(); ++column) {
    if (column < keys.size()) {
      makeNonNull(batch, column);
      EXPECT_EQ(data->columnAt(column).nullMask(), 0);
    } else {
      EXPECT_NE(data->columnAt(column).nullMask(), 0);
    }
    DecodedVector decoded(*batch->childAt(column), allRows);
    for (auto index = 0; index < kNumRows; ++index) {
      data->store(decoded, index, rows[index], column);
    }
  }
  checkSizes(rows, *data);
  data->checkConsistency();
  auto copy = std::static_pointer_cast<RowVector>(
      BaseVector::create(batch->type(), batch->size(), pool_.get()));
  for (auto column = 0; column < batch->childrenSize(); ++column) {
    testExtractColumn(*data, rows, column, batch->childAt(column));

    auto extracted = copy->childAt(column);
    extracted->resize(kNumRows);
    data->extractColumn(rows.data(), rows.size(), column, extracted);
    raw_vector<uint64_t> hashes(kNumRows);
    auto source = batch->childAt(column);
    auto columnType = batch->type()->as<TypeKind::ROW>().childAt(column);
    VectorHasher hasher(columnType, column);
    hasher.decode(*source, allRows);
    hasher.hash(allRows, false, hashes);
    DecodedVector decoded(*extracted, allRows);
    std::vector<uint64_t> rowHashes(kNumRows);
    data->hash(
        column,
        folly::Range<char**>(rows.data(), rows.size()),
        false,
        rowHashes.data());
    for (auto i = 0; i < kNumRows; ++i) {
      if (column) {
        EXPECT_EQ(hashes[i], rowHashes[i]);
      }
      EXPECT_TRUE(source->equalValueAt(extracted.get(), i, i));
      EXPECT_EQ(source->compare(extracted.get(), i, i), 0);
      EXPECT_EQ(source->hashValueAt(i), hashes[i]);
      // Test non-null and nullable variants of equals.
      if (column < keys.size()) {
        EXPECT_TRUE(
            data->equals<false>(rows[i], data->columnAt(column), decoded, i));
      } else {
        EXPECT_TRUE(
            data->equals<true>(rows[i], data->columnAt(column), decoded, i));
      }
      if (i > 0) {
        // We compare the values on consecutive rows
        auto result = sign(source->compare(source.get(), i, i - 1));
        EXPECT_EQ(
            result,
            sign(data->compare(
                rows[i], data->columnAt(column), decoded, i - 1)));
        EXPECT_EQ(result, sign(data->compare(rows[i], rows[i - 1], column)));
      }
    }
  }
  // We check that there is unused space in rows and variable length
  // data.
  auto free = data->freeSpace();
  EXPECT_LT(0, free.first);
  EXPECT_LT(0, free.second);
}

TEST_F(RowContainerTest, erase) {
  constexpr int32_t kNumRows = 100;
  auto data = makeRowContainer({SMALLINT()}, {SMALLINT()});

  // The layout is expected to be smallint - 6 bytes of padding - 1 byte of bits
  // - smallint - next pointer. The bits are a null flag for the second
  // smallint, a probed flag and a free flag.
  EXPECT_EQ(data->nextOffset(), 11);
  // 2nd bit in first byte of flags.
  EXPECT_EQ(data->probedFlagOffset(), 8 * 8 + 1);
  std::unordered_set<char*> rowSet;
  std::vector<char*> rows;
  for (int i = 0; i < kNumRows; ++i) {
    rows.push_back(data->newRow());
    rowSet.insert(rows.back());
  }
  EXPECT_EQ(kNumRows, data->numRows());
  std::vector<char*> rowsFromContainer(kNumRows);
  RowContainerIterator iter;
  EXPECT_EQ(
      data->listRows(&iter, kNumRows, rowsFromContainer.data()), kNumRows);
  EXPECT_EQ(0, data->listRows(&iter, kNumRows, rows.data()));
  EXPECT_EQ(rows, rowsFromContainer);

  std::vector<char*> erased;
  for (auto i = 0; i < rows.size(); i += 2) {
    erased.push_back(rows[i]);
  }
  data->eraseRows(folly::Range<char**>(erased.data(), erased.size()));
  data->checkConsistency();

  std::vector<char*> remaining(data->numRows());
  iter.reset();
  EXPECT_EQ(
      remaining.size(),
      data->listRows(&iter, data->numRows(), remaining.data()));
  // We check that the next new rows reuse the erased rows.
  for (auto i = 0; i < erased.size(); ++i) {
    auto reused = data->newRow();
    EXPECT_NE(rowSet.end(), rowSet.find(reused));
  }
  // The next row will be a new one.
  auto newRow = data->newRow();
  EXPECT_EQ(rowSet.end(), rowSet.find(newRow));
  data->checkConsistency();
  data->clear();
  EXPECT_EQ(0, data->numRows());
  auto free = data->freeSpace();
  EXPECT_EQ(0, free.first);
  EXPECT_EQ(0, free.second);
  data->checkConsistency();
}

TEST_F(RowContainerTest, initialNulls) {
  std::vector<TypePtr> keys{INTEGER()};
  std::vector<TypePtr> dependent{INTEGER()};
  // Join build.
  auto data = makeRowContainer(keys, dependent, true);
  auto row = data->newRow();
  auto isNullAt = [](const RowContainer& data, const char* row, int32_t i) {
    auto column = data.columnAt(i);
    return RowContainer::isNullAt(row, column.nullByte(), column.nullMask());
  };

  EXPECT_FALSE(isNullAt(*data, row, 0));
  EXPECT_FALSE(isNullAt(*data, row, 1));
  // Non-join build.
  data = makeRowContainer(keys, dependent, false);
  row = data->newRow();
  EXPECT_FALSE(isNullAt(*data, row, 0));
  EXPECT_FALSE(isNullAt(*data, row, 1));
}

TEST_F(RowContainerTest, rowSize) {
  constexpr int32_t kNumRows = 100;
  auto data = makeRowContainer({SMALLINT()}, {VARCHAR()});

  // The layout is expected to be smallint - 6 bytes of padding - 1 byte of bits
  // - StringView - rowSize - next pointer. The bits are a null flag for the
  // second smallint, a probed flag and a free flag.
  EXPECT_EQ(25, data->rowSizeOffset());
  EXPECT_EQ(29, data->nextOffset());
  // 2nd bit in first byte of flags.
  EXPECT_EQ(data->probedFlagOffset(), 8 * 8 + 1);
  std::unordered_set<char*> rowSet;
  std::vector<char*> rows;
  for (int i = 0; i < kNumRows; ++i) {
    rows.push_back(data->newRow());
    rowSet.insert(rows.back());
  }
  EXPECT_EQ(kNumRows, data->numRows());
  for (auto i = 0; i < rows.size(); ++i) {
    data->incrementRowSize(rows[i], i * 1000);
  }
  // The first size overflows 32 bits.
  data->incrementRowSize(rows[0], 2000000000);
  data->incrementRowSize(rows[0], 2000000000);
  data->incrementRowSize(rows[0], 2000000000);
  std::vector<char*> rowsFromContainer(kNumRows);
  RowContainerIterator iter;
  // The first row is returned alone.
  auto numRows =
      data->listRows(&iter, kNumRows, 100000, rowsFromContainer.data());
  EXPECT_EQ(1, numRows);
  while (numRows < kNumRows) {
    numRows += data->listRows(
        &iter, kNumRows, 100000, rowsFromContainer.data() + numRows);
  }
  EXPECT_EQ(0, data->listRows(&iter, kNumRows, rows.data()));

  EXPECT_EQ(rows, rowsFromContainer);
}

// Verify comparison of fringe float valuesg
TEST_F(RowContainerTest, compareFloat) {
  // Verify ascending order
  testCompareFloats<float>(REAL(), true, true);
  testCompareFloats<float>(REAL(), true, false);
  //  Verify descending order
  testCompareFloats<float>(REAL(), false, true);
  testCompareFloats<float>(REAL(), false, false);
}

// Verify comparison of fringe double values
TEST_F(RowContainerTest, compareDouble) {
  // Verify ascending order
  testCompareFloats<double>(DOUBLE(), true, true);
  testCompareFloats<double>(DOUBLE(), true, false);
  // Verify descending order
  testCompareFloats<double>(DOUBLE(), false, true);
  testCompareFloats<double>(DOUBLE(), false, false);
}

TEST_F(RowContainerTest, partition) {
  // We assign an arbitrary partition number to each row and iterate
  // over the rows a partition at a time.
  constexpr int32_t kNumRows = 100019;
  constexpr uint8_t kNumPartitions = 16;
  auto batch = makeDataset(
      ROW(
          {{"int_val", INTEGER()},
           {"long_val", BIGINT()},
           {"string_val", VARCHAR()}}),
      kNumRows,
      [](RowVectorPtr /*rows*/) {});

  auto data = roundTrip(batch);
  std::vector<char*> rows(kNumRows);
  RowContainerIterator iter;
  data->listRows(&iter, kNumRows, RowContainer::kUnlimited, rows.data());

  // Test random skipping of RowContainerIterator.
  for (auto count = 0; count < 100; ++count) {
    auto index = (count * 121) % kNumRows;
    iter.reset();
    data->skip(iter, index);
    EXPECT_EQ(iter.currentRow(), rows[index]);
    if (index + count < kNumRows) {
      data->skip(iter, count);
      EXPECT_EQ(iter.currentRow(), rows[index + count]);
    }
  }

  auto& partitions = data->partitions();
  std::vector<uint8_t> rowPartitions(kNumRows);
  // Assign a partition to each row based on  modulo of first column.
  std::vector<std::vector<char*>> partitionRows(kNumPartitions);
  auto column = batch->childAt(0)->as<FlatVector<int32_t>>();
  for (auto i = 0; i < kNumRows; ++i) {
    uint8_t partition =
        static_cast<uint32_t>(column->valueAt(i)) % kNumPartitions;
    rowPartitions[i] = partition;
    partitionRows[partition].push_back(rows[i]);
  }
  partitions.appendPartitions(
      folly::Range<const uint8_t*>(rowPartitions.data(), kNumRows));
  for (auto partition = 0; partition < kNumPartitions; ++partition) {
    std::vector<char*> result(partitionRows[partition].size() + 10);
    iter.reset();
    int32_t numFound = 0;
    int32_t resultBatch = 1;
    // Read the rows in multiple batches.
    while (auto numResults = data->listPartitionRows(
               iter, partition, resultBatch, result.data() + numFound)) {
      numFound += numResults;
      resultBatch += 13;
    }
    EXPECT_EQ(numFound, partitionRows[partition].size());
    result.resize(numFound);
    EXPECT_EQ(partitionRows[partition], result);
  }
}
