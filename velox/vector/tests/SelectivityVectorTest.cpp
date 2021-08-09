/*
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

#include "velox/vector/SelectivityVector.h"

#include <gtest/gtest.h>

namespace facebook {
namespace velox {
namespace test {
namespace {

void assertState(
    const std::vector<bool>& expected,
    const SelectivityVector& selectivityVector,
    bool testProperties = true) {
  ASSERT_EQ(expected.size(), selectivityVector.size());

  size_t startIndexIncl = 0;
  size_t endIndexExcl = 0;

  size_t count = 0;

  for (size_t i = 0; i < expected.size(); ++i) {
    ASSERT_EQ(expected.at(i), selectivityVector.isValid(i))
        << "Mismatch at index " << i
        << ", selectivityVector: " << selectivityVector
        << ", expected: " << ::testing::PrintToString(expected);
    if (expected.at(i)) {
      endIndexExcl = i + 1;
      if (count == 0) {
        startIndexIncl = i;
      }

      ++count;
    }
  }

  if (testProperties) {
    ASSERT_EQ(startIndexIncl, selectivityVector.begin()) << selectivityVector;
    ASSERT_EQ(endIndexExcl, selectivityVector.end()) << selectivityVector;
    ASSERT_EQ(startIndexIncl < endIndexExcl, selectivityVector.hasSelections())
        << selectivityVector;
    // countSelected relies on startIndexIncl and endIndexExcl being correct
    // hence why it's inside this if block.
    ASSERT_EQ(count, selectivityVector.countSelected());
  }
}

void setValid_normal(bool setToValue) {
  // A little bit more than 2 simd widths, so overflow.
  const size_t vectorSize = 513;

  // We'll set everything to the opposite of the setToValue
  std::vector<bool> expected(vectorSize, !setToValue);
  SelectivityVector vector(vectorSize);
  if (setToValue) {
    vector.clearAll();
  }

  auto setAndAssert = [&](size_t index, bool value) {
    vector.setValid(index, value);
    expected[index] = value;
    ASSERT_NO_FATAL_FAILURE(
        assertState(expected, vector, false /*testProperties*/))
        << "Setting to " << value << " at " << index;
  };

  for (size_t i = 0; i < expected.size(); ++i) {
    ASSERT_NO_FATAL_FAILURE(setAndAssert(i, setToValue));
    ASSERT_NO_FATAL_FAILURE(setAndAssert(i, !setToValue));
  }
}

}; // namespace

TEST(SelectivityVectorTest, setValid_true_normal) {
  setValid_normal(true);
}

TEST(SelectivityVectorTest, setValid_false_normal) {
  setValid_normal(false);
}

TEST(SelectivityVectorTest, basicOperation) {
  const size_t vectorSize = 10;
  SelectivityVector vector(vectorSize);
  std::vector<bool> expected(vectorSize, true);

  ASSERT_NO_FATAL_FAILURE(assertState(expected, vector)) << "Starting state";

  vector.clearAll();
  expected = std::vector<bool>(vectorSize, false);
  ASSERT_NO_FATAL_FAILURE(assertState(expected, vector)) << "clearAll() called";

  vector.setAll();
  expected = std::vector<bool>(vectorSize, true);
  ASSERT_NO_FATAL_FAILURE(assertState(expected, vector)) << "setAll() called";
}

TEST(SelectivityVectorTest, updateBounds) {
  const size_t vectorSize = 10;
  SelectivityVector vector(vectorSize);
  std::vector<bool> expected(vectorSize, true);

  auto setAndAssert = [&](size_t index, bool value) {
    vector.setValid(index, value);
    vector.updateBounds();

    expected[index] = value;

    assertState(expected, vector);
  };

  ASSERT_NO_FATAL_FAILURE(setAndAssert(9, false));
  ASSERT_NO_FATAL_FAILURE(setAndAssert(0, false));
  ASSERT_NO_FATAL_FAILURE(setAndAssert(9, true));
  ASSERT_NO_FATAL_FAILURE(setAndAssert(0, true));
}

TEST(SelectivityVectorTest, merge) {
  const size_t vectorSize = 10;
  SelectivityVector vector(vectorSize);
  SelectivityVector other(vectorSize);

  other.setValid(5, false);
  other.updateBounds();
  vector.intersect(other);

  std::vector<bool> expected(vectorSize, true);
  expected[5] = false;

  ASSERT_NO_FATAL_FAILURE(assertState(expected, vector));
}

TEST(SelectivityVectorTest, deselect) {
  const size_t vectorSize = 10;
  SelectivityVector vector(vectorSize);
  SelectivityVector other(vectorSize);

  other.clearAll();
  other.setValid(5, true);
  other.updateBounds();
  vector.deselect(other);

  std::vector<bool> expected(vectorSize, true);
  expected[5] = false;

  ASSERT_NO_FATAL_FAILURE(assertState(expected, vector));
}

TEST(SelectivityVectorTest, deselectTwice) {
  const size_t vectorSize = 1;
  SelectivityVector vector(vectorSize);
  SelectivityVector other(vectorSize);

  vector.deselect(other);
  vector.deselect(other);

  std::vector<bool> expected(vectorSize, false);

  ASSERT_NO_FATAL_FAILURE(assertState(expected, vector));
}

TEST(SelectivityVectorTest, setValidRange) {
  const size_t vectorSize = 1000;
  SelectivityVector vector(vectorSize);
  std::vector<bool> expected(vectorSize, true);

  auto setRangeAndAssert =
      [&](size_t startIdxInclusive, size_t endIdxExclusive, bool valid) {
        vector.setValidRange(startIdxInclusive, endIdxExclusive, valid);
        vector.updateBounds();
        std::fill(
            expected.begin() + startIdxInclusive,
            expected.begin() + endIdxExclusive,
            valid);
        assertState(expected, vector);
      };

  ASSERT_NO_FATAL_FAILURE(setRangeAndAssert(5, 800, false)) << "set to false";

  ASSERT_NO_FATAL_FAILURE(setRangeAndAssert(100, 300, true))
      << "set inner range to true";

  ASSERT_NO_FATAL_FAILURE(setRangeAndAssert(0, 200, false)) << "start to false";

  ASSERT_NO_FATAL_FAILURE(setRangeAndAssert(900, vectorSize, false))
      << "end to false";

  ASSERT_NO_FATAL_FAILURE(setRangeAndAssert(0, vectorSize, false))
      << "all to false";
}

TEST(SelectivityVectorTest, setStartEndIdx) {
  SelectivityVector vector(1000 /*vectorSize*/);

  vector.setActiveRange(55, 100);

  EXPECT_EQ(55, vector.begin());
  EXPECT_EQ(100, vector.end());
}

TEST(SelectivityVectorTest, clearAll) {
  auto size = 3;
  // Explicitly set all bits to false.
  // Will call clearAll
  SelectivityVector vector(size, false);

  // Build another vector and set all bits to 0 in brute-force way
  SelectivityVector expected(size);

  for (auto i = 0; i < size; ++i) {
    expected.setValid(i, false);
  }
  expected.updateBounds();

  EXPECT_EQ(expected, vector);
}

TEST(SelectivityVectorTest, setAll) {
  auto size = 3;
  // Initialize with all bits to false
  // Will call clearAll
  SelectivityVector vector(size, false);
  vector.setAll();

  // Build another vector and set all bits to 1 in brute-force way
  SelectivityVector expected(size, true);

  EXPECT_EQ(expected, vector);
}

namespace {

void testEquals(
    bool expectEqual,
    std::function<void(SelectivityVector&)> mungeFunc = nullptr) {
  const size_t vectorSize = 1000;
  SelectivityVector vector1(vectorSize);
  SelectivityVector vector2(vectorSize);

  if (mungeFunc != nullptr) {
    mungeFunc(vector2);
  }

  ASSERT_EQ(expectEqual, vector1 == vector2);
  ASSERT_NE(expectEqual, vector1 != vector2);
}

} // namespace

TEST(SelectivityVectorTest, operatorEquals_allEqual) {
  testEquals(true /*expectEqual*/);
}

TEST(SelectivityVectorTest, operatorEquals_dataNotEqual) {
  testEquals(
      false /*expectEqual*/, [](auto& vector) { vector.setValid(10, false); });
}

TEST(SelectivityVectorTest, operatorEquals_startIdxNotEqual) {
  testEquals(false /*expectEqual*/, [](auto& vector) {
    vector.setActiveRange(10, vector.end());
  });
}

TEST(SelectivityVectorTest, operatorEquals_endIdxNotEqual) {
  testEquals(false /*expectEqual*/, [](auto& vector) {
    vector.setActiveRange(vector.begin(), 10);
  });
}

TEST(SelectivityVectorTest, emptyIterator) {
  SelectivityVector vector(2011);
  vector.clearAll();
  SelectivityIterator empty(vector);
  vector_size_t row = 0;
  EXPECT_FALSE(empty.next(row));
  int32_t count = 0;
  vector.applyToSelected([&count](vector_size_t /* unused */) {
    ++count;
    return true;
  });
  EXPECT_EQ(count, 0);
}

TEST(SelectivityVectorTest, iterator) {
  SelectivityVector vector(2011);
  vector.clearAll();
  std::vector<int32_t> selected;
  vector_size_t row = 0;
  for (int32_t i = 0; i < 1000; ++i) {
    selected.push_back(row);
    vector.setValid(row, true);
    row += (i & 7) | 1;
    if (row > 1500) {
      row += 64;
    }
    if (row > vector.size()) {
      break;
    }
  }
  vector.updateBounds();
  int32_t count = 0;
  SelectivityIterator iter(vector);
  while (iter.next(row)) {
    ASSERT_EQ(row, selected[count]);
    count++;
  }
  ASSERT_EQ(count, selected.size());
  count = 0;
  vector.applyToSelected([selected, &count](int32_t row) {
    EXPECT_EQ(row, selected[count]);
    count++;
    return true;
  });
  ASSERT_EQ(count, selected.size());

  std::vector<uint64_t> contiguous(10);
  bits::fillBits(&contiguous[0], 67, 227, true);
  // Set some trailing bits that are after the range.
  bits::fillBits(&contiguous[0], 240, 540, true);
  SelectivityVector fromBits;
  fromBits.setFromBits(&contiguous[0], 64 * contiguous.size());
  fromBits.setActiveRange(fromBits.begin(), 227);
  EXPECT_EQ(fromBits.begin(), 67);
  EXPECT_EQ(fromBits.end(), 227);
  EXPECT_FALSE(fromBits.isAllSelected());
  count = 0;
  fromBits.applyToSelected([&count](int32_t row) {
    EXPECT_EQ(row, count + 67);
    count++;
    return true;
  });
  EXPECT_EQ(count, bits::countBits(&contiguous[0], 0, 240));
  fromBits.setActiveRange(64, 227);
  EXPECT_FALSE(fromBits.isAllSelected());
  count = 0;
  fromBits.applyToSelected([&count](int32_t row) {
    EXPECT_EQ(row, count + 67);
    count++;
    return true;
  });
  EXPECT_EQ(count, bits::countBits(&contiguous[0], 0, 240));
  count = 0;
  SelectivityIterator iter2(fromBits);
  while (iter2.next(row)) {
    EXPECT_EQ(row, count + 67);
    count++;
  }
  EXPECT_EQ(count, bits::countBits(&contiguous[0], 0, 240));
}

} // namespace test
} // namespace velox
} // namespace facebook
