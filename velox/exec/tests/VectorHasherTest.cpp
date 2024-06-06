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
#include "velox/exec/VectorHasher.h"
#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

class VectorHasherTest : public testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    allRows_ = SelectivityVector(100);

    oddRows_ = VectorHasherTest::makeOddRows(100);
  }

  static SelectivityVector makeOddRows(vector_size_t size) {
    SelectivityVector oddRows(size);
    for (int32_t i = 0; i < size; i += 2) {
      oddRows.setValid(i, false);
    }
    oddRows.updateBounds();
    return oddRows;
  }

  template <typename T>
  void testComputeValueIds(bool withNulls) {
    vector_size_t size = 1'111;
    auto isNullAt = withNulls ? test::VectorMaker::nullEvery(5) : nullptr;

    // values in the middle of the range
    auto vector = makeFlatVector<T>(
        size, [](vector_size_t row) { return row % 17; }, isNullAt);
    auto outOfRangeVector = makeFlatVector<T>(
        size, [](vector_size_t row) { return row % 19; }, isNullAt);
    testComputeValueIds(vector, outOfRangeVector);
    testComputeValueIds(vector, outOfRangeVector, 27);

    // values at the lower end of the range
    vector = makeFlatVector<T>(
        size,
        [](vector_size_t row) {
          return std::numeric_limits<T>::min() + row % 17;
        },
        isNullAt);
    testComputeValueIds(vector);

    // values at the upper end of the range
    vector = makeFlatVector<T>(
        size,
        [](vector_size_t row) {
          return std::numeric_limits<T>::max() - 16 + row % 17;
        },
        isNullAt);
    testComputeValueIds(vector);
  }

  void testComputeValueIds(
      const VectorPtr& vector,
      const VectorPtr& outOfRangeVector = nullptr,
      uint64_t multiplier = 1) {
    auto size = vector->size();

    SelectivityVector allRows(size);
    auto hasher = exec::VectorHasher::create(vector->type(), 0);
    raw_vector<uint64_t> result(size);
    std::fill(result.begin(), result.end(), 0);
    hasher->decode(*vector, allRows);
    auto ok = hasher->computeValueIds(allRows, result);
    ASSERT_FALSE(ok);

    uint64_t asRange;
    uint64_t asDistinct;
    hasher->cardinality(0, asRange, asDistinct);
    ASSERT_EQ(18, asRange);
    ASSERT_EQ(18, asDistinct);

    auto rangeSize = hasher->enableValueRange(multiplier, 0);
    ASSERT_EQ(18 * multiplier, rangeSize);

    hasher->decode(*vector, allRows);
    ok = hasher->computeValueIds(allRows, result);
    ASSERT_TRUE(ok);
    for (auto i = 0; i < size; i++) {
      if (vector->isNullAt(i)) {
        ASSERT_EQ(0, result[i]) << "at " << i;
      } else {
        ASSERT_EQ((i % 17 + 1) * multiplier, result[i]) << "at " << i;
      }
    }

    auto oddRows = makeOddRows(size);
    memset(result.data(), 0, sizeof(uint64_t) * size);
    hasher->decode(*vector, oddRows);
    ok = hasher->computeValueIds(oddRows, result);
    ASSERT_TRUE(ok);
    for (auto i = 0; i < size; i++) {
      if (i % 2 == 0 || vector->isNullAt(i)) {
        ASSERT_EQ(0, result[i]) << "at " << i;
      } else {
        ASSERT_EQ((i % 17 + 1) * multiplier, result[i]) << "at " << i;
      }
    }

    if (outOfRangeVector) {
      hasher->decode(*outOfRangeVector, allRows);
      ok = hasher->computeValueIds(allRows, result);
      ASSERT_FALSE(ok);

      hasher->cardinality(0, asRange, asDistinct);
      ASSERT_GT(asRange, 18);
      ASSERT_GT(asDistinct, 18);
    }
  }

  BufferPtr makeIndices(
      vector_size_t size,
      std::function<vector_size_t(vector_size_t)> indexAt) {
    BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(size, pool());
    auto rawIndices = indices->asMutable<vector_size_t>();
    for (auto i = 0; i < size; i++) {
      rawIndices[i] = indexAt(i);
    }
    return indices;
  }

  VectorPtr makeDictionary(vector_size_t size, const VectorPtr& base) {
    auto baseSize = base->size();
    return BaseVector::wrapInDictionary(
        BufferPtr(nullptr),
        makeIndices(
            size, [baseSize](vector_size_t row) { return row % baseSize; }),
        size,
        base);
  }

  SelectivityVector allRows_;
  SelectivityVector oddRows_;
};

TEST_F(VectorHasherTest, flat) {
  auto hasher = exec::VectorHasher::create(BIGINT(), 1);
  ASSERT_EQ(hasher->channel(), 1);
  ASSERT_EQ(hasher->typeKind(), TypeKind::BIGINT);

  auto vector = BaseVector::create(BIGINT(), 100, pool());
  auto flatVector = vector->asFlatVector<int64_t>();
  for (int32_t i = 0; i < 100; i++) {
    if (i % 5 == 0) {
      flatVector->setNull(i, true);
    } else {
      flatVector->set(i, i);
    }
  }

  raw_vector<uint64_t> hashes(100);
  std::fill(hashes.begin(), hashes.end(), 0);
  hasher->decode(*vector, oddRows_);
  hasher->hash(oddRows_, false, hashes);
  for (int32_t i = 0; i < 100; i++) {
    if (i % 2 == 0) {
      EXPECT_EQ(hashes[i], 0);
    } else if (i % 5 == 0) {
      EXPECT_EQ(hashes[i], exec::VectorHasher::kNullHash) << "at " << i;
    } else {
      EXPECT_EQ(hashes[i], folly::hasher<int64_t>()(i)) << "at " << i;
    }
  }

  hasher->decode(*vector, allRows_);
  hasher->hash(allRows_, false, hashes);
  for (int32_t i = 0; i < 100; i++) {
    if (i % 5 == 0) {
      EXPECT_EQ(hashes[i], exec::VectorHasher::kNullHash) << "at " << i;
    } else {
      EXPECT_EQ(hashes[i], folly::hasher<int64_t>()(i)) << "at " << i;
    }
  }

  // Test precompute methods for single null value.
  hasher->precompute(*vector);
  hasher->hashPrecomputed(allRows_, false, hashes);
  for (int32_t i = 0; i < 100; i++) {
    EXPECT_EQ(exec::VectorHasher::kNullHash, hashes[i]);
  }

  // Test precompute methods for single value '7'.
  flatVector->set(0, 7);
  hasher->precompute(*vector);
  hasher->hashPrecomputed(allRows_, false, hashes);
  auto expected = folly::hasher<int64_t>()(7);
  for (int32_t i = 0; i < 100; i++) {
    EXPECT_EQ(expected, hashes[i]);
  }

  // Test precompute methods for mixed value '7' with value '55'.
  flatVector->set(0, 55);
  hasher->precompute(*vector);
  hasher->hashPrecomputed(allRows_, true, hashes);
  expected = bits::hashMix(expected, folly::hasher<int64_t>()(55));
  for (int32_t i = 0; i < 100; i++) {
    EXPECT_EQ(expected, hashes[i]);
  }
}

TEST_F(VectorHasherTest, nans) {
  // Sanity check to ensure the NaNs are correctly hashed, that is, all NaNs are
  // considered equal and therefore should have the same hash.
  static const auto kNaN = std::numeric_limits<double>::quiet_NaN();
  static const auto kSNaN = std::numeric_limits<double>::signaling_NaN();
  folly::hasher<double> hasher;
  static const auto kNaNHash = hasher(kNaN);
  auto vectorHasher = exec::VectorHasher::create(DOUBLE(), 1);
  ASSERT_EQ(vectorHasher->channel(), 1);
  ASSERT_EQ(vectorHasher->typeKind(), TypeKind::DOUBLE);

  // Using two different binary representations of NaN.
  std::vector<double> inputValues = {1.0, -1.0, kNaN, kSNaN, 0.0, -0.0};

  auto vector = BaseVector::create(DOUBLE(), inputValues.size(), pool());
  auto flatVector = vector->asFlatVector<double>();
  for (int32_t i = 0; i < inputValues.size(); i++) {
    flatVector->set(i, inputValues[i]);
  }

  raw_vector<uint64_t> hashes(inputValues.size());
  std::fill(hashes.begin(), hashes.end(), 0);
  SelectivityVector allRows = SelectivityVector(inputValues.size());
  vectorHasher->decode(*vector, allRows);
  vectorHasher->hash(allRows, false, hashes);
  std::vector<uint64_t> expected = {
      hasher(1.0), hasher(-1.0), kNaNHash, kNaNHash, hasher(0.0), hasher(0.0)};
  for (int32_t i = 0; i < inputValues.size(); i++) {
    EXPECT_EQ(hashes[i], expected[i]) << "at " << i;
  }
}

TEST_F(VectorHasherTest, nonNullConstant) {
  auto hasher = exec::VectorHasher::create(INTEGER(), 1);
  auto vector = BaseVector::createConstant(INTEGER(), 123, 100, pool());

  auto hash = folly::hasher<int32_t>()(123);

  raw_vector<uint64_t> hashes(100);
  std::fill(hashes.begin(), hashes.end(), 0);
  hasher->decode(*vector, oddRows_);
  hasher->hash(oddRows_, false, hashes);
  for (int32_t i = 0; i < 100; i++) {
    EXPECT_EQ(hashes[i], (i % 2 == 0) ? 0 : hash) << "at " << i;
  }

  hasher->decode(*vector, allRows_);
  hasher->hash(allRows_, false, hashes);
  for (int32_t i = 0; i < 100; i++) {
    EXPECT_EQ(hashes[i], hash) << "at " << i;
  }
}

TEST_F(VectorHasherTest, nullConstant) {
  auto hasher = exec::VectorHasher::create(INTEGER(), 1);
  auto vector = BaseVector::createNullConstant(INTEGER(), 100, pool());

  raw_vector<uint64_t> hashes(100);
  std::fill(hashes.begin(), hashes.end(), 0);
  hasher->decode(*vector, oddRows_);
  hasher->hash(oddRows_, false, hashes);
  for (int32_t i = 0; i < 100; i++) {
    EXPECT_EQ(hashes[i], (i % 2 == 0) ? 0 : exec::VectorHasher::kNullHash)
        << "at " << i;
  }

  hasher->decode(*vector, allRows_);
  hasher->hash(allRows_, false, hashes);
  for (int32_t i = 0; i < 100; i++) {
    EXPECT_EQ(hashes[i], exec::VectorHasher::kNullHash) << "at " << i;
  }
}

TEST_F(VectorHasherTest, unknown) {
  auto hasher = exec::VectorHasher::create(UNKNOWN(), 1);
  auto vector = makeAllNullFlatVector<UnknownValue>(100);

  // Test hashing without mixing.
  raw_vector<uint64_t> hashes(100);
  std::fill(hashes.begin(), hashes.end(), 0);
  hasher->decode(*vector, oddRows_);
  hasher->hash(oddRows_, false, hashes);
  for (int32_t i = 0; i < 100; i++) {
    EXPECT_EQ(hashes[i], (i % 2 == 0) ? 0 : exec::VectorHasher::kNullHash)
        << "at " << i;
  }

  hasher->decode(*vector, allRows_);
  hasher->hash(allRows_, false, hashes);
  for (int32_t i = 0; i < 100; i++) {
    EXPECT_EQ(hashes[i], exec::VectorHasher::kNullHash) << "at " << i;
  }

  // Test mixing.
  std::iota(hashes.begin(), hashes.end(), 0);
  hasher->hash(allRows_, true, hashes);
  for (int32_t i = 0; i < 100; i++) {
    auto expected = bits::hashMix(i, exec::VectorHasher::kNullHash);
    EXPECT_EQ(hashes[i], expected) << "at " << i;
  }
}

TEST_F(VectorHasherTest, dictionary) {
  auto hasher = exec::VectorHasher::create(BIGINT(), 1);

  // 10 consecutive values: 3, 4, 5..12
  auto vector = BaseVector::create(BIGINT(), 100, pool());
  auto flatVector = vector->asFlatVector<int64_t>();
  for (int32_t i = 0; i < 10; i++) {
    flatVector->set(i, i + 3);
  }

  // above sequence repeated 10 times: 3, 4, 5..12, 3, 4, 5..12,..
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(100, pool());
  auto indicesPtr = indices->asMutable<vector_size_t>();
  for (int32_t i = 0; i < 100; i++) {
    indicesPtr[i] = i % 10;
  }
  auto dictionaryVector =
      BaseVector::wrapInDictionary(BufferPtr(nullptr), indices, 100, vector);

  raw_vector<uint64_t> hashes(100);
  std::fill(hashes.begin(), hashes.end(), 0);
  hasher->decode(*dictionaryVector, oddRows_);
  hasher->hash(oddRows_, false, hashes);
  for (int32_t i = 0; i < 100; i++) {
    if (i % 2 == 0) {
      EXPECT_EQ(hashes[i], 0) << "at " << i;
    } else {
      EXPECT_EQ(hashes[i], folly::hasher<int64_t>()(i % 10 + 3)) << "at " << i;
    }
  }

  hasher->decode(*dictionaryVector, allRows_);
  hasher->hash(allRows_, false, hashes);
  for (int32_t i = 0; i < 100; i++) {
    EXPECT_EQ(hashes[i], folly::hasher<int64_t>()(i % 10 + 3)) << "at " << i;
  }
}

// Tests how strings are mapped to uint64_t (if they fit) and to
// consecutive ids of distinct values for the general case.
TEST_F(VectorHasherTest, stringIds) {
  auto hasher = exec::VectorHasher::create(VARCHAR(), 1);
  auto vector = BaseVector::create(VARCHAR(), 100, pool());
  auto flatVector = vector->asFlatVector<StringView>();
  char zeros[9] = {};
  char digits[10] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
  SelectivityVector rows(sizeof(zeros) + sizeof(digits));
  flatVector->resize(sizeof(zeros) + sizeof(digits));
  raw_vector<uint64_t> hashes(flatVector->size());
  for (int i = 0; i < sizeof(zeros); ++i) {
    flatVector->set(i, StringView(zeros, i));
    if (i > 7) {
      rows.setValid(i, false);
    }
  }
  for (int i = 0; i < sizeof(digits); ++i) {
    flatVector->set(i + sizeof(zeros), StringView(digits, i));
    if (i > 7) {
      rows.setValid(i + sizeof(zeros), false);
    }
  }
  rows.updateBounds();
  // The test data has strings of 0 - 9 zeros followed by strings of
  // 0-10 digits. The elements where size <= 7 are in 'rows'. These
  // values have an integer range mapping.  We run these elements
  // through the hasher.
  hasher->decode(*vector, rows);
  EXPECT_FALSE(hasher->computeValueIds(rows, hashes));
  uint64_t asRange;
  uint64_t asDistincts;
  // Get the range of ids as min-max range and as count of distincts.
  hasher->cardinality(0, asRange, asDistincts);
  // All fit in range representation since no longer than 7 bytes.
  EXPECT_NE(asRange, exec::VectorHasher::kRangeTooLarge);
  // 15 values + 1 for null. The 0-length string is in common between zeros and
  // digits.
  EXPECT_EQ(asDistincts, 16);
  hasher->enableValueIds(1, 10);
  hasher->decode(*vector, rows);
  EXPECT_TRUE(hasher->computeValueIds(rows, hashes));
  // The 8 first have sequential ids.
  for (auto i = 0; i < 8; ++i) {
    EXPECT_EQ(hashes[i], i + 1);
  }
  hasher->enableValueRange(1, 200);
  hasher->decode(*vector, rows);
  EXPECT_TRUE(hasher->computeValueIds(rows, hashes));
  for (auto i = 0; i < 8; ++i) {
    // Since the range is padded with 100 values above and below, all
    // the values should be >= 101 (0 stands for null).
    EXPECT_GE(hashes[i], 101);
  }

  // Now we process values that do not fit.
  rows.clearAll();
  auto numInRange = 0;
  for (int i = 0; i < flatVector->size(); ++i) {
    if (flatVector->valueAt(i).size() > 7) {
      rows.setValid(i, true);
    } else {
      ++numInRange;
    }
  }
  rows.updateBounds();
  VectorHasher::ScratchMemory scratchMemory;
  hasher->lookupValueIds(*vector, rows, scratchMemory, hashes);
  // Since none of the values fit the range, all bits should be clear in rows.
  EXPECT_EQ(rows.countSelected(), 0);
  // We expect a multiplier of 1 * (15 distinct values + 1 for null + 50% for
  // reserve).
  EXPECT_EQ(23, hasher->enableValueIds(1, 50));
  rows.setValidRange(0, sizeof(zeros) + sizeof(digits), true);
  rows.updateBounds();
  // We get ids for values that have been seen, we expect found for all in
  // range.
  hasher->lookupValueIds(*vector, rows, scratchMemory, hashes);
  EXPECT_EQ(numInRange, rows.countSelected());
}

// Tests distinct overflow, but starting with a small string
TEST_F(VectorHasherTest, stringDistinctOverflow) {
  auto hasher = exec::VectorHasher::create(VARCHAR(), 1);

  constexpr uint32_t numRows{10000};

  // 7 vectors, 10000 rows each.
  // The 1st row of every batch has a small string.
  std::vector<FlatVectorPtr<StringView>> batches;
  std::vector<std::vector<std::string>> strings;
  strings.resize(7);
  for (auto i = 0; i < 7; ++i) {
    auto& stringVec = strings[i];
    stringVec.resize(numRows);
    batches.emplace_back(makeFlatVector<StringView>(
        numRows, [&i, &stringVec, numRows](vector_size_t row) {
          const auto num = numRows * i + row;
          stringVec[row] = (row != 0)
              ? fmt::format("abcdefghijabcdefghij{}", num)
              : fmt::format("s{}", num);
          return StringView(stringVec[row]);
        }));
  }

  SelectivityVector rows(numRows, true);
  raw_vector<uint64_t> hashes{numRows};
  for (auto i = 0; i < 7; ++i) {
    if (i < 5) {
      ASSERT_TRUE(hasher->mayUseValueIds());
      ASSERT_EQ(i * numRows, hasher->numUniqueValues());
    } else {
      ASSERT_FALSE(hasher->mayUseValueIds());
      ASSERT_EQ(0, hasher->numUniqueValues());
    }
    if (hasher->mayUseValueIds()) {
      hasher->decode(*batches[i], rows);
      hasher->computeValueIds(rows, hashes);
    }
  }
}

TEST_F(VectorHasherTest, integerIds) {
  auto vector = BaseVector::create(BIGINT(), 100, pool());
  auto ints = vector->as<FlatVector<int64_t>>();
  static constexpr int64_t kMin = std::numeric_limits<int64_t>::min();
  ints->setNull(0, true);
  for (int64_t i = 0; i < 99; ++i) {
    ints->set(i + 1, kMin + i * 10);
  }
  auto hasher = exec::VectorHasher::create(BIGINT(), 1);
  raw_vector<uint64_t> hashes(ints->size());
  SelectivityVector rows(ints->size());
  hasher->decode(*vector, rows);
  EXPECT_FALSE(hasher->computeValueIds(rows, hashes));
  hasher->enableValueRange(1, 50);
  hasher->decode(*vector, rows);
  EXPECT_TRUE(hasher->computeValueIds(rows, hashes));
  // null is always 0
  EXPECT_EQ(hashes[0], 0);
  // min int64_t should be 1.
  EXPECT_EQ(hashes[1], 1);
  EXPECT_EQ(hashes[11], 101);
  uint64_t numRange;
  uint64_t numDistinct;
  hasher->cardinality(0, numRange, numDistinct);
  EXPECT_EQ(numDistinct, 100);
  EXPECT_GT(numRange, 1001);
  ints->set(10, 10000);

  hasher->decode(*vector, rows);
  EXPECT_FALSE(hasher->computeValueIds(rows, hashes));
  hasher->cardinality(0, numRange, numDistinct);
  EXPECT_EQ(numRange, VectorHasher::kRangeTooLarge);

  auto filter = hasher->getFilter(false);
  ASSERT_TRUE(filter != nullptr);
  auto bigintValues =
      dynamic_cast<common::BigintValuesUsingHashTable*>(filter.get());
  ASSERT_TRUE(bigintValues != nullptr);
  ASSERT_FALSE(bigintValues->testNull());
  ASSERT_TRUE(bigintValues->testInt64(kMin + 100));
  ASSERT_FALSE(bigintValues->testInt64(kMin + 101));
  ASSERT_FALSE(bigintValues->testInt64(0));

  hasher = exec::VectorHasher::create(BIGINT(), 1);
  hasher->enableValueIds(1, VectorHasher::kNoLimit);
  // We add values that are over 100K distinct and withmax - min > int64_t max.
  hasher->decode(*vector, rows);
  EXPECT_TRUE(hasher->computeValueIds(rows, hashes));
  // null is still 0.
  EXPECT_EQ(hashes[0], 0);
  for (int count = 0; count < 1000; ++count) {
    vector_size_t index = 0;
    for (int64_t value = count * 100; value < count * 100 + 100; ++value) {
      ints->set(index++, value);
    }
    hasher->decode(*vector, rows);
    hasher->computeValueIds(rows, hashes);
  }

  hasher->cardinality(0, numRange, numDistinct);
  EXPECT_EQ(numRange, VectorHasher::kRangeTooLarge);
  EXPECT_EQ(numDistinct, VectorHasher::kRangeTooLarge);
}

TEST_F(VectorHasherTest, dateIds) {
  auto vector = BaseVector::create(DATE(), 100, pool());
  auto* dates = vector->as<FlatVector<int32_t>>();
  static constexpr int32_t kMin = std::numeric_limits<int32_t>::min();
  dates->setNull(0, true);
  for (auto i = 0; i < 99; ++i) {
    dates->set(i + 1, kMin + i * 10);
  }
  auto hasher = exec::VectorHasher::create(DATE(), 1);
  raw_vector<uint64_t> hashes(dates->size());
  SelectivityVector rows(dates->size());
  hasher->decode(*vector, rows);
  EXPECT_FALSE(hasher->computeValueIds(rows, hashes));
  hasher->enableValueRange(1, 0);
  hasher->decode(*vector, rows);
  EXPECT_TRUE(hasher->computeValueIds(rows, hashes));
  // Hash of null is always 0.
  EXPECT_EQ(hashes[0], 0);
  EXPECT_EQ(hashes[1], 1);
  EXPECT_EQ(hashes[11], 101);

  uint64_t numRange;
  uint64_t numDistinct;
  hasher->cardinality(0, numRange, numDistinct);
  EXPECT_EQ(numDistinct, 100);
  EXPECT_GT(numRange, 100);

  dates->set(10, 10000);
  hasher->decode(*vector, rows);
  EXPECT_FALSE(hasher->computeValueIds(rows, hashes));
  hasher->cardinality(10, numRange, numDistinct);
  // The range is padded up by 10%.
  EXPECT_LE(2360000000, numRange);

  hasher = exec::VectorHasher::create(DATE(), 1);
  hasher->enableValueIds(1, VectorHasher::kNoLimit);
  // We add values that are over 100K distinct and with max - min > int32_t max.
  hasher->decode(*vector, rows);
  EXPECT_TRUE(hasher->computeValueIds(rows, hashes));
  // Hash of null is still 0.
  EXPECT_EQ(hashes[0], 0);
  for (auto count = 0; count < 1000; ++count) {
    vector_size_t index = 0;
    for (int64_t value = count * 100; value < count * 100 + 100; ++value) {
      dates->set(index++, value);
    }
    hasher->decode(*vector, rows);
    hasher->computeValueIds(rows, hashes);
  }

  hasher->cardinality(0, numRange, numDistinct);
  EXPECT_EQ(numRange, 2147583649);
  EXPECT_EQ(numDistinct, VectorHasher::kRangeTooLarge);
}

TEST_F(VectorHasherTest, boolNoNulls) {
  auto vector = BaseVector::create(BOOLEAN(), 100, pool());
  auto bools = vector->as<FlatVector<bool>>();
  bools->resize(3);
  bools->set(0, true);
  bools->set(1, false);
  bools->set(2, true);
  raw_vector<uint64_t> hashes(bools->size());
  std::fill(hashes.begin(), hashes.end(), 0);
  SelectivityVector rows(bools->size());
  auto hasher = exec::VectorHasher::create(BOOLEAN(), 1);
  hasher->enableValueRange(2, 2000);
  hasher->decode(*vector, rows);
  EXPECT_TRUE(hasher->computeValueIds(rows, hashes));
  // We expect false is 1, true is 2 times 2 because of the
  // multiplier passed to enableValueRange().
  EXPECT_EQ(4, hashes[0]);
  EXPECT_EQ(2, hashes[1]);
  EXPECT_EQ(4, hashes[2]);
  uint64_t numRange;
  uint64_t numDistinct;
  hasher->cardinality(0, numRange, numDistinct);
  EXPECT_EQ(numRange, 3);
  EXPECT_EQ(numDistinct, 3);
}

TEST_F(VectorHasherTest, boolWithNulls) {
  auto vector = BaseVector::create(BOOLEAN(), 100, pool());
  auto bools = vector->as<FlatVector<bool>>();
  bools->resize(3);
  bools->setNull(0, true);
  bools->set(1, false);
  bools->set(2, true);
  raw_vector<uint64_t> hashes(bools->size());
  std::fill(hashes.begin(), hashes.end(), 0);
  SelectivityVector rows(bools->size());
  auto hasher = exec::VectorHasher::create(BOOLEAN(), 1);
  hasher->enableValueRange(2, 2000);
  hasher->decode(*vector, rows);
  EXPECT_TRUE(hasher->computeValueIds(rows, hashes));
  // We expect null is 0, false is 2, true is 2 times 2 because of the
  // multiplier passed to enableValueRange().
  EXPECT_EQ(hashes[0], 0);
  EXPECT_EQ(hashes[1], 1 * 2);
  EXPECT_EQ(hashes[2], 2 * 2);
  uint64_t numRange;
  uint64_t numDistinct;
  hasher->cardinality(0, numRange, numDistinct);
  EXPECT_EQ(numRange, 3);
  EXPECT_EQ(numDistinct, 3);
}

TEST_F(VectorHasherTest, merge) {
  constexpr vector_size_t kSize = 100;
  auto vector =
      makeFlatVector<int64_t>(kSize, [](vector_size_t row) { return row; });

  VectorHasher hasher(BIGINT(), 0);
  SelectivityVector rows(kSize);
  raw_vector<uint64_t> hashes(kSize);
  hasher.decode(*vector, rows);
  hasher.computeValueIds(rows, hashes);
  auto otherVector = makeFlatVector<int64_t>(kSize, [](vector_size_t row) {
    return row < kSize / 2 ? row : row + 1000;
  });
  VectorHasher otherHasher(BIGINT(), 0);
  otherHasher.decode(*otherVector, rows);
  otherHasher.computeValueIds(rows, hashes);
  // hasher has 0..99 and otherHasher has 0..49, 1050..1099.
  VectorHasher emptyHasher(BIGINT(), 0);
  VectorHasher otherEmptyHasher(BIGINT(), 0);
  EXPECT_TRUE(emptyHasher.empty());
  emptyHasher.merge(otherHasher);
  hasher.merge(emptyHasher);
  hasher.merge(otherEmptyHasher);
  uint64_t numRange;
  uint64_t numDistinct;
  hasher.cardinality(0, numRange, numDistinct);
  // [0..1100] plus 1 for null.
  EXPECT_EQ(numRange, 1 + 1000 + kSize);
  // Half the values are in common, plus 1 for null.
  EXPECT_EQ(numDistinct, 1 + kSize + (kSize / 2));

  auto filter = hasher.getFilter(false);
  ASSERT_TRUE(filter != nullptr);
  auto bigintValues =
      dynamic_cast<common::BigintValuesUsingBitmask*>(filter.get());
  ASSERT_TRUE(bigintValues != nullptr);
  ASSERT_FALSE(bigintValues->testNull());
  ASSERT_TRUE(bigintValues->testInt64(56));
  ASSERT_TRUE(bigintValues->testInt64(1066));
  ASSERT_FALSE(bigintValues->testInt64(304));
  ASSERT_FALSE(bigintValues->testInt64(123));

  std::unordered_set<uint64_t> ids;
  hasher.enableValueIds(1, 0);
  hasher.decode(*vector, rows);
  hasher.computeValueIds(rows, hashes);
  for (auto& h : hashes) {
    ids.insert(h);
  }
  hasher.decode(*otherVector, rows);
  hasher.computeValueIds(rows, hashes);
  for (auto& h : hashes) {
    ids.insert(h);
  }

  // Check all values have distinct id. -1 to account for null that
  // does not occur in the data.
  EXPECT_EQ(numDistinct - 1, ids.size());
}

TEST_F(VectorHasherTest, computeValueIdsBigint) {
  testComputeValueIds<int64_t>(false);
  testComputeValueIds<int64_t>(true);
}

TEST_F(VectorHasherTest, computeValueIdsInteger) {
  testComputeValueIds<int32_t>(false);
  testComputeValueIds<int32_t>(true);
}

TEST_F(VectorHasherTest, computeValueIdsSmallint) {
  testComputeValueIds<int16_t>(false);
  testComputeValueIds<int16_t>(true);
}

TEST_F(VectorHasherTest, computeValueIdsTinyint) {
  testComputeValueIds<int8_t>(false);
  testComputeValueIds<int8_t>(true);
}

TEST_F(VectorHasherTest, computeValueIdsBoolDictionary) {
  vector_size_t size = 1'000;
  auto vector = makeDictionary(
      size, makeFlatVector<bool>(11, [](auto row) { return row % 2 == 0; }));

  SelectivityVector allRows(size);
  auto hasher = exec::VectorHasher::create(BOOLEAN(), 0);
  uint64_t rangeSize;
  uint64_t distinctSize;
  hasher->cardinality(0, rangeSize, distinctSize);
  EXPECT_EQ(3, rangeSize);
  EXPECT_EQ(3, distinctSize);
  raw_vector<uint64_t> result(size);
  std::fill(result.begin(), result.end(), 0);
  hasher->decode(*vector, allRows);
  auto ok = hasher->computeValueIds(allRows, result);
  ASSERT_TRUE(ok);
  // A boolean counts as as a range of 3 and the extra margin has no effect.
  EXPECT_EQ(6, hasher->enableValueRange(2, 11));
}

TEST_F(VectorHasherTest, computeValueIdsStrings) {
  auto b0 = makeFlatVector<StringView>({"2021-02-02", "2021-02-01"});
  auto b1 = makeFlatVector<StringView>({"red", "green"});
  auto b2 = makeFlatVector<StringView>(
      {"apple", "orange", "grapefruit", "banana", "star fruit", "potato"});
  auto b3 =
      makeFlatVector<StringView>({"pine", "birch", "elm", "maple", "chestnut"});
  std::vector<VectorPtr> baseVectors = {b0, b1, b2, b3};

  vector_size_t size = 1'111;

  std::vector<VectorPtr> dictionaryVectors;
  dictionaryVectors.reserve(baseVectors.size());

  for (auto& baseVector : baseVectors) {
    dictionaryVectors.emplace_back(makeDictionary(size, baseVector));
  }

  std::vector<std::unique_ptr<exec::VectorHasher>> hashers;
  hashers.reserve(4);
  for (int i = 0; i < 4; i++) {
    hashers.emplace_back(
        std::make_unique<exec::VectorHasher>(dictionaryVectors[i]->type(), i));
  }

  SelectivityVector allRows(size);
  uint64_t multiplier = 1;
  for (int i = 0; i < 4; i++) {
    auto hasher = hashers[i].get();
    raw_vector<uint64_t> result(size);
    hasher->decode(*dictionaryVectors[i], allRows);
    auto ok = hasher->computeValueIds(allRows, result);
    ASSERT_FALSE(ok);

    uint64_t asRange;
    uint64_t asDistinct;
    hasher->cardinality(0, asRange, asDistinct);
    ASSERT_EQ(baseVectors[i]->size() + 1, asDistinct);

    multiplier = hasher->enableValueIds(multiplier, 0);
  }

  raw_vector<uint64_t> result(size);
  for (int i = 0; i < 4; i++) {
    auto hasher = hashers[i].get();
    hasher->decode(*dictionaryVectors[i], allRows);
    bool ok = hasher->computeValueIds(allRows, result);
    ASSERT_TRUE(ok);
  }

  auto stringAt = [&](vector_size_t i, vector_size_t row) {
    return dictionaryVectors[i]->as<SimpleVector<StringView>>()->valueAt(row);
  };

  auto stringsAt = [&](vector_size_t i) -> std::vector<StringView> {
    return {stringAt(0, i), stringAt(1, i), stringAt(2, i), stringAt(3, i)};
  };

  std::unordered_map<uint64_t, std::vector<StringView>> uniqueValues;
  for (auto i = 0; i < size; i++) {
    auto id = result[i];
    if (uniqueValues.find(id) == uniqueValues.end()) {
      uniqueValues.insert({id, stringsAt(i)});
    } else {
      ASSERT_EQ(uniqueValues.find(id)->second, stringsAt(i)) << "at " << i;
    };
  }

  ASSERT_LE(uniqueValues.size(), multiplier);

  {
    result[42] = 0xAAAAAAAAAAAAAAAA;
    VectorHasher hasher(dictionaryVectors[0]->type(), 0);
    SelectivityVector oneRow(size, false);
    oneRow.setValid(42, true);
    oneRow.updateBounds();
    hasher.decode(*dictionaryVectors[0], oneRow);
    ASSERT_FALSE(hasher.computeValueIds(oneRow, result));
    uint64_t asRange;
    uint64_t asDistinct;
    hasher.cardinality(0, asRange, asDistinct);
    ASSERT_EQ(asDistinct, 2);
    hasher.enableValueIds(1, 0);
    hasher.decode(*dictionaryVectors[0], oneRow);
    ASSERT_TRUE(hasher.computeValueIds(oneRow, result));
    ASSERT_EQ(result[42], 1);
  }
}

namespace {

// enum for marking special values to be tested in a type.
enum class Value { kNull, kMin, kMax, kZero };

template <typename T>
// Appends a null for id 0, min of T for 1, max of T for 2 and 0 for 3.
void append(FlatVectorPtr<T>& vector, Value id) {
  auto size = vector->size();
  vector->resize(size + 1);
  if (id == Value::kNull) {
    vector->setNull(size, true);
  } else {
    vector->set(
        size,
        id == Value::kMin       ? std::numeric_limits<T>::min()
            : id == Value::kMax ? std::numeric_limits<T>::max()
                                : 0);
  }
}
} // namespace

TEST_F(VectorHasherTest, endOfRange) {
  // We sample with large but not end-reaching ranges of different
  // ints.  We check that ranges are rounded up to ends of the data
  // types.  We check that nulls, min and max values in each column
  // combine to make distinct normalized keys. For example the
  // normalized key of max of key 1, null of key 2 must be different
  // from null of key 1, min of key 2. This tests that the ends of
  // ranges are padded properly and that the multipliers of each key
  // cover all the values in range for the key for 8, 16 and 32 bit
  // types. 64 bit types limit the range of max - min to less than 63
  // bits, so we can't have a range that touches both upper and lower
  // bounds of the data type.

  constexpr int64_t kTinyRange = std::numeric_limits<uint8_t>::max();
  constexpr int64_t kSmallRange = std::numeric_limits<uint16_t>::max();
  constexpr int64_t kIntRange = std::numeric_limits<uint32_t>::max();

  using exec::VectorHasher;

  // Make samples of 8, 16 and 32 bit values such that when rounding, the
  // extended range will cover both ends of the type.
  auto tinySample = makeFlatVector(std::vector<int8_t>{-100, 110});
  auto smallSample = makeFlatVector(std::vector<int16_t>{-30000, 31000});
  auto intSample =
      makeFlatVector(std::vector<int32_t>{-2001000000, 2000000000});
  auto tinyHasher = VectorHasher::create(TINYINT(), 0);
  auto smallHasher = VectorHasher::create(SMALLINT(), 1);
  auto intHasher = VectorHasher::create(INTEGER(), 2);
  raw_vector<uint64_t> result;
  // Analyze the sample data.
  SelectivityVector rows(2);
  tinyHasher->decode(*tinySample, rows);
  tinyHasher->computeValueIds(rows, result);

  smallHasher->decode(*smallSample, rows);
  smallHasher->computeValueIds(rows, result);

  intHasher->decode(*intSample, rows);
  intHasher->computeValueIds(rows, result);

  // Check that the analysis concluded that the padded ranges extend the width
  // of the data type.
  uint64_t distinct;
  uint64_t range;
  tinyHasher->cardinality(50, range, distinct);
  // Distinct count is 4 because 2 values + 50% is 3 and 1 for the null value.
  EXPECT_EQ(4, distinct);
  EXPECT_EQ(range, kTinyRange + 2);
  smallHasher->cardinality(50, range, distinct);
  EXPECT_EQ(4, distinct);
  EXPECT_EQ(range, kSmallRange + 2);
  intHasher->cardinality(50, range, distinct);
  EXPECT_EQ(4, distinct);
  EXPECT_EQ(range, kIntRange + 2);

  // Set the VectorHashers to encode inputs as offsets within the the range of
  // each.
  auto multiplier1 = tinyHasher->enableValueRange(1, 50);
  auto multiplier2 = smallHasher->enableValueRange(multiplier1, 50);
  auto multiplier3 = intHasher->enableValueRange(multiplier2, 50);
  // The total number of combinations is the product of the
  // ranges. Each range includes the end points and an extra value for
  // null, hence (max - min) + 2. E.g. the range of 1 .. 2 inclusive
  // plus null has three distinct values.
  EXPECT_EQ(
      (kTinyRange + 2) * (kSmallRange + 2) * (kIntRange + 2), multiplier3);

  // Make test data. Each vector of 8, 16 and 32 bit values has the min, 0, max
  // of its type and null.
  auto tinyData = BaseVector::create<FlatVector<int8_t>>(TINYINT(), 0, pool());
  auto smallData =
      BaseVector::create<FlatVector<int16_t>>(SMALLINT(), 0, pool());
  auto intData = BaseVector::create<FlatVector<int32_t>>(INTEGER(), 0, pool());
  std::vector<Value> values = {
      Value::kNull, Value::kMin, Value::kZero, Value::kMax};

  for (auto tinyId : values) {
    for (auto smallId : values) {
      for (auto intId : values) {
        append<int8_t>(tinyData, tinyId);
        append<int16_t>(smallData, smallId);
        append<int32_t>(intData, intId);
      }
    }
  }

  // make normalized keys from the 8, 16 and 32 bit key parts.
  result.resize(tinyData->size());
  rows.resize(tinyData->size());

  tinyHasher->decode(*tinyData, rows);
  tinyHasher->computeValueIds(rows, result);

  smallHasher->decode(*smallData, rows);
  smallHasher->computeValueIds(rows, result);

  intHasher->decode(*intData, rows);
  intHasher->computeValueIds(rows, result);

  // Check that all the results are distinct.
  std::unordered_set<uint64_t> uniques;
  for (auto id : result) {
    uniques.insert(id);
  }
  EXPECT_EQ(tinyData->size(), uniques.size());
}

TEST_F(VectorHasherTest, hashCollision) {
  constexpr int kValue = 42;
  UniqueValue x(kValue);
  UniqueValue y(kValue | (1ull << 32));
  UniqueValueHasher h;
  EXPECT_NE(h(x), h(y));
}

TEST_F(VectorHasherTest, simdRange) {
  // Tests the SIMD path for integer value ranges. We make hashers based on a
  // sample, then introduce out of range values and check that unmappable rows
  // are detected and correctt normalized keys are produced for the rows with
  // in-range values.
  constexpr int32_t kNumRows = 1001;
  using exec::VectorHasher;

  auto smallValues =
      makeFlatVector<int16_t>(kNumRows, [](auto i) { return i; });
  auto intValues = makeFlatVector<int32_t>(kNumRows, [](auto i) { return i; });
  auto int64Values =
      makeFlatVector<int64_t>(kNumRows, [](auto i) { return i; });

  auto smallHasher = VectorHasher::create(SMALLINT(), 0);
  auto intHasher = VectorHasher::create(INTEGER(), 1);
  auto int64Hasher = VectorHasher::create(BIGINT(), 2);

  raw_vector<uint64_t> result(kNumRows);

  // Analyze the sample data.
  SelectivityVector rows(kNumRows);
  smallHasher->decode(*smallValues, rows);
  smallHasher->computeValueIds(rows, result);

  intHasher->decode(*intValues, rows);
  intHasher->computeValueIds(rows, result);

  int64Hasher->decode(*int64Values, rows);
  int64Hasher->computeValueIds(rows, result);

  // Set the VectorHashers to encode inputs as offsets within the the range of
  // each.
  auto multiplier1 = smallHasher->enableValueRange(1, 0);
  auto multiplier2 = intHasher->enableValueRange(multiplier1, 0);
  auto multiplier3 = int64Hasher->enableValueRange(multiplier2, 0);
  // All have kNumRows distinct values.
  EXPECT_EQ((kNumRows + 1) * (kNumRows + 1) * (kNumRows + 1), multiplier3);

  for (auto i = 0; i < kNumRows; ++i) {
    // We add some unmappable values at fixed positions.
    if (i % 7 == 0) {
      smallValues->set(i, -10000);
    }
    if (i % 11 == 0) {
      intValues->set(i, -10000);
    }
    if (i % 13 == 0) {
      int64Values->set(i, 10000);
    }
  }
  // make normalized keys.
  result.resize(kNumRows);
  rows.resize(kNumRows);

  VectorHasher::ScratchMemory scratch;
  smallHasher->lookupValueIds(*smallValues, rows, scratch, result);
  intHasher->lookupValueIds(*intValues, rows, scratch, result);
  int64Hasher->lookupValueIds(*int64Values, rows, scratch, result);

  for (auto i = 0; i < kNumRows; ++i) {
    if (i % 7 == 0 || i % 11 == 0 || i % 13 == 0) {
      EXPECT_FALSE(rows.isValid(i));
    } else {
      EXPECT_TRUE(rows.isValid(i));
      EXPECT_EQ(
          smallValues->valueAt(i) + 1 +
              ((intValues->valueAt(i) + 1) * multiplier1) +
              ((int64Values->valueAt(i) + 1) * multiplier2),
          result[i]);
    }
  }
}

TEST_F(VectorHasherTest, typeMismatch) {
  auto hasher = VectorHasher::create(BIGINT(), 0);

  auto data = makeFlatVector<std::string>(
      {"a",
       "b"
       "c"});
  SelectivityVector rows(data->size());
  VELOX_ASSERT_THROW(
      hasher->decode(*data, rows), "Type mismatch: BIGINT vs. VARCHAR");
}
