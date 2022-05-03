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

#include "velox/dwio/dwrf/common/DecoderUtil.h"
#include <folly/Random.h>
#include "velox/common/base/Nulls.h"
#include "velox/type/Filter.h"

#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::dwrf;

class DecoderUtilTest : public testing::Test {
 protected:
  void SetUp() override {
    rng_.seed(1);
  }

  void randomBits(std::vector<uint64_t>& bits, int32_t onesPer1000) {
    for (auto i = 0; i < bits.size() * 64; ++i) {
      if (folly::Random::rand32(rng_) % 1000 < onesPer1000) {
        bits::setBit(bits.data(), i);
      }
    }
  }

  void randomRows(
      int32_t numRows,
      int32_t rowsPer1000,
      raw_vector<int32_t>& result) {
    for (auto i = 0; i < numRows; ++i) {
      if (folly::Random::rand32(rng_) % 1000 < rowsPer1000) {
        result.push_back(i);
      }
    }
  }

  template <bool isFilter, bool outputNulls>
  bool nonNullRowsFromSparseReference(
      const uint64_t* nulls,
      RowSet rows,
      raw_vector<int32_t>& innerRows,
      raw_vector<int32_t>& outerRows,
      uint64_t* resultNulls,
      int32_t& tailSkip) {
    bool anyNull = false;
    auto numIn = rows.size();
    innerRows.resize(numIn);
    outerRows.resize(numIn);
    int32_t lastRow = -1;
    int32_t numNulls = 0;
    int32_t numInner = 0;
    int32_t lastNonNull = -1;
    for (auto i = 0; i < numIn; ++i) {
      auto row = rows[i];
      if (row > lastRow + 1) {
        numNulls += bits::countNulls(nulls, lastRow + 1, row);
      }
      if (bits::isBitNull(nulls, row)) {
        ++numNulls;
        lastRow = row;
        if (!isFilter && outputNulls) {
          bits::setNull(resultNulls, i);
          anyNull = true;
        }
      } else {
        innerRows[numInner] = row - numNulls;
        outerRows[numInner++] = isFilter ? row : i;
        lastNonNull = row;
        lastRow = row;
      }
    }
    innerRows.resize(numInner);
    outerRows.resize(numInner);
    tailSkip = bits::countBits(nulls, lastNonNull + 1, lastRow);
    return anyNull;
  }

  // Maps 'rows' where the row falls on a non-null in 'nulls' to an
  // index in non-null rows. This uses both a reference implementation
  // and the SIMDized fast path and checks consistent results.
  template <bool isFilter, bool outputNulls>
  void testNonNullFromSparse(uint64_t* nulls, RowSet rows) {
    raw_vector<int32_t> referenceInner;
    raw_vector<int32_t> referenceOuter;
    std::vector<uint64_t> referenceNulls(bits::nwords(rows.size()), ~0ULL);
    int32_t referenceSkip;
    auto referenceAnyNull =
        nonNullRowsFromSparseReference<isFilter, outputNulls>(
            nulls,
            rows,
            referenceInner,
            referenceOuter,
            referenceNulls.data(),
            referenceSkip);
    raw_vector<int32_t> testInner;
    raw_vector<int32_t> testOuter;
    std::vector<uint64_t> testNulls(bits::nwords(rows.size()), ~0ULL);
    int32_t testSkip;
    auto testAnyNull = nonNullRowsFromSparse<isFilter, outputNulls>(
        nulls, rows, testInner, testOuter, testNulls.data(), testSkip);

    EXPECT_EQ(testAnyNull, referenceAnyNull);
    EXPECT_EQ(testSkip, referenceSkip);
    for (auto i = 0; i < testInner.size() && i < testOuter.size(); ++i) {
      EXPECT_EQ(testInner[i], referenceInner[i]);
      EXPECT_EQ(testOuter[i], referenceOuter[i]);
    }
    EXPECT_EQ(testInner.size(), referenceInner.size());
    EXPECT_EQ(testOuter.size(), referenceOuter.size());

    if (outputNulls) {
      for (auto i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(
            bits::isBitSet(testNulls.data(), i),
            bits::isBitSet(referenceNulls.data(), i));
      }
    }
  }

  void testNonNullFromSparseCases(uint64_t* nulls, RowSet rows) {
    testNonNullFromSparse<false, true>(nulls, rows);
    testNonNullFromSparse<true, false>(nulls, rows);
  }

  folly::Random::DefaultGenerator rng_;
};

// Running for about 13 seconds.
TEST_F(DecoderUtilTest, nonNullsFromSparse) {
  // We cover cases with different null frequencies and different density of
  // access.
  constexpr int32_t kSize = 2000;
  for (auto nullsIn1000 = 1; nullsIn1000 < 1011; nullsIn1000 += 10) {
    for (auto rowsIn1000 = 1; rowsIn1000 < 1011; rowsIn1000 += 10) {
      raw_vector<int32_t> rows;
      // Have an extra word at the end to allow 64 bit access.
      std::vector<uint64_t> nulls(bits::nwords(kSize) + 1);
      randomBits(nulls, 1000 - nullsIn1000);
      randomRows(kSize, rowsIn1000, rows);
      if (rows.empty()) {
        // The operation is not defined for 0 rows.
        rows.push_back(1234);
      }
      testNonNullFromSparseCases(nulls.data(), rows);
    }
  }
}

namespace facebook::velox::dwrf {
// Excerpt from LazyVector.h.
struct NoHook {
  void addValues(
      const int32_t* /*rows*/,
      const void* /*values*/,
      int32_t /*size*/,
      uint8_t /*valueWidth*/) {}
};

} // namespace facebook::velox::dwrf

TEST_F(DecoderUtilTest, processFixedWithRun) {
  // Tests processing consecutive batches of integers with processFixedWidthRun.
  constexpr int kSize = 100;
  constexpr int32_t kStep = 17;
  raw_vector<int32_t> data;
  raw_vector<int32_t> scatter;
  data.reserve(kSize);
  scatter.reserve(kSize);
  // Data is 0, 100,  2, 98 ... 98, 2.
  // scatter is 0, 2, 4,6 ... 196, 198.
  for (auto i = 0; i < kSize; i += 2) {
    data.push_back(i / 2);
    data.push_back(kSize - i);
    scatter.push_back(i * 2);
    scatter.push_back((i + 1) * 2);
  }

  // the row numbers that pass the filter come here, translated via scatter.
  raw_vector<int32_t> hits(kSize);
  // Each valid index in 'data'
  raw_vector<int32_t> rows(kSize);
  auto filter = std::make_unique<common::BigintRange>(40, 1000, false);
  std::iota(rows.begin(), rows.end(), 0);
  // The passing values are gathered here. Before each call to
  // processFixedWidthRun, the candidate values are appended here and
  // processFixedWidthRun overwrites them with the passing values and sets
  // numValues to be the first unused index after the passing values.
  raw_vector<int32_t> results;
  int32_t numValues = 0;
  for (auto rowIndex = 0; rowIndex < kSize; rowIndex += kStep) {
    int32_t numInput = std::min<int32_t>(kStep, kSize - rowIndex);
    results.resize(numValues + numInput);
    std::memcpy(
        results.data() + numValues,
        data.data() + rowIndex,
        numInput * sizeof(results[0]));

    NoHook noHook;
    processFixedWidthRun<int32_t, false, true, false>(
        rows,
        rowIndex,
        numInput,
        scatter.data(),
        results.data(),
        hits.data(),
        numValues,
        *filter,
        noHook);
  }
  // Check that each value that passes the filter is in 'results' and that   its
  // index times 2 is in 'data' is in 'hits'. The 2x is because the scatter maps
  // each row to 2x the row number.
  int32_t passedCount = 0;
  for (auto i = 0; i < kSize; ++i) {
    if (data[i] >= 40) {
      EXPECT_EQ(data[i], results[passedCount]);
      EXPECT_EQ(i * 2, hits[passedCount]);
      ++passedCount;
    }
  }
}
