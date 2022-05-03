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
#include "velox/common/base/Nulls.h"
#include "velox/common/base/SimdUtil.h"

namespace facebook::velox::dwrf {

int32_t nonNullRowsFromDense(
    const uint64_t* nulls,
    int32_t numRows,
    raw_vector<int32_t>& nonNullRows) {
  nonNullRows.resize(numRows);
  auto size = simd::indicesOfSetBits(nulls, 0, numRows, nonNullRows.data());
  nonNullRows.resize(size);
  return size;
}
// Returns 8 bits starting at bit 'index'.
uint8_t load8Bits(const uint64_t* bits, int32_t index) {
  uint8_t shift = index & 7;
  uint32_t byte = index >> 3;
  auto asBytes = reinterpret_cast<const uint8_t*>(bits);
  return (*reinterpret_cast<const int16_t*>(asBytes + byte) >> shift);
}

// Loads 'width' bits starting at bit 'index' in 'bits'. 'width is
// 56 bits or less, so that we can for any index load all the bits
// in a 64 bit load.
uint64_t loadUpTo56Bits(const uint64_t* bits, int32_t index, int32_t width) {
  uint8_t shift = index & 7;
  uint32_t byte = index >> 3;
  auto asBytes = reinterpret_cast<const uint8_t*>(bits);
  return (*reinterpret_cast<const uint64_t*>(asBytes + byte) >> shift) &
      bits::lowMask(width);
}

template <bool isFilter, bool outputNulls>
bool nonNullRowsFromSparse(
    const uint64_t* nulls,
    RowSet rows,
    raw_vector<int32_t>& innerRows,
    raw_vector<int32_t>& outerRows,
    uint64_t* resultNulls,
    int32_t& tailSkip) {
  constexpr int32_t kStep = 8;
  bool anyNull = false;
  auto numIn = rows.size();
  innerRows.resize(numIn);
  outerRows.resize(numIn);
  auto inner = innerRows.data();
  auto outer = outerRows.data();
  int32_t numNulls = 0;
  int32_t numInner = 0;
  int32_t lastNonNull = -1;
  auto resultNullBytes = reinterpret_cast<uint8_t*>(resultNulls);

  // Returns the index in terms of non-null rows for
  // 'rows[i]'. Assumes that i is increasing between calls.
  auto innerFor = [&](int32_t i) {
    auto row = rows[i];
    DCHECK_GT(row, lastNonNull);
    auto skip = row - lastNonNull - 1;
    if (!skip) {
      // Consecutive non-nulls
    } else if (skip < 56) {
      auto bits = loadUpTo56Bits(nulls, lastNonNull + 1, skip);
      numNulls += skip - __builtin_popcountll(bits);
    } else {
      numNulls += skip -
          bits::countBits(nulls, lastNonNull + 1, lastNonNull + skip + 1);
    }
    lastNonNull = row;
    return row - numNulls;
  };
  for (auto i = 0; i < numIn; i += kStep) {
    int32_t width = i + kStep > numIn ? numIn - i : kStep;
    int16_t widthMask = bits::lowMask(width);
    if (isDense(rows.data() + i, width)) {
      uint16_t flags = load8Bits(nulls, rows[i]) & widthMask;
      if (outputNulls) {
        resultNullBytes[i / 8] = flags;
        anyNull |= flags != widthMask;
      }
      if (!flags) {
        continue;
      }
      auto numNonNull = __builtin_popcount(flags);
      // contiguous inner rows.
      auto innerStart = innerFor(i);
      (simd::iota<int32_t>() + innerStart).store_unaligned(inner + numInner);
      if (isFilter) {
        simd::filter(xsimd::load_unaligned(rows.data() + i), flags)
            .store_unaligned(outer + numInner);
      } else {
        (detail::bitMaskIndices<int32_t>(flags) + i)
            .store_unaligned(outer + numInner);
      }
      // We processed 'width' consecutive. The inner count is incremented
      // by number of non-nulls. Nulls are counted for the 'width' rows,
      // so we set lastNonNull to be the last of these and increment
      // numNulls by the number of nulls in the 8 rows. This is
      // correct even if the last of the 8 is null since the null
      // count is correct up to that row.
      numInner += numNonNull;
      lastNonNull = rows[i + width - 1];
      numNulls += width - numNonNull;
    } else {
      auto next8Rows = xsimd::load_unaligned(rows.data() + i);
      uint16_t flags = simd::gather8Bits(nulls, next8Rows, width);
      if (outputNulls) {
        resultNullBytes[i / 8] = flags;
        anyNull |= flags != widthMask;
      }
      if (!flags) {
        continue;
      }
      if (isFilter) {
        // The non-null indices among 'rows' are possible filter results.
        simd::filter(next8Rows, flags).store_unaligned(outer + numInner);
      } else {
        // Make scatter indices so that there are gaps for 'rows' that hit a
        // null.
        (detail::bitMaskIndices<int32_t>(flags) + i)
            .store_unaligned(outer + numInner);
      }
      // Calculate the inner row corresponding to each non-null in 'next8Rows'.
      while (flags) {
        int32_t index = bits::getAndClearLastSetBit(flags);
        inner[numInner++] = innerFor(i + index);
      }
    }
  }
  innerRows.resize(numInner);
  outerRows.resize(numInner);
  // If ending with a null, skip the non-nulls between last non-null in
  // rows and the last null in 'rows'.
  tailSkip = bits::countBits(nulls, lastNonNull + 1, rows.back());

  return anyNull;
}

template bool nonNullRowsFromSparse<true, false>(
    const uint64_t* nulls,
    RowSet rows,
    raw_vector<int32_t>& innerRows,
    raw_vector<int32_t>& outerRows,
    uint64_t* resultNulls,
    int32_t& tailSkip);

template bool nonNullRowsFromSparse<false, true>(
    const uint64_t* nulls,
    RowSet rows,
    raw_vector<int32_t>& innerRows,
    raw_vector<int32_t>& outerRows,
    uint64_t* resultNulls,
    int32_t& tailSkip);

template bool nonNullRowsFromSparse<false, false>(
    const uint64_t* nulls,
    RowSet rows,
    raw_vector<int32_t>& innerRows,
    raw_vector<int32_t>& outerRows,
    uint64_t* resultNulls,
    int32_t& tailSkip);

template <typename T>
void scatterNonNulls(
    int32_t targetBegin,
    int32_t numValues,
    int32_t sourceBegin,
    const int32_t* target,
    T* data) {
  for (auto index = numValues - 1; index >= 0; --index) {
    auto destination = target[targetBegin + index];
    if (destination == sourceBegin + index) {
      break;
    }
    data[destination] = data[sourceBegin + index];
  }
}

template void scatterNonNulls(
    int32_t rowIndex,
    int32_t numRows,
    int32_t numValues,
    const int32_t* target,
    int64_t* data);

template void scatterNonNulls(
    int32_t rowIndex,
    int32_t numRows,
    int32_t numValues,
    const int32_t* target,
    int32_t* data);

template void scatterNonNulls(
    int32_t rowIndex,
    int32_t numRows,
    int32_t numValues,
    const int32_t* target,
    int16_t* data);

template void scatterNonNulls(
    int32_t rowIndex,
    int32_t numRows,
    int32_t numValues,
    const int32_t* target,
    float* data);

template void scatterNonNulls(
    int32_t rowIndex,
    int32_t numRows,
    int32_t numValues,
    const int32_t* target,
    double* data);

} // namespace facebook::velox::dwrf
