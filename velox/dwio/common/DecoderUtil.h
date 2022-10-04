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

#pragma once

#include <algorithm>
#include "velox/common/base/RawVector.h"
#include "velox/common/process/ProcessBase.h"
#include "velox/dwio/common/StreamUtil.h"

namespace facebook::velox {
// Same as in LazyVector.h.
using RowSet = folly::Range<const int32_t*>;
namespace common {
class AlwaysTrue;
template <typename TFilter, typename T>
static bool applyFilter(TFilter& filter, T value);
} // namespace common
} // namespace facebook::velox

namespace facebook::velox::dwio::common {

inline int32_t firstNullIndex(const uint64_t* nulls, int32_t numRows) {
  int32_t first = -1;
  bits::testUnsetBits(nulls, 0, numRows, [&](int32_t row) {
    first = row;
    return false;
  });
  return first;
}

template <typename T, typename Any>
void scatterDense(
    const Any* data,
    const int32_t* indices,
    int32_t size,
    T* target) {
  auto source = reinterpret_cast<const T*>(data);
  if (source >= target && source < target + indices[size - 1]) {
    for (int32_t i = size - 1; i >= 0; --i) {
      target[indices[i]] = source[i];
    }
  } else {
    for (auto i = 0; i < size; ++i) {
      target[indices[i]] = source[i];
    }
  }
}

namespace detail {

template <typename T, typename A = xsimd::default_arch>
auto bitMaskIndices(uint8_t bits, const A& arch = {}) {
  return simd::loadGatherIndices<T>(simd::byteSetBits(bits), arch);
}

} // namespace detail

// Filters and writes a SIMD register worth of values into scan
// output. T is the element type of 'values'. 'width' is the number
// of valid leading elements in 'values'.  Appends the row numbers
// of passing values to 'filterHits', starting at 'numValues'. If
// 'filterOnly' is false, also appends the passing values themselves
// into 'rawValues', starting at 'numValues'. Increments 'numValues'
// with the count of passing values. 'loadIndices' is a lambda that
// returns the row numbers corresponding to the elements in
// 'values'. These are not necessarily contiguous since values from
// non-contiguous rows may have been gathered for a single call to
// this. loadIndices is called with an argument of 0 to geth the 8
// first row numbers and if values has 16 elements, an argument of 1
// gets the next 8 row numbers.
template <
    typename T,
    bool filterOnly,
    bool scatter,
    bool dense,
    typename TFilter,
    typename LoadIndices>
inline void processFixedFilter(
    xsimd::batch<T> values,
    int32_t width,
    int32_t firstRow,
    TFilter& filter,
    LoadIndices loadIndices,
    T* rawValues,
    int32_t* filterHits,
    int32_t& numValues) {
  constexpr bool is16 = sizeof(T) == 2;
  constexpr int kIndexLaneCount = xsimd::batch<int32_t>::size;
  auto word = simd::toBitMask(filter.testValues(values));
  if (!word) {
    ; /* no values passed, no action*/
  } else if (word == simd::allSetBitMask<T>()) {
    loadIndices(0).store_unaligned(filterHits + numValues);
    if (is16) {
      // If 16 values in 'values', copy the next 8x 32 bit indices.
      loadIndices(1).store_unaligned(filterHits + numValues + kIndexLaneCount);
    }
    if (!filterOnly) {
      // 4, 8 or 16 values in 'values'.
      values.store_unaligned(rawValues + numValues);
    }
    numValues += width;
  } else {
    auto allBits = word & bits::lowMask(width);
    auto bits = is16 ? allBits & ((1 << kIndexLaneCount) - 1) : allBits;
    if (dense && !scatter) {
      (detail::bitMaskIndices<T>(bits) + firstRow)
          .store_unaligned(filterHits + numValues);
    } else {
      simd::filter<int32_t>(loadIndices(0), bits, xsimd::default_arch{})
          .store_unaligned(filterHits + numValues);
    }
    filterHits += __builtin_popcount(bits);
    if (is16) {
      firstRow += kIndexLaneCount;
      bits = allBits >> kIndexLaneCount;
      if (bits) {
        if (dense && !scatter) {
          (detail::bitMaskIndices<T>(bits) + firstRow)
              .store_unaligned(filterHits + numValues);
        } else {
          simd::filter<int32_t>(loadIndices(1), bits, xsimd::default_arch{})
              .store_unaligned(filterHits + numValues);
        }
      }
    }
    if (!filterOnly) {
      simd::filter(values, allBits).store_unaligned(rawValues + numValues);
    }
    numValues += __builtin_popcount(allBits);
  }
}

struct NoHook;

template <
    typename T,
    bool filterOnly,
    bool scatter,
    typename TFilter,
    typename THook>
void fixedWidthScan(
    folly::Range<const int32_t*> rows,
    const int32_t* scatterRows,
    void* voidValues,
    int32_t* filterHits,
    int32_t& numValues,
    dwio::common::SeekableInputStream& input,
    const char*& bufferStart,
    const char*& bufferEnd,
    TFilter& filter,
    THook& hook) {
  constexpr int32_t kWidth = xsimd::batch<T>::size;
  constexpr bool is16 = sizeof(T) == 2;
  constexpr int32_t kStep = is16 ? 16 : 8;
  constexpr bool hasFilter =
      !std::is_same_v<TFilter, velox::common::AlwaysTrue>;
  constexpr bool hasHook = !std::is_same_v<THook, NoHook>;
  auto rawValues = reinterpret_cast<T*>(voidValues);
  loopOverBuffers<T>(
      rows,
      0,
      input,
      bufferStart,
      bufferEnd,
      [&](T value, int32_t rowIndex) {
        if (!hasFilter) {
          if (hasHook) {
            hook.addValue(scatterRows[rowIndex], &value);
          } else {
            auto targetRow = scatter ? scatterRows[rowIndex] : rowIndex;
            rawValues[targetRow] = value;
          }
          ++numValues;
        } else {
          if (velox::common::applyFilter(filter, value)) {
            auto targetRow = scatter ? scatterRows[rowIndex] : rows[rowIndex];
            filterHits[numValues++] = targetRow;
            if (!filterOnly) {
              rawValues[numValues - 1] = value;
            }
          }
        }
      },

      [&](const int32_t* rows,
          int32_t rowIndex,
          int32_t numRowsInBuffer,
          int32_t rowOffset,
          const T* buffer) {
        rowLoop(
            rows,
            rowIndex,
            rowIndex + numRowsInBuffer,
            kStep,
            [&](int32_t rowIndex) {
              auto firstRow = rows[rowIndex];
              if (!hasFilter) {
                if (hasHook) {
                  hook.addValues(
                      scatterRows + rowIndex,
                      buffer + firstRow - rowOffset,
                      kStep,
                      sizeof(T));
                } else {
                  if (scatter) {
                    scatterDense(
                        buffer + firstRow - rowOffset,
                        scatterRows + rowIndex,
                        kStep,
                        rawValues);
                  } else {
                    simd::memcpy(
                        rawValues + numValues,
                        buffer + firstRow - rowOffset,
                        sizeof(T) * kStep);
                  }
                }
                numValues += kStep;
              } else {
                for (auto step = 0; step < kStep / kWidth; ++step) {
                  auto values =
                      xsimd::load_unaligned(buffer + firstRow - rowOffset);
                  processFixedFilter<T, filterOnly, scatter, true>(
                      values,
                      kWidth,
                      firstRow,
                      filter,
                      [&](int32_t offset) {
                        return simd::loadGatherIndices<T>(
                            (scatter ? scatterRows : rows) + rowIndex +
                            8 * offset);
                      },
                      rawValues,
                      filterHits,
                      numValues);
                  firstRow += kWidth;
                  rowIndex += kWidth;
                }
              }
            },
            [&](int32_t rowIndex) {
              for (auto step = 0; step < kStep / kWidth; ++step) {
                auto indices = simd::loadGatherIndices<T>(rows + rowIndex);
                xsimd::batch<T> values;
                if constexpr (is16) {
                  values =
                      simd::gather(buffer - rowOffset, rows + rowIndex, 16);
                } else {
                  values = simd::gather(buffer - rowOffset, indices);
                }
                if (!hasFilter) {
                  if (hasHook) {
                    hook.addValues(
                        scatterRows + rowIndex, &values, kWidth, sizeof(T));
                  } else {
                    if (scatter) {
                      scatterDense<T>(
                          &values, scatterRows + rowIndex, kWidth, rawValues);
                    } else {
                      values.store_unaligned(rawValues + numValues);
                    }
                    numValues += kWidth;
                  }
                } else {
                  processFixedFilter<T, filterOnly, scatter, false>(
                      values,
                      kWidth,
                      -1,
                      filter,
                      [&](int32_t offset) {
                        if (offset) {
                          return simd::loadGatherIndices<T>(
                              (scatter ? scatterRows : rows) + rowIndex +
                              8 * offset);
                        }
                        return scatter
                            ? simd::loadGatherIndices<T>(scatterRows + rowIndex)
                            : indices;
                      },
                      rawValues,
                      filterHits,
                      numValues);
                }
                rowIndex += kWidth;
              }
            },
            [&](int32_t rowIndex, int32_t numRows) {
              int32_t step = 0;
              while (step < numRows) {
                xsimd::batch<T> values;
                int width = std::min<int32_t>(kWidth, numRows - step);
                if constexpr (is16) {
                  values = simd::gather(
                      buffer - rowOffset, rows + rowIndex, numRows);
                } else {
                  auto indices = simd::loadGatherIndices<T>(rows + rowIndex);
                  if (width < kWidth) {
                    values = simd::maskGather(
                        xsimd::broadcast<T>(0),
                        simd::leadingMask<T>(width),
                        buffer - rowOffset,
                        indices);
                  } else {
                    values = simd::gather(buffer - rowOffset, indices);
                  }
                }
                if (!hasFilter) {
                  if (hasHook) {
                    hook.addValues(
                        scatterRows + rowIndex, &values, width, sizeof(T));
                  } else {
                    if (scatter) {
                      scatterDense<T>(
                          &values, scatterRows + rowIndex, width, rawValues);
                    } else {
                      values.store_unaligned(rawValues + numValues);
                      numValues += width;
                    }
                  }
                } else {
                  processFixedFilter<T, filterOnly, scatter, false>(
                      values,
                      width,
                      -1,
                      filter,
                      [&](int32_t offset) {
                        return simd::loadGatherIndices<T>(
                            (scatter ? scatterRows : rows) + rowIndex +
                            8 * offset);
                      },
                      rawValues,
                      filterHits,
                      numValues);
                }
                rowIndex += width;
                step += width;
              }
            });
      });
}

int32_t nonNullRowsFromDense(
    const uint64_t* nulls,
    int32_t numRows,
    raw_vector<int32_t>& nonNullRows);

// Translates between row numbers in terms of positions in a nullable
// column and row numbers in terms of actually stored non-null values.
//
// For each non-null row in 'rows', with null flags in 'nulls',
// innerRows gets the index into the non-null domain and outerRows is set to
// the corresponding row from 'rows'. If isFilter is true,
// 'outerRows[i] is rows[j], where j is the index of the 'ith'
// non-null in 'rows'. If isFilter is false, outerRows[i] is j, where
// j is the index of the 'ith' non-null in rows.  If outputNulls is
// true, 'resultNulls' has the ith bit set if rows[i] is null. The
// return value is true if outputNulls is true and 'rows'referenced at
// least one null row.
//
// If rows is {1, 3, 5} and rows 0 and 3 are null, and isFilter is
// false, innerRows is {0, 3} and outerRows is {0, 2}. Non-nulls get
// output so that we have gaps for rows that hit a null.  If
// 'isFilter' is true, then outerRows is {1, 5} because these are the
// non-null row numbers in the original 'rows'.. In other words, the
// filter case produces row numbers for the passing rows. A filter
// cannot pass for a null on this code path.
template <bool isFilter, bool outputNulls>
bool nonNullRowsFromSparse(
    const uint64_t* nulls,
    RowSet rows,
    raw_vector<int32_t>& innerRows,
    raw_vector<int32_t>& outerRows,
    uint64_t* resultNulls,
    int32_t& tailSkip);

// See SelectiveColumnReader::useBulkPath.
template <typename Visitor, bool hasNulls>
bool useFastPath(Visitor& visitor) {
  return process::hasAvx2() && Visitor::FilterType::deterministic &&
      Visitor::kHasBulkPath &&
      (std::
           is_same_v<typename Visitor::FilterType, velox::common::AlwaysTrue> ||
       !hasNulls || !visitor.allowNulls()) &&
      (std::is_same_v<typename Visitor::HookType, NoHook> || !hasNulls ||
       Visitor::HookType::kSkipNulls);
}

// Scatters 'numValues' elements of 'data' starting at data[sourceBegin] to
// indices given starting with target[targetBegin]. The scatter is done from
// last to first so as not to overwrite source data when copying from lower to
// higher indices. data[target[targetBegin + numValues - 1] = data[sourceBegin +
// numValues - 1] is the first copy in execution order and
// data[target[targetBegin]] = data[sourceBegin] is the last copy.
template <typename T>
void scatterNonNulls(
    int32_t targetBegin,
    int32_t numValues,
    int32_t sourceBegin,
    const int32_t* target,
    T* data);

// Processes a run of contiguous Ts in 'values'. 'values', starting at
// 'numValues' have been decoded from consecutive (dense case) or
// non-consecutive places in the encoding. The row number in the
// non-null space for values[numValues + i] is rows[rowIndex +i ].
// If hook, call it on
// all.  If no filter and no hook and no nulls, do nothing since the
// values are already in place. If no filter and nulls, scatter the
// values so that there is a gap for the nulls.
//
// If filter, filter the values and shift them down so that there are
// no gaps. Produce the passing row numbers in 'filterHits'. If there
// are nulls, scatter the passing row numbers.
//
// 'numInput' is the number of new values to process. The first of these is in
// values[numValues]. 'rowIndex' is the index of the corresponding value in
// 'scatterRows'
template <
    typename T,
    bool filterOnly,
    bool scatter,
    bool dense,
    typename TFilter,
    typename THook>
void processFixedWidthRun(
    folly::Range<const int32_t*> rows,
    int32_t rowIndex,
    int32_t numInput,
    const int32_t* scatterRows,
    T* values,
    int32_t* filterHits,
    int32_t& numValues,
    TFilter& filter,
    THook& hook) {
  constexpr int32_t kWidth = xsimd::batch<T>::size;
  constexpr bool hasFilter =
      !std::is_same_v<TFilter, velox::common::AlwaysTrue>;
  constexpr bool hasHook = !std::is_same_v<THook, NoHook>;
  if (!hasFilter) {
    if (hasHook) {
      hook.addValues(scatterRows + rowIndex, values, rows.size(), sizeof(T));
    } else if (scatter) {
      scatterNonNulls(rowIndex, numInput, numValues, scatterRows, values);
      numValues = scatterRows[rowIndex + numInput - 1] + 1;
    } else {
      // The values are already in place.
      numValues += numInput;
    }
    return;
  }
  auto end = numInput & ~(kWidth - 1);
  int32_t row = 0;
  int32_t valuesBegin = numValues;
  // Process full vectors
  for (; row < end; row += kWidth) {
    auto valueVector = xsimd::load_unaligned(values + valuesBegin + row);
    processFixedFilter<T, filterOnly, scatter, dense>(
        valueVector,
        kWidth,
        rows[rowIndex + row],
        filter,
        [&](int32_t offset) {
          return simd::loadGatherIndices<T>(
              (scatter ? scatterRows : rows.data()) + rowIndex + row +
              offset * 8);
        },
        values,
        filterHits,
        numValues);
  }
  if (numInput > end) {
    auto valueVector = xsimd::load_unaligned(values + valuesBegin + row);
    processFixedFilter<T, filterOnly, scatter, dense>(
        valueVector,
        numInput - end,
        rows[rowIndex + row],
        filter,
        [&](int32_t offset) {
          return simd::loadGatherIndices<T>(
              (scatter ? scatterRows : rows.data()) + row + rowIndex +
              offset * 8);
        },
        values,
        filterHits,
        numValues);
  }
}

} // namespace facebook::velox::dwio::common
