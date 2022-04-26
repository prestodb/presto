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
#include "velox/dwio/dwrf/common/StreamUtil.h"

namespace facebook::velox {
// Same as in LazyVector.h.
using RowSet = folly::Range<const int32_t*>;
namespace common {
class AlwaysTrue;
template <typename TFilter, typename T>
static bool applyFilter(TFilter& filter, T value);
} // namespace common
} // namespace facebook::velox

namespace facebook::velox::dwrf {

template <typename T, typename Filter>
__m256i testSimd(Filter& filter, __m256i values) {
  if (std::is_same<T, int64_t>::value) {
    return filter.test4x64(values);
  }
  if (std::is_same<T, double>::value) {
    return filter.test4xDouble(values);
  }
  if (std::is_same<T, int32_t>::value) {
    return reinterpret_cast<__m256i>(filter.test8x32(values));
  }
  if (std::is_same<T, float>::value) {
    return reinterpret_cast<__m256i>(filter.test8xFloat(values));
  }
  if (std::is_same<T, int16_t>::value) {
    return reinterpret_cast<__m256i>(filter.test16x16(values));
  }
}

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
    __m256i values,
    int32_t width,
    int32_t firstRow,
    TFilter& filter,
    LoadIndices loadIndices,
    T* rawValues,
    int32_t* filterHits,
    int32_t& numValues) {
  using V32 = simd::Vectors<int32_t>;
  using TV = simd::Vectors<T>;
  constexpr bool is16 = sizeof(T) == 2;
  auto word = TV::compareBitMask(
      reinterpret_cast<typename TV::CompareType>(testSimd<T>(filter, values)));
  if (!word) {
    ; /* no values passed, no action*/
  } else if (word == simd::Vectors<T>::kAllTrue) {
    V32::store(filterHits + numValues, loadIndices(0));
    if (is16) {
      // If 16 values in 'values', copy the next 8x 32 bit indices.
      V32::store(filterHits + numValues + 8, loadIndices(1));
    }
    if (!filterOnly) {
      // 4, 8 or 16 values in 'values'.
      simd::Vectors<int64_t>::store(rawValues + numValues, values);
    }
    numValues += width;
  } else {
    auto allBits = word & bits::lowMask(width);
    auto bits = is16 ? allBits & 0xff : allBits;
    auto setBits = simd::Vectors<T>::compareSetBits(bits);
    auto numBits = __builtin_popcount(bits);
    if (dense && !scatter) {
      V32::store(filterHits + numValues, setBits + firstRow);
    } else {
      simd::storePermute(filterHits + numValues, loadIndices(0), setBits);
    }
    if (!filterOnly) {
      if (sizeof(T) == 4) {
        simd::storePermute(
            rawValues + numValues, reinterpret_cast<__m256si>(values), setBits);
      } else if (sizeof(T) == 8) {
        simd::storePermute(
            rawValues + numValues,
            (__m256si)values,
            V32::load(&simd::Vectors<int64_t>::permuteIndices()[bits]));
      } else if (is16) {
        simd::storePermute16<0>(rawValues + numValues, values, setBits);
      } else {
        VELOX_FAIL("Unsupported size");
      }
    }
    numValues += numBits;
    if (is16) {
      // Process the upper 8 compare results.
      firstRow += 8;
      bits = allBits >> 8;
      if (bits) {
        setBits = simd::Vectors<T>::compareSetBits(bits);
        numBits = __builtin_popcount(bits);
        if (dense && !scatter) {
          V32::store(filterHits + numValues, setBits + firstRow);
        } else {
          simd::storePermute(filterHits + numValues, loadIndices(1), setBits);
        }
        if (!filterOnly) {
          simd::storePermute16<1>(rawValues + numValues, values, setBits);
        }
        numValues += numBits;
      }
    }
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
    SeekableInputStream& input,
    const char*& bufferStart,
    const char*& bufferEnd,
    TFilter& filter,
    THook& hook) {
  constexpr int32_t kWidth = simd::Vectors<T>::VSize;
  constexpr bool is16 = sizeof(T) == 2;
  constexpr int32_t kStep = is16 ? 16 : 8;
  constexpr bool hasFilter = !std::is_same<TFilter, common::AlwaysTrue>::value;
  constexpr bool hasHook = !std::is_same<THook, NoHook>::value;
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
          if (common::applyFilter(filter, value)) {
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
                      simd::Vectors<T>::load(buffer + firstRow - rowOffset);
                  processFixedFilter<T, filterOnly, scatter, true>(
                      reinterpret_cast<__m256i>(values),
                      kWidth,
                      firstRow,
                      filter,
                      [&](int32_t offset) {
                        return simd::Vectors<T>::loadGather32Indices(
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
                auto indices =
                    simd::Vectors<T>::loadGather32Indices(rows + rowIndex);
                __m256i values;
                if (is16) {
                  values = reinterpret_cast<__m256i>(simd::gather16x32(
                      buffer - rowOffset, rows + rowIndex, 16));
                } else {
                  values = reinterpret_cast<__m256i>(
                      simd::Vectors<T>::gather32(buffer - rowOffset, indices));
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
                      simd::Vectors<int64_t>::store(
                          rawValues + numValues, values);
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
                          return simd::Vectors<T>::loadGather32Indices(
                              (scatter ? scatterRows : rows) + rowIndex +
                              8 * offset);
                        }
                        return scatter ? simd::Vectors<T>::loadGather32Indices(
                                             scatterRows + rowIndex)
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
                __m256i values;
                int width = std::min<int32_t>(kWidth, numRows - step);
                if (is16) {
                  values = reinterpret_cast<__m256i>(simd::gather16x32(
                      buffer - rowOffset, rows + rowIndex, numRows));
                } else {
                  auto indices =
                      simd::Vectors<T>::loadGather32Indices(rows + rowIndex);
                  if (width < kWidth) {
                    values = reinterpret_cast<__m256i>(
                        simd::Vectors<T>::maskGather32(
                            simd::Vectors<T>::setAll(0),
                            simd::Vectors<T>::leadingMask(width),
                            buffer - rowOffset,
                            indices));
                  } else {
                    values =
                        reinterpret_cast<__m256i>(simd::Vectors<T>::gather32(
                            buffer - rowOffset, indices));
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
                      simd::Vectors<int64_t>::store(
                          rawValues + numValues, values);
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
                        return simd::Vectors<T>::loadGather32Indices(
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
      (std::is_same<typename Visitor::FilterType, common::AlwaysTrue>::value ||
       !hasNulls || !visitor.allowNulls()) &&
      (std::is_same<typename Visitor::HookType, NoHook>::value || !hasNulls ||
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
  constexpr int32_t kWidth = simd::Vectors<T>::VSize;
  constexpr bool hasFilter = !std::is_same<TFilter, common::AlwaysTrue>::value;
  constexpr bool hasHook = !std::is_same<THook, NoHook>::value;
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
    auto valueVector = simd::Vectors<T>::load(values + valuesBegin + row);
    processFixedFilter<T, filterOnly, scatter, dense>(
        reinterpret_cast<__m256i>(valueVector),
        kWidth,
        rows[rowIndex + row],
        filter,
        [&](int32_t offset) {
          return simd::Vectors<T>::loadGather32Indices(
              (scatter ? scatterRows : rows.data()) + rowIndex + row +
              offset * 8);
        },
        values,
        filterHits,
        numValues);
  }
  if (numInput > end) {
    auto valueVector = simd::Vectors<T>::load(values + valuesBegin + row);
    processFixedFilter<T, filterOnly, scatter, dense>(
        reinterpret_cast<__m256i>(valueVector),
        numInput - end,
        rows[rowIndex + row],
        filter,
        [&](int32_t offset) {
          return simd::Vectors<T>::loadGather32Indices(
              (scatter ? scatterRows : rows.data()) + row + rowIndex +
              offset * 8);
        },
        values,
        filterHits,
        numValues);
  }
}

} // namespace facebook::velox::dwrf
