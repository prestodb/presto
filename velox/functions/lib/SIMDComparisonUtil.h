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

#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions {

namespace detail {

template <typename T, bool kIsConstant>
inline auto loadSimdData(const T* rawData, vector_size_t offset) {
  using d_type = xsimd::batch<T>;
  if constexpr (kIsConstant) {
    return xsimd::broadcast<T>(rawData[0]);
  }
  return d_type::load_unaligned(rawData + offset);
}

inline uint64_t to64Bits(const int8_t* resultData) {
  using d_type = xsimd::batch<int8_t>;
  constexpr auto numScalarElements = d_type::size;
  static_assert(
      numScalarElements == 16 || numScalarElements == 32 ||
          numScalarElements == 64,
      "Unsupported number of scalar elements");
  uint64_t res = 0UL;
  if constexpr (numScalarElements == 64) {
    res = simd::toBitMask(xsimd::batch_bool<int8_t>(
        simd::reinterpretBatch<uint8_t>(d_type::load_unaligned(resultData))));
  } else if constexpr (numScalarElements == 32) {
    auto* addr = reinterpret_cast<uint32_t*>(&res);
    *(addr) = simd::toBitMask(xsimd::batch_bool<int8_t>(
        simd::reinterpretBatch<uint8_t>(d_type::load_unaligned(resultData))));
    *(addr + 1) = simd::toBitMask(
        xsimd::batch_bool<int8_t>(simd::reinterpretBatch<uint8_t>(
            d_type::load_unaligned(resultData + 32))));
  } else if constexpr (numScalarElements == 16) {
    auto* addr = reinterpret_cast<uint16_t*>(&res);
    *(addr) = simd::toBitMask(xsimd::batch_bool<int8_t>(
        simd::reinterpretBatch<uint8_t>(d_type::load_unaligned(resultData))));
    *(addr + 1) = simd::toBitMask(
        xsimd::batch_bool<int8_t>(simd::reinterpretBatch<uint8_t>(
            d_type::load_unaligned(resultData + 16))));
    *(addr + 2) = simd::toBitMask(
        xsimd::batch_bool<int8_t>(simd::reinterpretBatch<uint8_t>(
            d_type::load_unaligned(resultData + 32))));
    *(addr + 3) = simd::toBitMask(
        xsimd::batch_bool<int8_t>(simd::reinterpretBatch<uint8_t>(
            d_type::load_unaligned(resultData + 48))));
  }
  return res;
}

template <typename A, typename B, typename Compare>
void applyAutoSimdComparisonInternal(
    const SelectivityVector& rows,
    const A* __restrict rawA,
    const B* __restrict rawB,
    Compare cmp,
    VectorPtr& result) {
  int8_t tempBuffer[64];
  int8_t* __restrict resultData = tempBuffer;
  const vector_size_t rowsBegin = rows.begin();
  const vector_size_t rowsEnd = rows.end();
  auto* rowsData = reinterpret_cast<const uint64_t*>(rows.allBits());
  auto* resultVector = result->asUnchecked<FlatVector<bool>>();
  auto* rawResult = resultVector->mutableRawValues<uint64_t>();
  if (rows.isAllSelected()) {
    auto i = 0;
    for (; i + 64 <= rowsEnd; i += 64) {
      for (auto j = 0; j < 64; ++j) {
        resultData[j] = cmp(rawA, rawB, i + j) ? -1 : 0;
      }
      rawResult[i / 64] = to64Bits(resultData);
    }
    for (; i < rowsEnd; ++i) {
      bits::setBit(rawResult, i, cmp(rawA, rawB, i));
    }
  } else {
    static constexpr uint64_t kAllSet = -1ULL;
    bits::forEachWord(
        rowsBegin,
        rowsEnd,
        [&](int32_t idx, uint64_t mask) {
          auto word = rowsData[idx] & mask;
          if (!word) {
            return;
          }
          const size_t start = idx * 64;
          while (word) {
            auto index = start + __builtin_ctzll(word);
            bits::setBit(rawResult, index, cmp(rawA, rawB, index));
            word &= word - 1;
          }
        },
        [&](int32_t idx) {
          auto word = rowsData[idx];
          const size_t start = idx * 64;
          if (kAllSet == word) {
            // Do 64 comparisons in a batch, set results by SIMD.
            for (size_t row = 0; row < 64; ++row) {
              resultData[row] = cmp(rawA, rawB, row + start) ? -1 : 0;
            }
            rawResult[idx] = to64Bits(resultData);
          } else {
            while (word) {
              auto index = __builtin_ctzll(word);
              resultData[index] = cmp(rawA, rawB, start + index) ? -1 : 0;
              word &= word - 1;
            }
            // Set results only for selected rows.
            uint64_t mask = rowsData[idx];
            rawResult[idx] =
                (rawResult[idx] & ~mask) | (to64Bits(resultData) & mask);
          }
        });
  }
}
} // namespace detail

template <
    typename T,
    bool kIsLeftConstant,
    bool kIsRightConstant,
    typename ComparisonOp>
void applySimdComparison(
    const vector_size_t begin,
    const vector_size_t end,
    const T* rawLhs,
    const T* rawRhs,
    uint8_t* rawResult) {
  using d_type = xsimd::batch<T>;
  constexpr auto numScalarElements = d_type::size;
  const auto vectorEnd = (end - begin) - (end - begin) % numScalarElements;
  static_assert(
      numScalarElements == 2 || numScalarElements == 4 ||
          numScalarElements == 8 || numScalarElements == 16 ||
          numScalarElements == 32,
      "Unsupported number of scalar elements");
  if constexpr (numScalarElements == 2 || numScalarElements == 4) {
    for (auto i = begin; i < vectorEnd; i += 8) {
      rawResult[i / 8] = 0;
      for (auto j = 0; j < 8 && (i + j) < vectorEnd; j += numScalarElements) {
        auto left = detail::loadSimdData<T, kIsLeftConstant>(rawLhs, i + j);
        auto right = detail::loadSimdData<T, kIsRightConstant>(rawRhs, i + j);

        uint8_t res = simd::toBitMask(ComparisonOp()(left, right));
        rawResult[i / 8] |= res << j;
      }
    }
  } else {
    for (auto i = begin; i < vectorEnd; i += numScalarElements) {
      auto left = detail::loadSimdData<T, kIsLeftConstant>(rawLhs, i);
      auto right = detail::loadSimdData<T, kIsRightConstant>(rawRhs, i);

      auto res = simd::toBitMask(ComparisonOp()(left, right));
      if constexpr (numScalarElements == 8) {
        rawResult[i / 8] = res;
      } else if constexpr (numScalarElements == 16) {
        uint16_t* addr = reinterpret_cast<uint16_t*>(rawResult + i / 8);
        *addr = res;
      } else if constexpr (numScalarElements == 32) {
        uint32_t* addr = reinterpret_cast<uint32_t*>(rawResult + i / 8);
        *addr = res;
      }
    }
  }

  // Evaluate remaining values.
  for (auto i = vectorEnd; i < end; i++) {
    if constexpr (kIsRightConstant && kIsLeftConstant) {
      bits::setBit(rawResult, i, ComparisonOp()(rawLhs[0], rawRhs[0]));
    } else if constexpr (kIsRightConstant) {
      bits::setBit(rawResult, i, ComparisonOp()(rawLhs[i], rawRhs[0]));
    } else if constexpr (kIsLeftConstant) {
      bits::setBit(rawResult, i, ComparisonOp()(rawLhs[0], rawRhs[i]));
    } else {
      bits::setBit(rawResult, i, ComparisonOp()(rawLhs[i], rawRhs[i]));
    }
  }
}

template <typename T, typename ComparisonOp>
void applySimdComparison(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args,
    VectorPtr& result) {
  auto resultVector = result->asUnchecked<FlatVector<bool>>();
  auto rawResult = resultVector->mutableRawValues<uint8_t>();
  if (args[0]->isConstantEncoding() && args[1]->isConstantEncoding()) {
    auto l = args[0]->asUnchecked<ConstantVector<T>>()->valueAt(0);
    auto r = args[1]->asUnchecked<ConstantVector<T>>()->valueAt(0);
    applySimdComparison<T, true, true, ComparisonOp>(
        rows.begin(), rows.end(), &l, &r, rawResult);
  } else if (args[0]->isConstantEncoding()) {
    auto l = args[0]->asUnchecked<ConstantVector<T>>()->valueAt(0);
    auto rawRhs = args[1]->asUnchecked<FlatVector<T>>()->rawValues();
    applySimdComparison<T, true, false, ComparisonOp>(
        rows.begin(), rows.end(), &l, rawRhs, rawResult);
  } else if (args[1]->isConstantEncoding()) {
    auto rawLhs = args[0]->asUnchecked<FlatVector<T>>()->rawValues();
    auto r = args[1]->asUnchecked<ConstantVector<T>>()->valueAt(0);
    applySimdComparison<T, false, true, ComparisonOp>(
        rows.begin(), rows.end(), rawLhs, &r, rawResult);
  } else {
    auto rawLhs = args[0]->asUnchecked<FlatVector<T>>()->rawValues();
    auto rawRhs = args[1]->asUnchecked<FlatVector<T>>()->rawValues();
    applySimdComparison<T, false, false, ComparisonOp>(
        rows.begin(), rows.end(), rawLhs, rawRhs, rawResult);
  }
}

template <typename A, typename B, typename Compare, typename... Args>
void applyAutoSimdComparison(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args,
    VectorPtr& result,
    Args... cmpArgs) {
  const Compare cmp;
  if (args[0]->isFlatEncoding() && args[1]->isFlatEncoding()) {
    const A* __restrict rawA =
        args[0]->asUnchecked<FlatVector<A>>()->template rawValues<A>();
    const B* __restrict rawB =
        args[1]->asUnchecked<FlatVector<B>>()->template rawValues<B>();
    detail::applyAutoSimdComparisonInternal(
        rows,
        rawA,
        rawB,
        [&](const A* __restrict rawA, const B* __restrict rawB, int i) {
          if constexpr (sizeof...(cmpArgs) > 0) {
            return Compare::apply(rawA[i], rawB[i], cmpArgs...);
          } else {
            return cmp(rawA[i], rawB[i]);
          }
        },
        result);
  } else if (args[0]->isConstantEncoding() && args[1]->isFlatEncoding()) {
    const A constA = args[0]->asUnchecked<ConstantVector<A>>()->valueAt(0);
    const A* __restrict rawA = &constA;
    const B* __restrict rawB =
        args[1]->asUnchecked<FlatVector<B>>()->template rawValues<B>();
    detail::applyAutoSimdComparisonInternal(
        rows,
        rawA,
        rawB,
        [&](const A* __restrict rawA, const B* __restrict rawB, int i) {
          if constexpr (sizeof...(cmpArgs) > 0) {
            return Compare::apply(rawA[0], rawB[i], cmpArgs...);
          } else {
            return cmp(rawA[0], rawB[i]);
          }
        },
        result);
  } else if (args[0]->isFlatEncoding() && args[1]->isConstantEncoding()) {
    const A* __restrict rawA =
        args[0]->asUnchecked<FlatVector<A>>()->template rawValues<A>();
    const B constB = args[1]->asUnchecked<ConstantVector<B>>()->valueAt(0);
    const B* __restrict rawB = &constB;
    detail::applyAutoSimdComparisonInternal(
        rows,
        rawA,
        rawB,
        [&](const A* __restrict rawA, const B* __restrict rawB, int i) {
          if constexpr (sizeof...(cmpArgs) > 0) {
            return Compare::apply(rawA[i], rawB[0], cmpArgs...);
          } else {
            return cmp(rawA[i], rawB[0]);
          }
        },
        result);
  } else if (args[0]->isConstantEncoding() && args[1]->isConstantEncoding()) {
    const A constA = args[0]->asUnchecked<ConstantVector<A>>()->valueAt(0);
    const A* __restrict rawA = &constA;
    const B constB = args[1]->asUnchecked<ConstantVector<B>>()->valueAt(0);
    const B* __restrict rawB = &constB;
    detail::applyAutoSimdComparisonInternal(
        rows,
        rawA,
        rawB,
        [&](const A* __restrict rawA, const B* __restrict rawB, int i) {
          if constexpr (sizeof...(cmpArgs) > 0) {
            return Compare::apply(rawA[0], rawB[0], cmpArgs...);
          } else {
            return cmp(rawA[0], rawB[0]);
          }
        },
        result);
  } else {
    VELOX_UNREACHABLE();
  }
}
} // namespace facebook::velox::functions
