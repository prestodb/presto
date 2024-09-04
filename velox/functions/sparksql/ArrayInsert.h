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

#include "velox/functions/Udf.h"

namespace facebook::velox::functions::sparksql {

/// array_insert(array(E), pos, E, bool) → array(E)
/// Places new element into index pos of the input array.
template <typename T>
struct ArrayInsert {
  VELOX_DEFINE_FUNCTION_TYPES(T)

  static constexpr int32_t kMaxNumberOfElements = 10'000;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Array<Generic<T1>>>* /*srcArray*/,
      const arg_type<int32_t>* /*pos*/,
      const arg_type<Generic<T1>>* /*item*/,
      const arg_type<bool>* legacyNegativeIndex) {
    if (legacyNegativeIndex == nullptr) {
      VELOX_USER_FAIL("Constant legacyNegativeIndex is expected.");
    }
    legacyNegativeIndex_ = *legacyNegativeIndex;
  }

  FOLLY_ALWAYS_INLINE bool callNullable(
      out_type<Array<Generic<T1>>>& out,
      const arg_type<Array<Generic<T1>>>* srcArray,
      const arg_type<int32_t>* pos,
      const arg_type<Generic<T1>>* item,
      const arg_type<bool>* /*legacyNegativeIndex*/) {
    if (srcArray == nullptr || pos == nullptr) {
      return false;
    }
    VELOX_USER_CHECK_NE(*pos, 0, "Array insert position should not be 0.");

    if (*pos > 0) {
      // Insert element into input array on the given position. Append nulls
      // after the original elements if the given position is above the input
      // array size.
      const int64_t newArrayLength =
          std::max((int64_t)srcArray->size() + 1, (int64_t)*pos);
      VELOX_USER_CHECK_LE(
          newArrayLength,
          kMaxNumberOfElements,
          "The size of result array must be less than or equal to {}.",
          kMaxNumberOfElements);

      out.reserve(newArrayLength);
      int32_t posIdx = *pos - 1;
      int32_t nextIdx = 0;
      for (int32_t i = 0; i < newArrayLength; i++) {
        if (i == posIdx) {
          item ? out.push_back(*item) : out.add_null();
        } else {
          // If inserted, i is large than the source index by 1.
          int32_t srcIdx = i > posIdx ? i - 1 : i;
          if (srcIdx < srcArray->size()) {
            out.push_back((*srcArray)[srcIdx]);
          } else {
            out.add_null();
          }
        }
      }
    } else {
      bool newPosExtendsArrayLeft = -(int64_t)(*pos) > srcArray->size();
      if (newPosExtendsArrayLeft) {
        // Insert element at the beginning of the input array followed by nulls
        // and the original array. If legacyNegativeIndex_ is true, the new
        // array size is larger by 1.
        int64_t newArrayLength = -(int64_t)(*pos) + legacyNegativeIndex_;
        VELOX_USER_CHECK_LE(
            newArrayLength,
            kMaxNumberOfElements,
            "The size of result array must be less than or equal to {}.",
            kMaxNumberOfElements);

        out.reserve(newArrayLength);
        item ? out.push_back(*item) : out.add_null();
        int64_t nullsToFill = newArrayLength - 1 - srcArray->size();
        while (nullsToFill > 0) {
          out.add_null();
          nullsToFill--;
        }
        for (const auto& element : *srcArray) {
          out.push_back(element);
        }
      } else {
        // Insert element into input array on the calculated positive position.
        // When legacyNegativeIndex_ is true, the inserting position is more to
        // the left by 1.
        int64_t posIdx = *pos + srcArray->size() + !legacyNegativeIndex_;
        int64_t newArrayLength = (int64_t)srcArray->size() + 1;
        VELOX_USER_CHECK_LE(
            newArrayLength,
            kMaxNumberOfElements,
            "The size of result array must be less than or equal to {}.",
            kMaxNumberOfElements);

        out.reserve(newArrayLength);
        int32_t nextIdx = 0;
        for (const auto& element : *srcArray) {
          if (nextIdx == posIdx) {
            item ? out.push_back(*item) : out.add_null();
            nextIdx++;
          }
          out.push_back(element);
          nextIdx++;
        }
        if (nextIdx < newArrayLength) {
          item ? out.push_back(*item) : out.add_null();
        }
      }
    }

    return true;
  }

 private:
  // If true, -1 points to the last but one position. Otherwise, -1 points to
  // the last position.
  bool legacyNegativeIndex_;
};
} // namespace facebook::velox::functions::sparksql
