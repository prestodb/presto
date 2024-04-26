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

/// flatten(array(array(E))) → array(E)
/// Flattens nested array by concatenating the contained arrays.
template <typename T>
struct ArrayFlattenFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T)

  // INT_MAX - 15, keep the same limit with spark.
  static constexpr int32_t kMaxNumberOfElements = 2147483632;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Array<Generic<T1>>>& out,
      const arg_type<Array<Array<Generic<T1>>>>& arrays) {
    int64_t elementCount = 0;
    for (const auto& array : arrays) {
      if (array.has_value()) {
        elementCount += array.value().size();
      } else {
        // Return NULL if any of the nested arrays is NULL.
        return false;
      }
    }

    VELOX_USER_CHECK_LE(
        elementCount,
        kMaxNumberOfElements,
        "array flatten result exceeds the max array size limit {}",
        kMaxNumberOfElements);

    out.reserve(elementCount);
    for (const auto& array : arrays) {
      out.add_items(array.value());
    };
    return true;
  }
};
} // namespace facebook::velox::functions::sparksql
