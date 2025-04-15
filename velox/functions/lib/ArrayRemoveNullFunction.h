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

#include "velox/functions/Macros.h"

namespace facebook::velox::functions {

/// DEFINITION:
/// Presto: remove_nulls(array(E)) -> array(E)
/// Spark: array_compact(array(E)) -> array(E)
///
/// Removes all NULL elements from the input array. Returns empty array if the
/// input array is empty or all elements in it are NULL.
template <typename T>
struct ArrayRemoveNullFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Fast path for primitives.
  template <typename Out, typename In>
  FOLLY_ALWAYS_INLINE void call(Out& out, const In& inputArray) {
    if (inputArray.mayHaveNulls()) {
      for (const auto& value : inputArray.skipNulls()) {
        out.push_back(value);
      }
      return;
    }

    // No nulls, skip reading nullity.
    for (const auto& value : inputArray) {
      out.push_back(value);
    }
  }

  // Generic implementation.
  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<Generic<T1>>>& out,
      const arg_type<Array<Generic<T1>>>& inputArray) {
    if (inputArray.mayHaveNulls()) {
      for (const auto& value : inputArray.skipNulls()) {
        out.push_back(value);
      }
      return;
    }

    // No nulls, skip reading nullity.
    for (const auto& value : inputArray) {
      out.push_back(value);
    }
  }
};

/// Removes all NULL elements from the input string array. Returns empty string
/// array if the input string array is empty or all elements in it are NULL.
/// Optimised by avoiding copy of strings.
template <typename T>
struct ArrayRemoveNullFunctionString {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr int32_t reuse_strings_from_arg = 0;

  // String version that avoids copy of strings.
  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<Varchar>>& out,
      const arg_type<Array<Varchar>>& inputArray) {
    if (inputArray.mayHaveNulls()) {
      for (const auto& element : inputArray.skipNulls()) {
        auto& newItem = out.add_item();
        newItem.setNoCopy(element);
      }
      return;
    }

    // No nulls, skip reading nullity.
    for (const auto& element : inputArray) {
      auto& newItem = out.add_item();
      newItem.setNoCopy(element.value());
    }
  }
};

} // namespace facebook::velox::functions
