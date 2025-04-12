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

namespace facebook::velox::functions::sparksql {

/// array_prepend(array(E), element) -> array(E)
/// Given an array and another element, append the element at the front of the
/// array.
template <typename TExec>
struct ArrayPrependFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  // Results refer to the first input strings parameter buffer.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // Fast path for primitives.
  template <typename Out, typename In, typename E>
  FOLLY_ALWAYS_INLINE bool callNullable(Out& out, const In* array, E* element) {
    if (array == nullptr) {
      return false;
    }
    out.reserve(array->size() + 1);
    element ? out.push_back(*element) : out.add_null();
    out.add_items(*array);
    return true;
  }

  // Generic implementation.
  FOLLY_ALWAYS_INLINE bool callNullable(
      out_type<Array<Generic<T1>>>& out,
      const arg_type<Array<Generic<T1>>>* array,
      const arg_type<Generic<T1>>* element) {
    if (array == nullptr) {
      return false;
    }
    out.reserve(array->size() + 1);
    element ? out.push_back(*element) : out.add_null();
    out.add_items(*array);
    return true;
  }
};

} // namespace facebook::velox::functions::sparksql
