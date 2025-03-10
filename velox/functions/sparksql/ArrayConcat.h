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

/// concat(array1, array2, ..., arrayN) → array
/// Concatenates the arrays array1, array2, ..., arrayN. This function
/// provides the same functionality as the SQL-standard concatenation
/// operator (||).
template <typename TExec, typename T>
struct ArrayConcatFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec)

  static constexpr int32_t kMaxElements = INT32_MAX - 15;

  void call(
      out_type<Array<T>>& out,
      const arg_type<Variadic<Array<T>>>& arrays) {
    size_t elementCount = 0;
    for (const auto& array : arrays) {
      elementCount += array.value().size();
    }
    VELOX_USER_CHECK_LE(
        elementCount,
        kMaxElements,
        "Unsuccessful try to concat arrays with {} elements due to exceeding the array size limit {}.",
        elementCount,
        kMaxElements);
    out.reserve(elementCount);
    for (const auto& array : arrays) {
      out.add_items(array.value());
    }
  }
};

} // namespace facebook::velox::functions::sparksql
