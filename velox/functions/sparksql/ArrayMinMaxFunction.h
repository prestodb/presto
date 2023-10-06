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

#include <cmath>
#include <type_traits>
#include "velox/functions/Udf.h"

namespace facebook::velox::functions::sparksql {

template <typename TExecCtx, bool isMax>
struct ArrayMinMaxFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecCtx);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  template <typename T>
  void update(T& currentValue, const T& candidateValue) {
    // NaN is greater than any non-NaN elements for double/float type.
    if constexpr (std::is_floating_point_v<T>) {
      if constexpr (isMax) {
        if (std::isnan(candidateValue) ||
            (!std::isnan(currentValue) && candidateValue > currentValue)) {
          currentValue = candidateValue;
        }
      } else {
        if (std::isnan(currentValue) ||
            (!std::isnan(candidateValue) && candidateValue < currentValue)) {
          currentValue = candidateValue;
        }
      }
      return;
    }

    if constexpr (isMax) {
      if (candidateValue > currentValue) {
        currentValue = candidateValue;
      }
    } else {
      if (candidateValue < currentValue) {
        currentValue = candidateValue;
      }
    }
  }

  template <typename T>
  void assign(T& out, const T& value) {
    out = value;
  }

  void assign(out_type<Varchar>& out, const arg_type<Varchar>& value) {
    out.setNoCopy(value);
  }

  template <typename TReturn, typename TInput>
  bool call(TReturn& out, const TInput& array) {
    // Result is null if array is empty.
    if (array.size() == 0) {
      return false;
    }

    if (!array.mayHaveNulls()) {
      // Input array does not have nulls.
      auto currentValue = *array[0];
      for (auto i = 1; i < array.size(); i++) {
        update(currentValue, array[i].value());
      }
      assign(out, currentValue);
      return true;
    }

    // Try to find the first non-null element.
    auto it = array.begin();
    while (it != array.end() && !it->has_value()) {
      ++it;
    }
    // If array contains only NULL elements, return NULL.
    if (it == array.end()) {
      return false;
    }

    // Now 'it' point to the first non-null element.
    auto currentValue = it->value();
    ++it;
    while (it != array.end()) {
      if (it->has_value()) {
        update(currentValue, it->value());
      }
      ++it;
    }

    assign(out, currentValue);
    return true;
  }
};

template <typename TExecCtx>
struct ArrayMinFunction : public ArrayMinMaxFunction<TExecCtx, false> {};

template <typename TExecCtx>
struct ArrayMaxFunction : public ArrayMinMaxFunction<TExecCtx, true> {};
} // namespace facebook::velox::functions::sparksql
