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

namespace facebook::velox::functions {

template <typename TExecCtx, bool isMax>
struct ArrayMinMaxFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecCtx);

  template <typename T>
  void update(T& currentValue, const T& candidateValue) {
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

  template <typename TReturn, typename TInput>
  void assign(TReturn& out, const TInput& value) {
    out = value;
  }

  void assign(out_type<Varchar>& out, const arg_type<Varchar>& value) {
    // TODO: reuse strings once support landed.
    out.resize(value.size());
    if (value.size() != 0) {
      std::memcpy(out.data(), value.data(), value.size());
    }
  }

  template <typename TReturn, typename TInput>
  FOLLY_ALWAYS_INLINE bool call(TReturn& out, const TInput& array) {
    // Result is null if array is empty.
    if (array.size() == 0) {
      return false;
    }

    if (!array.mayHaveNulls()) {
      // Input array does not have nulls.
      auto currentValue = *array[0];
      for (const auto& item : array) {
        update(currentValue, item.value());
      }
      assign(out, currentValue);
      return true;
    }

    auto it = array.begin();
    // Result is null if any element is null.
    if (!it->has_value()) {
      return false;
    }

    auto currentValue = it->value();
    it++;
    while (it != array.end()) {
      if (!it->has_value()) {
        return false;
      }
      update(currentValue, it->value());
      it++;
    }

    assign(out, currentValue);
    return true;
  }
};

template <typename TExecCtx>
struct ArrayMinFunction : public ArrayMinMaxFunction<TExecCtx, false> {};

template <typename TExecCtx>
struct ArrayMaxFunction : public ArrayMinMaxFunction<TExecCtx, true> {};

template <typename T>
inline void registerArrayMinMaxFunctions() {
  registerFunction<ArrayMinFunction, T, Array<T>>({"array_min"});
  registerFunction<ArrayMaxFunction, T, Array<T>>({"array_max"});
}

} // namespace facebook::velox::functions
