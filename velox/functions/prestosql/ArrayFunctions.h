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

template <bool isMax, typename VeloxType>
VELOX_UDF_BEGIN(array_min_max)

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

void assign(out_type<VeloxType>& out, const arg_type<VeloxType>& value) {
  if constexpr (std::is_same<Varchar, VeloxType>::value) {
    // TODO: reuse strings once support landed.
    out.resize(value.size());
    if (value.size() != 0) {
      std::memcpy(out.data(), value.data(), value.size());
    }
  } else {
    out = value;
  }
}

FOLLY_ALWAYS_INLINE bool call(
    out_type<VeloxType>& out,
    const arg_type<Array<VeloxType>>& array) {
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

VELOX_UDF_END()

template <typename T>
inline void registerArrayMinMaxFunctions() {
  registerFunction<udf_array_min_max<false, T>, T, Array<T>>({"array_min"});
  registerFunction<udf_array_min_max<true, T>, T, Array<T>>({"array_max"});
}
} // namespace facebook::velox::functions
