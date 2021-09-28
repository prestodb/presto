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
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions::sparksql {

// Comparison functions that implement SparkSQL semantics.
// Intended to be used with scalar types (including strings and timestamps).
template <typename T>
static inline bool isNan(const T& value) {
  if constexpr (std::is_floating_point<T>::value) {
    return std::isnan(value);
  } else {
    return false;
  }
}

template <typename T>
struct Less {
  constexpr bool operator()(const T& a, const T& b) const {
    if (isNan(a)) {
      return false;
    }
    if (isNan(b)) {
      return true;
    }
    return a < b;
  }
};

template <typename T>
struct Greater : private Less<T> {
  constexpr bool operator()(const T& a, const T& b) const {
    return Less<T>::operator()(b, a);
  }
};

template <typename T>
struct Equal {
  constexpr bool operator()(const T& a, const T& b) const {
    // In SparkSQL, NaN is defined to equal NaN.
    if (isNan(a)) {
      return isNan(b);
    }
    return a == b;
  }
};

} // namespace facebook::velox::functions::sparksql
