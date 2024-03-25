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

template <typename TExec>
struct GenericInPredicateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE bool callNullable(
      bool& result,
      const arg_type<Generic<T1>>* value,
      const arg_type<Variadic<Generic<T1>>>* inList) {
    if (value == nullptr || inList == nullptr) {
      return false; // NULL result.
    }

    if (inList->size() == 0) {
      return false;
    }

    bool hasNull = false;
    for (const auto& v : *inList) {
      if (v.has_value()) {
        if (*value == v) {
          result = true;
          return true; // Non-NULL result.
        }
      } else {
        hasNull = true;
      }
    }

    return !hasNull;
  }
};

} // namespace facebook::velox::functions
