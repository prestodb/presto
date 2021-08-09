/*
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

#include "velox/functions/Macros.h"

namespace facebook::velox::functions::sparksql {

template <typename T>
VELOX_UDF_BEGIN(pmod)
FOLLY_ALWAYS_INLINE bool call(T& result, const T a, const T n) {
  if (UNLIKELY(n == 0)) {
    return false;
  }
  T r = a % n;
  result = (r > 0) ? r : (r + n) % n;
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(remainder)
FOLLY_ALWAYS_INLINE bool call(T& result, const T a, const T n) {
  if (UNLIKELY(n == 0)) {
    return false;
  }
  result = a % n;
  return true;
}
VELOX_UDF_END();

} // namespace facebook::velox::functions::sparksql
