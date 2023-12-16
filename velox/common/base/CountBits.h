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

namespace facebook::velox {

// Copied from format.h of fmt.
FOLLY_ALWAYS_INLINE int countDigits(__uint128_t n) {
  int count = 1;
  for (;;) {
    if (n < 10) {
      return count;
    }
    if (n < 100) {
      return count + 1;
    }
    if (n < 1000) {
      return count + 2;
    }
    if (n < 10000) {
      return count + 3;
    }
    n /= 10000u;
    count += 4;
  }
}

} // namespace facebook::velox
