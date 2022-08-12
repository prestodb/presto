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

#include "velox/type/UnscaledLongDecimal.h"

namespace std {

string to_string(facebook::velox::int128_t x) {
  if (x == 0) {
    return "0";
  }
  string ans;
  bool negative = x < 0;
  while (x != 0) {
    ans += '0' + abs(static_cast<int>(x % 10));
    x /= 10;
  }
  if (negative) {
    ans += '-';
  }
  reverse(ans.begin(), ans.end());
  return ans;
}

} // namespace std
