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

#include "velox/type/HugeInt.h"

namespace facebook::velox {

int128_t HugeInt::parse(const std::string& str) {
  int128_t result = 0;
  bool negative = false;
  size_t idx = 0;

  VELOX_CHECK(!str.empty(), "Empty string cannot be converted to int128_t.");

  for (; idx < str.length() && str.at(idx) == ' '; ++idx) {
  }

  if (idx < str.length() && str.at(idx) == '+') {
    ++idx;
  } else if (idx < str.length() && str.at(idx) == '-') {
    ++idx;
    negative = true;
  }

  int128_t max = std::numeric_limits<int128_t>::max();
  int128_t min = std::numeric_limits<int128_t>::min();
  for (; idx < str.size(); ++idx) {
    VELOX_CHECK(
        std::isdigit(str[idx]),
        "Invalid character {} in the string.",
        str[idx]);

    // Throw error if the result is out of the range of int128_t, and return the
    // result before computing the last digit if the digit string would be the
    // min or max value of int128_t to avoid the potential overflow issue making
    // it more robust.
    int128_t cur = str[idx] - '0';
    if ((result > max / 10)) {
      VELOX_FAIL(fmt::format("{} is out of range of int128_t", str));
    }

    int128_t num = cur - (max % 10);
    if (result == (max / 10)) {
      if (negative) {
        if (num > 1) {
          VELOX_FAIL(fmt::format("{} is out of range of int128_t", str));
        } else if (num == 1) {
          return min;
        }
      } else {
        if (num > 0) {
          VELOX_FAIL(fmt::format("{} is out of range of int128_t", str));
        } else if (num == 0) {
          return max;
        }
      }
    }

    result = result * 10 + cur;
  }

  return negative ? -result : result;
}
} // namespace facebook::velox

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
