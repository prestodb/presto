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

#include "velox/functions/lib/string/StringImpl.h"
#include "velox/functions/sparksql/CharVarcharUtils.h"

namespace facebook::velox::functions::sparksql {

/// Ensures that an input string fits within the specified length `limit` in
/// characters. If the length of the input string is equal to `limit`, it is
/// returned as-is. If the length of the input string is less than `limit`, it
/// is padded with trailing spaces(0x20) to the length of `limit`. If the
/// length of the input string is greater than `limit`, trailing spaces are
/// trimmed to fit within `limit`. Throws an exception if the trimmed string
/// still exceeds `limit` or if `limit` is not a positive value.
template <typename T>
struct CharTypeWriteSideCheckFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      int32_t limit) {
    doCall<false>(result, input, limit);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      int32_t limit) {
    doCall<true>(result, input, limit);
  }

 private:
  template <bool isAscii>
  FOLLY_ALWAYS_INLINE void doCall(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      int32_t limit) {
    VELOX_USER_CHECK_GT(limit, 0, "The length limit must be greater than 0.");

    auto numCharacters = stringImpl::length<isAscii>(input);

    if (numCharacters == limit) {
      result.setNoCopy(input);
    } else if (numCharacters < limit) {
      // Rpad spaces(0x20) to limit.
      stringImpl::pad</*lpad=*/false, isAscii>(result, input, limit, {" "});
    } else {
      trimTrailingSpaces(result, input, numCharacters, limit);
    }
  }
};

} // namespace facebook::velox::functions::sparksql
