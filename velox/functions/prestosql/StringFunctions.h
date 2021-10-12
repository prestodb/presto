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

#define XXH_INLINE_ALL

#include "velox/external/xxhash.h"
#include "velox/functions/Udf.h"
#include "velox/functions/lib/string/StringCore.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::velox::functions {

/// chr(n) → varchar
/// Returns the Unicode code point n as a single character string.
VELOX_UDF_BEGIN(chr)
FOLLY_ALWAYS_INLINE bool call(
    out_type<Varchar>& result,
    const int64_t& codePoint) {
  stringImpl::codePointToString(result, codePoint);
  return true;
}
VELOX_UDF_END();

/// codepoint(string) → integer
/// Returns the Unicode code point of the only character of string.
VELOX_UDF_BEGIN(codepoint)
FOLLY_ALWAYS_INLINE bool call(
    int32_t& result,
    const arg_type<Varchar>& inputChar) {
  result = stringImpl::charToCodePoint(inputChar);
  return true;
}
VELOX_UDF_END();

/// xxhash64(varbinary) → varbinary
/// Return an 8-byte binary to hash64 of input (varbinary such as string)
VELOX_UDF_BEGIN(xxhash64)
FOLLY_ALWAYS_INLINE
bool call(out_type<Varbinary>& result, const arg_type<Varbinary>& input) {
  // Seed is set to 0.
  int64_t hash = XXH64(input.data(), input.size(), 0);
  static const auto kLen = sizeof(int64_t);

  // Resizing output and copy
  result.resize(kLen);
  std::memcpy(result.data(), &hash, kLen);
  return true;
}
VELOX_UDF_END();

/// md5(varbinary) → varbinary
template <typename To, typename From>
VELOX_UDF_BEGIN(md5)
FOLLY_ALWAYS_INLINE
    bool call(out_type<To>& result, const arg_type<From>& input) {
  return stringImpl::md5(result, input);
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(to_hex)
FOLLY_ALWAYS_INLINE bool call(
    out_type<Varbinary>& result,
    const arg_type<Varchar>& input) {
  return stringImpl::toHex(result, input);
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(from_hex)
FOLLY_ALWAYS_INLINE bool call(
    out_type<Varchar>& result,
    const arg_type<Varbinary>& input) {
  return stringImpl::fromHex(result, input);
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(to_base64)
FOLLY_ALWAYS_INLINE bool call(
    out_type<Varchar>& result,
    const arg_type<Varbinary>& input) {
  return stringImpl::toBase64(result, input);
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(from_base64)
FOLLY_ALWAYS_INLINE bool call(
    out_type<Varbinary>& result,
    const arg_type<Varchar>& input) {
  return stringImpl::fromBase64(result, input);
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(url_encode)
FOLLY_ALWAYS_INLINE bool call(
    out_type<Varchar>& result,
    const arg_type<Varbinary>& input) {
  return stringImpl::urlEscape(result, input);
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(url_decode)
FOLLY_ALWAYS_INLINE bool call(
    out_type<Varchar>& result,
    const arg_type<Varbinary>& input) {
  return stringImpl::urlUnescape(result, input);
}
VELOX_UDF_END();

/// substr(string, start) -> varchar
///
///     Returns the rest of string from the starting position start.
///     Positions start with 1. A negative starting position is interpreted as
///     being relative to the end of the string.
///
/// substr(string, start, length) -> varchar
///
///     Returns a substring from string of length length from the
///     starting position start. Positions start with 1. A negative starting
///     position is interpreted as being relative to the end of the string.
template <typename I>
VELOX_UDF_BEGIN(substr)

// Results refer to strings in the first argument.
static constexpr int32_t reuse_strings_from_arg = 0;

// ASCII input always produces ASCII result.
static constexpr bool is_default_ascii_behavior = true;

FOLLY_ALWAYS_INLINE bool call(
    out_type<Varchar>& result,
    const arg_type<Varchar>& input,
    I start,
    I length = std::numeric_limits<I>::max()) {
  return doCall<false>(result, input, start, length);
}

FOLLY_ALWAYS_INLINE bool callAscii(
    out_type<Varchar>& result,
    const arg_type<Varchar>& input,
    I start,
    I length = std::numeric_limits<I>::max()) {
  return doCall<true>(result, input, start, length);
}

template <bool isAscii>
FOLLY_ALWAYS_INLINE bool doCall(
    out_type<Varchar>& result,
    const arg_type<Varchar>& input,
    I start,
    I length = std::numeric_limits<I>::max()) {
  // Following Presto semantics
  if (start == 0) {
    result.setEmpty();
    return true;
  }

  I numCharacters = stringImpl::length<isAscii>(input);

  // Adjusting start
  if (start < 0) {
    start = numCharacters + start + 1;
  }

  // Following Presto semantics
  if (start <= 0 || start > numCharacters || length <= 0) {
    result.setEmpty();
    return true;
  }

  // Adjusting length
  if (length == std::numeric_limits<I>::max() ||
      length + start - 1 > numCharacters) {
    // set length to the max valid length
    length = numCharacters - start + 1;
  }

  auto byteRange =
      stringCore::getByteRange<isAscii>(input.data(), start, length);

  // Generating output string
  result.setNoCopy(StringView(
      input.data() + byteRange.first, byteRange.second - byteRange.first));
  return true;
}
VELOX_UDF_END();

/// Trim functions
/// ltrim(string) -> varchar
///    Removes leading whitespaces from the string.
/// rtrim(string) -> varchar
///    Removes trailing whitespaces from the string.
/// trim(string) -> varchar
///    Removes leading and trailing whitespaces from the string.
template <bool leftTrim, bool rightTrim>
VELOX_UDF_BEGIN(trim)

// Results refer to strings in the first argument.
static constexpr int32_t reuse_strings_from_arg = 0;

// ASCII input always produces ASCII result.
static constexpr bool is_default_ascii_behavior = true;

FOLLY_ALWAYS_INLINE bool call(
    out_type<Varchar>& result,
    const arg_type<Varchar>& input) {
  stringImpl::trimUnicodeWhiteSpace<leftTrim, rightTrim>(result, input);
  return true;
}

FOLLY_ALWAYS_INLINE bool callAscii(
    out_type<Varchar>& result,
    const arg_type<Varchar>& input) {
  stringImpl::trimAsciiWhiteSpace<leftTrim, rightTrim>(result, input);
  return true;
}
VELOX_UDF_END();

} // namespace facebook::velox::functions
