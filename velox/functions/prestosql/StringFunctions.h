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
#include <xxhash.h>

#include "velox/functions/Udf.h"
#include "velox/functions/lib/string/StringCore.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::velox::functions {

/// chr(n) → varchar
/// Returns the Unicode code point n as a single character string.
template <typename T>
struct ChrFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const int64_t& codePoint) {
    stringImpl::codePointToString(result, codePoint);
    return true;
  }
};

/// codepoint(string) → integer
/// Returns the Unicode code point of the only character of string.
template <typename T>
struct CodePointFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      int32_t& result,
      const arg_type<Varchar>& inputChar) {
    result = stringImpl::charToCodePoint(inputChar);
    return true;
  }
};

/// xxhash64(varbinary) → varbinary
/// Return an 8-byte binary to hash64 of input (varbinary such as string)
template <typename T>
struct XxHash64Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

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
};

/// md5(varbinary) → varbinary
template <typename T>
struct Md5Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TTo, typename TFrom>
  FOLLY_ALWAYS_INLINE bool call(TTo& result, const TFrom& input) {
    return stringImpl::md5(result, input);
  }
};

/// sha256(varbinary) -> varbinary
template <typename T>
struct Sha256Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TTo, typename TFrom>
  FOLLY_ALWAYS_INLINE bool call(TTo& result, const TFrom& input) {
    return stringImpl::sha256(result, input);
  }
};

/// sha512(varbinary) -> varbinary
template <typename T>
struct Sha512Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TTo, typename TFrom>
  FOLLY_ALWAYS_INLINE void call(TTo& result, const TFrom& input) {
    stringImpl::sha512(result, input);
  }
};

template <typename T>
struct ToHexFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varbinary>& result,
      const arg_type<Varchar>& input) {
    return stringImpl::toHex(result, input);
  }
};

template <typename T>
struct FromHexFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varbinary>& input) {
    return stringImpl::fromHex(result, input);
  }
};

template <typename T>
struct ToBase64Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varbinary>& input) {
    return stringImpl::toBase64(result, input);
  }
};

template <typename T>
struct FromBase64Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varbinary>& result,
      const arg_type<Varchar>& input) {
    return stringImpl::fromBase64(result, input);
  }
};

template <typename T>
struct UrlEncodeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varbinary>& input) {
    return stringImpl::urlEscape(result, input);
  }
};

template <typename T>
struct UrlDecodeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varbinary>& input) {
    return stringImpl::urlUnescape(result, input);
  }
};

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
template <typename T>
struct SubstrFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  template <typename I>
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      I start,
      I length = std::numeric_limits<I>::max()) {
    return doCall<false>(result, input, start, length);
  }

  template <typename I>
  FOLLY_ALWAYS_INLINE bool callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      I start,
      I length = std::numeric_limits<I>::max()) {
    return doCall<true>(result, input, start, length);
  }

  template <bool isAscii, typename I>
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
};

/// Trim functions
/// ltrim(string) -> varchar
///    Removes leading whitespaces from the string.
/// rtrim(string) -> varchar
///    Removes trailing whitespaces from the string.
/// trim(string) -> varchar
///    Removes leading and trailing whitespaces from the string.
template <typename T, bool leftTrim, bool rightTrim>
struct TrimFunctionBase {
  VELOX_DEFINE_FUNCTION_TYPES(T);

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
};

template <typename T>
struct TrimFunction : public TrimFunctionBase<T, true, true> {};

template <typename T>
struct LTrimFunction : public TrimFunctionBase<T, true, false> {};

template <typename T>
struct RTrimFunction : public TrimFunctionBase<T, false, true> {};

/// Returns number of characters in the specified string.
template <typename T>
struct LengthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(int64_t& result, const StringView& input) {
    result = stringImpl::length<false>(input);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool callAscii(int64_t& result, const StringView& input) {
    result = stringImpl::length<true>(input);
    return true;
  }
};

/// Pad functions
/// lpad(string, size, padString) → varchar
///     Left pads string to size characters with padString.  If size is
///     less than the length of string, the result is truncated to size
///     characters.  size must not be negative and padString must be non-empty.
/// rpad(string, size, padString) → varchar
///     Right pads string to size characters with padString.  If size is
///     less than the length of string, the result is truncated to size
///     characters.  size must not be negative and padString must be non-empty.
template <typename T, bool lpad>
struct PadFunctionBase {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& string,
      const arg_type<int64_t>& size,
      const arg_type<Varchar>& padString) {
    stringImpl::pad<lpad, false /*isAscii*/>(result, string, size, padString);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& string,
      const arg_type<int64_t>& size,
      const arg_type<Varchar>& padString) {
    stringImpl::pad<lpad, true /*isAscii*/>(result, string, size, padString);
    return true;
  }
};

template <typename T>
struct LPadFunction : public PadFunctionBase<T, true> {};

template <typename T>
struct RPadFunction : public PadFunctionBase<T, false> {};

} // namespace facebook::velox::functions
