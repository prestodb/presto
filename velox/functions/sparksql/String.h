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

#include "folly/ssl/OpenSSLHash.h"

#include <codecvt>
#include <string>
#include "velox/expression/VectorFunction.h"
#include "velox/functions/Macros.h"
#include "velox/functions/UDFOutputString.h"
#include "velox/functions/lib/string/StringCore.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::velox::functions::sparksql {

template <typename T, bool lpad>
struct PadFunctionBase {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& string,
      const arg_type<int64_t>& size,
      const arg_type<Varchar>& padString) {
    stringImpl::pad<lpad, false /*isAscii*/>(result, string, size, padString);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& string,
      const arg_type<int64_t>& size) {
    stringImpl::pad<lpad, false /*isAscii*/>(result, string, size, {" "});
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& string,
      const arg_type<int64_t>& size,
      const arg_type<Varchar>& padString) {
    stringImpl::pad<lpad, true /*isAscii*/>(result, string, size, padString);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& string,
      const arg_type<int64_t>& size) {
    stringImpl::pad<lpad, true /*isAscii*/>(result, string, size, {" "});
  }
};

template <typename T>
struct LPadFunction : public PadFunctionBase<T, true> {};

template <typename T>
struct RPadFunction : public PadFunctionBase<T, false> {};

template <typename T>
struct AsciiFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Varchar>& s) {
    if (s.empty()) {
      result = 0;
      return;
    }
    int size;
    result = utf8proc_codepoint(s.data(), s.data() + s.size(), size);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      int32_t& result,
      const arg_type<Varchar>& s) {
    result = s.empty() ? 0 : s.data()[0];
  }
};

template <typename T>
struct BitLengthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(int32_t& result, TInput& input) {
    result = input.size() * 8;
  }
};

/// chr function
/// chr(n) -> string
/// Returns the Unicode code point ``n`` as a single character string.
/// If ``n < 0``, the result is an empty string.
/// If ``n >= 256``, the result is equivalent to chr(``n % 256``).
template <typename T>
struct ChrFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<Varchar>& result, int64_t n) {
    if (n < 0) {
      result.resize(0);
    } else {
      n = n & 0xFF;
      if (n < 0x80) {
        result.resize(1);
        result.data()[0] = n;
      } else {
        result.resize(2);
        result.data()[0] = 0xC0 + (n >> 6);
        result.data()[1] = 0x80 + (n & 0x3F);
      }
    }
  }
};

template <typename T>
struct Md5Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TTo, typename TFrom>
  FOLLY_ALWAYS_INLINE bool call(TTo& result, const TFrom& input) {
    stringImpl::md5_radix(result, input, 16);
    return true;
  }
};

std::vector<std::shared_ptr<exec::FunctionSignature>> instrSignatures();

std::shared_ptr<exec::VectorFunction> makeInstr(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

std::vector<std::shared_ptr<exec::FunctionSignature>> lengthSignatures();

std::shared_ptr<exec::VectorFunction> makeLength(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

/// Expands each char of the digest data to two chars,
/// representing the hex value of each digest char, in order.
/// Note: digestSize must be one-half of outputSize.
void encodeDigestToBase16(uint8_t* output, int digestSize);

/// sha1 function
/// sha1(varbinary) -> string
/// Calculate SHA-1 digest and convert the result to a hex string.
/// Returns SHA-1 digest as a 40-character hex string.
template <typename T>
struct Sha1HexStringFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<Varchar>& result, const arg_type<Varbinary>& input) {
    static const int kSha1Length = 20;
    result.resize(kSha1Length * 2);
    folly::ssl::OpenSSLHash::sha1(
        folly::MutableByteRange((uint8_t*)result.data(), kSha1Length),
        folly::ByteRange((const uint8_t*)input.data(), input.size()));
    encodeDigestToBase16((uint8_t*)result.data(), kSha1Length);
  }
};

/// sha2 function
/// sha2(varbinary, bitLength) -> string
/// Calculate SHA-2 family of functions (SHA-224, SHA-256,
/// SHA-384, and SHA-512) and convert the result to a hex string.
/// The second argument indicates the desired bit length of the result, which
/// must have a value of 224, 256, 384, 512, or 0 (which is equivalent to 256).
/// If asking for an unsupported bitLength, the return value is NULL.
/// Returns SHA-2 digest as hex string.
template <typename T>
struct Sha2HexStringFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  bool call(
      out_type<Varchar>& result,
      const arg_type<Varbinary>& input,
      const int32_t& bitLength) {
    const int32_t nonzeroBitLength = (bitLength == 0) ? 256 : bitLength;
    const EVP_MD* hashAlgorithm;
    switch (nonzeroBitLength) {
      case 224:
        hashAlgorithm = EVP_sha224();
        break;
      case 256:
        hashAlgorithm = EVP_sha256();
        break;
      case 384:
        hashAlgorithm = EVP_sha384();
        break;
      case 512:
        hashAlgorithm = EVP_sha512();
        break;
      default:
        // For an unsupported bitLength, the return value is NULL.
        return false;
    }
    const int32_t digestLength = nonzeroBitLength >> 3;
    result.resize(digestLength * 2);
    auto resultBuffer =
        folly::MutableByteRange((uint8_t*)result.data(), digestLength);
    auto inputBuffer =
        folly::ByteRange((const uint8_t*)input.data(), input.size());
    folly::ssl::OpenSSLHash::hash(resultBuffer, hashAlgorithm, inputBuffer);
    encodeDigestToBase16((uint8_t*)result.data(), digestLength);
    return true;
  }
};

/// contains function
/// contains(string, string) -> bool
/// Searches the second argument in the first one.
/// Returns true if it is found
template <typename T>
struct ContainsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<bool>& result,
      const arg_type<Varchar>& str,
      const arg_type<Varchar>& pattern) {
    result = std::string_view(str).find(std::string_view(pattern)) !=
        std::string_view::npos;
    return true;
  }
};

/// startsWith function
/// startsWith(string, string) -> bool
/// Returns true if the first string starts with the second string
template <typename T>
struct StartsWithFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<bool>& result,
      const arg_type<Varchar>& str,
      const arg_type<Varchar>& pattern) {
    auto str1 = std::string_view(str);
    auto str2 = std::string_view(pattern);
    // TODO: Once C++20 supported we may want to replace this with
    // string_view::starts_with

    if (str2.length() > str1.length()) {
      result = false;
    } else {
      result = str1.substr(0, str2.length()) == str2;
      ;
    }
    return true;
  }
};

/// endsWith function
/// endsWith(string, string) -> bool
/// Returns true if the first string ends with the second string
template <typename T>
struct EndsWithFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<bool>& result,
      const arg_type<Varchar>& str,
      const arg_type<Varchar>& pattern) {
    auto str1 = std::string_view(str);
    auto str2 = std::string_view(pattern);
    // TODO Once C++20 supported we may want to replace this with
    // string_view::ends_with
    if (str2.length() > str1.length()) {
      result = false;
    } else {
      result =
          str1.substr(str1.length() - str2.length(), str2.length()) == str2;
    }
    return true;
  }
};

/// Returns the substring from str before count occurrences of the delimiter
/// delim. If count is positive, everything to the left of the final delimiter
/// (counting from the left) is returned. If count is negative, everything to
/// the right of the final delimiter (counting from the right) is returned. The
/// function substring_index performs a case-sensitive match when searching for
/// delim.
template <typename T>
struct SubstringIndexFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& str,
      const arg_type<Varchar>& delim,
      const int32_t& count) {
    if (count == 0) {
      result.setEmpty();
      return;
    }

    int64_t index;
    if (count > 0) {
      index = stringImpl::stringPosition<true, true>(str, delim, count);
    } else {
      index = stringImpl::stringPosition<true, false>(str, delim, -count);
    }

    // If 'delim' is not found or found fewer than 'count' times,
    // return the input string directly.
    if (index == 0) {
      result.setNoCopy(str);
      return;
    }

    auto start = 0;
    auto length = str.size();
    const auto delimLength = delim.size();
    if (count > 0) {
      length = index - 1;
    } else {
      start = index + delimLength - 1;
      length -= start;
    }

    result.setNoCopy(StringView(str.data() + start, length));
  }
};

/// ltrim(trimStr, srcStr) -> varchar
///     Remove leading specified characters from srcStr. The specified character
///     is any character contained in trimStr.
/// rtrim(trimStr, srcStr) -> varchar
///     Remove trailing specified characters from srcStr. The specified
///     character is any character contained in trimStr.
/// trim(trimStr, srcStr) -> varchar
///     Remove leading and trailing specified characters from srcStr. The
///     specified character is any character contained in trimStr.
template <typename T, bool leftTrim, bool rightTrim>
struct TrimFunctionBase {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the second argument.
  static constexpr int32_t reuse_strings_from_arg = 1;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& trimStr,
      const arg_type<Varchar>& srcStr) {
    if (srcStr.empty()) {
      result.setEmpty();
      return;
    }
    if (trimStr.empty()) {
      result.setNoCopy(srcStr);
      return;
    }

    auto trimStrView = std::string_view(trimStr);
    size_t resultStartIndex = 0;
    if constexpr (leftTrim) {
      resultStartIndex =
          std::string_view(srcStr).find_first_not_of(trimStrView);
      if (resultStartIndex == std::string_view::npos) {
        result.setEmpty();
        return;
      }
    }

    size_t resultSize = srcStr.size() - resultStartIndex;
    if constexpr (rightTrim) {
      size_t lastIndex =
          std::string_view(srcStr.data() + resultStartIndex, resultSize)
              .find_last_not_of(trimStrView);
      if (lastIndex == std::string_view::npos) {
        result.setEmpty();
        return;
      }
      resultSize = lastIndex + 1;
    }

    result.setNoCopy(StringView(srcStr.data() + resultStartIndex, resultSize));
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& trimStr,
      const arg_type<Varchar>& srcStr) {
    if (srcStr.empty()) {
      result.setEmpty();
      return;
    }
    if (trimStr.empty()) {
      result.setNoCopy(srcStr);
      return;
    }

    auto trimStrView = std::string_view(trimStr);
    auto resultBegin = srcStr.begin();
    if constexpr (leftTrim) {
      while (resultBegin < srcStr.end()) {
        int charLen = utf8proc_char_length(resultBegin);
        auto c = std::string_view(resultBegin, charLen);
        if (trimStrView.find(c) == std::string_view::npos) {
          break;
        }
        resultBegin += charLen;
      }
    }

    auto resultEnd = srcStr.end();
    if constexpr (rightTrim) {
      auto curPos = resultEnd - 1;
      while (curPos >= resultBegin) {
        if (utf8proc_char_first_byte(curPos)) {
          auto c = std::string_view(curPos, resultEnd - curPos);
          if (trimStrView.find(c) == std::string_view::npos) {
            break;
          }
          resultEnd = curPos;
        }
        --curPos;
      }
    }

    result.setNoCopy(StringView(resultBegin, resultEnd - resultBegin));
  }
};

/// ltrim(srcStr) -> varchar
///     Removes leading 0x20(space) characters from srcStr.
/// rtrim(srcStr) -> varchar
///     Removes trailing 0x20(space) characters from srcStr.
/// trim(srcStr) -> varchar
///     Remove leading and trailing 0x20(space) characters from srcStr.
template <typename T, bool leftTrim, bool rightTrim>
struct TrimSpaceFunctionBase {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& srcStr) {
    // Because utf-8 and Ascii have the same space character code, both are
    // char=32. So trimAsciiSpace can be reused here.
    stringImpl::trimAscii<leftTrim, rightTrim>(
        result, srcStr, stringImpl::isAsciiSpace);
  }
};

template <typename T>
struct TrimFunction : public TrimFunctionBase<T, true, true> {};

template <typename T>
struct LTrimFunction : public TrimFunctionBase<T, true, false> {};

template <typename T>
struct RTrimFunction : public TrimFunctionBase<T, false, true> {};

template <typename T>
struct TrimSpaceFunction : public TrimSpaceFunctionBase<T, true, true> {};

template <typename T>
struct LTrimSpaceFunction : public TrimSpaceFunctionBase<T, true, false> {};

template <typename T>
struct RTrimSpaceFunction : public TrimSpaceFunctionBase<T, false, true> {};

/// substr(string, start) -> varchar
///
///     Returns the rest of string from the starting position start.
///     Positions start with 1. A negative starting position is interpreted as
///     being relative to the end of the string. When the starting position is
///     0, the meaning is to refer to the first character.

///
/// substr(string, start, length) -> varchar
///
///     Returns a substring from string of length length from the
///     starting position start. Positions start with 1. A negative starting
///     position is interpreted as being relative to the end of the string.
///     When the starting position is 0, the meaning is to refer to the
///     first character.
template <typename T>
struct SubstrFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      int32_t start,
      int32_t length = std::numeric_limits<int32_t>::max()) {
    doCall<false>(result, input, start, length);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      int32_t start,
      int32_t length = std::numeric_limits<int32_t>::max()) {
    doCall<true>(result, input, start, length);
  }

  template <bool isAscii>
  FOLLY_ALWAYS_INLINE void doCall(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      int32_t start,
      int32_t length = std::numeric_limits<int32_t>::max()) {
    if (length <= 0) {
      result.setEmpty();
      return;
    }
    // Following Spark semantics
    if (start == 0) {
      start = 1;
    }

    int32_t numCharacters = stringImpl::length<isAscii>(input);

    // negative starting position
    if (start < 0) {
      start = numCharacters + start + 1;
    }

    // Adjusting last
    int32_t last;
    bool lastOverflow = __builtin_add_overflow(start, length - 1, &last);
    if (lastOverflow || last > numCharacters) {
      last = numCharacters;
    }

    // Following Spark semantics
    if (start <= 0) {
      start = 1;
    }

    // Adjusting length
    length = last - start + 1;
    if (length <= 0) {
      result.setEmpty();
      return;
    }

    auto byteRange = stringCore::getByteRange<isAscii>(
        input.data(), input.size(), start, length);

    // Generating output string
    result.setNoCopy(StringView(
        input.data() + byteRange.first, byteRange.second - byteRange.first));
  }
};

struct OverlayFunctionBase {
  template <bool isAscii, bool isVarchar>
  FOLLY_ALWAYS_INLINE void doCall(
      exec::StringWriter<false>& result,
      StringView input,
      StringView replace,
      int32_t pos,
      int32_t len) {
    // Calculate and append first part.
    auto startAndLength = substring<isAscii, isVarchar>(input, 1, pos - 1);
    append<isAscii, isVarchar>(result, input, startAndLength);

    // Append second part.
    result.append(replace);

    // Calculate and append last part.
    int32_t length = 0;
    if (len >= 0) {
      length = len;
    } else {
      if constexpr (isVarchar && !isAscii) {
        length = stringImpl::lengthUnicode(replace.data(), replace.size());
      } else {
        length = replace.size();
      }
    }
    int64_t start = (int64_t)pos + (int64_t)length;
    startAndLength = substring<isAscii, isVarchar>(input, start, INT32_MAX);
    append<isAscii, isVarchar>(result, input, startAndLength);
  }

  template <bool isAscii, bool isVarchar>
  FOLLY_ALWAYS_INLINE void append(
      exec::StringWriter<false>& result,
      StringView input,
      std::pair<int32_t, int32_t> pair) {
    if constexpr (isVarchar && !isAscii) {
      auto byteRange = stringCore::getByteRange<false>(
          input.data(), input.size(), pair.first + 1, pair.second);
      result.append(StringView(
          input.data() + byteRange.first, byteRange.second - byteRange.first));
    } else {
      result.append(StringView(input.data() + pair.first, pair.second));
    }
  }

  // Information regarding the pos calculation:
  // Hive and SQL use one-based indexing for SUBSTR arguments but also accept
  // zero and negative indices for start positions. If a start index i is
  // greater than 0, it refers to element i-1 in the sequence. If a start index
  // i is less than 0, it refers to the -ith element before the end of the
  // sequence. If a start index i is 0, it refers to the first element. Return
  // pair of first indices and length.
  template <bool isAscii, bool isVarchar>
  FOLLY_ALWAYS_INLINE std::pair<int32_t, int32_t>
  substring(const StringView& input, const int64_t pos, const int64_t length) {
    int64_t len = 0;
    if constexpr (isVarchar && !isAscii) {
      len = stringImpl::lengthUnicode(input.data(), input.size());
    } else {
      len = input.size();
    }
    int64_t start = (pos > 0) ? pos - 1 : ((pos < 0) ? len + pos : 0);
    int64_t end = 0;
    if (start + length > INT32_MAX) {
      end = INT32_MAX;
    } else if (start + length < INT32_MIN) {
      end = INT32_MIN;
    } else {
      end = start + length;
    }

    if (end <= start || start >= len) {
      return std::make_pair(0, 0);
    }

    int64_t zero = 0;
    int32_t i = std::min(len, std::max(zero, start));
    int32_t j = std::min(len, std::max(zero, end));

    if (j > i) {
      return std::make_pair(i, j - i);
    } else {
      return std::make_pair(0, 0);
    }
  }
};

template <typename T>
struct OverlayVarcharFunction : public OverlayFunctionBase {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& replace,
      const int32_t pos,
      const int32_t len) {
    OverlayFunctionBase::doCall<false, true>(result, input, replace, pos, len);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& replace,
      const int32_t pos,
      const int32_t len) {
    OverlayFunctionBase::doCall<true, true>(result, input, replace, pos, len);
  }
};

template <typename T>
struct OverlayVarbinaryFunction : public OverlayFunctionBase {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varbinary>& result,
      const arg_type<Varbinary>& input,
      const arg_type<Varbinary>& replace,
      const int32_t pos,
      const int32_t len) {
    OverlayFunctionBase::doCall<false, false>(result, input, replace, pos, len);
  }
};

/// left function
/// left(string, length) -> string
/// Returns the leftmost length characters from the string
/// Return an empty string if length is less or equal than 0
template <typename T>
struct LeftFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      int32_t length) {
    doCall<false>(result, input, length);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      int32_t length) {
    doCall<true>(result, input, length);
  }

  template <bool isAscii>
  FOLLY_ALWAYS_INLINE void doCall(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      int32_t length) {
    if (length <= 0) {
      result.setEmpty();
      return;
    }

    int32_t numCharacters = stringImpl::length<isAscii>(input);

    if (length > numCharacters) {
      length = numCharacters;
    }

    int32_t start = 1;

    auto byteRange = stringCore::getByteRange<isAscii>(
        input.data(), input.size(), start, length);

    // Generating output string
    result.setNoCopy(StringView(
        input.data() + byteRange.first, byteRange.second - byteRange.first));
  }
};

/// translate(string, match, replace) -> varchar
///
///   Returns a new translated string. It translates the character in ``string``
///   by a character in ``replace``. The character in ``replace`` is
///   corresponding to the character in ``match``. The translation will
///   happen when any character in ``string`` matching with a character in
///   ``match``.
template <typename T>
struct TranslateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  std::optional<folly::F14FastMap<std::string, std::string>> unicodeDictionary_;
  std::optional<folly::F14FastMap<char, char>> asciiDictionary_;

  bool isConstantDictionary_ = false;

  folly::F14FastMap<std::string, std::string> buildUnicodeDictionary(
      const arg_type<Varchar>& match,
      const arg_type<Varchar>& replace) {
    folly::F14FastMap<std::string, std::string> dictionary;
    int i = 0;
    int j = 0;
    while (i < match.size()) {
      std::string replaceChar;
      // If match's character size is larger than replace's, the extra
      // characters in match will be removed from input string.
      if (j < replace.size()) {
        int replaceCharLength = utf8proc_char_length(replace.data() + j);
        replaceChar = std::string(replace.data() + j, replaceCharLength);
        j += replaceCharLength;
      }
      int matchCharLength = utf8proc_char_length(match.data() + i);
      std::string matchChar = std::string(match.data() + i, matchCharLength);
      // Only considers the first occurrence of a character in match.
      dictionary.emplace(matchChar, replaceChar);
      i += matchCharLength;
    }
    return dictionary;
  }

  folly::F14FastMap<char, char> buildAsciiDictionary(
      const arg_type<Varchar>& match,
      const arg_type<Varchar>& replace) {
    folly::F14FastMap<char, char> dictionary;
    int i = 0;
    for (; i < std::min(match.size(), replace.size()); i++) {
      char matchChar = *(match.data() + i);
      char replaceChar = *(replace.data() + i);
      // Only consider the first occurrence of a character in match.
      dictionary.emplace(matchChar, replaceChar);
    }
    for (; i < match.size(); i++) {
      char matchChar = *(match.data() + i);
      dictionary.emplace(matchChar, '\0');
    }
    return dictionary;
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* /*string*/,
      const arg_type<Varchar>* match,
      const arg_type<Varchar>* replace) {
    if (match != nullptr && replace != nullptr) {
      isConstantDictionary_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& match,
      const arg_type<Varchar>& replace) {
    if (!isConstantDictionary_ || !unicodeDictionary_.has_value()) {
      unicodeDictionary_ = buildUnicodeDictionary(match, replace);
    }
    // No need to do the translation.
    if (unicodeDictionary_->empty()) {
      result.append(input);
      return;
    }
    // Initial capacity is input size. Larger capacity can be reserved below.
    result.reserve(input.size());
    int i = 0;
    int k = 0;
    while (k < input.size()) {
      int inputCharLength = utf8proc_char_length(input.data() + k);
      auto inputChar = std::string(input.data() + k, inputCharLength);
      auto it = unicodeDictionary_->find(inputChar);
      if (it == unicodeDictionary_->end()) {
        // Final result size can be larger than the initial size (input size),
        // e.g., replace a ascii character with a longer utf8 character.
        result.reserve(i + inputCharLength);
        std::memcpy(result.data() + i, inputChar.data(), inputCharLength);
        i += inputCharLength;
      } else {
        result.reserve(i + it->second.size());
        std::memcpy(result.data() + i, it->second.data(), it->second.size());
        i += it->second.size();
      }
      k += inputCharLength;
    }
    result.resize(i);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& match,
      const arg_type<Varchar>& replace) {
    if (!isConstantDictionary_ || !asciiDictionary_.has_value()) {
      asciiDictionary_ = buildAsciiDictionary(match, replace);
    }
    // No need to do the translation.
    if (asciiDictionary_->empty()) {
      result.append(input);
      return;
    }
    // Result size cannot be larger than input size for all ascii input.
    result.reserve(input.size());
    int i = 0;
    for (int k = 0; k < input.size(); k++) {
      auto inputChar = *(input.data() + k);
      auto it = asciiDictionary_->find(inputChar);
      if (it == asciiDictionary_->end()) {
        std::memcpy(result.data() + i, &inputChar, 1);
        ++i;
      } else {
        if (it->second != '\0') {
          std::memcpy(result.data() + i, &(it->second), 1);
          ++i;
        }
      }
    }
    result.resize(i);
  }
};

template <typename T>
struct ConvFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_default_ascii_behavior = true;

  static const uint64_t kMaxUnsignedInt64_ = 0xFFFFFFFFFFFFFFFF;
  static const int kMinBase = 2;
  static const int kMaxBase = 36;

  static bool checkInput(StringView input, int32_t fromBase, int32_t toBase) {
    if (input.empty()) {
      return false;
    }
    // Consistent with spark, only supports fromBase belonging to [2, 36]
    // and toBase belonging to [2, 36] or [-36, -2].
    if (fromBase < kMinBase || fromBase > kMaxBase ||
        std::abs(toBase) < kMinBase || std::abs(toBase) > kMaxBase) {
      return false;
    }
    return true;
  }

  static int32_t skipLeadingSpaces(StringView input) {
    // Ignore leading spaces.
    int i = 0;
    for (; i < input.size(); i++) {
      if (input.data()[i] != ' ') {
        break;
      }
    }
    return i;
  }

  static uint64_t
  toUnsigned(StringView input, int32_t start, int32_t fromBase) {
    uint64_t unsignedValue;
    auto fromStatus = std::from_chars(
        input.data() + start,
        input.data() + input.size(),
        unsignedValue,
        fromBase);
    if (fromStatus.ec == std::errc::invalid_argument) {
      return 0;
    }
    if (fromStatus.ec == std::errc::result_out_of_range) {
      return kMaxUnsignedInt64_;
    }
    return unsignedValue;
  }

  static void toUpper(char* buffer, const int32_t size) {
    for (int i = 0; i < size; i++) {
      buffer[i] = std::toupper(buffer[i]);
    }
  }

  static std::pair<int64_t, int32_t> getSignedValueAndResultSize(
      uint64_t unsignedValue,
      bool isNegativeInput,
      int32_t toBase) {
    // This flag is used to make sure when we calculate the resultSize in
    // `toChars` we always get a positive number. It is due to the
    // `std::abs(min_int64)` would return a negative number.
    auto isMinInt64Num =
        unsignedValue == (uint64_t)std::numeric_limits<int64_t>::min();
    int64_t signedValue;
    int64_t absValue;
    if (isMinInt64Num) {
      signedValue = (int64_t)unsignedValue;
      // `std::abs(min_int64)` return a negative number, so here we set
      // absValue to max_int64 manually.
      absValue = std::numeric_limits<int64_t>::max();
    } else if (!isNegativeInput) {
      signedValue = (int64_t)unsignedValue;
      absValue = std::abs(signedValue);
    } else {
      signedValue = -std::abs((int64_t)unsignedValue);
      absValue = std::abs(signedValue);
    }
    int32_t resultSize =
        (int32_t)std::floor(std::log(absValue) / std::log(-toBase)) + 1;
    // Negative symbol is considered.
    if (signedValue < 0) {
      ++resultSize;
    }
    return std::make_pair(signedValue, resultSize);
  }

  static std::pair<uint64_t, int32_t> getUnsignedValueAndResultSize(
      uint64_t unsignedInput,
      bool isNegativeInput,
      int32_t toBase) {
    uint64_t unsignedValue = unsignedInput;
    if (isNegativeInput) {
      int64_t negativeInput = -std::abs((int64_t)unsignedValue);
      unsignedValue = (uint64_t)negativeInput;
    } // Here directly use unsignedValue if isNegativeInput = false.
    int32_t resultSize =
        (int32_t)std::floor(std::log(unsignedValue) / std::log(toBase)) + 1;
    return std::make_pair(unsignedValue, resultSize);
  }

  // For signed value, toBase is negative.
  static void toChars(
      out_type<Varchar>& result,
      int64_t signedValue,
      int32_t toBase,
      int32_t resultSize) {
    result.resize(resultSize);
    auto toStatus = std::to_chars(
        result.data(), result.data() + result.size(), signedValue, -toBase);
    result.resize(toStatus.ptr - result.data());
  }

  // For unsigned value, toBase is positive.
  static void toChars(
      out_type<Varchar>& result,
      uint64_t unsignedValue,
      int32_t toBase,
      int32_t resultSize) {
    result.resize(resultSize);
    auto toStatus = std::to_chars(
        result.data(),
        result.data() + result.size(),
        unsignedValue,
        std::abs(toBase));
    result.resize(toStatus.ptr - result.data());
  }

  bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      int32_t fromBase,
      int32_t toBase) {
    if (!checkInput(input, fromBase, toBase)) {
      return false;
    }

    auto i = skipLeadingSpaces(input);
    // All are spaces.
    if (i == input.size()) {
      return false;
    }
    const bool isNegativeInput = (input.data()[i] == '-');
    // Skips negative symbol.
    if (isNegativeInput) {
      ++i;
    }

    uint64_t unsignedInput = toUnsigned(input, i, fromBase);
    if (unsignedInput == 0) {
      result.append("0");
      return true;
    }

    // When toBase is negative, converts to signed value. Otherwise, converts to
    // unsigned value. Overflow is allowed, consistent with Spark.
    if (toBase < 0) {
      auto [signedValue, resultSize] =
          getSignedValueAndResultSize(unsignedInput, isNegativeInput, toBase);
      toChars(result, signedValue, toBase, resultSize);
    } else {
      auto [unsignedValue, resultSize] =
          getUnsignedValueAndResultSize(unsignedInput, isNegativeInput, toBase);
      toChars(result, unsignedValue, toBase, resultSize);
    }

    // Converts to uppper case, consistent with Spark.
    if (std::abs(toBase) > 10) {
      toUpper(result.data(), result.size());
    }
    return true;
  }
};

/// replace(input, replaced) -> varchar
///
///     Removes all instances of ``replaced`` from ``input``.
///     If ``replaced`` is an empty string, returns the original ``input``
///     string.

///
/// replace(input, replaced, replacement) -> varchar
///
///     Replaces all instances of ``replaced`` with ``replacement`` in
///     ``input``. If ``replaced`` is an empty string, returns the original
///     ``input`` string.
template <typename T>
struct ReplaceFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& replaced) {
    result.reserve(input.size());
    auto resultSize = stringCore::replace<true /*ignoreEmptyReplaced*/>(
        result.data(),
        std::string_view(input.data(), input.size()),
        std::string_view(replaced.data(), replaced.size()),
        std::string_view(),
        false);
    result.resize(resultSize);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& replaced,
      const arg_type<Varchar>& replacement) {
    size_t reserveSize = input.size();
    if (replaced.size() != 0 && replacement.size() > replaced.size()) {
      reserveSize = (input.size() / replaced.size()) * replacement.size() +
          input.size() % replaced.size();
    }
    result.reserve(reserveSize);
    auto resultSize = stringCore::replace<true /*ignoreEmptyReplaced*/>(
        result.data(),
        std::string_view(input.data(), input.size()),
        std::string_view(replaced.data(), replaced.size()),
        std::string_view(replacement.data(), replacement.size()),
        false);
    result.resize(resultSize);
  }
};

template <typename T>
struct FindInSetFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<int32_t>& result,
      const arg_type<Varchar>& str,
      const arg_type<Varchar>& strArray) {
    if (std::string_view(str).find(',') != std::string::npos) {
      result = 0;
      return;
    }

    int32_t index = 1;
    int32_t lastComma = -1;
    auto arrayData = strArray.data();
    auto matchData = str.data();
    size_t arraySize = strArray.size();
    size_t matchSize = str.size();

    for (int i = 0; i < arraySize; i++) {
      if (arrayData[i] == ',') {
        if (i - (lastComma + 1) == matchSize &&
            std::memcmp(arrayData + (lastComma + 1), matchData, matchSize) ==
                0) {
          result = index;
          return;
        }
        lastComma = i;
        index++;
      }
    }

    if (arraySize - (lastComma + 1) == matchSize &&
        std::memcmp(arrayData + (lastComma + 1), matchData, matchSize) == 0) {
      result = index;
      return;
    }

    result = 0;
    return;
  }
};

template <typename T>
struct SoundexFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_default_ascii_behavior = true;

  /// Soundex is a phonetic algorithm for indexing names by sound, for details,
  /// please see https://en.wikipedia.org/wiki/Soundex.
  void call(out_type<Varchar>& result, const arg_type<Varchar>& input) {
    size_t inputSize = input.size();
    if (inputSize == 0) {
      result.resize(0);
      return;
    }
    if (!std::isalpha(input.data()[0])) {
      // First character must be a letter, otherwise input is returned.
      result = input;
      return;
    }
    result.resize(4);
    result.data()[0] = std::toupper(input.data()[0]);
    int32_t soundexIndex = 1;
    int32_t dataIndex = result.data()[0] - 'A';
    char lastCode = kUSEnglishMapping[dataIndex];
    for (auto i = 1; i < inputSize; ++i) {
      if (!std::isalpha(input.data()[i])) {
        lastCode = '0';
        continue;
      }
      dataIndex = std::toupper(input.data()[i]) - 'A';
      char code = kUSEnglishMapping[dataIndex];
      if (code != '7') {
        if (code != '0' && code != lastCode) {
          result.data()[soundexIndex++] = code;
          if (soundexIndex > 3) {
            break;
          }
        }
        lastCode = code;
      }
    }
    for (; soundexIndex < 4; soundexIndex++) {
      result.data()[soundexIndex] = '0';
    }
  }

 private:
  // Soundex mapping table.
  static constexpr char kUSEnglishMapping[] = {
      '0', '1', '2', '3', '0', '1', '2', '7', '0', '2', '2', '4', '5',
      '5', '0', '1', '2', '6', '2', '3', '0', '1', '7', '2', '0', '2'};
};
} // namespace facebook::velox::functions::sparksql
