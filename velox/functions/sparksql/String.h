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

#include "velox/expression/VectorFunction.h"
#include "velox/functions/Macros.h"
#include "velox/functions/UDFOutputString.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::velox::functions::sparksql {

template <typename T>
struct AsciiFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(int32_t& result, const arg_type<Varchar>& s) {
    result = s.empty() ? 0 : s.data()[0];
    return true;
  }
};

template <typename T>
struct ChrFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<Varchar>& result, int64_t ord) {
    if (ord < 0) {
      result.resize(0);
    } else {
      result.resize(1);
      *result.data() = ord;
    }
    return true;
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
    const std::vector<exec::VectorFunctionArg>& inputArgs);

std::vector<std::shared_ptr<exec::FunctionSignature>> lengthSignatures();

std::shared_ptr<exec::VectorFunction> makeLength(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs);

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

  // Results refer to strings in the first argument.
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
    stringImpl::
        trimAsciiWhiteSpace<leftTrim, rightTrim, stringImpl::isAsciiSpace>(
            result, srcStr);
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

    auto byteRange =
        stringCore::getByteRange<isAscii>(input.data(), start, length);

    // Generating output string
    result.setNoCopy(StringView(
        input.data() + byteRange.first, byteRange.second - byteRange.first));
  }
};

} // namespace facebook::velox::functions::sparksql
