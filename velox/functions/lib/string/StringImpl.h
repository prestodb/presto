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

#include <assert.h>
#include <fmt/format.h>
#include <stdio.h>
#include <stdlib.h>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>
#include "folly/CPortability.h"
#include "folly/Likely.h"
#include "folly/ssl/OpenSSLHash.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/encode/Base64.h"
#include "velox/external/md5/md5.h"
#include "velox/functions/lib/string/StringCore.h"
#include "velox/type/StringView.h"

namespace facebook::velox::functions::stringImpl {
using namespace stringCore;

/// Perform upper for a UTF8 string
template <bool ascii, typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE bool upper(TOutString& output, const TInString& input) {
  if constexpr (ascii) {
    output.resize(input.size());
    upperAscii(output.data(), input.data(), input.size());
  } else {
    output.resize(input.size() * 4);
    auto size =
        upperUnicode(output.data(), output.size(), input.data(), input.size());
    output.resize(size);
  }
  return true;
}

/// Perform lower for a UTF8 string
template <bool ascii, typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE bool lower(TOutString& output, const TInString& input) {
  if constexpr (ascii) {
    output.resize(input.size());
    lowerAscii(output.data(), input.data(), input.size());
  } else {
    output.resize(input.size() * 4);
    auto size =
        lowerUnicode(output.data(), output.size(), input.data(), input.size());
    output.resize(size);
  }
  return true;
}

/// Inplace ascii lower
template <typename T>
FOLLY_ALWAYS_INLINE bool lowerAsciiInPlace(T& str) {
  lowerAscii(str.data(), str.data(), str.size());
  return true;
}

/// Inplace ascii upper
template <typename T>
FOLLY_ALWAYS_INLINE bool upperAsciiInPlace(T& str) {
  upperAscii(str.data(), str.data(), str.size());
  return true;
}

/// Apply a set of appenders on an output string, an appender is a lambda
/// that takes an output string and append a string to it. This can be used by
/// code-gen to reduce copying in concat by evaluating nested expressions
/// in place ex concat(lower(..), upper(..))
template <typename TOutString, typename... Funcs>
void concatLazy(TOutString& output, Funcs... funcs) {
  applyAppendersRecursive(output, funcs...);
}

/// Concat function that operates on a variadic number of string arguments, the
/// number is known at compile time
template <typename TOutString, typename... Args>
void concatStatic(TOutString& output, const Args&... inputs) {
  concatLazy(output, [&](TOutString& out) {
    if (inputs.size() != 0) {
      auto writeOffset = out.size();
      out.resize(out.size() + inputs.size());
      std::memcpy(out.data() + writeOffset, inputs.data(), inputs.size());
    }
  }...);
}

/// Return length of the input string in chars
template <bool isAscii, typename T>
FOLLY_ALWAYS_INLINE int64_t length(const T& input) {
  if constexpr (isAscii) {
    return input.size();
  } else {
    return lengthUnicode(input.data(), input.size());
  }
}

/// Write the Unicode codePoint as string to the output string. The function
/// behavior is undefined when code point it invalid. Implements the logic of
/// presto chr function.
template <typename TOutString>
FOLLY_ALWAYS_INLINE void codePointToString(
    TOutString& output,
    const int64_t codePoint) {
  auto validCodePoint =
      codePoint <= INT32_MAX && utf8proc_codepoint_valid(codePoint);
  VELOX_USER_CHECK(
      validCodePoint, "Not a valid Unicode code point: {}", codePoint);

  output.reserve(4);
  auto size = utf8proc_encode_char(
      codePoint, reinterpret_cast<unsigned char*>(output.data()));

  output.resize(size);
}

/// Returns the Unicode code point of the first char in a single char input
/// string. Implements the logic of presto codepoint function.
template <typename T>
FOLLY_ALWAYS_INLINE int32_t charToCodePoint(const T& inputString) {
  auto length = stringImpl::length</*isAscii*/ false>(inputString);
  VELOX_USER_CHECK_EQ(
      length,
      1,
      "Unexpected parameters (varchar({})) for function codepoint. Expected: codepoint(varchar(1))",
      length);

  int size;
  auto codePoint = utf8proc_codepoint(inputString.data(), size);
  return codePoint;
}

/// Returns the starting position in characters of the Nth instance of the
/// substring in string. Positions start with 1. If not found, 0 is returned. If
/// subString is empty result is 1.
template <bool isAscii, typename T>
FOLLY_ALWAYS_INLINE int64_t
stringPosition(const T& string, const T& subString, int64_t instance = 0) {
  if (subString.size() == 0) {
    return 1;
  }

  VELOX_USER_CHECK_GT(instance, 0, "'instance' must be a positive number");

  auto byteIndex = findNthInstanceByteIndex(
      std::string_view(string.data(), string.size()),
      std::string_view(subString.data(), subString.size()),
      instance);

  if (byteIndex == -1) {
    return 0;
  }

  // Return the number of characters from the beginning of the string to
  // byteIndex.
  return length<isAscii>(std::string_view(string.data(), byteIndex)) + 1;
}

/// Replace replaced with replacement in inputString and write results to
/// outputString.
template <typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE void replace(
    TOutString& outputString,
    const TInString& inputString,
    const TInString& replaced,
    const TInString& replacement) {
  if (replaced.size() == 0) {
    // Add replacement before and after each character.
    outputString.reserve(
        inputString.size() + replacement.size() +
        inputString.size() * replacement.size());
  } else {
    outputString.reserve(
        inputString.size() + replacement.size() +
        (inputString.size() / replaced.size()) * replacement.size());
  }

  auto outputSize = stringCore::replace(
      outputString.data(),
      std::string_view(inputString.data(), inputString.size()),
      std::string_view(replaced.data(), replaced.size()),
      std::string_view(replacement.data(), replacement.size()),
      false);

  outputString.resize(outputSize);
}

/// Replace replaced with replacement in place in string.
template <typename TInOutString, typename TInString>
FOLLY_ALWAYS_INLINE void replaceInPlace(
    TInOutString& string,
    const TInString& replaced,
    const TInString& replacement) {
  assert(replacement.size() <= replaced.size() && "invalid inplace replace");

  auto outputSize = stringCore::replace(
      string.data(),
      std::string_view(string.data(), string.size()),
      std::string_view(replaced.data(), replaced.size()),
      std::string_view(replacement.data(), replacement.size()),
      true);

  string.resize(outputSize);
}

/// Compute the MD5 Hash.
template <typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE bool md5(TOutString& output, const TInString& input) {
  static const auto kByteLength = 16;
  output.resize(kByteLength);
  crypto::MD5Context md5Context;
  md5Context.Add((const uint8_t*)input.data(), input.size());
  md5Context.Finish((uint8_t*)output.data());
  return true;
}

/// Compute the MD5 Hash.
template <typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE bool md5_radix(
    TOutString& output,
    const TInString& input,
    const int32_t radix = 16) {
  static const auto kMaxTextLength = 64;

  crypto::MD5Context md5Context;
  md5Context.Add((const uint8_t*)input.data(), input.size());
  output.reserve(kMaxTextLength);
  int size = 0;
  switch (radix) {
    case 16:
      size = md5Context.FinishHex((char*)output.data());
      break;
    case 10:
      size = md5Context.FinishDec((char*)output.data());
      break;
    default:
      VELOX_USER_FAIL(
          "Not a valid radix for md5: {}. Supported values are 10 or 16",
          radix);
  }

  output.resize(size);
  return true;
}

/// Compute the SHA256 Hash.
template <typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE bool sha256(TOutString& output, const TInString& input) {
  output.resize(32);
  folly::ssl::OpenSSLHash::sha256(
      folly::MutableByteRange((uint8_t*)output.data(), output.size()),
      folly::ByteRange((const uint8_t*)input.data(), input.size()));
  return true;
}

/// Compute the SHA512 Hash.
template <typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE void sha512(TOutString& output, const TInString& input) {
  output.resize(64);
  folly::ssl::OpenSSLHash::sha512(
      folly::MutableByteRange((uint8_t*)output.data(), output.size()),
      folly::ByteRange((const uint8_t*)input.data(), input.size()));
}

template <typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE bool toHex(TOutString& output, const TInString& input) {
  static const char* const kHexTable =
      "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F"
      "202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F"
      "404142434445464748494A4B4C4D4E4F505152535455565758595A5B5C5D5E5F"
      "606162636465666768696A6B6C6D6E6F707172737475767778797A7B7C7D7E7F"
      "808182838485868788898A8B8C8D8E8F909192939495969798999A9B9C9D9E9F"
      "A0A1A2A3A4A5A6A7A8A9AAABACADAEAFB0B1B2B3B4B5B6B7B8B9BABBBCBDBEBF"
      "C0C1C2C3C4C5C6C7C8C9CACBCCCDCECFD0D1D2D3D4D5D6D7D8D9DADBDCDDDEDF"
      "E0E1E2E3E4E5E6E7E8E9EAEBECEDEEEFF0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF";

  const auto inputSize = input.size();
  output.resize(inputSize * 2);

  const unsigned char* inputBuffer =
      reinterpret_cast<const unsigned char*>(input.data());
  char* resultBuffer = output.data();

  for (auto i = 0; i < inputSize; ++i) {
    resultBuffer[i * 2] = kHexTable[inputBuffer[i] * 2];
    resultBuffer[i * 2 + 1] = kHexTable[inputBuffer[i] * 2 + 1];
  }

  return true;
}

FOLLY_ALWAYS_INLINE static uint8_t fromHex(char c) {
  if (c >= '0' && c <= '9') {
    return c - '0';
  }

  if (c >= 'A' && c <= 'F') {
    return 10 + c - 'A';
  }

  if (c >= 'a' && c <= 'f') {
    return 10 + c - 'a';
  }

  VELOX_USER_FAIL("Invalid hex character: {}", c);
}

FOLLY_ALWAYS_INLINE unsigned char toHex(unsigned char c) {
  return c < 10 ? (c + '0') : (c + 'A' - 10);
}

template <typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE bool fromHex(TOutString& output, const TInString& input) {
  VELOX_USER_CHECK_EQ(
      input.size() % 2,
      0,
      "Invalid input length for from_hex(): {}",
      input.size());

  const auto resultSize = input.size() / 2;
  output.resize(resultSize);

  const char* inputBuffer = input.data();
  char* resultBuffer = output.data();

  for (auto i = 0; i < resultSize; ++i) {
    resultBuffer[i] =
        (fromHex(inputBuffer[i * 2]) << 4) | fromHex(inputBuffer[i * 2 + 1]);
  }

  return true;
}

template <typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE bool toBase64(TOutString& output, const TInString& input) {
  output.resize(encoding::Base64::calculateEncodedSize(input.size()));
  encoding::Base64::encode(input.data(), input.size(), output.data());
  return true;
}

template <typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE bool fromBase64(
    TOutString& output,
    const TInString& input) {
  try {
    auto inputSize = input.size();
    output.resize(
        encoding::Base64::calculateDecodedSize(input.data(), inputSize));
    encoding::Base64::decode(input.data(), input.size(), output.data());
  } catch (const encoding::Base64Exception& e) {
    VELOX_USER_FAIL(e.what());
  }
  return true;
}

FOLLY_ALWAYS_INLINE void charEscape(unsigned char c, char* output) {
  output[0] = '%';
  output[1] = toHex(c / 16);
  output[2] = toHex(c % 16);
}

/// Escapes ``input`` by encoding it so that it can be safely included in
/// URL query parameter names and values:
///
///  * Alphanumeric characters are not encoded.
///  * The characters ``.``, ``-``, ``*`` and ``_`` are not encoded.
///  * The ASCII space character is encoded as ``+``.
///  * All other characters are converted to UTF-8 and the bytes are encoded
///    as the string ``%XX`` where ``XX`` is the uppercase hexadecimal
///    value of the UTF-8 byte.
template <typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE bool urlEscape(TOutString& output, const TInString& input) {
  auto inputSize = input.size();
  output.reserve(inputSize * 3);

  auto inputBuffer = input.data();
  auto outputBuffer = output.data();

  size_t outIndex = 0;
  for (auto i = 0; i < inputSize; ++i) {
    unsigned char p = inputBuffer[i];

    if ((p >= 'a' && p <= 'z') || (p >= 'A' && p <= 'Z') ||
        (p >= '0' && p <= '9') || p == '-' || p == '_' || p == '.' ||
        p == '*') {
      outputBuffer[outIndex++] = p;
    } else if (p == ' ') {
      outputBuffer[outIndex++] = '+';
    } else {
      charEscape(p, outputBuffer + outIndex);
      outIndex += 3;
    }
  }
  output.resize(outIndex);
  return true;
}

template <typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE bool urlUnescape(
    TOutString& output,
    const TInString& input) {
  auto inputSize = input.size();
  output.reserve(inputSize);

  auto outputBuffer = output.data();
  const char* p = input.data();
  const char* end = p + inputSize;
  char buf[3];
  buf[2] = '\0';
  char* endptr;
  for (; p < end; ++p) {
    if (*p == '+') {
      *outputBuffer++ = ' ';
    } else if (*p == '%') {
      if (p + 2 < end) {
        buf[0] = p[1];
        buf[1] = p[2];
        int val = strtol(buf, &endptr, 16);
        if (endptr == buf + 2) {
          *outputBuffer++ = (char)val;
          p += 2;
        } else {
          VELOX_USER_FAIL(
              "Illegal hex characters in escape (%) pattern: {}", buf);
        }
      } else {
        VELOX_USER_FAIL("Incomplete trailing escape (%) pattern");
      }
    } else {
      *outputBuffer++ = *p;
    }
  }
  output.resize(outputBuffer - output.data());
  return true;
}

// Presto supports both ascii whitespace and unicode line separator \u2028.
FOLLY_ALWAYS_INLINE bool isUnicodeWhiteSpace(utf8proc_int32_t codePoint) {
  // 9 -> \t, 10 -> \n, 13 -> \r, 32 -> ' ', 8232 -> \u2028
  return codePoint == 9 || codePoint == 10 || codePoint == 13 ||
      codePoint == 8232 || codePoint == 32;
}

FOLLY_ALWAYS_INLINE bool isAsciiWhiteSpace(char ch) {
  return ch == '\t' || ch == '\n' || ch == '\r' || ch == ' ';
}

// Returns -1 if 'data' does not end with a white space, otherwise returns the
// size of the white space character at the end of 'data'. 'size' is the size of
// 'data' in bytes.
FOLLY_ALWAYS_INLINE int endsWithUnicodeWhiteSpace(
    const char* data,
    size_t size) {
  if (size >= 1) {
    // check 1 byte characters.
    // codepoints: 9, 10, 13, 32.
    auto& lastChar = data[size - 1];
    if (isAsciiWhiteSpace(lastChar)) {
      return 1;
    }
  }

  if (size >= 3) {
    // Check if the last character is \u2028.
    if (data[size - 3] == '\xe2' && data[size - 2] == '\x80' &&
        data[size - 1] == '\xa8') {
      return 3;
    }
  }

  return -1;
}

template <typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE bool splitPart(
    TOutString& output,
    const TInString& input,
    const TInString& delimiter,
    const int64_t& index) {
  std::string_view delim = std::string_view(delimiter.data(), delimiter.size());
  std::string_view inputSv = std::string_view(input.data(), input.size());
  int64_t iteration = 1;
  size_t curPos = 0;
  if (delim.size() == 0) {
    if (index == 1) {
      output.setNoCopy(StringView(input.data(), input.size()));
      return true;
    }
    return false;
  }
  while (curPos <= inputSv.size()) {
    size_t start = curPos;
    curPos = inputSv.find(delim, curPos);
    if (iteration == index) {
      size_t end = curPos;
      if (end == std::string_view::npos) {
        end = inputSv.size();
      }
      output.setNoCopy(StringView(input.data() + start, end - start));
      return true;
    }

    if (curPos == std::string_view::npos) {
      return false;
    }
    curPos += delim.size();
    iteration++;
  }
  return false;
}

template <
    bool leftTrim,
    bool rightTrim,
    typename TOutString,
    typename TInString>
FOLLY_ALWAYS_INLINE void trimAsciiWhiteSpace(
    TOutString& output,
    const TInString& input) {
  if (input.empty()) {
    output.setEmpty();
    return;
  }

  auto curPos = input.begin();
  if constexpr (leftTrim) {
    while (curPos < input.end() && isAsciiWhiteSpace(*curPos)) {
      curPos++;
    }
  }
  if (curPos >= input.end()) {
    output.setEmpty();
    return;
  }
  auto start = curPos;
  curPos = input.end() - 1;
  if constexpr (rightTrim) {
    while (curPos >= start && isAsciiWhiteSpace(*curPos)) {
      curPos--;
    }
  }
  output.setNoCopy(StringView(start, curPos - start + 1));
}

template <
    bool leftTrim,
    bool rightTrim,
    typename TOutString,
    typename TInString>
FOLLY_ALWAYS_INLINE void trimUnicodeWhiteSpace(
    TOutString& output,
    const TInString& input) {
  if (input.empty()) {
    output.setEmpty();
    return;
  }

  auto curStartPos = 0;
  if constexpr (leftTrim) {
    int codePointSize = 0;
    while (curStartPos < input.size()) {
      auto codePoint =
          utf8proc_codepoint(input.data() + curStartPos, codePointSize);
      if (!isUnicodeWhiteSpace(codePoint)) {
        break;
      }
      curStartPos += codePointSize;
    }

    if (curStartPos >= input.size()) {
      output.setEmpty();
      return;
    }
  }

  const auto startIndex = curStartPos;
  const auto* stringStart = input.data() + startIndex;
  auto endIndex = input.size() - 1;

  if constexpr (rightTrim) {
    // Right trim traverses the string backwards.

    while (endIndex >= startIndex) {
      auto charSize =
          endsWithUnicodeWhiteSpace(stringStart, endIndex - startIndex + 1);
      if (charSize == -1) {
        break;
      }
      endIndex -= charSize;
    }

    if (endIndex < startIndex) {
      output.setEmpty();
      return;
    }
  }

  output.setNoCopy(StringView(stringStart, endIndex - startIndex + 1));
}

template <bool ascii, typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE void reverse(TOutString& output, const TInString& input) {
  auto inputSize = input.size();
  output.resize(inputSize);

  if constexpr (ascii) {
    reverseAscii(output.data(), input.data(), inputSize);
  } else {
    reverseUnicode(output.data(), input.data(), inputSize);
  }
}

template <bool lpad, bool isAscii, typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE void pad(
    TOutString& output,
    const TInString& string,
    const int64_t size,
    const TInString& padString) {
  static constexpr size_t padMaxSize = 1024 * 1024; // 1MB

  VELOX_USER_CHECK(
      size >= 0 && size <= padMaxSize,
      "pad size must be in the range [0..{})",
      padMaxSize);
  VELOX_USER_CHECK(padString.size() > 0, "padString must not be empty");

  int64_t stringCharLength = length<isAscii>(string);
  // If string has at most size characters, truncate it if necessary
  // and return it as the result.
  if (UNLIKELY(stringCharLength >= size)) {
    size_t prefixByteSize =
        stringCore::getByteRange<isAscii>(string.data(), 1, size).second;
    output.resize(prefixByteSize);
    if (LIKELY(prefixByteSize > 0)) {
      std::memcpy(output.data(), string.data(), prefixByteSize);
    }
    return;
  }

  int64_t padStringCharLength = length<isAscii>(padString);
  // How many characters do we need to add to string.
  int64_t fullPaddingCharLength = size - stringCharLength;
  // How many full copies of padString need to be added.
  int64_t fullPadCopies = fullPaddingCharLength / padStringCharLength;
  // If the length of padString does not evenly divide the length of the
  // padding we need to add, how long of a prefix of padString needs to be
  // added at the end of the padding.  Will be 0 if it is evenly divisible.
  size_t padPrefixByteLength =
      stringCore::getByteRange<isAscii>(
          padString.data(), 1, fullPaddingCharLength % padStringCharLength)
          .second;
  int64_t fullPaddingByteLength =
      padString.size() * fullPadCopies + padPrefixByteLength;
  // The final size of the output string in bytes.
  int64_t outputByteLength = string.size() + fullPaddingByteLength;
  // What byte index in the ouptut to start writing the padding at.
  int64_t paddingOffset;

  output.resize(outputByteLength);

  if constexpr (lpad) {
    paddingOffset = 0;
    // Copy string after the padding.
    std::memcpy(
        output.data() + fullPaddingByteLength, string.data(), string.size());
  } else {
    paddingOffset = string.size();
    // Copy string at the beginning of the output.
    std::memcpy(output.data(), string.data(), string.size());
  }

  for (int i = 0; i < fullPadCopies; i++) {
    std::memcpy(
        output.data() + paddingOffset + i * padString.size(),
        padString.data(),
        padString.size());
  }

  // Add the final prefix of padString to the padding in output (if necessary).
  std::memcpy(
      output.data() + paddingOffset + fullPadCopies * padString.size(),
      padString.data(),
      padPrefixByteLength);
}
} // namespace facebook::velox::functions::stringImpl
