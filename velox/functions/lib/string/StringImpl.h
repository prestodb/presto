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
#include "velox/common/base/Exceptions.h"
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

/// Return a capped length in characters(controlled by maxLength) of a string.
/// The returned length is not greater than maxLength.
template <bool isAscii, typename T>
FOLLY_ALWAYS_INLINE int64_t cappedLength(const T& input, size_t maxLength) {
  if constexpr (isAscii) {
    return input.size() > maxLength ? maxLength : input.size();
  } else {
    return cappedLengthUnicode(input.data(), input.size(), maxLength);
  }
}

/// Return a capped length in bytes(controlled by maxCharacters) of a string.
/// The returned length may be greater than maxCharacters if there are
/// multi-byte characters present in the input string.
template <bool isAscii, typename TString>
FOLLY_ALWAYS_INLINE int64_t
cappedByteLength(const TString& input, size_t maxCharacters) {
  if constexpr (isAscii) {
    return input.size() > maxCharacters ? maxCharacters : input.size();
  } else {
    return cappedByteLengthUnicode(input.data(), input.size(), maxCharacters);
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
  auto codePoint = utf8proc_codepoint(
      inputString.data(), inputString.data() + inputString.size(), size);
  return codePoint;
}

/// Returns the sequence of character Unicode code point of an input string.
template <typename T>
std::vector<int32_t> stringToCodePoints(const T& inputString) {
  int64_t length = inputString.size();
  std::vector<int32_t> codePoints;
  codePoints.reserve(length);

  int64_t inputIndex = 0;
  while (inputIndex < length) {
    utf8proc_int32_t codepoint;
    int size;
    codepoint = utf8proc_codepoint(
        inputString.data() + inputIndex, inputString.data() + length, size);
    VELOX_USER_CHECK_GE(
        codepoint,
        0,
        "Invalid UTF-8 encoding in characters: {}",
        StringView(
            inputString.data() + inputIndex,
            std::min(length - inputIndex, inputIndex + 12)));
    codePoints.push_back(codepoint);
    inputIndex += size;
  }
  return codePoints;
}

/// Returns the starting position in characters of the Nth instance(counting
/// from the left if lpos==true and from the end otherwise) of the substring in
/// string. Positions start with 1. If not found, 0 is returned. If subString is
/// empty result is 1.
template <bool isAscii, bool lpos = true, typename T>
FOLLY_ALWAYS_INLINE int64_t
stringPosition(const T& string, const T& subString, int64_t instance = 0) {
  VELOX_USER_CHECK_GT(instance, 0, "'instance' must be a positive number");
  if (subString.size() == 0) {
    return 1;
  }

  int64_t byteIndex = -1;
  if constexpr (lpos) {
    byteIndex = findNthInstanceByteIndexFromStart(
        std::string_view(string.data(), string.size()),
        std::string_view(subString.data(), subString.size()),
        instance);
  } else {
    byteIndex = findNthInstanceByteIndexFromEnd(
        std::string_view(string.data(), string.size()),
        std::string_view(subString.data(), subString.size()),
        instance);
  }

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

namespace {
FOLLY_ALWAYS_INLINE int64_t asciiWhitespaces() {
  std::vector<int32_t> codes = {9, 10, 11, 12, 13, 28, 29, 30, 31, 32};
  int64_t bitMask = 0;
  for (auto code : codes) {
    bits::setBit(&bitMask, code, true);
  }
  return bitMask;
}

FOLLY_ALWAYS_INLINE int64_t asciiWhitespaceCodes() {
  std::vector<int32_t> codes = {9, 10, 11, 12, 13, 28, 29, 30, 31, 32};
  int64_t bitMask = 0;
  for (auto code : codes) {
    bits::setBit(&bitMask, code, true);
  }
  return bitMask;
}

FOLLY_ALWAYS_INLINE std::array<int64_t, 2> unicodeWhitespaceCodes() {
  std::vector<int32_t> codes = {
      8192,
      8193,
      8194,
      8195,
      8196,
      8197,
      8198,
      8200,
      8201,
      8202,
      8232,
      8233,
      8287};
  std::array<int64_t, 2> bitMask{0, 0};
  for (auto code : codes) {
    bits::setBit(&bitMask, code - 8192, true);
  }
  return bitMask;
}
} // namespace

/// Unicode codepoints recognized as whitespace in Presto:
// clang-format off
/// [9, 10, 11, 12, 13, 28, 29, 30, 31, 32,
///  5760,
///  8192, 8193, 8194, 8195, 8196, 8197, 8198, 8200, 8201, 8202, 8232, 8233, 8287,
///  12288]
// clang-format on
// This function need to handle invalid codepoints with out crashing.
FOLLY_ALWAYS_INLINE bool isUnicodeWhiteSpace(utf8proc_int32_t codePoint) {
  static const auto kAsciiCodes = asciiWhitespaceCodes();
  static const auto kUnicodeCodes = unicodeWhitespaceCodes();
  if (codePoint < 0) {
    return false;
  }

  if (codePoint < 5'000) {
    if (codePoint > 32) {
      return false; // Most common path. Uses 2 comparisons.
    }

    return bits::isBitSet(&kAsciiCodes, codePoint);
  }

  if (codePoint >= 8192) {
    if (codePoint <= 8287) {
      return bits::isBitSet(kUnicodeCodes.data(), codePoint - 8192);
    }

    return codePoint == 12288;
  }

  return codePoint == 5760;
}

FOLLY_ALWAYS_INLINE bool isAsciiWhiteSpace(char ch) {
  static const auto kAsciiCodes = asciiWhitespaceCodes();

  const uint32_t code = ch;

  if (code <= 32) {
    return bits::isBitSet(&kAsciiCodes, code);
  }

  return false;
}

FOLLY_ALWAYS_INLINE bool isAsciiSpace(char ch) {
  return ch == ' ';
}

// Returns -1 if 'data' does not end with a white space, otherwise returns the
// size of the white space character at the end of 'data'. 'size' is the size of
// 'data' in bytes.
FOLLY_ALWAYS_INLINE int endsWithUnicodeWhiteSpace(
    const char* data,
    size_t size) {
  if (size >= 1) {
    // Check ASCII whitespaces.
    auto& lastChar = data[size - 1];
    if (isAsciiWhiteSpace(lastChar)) {
      return 1;
    }
  }

  // All Unicode whitespaces are 3-byte characters.
  if (size >= 3) {
    int32_t codePointSize;
    auto codePoint =
        utf8proc_codepoint(data + size - 3, data + size, codePointSize);
    if (codePoint != -1 && codePointSize == 3 &&
        isUnicodeWhiteSpace(codePoint)) {
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
    typename TShouldTrim,
    typename TOutString,
    typename TInString>
FOLLY_ALWAYS_INLINE void
trimAscii(TOutString& output, const TInString& input, TShouldTrim shouldTrim) {
  if (input.empty()) {
    output.setEmpty();
    return;
  }

  auto curPos = input.begin();
  if constexpr (leftTrim) {
    while (curPos < input.end() && shouldTrim(*curPos)) {
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
    while (curPos >= start && shouldTrim(*curPos)) {
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
  auto emptyOutput = [&]() {
    if constexpr (std::is_same_v<TOutString, StringView>) {
      output = StringView("");
    } else {
      output.setEmpty();
    }
  };
  if (input.empty()) {
    emptyOutput();
    return;
  }

  auto curStartPos = 0;
  if constexpr (leftTrim) {
    int codePointSize = 0;
    while (curStartPos < input.size()) {
      auto codePoint = utf8proc_codepoint(
          input.data() + curStartPos,
          input.data() + input.size(),
          codePointSize);
      if (codePoint == -1 || !isUnicodeWhiteSpace(codePoint)) {
        break;
      }
      curStartPos += codePointSize;
    }

    if (curStartPos >= input.size()) {
      emptyOutput();
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
      emptyOutput();
      return;
    }
  }

  auto view = StringView(stringStart, endIndex - startIndex + 1);
  if constexpr (std::is_same_v<TOutString, StringView>) {
    output = view;
  } else {
    output.setNoCopy(view);
  }
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
        stringCore::getByteRange<isAscii>(string.data(), string.size(), 1, size)
            .second;
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
  size_t padPrefixByteLength = stringCore::getByteRange<isAscii>(
                                   padString.data(),
                                   padString.size(),
                                   1,
                                   fullPaddingCharLength % padStringCharLength)
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
