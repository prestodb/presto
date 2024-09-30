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

#include <cstring>
#include <string>
#include <string_view>
#include "folly/CPortability.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/external/utf8proc/utf8procImpl.h"

#if (ENABLE_VECTORIZATION > 0) && !defined(_DEBUG) && !defined(DEBUG)
#if defined(__clang__) && (__clang_major__ > 7)
#define IS_SANITIZER                          \
  ((__has_feature(address_sanitizer) == 1) || \
   (__has_feature(memory_sanitizer) == 1) ||  \
   (__has_feature(thread_sanitizer) == 1) ||  \
   (__has_feature(undefined_sanitizer) == 1))

#if IS_SANITIZER == 0
#define VECTORIZE_LOOP_IF_POSSIBLE _Pragma("clang loop vectorize(enable)")
#endif
#endif
#endif

#ifndef VECTORIZE_LOOP_IF_POSSIBLE
// Not supported
#define VECTORIZE_LOOP_IF_POSSIBLE
#endif

namespace facebook::velox::functions {
namespace stringCore {

/// Check if a given string is ascii
static bool isAscii(const char* str, size_t length);

FOLLY_ALWAYS_INLINE bool isAscii(const char* str, size_t length) {
  const auto mask = xsimd::broadcast<uint8_t>(0x80);
  size_t i = 0;
  for (; i + mask.size <= length; i += mask.size) {
    auto batch =
        xsimd::load_unaligned(reinterpret_cast<const uint8_t*>(str) + i);
#if XSIMD_WITH_AVX
    // 1 instruction instead of 2 on AVX.
    if (!_mm256_testz_si256(batch, mask)) {
#else
    if (xsimd::any(batch >= mask)) {
#endif
      return false;
    }
  }
  for (; i < length; ++i) {
    if (str[i] & 0x80) {
      return false;
    }
  }
  return true;
}

/// Perform reverse for ascii string input
FOLLY_ALWAYS_INLINE static void
reverseAscii(char* output, const char* input, size_t length) {
  auto j = length - 1;
  VECTORIZE_LOOP_IF_POSSIBLE for (auto i = 0; i < length; ++i, --j) {
    output[i] = input[j];
  }
}

/// Perform reverse for utf8 string input
FOLLY_ALWAYS_INLINE static void
reverseUnicode(char* output, const char* input, size_t length) {
  auto inputIdx = 0;
  auto outputIdx = length;
  while (inputIdx < length) {
    int size = 1;
    auto valid = utf8proc_codepoint(&input[inputIdx], input + length, size);

    // if invalid utf8 gets byte sequence with nextCodePoint==-1 and size==1,
    // continue reverse invalid sequence byte by byte.
    if (valid == -1) {
      size = 1;
    }

    VELOX_USER_CHECK_GE(outputIdx, size, "access out of bound");
    outputIdx -= size;

    VELOX_USER_CHECK_LT(outputIdx, length, "access out of bound");
    std::memcpy(&output[outputIdx], &input[inputIdx], size);
    inputIdx += size;
  }
}

/// Perform upper for ascii string input
FOLLY_ALWAYS_INLINE static void
upperAscii(char* output, const char* input, size_t length) {
  VECTORIZE_LOOP_IF_POSSIBLE for (auto i = 0; i < length; i++) {
    if (input[i] >= 'a' && input[i] <= 'z') {
      output[i] = input[i] - 32;
    } else {
      output[i] = input[i];
    }
  }
}

/// Perform lower for ascii string input
FOLLY_ALWAYS_INLINE static void
lowerAscii(char* output, const char* input, size_t length) {
  VECTORIZE_LOOP_IF_POSSIBLE for (auto i = 0; i < length; i++) {
    if (input[i] >= 'A' && input[i] <= 'Z') {
      output[i] = input[i] + 32;
    } else {
      output[i] = input[i];
    }
  }
}

/// Perform upper for utf8 string input, output should be pre-allocated and
/// large enough for the results. outputLength refers to the number of bytes
/// available in the output buffer, and inputLength is the number of bytes in
/// the input string
FOLLY_ALWAYS_INLINE size_t upperUnicode(
    char* output,
    size_t outputLength,
    const char* input,
    size_t inputLength) {
  auto inputIdx = 0;
  auto outputIdx = 0;

  while (inputIdx < inputLength) {
    utf8proc_int32_t nextCodePoint;
    int size;
    nextCodePoint =
        utf8proc_codepoint(&input[inputIdx], input + inputLength, size);
    if (UNLIKELY(nextCodePoint == -1)) {
      // invalid input string, copy the remaining of the input string as is to
      // the output.
      std::memcpy(&output[outputIdx], &input[inputIdx], inputLength - inputIdx);
      outputIdx += inputLength - inputIdx;
      return outputIdx;
    }
    inputIdx += size;

    auto upperCodePoint = utf8proc_toupper(nextCodePoint);

    assert(
        (outputIdx + utf8proc_codepoint_length(upperCodePoint)) <
            outputLength &&
        "access out of bound");

    auto newSize = utf8proc_encode_char(
        upperCodePoint, reinterpret_cast<unsigned char*>(&output[outputIdx]));
    outputIdx += newSize;
  }
  return outputIdx;
}

/// Perform lower for utf8 string input, output should be pre-allocated and
/// large enough for the results outputLength refers to the number of bytes
/// available in the output buffer, and inputLength is the number of bytes in
/// the input string
FOLLY_ALWAYS_INLINE size_t lowerUnicode(
    char* output,
    size_t outputLength,
    const char* input,
    size_t inputLength) {
  auto inputIdx = 0;
  auto outputIdx = 0;

  while (inputIdx < inputLength) {
    utf8proc_int32_t nextCodePoint;
    int size;
    nextCodePoint =
        utf8proc_codepoint(&input[inputIdx], input + inputLength, size);
    if (UNLIKELY(nextCodePoint == -1)) {
      // invalid input string, copy the remaining of the input string as is to
      // the output.
      std::memcpy(&output[outputIdx], &input[inputIdx], inputLength - inputIdx);
      outputIdx += inputLength - inputIdx;
      return outputIdx;
    }

    inputIdx += size;
    auto lowerCodePoint = utf8proc_tolower(nextCodePoint);

    assert(
        (outputIdx + utf8proc_codepoint_length(lowerCodePoint)) <
            outputLength &&
        "access out of bound");

    auto newSize = utf8proc_encode_char(
        lowerCodePoint, reinterpret_cast<unsigned char*>(&output[outputIdx]));
    outputIdx += newSize;
  }
  return outputIdx;
}

/// Apply a sequence of appenders to the output string sequentially.
/// @param output the output string that appenders are applied to
/// @param appenderFunc a function that appends some string to an input string
/// of type TOutStr
template <typename TOutStr, typename Func>
static void applyAppendersRecursive(TOutStr& output, Func appenderFunc) {
  appenderFunc(output);
}

template <typename TOutStr, typename Func, typename... Funcs>
static void
applyAppendersRecursive(TOutStr& output, Func appenderFunc, Funcs... funcs) {
  appenderFunc(output);
  applyAppendersRecursive(output, funcs...);
}

/**
 * Return the length in chars of a utf8 string stored in the input buffer
 * @param inputBuffer input buffer that hold the string
 * @param bufferLength size of input buffer
 * @return the number of characters represented by the input utf8 string
 */
FOLLY_ALWAYS_INLINE int64_t
lengthUnicode(const char* inputBuffer, size_t bufferLength) {
  // First address after the last byte in the buffer
  auto buffEndAddress = inputBuffer + bufferLength;
  auto currentChar = inputBuffer;
  int64_t size = 0;
  while (currentChar < buffEndAddress) {
    // This function detects bytes that come after the first byte in a
    // multi-byte UTF-8 character (provided that the string is valid UTF-8). We
    // increment size only for the first byte so that we treat all bytes as part
    // of a single character.
    if (!utf_cont(*currentChar)) {
      size++;
    }

    currentChar++;
  }
  return size;
}

/**
 * Return an capped length(controlled by maxChars) of a unicode string. The
 * returned length is not greater than maxChars.
 *
 * This method is used to tell whether a string is longer or the same length of
 * another string, in these scenarios we don't need accurate length, by
 * providing maxChars we can get better performance by avoid calculating whole
 * length of a string which might be very long.
 *
 * @param input input buffer that hold the string
 * @param size size of input buffer
 * @param maxChars stop counting characters if the string is longer
 * than this value
 * @return the number of characters represented by the input utf8 string
 */
FOLLY_ALWAYS_INLINE int64_t
cappedLengthUnicode(const char* input, size_t size, size_t maxChars) {
  // First address after the last byte in the input
  auto end = input + size;
  auto currentChar = input;
  int64_t numChars = 0;

  // Use maxChars to early stop to avoid calculating the whole
  // length of long string.
  while (currentChar < end && numChars < maxChars) {
    auto charSize = utf8proc_char_length(currentChar);
    // Skip bad byte if we get utf length < 0.
    currentChar += UNLIKELY(charSize < 0) ? 1 : charSize;
    numChars++;
  }

  return numChars;
}

///
/// Return an capped length in bytes(controlled by maxChars) of a unicode
/// string. The returned length may be greater than maxCharacters if there are
/// multi-byte characters present in the input string.
///
/// This method is used to help with indexing unicode strings by byte position.
/// It is used to find the byte position of the Nth character in a string.
///
/// @param input input buffer that hold the string
/// @param size size of input buffer
/// @param maxChars stop counting characters if the string is longer
/// than this value
/// @return the number of bytes represented by the input utf8 string up to
/// maxChars
///
FOLLY_ALWAYS_INLINE int64_t
cappedByteLengthUnicode(const char* input, size_t size, int64_t maxChars) {
  size_t utf8Position = 0;
  size_t numCharacters = 0;
  while (utf8Position < size && numCharacters < maxChars) {
    auto charSize = utf8proc_char_length(input + utf8Position);
    utf8Position += UNLIKELY(charSize < 0) ? 1 : charSize;
    numCharacters++;
  }
  return utf8Position;
}

/// Returns the start byte index of the Nth instance of subString in
/// string. Search starts from startPosition. Positions start with 0. If not
/// found, -1 is returned. To facilitate finding overlapping strings, the
/// nextStartPosition is incremented by 1
static inline int64_t findNthInstanceByteIndexFromStart(
    const std::string_view& string,
    const std::string_view subString,
    const size_t instance = 1,
    const size_t startPosition = 0) {
  assert(instance > 0);

  if (startPosition >= string.size()) {
    return -1;
  }

  auto byteIndex = string.find(subString, startPosition);
  // Not found
  if (byteIndex == std::string_view::npos) {
    return -1;
  }

  // Search done
  if (instance == 1) {
    return byteIndex;
  }

  // Find next occurrence
  return findNthInstanceByteIndexFromStart(
      string, subString, instance - 1, byteIndex + 1);
}

/// Returns the start byte index of the Nth instance of subString in
/// string from the end. Search starts from endPosition. Positions start with 0.
/// If not found, -1 is returned. To facilitate finding overlapping strings, the
/// nextStartPosition is incremented by 1
inline int64_t findNthInstanceByteIndexFromEnd(
    const std::string_view string,
    const std::string_view subString,
    const size_t instance = 1) {
  assert(instance > 0);

  if (subString.empty()) {
    return 0;
  }

  size_t foundCnt = 0;
  size_t index = string.size();
  do {
    if (index == 0) {
      return -1;
    }

    index = string.rfind(subString, index - 1);
    if (index == std::string_view::npos) {
      return -1;
    }
    ++foundCnt;
  } while (foundCnt < instance);
  return index;
}

/// Replace replaced with replacement in inputString and write results in
/// outputString. If inPlace=true inputString and outputString are assumed to
/// be the same. When replaced is empty and ignoreEmptyReplaced is false,
/// replacement is added before and after each charecter. When replaced is
/// empty and ignoreEmptyReplaced is true, the result is the inputString value.
/// When inputString and replaced strings are empty, result is the
/// replacement string if ignoreEmptyReplaced is false, otherwise the result is
/// empty.
///
/// replace("", "", "x") = "" -- when ignoreEmptyReplaced is true
/// replace("", "", "x") = "x" -- when ignoreEmptyReplaced is false
/// replace("aa", "", "x") = "xaxax" -- when ignoreEmptyReplaced is false
/// replace("aa", "", "x") = "aa" -- when ignoreEmptyReplaced is true
template <bool ignoreEmptyReplaced = false>
inline static size_t replace(
    char* outputString,
    const std::string_view& inputString,
    const std::string_view& replaced,
    const std::string_view& replacement,
    bool inPlace = false) {
  if (inputString.empty()) {
    if (!ignoreEmptyReplaced && replaced.empty() && !replacement.empty()) {
      std::memcpy(outputString, replacement.data(), replacement.size());
      return replacement.size();
    }
    return 0;
  }

  if constexpr (ignoreEmptyReplaced) {
    if (replaced.empty()) {
      if (!inPlace) {
        std::memcpy(outputString, inputString.data(), inputString.size());
      }
      return inputString.size();
    }
  }

  size_t readPosition = 0;
  size_t writePosition = 0;
  // Copy needed in out of place replace, and when replaced and replacement are
  // of different sizes.
  bool doCopyUnreplaced = !inPlace || (replaced.size() != replacement.size());

  auto findNextReplaced = [&]() {
    return findNthInstanceByteIndexFromStart(
        inputString, replaced, 1, readPosition);
  };

  auto writeUnchanged = [&](ssize_t size) {
    assert(size >= 0 && "Probable math error?");
    if (size <= 0) {
      return;
    }

    if (inPlace) {
      if (doCopyUnreplaced) {
        // memcpy does not allow overllapping
        std::memmove(
            &outputString[writePosition],
            &inputString.data()[readPosition],
            size);
      }
    } else {
      std::memcpy(
          &outputString[writePosition],
          &inputString.data()[readPosition],
          size);
    }
    writePosition += size;
    readPosition += size;
  };

  auto writeReplacement = [&]() {
    if (replacement.size() > 0) {
      std::memcpy(
          &outputString[writePosition], replacement.data(), replacement.size());
      writePosition += replacement.size();
    }
    readPosition += replaced.size();
  };

  // Special case when size of replaced is 0
  if (replaced.empty()) {
    if (replacement.empty()) {
      if (!inPlace) {
        std::memcpy(outputString, inputString.data(), inputString.size());
      }
      return inputString.size();
    }

    // Can never be in place since replacement.size()>replaced.size()
    assert(!inPlace && "wrong inplace replace usage");

    // add replacement before and after each char in inputString
    for (auto i = 0; i < inputString.size(); i++) {
      writeReplacement();

      outputString[writePosition] = inputString[i];
      writePosition++;
    }

    writeReplacement();
    return writePosition;
  }

  while (readPosition < inputString.size()) {
    // Find next token to replace
    auto position = findNextReplaced();

    if (position == -1) {
      break;
    }
    assert(position >= 0 && "invalid position found");
    auto unchangedSize = position - readPosition;
    writeUnchanged(unchangedSize);
    writeReplacement();
  }

  auto unchangedSize = inputString.size() - readPosition;
  writeUnchanged(unchangedSize);

  return writePosition;
}

/// Given a utf8 string, a starting position and length returns the
/// corresponding underlying byte range [startByteIndex, endByteIndex).
/// Byte indicies starts from 0, UTF8 character positions starts from 1.
/// If a bad unicode byte is encountered, then we skip that bad byte and
/// count that as one codepoint.
template <bool isAscii>
static inline std::pair<size_t, size_t> getByteRange(
    const char* str,
    size_t strLength,
    size_t startCharPosition,
    size_t length) {
  // If the length is 0, then we return an empty range directly.
  if (length == 0) {
    return std::make_pair(0, 0);
  }

  if (startCharPosition < 1) {
    throw std::invalid_argument("start position must be >= 1");
  }

  VELOX_CHECK_GE(
      strLength,
      length,
      "The length of the string must be at least as large as the length of the substring requested.");

  VELOX_CHECK_GE(
      strLength,
      startCharPosition,
      "The offset of the substring requested must be within the string.");

  if constexpr (isAscii) {
    return std::make_pair(
        startCharPosition - 1, startCharPosition + length - 1);
  } else {
    size_t startByteIndex = 0;
    size_t nextCharOffset = 0;

    // Skips any Unicode continuation bytes. These are bytes that appear after
    // the first byte in a multi-byte Unicode character.  They do not count
    // towards the position in or length of a string.
    auto skipContBytes = [&]() {
      while (nextCharOffset < strLength && utf_cont(str[nextCharOffset])) {
        nextCharOffset++;
      }
    };

    // Skip any invalid continuation bytes at the beginning of the string.
    skipContBytes();

    // Find startByteIndex
    for (auto i = 0; nextCharOffset < strLength && i < startCharPosition - 1;
         i++) {
      nextCharOffset++;

      skipContBytes();
    }

    startByteIndex = nextCharOffset;
    size_t charCountInRange = 0;

    // Find endByteIndex
    for (auto i = 0; nextCharOffset < strLength && i < length; i++) {
      nextCharOffset++;
      charCountInRange++;

      skipContBytes();
    }

    VELOX_CHECK_EQ(
        charCountInRange,
        length,
        "The substring requested at {} of length {} exceeds the bounds of the string.",
        startCharPosition,
        length);

    return std::make_pair(startByteIndex, nextCharOffset);
  }
}
} // namespace stringCore
} // namespace facebook::velox::functions
