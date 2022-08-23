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
  for (auto i = 0; i < length; i++) {
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
    int size;
    utf8proc_codepoint(&input[inputIdx], size);
    // invalid utf8 gets byte sequence with nextCodePoint==-1 and size==1,
    // continue reverse invalid sequence byte by byte.
    assert(
        size > 0 && "UNLIKELY: could not get size of invalid utf8 code point");
    assert(outputIdx >= size && "access out of bound");
    outputIdx -= size;

    assert(outputIdx < length && "access out of bound");
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
    nextCodePoint = utf8proc_codepoint(&input[inputIdx], size);
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
    nextCodePoint = utf8proc_codepoint(&input[inputIdx], size);
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
 * @param numBytes size of input buffer
 * @return the number of characters represented by the input utf8 string
 */
FOLLY_ALWAYS_INLINE int64_t
lengthUnicode(const char* inputBuffer, size_t bufferLength) {
  // First address after the last byte in the buffer
  auto buffEndAddress = inputBuffer + bufferLength;
  auto currentChar = inputBuffer;
  int64_t size = 0;
  while (currentChar < buffEndAddress) {
    auto chrOffset = utf8proc_char_length(currentChar);
    // Skip bad byte if we get utf length < 0.
    currentChar += UNLIKELY(chrOffset < 0) ? 1 : chrOffset;
    size++;
  }
  return size;
}

/// Returns the start byte index of the Nth instance of subString in
/// string. Search starts from startPosition. Positions start with 0. If not
/// found, -1 is returned.
/// stringPosition for Unicode uses this by first finding the byte index of
/// substring and then computing the length of substring[0, byteIndex). This is
/// safe because in UTF8 a char can not be a subset of another char (in bytes
/// representation).
static int64_t findNthInstanceByteIndex(
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
  return findNthInstanceByteIndex(
      string, subString, instance - 1, byteIndex + subString.size());
}

/// Replace replaced with replacement in inputString and write results in
/// outputString. If inPlace=true inputString and outputString are assumed to
/// tbe the same. When replaced is empty, replacement is added before and after
/// each charecter. When inputString is empty results is empty.
/// replace("", "", "x") = ""
/// replace("aa", "", "x") = "xaxax"
inline static size_t replace(
    char* outputString,
    const std::string_view& inputString,
    const std::string_view& replaced,
    const std::string_view& replacement,
    bool inPlace = false) {
  if (inputString.size() == 0) {
    return 0;
  }

  size_t readPosition = 0;
  size_t writePosition = 0;
  // Copy needed in out of place replace, and when replaced and replacement are
  // of different sizes.
  bool doCopyUnreplaced = !inPlace || (replaced.size() != replacement.size());

  auto findNextReplaced = [&]() {
    return findNthInstanceByteIndex(inputString, replaced, 1, readPosition);
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
  if (replaced.size() == 0) {
    if (replacement.size() == 0) {
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
template <bool isAscii>
static inline std::pair<size_t, size_t>
getByteRange(const char* str, size_t startCharPosition, size_t length) {
  if (startCharPosition < 1 && length > 0) {
    throw std::invalid_argument(
        "start position must be >= 1 and length must be > 0");
  }
  if constexpr (isAscii) {
    return std::make_pair(
        startCharPosition - 1, startCharPosition + length - 1);
  } else {
    size_t startByteIndex = 0;
    size_t nextCharOffset = 0;

    // Find startByteIndex
    for (auto i = 0; i < startCharPosition - 1; i++) {
      nextCharOffset += utf8proc_char_length(&str[nextCharOffset]);
    }

    startByteIndex = nextCharOffset;

    // Find endByteIndex
    for (auto i = 0; i < length; i++) {
      nextCharOffset += utf8proc_char_length(&str[nextCharOffset]);
    }

    return std::make_pair(startByteIndex, nextCharOffset);
  }
}
} // namespace stringCore
} // namespace facebook::velox::functions
