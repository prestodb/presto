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

#include <cstdint>

#include "folly/CPortability.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::functions {

#define IS_ASCII(x) !((x) & 0x80)

/// This function is not part of the original utf8proc.
/// Tries to get the length of UTF-8 encoded code point. A
/// positive return value means the UTF-8 sequence is valid, and
/// the result is the length of the code point. A negative return value means
/// the UTF-8 sequence at the position is invalid, and the length of the invalid
/// sequence is the absolute value of the result. A byte sequence is recognized
/// as an invalid UTF-8 code point of length N in either of the folllowing
/// situations:
///   1. The first byte is a continuation byte or indicates the length of the
///      code point is greater than 6. N is 1 in this situation.
///   2. The first byte indicates a length of M > N, but there are only N-1
///      bytes left afterwards in the buffer of the given `size`.
///   3. The first byte indicates a length of M > N, but only the subsequent
///      N-1 bytes are continuation bytes.
///   4. The first byte indicates a length of N, but the code point is
///      overlong-encoded, a surrogate character not allowed in UTF-8, or above
///      the Unicode upper bound 0x10FFFF.
///   5. The first byte indicates a length of N > 4. Code points of more
///      than 4 bytes are no longer allowed per RFC3629.
///
/// @param input Pointer to the first byte of the code point. Must not be null.
/// @param size Number of available bytes. Must be greater than zero.
/// @param codePoint Populated with the code point it refers to. This is only
/// valid if the return value is positive.
/// @return the length of the code point or negative the number of bytes in the
/// invalid UTF-8 sequence.
///
/// Adapted from tryGetCodePointAt in
/// https://github.com/airlift/slice/blob/master/src/main/java/io/airlift/slice/SliceUtf8.java
int32_t
tryGetUtf8CharLength(const char* input, int64_t size, int32_t& codePoint);

/// Return the length in byte of the next UTF-8 encoded character at the
/// beginning of `string`. If the beginning of `string` is not valid UTF-8
/// encoding, return -1.
FOLLY_ALWAYS_INLINE int validateAndGetNextUtf8Length(
    const unsigned char* string,
    const unsigned char* end) {
  VELOX_DCHECK(string < end, "Expect non-empty string.");

  if ((*string & 0x80u) == 0) {
    return 1;
  }
  if ((*string & 0xE0u) == 0xC0u && (string + 1) < end &&
      (*(string + 1) & 0xC0u) == 0x80u) {
    return 2;
  }
  if ((*string & 0xF0u) == 0xE0u && (string + 2) < end &&
      (*(string + 1) & 0xC0u) == 0x80u && (*(string + 2) & 0xC0u) == 0x80u) {
    return 3;
  }
  if ((*string & 0xF8u) == 0xF0u && (string + 3) < end &&
      (*(string + 1) & 0xC0u) == 0x80u && (*(string + 2) & 0xC0u) == 0x80u &&
      (*(string + 3) & 0xC0u) == 0x80u) {
    return 4;
  }
  return -1;
}

/// Returns the length of a UTF-8 character indicated by the first byte. Returns
/// -1 for invalid UTF-8 first byte.
int firstByteCharLength(const char* u_input);

/// Invalid character replacement matrix.
constexpr std::array<std::string_view, 6> kReplacementCharacterStrings{
    "\xef\xbf\xbd",
    "\xef\xbf\xbd\xef\xbf\xbd",
    "\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd",
    "\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd",
    "\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd",
    "\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd"};

/// Returns true if there are multiple UTF-8 invalid sequences.
template <typename T>
FOLLY_ALWAYS_INLINE bool isMultipleInvalidSequences(
    const T& inputBuffer,
    size_t inputIndex) {
  return
      // 0xe0 followed by a value less than 0xe0 or 0xf0 followed by a
      // value less than 0x90 is considered an overlong encoding.
      (inputBuffer[inputIndex] == '\xe0' &&
       (inputBuffer[inputIndex + 1] & 0xe0) == 0x80) ||
      (inputBuffer[inputIndex] == '\xf0' &&
       (inputBuffer[inputIndex + 1] & 0xf0) == 0x80) ||
      // 0xf4 followed by a byte >= 0x90 looks valid to
      // tryGetUtf8CharLength, but is actually outside the range of valid
      // code points.
      (inputBuffer[inputIndex] == '\xf4' &&
       (inputBuffer[inputIndex + 1] & 0xf0) != 0x80) ||
      // The bytes 0xf5-0xff, 0xc0, and 0xc1 look like the start of
      // multi-byte code points to tryGetUtf8CharLength, but are not part of
      // any valid code point.
      (unsigned char)inputBuffer[inputIndex] > 0xf4 ||
      inputBuffer[inputIndex] == '\xc0' || inputBuffer[inputIndex] == '\xc1';
}

inline const std::string_view&
getInvalidUTF8ReplacementString(const char* input, int len, int codePointSize) {
  auto index =
      len >= 2 && isMultipleInvalidSequences(input, 0) ? codePointSize - 1 : 0;
  return kReplacementCharacterStrings[index];
}

/// Replaces invalid UTF-8 characters with replacement characters similar to
/// that produced by Presto java. The function requires that output have
/// sufficient capacity for the output string.
/// @param out Pointer to output string
/// @param input Pointer to input string
/// @param len Length of input string
/// @return number of bytes written
size_t
replaceInvalidUTF8Characters(char* output, const char* input, int32_t len);

/// Replaces invalid UTF-8 characters with replacement characters similar to
/// that produced by Presto java. The function will allocate 1 byte for each
/// orininal character plus extra 2 bytes for each maximal subpart of an
/// ill-formed subsequence for an upper bound of 3x size of the input string.
/// @param out Reference to output string
/// @param input Pointer to input string
/// @param len Length of input string
template <typename TOutString>
void replaceInvalidUTF8Characters(
    TOutString& out,
    const char* input,
    int32_t len) {
  auto maxLen = len * kReplacementCharacterStrings[0].size();
  out.reserve(maxLen);
  auto outputBuffer = out.data();
  auto outputIndex = replaceInvalidUTF8Characters(outputBuffer, input, len);
  out.resize(outputIndex);
}

template <>
void replaceInvalidUTF8Characters(
    std::string& out,
    const char* input,
    int32_t len);

} // namespace facebook::velox::functions
