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
/// @return the length of the code point or negative the number of bytes in the
/// invalid UTF-8 sequence.
///
/// Adapted from tryGetCodePointAt in
/// https://github.com/airlift/slice/blob/master/src/main/java/io/airlift/slice/SliceUtf8.java
int32_t tryGetCharLength(const char* input, int64_t size);

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

} // namespace facebook::velox::functions
