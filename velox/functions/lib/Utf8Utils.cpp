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
#include "velox/functions/lib/Utf8Utils.h"
#include "velox/common/base/Exceptions.h"
#include "velox/external/utf8proc/utf8procImpl.h"

namespace facebook::velox::functions {
namespace {

// Returns the length of a UTF-8 character indicated by the first byte. Returns
// -1 for invalid UTF-8 first byte.
int firstByteCharLength(const char* u_input) {
  auto u = (const unsigned char*)u_input;
  unsigned char u0 = u[0];
  if (u0 < 0b10000000) {
    // normal ASCII
    // 0xxx_xxxx
    return 1;
  }
  if (u0 < 0b11000000) {
    // illegal bytes
    // 10xx_xxxx
    return -1;
  }
  if (u0 < 0b11100000) {
    // 110x_xxxx 10xx_xxxx
    return 2;
  }
  if (u0 < 0b11110000) {
    // 1110_xxxx 10xx_xxxx 10xx_xxxx
    return 3;
  }
  if (u0 < 0b11111000) {
    // 1111_0xxx 10xx_xxxx 10xx_xxxx 10xx_xxxx
    return 4;
  }
  if (u0 < 0b11111100) {
    // 1111_10xx 10xx_xxxx 10xx_xxxx 10xx_xxxx 10xx_xxxx
    return 5;
  }
  if (u0 < 0b11111110) {
    // 1111_10xx 10xx_xxxx 10xx_xxxx 10xx_xxxx 10xx_xxxx
    return 6;
  }
  // No unicode codepoint can be longer than 6 bytes.
  return -1;
}

} // namespace

int32_t tryGetCharLength(const char* input, int64_t size) {
  VELOX_DCHECK_NOT_NULL(input);
  VELOX_DCHECK_GT(size, 0);

  auto charLength = firstByteCharLength(input);
  if (charLength < 0) {
    return -1;
  }

  if (charLength == 1) {
    // Normal ASCII: 0xxx_xxxx.
    return 1;
  }

  auto firstByte = input[0];

  // Process second byte.
  if (size < 2) {
    return -1;
  }

  auto secondByte = input[1];
  if (!utf_cont(secondByte)) {
    return -1;
  }

  if (charLength == 2) {
    // 110x_xxxx 10xx_xxxx
    int codePoint = ((firstByte & 0b00011111) << 6) | (secondByte & 0b00111111);
    // Fail if overlong encoding.
    return codePoint < 0x80 ? -2 : 2;
  }

  // Process third byte.
  if (size < 3) {
    return -2;
  }

  auto thirdByte = input[2];
  if (!utf_cont(thirdByte)) {
    return -2;
  }

  if (charLength == 3) {
    // 1110_xxxx 10xx_xxxx 10xx_xxxx
    int codePoint = ((firstByte & 0b00001111) << 12) |
        ((secondByte & 0b00111111) << 6) | (thirdByte & 0b00111111);

    // Surrogates are invalid.
    static const int kMinSurrogate = 0xd800;
    static const int kMaxSurrogate = 0xdfff;
    if (kMinSurrogate <= codePoint && codePoint <= kMaxSurrogate) {
      return -3;
    }

    // Fail if overlong encoding.
    return codePoint < 0x800 ? -3 : 3;
  }

  // Process forth byte.
  if (size < 4) {
    return -3;
  }

  auto forthByte = input[3];
  if (!utf_cont(forthByte)) {
    return -3;
  }

  if (charLength == 4) {
    // 1111_0xxx 10xx_xxxx 10xx_xxxx 10xx_xxxx
    int codePoint = ((firstByte & 0b00000111) << 18) |
        ((secondByte & 0b00111111) << 12) | ((thirdByte & 0b00111111) << 6) |
        (forthByte & 0b00111111);
    // Fail if overlong encoding or above upper bound of Unicode.
    if (codePoint < 0x110000 && codePoint >= 0x10000) {
      return 4;
    }
    return -4;
  }

  if (size < 5) {
    return -4;
  }

  auto fifthByte = input[4];
  if (!utf_cont(fifthByte)) {
    return -4;
  }

  if (charLength == 5) {
    // Per RFC3629, UTF-8 is limited to 4 bytes, so more bytes are illegal.
    return -5;
  }

  if (size < 6) {
    return -5;
  }

  auto sixthByte = input[5];
  if (!utf_cont(sixthByte)) {
    return -5;
  }

  if (charLength == 6) {
    // Per RFC3629, UTF-8 is limited to 4 bytes, so more bytes are illegal.
    return -6;
  }
  // for longer sequence, which can't happen.
  return -1;
}

} // namespace facebook::velox::functions
