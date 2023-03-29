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
#include "velox/functions/prestosql/Utf8Utils.h"
#include "velox/common/base/Exceptions.h"
#include "velox/external/utf8proc/utf8procImpl.h"

namespace facebook::velox::functions {

int32_t tryGetCharLength(const char* input, int64_t size) {
  VELOX_DCHECK_NOT_NULL(input);
  VELOX_DCHECK_GT(size, 0);

  auto charLength = utf8proc_char_length(input);
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

  VELOX_UNREACHABLE();
}
} // namespace facebook::velox::functions
