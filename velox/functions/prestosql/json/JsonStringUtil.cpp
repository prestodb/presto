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
#include <array>

#include "folly/Unicode.h"

#include "velox/common/base/Exceptions.h"
#include "velox/functions/lib/Utf8Utils.h"
#include "velox/functions/prestosql/json/JsonStringUtil.h"

using namespace facebook::velox::functions;

namespace facebook::velox {
namespace {

FOLLY_ALWAYS_INLINE char hexDigit(uint8_t c) {
  VELOX_DCHECK_LT(c, 16);
  return c < 10 ? c + '0' : c - 10 + 'A';
}

FOLLY_ALWAYS_INLINE void writeHex(char16_t value, char*& out) {
  value = folly::Endian::little(value);
  *out++ = '\\';
  *out++ = 'u';
  *out++ = hexDigit((value >> 12) & 0x0F);
  *out++ = hexDigit((value >> 8) & 0x0F);
  *out++ = hexDigit((value >> 4) & 0x0F);
  *out++ = hexDigit(value & 0x0F);
}

std::array<int8_t, 128> getAsciiEscapes() {
  std::array<int8_t, 128> escapes;
  std::fill(escapes.data(), escapes.data() + 32, -1);
  escapes['"'] = '"';
  escapes['\\'] = '\\';
  escapes['\b'] = 'b';
  escapes['\t'] = 't';
  escapes['\n'] = 'n';
  escapes['\f'] = 'f';
  escapes['\r'] = 'r';
  return escapes;
}
static const std::array<int8_t, 128> asciiEscapes = getAsciiEscapes();

FOLLY_ALWAYS_INLINE void encodeAscii(int8_t value, char*& out) {
  int8_t escapeCode = asciiEscapes[value];
  if (escapeCode == 0) {
    *out++ = char(value);
  } else if (escapeCode > 0) {
    *out++ = '\\';
    *out++ = char(escapeCode);
  } else {
    writeHex(value, out);
  }
}

std::array<int8_t, 128> getEncodedAsciiSizes() {
  std::array<int8_t, 128> sizes;
  for (int c = 0; c < 128; c++) {
    int8_t escapeCode = asciiEscapes[c];
    if (escapeCode == 0) {
      sizes[c] = 1;
    } else if (escapeCode > 0) {
      sizes[c] = 2;
    } else {
      sizes[c] = 6;
    }
  }
  return sizes;
}
static const std::array<int8_t, 128> encodedAsciiSizes = getEncodedAsciiSizes();

// Encode `codePoint` value into one or two UTF-16 code units. Write each code
// unit as prefixed hexadecimals of 6 chars.
FOLLY_ALWAYS_INLINE void encodeUtf16Hex(char32_t codePoint, char*& out) {
  VELOX_DCHECK(codePoint <= 0x10FFFFu);
  // Two 16-bit code units are needed.
  if (codePoint >= 0x10000u) {
    writeHex(
        static_cast<char16_t>(
            0xD800u + (((codePoint - 0x10000u) >> 10) & 0x3FFu)),
        out);
    writeHex(
        static_cast<char16_t>(0xDC00u + ((codePoint - 0x10000u) & 0x3FFu)),
        out);
    return;
  }
  // One 16-bit code unit is needed.
  writeHex(static_cast<char16_t>(codePoint), out);
}

} // namespace

void testingEncodeUtf16Hex(char32_t codePoint, char*& out) {
  encodeUtf16Hex(codePoint, out);
}

void escapeString(const char* input, size_t length, char* output) {
  char* pos = output;

  auto* start = reinterpret_cast<const unsigned char*>(input);
  auto* end = reinterpret_cast<const unsigned char*>(input + length);
  while (start < end) {
    int count = validateAndGetNextUtf8Length(start, end);
    switch (count) {
      case 1: {
        encodeAscii(int8_t(*start), pos);
        start++;
        continue;
      }
      case 2: {
        memcpy(pos, reinterpret_cast<const char*>(start), 2);
        pos += 2;
        start += 2;
        continue;
      }
      case 3: {
        memcpy(pos, reinterpret_cast<const char*>(start), 3);
        pos += 3;
        start += 3;
        continue;
      }
      case 4: {
        char32_t codePoint = folly::utf8ToCodePoint(start, end, true);
        if (codePoint == U'\ufffd') {
          writeHex(0xFFFDu, pos);
          continue;
        }
        encodeUtf16Hex(codePoint, pos);
        continue;
      }
      default: {
        writeHex(0xFFFDu, pos);
        start++;
      }
    }
  }
}

size_t escapedStringSize(const char* input, size_t length) {
  // 6 chars that is returned by `writeHex`.
  constexpr size_t kEncodedHexSize = 6;

  size_t outSize = 0;

  auto* start = reinterpret_cast<const unsigned char*>(input);
  auto* end = reinterpret_cast<const unsigned char*>(input + length);
  while (start < end) {
    int count = validateAndGetNextUtf8Length(start, end);
    switch (count) {
      case 1:
        outSize += encodedAsciiSizes[int8_t(*start)];
        break;
      case 2:
      case 3:
        outSize += count;
        break;
      case 4:
        outSize += kEncodedHexSize * 2;
        break;
      default:
        outSize += kEncodedHexSize;
        count = 1;
    }
    start += count;
  }

  return outSize;
}

} // namespace facebook::velox
