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
#include "velox/external/utf8proc/utf8procImpl.h"
#include "velox/functions/lib/Utf8Utils.h"
#include "velox/functions/prestosql/json/JsonStringUtil.h"

using namespace facebook::velox::functions;

namespace facebook::velox {
namespace {

FOLLY_ALWAYS_INLINE char hexDigit(uint8_t c) {
  VELOX_DCHECK_LT(c, 16);
  return c < 10 ? c + '0' : c - 10 + 'A';
}

FOLLY_ALWAYS_INLINE int32_t digitToHex(char c) {
  if (c >= '0' && c <= '9') {
    return c - '0';
  }
  if (c >= 'A' && c <= 'F') {
    return c - 'A' + 10;
  }
  if (c >= 'a' && c <= 'f') {
    return c - 'a' + 10;
  }

  VELOX_USER_FAIL("Invalid escape digit: {}", c);
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

void normalizeForJsonCast(const char* input, size_t length, char* output) {
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
  VELOX_DCHECK_EQ(pos - output, normalizedSizeForJsonCast(input, length));
}

size_t normalizedSizeForJsonCast(const char* input, size_t length) {
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
        ++start;
        break;
      case 2:
      case 3:
        outSize += count;
        start += count;
        break;
      case 4: {
        auto codePoint = folly::utf8ToCodePoint(start, end, true);
        outSize += codePoint < 0x10000u ? kEncodedHexSize : 2 * kEncodedHexSize;
        break;
      }
      default:
        outSize += kEncodedHexSize;
        ++start;
    }
  }

  return outSize;
}

namespace {

FOLLY_ALWAYS_INLINE int32_t parseHex(std::string_view hexString) {
  int32_t result = 0;
  for (auto c : hexString) {
    result = (result << 4) + digitToHex(c);
  }

  return result;
}

bool isHighSurrogate(int32_t code_point) {
  return code_point >= 0xD800 && code_point <= 0xDBFF;
}

bool isLowSurrogate(int32_t code_point) {
  return code_point >= 0xDC00 && code_point <= 0xDFFF;
}

bool isSpecialCode(int32_t codePoint) {
  // Java implementation ignores some code points
  // in the first plane from 0x00 to 0x1F.

  if (codePoint >= 0 && codePoint <= 0x1F) {
    return true;
  }

  return false;
}

// Gets codepoint of a char and increments position pos
// in view to next char.
int32_t getEscapedChar(std::string_view view, size_t& pos) {
  if (FOLLY_UNLIKELY(view[pos] == '\\')) {
    switch (view[pos + 1]) {
      case '/':
        pos++;
        return '/';
      case 'u': {
        if (pos + 6 > view.size()) {
          VELOX_USER_FAIL("Invalid escape sequence at the end of string");
        }

        // Read 4 hex digits.
        auto codePoint = parseHex(std::string_view(view.data() + pos + 2, 4));
        pos += 6;
        return codePoint;
      }
      case '"':
        pos += 2;
        return '"';
      case 'b':
        pos += 2;
        return '\b';
      case 'n':
        pos += 2;
        return '\n';
      case 'f':
        pos += 2;
        return '\f';
      case 'r':
        pos += 2;
        return '\r';
      case 't':
        pos += 2;
        return '\t';
      case '\\':
        pos += 2;
        return '\\';

      default:
        // Presto java ignores bad escape sequences.
        pos += 1;
        return view[pos];
    }
  }

  // Not escaped, just return character.
  return view[pos++];
}

int32_t compareChars(
    const std::string_view& first,
    const std::string_view& second,
    size_t& i,
    size_t& j) {
  // Both are ASCII.
  if (FOLLY_LIKELY(!(first[i] & 0x80) && !(second[j] & 0x80))) {
    // Check if escaped.
    auto firstChar = getEscapedChar(first, i);
    auto secondChar = getEscapedChar(second, j);
    return firstChar - secondChar;
  } else {
    // Assume unicode.
    uint32_t firstCodePoint = 0;
    uint32_t secondCodePoint = 0;
    auto firstSize = 0;
    auto secondSize = 0;
    if (first[i] & 0x80) {
      firstCodePoint = utf8proc_codepoint(
          first.data() + i, first.data() + first.size(), firstSize);
    } else {
      firstCodePoint = first[i];
    }

    if (second[j] & 0x80) {
      secondCodePoint = utf8proc_codepoint(
          second.data() + j, second.data() + second.size(), secondSize);
    } else {
      secondCodePoint = second[j];
    }

    i += firstSize > 0 ? firstSize : 1;
    j += secondSize > 0 ? secondSize : 1;
    return firstCodePoint - secondCodePoint;
  }
}
} // namespace

bool lessThanForJsonParse(
    const std::string_view& first,
    const std::string_view& second) {
  size_t firstLength = first.size();
  size_t secondLength = second.size();
  size_t minLength = std::min(firstLength, secondLength);

  for (size_t i = 0, j = 0; i < minLength && j < minLength;) {
    auto result = compareChars(first, second, i, j);
    if (result != 0) {
      return result < 0;
    }
  }

  return firstLength < secondLength;
}

size_t normalizeForJsonParse(const char* input, size_t length, char* output) {
  char* pos = output;
  auto* start = input;
  auto* end = input + length;
  while (start < end) {
    // Unescape characters that are escaped by \ character.
    if (FOLLY_UNLIKELY(*start == '\\')) {
      VELOX_USER_CHECK(
          start + 1 != end, "Invalid escape sequence at the end of string");
      // Presto java implementation only unescapes the / character.
      switch (*(start + 1)) {
        case '/':
          *pos++ = '/';
          start += 2;
          continue;
        case 'u': {
          VELOX_USER_CHECK(
              start + 6 <= end, "Invalid escape sequence at the end of string");

          // Read 4 hex digits.
          auto codePoint = parseHex(std::string_view(start + 2, 4));

          // Presto java implementation doesnt unescape surrogate pairs.
          // Thus we just write it out in the same way as it is.
          if (isHighSurrogate(codePoint) || isLowSurrogate(codePoint) ||
              isSpecialCode(codePoint)) {
            *pos++ = '\\';
            *pos++ = 'u';
            start += 2;
            // java upper cases the code points
            for (auto k = 0; k < 4; k++) {
              *pos++ = std::toupper(start[k]);
            }

            start += 4;
            continue;
          }

          // Otherwise write it as a single code point.
          auto increment = utf8proc_encode_char(
              codePoint, reinterpret_cast<unsigned char*>(pos));
          pos += increment;
          start += 6;
          continue;
        }
        default:
          *pos++ = *start;
          *pos++ = *(start + 1);
          start += 2;
          continue;
      }
    }
    if (FOLLY_LIKELY(IS_ASCII(*start))) {
      *pos++ = *start++;
      continue;
    }
    int32_t codePoint;
    int count = tryGetUtf8CharLength(start, end - start, codePoint);
    switch (count) {
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
        if (codePoint == U'\ufffd') {
          writeHex(0xFFFDu, pos);
        } else {
          encodeUtf16Hex(codePoint, pos);
        }
        start += 4;
        continue;
      }
      default: {
        // Invalid character.
        VELOX_DCHECK_LT(count, 0);
        count = -count;
        const auto& replacement =
            getInvalidUTF8ReplacementString(start, end - start, count);
        std::memcpy(pos, replacement.data(), replacement.size());
        pos += replacement.size();
        start += count;
      }
    }
  }
  VELOX_DCHECK_EQ(pos - output, normalizedSizeForJsonParse(input, length));
  return pos - output;
}

size_t normalizedSizeForJsonParse(const char* input, size_t length) {
  auto* start = input;
  auto* end = input + length;
  size_t outSize = 0;
  while (start < end) {
    if (FOLLY_UNLIKELY(*start == '\\')) {
      VELOX_USER_CHECK(
          start + 1 != end, "Invalid escape sequence at the end of string");
      switch (*(start + 1)) {
        case '/':
          ++outSize;
          start += 2;
          continue;
        case 'u': {
          VELOX_USER_CHECK(
              start + 6 <= end, "Invalid escape sequence at the end of string");
          auto codePoint = parseHex(std::string_view(start + 2, 4));
          if (isHighSurrogate(codePoint) || isLowSurrogate(codePoint) ||
              isSpecialCode(codePoint)) {
            outSize += 6;
          } else {
            unsigned char buf[4];
            auto increment = utf8proc_encode_char(codePoint, buf);
            outSize += increment;
          }
          start += 6;
          continue;
        }
        default:
          outSize += 2;
          start += 2;
          continue;
      }
    }
    if (FOLLY_LIKELY(IS_ASCII(*start))) {
      ++outSize;
      ++start;
      continue;
    }
    int32_t codePoint;
    auto count = tryGetUtf8CharLength(start, end - start, codePoint);
    switch (count) {
      case 2:
      case 3:
        outSize += count;
        start += count;
        continue;
      case 4: {
        if (codePoint >= 0x10000u) {
          // Need to write out 2 \u escape sequences.
          outSize += 12;
        } else {
          outSize += 6;
        }
        start += 4;
        continue;
      }
      default: {
        // Invalid character.
        VELOX_DCHECK_LT(count, 0);
        count = -count;
        const auto& replacement =
            getInvalidUTF8ReplacementString(start, end - start, count);
        outSize += replacement.size();
        start += count;
      }
    }
  }
  return outSize;
}

size_t
unescapeSizeForJsonFunctions(const char* input, size_t length, bool fully) {
  auto* start = input;
  auto* end = input + length;
  size_t outSize = 0;
  while (start < end) {
    if (FOLLY_UNLIKELY(*start == '\\')) {
      VELOX_USER_CHECK(
          start + 1 != end, "Invalid escape sequence at the end of string");
      switch (*(start + 1)) {
        case '/':
          ++outSize;
          start += 2;
          continue;
        case 'u': {
          VELOX_USER_CHECK(
              start + 6 <= end, "Invalid escape sequence at the end of string");
          auto codePoint = parseHex(std::string_view(start + 2, 4));
          if (isHighSurrogate(codePoint)) {
            // Skip the next 6 characters.
            start += 6;
            // Read the next 6 characters.
            if (start + 6 > end) {
              VELOX_USER_FAIL("Invalid escape sequence at the end of string");
            }
            auto lowCodePoint = parseHex(std::string_view(start + 2, 4));
            if (!isLowSurrogate(lowCodePoint)) {
              VELOX_USER_FAIL("Invalid escape sequence at the end of string");
            }
            outSize += 4;
            start += 6;
            continue;
          } else {
            if (!isSpecialCode(codePoint)) {
              unsigned char buf[4];
              auto increment = utf8proc_encode_char(codePoint, buf);
              outSize += increment;
            } else {
              outSize += 6;
            }
          }
          start += 6;
          continue;
        }
        case 'b':
        case 'n':
        case 'f':
        case 'r':
        case 't':
        case '"': {
          if (fully) {
            ++outSize;
            start += 2;
            continue;
          }
        }
          [[fallthrough]];
        default:
          outSize += 2;
          start += 2;
          continue;
      }
    }
    if (FOLLY_LIKELY(IS_ASCII(*start))) {
      ++outSize;
      ++start;
      continue;
    }
    int32_t codePoint;
    auto count = tryGetUtf8CharLength(start, end - start, codePoint);
    switch (count) {
      case 2:
      case 3:
      case 4:
        outSize += count;
        start += count;
        continue;
      default: {
        // Invalid character.
        VELOX_DCHECK_LT(count, 0);
        count = -count;
        const auto& replacement =
            getInvalidUTF8ReplacementString(start, end - start, count);
        outSize += replacement.size();
        start += count;
      }
    }
  }
  return outSize;
}

size_t unescapeSizeForJsonCast(const char* input, size_t length) {
  return unescapeSizeForJsonFunctions(input, length, false);
}

void unescapeForJsonFunctions(
    const char* input,
    size_t length,
    char* output,
    bool fully) {
  char* pos = output;
  auto* start = input;
  auto* end = input + length;

  while (start < end) {
    if (FOLLY_UNLIKELY(*start == '\\')) {
      VELOX_USER_CHECK(
          start + 1 != end, "Invalid escape sequence at the end of string");
      switch (*(start + 1)) {
        case '/':
          *pos++ = '/';
          start += 2;
          continue;
        case 'u': {
          VELOX_USER_CHECK(
              start + 6 <= end, "Invalid escape sequence at the end of string");
          auto codePoint = parseHex(std::string_view(start + 2, 4));
          if (isHighSurrogate(codePoint)) {
            // Skip the next 6 characters.
            start += 6;
            // Read the next 6 characters.
            if (start + 6 > end) {
              VELOX_USER_FAIL("Invalid escape sequence at the end of string");
            }
            auto lowCodePoint = parseHex(std::string_view(start + 2, 4));
            if (!isLowSurrogate(lowCodePoint)) {
              VELOX_USER_FAIL("Invalid escape sequence at the end of string");
            }
            auto convertedPoint = (codePoint - 0xD800) * 0x400 +
                (lowCodePoint - 0xDC00) + 0x10000;
            auto increment = utf8proc_encode_char(
                convertedPoint, reinterpret_cast<unsigned char*>(pos));
            pos += increment;
            start += 6;
            continue;
          } else {
            if (!isSpecialCode(codePoint)) {
              auto increment = utf8proc_encode_char(
                  codePoint, reinterpret_cast<unsigned char*>(pos));
              pos += increment;
            } else {
              *pos++ = '\\';
              *pos++ = 'u';
              start += 2;
              // java upper cases the code points
              for (auto k = 0; k < 4; k++) {
                *pos++ = std::toupper(start[k]);
              }
              start += 4;
              continue;
            }
          }
          start += 6;
          continue;
        }
        case 'b':
        case 'n':
        case 'f':
        case 'r':
        case 't':
        case '"': {
          if (fully) {
            size_t index = 0;
            *pos++ = getEscapedChar(std::string_view(start, 2), index);
            start += 2;
            continue;
          }
        }
          [[fallthrough]];
        default:
          *pos++ = *start;
          *pos++ = *(start + 1);
          start += 2;
          continue;
      }
    }
    if (FOLLY_LIKELY(IS_ASCII(*start))) {
      *pos++ = *start++;
      continue;
    }
    int32_t codePoint;
    auto count = tryGetUtf8CharLength(start, end - start, codePoint);
    switch (count) {
      case 2:
      case 3:
      case 4: {
        memcpy(pos, reinterpret_cast<const char*>(start), count);
        pos += count;
        start += count;
        continue;
      }
      default: {
        // Invalid character.
        VELOX_DCHECK_LT(count, 0);
        count = -count;
        const auto& replacement =
            getInvalidUTF8ReplacementString(start, end - start, count);
        std::memcpy(pos, replacement.data(), replacement.size());
        pos += replacement.size();
        start += count;
      }
    }
  }
}
void unescapeForJsonCast(const char* input, size_t length, char* output) {
  unescapeForJsonFunctions(input, length, output, false);
}

} // namespace facebook::velox
