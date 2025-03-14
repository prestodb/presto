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

#include "velox/common/base/SimdUtil.h"

namespace facebook::velox {

/// Note the below function is only used for converting Varchar to JSON
/// and thus escapes the varchar.
/// Escape the unicode characters of `input` to make it canonical for JSON
/// and legal to print in JSON text. It is assumed that the input is UTF-8
/// encoded.
/// It handles the different unicode planes or code point ranges as follows,
/// 1. Basic Multilingual Plane [0, 0xFFFF]
///    a. [0, 0x7F] ASCII. Input is encoded by one UTF-8 byte. Refer to
///       the `encodeAscii` function for output.
///    b. [0x80, 0x07FF]. Input is encoded by two UTF-8 bytes. Output the UTF-8
///       encoding of the code point, which are thus identical bytes as
///       the input.
///    c. [0x0800, 0xD7FF] + [0xE000, 0xFFFF]. Input is encoded by three UTF-8
///       bytes. Output the UTF-8 encoding of the code point, which are thus
///       identical bytes as the input.
/// 2. 16 Supplementary Planes [0x10000, 0x10FFFF]
///    a. [0x10000, 0x10FFFF]. Input is encoded by four UTF-8 bytes. Output
///       the UTF-16 encoding of the code point, with two UTF-16 code units in
///       uppercase hexadecimal and prefixed with '\' and 'u'.
/// For illegal code point value or invalid UTF-8 input, return "\uFFFD".
/// @param input: Input string to escape that is UTF-8 encoded.
/// @param length: Length of the input string.
/// @param output: Output string to write the escaped input to. The caller is
///                responsible to allocate enough space for output.
void normalizeForJsonCast(const char* input, size_t length, char* output);

/// Return the size of string after the unicode characters of `input` are
/// escaped using the method as in`escapeString`. The function will iterate
/// over `input` once.
/// @param input: Input string to escape that is UTF-8 encoded.
/// @param length: Length of the input string.
/// @return The size of the string after escaping.
size_t normalizedSizeForJsonCast(const char* input, size_t length);

/// Unescape the unicode characters of `input` to make it canonical for JSON
/// The behavior is compatible with Presto Java's json_parse.
/// Presto java json_parse will unescape the following characters:
/// \/ and non surrogate unicode characters.
/// Other behavior is similar to escapeString.
/// @param input: Input string to escape that is UTF-8 encoded.
/// @param length: Length of the input string.
/// @param output: Output string to write the escaped input to. The caller is
///                responsible to allocate enough space for output.
/// @return The number of bytes written to the output.
size_t normalizeForJsonParse(const char* input, size_t length, char* output);

size_t normalizedSizeForJsonParse(const char* input, size_t length);

/// Return whether the string need normalize or special treatment for sort
/// object string keys (i.e. lessThanForJsonParse below).
inline bool needNormalizeForJsonParse(const char* input, size_t length) {
  const auto unicodeMask = xsimd::broadcast<uint8_t>(0x80);
  const auto escape = xsimd::broadcast<uint8_t>('\\');
  size_t i = 0;
  for (; i + unicodeMask.size <= length; i += unicodeMask.size) {
    auto batch =
        xsimd::load_unaligned(reinterpret_cast<const uint8_t*>(input) + i);
    if (xsimd::any(batch >= unicodeMask || batch == escape)) {
      return true;
    }
  }
  for (; i < length; ++i) {
    if ((input[i] & 0x80) || (input[i] == '\\')) {
      return true;
    }
  }
  return false;
}

/// Unescape for JSON casting. This is used when going from
/// JSON  -> ARRAY(JSON) or JSON -> MAP(_, JSON) etc.
/// In these cases Presto unescapes the string, replacing \\ with \, \n with
/// newline, etc. The function will return valid utf-8
void unescapeForJsonCast(const char* input, size_t length, char* output);

/// Size of output buffer required when unescaping for json cast.
size_t unescapeSizeForJsonCast(const char* input, size_t length);

/// Compares two string views. The comparison takes into account
/// escape sequences and also unicode characters.
/// Returns true if first is less than second else false.
/// @param first: First string to compare.
/// @param second: Second string to compare.
bool lessThanForJsonParse(
    const std::string_view& first,
    const std::string_view& second);

/// For test only. Encode `codePoint` value by UTF-16 and write the one or two
/// prefixed hexadecimals to `out`. Move `out` forward by 6 or 12 chars
/// accordingly. The caller shall ensure there is enough space in `out`.
void testingEncodeUtf16Hex(char32_t codePoint, char*& out);

} // namespace facebook::velox
