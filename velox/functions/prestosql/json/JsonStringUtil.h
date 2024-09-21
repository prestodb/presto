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

namespace facebook::velox {
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
void escapeString(const char* input, size_t length, char* output);

/// Return the size of string after the unicode characters of `input` are
/// escaped using the method as in`escapeString`. The function will iterate
/// over `input` once.
/// @param input: Input string to escape that is UTF-8 encoded.
/// @param length: Length of the input string.
size_t escapedStringSize(const char* input, size_t length);

/// For test only. Encode `codePoint` value by UTF-16 and write the one or two
/// prefixed hexadecimals to `out`. Move `out` forward by 6 or 12 chars
/// accordingly. The caller shall ensure there is enough space in `out`.
void testingEncodeUtf16Hex(char32_t codePoint, char*& out);
} // namespace facebook::velox
