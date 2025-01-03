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

#include <gtest/gtest.h>
#include "velox/functions/lib/Utf8Utils.h"

namespace facebook::velox::functions {
namespace {

TEST(Utf8Test, tryCharLength) {
  int32_t codepoint;
  // Single-byte ASCII character.
  ASSERT_EQ(1, tryGetUtf8CharLength("Hello", 5, codepoint));
  ASSERT_EQ('H', codepoint);

  // 2-byte character. British pound sign.
  static const char* kPound = "\u00A3tail";
  ASSERT_EQ(2, tryGetUtf8CharLength(kPound, 5, codepoint));
  ASSERT_EQ(0xA3, codepoint);
  // First byte alone is not a valid character.
  ASSERT_EQ(-1, tryGetUtf8CharLength(kPound, 1, codepoint));
  // Second byte alone is not a valid character.
  ASSERT_EQ(-1, tryGetUtf8CharLength(kPound + 1, 5, codepoint));
  // ASCII character 't' after the pound sign is valid.
  ASSERT_EQ(1, tryGetUtf8CharLength(kPound + 2, 5, codepoint));

  // 3-byte character. Euro sign.
  static const char* kEuro = "\u20ACtail";
  ASSERT_EQ(3, tryGetUtf8CharLength(kEuro, 5, codepoint));
  ASSERT_EQ(0x20AC, codepoint);
  // First byte or first 2 bytes alone are not a valid character.
  ASSERT_EQ(-1, tryGetUtf8CharLength(kEuro, 1, codepoint));
  ASSERT_EQ(-2, tryGetUtf8CharLength(kEuro, 2, codepoint));
  // Byte sequence starting from 2nd or 3rd byte is not a valid character.
  ASSERT_EQ(-1, tryGetUtf8CharLength(kEuro + 1, 5, codepoint));
  ASSERT_EQ(-1, tryGetUtf8CharLength(kEuro + 2, 5, codepoint));
  // ASCII character 't' after the euro sign is valid.
  ASSERT_EQ(1, tryGetUtf8CharLength(kEuro + 3, 5, codepoint));
  ASSERT_EQ('t', codepoint);
  ASSERT_EQ(1, tryGetUtf8CharLength(kEuro + 4, 5, codepoint));
  ASSERT_EQ('a', codepoint);

  // 4-byte character. Musical symbol F CLEF.
  static const char* kClef = "\U0001D122tail";
  ASSERT_EQ(4, tryGetUtf8CharLength(kClef, 5, codepoint));
  ASSERT_EQ(0x1D122, codepoint);
  // First byte, first 2 bytes, or first 3 bytes alone are not a valid
  // character.
  ASSERT_EQ(-1, tryGetUtf8CharLength(kClef, 1, codepoint));
  ASSERT_EQ(-2, tryGetUtf8CharLength(kClef, 2, codepoint));
  ASSERT_EQ(-3, tryGetUtf8CharLength(kClef, 3, codepoint));
  // Byte sequence starting from 2nd, 3rd or 4th byte is not a valid character.
  ASSERT_EQ(-1, tryGetUtf8CharLength(kClef + 1, 3, codepoint));
  ASSERT_EQ(-1, tryGetUtf8CharLength(kClef + 2, 3, codepoint));
  ASSERT_EQ(-1, tryGetUtf8CharLength(kClef + 3, 3, codepoint));

  // ASCII character 't' after the clef sign is valid.
  ASSERT_EQ(1, tryGetUtf8CharLength(kClef + 4, 5, codepoint));
  ASSERT_EQ('t', codepoint);

  // Test overlong encoding.

  auto tryCharLength = [](const std::vector<unsigned char>& bytes) {
    int32_t codepoint;
    return tryGetUtf8CharLength(
        reinterpret_cast<const char*>(bytes.data()), bytes.size(), codepoint);
  };

  // 2-byte encoding of 0x2F.
  ASSERT_EQ(-2, tryCharLength({0b11000000, 0xAF}));
  // 2-byte encoding of 0x7F.
  ASSERT_EQ(-2, tryCharLength({0b11000001, 0xBF}));
  // 3-byte encoding of 0x7F.
  ASSERT_EQ(-3, tryCharLength({0b11100000, 0x81, 0xBF}));
  // 3-byte encoding of 0x400.
  ASSERT_EQ(-3, tryCharLength({0b11100000, 0x90, 0x80}));
  // 3-byte encoding of 0x7FF.
  ASSERT_EQ(-3, tryCharLength({0b11100000, 0x9F, 0xBF}));
  // 4-byte encoding of 0xD800.
  ASSERT_EQ(-4, tryCharLength({0b11110000, 0x8D, 0xA0, 0x80}));
  // 4-byte encoding of 0xDFFF.
  ASSERT_EQ(-4, tryCharLength({0b11110000, 0x8D, 0xBF, 0xBF}));
  // 4-byte encoding of 0xFFFF.
  ASSERT_EQ(-4, tryCharLength({0b11110000, 0x8F, 0xBF, 0xBF}));

  // Test min and max surrogate characters.
  ASSERT_EQ(-3, tryCharLength({0b11101101, 0xA0, 0x80}));
  ASSERT_EQ(-3, tryCharLength({0b11101101, 0xBF, 0xBF}));

  // Invalid single-byte character.
  ASSERT_EQ(-1, tryCharLength({0xBF}));
}

TEST(UTF8Test, replaceInvalidUTF8Characters) {
  auto testReplaceInvalidUTF8Chars = [](const std::string& input,
                                        const std::string& expected) {
    std::string output;
    replaceInvalidUTF8Characters(output, input.data(), input.size());
    ASSERT_EQ(expected, output);
  };

  // Good case
  testReplaceInvalidUTF8Chars("Hello World", "Hello World");
  // Bad encoding
  testReplaceInvalidUTF8Chars("hello \xBF world", "hello � world");
  // Bad encoding with 3 byte char
  testReplaceInvalidUTF8Chars("hello \xe0\x94\x83 world", "hello ��� world");
  // Bad encoding with 4 byte char
  testReplaceInvalidUTF8Chars(
      "hello \xf0\x80\x80\x80\x80 world", "hello ����� world");

  // Overlong 4 byte utf8 character.
  testReplaceInvalidUTF8Chars(
      "hello \xef\xbf\xbd\xef\xbf\xbd world", "hello �� world");

  // Test invalid byte 0xC0
  testReplaceInvalidUTF8Chars(
      "hello \xef\xbf\xbd\xef\xbf\xbd world", "hello �� world");

  // Test long 4 byte utf8 with continuation byte
  testReplaceInvalidUTF8Chars(
      "hello \xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd world",
      "hello ���� world");
}

} // namespace
} // namespace facebook::velox::functions
