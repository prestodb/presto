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
  // Single-byte ASCII character.
  ASSERT_EQ(1, tryGetCharLength("Hello", 5));

  // 2-byte character. British pound sign.
  static const char* kPound = "\u00A3tail";
  ASSERT_EQ(2, tryGetCharLength(kPound, 5));
  // First byte alone is not a valid character.
  ASSERT_EQ(-1, tryGetCharLength(kPound, 1));
  // Second byte alone is not a valid character.
  ASSERT_EQ(-1, tryGetCharLength(kPound + 1, 5));
  // ASCII character 't' after the pound sign is valid.
  ASSERT_EQ(1, tryGetCharLength(kPound + 2, 5));

  // 3-byte character. Euro sign.
  static const char* kEuro = "\u20ACtail";
  ASSERT_EQ(3, tryGetCharLength(kEuro, 5));
  // First byte or first 2 bytes alone are not a valid character.
  ASSERT_EQ(-1, tryGetCharLength(kEuro, 1));
  ASSERT_EQ(-2, tryGetCharLength(kEuro, 2));
  // Byte sequence starting from 2nd or 3rd byte is not a valid character.
  ASSERT_EQ(-1, tryGetCharLength(kEuro + 1, 5));
  ASSERT_EQ(-1, tryGetCharLength(kEuro + 2, 5));
  ASSERT_EQ(1, tryGetCharLength(kEuro + 3, 5));
  // ASCII character 't' after the euro sign is valid.
  ASSERT_EQ(1, tryGetCharLength(kPound + 4, 5));

  // 4-byte character. Musical symbol F CLEF.
  static const char* kClef = "\U0001D122tail";
  ASSERT_EQ(4, tryGetCharLength(kClef, 5));
  // First byte, first 2 bytes, or first 3 bytes alone are not a valid
  // character.
  ASSERT_EQ(-1, tryGetCharLength(kClef, 1));
  ASSERT_EQ(-2, tryGetCharLength(kClef, 2));
  ASSERT_EQ(-3, tryGetCharLength(kClef, 3));
  // Byte sequence starting from 2nd, 3rd or 4th byte is not a valid character.
  ASSERT_EQ(-1, tryGetCharLength(kClef + 1, 3));
  ASSERT_EQ(-1, tryGetCharLength(kClef + 2, 3));
  ASSERT_EQ(-1, tryGetCharLength(kClef + 3, 3));

  // ASCII character 't' after the clef sign is valid.
  ASSERT_EQ(1, tryGetCharLength(kClef + 4, 5));

  // Test overlong encoding.

  auto tryCharLength = [](const std::vector<unsigned char>& bytes) {
    return tryGetCharLength(
        reinterpret_cast<const char*>(bytes.data()), bytes.size());
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

} // namespace
} // namespace facebook::velox::functions
