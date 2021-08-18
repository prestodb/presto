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

#include "velox/functions/lib/string/StringImpl.h"
#include "velox/core/CoreTypeSystem.h"
#include "velox/type/StringView.h"

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

using namespace facebook::velox;
using namespace facebook::velox::functions::stringImpl;
using namespace facebook::velox::functions::stringCore;

class StringImplTest : public testing::Test {
 public:
  std::vector<std::tuple<std::string, std::string>> getUpperAsciiTestData() {
    return {
        {"abcdefg", "ABCDEFG"},
        {"ABCDEFG", "ABCDEFG"},
        {"a B c D e F g", "A B C D E F G"},
    };
  }

  std::vector<std::tuple<std::string, std::string>> getUpperUnicodeTestData() {
    return {
        {"àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþ", "ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ"},
        {"αβγδεζηθικλμνξοπρςστυφχψ", "ΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΣΤΥΦΧΨ"},
        {"абвгдежзийклмнопрстуфхцчшщъыьэюя",
         "АБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ"}};
  }

  std::vector<std::tuple<std::string, std::string>> getLowerAsciiTestData() {
    return {
        {"ABCDEFG", "abcdefg"},
        {"abcdefg", "abcdefg"},
        {"a B c D e F g", "a b c d e f g"},
    };
  }

  std::vector<std::tuple<std::string, std::string>> getLowerUnicodeTestData() {
    return {
        {"ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ", "àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþ"},
        {"ΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΣΤΥΦΧΨ", "αβγδεζηθικλμνξοπρσστυφχψ"},
        {"АБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ",
         "абвгдежзийклмнопрстуфхцчшщъыьэюя"}};
  }
};

TEST_F(StringImplTest, upperAscii) {
  for (auto& testCase : getUpperAsciiTestData()) {
    auto input = StringView(std::get<0>(testCase));
    auto& expectedUpper = std::get<1>(testCase);

    std::string upperOutput;
    upper<StringEncodingMode::ASCII>(upperOutput, input);
    ASSERT_EQ(upperOutput, expectedUpper);

    upperOutput.clear();
    upper<StringEncodingMode::MOSTLY_ASCII>(upperOutput, input);
    ASSERT_EQ(upperOutput, expectedUpper);
  }
}

TEST_F(StringImplTest, lowerAscii) {
  for (auto& testCase : getLowerAsciiTestData()) {
    auto input = StringView(std::get<0>(testCase));
    auto& expectedLower = std::get<1>(testCase);

    std::string lowerOutput;
    lower<StringEncodingMode::ASCII>(lowerOutput, input);
    ASSERT_EQ(lowerOutput, expectedLower);

    lowerOutput.clear();
    lower<StringEncodingMode::MOSTLY_ASCII>(lowerOutput, input);
    ASSERT_EQ(lowerOutput, expectedLower);
  }
}

TEST_F(StringImplTest, upperUnicode) {
  for (auto& testCase : getUpperUnicodeTestData()) {
    auto input = StringView(std::get<0>(testCase));
    auto& expectedUpper = std::get<1>(testCase);

    std::string upperOutput;
    upper<StringEncodingMode::UTF8>(upperOutput, input);
    ASSERT_EQ(upperOutput, expectedUpper);

    upperOutput.clear();
    upper<StringEncodingMode::MOSTLY_ASCII>(upperOutput, input);
    ASSERT_EQ(upperOutput, expectedUpper);
  }
}

TEST_F(StringImplTest, lowerUnicode) {
  for (auto& testCase : getLowerUnicodeTestData()) {
    auto input = StringView(std::get<0>(testCase));
    auto& expectedLower = std::get<1>(testCase);

    std::string lowerOutput;
    lower<StringEncodingMode::UTF8>(lowerOutput, input);
    ASSERT_EQ(lowerOutput, expectedLower);

    lowerOutput.clear();
    lower<StringEncodingMode::MOSTLY_ASCII>(lowerOutput, input);
    ASSERT_EQ(lowerOutput, expectedLower);
  }
}

TEST_F(StringImplTest, concatDynamic) {
  core::StringWriter output;

  concatDynamic(output, std::vector<std::string>{"AA"});
  ASSERT_EQ(StringView("AA"), output);

  output.resize(0);
  concatDynamic(output, std::vector<std::string>{"AA", "BB"});
  ASSERT_EQ(StringView("AABB"), output);

  output.resize(0);
  concatDynamic(output, std::vector<std::string>{"AA", "BB", "CC"});
  ASSERT_EQ(StringView("AABBCC"), output);

  output.resize(0);
  concatDynamic(output, std::vector<std::string>{"AA", "", "CC"});
  ASSERT_EQ(StringView("AACC"), output);

  output.resize(0);
  concatDynamic(output, std::vector<std::string>{"hello na\u00EFve", " world"});
  ASSERT_EQ(StringView("hello na\u00EFve world"), output);
}

TEST_F(StringImplTest, concatLazy) {
  core::StringWriter output;

  // concat(lower(in1), upper(in2));
  auto f1 = [&](core::StringWriter& out) {
    std::string input("AA");
    out.reserve(out.size() + input.size());
    lowerAscii(out.data() + out.size(), input.data(), input.size());
    out.resize(out.size() + input.size());
  };

  auto f2 = [&](core::StringWriter& out) {
    std::string input("bb");
    out.reserve(out.size() + input.size());
    upperAscii(out.data() + out.size(), input.data(), input.size());
    out.resize(out.size() + input.size());
  };

  concatLazy(output, f1, f2);
  ASSERT_EQ(StringView("aaBB"), output);
}

TEST_F(StringImplTest, length) {
  auto lengthUtf8Ref = [](const char* inputBuffer, size_t bufferLength) {
    size_t size = 0;
    for (size_t i = 0; i < bufferLength; i++) {
      if ((static_cast<const unsigned char>(inputBuffer[i]) & 0xC0) != 0x80) {
        size++;
      }
    }
    return size;
  };

  // Test ascii inputs
  for (const auto& test : getUpperAsciiTestData()) {
    auto& inputString = std::get<0>(test);

    ASSERT_EQ(
        length<StringEncodingMode::ASCII>(inputString), inputString.size());
    ASSERT_EQ(
        length<StringEncodingMode::UTF8>(inputString), inputString.size());
    ASSERT_EQ(
        length<StringEncodingMode::MOSTLY_ASCII>(inputString),
        inputString.size());
  }

  // Test unicode inputs
  for (auto& test : getLowerUnicodeTestData()) {
    auto& inputString = std::get<0>(test);

    ASSERT_EQ(
        length<StringEncodingMode::UTF8>(inputString),
        lengthUtf8Ref(inputString.data(), inputString.size()));
    ASSERT_EQ(
        length<StringEncodingMode::MOSTLY_ASCII>(inputString),
        lengthUtf8Ref(inputString.data(), inputString.size()));
  }
}

TEST_F(StringImplTest, codePointToString) {
  auto testValidInput = [](const int64_t codePoint,
                           const std::string& expectedString) {
    core::StringWriter output;
    codePointToString(output, codePoint);
    ASSERT_EQ(
        StringView(expectedString), StringView(output.data(), output.size()));
  };

  auto testInvalidCodePoint = [](const int64_t codePoint) {
    core::StringWriter output;
    EXPECT_THROW(codePointToString(output, codePoint), VeloxUserError)
        << "codePoint " << codePoint;
  };

  testValidInput(65, "A");
  testValidInput(9731, "\u2603");
  testValidInput(0, std::string("\0", 1));

  testInvalidCodePoint(-1);
  testInvalidCodePoint(1234567);
  testInvalidCodePoint(8589934592);
}

TEST_F(StringImplTest, charToCodePoint) {
  auto testValidInput = [](const std::string& charString,
                           const int64_t expectedCodePoint) {
    ASSERT_EQ(charToCodePoint(StringView(charString)), expectedCodePoint);
  };

  auto testValidInputRoundTrip = [](const int64_t codePoint) {
    core::StringWriter string;
    codePointToString(string, codePoint);
    ASSERT_EQ(charToCodePoint(string), codePoint) << "codePoint " << codePoint;
  };

  auto testExpectDeath = [](const std::string& charString) {
    EXPECT_THROW(charToCodePoint(StringView(charString)), VeloxUserError)
        << "charString " << charString;
  };

  testValidInput("x", 0x78);
  testValidInput("\u840C", 0x840C);

  testValidInputRoundTrip(128077);
  testValidInputRoundTrip(33804);

  testExpectDeath("hello");
  testExpectDeath("\u666E\u5217\u65AF\u6258");
  testExpectDeath("");
}

TEST_F(StringImplTest, stringPosition) {
  auto testValidInputAscii = [](const std::string& string,
                                const std::string& substr,
                                const int64_t instance,
                                const int64_t expectedPosition) {
    ASSERT_EQ(
        stringPosition<StringEncodingMode::ASCII>(
            StringView(string), StringView(substr), instance),
        expectedPosition);
    ASSERT_EQ(
        stringPosition<StringEncodingMode::MOSTLY_ASCII>(
            StringView(string), StringView(substr), instance),
        expectedPosition);
  };

  auto testValidInputUnicode = [](const std::string& string,
                                  const std::string& substr,
                                  const int64_t instance,
                                  const int64_t expectedPosition) {
    ASSERT_EQ(
        stringPosition<StringEncodingMode::UTF8>(
            StringView(string), StringView(substr), instance),
        expectedPosition);
    ASSERT_EQ(
        stringPosition<StringEncodingMode::MOSTLY_ASCII>(
            StringView(string), StringView(substr), instance),
        expectedPosition);
  };

  testValidInputAscii("high", "ig", 1, 2L);
  testValidInputAscii("high", "igx", 1, 0L);
  testValidInputAscii("Quadratically", "a", 1, 3L);
  testValidInputAscii("foobar", "foobar", 1, 1L);
  testValidInputAscii("foobar", "obar", 1, 3L);
  testValidInputAscii("zoo!", "!", 1, 4L);
  testValidInputAscii("x", "", 1, 1L);
  testValidInputAscii("", "", 1, 1L);
  testValidInputAscii("abc/xyz/foo/bar", "/", 3, 12L);

  testValidInputUnicode("\u4FE1\u5FF5,\u7231,\u5E0C\u671B", "\u7231", 1, 4L);
  testValidInputUnicode(
      "\u4FE1\u5FF5,\u7231,\u5E0C\u671B", "\u5E0C\u671B", 1, 6L);
  testValidInputUnicode("\u4FE1\u5FF5,\u7231,\u5E0C\u671B", "nice", 1, 0L);

  testValidInputUnicode("abc/xyz/foo/bar", "/", 1, 4L);
  testValidInputUnicode("abc/xyz/foo/bar", "/", 2, 8L);
  testValidInputUnicode("abc/xyz/foo/bar", "/", 3, 12L);
  testValidInputUnicode("abc/xyz/foo/bar", "/", 4, 0L);

  EXPECT_THROW(
      stringPosition<StringEncodingMode::MOSTLY_ASCII>(
          StringView("foobar"), StringView("foobar"), 0),
      VeloxUserError);
}

TEST_F(StringImplTest, replace) {
  auto runTest = [](const std::string& string,
                    const std::string& replaced,
                    const std::string& replacement,
                    const std::string& expectedResults) {
    // Test out of place
    core::StringWriter output;
    replace(
        output,
        StringView(string),
        StringView(replaced),
        StringView(replacement));

    ASSERT_EQ(
        StringView(output.data(), output.size()), StringView(expectedResults));

    // Test in place
    if (replacement.size() <= replaced.size()) {
      core::StringWriter inOutString;
      inOutString.resize(string.size());
      if (string.size()) {
        std::memcpy(inOutString.data(), string.data(), string.size());
      }

      replaceInPlace(
          inOutString, StringView(replaced), StringView(replacement));
      ASSERT_EQ(
          StringView(inOutString.data(), inOutString.size()),
          StringView(expectedResults));
    }
  };

  runTest("aaa", "a", "aa", "aaaaaa");
  runTest("abcdefabcdef", "cd", "XX", "abXXefabXXef");
  runTest("abcdefabcdef", "cd", "", "abefabef");
  runTest("123123tech", "123", "", "tech");
  runTest("123tech123", "123", "", "tech");
  runTest("222tech", "2", "3", "333tech");
  runTest("0000123", "0", "", "123");
  runTest("0000123", "0", " ", "    123");
  runTest("foo", "", "", "foo");
  runTest("foo", "foo", "", "");
  runTest("abc", "", "xx", "xxaxxbxxcxx");
  runTest("", "", "xx", "");
  runTest("", "", "", "");

  runTest(
      "\u4FE1\u5FF5,\u7231,\u5E0C\u671B",
      ",",
      "\u2014",
      "\u4FE1\u5FF5\u2014\u7231\u2014\u5E0C\u671B");
  runTest("\u00D6sterreich", "\u00D6", "Oe", "Oesterreich");
}

TEST_F(StringImplTest, getByteRange) {
  // Unicode string
  char* unicodeString = (char*)"\uFE3D\uFE4B\uFF05abc";

  // Number of characters
  int unicodeStringCharacters = 6;

  // Size of all its prefixes
  std::array<const char*, 7> unicodeStringPrefixes{
      "", // dummy
      "",
      "\uFE3D",
      "\uFE3D\uFE4B",
      "\uFE3D\uFE4B\uFF05",
      "\uFE3D\uFE4B\uFF05a",
      "\uFE3D\uFE4B\uFF05ab",
  };

  // Locations precomputed in bytes
  std::vector<int> locationInBytes(7);
  for (int i = 1; i <= unicodeStringCharacters; i++) {
    locationInBytes[i] = strlen(unicodeStringPrefixes[i]);
  }

  // Test getByteRange
  for (int i = 1; i <= unicodeStringCharacters; i++) {
    auto expectedStartByteIndex = locationInBytes[i];
    auto expectedEndByteIndex = strlen(unicodeString);

    // Find the byte range of unicodeString[i, end]
    auto range =
        getByteRange<StringEncodingMode::UTF8>(unicodeString, i, 6 - i + 1);

    EXPECT_EQ(expectedStartByteIndex, range.first);
    EXPECT_EQ(expectedEndByteIndex, range.second);

    range = getByteRange<StringEncodingMode::MOSTLY_ASCII>(
        unicodeString, i, 6 - i + 1);

    EXPECT_EQ(expectedStartByteIndex, range.first);
    EXPECT_EQ(expectedEndByteIndex, range.second);
  }
}
