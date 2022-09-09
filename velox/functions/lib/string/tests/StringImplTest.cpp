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
    upper</*ascii*/ true>(upperOutput, input);
    ASSERT_EQ(upperOutput, expectedUpper);

    upperOutput.clear();
    upper</*ascii*/ false>(upperOutput, input);
    ASSERT_EQ(upperOutput, expectedUpper);
  }
}

TEST_F(StringImplTest, lowerAscii) {
  for (auto& testCase : getLowerAsciiTestData()) {
    auto input = StringView(std::get<0>(testCase));
    auto& expectedLower = std::get<1>(testCase);

    std::string lowerOutput;
    lower</*ascii*/ true>(lowerOutput, input);
    ASSERT_EQ(lowerOutput, expectedLower);

    lowerOutput.clear();
    lower</*ascii*/ false>(lowerOutput, input);
    ASSERT_EQ(lowerOutput, expectedLower);
  }
}

TEST_F(StringImplTest, upperUnicode) {
  for (auto& testCase : getUpperUnicodeTestData()) {
    auto input = StringView(std::get<0>(testCase));
    auto& expectedUpper = std::get<1>(testCase);

    std::string upperOutput;
    upper</*ascii*/ false>(upperOutput, input);
    ASSERT_EQ(upperOutput, expectedUpper);

    upperOutput.clear();
    upper</*ascii*/ false>(upperOutput, input);
    ASSERT_EQ(upperOutput, expectedUpper);
  }
}

TEST_F(StringImplTest, lowerUnicode) {
  for (auto& testCase : getLowerUnicodeTestData()) {
    auto input = StringView(std::get<0>(testCase));
    auto& expectedLower = std::get<1>(testCase);

    std::string lowerOutput;
    lower</*ascii*/ false>(lowerOutput, input);
    ASSERT_EQ(lowerOutput, expectedLower);

    lowerOutput.clear();
    lower</*ascii*/ false>(lowerOutput, input);
    ASSERT_EQ(lowerOutput, expectedLower);
  }
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

    ASSERT_EQ(length</*isAscii*/ true>(inputString), inputString.size());
    ASSERT_EQ(length</*isAscii*/ false>(inputString), inputString.size());
    ASSERT_EQ(length</*isAscii*/ false>(inputString), inputString.size());
  }

  // Test unicode inputs
  for (auto& test : getLowerUnicodeTestData()) {
    auto& inputString = std::get<0>(test);

    ASSERT_EQ(
        length</*isAscii*/ false>(inputString),
        lengthUtf8Ref(inputString.data(), inputString.size()));
    ASSERT_EQ(
        length</*isAscii*/ false>(inputString),
        lengthUtf8Ref(inputString.data(), inputString.size()));
  }
}

TEST_F(StringImplTest, badUnicodeLength) {
  ASSERT_EQ(0, length</*isAscii*/ false>(std::string("")));
  ASSERT_EQ(2, length</*isAscii*/ false>(std::string("ab")));
  // Try a bunch of special case unicode chars
  ASSERT_EQ(1, length</*isAscii*/ false>(std::string("\u04FF")));
  ASSERT_EQ(1, length</*isAscii*/ false>(std::string("\U000E002F")));
  ASSERT_EQ(1, length</*isAscii*/ false>(std::string("\U0001D437")));
  ASSERT_EQ(1, length</*isAscii*/ false>(std::string("\U00002799")));

  std::string str;
  str.resize(2);
  // Create corrupt data below.
  char16_t c = u'\u04FF';
  str[0] = (char)c;
  str[1] = (char)c;

  auto len = length</*isAscii*/ false>(str);
  ASSERT_EQ(2, len);
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
        stringPosition</*isAscii*/ true>(
            StringView(string), StringView(substr), instance),
        expectedPosition);
    ASSERT_EQ(
        stringPosition</*isAscii*/ false>(
            StringView(string), StringView(substr), instance),
        expectedPosition);
  };

  auto testValidInputUnicode = [](const std::string& string,
                                  const std::string& substr,
                                  const int64_t instance,
                                  const int64_t expectedPosition) {
    ASSERT_EQ(
        stringPosition</*isAscii*/ false>(
            StringView(string), StringView(substr), instance),
        expectedPosition);
    ASSERT_EQ(
        stringPosition</*isAscii*/ false>(
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
      stringPosition</*isAscii*/ false>(
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
    auto range = getByteRange</*isAscii*/ false>(unicodeString, i, 6 - i + 1);

    EXPECT_EQ(expectedStartByteIndex, range.first);
    EXPECT_EQ(expectedEndByteIndex, range.second);

    range = getByteRange</*isAscii*/ false>(unicodeString, i, 6 - i + 1);

    EXPECT_EQ(expectedStartByteIndex, range.first);
    EXPECT_EQ(expectedEndByteIndex, range.second);
  }
}

TEST_F(StringImplTest, pad) {
  auto runTest = [](const std::string& string,
                    const int64_t size,
                    const std::string& padString,
                    const std::string& expectedLpadResult,
                    const std::string& expectedRpadResult) {
    core::StringWriter lpadOutput;
    core::StringWriter rpadOutput;

    bool stringIsAscii = isAscii(string.c_str(), string.size());
    bool padStringIsAscii = isAscii(padString.c_str(), padString.size());
    if (stringIsAscii && padStringIsAscii) {
      facebook::velox::functions::stringImpl::
          pad<true /*lpad*/, true /*isAscii*/>(
              lpadOutput, StringView(string), size, StringView(padString));
      facebook::velox::functions::stringImpl::
          pad<false /*lpad*/, true /*isAscii*/>(
              rpadOutput, StringView(string), size, StringView(padString));
    } else {
      // At least one of the string args is non-ASCII
      facebook::velox::functions::stringImpl::
          pad<true /*lpad*/, false /*IsAscii*/>(
              lpadOutput, StringView(string), size, StringView(padString));
      facebook::velox::functions::stringImpl::
          pad<false /*lpad*/, false /*IsAscii*/>(
              rpadOutput, StringView(string), size, StringView(padString));
    }

    ASSERT_EQ(
        StringView(lpadOutput.data(), lpadOutput.size()),
        StringView(expectedLpadResult));
    ASSERT_EQ(
        StringView(rpadOutput.data(), rpadOutput.size()),
        StringView(expectedRpadResult));
  };

  auto runTestUserError = [](const std::string& string,
                             const int64_t size,
                             const std::string& padString) {
    core::StringWriter output;

    EXPECT_THROW(
        (facebook::velox::functions::stringImpl::pad<true, true>(
            output, StringView(string), size, StringView(padString))),
        VeloxUserError);
  };

  // ASCII string with various values for size and padString
  runTest("text", 5, "x", "xtext", "textx");
  runTest("text", 4, "x", "text", "text");
  runTest("text", 6, "xy", "xytext", "textxy");
  runTest("text", 7, "xy", "xyxtext", "textxyx");
  runTest("text", 9, "xyz", "xyzxytext", "textxyzxy");
  // Non-ASCII string with various values for size and padString
  runTest(
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      10,
      "\u671B",
      "\u671B\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u671B");
  runTest(
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      11,
      "\u671B",
      "\u671B\u671B\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u671B\u671B");
  runTest(
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      12,
      "\u5E0C\u671B",
      "\u5E0C\u671B\u5E0C\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u5E0C\u671B\u5E0C");
  runTest(
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      13,
      "\u5E0C\u671B",
      "\u5E0C\u671B\u5E0C\u671B\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u5E0C\u671B\u5E0C\u671B");
  // Empty string
  runTest("", 3, "a", "aaa", "aaa");
  // Truncating string
  runTest("abc", 0, "e", "", "");
  runTest("text", 3, "xy", "tex", "tex");
  runTest(
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      5,
      "\u671B",
      "\u4FE1\u5FF5 \u7231 ",
      "\u4FE1\u5FF5 \u7231 ");

  // Empty padString
  runTestUserError("text", 10, "");
  // size outside the allowed range
  runTestUserError("text", -1, "a");
  runTestUserError(
      "text", ((int64_t)std::numeric_limits<int32_t>::max()) + 1, "a");
}
