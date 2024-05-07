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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox::functions {

namespace {

class FromUtf8Test : public test::FunctionBaseTest {
 protected:
  std::optional<std::string> fromUtf8(std::optional<std::string> value) {
    return evaluateOnce<std::string>("from_utf8(c0)", VARBINARY(), value);
  }

  std::optional<std::string> fromUtf8(
      std::optional<std::string> value,
      std::optional<int64_t> replacement) {
    auto data = makeRowVector({
        makeNullableFlatVector<std::string>({value}, VARBINARY()),
        makeNullableFlatVector<int64_t>({replacement}),
    });
    return evaluateOnce<std::string>("from_utf8(c0, c1)", data);
  }

  std::optional<std::string> fromUtf8(
      std::optional<std::string> value,
      std::optional<std::string> replacement) {
    auto data = makeRowVector({
        makeNullableFlatVector<std::string>({value}, VARBINARY()),
        makeNullableFlatVector<std::string>({replacement}),
    });
    return evaluateOnce<std::string>("from_utf8(c0, c1)", data);
  }

  static const char* kPound; // 2-byte pound sign.
  static const char* kEuro; // 3-byte euro sign.
  static const char* kClef; // 4 byte musical symbol F CLEF.

  static const std::vector<std::string>& validMultiByte() {
    static const std::vector<std::string> chars = {kPound, kEuro, kClef};
    return chars;
  }

  static const std::vector<std::string>& validAscii() {
    auto makeAscii = []() {
      std::vector<std::string> chars;
      for (auto c = 0; c < 128; ++c) {
        chars.push_back(std::string(1, c));
      }

      std::mt19937 randGen(std::random_device{}());
      std::shuffle(chars.begin(), chars.end(), randGen);

      return chars;
    };

    static const auto chars = makeAscii();
    return chars;
  }

  static const std::vector<std::string>& invalid() {
    auto fromBytes = [&](const std::vector<unsigned char>& bytes) {
      std::string s;
      s.resize(bytes.size());
      memcpy(s.data(), bytes.data(), bytes.size());
      return s;
    };

    static const std::vector<std::string> chars = {
        // Invalid single-byte character.
        fromBytes({0xBF}),
        // Overlong sequences.
        fromBytes({0b11000000, 0xAF}),
        fromBytes({0b11100000, 0x81, 0xBF}),
        fromBytes({0b11110000, 0x8D, 0xA0, 0x80}),
        // Surrogate.
        fromBytes({0b11101101, 0xA0, 0x80}),
        // Partial sequences.
        std::string(kPound).substr(1),
        std::string(kEuro).substr(0, 1),
        std::string(kEuro).substr(0, 2),
        std::string(kClef).substr(0, 1),
        std::string(kClef).substr(0, 2),
        std::string(kClef).substr(0, 3),
    };

    return chars;
  }
};

// static
const char* FromUtf8Test::kPound = "\u00A3";

// static
const char* FromUtf8Test::kEuro = "\u20AC";

// static
const char* FromUtf8Test::kClef = "\U0001D122";

std::string repeat(const std::string& s, uint32_t n) {
  std::ostringstream out;
  for (auto i = 0; i < n; ++i) {
    out << s;
  }
  return out.str();
}

TEST_F(FromUtf8Test, basic) {
  EXPECT_EQ("", fromUtf8(""));
  EXPECT_EQ("hello", fromUtf8("hello"));

  auto allAscii = folly::join("", validAscii());

  std::vector<std::string> chars = validMultiByte();
  chars.insert(chars.end(), validAscii().begin(), validAscii().end());

  std::mt19937 randGen(std::random_device{}());
  std::shuffle(chars.begin(), chars.end(), randGen);

  auto allValid = folly::join("", chars);
  auto numValid = allValid.size();

  EXPECT_EQ(allAscii, fromUtf8(allAscii));
  EXPECT_EQ(allAscii, fromUtf8(allAscii, '#'));

  EXPECT_EQ(allValid, fromUtf8(allValid));
  EXPECT_EQ(allValid, fromUtf8(allValid, '#'));

  auto allInvalid = folly::join("", invalid());
  auto numInvalid = invalid().size();

  EXPECT_EQ(repeat("\uFFFD", numInvalid), fromUtf8(allInvalid));
  EXPECT_EQ(repeat("#", numInvalid), fromUtf8(allInvalid, '#'));
  EXPECT_EQ("", fromUtf8(allInvalid, ""));
  EXPECT_EQ(repeat(kClef, numInvalid), fromUtf8(allInvalid, kClef));

  // Invalid characters at the beginning.
  EXPECT_EQ(
      repeat("\uFFFD", numInvalid) + allValid, fromUtf8(allInvalid + allValid));
  EXPECT_EQ(
      repeat("#", numInvalid) + allValid, fromUtf8(allInvalid + allValid, '#'));
  EXPECT_EQ(allValid, fromUtf8(allInvalid + allValid, ""));
  EXPECT_EQ(
      repeat(kClef, numInvalid) + allValid,
      fromUtf8(allInvalid + allValid, kClef));

  // Invalid characters at the end.
  EXPECT_EQ(
      allValid + repeat("\uFFFD", numInvalid), fromUtf8(allValid + allInvalid));
  EXPECT_EQ(
      allValid + repeat("#", numInvalid), fromUtf8(allValid + allInvalid, '#'));
  EXPECT_EQ(allValid, fromUtf8(allValid + allInvalid, ""));
  EXPECT_EQ(
      allValid + repeat(kClef, numInvalid),
      fromUtf8(allValid + allInvalid, kClef));

  // Invalid characters in the middle.
  EXPECT_EQ(
      allValid + repeat("\uFFFD", numInvalid) + allValid,
      fromUtf8(allValid + allInvalid + allValid));
  EXPECT_EQ(
      allValid + repeat("#", numInvalid) + allValid,
      fromUtf8(allValid + allInvalid + allValid, '#'));
  EXPECT_EQ(
      allValid + allValid, fromUtf8(allValid + allInvalid + allValid, ""));
  EXPECT_EQ(
      allValid + repeat(kClef, numInvalid) + allValid,
      fromUtf8(allValid + allInvalid + allValid, kClef));

  // A mix.
  std::ostringstream mix;
  for (auto i = 0; i < 100; ++i) {
    mix << chars[i % numValid];
    mix << invalid()[i % numInvalid];
  }

  auto makeExpected = [&](const std::string& replacement) {
    std::ostringstream expected;
    for (auto i = 0; i < 100; ++i) {
      expected << chars[i % chars.size()];
      expected << replacement;
    }
    return expected.str();
  };

  EXPECT_EQ(makeExpected("\uFFFD"), fromUtf8(mix.str()));
  EXPECT_EQ(makeExpected("#"), fromUtf8(mix.str(), '#'));
  EXPECT_EQ(makeExpected(""), fromUtf8(mix.str(), ""));
  EXPECT_EQ(makeExpected(kClef), fromUtf8(mix.str(), kClef));

  // Invalid codepoint of multi-byte long.
  {
    // The first byte 0xFB indicates a 5-byte long codepoint.
    auto result = evaluateOnce<std::string>(
        "from_utf8(from_hex(c0))", std::optional<std::string>{"FBB78EB6BE"});
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ("\uFFFD", result.value());

    // The first byte 0xFB indicates a 5-byte long codepoint, but only three
    // continuation bytes follow. The fifth byte is a valid character 'X'.
    result = evaluateOnce<std::string>(
        "from_utf8(from_hex(c0))", std::optional<std::string>{"FBB78EB658"});
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ("\uFFFDX", result.value());
  }
}

TEST_F(FromUtf8Test, invalidReplacement) {
  auto data = makeRowVector({
      makeNullableFlatVector<std::string>(
          {
              invalid().front(),
              validAscii().front(),
              std::nullopt,
              validMultiByte().front(),
              "",
              invalid().back(),
              validAscii().back(),
              validMultiByte().back(),
              std::nullopt,
          },
          VARBINARY()),
      makeNullableFlatVector<int64_t>({
          '#',
          '.',
          '#',
          22334455,
          '#',
          '.',
          '.',
          '#',
          '#',
      }),
  });

  VELOX_ASSERT_THROW(
      evaluate("from_utf8(c0, 1234567::bigint)", data),
      "Not a valid Unicode code point: 1234567");

  auto result = evaluate("try(from_utf8(c0, 1234567::bigint))", data);
  velox::test::assertEqualVectors(
      makeNullConstant(TypeKind::VARCHAR, 9), result);

  VELOX_ASSERT_THROW(
      evaluate("from_utf8(c0, 'Abc')", data),
      "Replacement string must be empty or a single character");

  result = evaluate("try(from_utf8(c0, 'Abc'))", data);
  velox::test::assertEqualVectors(
      makeNullConstant(TypeKind::VARCHAR, 9), result);

  VELOX_ASSERT_THROW(
      evaluate("from_utf8(c0, c1)", data),
      "Not a valid Unicode code point: 22334455");

  result = evaluate("try(from_utf8(c0, c1))", data);
  auto expected = makeNullableFlatVector<std::string>({
      "#",
      validAscii().front(),
      std::nullopt,
      std::nullopt,
      "",
      ".",
      validAscii().back(),
      validMultiByte().back(),
      std::nullopt,
  });
  velox::test::assertEqualVectors(expected, result);
}

TEST_F(FromUtf8Test, invalidReplacementWithAllValidRows) {
  // Make sure that replacement is checked to be valid even if all the rows have
  // valid UTF-8 data.
  // c0 : all ascii
  // c1 : all valid not ascii.
  // c2 : invalid integer replacement.
  // c3 : invalid string replacement.
  auto data = makeRowVector(
      {makeNullableFlatVector<std::string>(
           {validAscii().front(), validAscii().front()}, VARBINARY()),
       makeNullableFlatVector<std::string>(
           {validMultiByte().back(), validMultiByte().back()}, VARBINARY()),
       makeNullableFlatVector<int64_t>({22334455, 22334455}),
       makeNullableFlatVector<std::string>({"12"
                                            "12"})});

  // Test when replacement is integer.
  VELOX_ASSERT_THROW(
      evaluate("from_utf8(c0, 22334455::bigint)", data),
      "Not a valid Unicode code point: 22334455");

  VELOX_ASSERT_THROW(
      evaluate("from_utf8(c0, c2)", data),
      "Not a valid Unicode code point: 22334455");

  VELOX_ASSERT_THROW(
      evaluate("from_utf8(c1, 22334455::bigint)", data),
      "Not a valid Unicode code point: 22334455");

  VELOX_ASSERT_THROW(
      evaluate("from_utf8(c1, c2)", data),
      "Not a valid Unicode code point: 22334455");

  // Test when replacement is string.
  VELOX_ASSERT_THROW(
      evaluate("from_utf8(c0, '12')", data),
      "Replacement string must be empty or a single character");

  VELOX_ASSERT_THROW(
      evaluate("from_utf8(c0, c3)", data),
      "Replacement string must be empty or a single character");

  VELOX_ASSERT_THROW(
      evaluate("from_utf8(c1, '12')", data),
      "Replacement string must be empty or a single character");

  VELOX_ASSERT_THROW(
      evaluate("from_utf8(c1, c3)", data),
      "Replacement string must be empty or a single character");
}

} // namespace
} // namespace facebook::velox::functions
