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
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/type/Type.h"

#include <stdint.h>

namespace facebook::velox::functions::sparksql::test {
namespace {

// This is a five codepoint sequence that renders as a single emoji.
static constexpr char kWomanFacepalmingLightSkinTone[] =
    "\xF0\x9F\xA4\xA6\xF0\x9F\x8F\xBB\xE2\x80\x8D\xE2\x99\x80\xEF\xB8\x8F";

class StringTest : public SparkFunctionBaseTest {
 protected:
  std::optional<int32_t> ascii(std::optional<std::string> arg) {
    return evaluateOnce<int32_t>("ascii(c0)", arg);
  }

  std::optional<std::string> chr(std::optional<int64_t> arg) {
    return evaluateOnce<std::string>("chr(c0)", arg);
  }

  std::optional<int32_t> instr(
      std::optional<std::string> haystack,
      std::optional<std::string> needle) {
    return evaluateOnce<int32_t>("instr(c0, c1)", haystack, needle);
  }

  std::optional<int32_t> length(std::optional<std::string> arg) {
    return evaluateOnce<int32_t>("length(c0)", arg);
  }

  std::optional<int32_t> length_bytes(std::optional<std::string> arg) {
    return evaluateOnce<int32_t, std::string>(
        "length(c0)", {arg}, {VARBINARY()});
  }

  std::optional<std::string> md5(std::optional<std::string> arg) {
    return evaluateOnce<std::string, std::string>(
        "md5(c0)", {arg}, {VARBINARY()});
  }

  std::optional<std::string> sha1(std::optional<std::string> arg) {
    return evaluateOnce<std::string, std::string>(
        "sha1(c0)", {arg}, {VARBINARY()});
  }

  bool compareFunction(
      const std::string& function,
      const std::optional<std::string>& str,
      const std::optional<std::string>& pattern) {
    return evaluateOnce<bool>(function + "(c0, c1)", str, pattern).value();
  }

  std::optional<bool> startsWith(
      const std::optional<std::string>& str,
      const std::optional<std::string>& pattern) {
    return evaluateOnce<bool>("startsWith(c0, c1)", str, pattern);
  }
  std::optional<bool> endsWith(
      const std::optional<std::string>& str,
      const std::optional<std::string>& pattern) {
    return evaluateOnce<bool>("endsWith(c0, c1)", str, pattern);
  }
  std::optional<bool> contains(
      const std::optional<std::string>& str,
      const std::optional<std::string>& pattern) {
    return evaluateOnce<bool>("contains(c0, c1)", str, pattern);
  }
};

TEST_F(StringTest, Ascii) {
  EXPECT_EQ(ascii(std::string("\0", 1)), 0);
  EXPECT_EQ(ascii(" "), 32);
  EXPECT_EQ(ascii("😋"), -16);
  EXPECT_EQ(ascii(""), 0);
  EXPECT_EQ(ascii(std::nullopt), std::nullopt);
}

TEST_F(StringTest, Chr) {
  EXPECT_EQ(chr(0), std::string("\0", 1));
  EXPECT_EQ(chr(32), " ");
  EXPECT_EQ(chr(-16), "");
  EXPECT_EQ(chr(256), std::string("\0", 1));
  EXPECT_EQ(chr(256 + 32), std::string(" ", 1));
  EXPECT_EQ(chr(std::nullopt), std::nullopt);
}

TEST_F(StringTest, Instr) {
  EXPECT_EQ(instr("SparkSQL", "SQL"), 6);
  EXPECT_EQ(instr(std::nullopt, "SQL"), std::nullopt);
  EXPECT_EQ(instr("SparkSQL", std::nullopt), std::nullopt);
  EXPECT_EQ(instr("SparkSQL", "Spark"), 1);
  EXPECT_EQ(instr("SQL", "SparkSQL"), 0);
  EXPECT_EQ(instr("", ""), 1);
  EXPECT_EQ(instr("abdef", "g"), 0);
  EXPECT_EQ(instr("", "a"), 0);
  EXPECT_EQ(instr("abdef", ""), 1);
  EXPECT_EQ(instr("abc😋def", "😋"), 4);
  // Offsets are calculated in terms of codepoints, not characters.
  // kWomanFacepalmingLightSkinTone is five codepoints.
  EXPECT_EQ(
      instr(std::string(kWomanFacepalmingLightSkinTone) + "abc😋def", "😋"), 9);
  EXPECT_EQ(
      instr(std::string(kWomanFacepalmingLightSkinTone) + "abc😋def", "def"),
      10);
}

TEST_F(StringTest, LengthString) {
  EXPECT_EQ(length(""), 0);
  EXPECT_EQ(length(std::string("\0", 1)), 1);
  EXPECT_EQ(length("1"), 1);
  EXPECT_EQ(length("😋"), 1);
  // Consists of five codepoints.
  EXPECT_EQ(length(kWomanFacepalmingLightSkinTone), 5);
  EXPECT_EQ(length("1234567890abdef"), 15);
}

TEST_F(StringTest, LengthBytes) {
  EXPECT_EQ(length_bytes(""), 0);
  EXPECT_EQ(length_bytes(std::string("\0", 1)), 1);
  EXPECT_EQ(length_bytes("1"), 1);
  EXPECT_EQ(length_bytes("😋"), 4);
  EXPECT_EQ(length_bytes(kWomanFacepalmingLightSkinTone), 17);
  EXPECT_EQ(length_bytes("1234567890abdef"), 15);
}

TEST_F(StringTest, MD5) {
  EXPECT_EQ(md5(std::nullopt), std::nullopt);
  EXPECT_EQ(md5(""), "d41d8cd98f00b204e9800998ecf8427e");
  EXPECT_EQ(md5("Infinity"), "eb2ac5b04180d8d6011a016aeb8f75b3");
}

TEST_F(StringTest, sha1) {
  EXPECT_EQ(sha1(std::nullopt), std::nullopt);
  EXPECT_EQ(sha1(""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
  EXPECT_EQ(sha1("Spark"), "85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c");
  EXPECT_EQ(
      sha1("0123456789abcdefghijklmnopqrstuvwxyz"),
      "a26704c04fc5f10db5aab58468035531cc542485");
}

TEST_F(StringTest, startsWith) {
  EXPECT_EQ(startsWith("hello", "ello"), false);
  EXPECT_EQ(startsWith("hello", "hell"), true);
  EXPECT_EQ(startsWith("hello", "hello there!"), false);
  EXPECT_EQ(startsWith("hello there!", "hello"), true);
  EXPECT_EQ(startsWith("-- hello there!", "-"), true);
  EXPECT_EQ(startsWith("-- hello there!", ""), true);
  EXPECT_EQ(startsWith("-- hello there!", std::nullopt), std::nullopt);
  EXPECT_EQ(startsWith(std::nullopt, "abc"), std::nullopt);
}

TEST_F(StringTest, contains) {
  EXPECT_EQ(contains("hello", "ello"), true);
  EXPECT_EQ(contains("hello", "hell"), true);
  EXPECT_EQ(contains("hello", "hello there!"), false);
  EXPECT_EQ(contains("hello there!", "hello"), true);
  EXPECT_EQ(contains("hello there!", ""), true);
  EXPECT_EQ(contains("-- hello there!", std::nullopt), std::nullopt);
  EXPECT_EQ(contains(std::nullopt, "abc"), std::nullopt);
}

TEST_F(StringTest, endsWith) {
  EXPECT_EQ(endsWith("hello", "ello"), true);
  EXPECT_EQ(endsWith("hello", "hell"), false);
  EXPECT_EQ(endsWith("hello", "hello there!"), false);
  EXPECT_EQ(endsWith("hello there!", "hello"), false);
  EXPECT_EQ(endsWith("hello there!", "!"), true);
  EXPECT_EQ(endsWith("hello there!", "there!"), true);
  EXPECT_EQ(endsWith("hello there!", "hello there!"), true);
  EXPECT_EQ(endsWith("hello there!", ""), true);
  EXPECT_EQ(endsWith("hello there!", "hello there"), false);
  EXPECT_EQ(endsWith("-- hello there!", "hello there"), false);
  EXPECT_EQ(endsWith("-- hello there!", std::nullopt), std::nullopt);
  EXPECT_EQ(endsWith(std::nullopt, "abc"), std::nullopt);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
