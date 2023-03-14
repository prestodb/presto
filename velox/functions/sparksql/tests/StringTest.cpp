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

  std::optional<std::string> trim(std::optional<std::string> srcStr) {
    return evaluateOnce<std::string>("trim(c0)", srcStr);
  }

  std::optional<std::string> trim(
      std::optional<std::string> trimStr,
      std::optional<std::string> srcStr) {
    return evaluateOnce<std::string>("trim(c0, c1)", trimStr, srcStr);
  }

  std::optional<std::string> ltrim(std::optional<std::string> srcStr) {
    return evaluateOnce<std::string>("ltrim(c0)", srcStr);
  }

  std::optional<std::string> ltrim(
      std::optional<std::string> trimStr,
      std::optional<std::string> srcStr) {
    return evaluateOnce<std::string>("ltrim(c0, c1)", trimStr, srcStr);
  }

  std::optional<std::string> rtrim(std::optional<std::string> srcStr) {
    return evaluateOnce<std::string>("rtrim(c0)", srcStr);
  }

  std::optional<std::string> rtrim(
      std::optional<std::string> trimStr,
      std::optional<std::string> srcStr) {
    return evaluateOnce<std::string>("rtrim(c0, c1)", trimStr, srcStr);
  }

  std::optional<std::string> md5(std::optional<std::string> arg) {
    return evaluateOnce<std::string, std::string>(
        "md5(c0)", {arg}, {VARBINARY()});
  }

  std::optional<std::string> sha1(std::optional<std::string> arg) {
    return evaluateOnce<std::string, std::string>(
        "sha1(c0)", {arg}, {VARBINARY()});
  }

  std::optional<std::string> sha2(
      std::optional<std::string> str,
      std::optional<int32_t> bitLength) {
    return evaluateOnce<std::string, std::string, int32_t>(
        "sha2(cast(c0 as varbinary), c1)", str, bitLength);
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

TEST_F(StringTest, sha2) {
  EXPECT_EQ(sha2("Spark", -1), std::nullopt);
  EXPECT_EQ(sha2("Spark", 1), std::nullopt);
  EXPECT_EQ(
      sha2("", 0),
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  EXPECT_EQ(
      sha2("Spark", 0),
      "529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b");
  EXPECT_EQ(
      sha2("0123456789abcdefghijklmnopqrstuvwxyz", 0),
      "74e7e5bb9d22d6db26bf76946d40fff3ea9f0346b884fd0694920fccfad15e33");
  EXPECT_EQ(
      sha2("", 224),
      "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f");
  EXPECT_EQ(
      sha2("Spark", 224),
      "dbeab94971678d36af2195851c0f7485775a2a7c60073d62fc04549c");
  EXPECT_EQ(
      sha2("0123456789abcdefghijklmnopqrstuvwxyz", 224),
      "e6e4a6be069cc9bead8b6050856d2b26da6b3f7efa0951e5fb3a54dd");
  EXPECT_EQ(
      sha2("", 256),
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  EXPECT_EQ(
      sha2("Spark", 256),
      "529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b");
  EXPECT_EQ(
      sha2("0123456789abcdefghijklmnopqrstuvwxyz", 256),
      "74e7e5bb9d22d6db26bf76946d40fff3ea9f0346b884fd0694920fccfad15e33");
  EXPECT_EQ(
      sha2("", 384),
      "38b060a751ac96384cd9327eb1b1e36a21fdb71114be0743"
      "4c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b");
  EXPECT_EQ(
      sha2("Spark", 384),
      "1e40b8d06c248a1cc32428c22582b6219d072283078fa140"
      "d9ad297ecadf2cabefc341b857ad36226aa8d6d79f2ab67d");
  EXPECT_EQ(
      sha2("0123456789abcdefghijklmnopqrstuvwxyz", 384),
      "ce6d4ea5442bc6c830bea1942d4860db9f7b96f0e9d2c307"
      "3ffe47a0e1166d95612d840ff15e5efdd23c1f273096da32");
  EXPECT_EQ(
      sha2("", 512),
      "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce"
      "47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e");
  EXPECT_EQ(
      sha2("Spark", 512),
      "44844a586c54c9a212da1dbfe05c5f1705de1af5fda1f0d36297623249b279fd"
      "8f0ccec03f888f4fb13bf7cd83fdad58591c797f81121a23cfdd5e0897795238");
  EXPECT_EQ(
      sha2("0123456789abcdefghijklmnopqrstuvwxyz", 512),
      "95cadc34aa46b9fdef432f62fe5bad8d9f475bfbecf797d5802bb5f2937a85d9"
      "3ce4857a6262b03834c01c610d74cd1215f9a466dc6ad3dd15078e3309a03a6d");
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

TEST_F(StringTest, trim) {
  EXPECT_EQ(trim(""), "");
  EXPECT_EQ(trim("  data\t "), "data\t");
  EXPECT_EQ(trim("  data\t"), "data\t");
  EXPECT_EQ(trim("data\t "), "data\t");
  EXPECT_EQ(trim("data\t"), "data\t");
  EXPECT_EQ(trim("  \u6570\u636E\t "), "\u6570\u636E\t");
  EXPECT_EQ(trim("  \u6570\u636E\t"), "\u6570\u636E\t");
  EXPECT_EQ(trim("\u6570\u636E\t "), "\u6570\u636E\t");
  EXPECT_EQ(trim("\u6570\u636E\t"), "\u6570\u636E\t");

  EXPECT_EQ(trim("", ""), "");
  EXPECT_EQ(trim("", "srcStr"), "srcStr");
  EXPECT_EQ(trim("trimStr", ""), "");
  EXPECT_EQ(trim("data!egr< >int", "integer data!"), "");
  EXPECT_EQ(trim("int", "integer data!"), "eger data!");
  EXPECT_EQ(trim("!!at", "integer data!"), "integer d");
  EXPECT_EQ(trim("a", "integer data!"), "integer data!");
  EXPECT_EQ(
      trim("\u6570\u6574!\u6570 \u636E!", "\u6574\u6570 \u6570\u636E!"), "");
  EXPECT_EQ(trim(" \u6574\u6570 ", "\u6574\u6570 \u6570\u636E!"), "\u636E!");
  EXPECT_EQ(trim("! \u6570\u636E!", "\u6574\u6570 \u6570\u636E!"), "\u6574");
  EXPECT_EQ(
      trim("\u6570", "\u6574\u6570 \u6570\u636E!"),
      "\u6574\u6570 \u6570\u636E!");
}

TEST_F(StringTest, ltrim) {
  EXPECT_EQ(ltrim(""), "");
  EXPECT_EQ(ltrim("  data\t "), "data\t ");
  EXPECT_EQ(ltrim("  data\t"), "data\t");
  EXPECT_EQ(ltrim("data\t "), "data\t ");
  EXPECT_EQ(ltrim("data\t"), "data\t");
  EXPECT_EQ(ltrim("  \u6570\u636E\t "), "\u6570\u636E\t ");
  EXPECT_EQ(ltrim("  \u6570\u636E\t"), "\u6570\u636E\t");
  EXPECT_EQ(ltrim("\u6570\u636E\t "), "\u6570\u636E\t ");
  EXPECT_EQ(ltrim("\u6570\u636E\t"), "\u6570\u636E\t");

  EXPECT_EQ(ltrim("", ""), "");
  EXPECT_EQ(ltrim("", "srcStr"), "srcStr");
  EXPECT_EQ(ltrim("trimStr", ""), "");
  EXPECT_EQ(ltrim("data!egr< >int", "integer data!"), "");
  EXPECT_EQ(ltrim("int", "integer data!"), "eger data!");
  EXPECT_EQ(ltrim("!!at", "integer data!"), "integer data!");
  EXPECT_EQ(ltrim("a", "integer data!"), "integer data!");
  EXPECT_EQ(
      ltrim("\u6570\u6574!\u6570 \u636E!", "\u6574\u6570 \u6570\u636E!"), "");
  EXPECT_EQ(ltrim(" \u6574\u6570 ", "\u6574\u6570 \u6570\u636E!"), "\u636E!");
  EXPECT_EQ(
      ltrim("! \u6570\u636E!", "\u6574\u6570 \u6570\u636E!"),
      "\u6574\u6570 \u6570\u636E!");
  EXPECT_EQ(
      ltrim("\u6570", "\u6574\u6570 \u6570\u636E!"),
      "\u6574\u6570 \u6570\u636E!");
}

TEST_F(StringTest, rtrim) {
  EXPECT_EQ(rtrim(""), "");
  EXPECT_EQ(rtrim("  data\t "), "  data\t");
  EXPECT_EQ(rtrim("  data\t"), "  data\t");
  EXPECT_EQ(rtrim("data\t "), "data\t");
  EXPECT_EQ(rtrim("data\t"), "data\t");
  EXPECT_EQ(rtrim("  \u6570\u636E\t "), "  \u6570\u636E\t");
  EXPECT_EQ(rtrim("  \u6570\u636E\t"), "  \u6570\u636E\t");
  EXPECT_EQ(rtrim("\u6570\u636E\t "), "\u6570\u636E\t");
  EXPECT_EQ(rtrim("\u6570\u636E\t"), "\u6570\u636E\t");

  EXPECT_EQ(rtrim("", ""), "");
  EXPECT_EQ(rtrim("", "srcStr"), "srcStr");
  EXPECT_EQ(rtrim("trimStr", ""), "");
  EXPECT_EQ(rtrim("data!egr< >int", "integer data!"), "");
  EXPECT_EQ(rtrim("int", "integer data!"), "integer data!");
  EXPECT_EQ(rtrim("!!at", "integer data!"), "integer d");
  EXPECT_EQ(rtrim("a", "integer data!"), "integer data!");
  EXPECT_EQ(
      rtrim("\u6570\u6574!\u6570 \u636E!", "\u6574\u6570 \u6570\u636E!"), "");
  EXPECT_EQ(
      rtrim(" \u6574\u6570 ", "\u6574\u6570 \u6570\u636E!"),
      "\u6574\u6570 \u6570\u636E!");
  EXPECT_EQ(rtrim("! \u6570\u636E!", "\u6574\u6570 \u6570\u636E!"), "\u6574");
  EXPECT_EQ(
      rtrim("\u6570", "\u6574\u6570 \u6570\u636E!"),
      "\u6574\u6570 \u6570\u636E!");
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
