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
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/type/Type.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class MaskTest : public SparkFunctionBaseTest {
 protected:
  std::optional<std::string> maskWithOneArg(
      const std::optional<std::string>& srcStr) {
    return evaluateOnce<std::string>("mask(c0)", srcStr);
  };

  std::optional<std::string> maskWithTwoArg(
      const std::optional<std::string>& srcStr,
      const std::optional<std::string>& upperChar) {
    return evaluateOnce<std::string>("mask(c0, c1)", srcStr, upperChar);
  };

  std::optional<std::string> maskWithThreeArg(
      const std::optional<std::string>& srcStr,
      const std::optional<std::string>& upperChar,
      const std::optional<std::string>& lowerChar) {
    return evaluateOnce<std::string>(
        "mask(c0, c1, c2)", srcStr, upperChar, lowerChar);
  };

  std::optional<std::string> maskWithFourArg(
      const std::optional<std::string>& srcStr,
      const std::optional<std::string>& upperChar,
      const std::optional<std::string>& lowerChar,
      const std::optional<std::string>& digitChar) {
    return evaluateOnce<std::string>(
        "mask(c0, c1, c2, c3)", srcStr, upperChar, lowerChar, digitChar);
  };

  std::optional<std::string> maskWithFiveArg(
      const std::optional<std::string>& srcStr,
      const std::optional<std::string>& upperChar,
      const std::optional<std::string>& lowerChar,
      const std::optional<std::string>& digitChar,
      const std::optional<std::string>& otherChar) {
    return evaluateOnce<std::string>(
        "mask(c0, c1, c2, c3, c4)",
        srcStr,
        upperChar,
        lowerChar,
        digitChar,
        otherChar);
  };
};

TEST_F(MaskTest, mask) {
  std::string upperChar = "Y";
  std::string lowerChar = "y";
  std::string digitChar = "d";
  std::string otherChar = "*";

  EXPECT_EQ(maskWithOneArg("AbCD123-@$#"), "XxXXnnn-@$#");
  EXPECT_EQ(maskWithOneArg("abcd-EFGH-8765-4321"), "xxxx-XXXX-nnnn-nnnn");
  EXPECT_EQ(maskWithOneArg(std::nullopt), std::nullopt);
  EXPECT_EQ(maskWithOneArg(""), "");
  // Case to cover unicode characters.
  EXPECT_EQ(maskWithOneArg("синяяい绿AbCD123世界🙂"), "xxxxxい绿XxXXnnn世界🙂");
  // Case to cover invalid UTF-8 characters.
  EXPECT_EQ(maskWithOneArg("\xED\xA0\x80"), "\xED\xA0\x80");
  EXPECT_EQ(maskWithOneArg("Abc-\xED\xA0\x80@123"), "Xxx-\xED\xA0\x80@nnn");

  EXPECT_EQ(maskWithTwoArg("AbCD123-@$#", upperChar), "YxYYnnn-@$#");
  EXPECT_EQ(
      maskWithTwoArg("abcd-EFGH-8765-4321", upperChar), "xxxx-YYYY-nnnn-nnnn");
  EXPECT_EQ(maskWithTwoArg(std::nullopt, upperChar), std::nullopt);
  EXPECT_EQ(maskWithTwoArg("", upperChar), "");
  // Case to cover unicode characters.
  EXPECT_EQ(
      maskWithTwoArg("синяяい绿AbCD123世界🙂", upperChar),
      "xxxxxい绿YxYYnnn世界🙂");
  // Case to cover invalid UTF-8 characters.
  EXPECT_EQ(
      maskWithTwoArg("Abc-\xED\xA0\x80@123", upperChar),
      "Yxx-\xED\xA0\x80@nnn");
  EXPECT_EQ(maskWithTwoArg("AbCD123-@$#", std::nullopt), "AxCDnnn-@$#");
  EXPECT_EQ(
      maskWithTwoArg("abcd-EFGH-8765-4321", std::nullopt),
      "xxxx-EFGH-nnnn-nnnn");

  EXPECT_EQ(
      maskWithThreeArg("AbCD123-@$#", upperChar, lowerChar), "YyYYnnn-@$#");
  EXPECT_EQ(
      maskWithThreeArg("abcd-EFGH-8765-4321", upperChar, lowerChar),
      "yyyy-YYYY-nnnn-nnnn");
  EXPECT_EQ(maskWithThreeArg(std::nullopt, upperChar, lowerChar), std::nullopt);
  EXPECT_EQ(maskWithThreeArg("", upperChar, lowerChar), "");
  // Case to cover unicode characters.
  EXPECT_EQ(
      maskWithThreeArg("синяяい绿AbCD123世界🙂", upperChar, lowerChar),
      "yyyyyい绿YyYYnnn世界🙂");
  // Case to cover invalid UTF-8 characters.
  EXPECT_EQ(
      maskWithThreeArg("Abc-\xED\xA0\x80@123", upperChar, lowerChar),
      "Yyy-\xED\xA0\x80@nnn");
  EXPECT_EQ(
      maskWithThreeArg("AbCD123-@$#", upperChar, std::nullopt), "YbYYnnn-@$#");
  EXPECT_EQ(
      maskWithThreeArg("abcd-EFGH-8765-4321", std::nullopt, lowerChar),
      "yyyy-EFGH-nnnn-nnnn");
  EXPECT_EQ(
      maskWithThreeArg("AbCD123-@$#", std::nullopt, std::nullopt),
      "AbCDnnn-@$#");

  EXPECT_EQ(
      maskWithFourArg("AbCD123-@$#", upperChar, lowerChar, digitChar),
      "YyYYddd-@$#");
  EXPECT_EQ(
      maskWithFourArg("abcd-EFGH-8765-4321", upperChar, lowerChar, digitChar),
      "yyyy-YYYY-dddd-dddd");
  EXPECT_EQ(
      maskWithFourArg(std::nullopt, upperChar, lowerChar, digitChar),
      std::nullopt);
  EXPECT_EQ(maskWithFourArg("", upperChar, lowerChar, digitChar), "");
  // Case to cover unicode characters.
  EXPECT_EQ(
      maskWithFourArg(
          "синяяい绿AbCD123世界🙂", upperChar, lowerChar, digitChar),
      "yyyyyい绿YyYYddd世界🙂");
  // Case to cover invalid UTF-8 characters.
  EXPECT_EQ(
      maskWithFourArg("Abc-\xED\xA0\x80@123", upperChar, lowerChar, digitChar),
      "Yyy-\xED\xA0\x80@ddd");
  EXPECT_EQ(
      maskWithFourArg("AbCD123-@$#", std::nullopt, lowerChar, digitChar),
      "AyCDddd-@$#");
  EXPECT_EQ(
      maskWithFourArg("AbCD123-@$#", upperChar, std::nullopt, digitChar),
      "YbYYddd-@$#");
  EXPECT_EQ(
      maskWithFourArg("AbCD123-@$#", upperChar, lowerChar, std::nullopt),
      "YyYY123-@$#");
  EXPECT_EQ(
      maskWithFourArg("AbCD123-@$#", upperChar, std::nullopt, std::nullopt),
      "YbYY123-@$#");
  EXPECT_EQ(
      maskWithFourArg("AbCD123-@$#", std::nullopt, lowerChar, std::nullopt),
      "AyCD123-@$#");
  EXPECT_EQ(
      maskWithFourArg("AbCD123-@$#", std::nullopt, std::nullopt, digitChar),
      "AbCDddd-@$#");
  EXPECT_EQ(
      maskWithFourArg("AbCD123-@$#", std::nullopt, std::nullopt, std::nullopt),
      "AbCD123-@$#");

  EXPECT_EQ(
      maskWithFiveArg(
          "AbCD123-@$#", upperChar, lowerChar, digitChar, otherChar),
      "YyYYddd****");
  EXPECT_EQ(
      maskWithFiveArg(
          "abcd-EFGH-8765-4321", upperChar, lowerChar, digitChar, otherChar),
      "yyyy*YYYY*dddd*dddd");
  EXPECT_EQ(
      maskWithFiveArg(std::nullopt, upperChar, lowerChar, digitChar, otherChar),
      std::nullopt);
  EXPECT_EQ(
      maskWithFiveArg("", upperChar, lowerChar, digitChar, otherChar), "");
  // Case to cover unicode characters.
  EXPECT_EQ(
      maskWithFiveArg(
          "синяяい绿AbCD123世界🙂", upperChar, lowerChar, digitChar, otherChar),
      "yyyyy**YyYYddd***");
  EXPECT_EQ(
      maskWithFiveArg(
          "синяяい绿AbCD123世界🙂", upperChar, lowerChar, digitChar, "い"),
      "yyyyyいいYyYYdddいいい");
  EXPECT_EQ(
      maskWithFiveArg(
          "синяяい绿AbCD123世界🙂", upperChar, lowerChar, digitChar, "🚀"),
      "yyyyy🚀🚀YyYYddd🚀🚀🚀");
  // Case to cover invalid UTF-8 characters.
  EXPECT_EQ(
      maskWithFiveArg(
          "\xED\xA0\x80", upperChar, lowerChar, digitChar, otherChar),
      "***");
  EXPECT_EQ(
      maskWithFiveArg(
          "Abc-\xED\xA0\x80@123", upperChar, lowerChar, digitChar, otherChar),
      "Yyy*****ddd");
  EXPECT_EQ(
      maskWithFiveArg(
          "AbCD123-@$#", std::nullopt, lowerChar, digitChar, otherChar),
      "AyCDddd****");
  EXPECT_EQ(
      maskWithFiveArg(
          "AbCD123-@$#", upperChar, std::nullopt, digitChar, otherChar),
      "YbYYddd****");
  EXPECT_EQ(
      maskWithFiveArg(
          "AbCD123-@$#", upperChar, lowerChar, std::nullopt, otherChar),
      "YyYY123****");
  EXPECT_EQ(
      maskWithFiveArg(
          "AbCD123-@$#", upperChar, lowerChar, digitChar, std::nullopt),
      "YyYYddd-@$#");
  EXPECT_EQ(
      maskWithFiveArg(
          "AbCD123-@$#", std::nullopt, lowerChar, digitChar, std::nullopt),
      "AyCDddd-@$#");
  EXPECT_EQ(
      maskWithFiveArg(
          "AbCD123-@$#", upperChar, std::nullopt, digitChar, std::nullopt),
      "YbYYddd-@$#");
  EXPECT_EQ(
      maskWithFiveArg(
          "AbCD123-@$#", upperChar, lowerChar, std::nullopt, std::nullopt),
      "YyYY123-@$#");
  EXPECT_EQ(
      maskWithFiveArg(
          "AbCD123-@$#", std::nullopt, std::nullopt, digitChar, otherChar),
      "AbCDddd****");
  EXPECT_EQ(
      maskWithFiveArg(
          "AbCD123-@$#", std::nullopt, lowerChar, std::nullopt, otherChar),
      "AyCD123****");
  EXPECT_EQ(
      maskWithFiveArg(
          "AbCD123-@$#", upperChar, std::nullopt, std::nullopt, otherChar),
      "YbYY123****");
  EXPECT_EQ(
      maskWithFiveArg(
          "AbCD123-@$#", std::nullopt, std::nullopt, std::nullopt, otherChar),
      "AbCD123****");
  EXPECT_EQ(
      maskWithFiveArg(
          "AbCD123-@$#", upperChar, std::nullopt, std::nullopt, std::nullopt),
      "YbYY123-@$#");
  EXPECT_EQ(
      maskWithFiveArg(
          "AbCD123-@$#", std::nullopt, lowerChar, std::nullopt, std::nullopt),
      "AyCD123-@$#");
  EXPECT_EQ(
      maskWithFiveArg(
          "AbCD123-@$#", std::nullopt, std::nullopt, digitChar, std::nullopt),
      "AbCDddd-@$#");
  EXPECT_EQ(
      maskWithFiveArg(
          "AbCD123-@$#",
          std::nullopt,
          std::nullopt,
          std::nullopt,
          std::nullopt),
      "AbCD123-@$#");

  // Case to cover input is ASCII but replacement character is wide-character.
  EXPECT_EQ(maskWithThreeArg("ABCabc", "🚀", "🚀"), "🚀🚀🚀🚀🚀🚀");
}

TEST_F(MaskTest, maskWithError) {
  std::string upperChar = "Y";
  std::string lowerChar = "y";
  std::string digitChar = "d";
  std::string otherChar = "*";
  VELOX_ASSERT_USER_THROW(
      maskWithFiveArg("AbCD123-@$#", "", lowerChar, digitChar, otherChar),
      "Replacement string must contain a single character and cannot be empty.");

  VELOX_ASSERT_USER_THROW(
      maskWithFiveArg("AbCD123-@$#", "🚀🚀", lowerChar, digitChar, otherChar),
      "Replacement string must contain a single character and cannot be empty.");

  VELOX_ASSERT_USER_THROW(
      maskWithFiveArg("AbCD123-@$#", upperChar, "", digitChar, otherChar),
      "Replacement string must contain a single character and cannot be empty.");

  VELOX_ASSERT_USER_THROW(
      maskWithFiveArg("AbCD123-@$#", upperChar, lowerChar, "", otherChar),
      "Replacement string must contain a single character and cannot be empty.");

  VELOX_ASSERT_USER_THROW(
      maskWithFiveArg("AbCD123-@$#", upperChar, lowerChar, digitChar, ""),
      "Replacement string must contain a single character and cannot be empty.");
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
