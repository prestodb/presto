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
#include <exception>
#include <optional>

#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox {
namespace {

class RegexFunctionsTest : public functions::test::FunctionBaseTest {
 protected:
  std::optional<std::string> regexp_replace(
      const std::optional<std::string>& string,
      const std::string& pattern) {
    return evaluateOnce<std::string>(
        fmt::format("regexp_replace(c0, '{}')", pattern), string);
  }

  std::optional<std::string> regexp_replace(
      const std::optional<std::string>& string,
      const std::string& pattern,
      const std::string& replacement) {
    return evaluateOnce<std::string>(
        fmt::format("regexp_replace(c0, '{}', '{}')", pattern, replacement),
        string);
  }
};

TEST_F(RegexFunctionsTest, RegexpReplaceNoReplacement) {
  EXPECT_EQ(regexp_replace("abcd", "cd"), "ab");
  EXPECT_EQ(regexp_replace("a12b34c5", "\\d"), "abc");
  EXPECT_EQ(regexp_replace("abc", "\\w"), "");
  EXPECT_EQ(regexp_replace("", "\\d"), "");
  EXPECT_EQ(regexp_replace("abc", ""), "abc");
  EXPECT_EQ(regexp_replace("$$$.", "\\$"), ".");
  EXPECT_EQ(regexp_replace("???.", "\\?"), ".");
  EXPECT_EQ(regexp_replace(std::nullopt, "abc"), std::nullopt);
}

TEST_F(RegexFunctionsTest, RegexpReplaceWithReplacement) {
  EXPECT_EQ(regexp_replace("abcd", "cd", "ef"), "abef");
  EXPECT_EQ(regexp_replace("abc", "\\w", ""), "");
  EXPECT_EQ(regexp_replace("a12b34c5", "\\d", "."), "a..b..c.");
  EXPECT_EQ(regexp_replace("", "\\d", "."), "");
  EXPECT_EQ(regexp_replace("abc", "", "."), ".a.b.c.");
  EXPECT_EQ(
      regexp_replace("1a 2b 14m", "(\\d+)([ab]) ", "3c$2 "), "3ca 3cb 14m");
  EXPECT_EQ(regexp_replace("1a 2b 14m", "(\\d+)([ab])", "3c$2"), "3ca 3cb 14m");
  EXPECT_EQ(regexp_replace("abc", "(?P<alpha>\\w)", "1${alpha}"), "1a1b1c");
  EXPECT_EQ(
      regexp_replace("1a1b1c", "(?<digit>\\d)(?<alpha>\\w)", "${alpha}\\$"),
      "a$b$c$");
  EXPECT_EQ(
      regexp_replace(
          "1a2b3c", "(?<digit>\\d)(?<alpha>\\w)", "${alpha}${digit}"),
      "a1b2c3");
  EXPECT_EQ(regexp_replace("123", "(\\d)", "\\$"), "$$$");
  EXPECT_EQ(
      regexp_replace("123", "(?<digit>(?<nest>\\d))", ".${digit}"), ".1.2.3");
  EXPECT_EQ(
      regexp_replace("123", "(?<digit>(?<nest>\\d))", ".${nest}"), ".1.2.3");
  EXPECT_EQ(regexp_replace(std::nullopt, "abc", "def"), std::nullopt);

  EXPECT_THROW(regexp_replace("123", "(?<d", "."), VeloxUserError);
  EXPECT_THROW(regexp_replace("123", R"((?''digit''\d))", "."), VeloxUserError);
  EXPECT_THROW(regexp_replace("123", "(?P<>\\d)", "."), VeloxUserError);
  EXPECT_THROW(
      regexp_replace("123", "(?P<digit>\\d)", "${dd}"), VeloxUserError);
  EXPECT_THROW(regexp_replace("123", "(?P<digit>\\d)", "${}"), VeloxUserError);
}

} // namespace
} // namespace facebook::velox
