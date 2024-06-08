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

class RegexpReplaceTest : public functions::test::FunctionBaseTest {
 protected:
  std::optional<std::string> regexpReplace(
      const std::optional<std::string>& string,
      const std::string& pattern) {
    return evaluateOnce<std::string>(
        fmt::format("regexp_replace(c0, '{}')", pattern), string);
  }

  std::optional<std::string> regexpReplace(
      const std::optional<std::string>& string,
      const std::string& pattern,
      const std::string& replacement) {
    return evaluateOnce<std::string>(
        fmt::format("regexp_replace(c0, '{}', '{}')", pattern, replacement),
        string);
  }
};

TEST_F(RegexpReplaceTest, noReplacement) {
  EXPECT_EQ(regexpReplace("abcd", "cd"), "ab");
  EXPECT_EQ(regexpReplace("a12b34c5", "\\d"), "abc");
  EXPECT_EQ(regexpReplace("abc", "\\w"), "");
  EXPECT_EQ(regexpReplace("", "\\d"), "");
  EXPECT_EQ(regexpReplace("abc", ""), "abc");
  EXPECT_EQ(regexpReplace("$$$.", "\\$"), ".");
  EXPECT_EQ(regexpReplace("???.", "\\?"), ".");
  EXPECT_EQ(regexpReplace(std::nullopt, "abc"), std::nullopt);

  auto input = makeRowVector({
      makeFlatVector<std::string>(
          {"apple123", "1 banana", "orange 23 ...", "12 34 56"}),
      makeFlatVector<std::string>({"[0-9]+", "\\d+", "ge\\s", "[4-9 ]"}),
  });

  auto result = evaluate("regexp_replace(c0, c1)", input);

  auto expected =
      makeFlatVector<std::string>({"apple", " banana", "oran23 ...", "123"});
  test::assertEqualVectors(expected, result);
}

TEST_F(RegexpReplaceTest, withReplacement) {
  EXPECT_EQ(regexpReplace("abcd", "cd", "ef"), "abef");
  EXPECT_EQ(regexpReplace("abc", "\\w", ""), "");
  EXPECT_EQ(regexpReplace("a12b34c5", "\\d", "."), "a..b..c.");
  EXPECT_EQ(regexpReplace("", "\\d", "."), "");
  EXPECT_EQ(regexpReplace("abc", "", "."), ".a.b.c.");
  EXPECT_EQ(
      regexpReplace("1a 2b 14m", "(\\d+)([ab]) ", "3c$2 "), "3ca 3cb 14m");
  EXPECT_EQ(regexpReplace("1a 2b 14m", "(\\d+)([ab])", "3c$2"), "3ca 3cb 14m");
  EXPECT_EQ(regexpReplace("abc", "(?P<alpha>\\w)", "1${alpha}"), "1a1b1c");
  EXPECT_EQ(
      regexpReplace("1a1b1c", "(?<digit>\\d)(?<alpha>\\w)", "${alpha}\\$"),
      "a$b$c$");
  EXPECT_EQ(
      regexpReplace("1a2b3c", "(?<digit>\\d)(?<alpha>\\w)", "${alpha}${digit}"),
      "a1b2c3");
  EXPECT_EQ(regexpReplace("123", "(\\d)", "\\$"), "$$$");
  EXPECT_EQ(
      regexpReplace("123", "(?<digit>(?<nest>\\d))", ".${digit}"), ".1.2.3");
  EXPECT_EQ(
      regexpReplace("123", "(?<digit>(?<nest>\\d))", ".${nest}"), ".1.2.3");
  EXPECT_EQ(regexpReplace(std::nullopt, "abc", "def"), std::nullopt);

  EXPECT_THROW(regexpReplace("123", "(?<d", "."), VeloxUserError);
  EXPECT_THROW(regexpReplace("123", R"((?''digit''\d))", "."), VeloxUserError);
  EXPECT_THROW(regexpReplace("123", "(?P<>\\d)", "."), VeloxUserError);
  EXPECT_THROW(regexpReplace("123", "(?P<digit>\\d)", "${dd}"), VeloxUserError);
  EXPECT_THROW(regexpReplace("123", "(?P<digit>\\d)", "${}"), VeloxUserError);

  auto input = makeRowVector({
      makeFlatVector<std::string>(
          {"apple123", "1 banana", "orange 23 ...", "12 34 56"}),
      makeFlatVector<std::string>({"[0-9]+", "\\d+", "ge\\s", "[4-9]"}),
      makeFlatVector<std::string>({"_", ".", "", "[===]"}),
  });

  auto result = evaluate("regexp_replace(c0, c1, c2)", input);

  auto expected = makeFlatVector<std::string>(
      {"apple_", ". banana", "oran23 ...", "12 3[===] [===][===]"});
  test::assertEqualVectors(expected, result);

  // Constant 'replacement' with non-constant 'pattern'.
  result = evaluate("regexp_replace(c0, c1, '||')", input);

  expected = makeFlatVector<std::string>(
      {"apple||", "|| banana", "oran||23 ...", "12 3|| ||||"});
  test::assertEqualVectors(expected, result);
}

} // namespace
} // namespace facebook::velox
