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

#include "velox/common/base/tests/GTestUtils.h"
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
  EXPECT_EQ(regexpReplace("", "", "."), ".");
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

TEST_F(RegexpReplaceTest, lambda) {
  auto input = makeRowVector({
      makeFlatVector<std::string>(
          {"new york", "", "los angeles", "san francisco", "seattle"}),
      makeFlatVector<std::string>({"[a]", "[b]", "[c]", "[d]", "[e]"}),
      makeFlatVector<int64_t>({0, 1, 2, 3, 4}),
  });

  auto result = evaluate(
      "regexp_replace(c0, '(\\w)(\\w*)', x -> (concat(upper(x[1]), lower(x[2]))))",
      input);

  VectorPtr expected = makeFlatVector<std::string>(
      {"New York", "", "Los Angeles", "San Francisco", "Seattle"});
  test::assertEqualVectors(expected, result);

  // No matches for any input string. The result should be the same as input.
  result = evaluate(
      "regexp_replace(c0, '\\d+', x -> (concat(upper(x[1]), lower(x[2]))))",
      input);
  test::assertEqualVectors(input->childAt(0), result);

  // No matches for the second group.
  result = evaluate(
      "regexp_replace(c0, '(\\w)(\\d*)', x -> (concat(upper(x[1]), lower(x[2]))))",
      input);
  expected = makeFlatVector<std::string>(
      {"NEW YORK", "", "LOS ANGELES", "SAN FRANCISCO", "SEATTLE"});
  test::assertEqualVectors(expected, result);

  // Regex with no matching groups.
  result = evaluate("regexp_replace(c0, '\\w\\w*', x -> '_')", input);
  expected = makeFlatVector<std::string>({"_ _", "", "_ _", "_ _", "_"});
  test::assertEqualVectors(expected, result);

  // Lambda references non-existent matching groups.
  VELOX_ASSERT_THROW(
      (evaluate(
          "regexp_replace(c0, '\\w(\\w*)', x -> (concat(upper(x[1]), lower(x[2]))))",
          input)),
      "Array subscript out of bounds");

  result = evaluate(
      "regexp_replace(c0, '\\w(\\w*)', x -> try(concat(upper(x[1]), lower(x[2]))))",
      input);
  expected = makeNullableFlatVector<std::string>(
      {std::nullopt, "", std::nullopt, std::nullopt, std::nullopt});
  test::assertEqualVectors(expected, result);

  result = evaluate(
      "try(regexp_replace(c0, '\\w(\\w*)', x -> (concat(upper(x[1]), lower(x[2])))))",
      input);
  test::assertEqualVectors(expected, result);

  // Empty pattern.
  result = evaluate("regexp_replace(c0, '', x -> '_')", input);
  expected = makeFlatVector<std::string>(
      {"_n_e_w_ _y_o_r_k_",
       "_",
       "_l_o_s_ _a_n_g_e_l_e_s_",
       "_s_a_n_ _f_r_a_n_c_i_s_c_o_",
       "_s_e_a_t_t_l_e_"});
  test::assertEqualVectors(expected, result);

  // Regex that "matches" empty string.
  result = evaluate("regexp_replace(c0, '\\d*', x -> '_')", input);
  test::assertEqualVectors(expected, result);

  // Lambda with captures.
  result = evaluate(
      "regexp_replace(c0, '(\\w)(\\w*)', x -> concat(c1, lower(x[2])))", input);
  expected = makeFlatVector<std::string>(
      {"[a]ew [a]ork",
       "",
       "[c]os [c]ngeles",
       "[d]an [d]rancisco",
       "[e]eattle"});
  test::assertEqualVectors(expected, result);

  // Different lambdas for different rows.
  // Even rows: replace each word with the first letter uppercase.
  // Odd rows: remove first letter from each word.
  result = evaluate(
      "regexp_replace(c0, '(\\w)(\\w*)', if (c2 % 2 = 0, x -> upper(x[1]), x -> x[2]))",
      input);
  expected =
      makeFlatVector<std::string>({"N Y", "", "L A", "an rancisco", "S"});
  test::assertEqualVectors(expected, result);

  // Lambda fails for some rows.
  VELOX_ASSERT_THROW(
      (evaluate(
          "regexp_replace(c0, '(\\w)(\\w*)', x -> if (x[1] = 's', x[10], concat(upper(x[1]), lower(x[2]))))",
          input)),
      "Array subscript out of bounds");

  result = evaluate(
      "regexp_replace(c0, '(\\w)(\\w*)', x -> try(if (x[1] = 's', x[10], concat(upper(x[1]), lower(x[2])))))",
      input);
  expected = makeNullableFlatVector<std::string>(
      {"New York", "", "Los Angeles", std::nullopt, std::nullopt});
  test::assertEqualVectors(expected, result);

  // Nulls in 'string', 'pattern' or 'capture'.
  auto inputWithNulls = makeRowVector({
      makeNullableFlatVector<std::string>(
          {"new york",
           std::nullopt,
           "los angeles",
           "san francisco",
           "seattle",
           std::nullopt}),
      makeNullableFlatVector<std::string>({
          "(\\w+)",
          "(\\d+)",
          std::nullopt,
          "(\\w)",
          "(\\w)",
          "(\\d+)",
      }),
      makeNullableFlatVector<std::string>({
          "!",
          "[b]",
          "[c]",
          std::nullopt,
          "_",
          "_",
      }),

  });

  result = evaluate("regexp_replace(c0, c1, x -> upper(x[1]))", inputWithNulls);
  expected = makeNullableFlatVector<std::string>(
      {"NEW YORK",
       std::nullopt,
       std::nullopt,
       "SAN FRANCISCO",
       "SEATTLE",
       std::nullopt});
  test::assertEqualVectors(expected, result);

  result =
      evaluate("regexp_replace(c0, c1, x -> concat(c2, x[1]))", inputWithNulls);
  expected = makeNullableFlatVector<std::string>(
      {"!new !york",
       std::nullopt,
       std::nullopt,
       std::nullopt,
       "_s_e_a_t_t_l_e",
       std::nullopt});
  test::assertEqualVectors(expected, result);
}

} // namespace
} // namespace facebook::velox
