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
#include "velox/functions/lib/Re2Functions.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <functional>
#include <optional>
#include <string>

#include "velox/common/base/VeloxException.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/parse/TypeResolver.h"
#include "velox/type/StringView.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions {

using namespace facebook::velox::test;

namespace {

std::shared_ptr<exec::VectorFunction> makeRegexExtract(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  return makeRe2Extract(name, inputArgs, /*emptyNoMatch=*/false);
}

class Re2FunctionsTest : public test::FunctionBaseTest {
 public:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    exec::registerStatefulVectorFunction(
        "re2_match", re2MatchSignatures(), makeRe2Match);
    exec::registerStatefulVectorFunction(
        "re2_search", re2SearchSignatures(), makeRe2Search);
    exec::registerStatefulVectorFunction(
        "re2_extract", re2ExtractSignatures(), makeRegexExtract);
    exec::registerStatefulVectorFunction(
        "re2_extract_all", re2ExtractAllSignatures(), makeRe2ExtractAll);
    exec::registerStatefulVectorFunction("like", likeSignatures(), makeLike);
  }

 protected:
  const char* kLikePatternCharacterSet =
      "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ@#$^&*";
  const char* kWildcardCharacterSet = "_%";
  const char* kSingleWildcardCharacter = "_";
  const char* kAnyWildcardCharacter = "%";

  template <typename T>
  void testRe2ExtractAll(
      const std::vector<std::optional<std::string>>& inputs,
      const std::vector<std::optional<std::string>>& patterns,
      const std::vector<std::optional<T>>& groupIds,
      const std::vector<std::optional<std::vector<std::string>>>& output);

  std::string generateString(
      const char* characterSet,
      vector_size_t outputLength = 60) {
    vector_size_t arrLen = strlen(characterSet);
    std::string output;

    for (int i = 0; i < outputLength; i++) {
      output += characterSet[i % arrLen];
    }
    return output;
  }
};

template <typename Table, typename Row, std::size_t... I>
void addRow(Table& dataset, const Row& row, std::index_sequence<I...>) {
  (std::get<I>(dataset).push_back(std::get<I>(row)), ...);
}

// Helper class to make it easier to write readable tests for vector
// functions.
//
// First, call this functor with test cases and assert the results are
// correct. The arguments provided will be captured and stored for use in
// testBatchAll(), which automatically constructs a batches from the test
// cases provided earlier.
//
// So for example:
//
// VectorFunctionTester<bool, double> isnan("isnan(c0)");
// EXPECT_EQ(isnan(1.0), false);
// EXPECT_EQ(isnan(kIsNan), true);
// EXPECT_EQ(std::nullopt, std::nullopt);
// isnan.testBatchAll();
//
// Currently, testBatchAll() generates three types of test cases:
//   1) A batch with all the provided test cases in the provided order.
//   2) One batch of 1024 identical rows per test case using FlatVectors.
//   3) One batch of 1024 identical rows per test case using ConstantVectors.
template <typename ReturnType, typename... T>
class VectorFunctionTester : public test::FunctionBaseTest {
 public:
  explicit VectorFunctionTester(std::string expr) : expr(expr) {}

  // Use to assert individual test cases.
  std::optional<ReturnType> operator()(const std::optional<T>&... args) {
    auto result = evaluateOnce<ReturnType>(expr, args...);
    addRow(this->args, std::tuple(args...), std::index_sequence_for<T...>());
    expected.push_back(result);
    return result;
  }

  // Generate, run, and verify function test cases tested earlier still pass
  // when in batches.
  void testBatchAll() {
    // Test a batch with all test cases in order.
    std::apply([&](auto... args) { testBatch(expected, args...); }, args);
    // Generate one batch per test case, with 1024 identical rows.
    for (int i = 0; i < expected.size(); ++i) {
      std::apply(
          [&](auto... args) {
            testBatch(
                std::vector(1024, expected[i]), std::vector(1024, args[i])...);
            testConstantBatch(expected[i], args[i]...);
          },
          args);
    }
  }

 private:
  using EvalReturnType = EvalType<ReturnType>;
  const std::string expr;
  std::tuple<std::vector<std::optional<T>>...> args;
  std::vector<std::optional<ReturnType>> expected;

  void testBatch(
      const std::vector<std::optional<ReturnType>>& expected,
      const std::vector<std::optional<T>>&... args) {
    auto actual = evaluate<SimpleVector<EvalReturnType>>(
        expr, makeRowVector({makeNullableFlatVector(args)...}));
    ASSERT_EQ(actual->size(), expected.size());
    for (int i = 0; i < expected.size(); ++i) {
      EXPECT_EQ(
          actual->isNullAt(i) ? std::optional<ReturnType>{}
                              : ReturnType(actual->valueAt(i)),
          expected[i]);
    }
  }

  void testConstantBatch(
      const std::optional<ReturnType>& expected,
      std::optional<T>&... args) {
    constexpr std::size_t blocksize = 1024;
    auto actual = evaluate<SimpleVector<EvalReturnType>>(
        expr, makeRowVector({makeConstant(args, blocksize)...}));
    ASSERT_EQ(actual->size(), blocksize);
    for (int i = 0; i < blocksize; ++i) {
      EXPECT_EQ(
          actual->isNullAt(i) ? std::optional<ReturnType>{}
                              : ReturnType(actual->valueAt(i)),
          expected);
    }
  }

  // We inherit from FunctionBaseTest so that we can get access to the helpers
  // it defines, but since it is supposed to be a test fixture TestBody() is
  // declared pure virtual.  We must provide an implementation here.
  void TestBody() override {}
};

template <typename F>
void testRe2Match(F&& regexMatch) {
  // Empty string cases.
  EXPECT_EQ(true, regexMatch("", ""));
  EXPECT_EQ(false, regexMatch("abc", ""));
  EXPECT_EQ(false, regexMatch("", "abc"));
  // matching case.
  EXPECT_EQ(true, regexMatch("abc", "abcd*"));
  EXPECT_EQ(false, regexMatch("abd", "abcd*"));
  // Prefix match.
  EXPECT_EQ(false, regexMatch("abc ", "abcd*"));
  // Suffix match.
  EXPECT_EQ(false, regexMatch(" abc", "abcd*"));
  // Invalid regex.
  EXPECT_THROW(regexMatch("abc", "*"), VeloxException);
  // Null cases.
  EXPECT_EQ(std::nullopt, regexMatch(std::nullopt, ".*"));
  EXPECT_EQ(std::nullopt, regexMatch("", std::nullopt));
}

TEST_F(Re2FunctionsTest, regexMatchConstantPattern) {
  // If the pattern is a null literal, the resulting analyzer type will be
  // unknown. This is not a useful test, since the respective databases's
  // mapping into velox is expected to provide types explicitly. Therefore we
  // just return null directly in this case.
  testRe2Match([&](std::optional<std::string> str,
                   std::optional<std::string> pattern) {
    return pattern
        ? evaluateOnce<bool>("re2_match(c0, '" + *pattern + "')", str, pattern)
        : std::nullopt;
  });
}

TEST_F(Re2FunctionsTest, regexMatch) {
  testRe2Match(
      [&](std::optional<std::string> str, std::optional<std::string> pattern) {
        return evaluateOnce<bool>("re2_match(c0, c1)", str, pattern);
      });
}

TEST_F(Re2FunctionsTest, regexMatchBatch) {
  VectorFunctionTester<bool, std::string, std::string> re2Match(
      "re2_match(c0, c1)");
  testRe2Match(re2Match);
  re2Match.testBatchAll();
}

template <typename F>
void testRe2Search(F&& regexSearch) {
  // Empty string cases.
  EXPECT_EQ(true, regexSearch("", ""));
  EXPECT_EQ(true, regexSearch("abc", ""));
  EXPECT_EQ(false, regexSearch("", "abc"));
  // searching case.
  EXPECT_EQ(true, regexSearch("abc", "abcd*"));
  EXPECT_EQ(false, regexSearch("abd", "abcd*"));
  // Prefix search.
  EXPECT_EQ(true, regexSearch("abc ", "abcd*"));
  // Suffix search.
  EXPECT_EQ(true, regexSearch(" abc", "abcd*"));
  // Invalid regex.
  EXPECT_THROW(regexSearch("abc", "*"), VeloxException);
  // Null cases.
  EXPECT_EQ(std::nullopt, regexSearch(std::nullopt, ".*"));
  EXPECT_EQ(std::nullopt, regexSearch("", std::nullopt));
}

TEST_F(Re2FunctionsTest, regexSearchConstantPattern) {
  testRe2Search([&](std::optional<std::string> str,
                    std::optional<std::string> pattern) {
    return pattern
        ? evaluateOnce<bool>("re2_search(c0, '" + *pattern + "')", str, pattern)
        : std::nullopt;
    ;
  });
}

TEST_F(Re2FunctionsTest, regexSearch) {
  testRe2Search(
      [&](std::optional<std::string> str, std::optional<std::string> pattern) {
        return evaluateOnce<bool>("re2_search(c0, c1)", str, pattern);
      });
}

TEST_F(Re2FunctionsTest, regexSearchBatch) {
  VectorFunctionTester<bool, std::string, std::string> re2Search(
      "re2_search(c0, c1)");
  testRe2Search(re2Search);
  re2Search.testBatchAll();
}

template <typename F>
void testRe2Extract(F&& regexExtract) {
  // Regex with no subgroup matches.
  EXPECT_THROW(regexExtract(" 123 ", "\\d+", -1), VeloxException);
  EXPECT_EQ(regexExtract("123", "\\d+", 0), "123");
  EXPECT_EQ(regexExtract(" 123 ", "\\d+", 0), "123");
  EXPECT_EQ(
      regexExtract("stringwithmorethan16chars", "^[^\\d]+", 0),
      "stringwithmorethan");
  EXPECT_THROW(regexExtract(" 123 ", "\\d+", 1), VeloxException);
  // Regex with subgroup matches.
  EXPECT_THROW(regexExtract(" 123 ", "(\\d+) ", -1), VeloxException);
  EXPECT_EQ(regexExtract(" 123 ", "(\\d+)", 0), "123");
  EXPECT_EQ(regexExtract(" 123 ", "(\\d+)", 1), "123"); // Partial match.
  EXPECT_EQ(regexExtract(" 123 ", "(\\d+) ", 1), "123"); // Suffix match.
  EXPECT_EQ(regexExtract(" 123 ", " (\\d+)", 1), "123"); // Prefix match
  EXPECT_EQ(regexExtract(" 123 ", " (\\d+) ", 1), "123"); // Full match.
  // Test with string and match that will not be inlined.
  EXPECT_EQ(
      regexExtract("stringwithmorethan16chars", "^([^\\d]+)\\d", 1),
      "stringwithmorethan");
  EXPECT_THROW(regexExtract(" 123 ", " (\\d+)", 2), VeloxException);
  // Regex doesn't match.
  EXPECT_THROW(regexExtract("abc", "\\d+", -1), VeloxException);
  EXPECT_EQ(regexExtract("abc", "\\d+", 0), std::nullopt);
  EXPECT_THROW(regexExtract("abc", "\\d+", 1), VeloxException);
  // Invalid regex.
  EXPECT_THROW(regexExtract("abc", "*", 0), VeloxException);
  // NULL cases.
  EXPECT_EQ(regexExtract(std::nullopt, "\\d+", 0), std::nullopt);
  EXPECT_EQ(regexExtract(" 123 ", std::nullopt, 0), std::nullopt);
  EXPECT_EQ(regexExtract(" 123 ", "\\d+", std::nullopt), std::nullopt);
}

TEST_F(Re2FunctionsTest, regexExtract) {
  testRe2Extract([&](std::optional<std::string> str,
                     std::optional<std::string> pattern,
                     std::optional<int> group) {
    return evaluateOnce<std::string>(
        "re2_extract(c0, c1, c2)", str, pattern, group);
  });
}

TEST_F(Re2FunctionsTest, regexExtractBigintGroupId) {
  testRe2Extract([&](std::optional<std::string> str,
                     std::optional<std::string> pattern,
                     std::optional<int64_t> group) {
    return evaluateOnce<std::string>(
        "re2_extract(c0, c1, c2)", str, pattern, group);
  });
}

TEST_F(Re2FunctionsTest, regexExtractBatch) {
  VectorFunctionTester<std::string, std::string, std::string, int> re2Extract{
      "re2_extract(c0, c1, c2)"};
  testRe2Extract(re2Extract);
  re2Extract.testBatchAll();
}

TEST_F(Re2FunctionsTest, regexExtractConstantPattern) {
  testRe2Extract([&](std::optional<std::string> str,
                     std::optional<std::string> pattern,
                     std::optional<int> group) {
    return pattern ? evaluateOnce<std::string>(
                         "re2_extract(c0, '" + *pattern + "', c1)", str, group)
                   : std::nullopt;
  });
}

TEST_F(Re2FunctionsTest, regexExtractNoGroupId) {
  auto extract =
      ([&](std::optional<std::string> str, std::optional<std::string> pattern) {
        return evaluateOnce<std::string>("re2_extract(c0, c1)", str, pattern);
      });

  EXPECT_EQ(extract("a1 b2 c3", "\\d+"), "1");
  EXPECT_EQ(extract("a b245 c3", "\\d+"), "245");
}

TEST_F(Re2FunctionsTest, regexExtractConstantPatternNoGroupId) {
  auto extract =
      ([&](std::optional<std::string> str, std::optional<std::string> pattern) {
        return evaluateOnce<std::string>(
            "re2_extract(c0, '" + *pattern + "')", str);
      });

  EXPECT_EQ(extract("a1 b2 c3", "\\d+"), "1");
  EXPECT_EQ(extract("a b245 c3", "\\d+"), "245");
}

TEST_F(Re2FunctionsTest, likePattern) {
  auto like = [&](std::optional<std::string> str,
                  std::optional<std::string> pattern) {
    return evaluateOnce<bool>("like(c0, '" + *pattern + "')", str);
  };

  EXPECT_EQ(like("abc", "%b%"), true);
  EXPECT_EQ(like("bcd", "%b%"), true);
  EXPECT_EQ(like("cde", "%b%"), false);
  EXPECT_EQ(like("cde", "def"), false);

  EXPECT_EQ(like("abc", "_b%"), true);
  EXPECT_EQ(like("bcd", "_b%"), false);
  EXPECT_EQ(like("cde", "_b%"), false);

  EXPECT_EQ(like("abc", "b%"), false);
  EXPECT_EQ(like("bcd", "b%"), true);
  EXPECT_EQ(like("cde", "b%"), false);

  EXPECT_EQ(like("abc", "B%"), false);
  EXPECT_EQ(like("bcd", "B%"), false);
  EXPECT_EQ(like("cde", "B%"), false);

  EXPECT_EQ(like("stringwithmorethan16chars", "string%"), true);
  EXPECT_EQ(
      like("stringwithmorethan16chars", "stringwithmorethan16chars"), true);
  EXPECT_EQ(
      like("stringwithmorethan16chars", "stringwithlessthan16chars"), false);

  EXPECT_EQ(
      like(
          "\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 abc",
          "\u4FE1\u5FF5 \u7231%"),
      true);
  EXPECT_EQ(
      like("\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ", "\u4FE1%\u7231%"), true);
  EXPECT_EQ(
      like("\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ", "\u7231\u4FE1%\u7231%"),
      false);

  EXPECT_EQ(like("abc", "MEDIUM POLISHED%"), false);
}

TEST_F(Re2FunctionsTest, likeDeterminePatternKind) {
  auto testPattern =
      [&](StringView pattern, PatternKind patternKind, vector_size_t length) {
        EXPECT_EQ(
            determinePatternKind(pattern), std::make_pair(patternKind, length));
      };

  testPattern("_", PatternKind::kExactlyN, 1);
  testPattern("____", PatternKind::kExactlyN, 4);
  testPattern("%", PatternKind::kAtLeastN, 0);
  testPattern("__%%__", PatternKind::kAtLeastN, 4);
  testPattern("%_%%", PatternKind::kAtLeastN, 1);
  testPattern("_b%%__", PatternKind::kGeneric, 0);
  testPattern("%_%p", PatternKind::kGeneric, 0);

  testPattern("presto", PatternKind::kFixed, 6);
  testPattern("hello", PatternKind::kFixed, 5);
  testPattern("a", PatternKind::kFixed, 1);
  testPattern("helloPrestoWorld", PatternKind::kFixed, 16);
  testPattern("aBcD_", PatternKind::kGeneric, 0);
  testPattern("%aBc_D%", PatternKind::kGeneric, 0);

  testPattern("presto%", PatternKind::kPrefix, 6);
  testPattern("hello%%", PatternKind::kPrefix, 5);
  testPattern("a%", PatternKind::kPrefix, 1);
  testPattern("helloPrestoWorld%%%", PatternKind::kPrefix, 16);
  testPattern("aBcD%%e%", PatternKind::kGeneric, 0);
  testPattern("aBc_D%%", PatternKind::kGeneric, 0);

  testPattern("%presto", PatternKind::kSuffix, 6);
  testPattern("%%hello", PatternKind::kSuffix, 5);
  testPattern("%a", PatternKind::kSuffix, 1);
  testPattern("%%%helloPrestoWorld", PatternKind::kSuffix, 16);
  testPattern("%%_%aBcD", PatternKind::kGeneric, 0);
  testPattern("%%a%%BcD", PatternKind::kGeneric, 0);
  testPattern("foo%bar", PatternKind::kGeneric, 0);
}

TEST_F(Re2FunctionsTest, likePatternWildcard) {
  auto like = [&](std::string str, std::string pattern) {
    auto likeResult = evaluateOnce<bool>(
        fmt::format("like(c0, '{}')", pattern), std::make_optional(str));
    VELOX_CHECK(likeResult, "Like operator evaluation failed");
    return *likeResult;
  };

  EXPECT_TRUE(like("", ""));
  EXPECT_TRUE(like("", "%"));
  EXPECT_TRUE(like("abc", "%%%%"));
  EXPECT_TRUE(like("abc", "%%"));
  EXPECT_TRUE(like("abc", "%_%_%"));
  EXPECT_TRUE(like("abc", "_%_%_%"));
  EXPECT_TRUE(like("a", "_"));
  EXPECT_TRUE(like("ab", "__"));
  EXPECT_TRUE(like("abc", "___"));
  EXPECT_FALSE(like("", "_"));
  EXPECT_FALSE(like("ab", "_"));
  EXPECT_FALSE(like("abcd", "___"));
  EXPECT_FALSE(like("acb", "%ab_%"));
  EXPECT_FALSE(like("abcd", "_abc%d"));
  EXPECT_FALSE(like("abcd", "%ab_c%"));

  EXPECT_TRUE(like(
      generateString(kLikePatternCharacterSet),
      generateString(kWildcardCharacterSet)));
  EXPECT_FALSE(like(
      generateString(kLikePatternCharacterSet),
      generateString(kSingleWildcardCharacter, 65)));

  // Test that % matches newline.
  EXPECT_TRUE(like("\nabcde\n", "%bcd%"));
  EXPECT_TRUE(like("\nabcd", "%bcd"));
  EXPECT_TRUE(like("bcde\n", "bcd%"));
  EXPECT_FALSE(like("\nabcde\n", "bcd%"));
  EXPECT_FALSE(like("\nabcde\n", "%bcd"));
  EXPECT_FALSE(like("\nabcde\n", "%bcf%"));
}

TEST_F(Re2FunctionsTest, likePatternFixed) {
  auto like = [&](std::string str, std::string pattern) {
    auto likeResult = evaluateOnce<bool>(
        fmt::format("like(c0, '{}')", pattern), std::make_optional(str));
    VELOX_CHECK(likeResult, "Like operator evaluation failed");
    return *likeResult;
  };

  EXPECT_TRUE(like("", ""));
  EXPECT_TRUE(like("abcde", "abcde"));
  EXPECT_TRUE(like("ABCDE", "ABCDE"));
  EXPECT_FALSE(like("abcde", "uvwxy"));
  EXPECT_FALSE(like("ABCDE", "abcde"));
  EXPECT_TRUE(like("abc", "%%%%"));
  EXPECT_TRUE(like("abc", "%%"));
  EXPECT_TRUE(like("abc", "%_%_%"));
  EXPECT_TRUE(like("abc", "_%_%_%"));
  EXPECT_TRUE(like("a", "_"));
  EXPECT_FALSE(like("", "_"));
  EXPECT_FALSE(like("ab", "_"));
  EXPECT_FALSE(like("abc", "__%_%_"));
  EXPECT_FALSE(like("abcd", "_c%_%_"));
  EXPECT_TRUE(like("\nab\ncd\n", "\nab\ncd\n"));
  EXPECT_TRUE(like("abcd\n", "abcd\n"));
  EXPECT_TRUE(like("\nabcd", "\nabcd"));
  EXPECT_TRUE(like("\tab\ncd\b", "\tab\ncd\b"));
  EXPECT_FALSE(like("\nabcd\n", "\nabc\nd\n"));
  EXPECT_FALSE(like("\nab\tcd\b", "\nabcd\b"));

  std::string input = generateString(kLikePatternCharacterSet, 66);
  EXPECT_TRUE(like(input, input));
}

TEST_F(Re2FunctionsTest, likePatternPrefix) {
  auto like = [&](std::string str, std::string pattern) {
    auto likeResult = evaluateOnce<bool>(
        fmt::format("like(c0, '{}')", pattern), std::make_optional(str));
    VELOX_CHECK(likeResult, "Like operator evaluation failed");
    return *likeResult;
  };

  EXPECT_TRUE(like("", "%"));
  EXPECT_TRUE(like("", "%%"));
  EXPECT_TRUE(like("abcde", "abcd%"));
  EXPECT_TRUE(like("ABCDE", "ABC%"));
  EXPECT_TRUE(like("abcde", "abcd%%"));
  EXPECT_TRUE(like("ABCDE", "ABC_%"));
  EXPECT_TRUE(like("abcde", "ab%"));
  EXPECT_TRUE(like("ABCDE", "AB%"));
  EXPECT_TRUE(like("abcde", "ab_%"));
  EXPECT_TRUE(like("ABCDE", "AB%%"));
  EXPECT_FALSE(like("", "_%"));
  EXPECT_FALSE(like("abcde", "abce%"));
  EXPECT_FALSE(like("ABCDE", "ABD%"));
  EXPECT_FALSE(like("abcde", "abce%_"));
  EXPECT_FALSE(like("ABCDE", "ABD%%"));
  EXPECT_FALSE(like("abcde", "ad%"));
  EXPECT_FALSE(like("ABCDE", "abc%"));
  EXPECT_FALSE(like("abcde", "ad%%"));
  EXPECT_FALSE(like("ABCDE", "abc_%"));
  EXPECT_TRUE(like("ABC\n", "ABC_"));
  EXPECT_TRUE(like("ABC\n", "ABC_%"));
  EXPECT_TRUE(like("\nABC\n", "_ABC%"));
  EXPECT_TRUE(like("\nabcde\n", "\nab%"));
  EXPECT_TRUE(like("\nab\ncde\n", "\nab\n%"));
  EXPECT_FALSE(like("\nabc\nde\n", "ab\nc%"));
  EXPECT_FALSE(like("\nabc\nde\n", "abc%"));

  std::string input = generateString(kLikePatternCharacterSet, 66);
  EXPECT_TRUE(like(input, input + generateString(kAnyWildcardCharacter)));
}

TEST_F(Re2FunctionsTest, likePatternSuffix) {
  auto like = [&](std::string str, std::string pattern) {
    auto likeResult = evaluateOnce<bool>(
        fmt::format("like(c0, '{}')", pattern), std::make_optional(str));
    VELOX_CHECK(likeResult, "Like operator evaluation failed");
    return *likeResult;
  };

  EXPECT_TRUE(like("", "%"));
  EXPECT_TRUE(like("abcde", "%bcde"));
  EXPECT_TRUE(like("ABCDE", "%CDE"));
  EXPECT_TRUE(like("abcde", "%%cde"));
  EXPECT_TRUE(like("ABCDE", "%%DE"));
  EXPECT_TRUE(like("abcde", "%de"));
  EXPECT_TRUE(like("ABCDE", "%DE"));
  EXPECT_TRUE(like("abcde", "%%e"));
  EXPECT_TRUE(like("ABCDE", "%%E"));
  EXPECT_FALSE(like("", "%_"));
  EXPECT_FALSE(like("abcde", "%ccde"));
  EXPECT_FALSE(like("ABCDE", "%BDE"));
  EXPECT_FALSE(like("abcde", "%%ccde"));
  EXPECT_FALSE(like("ABCDE", "%%BDE"));
  EXPECT_FALSE(like("abcde", "%be"));
  EXPECT_FALSE(like("ABCDE", "%de"));
  EXPECT_FALSE(like("abcde", "%%ce"));
  EXPECT_FALSE(like("ABCDE", "%%e"));
  EXPECT_TRUE(like("\nabc\nde\n", "%\nde\n"));
  EXPECT_TRUE(like("\nabcde\n", "%%de\n"));
  EXPECT_TRUE(like("\nabc\tde\b", "%\tde\b"));
  EXPECT_TRUE(like("\nabcde\t", "%%de\t"));
  EXPECT_TRUE(like("\nabc\nde\t", "%bc_de_"));
  EXPECT_FALSE(like("\nabcde\n", "%de\b"));
  EXPECT_FALSE(like("\nabcde\n", "%d\n"));
  EXPECT_FALSE(like("\nabcde\n", "%e_\n"));

  std::string input = generateString(kLikePatternCharacterSet, 65);
  EXPECT_TRUE(like(input, generateString(kAnyWildcardCharacter) + input));
}

TEST_F(Re2FunctionsTest, likePatternAndEscape) {
  auto like = ([&](std::optional<std::string> str,
                   std::optional<std::string> pattern,
                   std::optional<char> escape) {
    return evaluateOnce<bool>(
        "like(c0, '" + *pattern + "', '" + *escape + "')", str);
  });

  EXPECT_EQ(like("a_c", "%#_%", '#'), true);
  EXPECT_EQ(like("_cd", "%#_%", '#'), true);
  EXPECT_EQ(like("cde", "%#_%", '#'), false);

  EXPECT_EQ(like("a%c", "%#%%", '#'), true);
  EXPECT_EQ(like("%cd", "%#%%", '#'), true);
  EXPECT_EQ(like("cde", "%#%%", '#'), false);

  EXPECT_THROW(like("abcd", "a#}#+", '#'), std::exception);
}

template <typename T>
void Re2FunctionsTest::testRe2ExtractAll(
    const std::vector<std::optional<std::string>>& inputs,
    const std::vector<std::optional<std::string>>& patterns,
    const std::vector<std::optional<T>>& groupIds,
    const std::vector<std::optional<std::vector<std::string>>>& output) {
  std::string constantPattern = "";
  std::string constantGroupId = "";
  std::string expression = "";

  auto result = [&] {
    auto input = makeFlatVector<StringView>(
        inputs.size(),
        [&inputs](vector_size_t row) {
          return inputs[row] ? StringView(*inputs[row]) : StringView();
        },
        [&inputs](vector_size_t row) { return !inputs[row].has_value(); });

    auto pattern = makeFlatVector<StringView>(
        patterns.size(),
        [&patterns](vector_size_t row) {
          return patterns[row] ? StringView(*patterns[row]) : StringView();
        },
        [&patterns](vector_size_t row) { return !patterns[row].has_value(); });
    if (patterns.size() == 1) {
      // Constant pattern
      constantPattern = std::string(", '") + patterns[0].value() + "'";
    }

    auto groupId = makeFlatVector<T>(
        groupIds.size(),
        [&groupIds](vector_size_t row) {
          return groupIds[row] ? *groupIds[row] : 0;
        },
        [&groupIds](vector_size_t row) { return !groupIds[row].has_value(); });
    if (groupIds.size() == 1) {
      // constant groupId
      constantGroupId = std::string(", ") + std::to_string(groupIds[0].value());
    }

    if (!constantPattern.empty()) {
      if (groupIds.empty()) {
        // Case 1: constant pattern, no groupId
        // for example: expression = re2_extract_all(c0, '(\\d+)([a-z]+)')
        expression = std::string("re2_extract_all(c0") + constantPattern + ")";
        return evaluate<ArrayVector>(expression, makeRowVector({input}));
      } else if (!constantGroupId.empty()) {
        // Case 2: constant pattern, constant groupId
        // for example: expression = re2_extract_all(c0, '(\\d+)([a-z]+)', 1)
        expression = std::string("re2_extract_all(c0") + constantPattern +
            constantGroupId + ")";
        return evaluate<ArrayVector>(expression, makeRowVector({input}));
      } else {
        // Case 3: constant pattern, variable groupId
        // for example: expression = re2_extract_all(c0, '(\\d+)([a-z]+)', c1)
        expression =
            std::string("re2_extract_all(c0") + constantPattern + ", c1)";
        return evaluate<ArrayVector>(
            expression, makeRowVector({input, groupId}));
      }
    }

    // Case 4: variable pattern, no groupId
    if (groupIds.empty()) {
      // for example: expression = re2_extract_all(c0, c1)
      expression = std::string("re2_extract_all(c0, c1)");
      return evaluate<ArrayVector>(expression, makeRowVector({input, pattern}));
    }

    // Case 5: variable pattern, constant groupId
    if (!constantGroupId.empty()) {
      // for example: expression = re2_extract_all(c0, c1, 0)
      expression =
          std::string("re2_extract_all(c0, c1") + constantGroupId + ")";
      return evaluate<ArrayVector>(expression, makeRowVector({input, pattern}));
    }

    // Case 6: variable pattern, variable groupId
    expression = std::string("re2_extract_all(c0, c1, c2)");
    return evaluate<ArrayVector>(
        expression, makeRowVector({input, pattern, groupId}));
  }();

  // Creating vectors for output string vectors
  auto sizeAtOutput = [&output](vector_size_t row) {
    return output[row] ? output[row]->size() : 0;
  };
  auto valueAtOutput = [&output](vector_size_t row, vector_size_t idx) {
    return output[row] ? StringView(output[row]->at(idx)) : StringView("");
  };
  auto nullAtOutput = [&output](vector_size_t row) {
    return !output[row].has_value();
  };
  auto expectedResult = makeArrayVector<StringView>(
      output.size(), sizeAtOutput, valueAtOutput, nullAtOutput);

  // Checking the results
  assertEqualVectors(expectedResult, result);
}

TEST_F(Re2FunctionsTest, regexExtractAllConstantPatternNoGroupId) {
  const std::vector<std::optional<std::string>> inputs = {
      "  123a   2b   14m  ", "123a 2b     14m", "123a2b14m"};
  const std::vector<std::optional<std::string>> constantPattern = {
      "(\\d+)([a-z]+)"};
  const std::vector<std::optional<int32_t>> intNoGroupId = {};
  const std::vector<std::optional<int64_t>> bigNoGroupId = {};
  const std::vector<std::optional<std::vector<std::string>>> expectedOutputs = {
      {{"123a", "2b", "14m"}},
      {{"123a", "2b", "14m"}},
      {{"123a", "2b", "14m"}}};

  testRe2ExtractAll(inputs, constantPattern, intNoGroupId, expectedOutputs);
  testRe2ExtractAll(inputs, constantPattern, bigNoGroupId, expectedOutputs);
}

TEST_F(Re2FunctionsTest, regexExtractAllConstantPatternConstantGroupId) {
  const std::vector<std::optional<std::string>> inputs = {
      "  123a   2b   14m  ", "123a 2b     14m", "123a2b14m"};
  const std::vector<std::optional<std::string>> constantPattern = {
      "(\\d+)([a-z]+)"};
  const std::vector<std::optional<int32_t>> intGroupIds = {1};
  const std::vector<std::optional<int64_t>> bigGroupIds = {1};
  const std::vector<std::optional<std::vector<std::string>>> expectedOutputs = {
      {{"123", "2", "14"}}, {{"123", "2", "14"}}, {{"123", "2", "14"}}};

  testRe2ExtractAll(inputs, constantPattern, intGroupIds, expectedOutputs);
  testRe2ExtractAll(inputs, constantPattern, bigGroupIds, expectedOutputs);
}

TEST_F(Re2FunctionsTest, regexExtractAllConstantPatternVariableGroupId) {
  const std::vector<std::optional<std::string>> inputs = {
      "  123a   2b   14m  ",
      "123a 2b     14m",
      "123a2b14m",
      "123a2b14m",
      "123a2b14m",
      "123a2b14m"};
  const std::vector<std::optional<std::string>> constantPattern = {
      "(\\d+)([a-z]+)"};
  const std::vector<std::optional<int32_t>> intGroupIds = {0, 1, 2, 0, 1, 2};
  const std::vector<std::optional<int64_t>> bigGroupIds = {0, 1, 2, 0, 1, 2};
  const std::vector<std::optional<std::vector<std::string>>> expectedOutputs = {
      {{"123a", "2b", "14m"}},
      {{"123", "2", "14"}},
      {{"a", "b", "m"}},
      {{"123a", "2b", "14m"}},
      {{"123", "2", "14"}},
      {{"a", "b", "m"}}};

  testRe2ExtractAll(inputs, constantPattern, intGroupIds, expectedOutputs);
  testRe2ExtractAll(inputs, constantPattern, bigGroupIds, expectedOutputs);
}

TEST_F(Re2FunctionsTest, regexExtractAllVariablePatternNoGroupId) {
  const std::vector<std::optional<std::string>> inputs = {
      "  123a   2b   14m  ", "123a 2b     14m", "123a2b14m"};
  const std::vector<std::optional<std::string>> patterns = {
      "(\\d+)([a-z]+)", "([a-z]+)", "(\\d+)"};
  const std::vector<std::optional<int32_t>> intGroupIds = {};
  const std::vector<std::optional<int64_t>> bigGroupIds = {};
  const std::vector<std::optional<std::vector<std::string>>> expectedOutputs = {
      {{"123a", "2b", "14m"}}, {{"a", "b", "m"}}, {{"123", "2", "14"}}};

  testRe2ExtractAll(inputs, patterns, intGroupIds, expectedOutputs);
  testRe2ExtractAll(inputs, patterns, bigGroupIds, expectedOutputs);
}

TEST_F(Re2FunctionsTest, regexExtractAllVariablePatternConstantGroupId) {
  const std::vector<std::optional<std::string>> inputs = {
      "  123a   2b   14m  ", "a123 b2     m14", "123a2b14m"};
  const std::vector<std::optional<std::string>> patterns = {
      "(\\d+)([a-z]+)", "([a-z]+)(\\d+)", "(\\d+)([a-b]+)"};
  const std::vector<std::optional<int32_t>> intGroupIds = {0};
  const std::vector<std::optional<int64_t>> bigGroupIds = {1};

  const std::vector<std::optional<std::vector<std::string>>>
      expectedOutputsInt = {
          {{"123a", "2b", "14m"}}, {{"a123", "b2", "m14"}}, {{"123a", "2b"}}};
  const std::vector<std::optional<std::vector<std::string>>>
      expectedOutputsBigInt = {
          {{"123", "2", "14"}}, {{"a", "b", "m"}}, {{"123", "2"}}};

  testRe2ExtractAll(inputs, patterns, intGroupIds, expectedOutputsInt);
  testRe2ExtractAll(inputs, patterns, bigGroupIds, expectedOutputsBigInt);
}

TEST_F(Re2FunctionsTest, regexExtractAllVariablePatternVariableGroupId) {
  const std::vector<std::optional<std::string>> inputs = {
      "  123a   2b   14m  ", "a123 b2     m14", "123a2b14m"};
  const std::vector<std::optional<std::string>> patterns = {
      "(\\d+)([a-z]+)", "([a-z]+)(\\d+)", "(\\d+)([a-b]+)"};
  const std::vector<std::optional<int32_t>> intGroupIds = {0, 1, 2};
  const std::vector<std::optional<int64_t>> bigGroupIds = {1, 2, 0};

  const std::vector<std::optional<std::vector<std::string>>>
      expectedOutputsInt = {
          {{"123a", "2b", "14m"}}, {{"a", "b", "m"}}, {{"a", "b"}}};
  const std::vector<std::optional<std::vector<std::string>>>
      expectedOutputsBigInt = {
          {{"123", "2", "14"}}, {{"123", "2", "14"}}, {{"123a", "2b"}}};

  testRe2ExtractAll(inputs, patterns, intGroupIds, expectedOutputsInt);
  testRe2ExtractAll(inputs, patterns, bigGroupIds, expectedOutputsBigInt);
}

TEST_F(Re2FunctionsTest, regexExtractAllNoMatch) {
  const std::vector<std::optional<int32_t>> noGroupId = {};
  const std::vector<std::optional<int32_t>> groupIds0 = {0};

  testRe2ExtractAll({""}, {"[0-9]+"}, noGroupId, {{{}}});
  testRe2ExtractAll({"(╯°□°)╯︵ ┻━┻"}, {"[0-9]+"}, noGroupId, {{{}}});
  testRe2ExtractAll({"abcde"}, {"[0-9]+"}, groupIds0, {{{}}});
  testRe2ExtractAll(
      {"rYBKVn6DnfSI2an4is4jbvf4btGpV"},
      {"81jnp58n31BtMdlUsP1hiF4QWSYv411"},
      noGroupId,
      {{{}}});
  testRe2ExtractAll(
      {"rYBKVn6DnfSI2an4is4jbvf4btGpV"},
      {"81jnp58n31BtMdlUsP1hiF4QWSYv411"},
      groupIds0,
      {{{}}});

  // Test empty pattern.
  testRe2ExtractAll<int32_t>(
      {"abcdef"}, {""}, {}, {{{"", "", "", "", "", "", ""}}});
}

TEST_F(Re2FunctionsTest, regexExtractAllBadArgs) {
  const auto eval = [&](std::optional<std::string> str,
                        std::optional<std::string> pattern,
                        std::optional<int32_t> groupId) {
    const std::string expression = "re2_extract_all(c0, c1, c2)";
    return evaluateOnce<std::string>(expression, str, pattern, groupId);
  };
  EXPECT_THROW(eval("123", std::nullopt, 0), VeloxException);
  EXPECT_THROW(eval(std::nullopt, "(\\d+)", 0), VeloxException);
  EXPECT_THROW(eval("", "", 123), VeloxException);
  EXPECT_THROW(eval("123", "", 99), VeloxException);
  EXPECT_THROW(eval("123", "(\\d+)", 1), VeloxException);
  EXPECT_THROW(eval("123", "[a-z]+", 1), VeloxException);
}

} // namespace
} // namespace facebook::velox::functions
