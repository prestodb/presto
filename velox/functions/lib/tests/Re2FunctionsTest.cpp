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

#include "velox/exec/tests/utils/FunctionUtils.h"
#include "velox/functions/common/tests/FunctionBaseTest.h"

namespace facebook::velox::functions {
namespace {

class Re2FunctionsTest : public test::FunctionBaseTest {
 public:
  static void SetUpTestCase() {
    exec::test::registerTypeResolver();
    exec::registerStatefulVectorFunction(
        "re2_match", re2MatchSignatures(), makeRe2Match);
    exec::registerStatefulVectorFunction(
        "re2_search", re2SearchSignatures(), makeRe2Search);
    exec::registerStatefulVectorFunction(
        "re2_extract", re2ExtractSignatures(), makeRe2Extract);
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

TEST_F(Re2FunctionsTest, regexMatchBadArgs) {
  const std::optional<std::string> null_string{};
  // Can't provide multiple template arguments inline in the EXPECT macro.
  EXPECT_THROW(
      evaluateOnce<bool>("re2_match(c0, '', '')", null_string),
      std::invalid_argument);
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

} // namespace
} // namespace facebook::velox::functions
