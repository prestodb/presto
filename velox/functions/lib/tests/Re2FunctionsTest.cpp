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
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
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
  template <typename T>
  void testRe2ExtractAll(
      const std::vector<std::optional<std::string>>& inputs,
      const std::vector<std::optional<std::string>>& patterns,
      const std::vector<std::optional<T>>& groupIds,
      const std::vector<std::optional<std::vector<std::string>>>& output);
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
