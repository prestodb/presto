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
#include <functional>
#include <optional>

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/functions/lib/Re2Functions.h"
#include "velox/functions/sparksql/RegexFunctions.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql {

using namespace facebook::velox::test;
namespace {

class RegexFunctionsTest : public test::SparkFunctionBaseTest {
 public:
  std::optional<bool> rlike(
      std::optional<std::string> str,
      std::string pattern) {
    return evaluateOnce<bool>(fmt::format("rlike(c0, '{}')", pattern), str);
  }

  std::optional<std::string> regexp_extract(
      std::optional<std::string> str,
      std::string pattern) {
    return evaluateOnce<std::string>(
        fmt::format("regexp_extract(c0, '{}')", pattern), str);
  }

  std::string testRegexReplace(
      const std::optional<std::string>& input,
      const std::string& pattern,
      const std::string& replace,
      std::optional<int32_t> position = std::nullopt) {
    auto result = [&] {
      if (!position) {
        return evaluateOnce<std::string>(
            fmt::format("regex_replace(c0, '{}', '{}')", pattern, replace),
            input);
      } else {
        return evaluateOnce<std::string>(
            fmt::format(
                "regex_replace(c0, '{}', '{}', {})",
                pattern,
                replace,
                position.value()),
            input);
      }
    }();
    return result.value();
  }

  std::shared_ptr<facebook::velox::SimpleVector<facebook::velox::StringView>>
  testingRegexReplaceRows(
      const std::vector<std::string>& input,
      const std::vector<std::string>& pattern,
      const std::vector<std::string>& replace,
      const std::optional<std::vector<int32_t>>& position = std::nullopt,
      int repeatCount = 1) {
    EXPECT_GT(repeatCount, 0);

    // Repeat the inputs to allow for testing very large dataframes.
    std::vector<std::string> repeatedInput = repeatVector(input, repeatCount);
    std::vector<std::string> repeatedPattern =
        repeatVector(pattern, repeatCount);
    std::vector<std::string> repeatedReplace =
        repeatVector(replace, repeatCount);
    std::vector<int32_t> repeatedPosition = position.has_value()
        ? repeatVector(*position, repeatCount)
        : std::vector<int32_t>();

    auto inputStringVector = makeFlatVector<std::string>(repeatedInput);
    auto patternStringVector = makeFlatVector<std::string>(repeatedPattern);
    auto replaceStringVector = makeFlatVector<std::string>(repeatedReplace);
    auto positionIntVector = makeFlatVector<int32_t>(repeatedPosition);

    std::shared_ptr<SimpleVector<StringView>> result;
    if (position) {
      result = evaluate<SimpleVector<StringView>>(
          "regex_replace(c0, c1, c2, c3)",
          makeRowVector(
              {inputStringVector,
               patternStringVector,
               replaceStringVector,
               positionIntVector}));
    } else {
      result = evaluate<SimpleVector<StringView>>(
          "regex_replace(c0, c1, c2)",
          makeRowVector(
              {inputStringVector, patternStringVector, replaceStringVector}));
    }
    return result;
  }

  std::shared_ptr<facebook::velox::SimpleVector<facebook::velox::StringView>>
  testingRegexReplaceConstantPattern(
      const std::vector<std::string>& input,
      const std::string& pattern,
      const std::vector<std::string>& replace,
      const std::optional<std::vector<int32_t>>& position = std::nullopt) {
    auto inputStringVector = makeFlatVector<std::string>(input);
    auto replaceStringVector = makeFlatVector<std::string>(replace);

    std::shared_ptr<SimpleVector<StringView>> result;
    if (position) {
      auto positionIntVector = makeFlatVector<int32_t>(*position);

      result = evaluate<SimpleVector<StringView>>(
          fmt::format("regex_replace(c0, '{}', c1, c2)", pattern),
          makeRowVector(
              {inputStringVector, replaceStringVector, positionIntVector}));
    } else {
      result = evaluate<SimpleVector<StringView>>(
          fmt::format("regex_replace(c0, '{}', c1)", pattern),
          makeRowVector({inputStringVector, replaceStringVector}));
    }
    return result;
  }

  template <typename T>
  std::vector<T> repeatVector(const std::vector<T>& vec, int repeatCount) {
    std::vector<T> result(vec.size() * repeatCount);
    for (int i = 0; i < repeatCount; ++i) {
      std::copy(vec.begin(), vec.end(), result.begin() + i * vec.size());
    }
    return result;
  }

  std::shared_ptr<facebook::velox::FlatVector<facebook::velox::StringView>>
  convertOutput(const std::vector<std::string>& output, size_t repeatCount) {
    std::vector<std::optional<facebook::velox::StringView>> repeatedOutput(
        output.size() * repeatCount);

    using StringView = facebook::velox::StringView;

    for (size_t i = 0; i < repeatCount; ++i) {
      for (size_t j = 0; j < output.size(); ++j) {
        repeatedOutput[i * output.size() + j] = !output[j].empty()
            ? std::optional<StringView>(output[j])
            : std::nullopt;
      }
    }

    return makeNullableFlatVector(repeatedOutput);
  }
};

// A list of known incompatibilities with java.util.regex. Most result in an
// error being thrown; some unsupported character class features result in
// different results.
TEST_F(RegexFunctionsTest, javaRegexIncompatibilities) {
  // Character class union is not supported; parsed as [a\[b]\].
  EXPECT_THROW(rlike("[]", R"([a[b]])"), VeloxUserError);
  // Character class intersection not supported; parsed as [a&\[b]\].
  EXPECT_THROW(rlike("&]", R"([a&&[b]])"), VeloxUserError);
  // Character class difference not supported; parsed as [\w&\[\^b]\].
  EXPECT_THROW(rlike("^]", R"([\w&&[^b]])"), VeloxUserError);
  // Unsupported character classes.
  EXPECT_THROW(rlike(" ", "\\h"), VeloxUserError);
  EXPECT_THROW(rlike(" ", "\\H"), VeloxUserError);
  EXPECT_THROW(rlike(" ", "\\V"), VeloxUserError);
  EXPECT_THROW(rlike(" ", "\\uffff"), VeloxUserError);
  EXPECT_THROW(rlike(" ", "\\e"), VeloxUserError);
  EXPECT_THROW(rlike(" ", "\\c1"), VeloxUserError);
  EXPECT_THROW(rlike(" ", "\\G"), VeloxUserError);
  EXPECT_THROW(rlike(" ", "\\Z"), VeloxUserError);
  EXPECT_THROW(rlike(" ", "\\R"), VeloxUserError);
  // Backreferences not supported.
  EXPECT_THROW(rlike("00", R"((\d)\1)"), VeloxUserError);
  // Possessive quantifiers not supported.
  EXPECT_THROW(rlike(" ", " ?+"), VeloxUserError);
  EXPECT_THROW(rlike(" ", " *+"), VeloxUserError);
  EXPECT_THROW(rlike(" ", " ++"), VeloxUserError);
  EXPECT_THROW(rlike(" ", " {1}+"), VeloxUserError);
  // Possessive quantifiers not supported.
  EXPECT_THROW(rlike(" ", " ?+"), VeloxUserError);
  EXPECT_THROW(rlike(" ", " *+"), VeloxUserError);
  EXPECT_THROW(rlike(" ", " ++"), VeloxUserError);
  EXPECT_THROW(rlike(" ", " {1}+"), VeloxUserError);
  // Lookahead.
  EXPECT_THROW(rlike(" ", "(?= )"), VeloxUserError);
  EXPECT_THROW(rlike(" ", "(?! )"), VeloxUserError);
  EXPECT_THROW(rlike(" ", "(?<= )"), VeloxUserError);
  EXPECT_THROW(rlike(" ", "(?<! )"), VeloxUserError);
  EXPECT_THROW(rlike(" ", "(?<! )"), VeloxUserError);
}

TEST_F(RegexFunctionsTest, allowSimpleConstantRegex) {
  // rlike returns std::optional<bool>; EXPECT_TRUE would check for non-null,
  // not check the result.
  EXPECT_EQ(rlike("a", "a*"), true);
  EXPECT_EQ(rlike("b", "a*"), true);
  EXPECT_EQ(rlike("b", "a+"), false);
  EXPECT_EQ(rlike("a", "^[ab]*$"), true);
  EXPECT_EQ(rlike(std::nullopt, "a*"), std::nullopt);
}

TEST_F(RegexFunctionsTest, blockUnsupportedEdgeCases) {
  // Non-constant pattern.
  EXPECT_THROW(
      evaluateOnce<bool>("rlike('a', c0)", std::optional<std::string>("a*")),
      VeloxUserError);
  // Unsupported set union syntax.
  EXPECT_THROW(rlike("", "[a[b]]"), VeloxUserError);
}

TEST_F(RegexFunctionsTest, regexMatchRegistration) {
  EXPECT_THROW(
      evaluateOnce<std::string>(
          "regexp_extract('a', c0)", std::optional<std::string>("a*")),
      VeloxUserError);
  EXPECT_EQ(regexp_extract("abc", "a."), "ab");
  EXPECT_THROW(regexp_extract("[]", "[a[b]]"), VeloxUserError);
}

TEST_F(RegexFunctionsTest, regexReplaceRegistration) {
  std::string output = "teeheebc";
  auto result = testRegexReplace("abc", "a", "teehee");
  EXPECT_EQ(result, output);
}

TEST_F(RegexFunctionsTest, regexReplaceEmptyString) {
  std::string output = "";
  auto result = testRegexReplace("", "empty string", "nothing");
  EXPECT_EQ(result, output);
}

TEST_F(RegexFunctionsTest, regexReplaceSimple) {
  std::string output = "HeLLo WorLd";
  auto result = testRegexReplace("Hello World", "l", "L");
  EXPECT_EQ(result, output);
}

TEST_F(RegexFunctionsTest, regexReplaceSimplePosition) {
  std::string output = "Hello WorLd";
  auto result = testRegexReplace("Hello World", "l", "L", {6});
  EXPECT_EQ(result, output);
}

TEST_F(RegexFunctionsTest, regexReplaceNonAscii) {
  std::string output = "♫ Resume is updated!♫ ";
  auto result = testRegexReplace("♫ Resume is updated¡♫ ", "¡", "!");
  EXPECT_EQ(result, output);
}

TEST_F(RegexFunctionsTest, regexReplaceNonAsciiPositionNoChange) {
  std::string output = "♫ Resume is updated¡♫ Some padding";
  auto result =
      testRegexReplace("♫ Resume is updated¡♫ Some padding", "¡", "!", {21});
  EXPECT_EQ(result, output);
}

TEST_F(RegexFunctionsTest, regexReplaceNonAsciiPositionWithChange) {
  std::string output = "♫ Resume is updated!♫ Some padding";
  auto result =
      testRegexReplace("♫ Resume is updated¡♫ Some padding", "¡", "!", {20});
  EXPECT_EQ(result, output);
}

TEST_F(RegexFunctionsTest, regexReplaceMatchSparkSqlTestSimple) {
  std::vector<int32_t> positions = {1, 1, 1};
  const std::vector<std::string> outputVector = {"300", "400", "400-400"};
  auto result = testingRegexReplaceRows(
      {"100-200", "100-200", "100-200"},
      {"(\\d+)-(\\d+)", "(\\d+)-(\\d+)", "(\\d+)"},
      {"300", "400", "400"},
      positions);
  auto output = convertOutput({"300", "400", "400-400"}, 1);
  assertEqualVectors(result, output);
}

TEST_F(RegexFunctionsTest, regexReplaceWithEmptyString) {
  std::string output = "bc";
  auto result = testRegexReplace("abc", "a", "");
  EXPECT_EQ(result, output);
}

TEST_F(RegexFunctionsTest, regexBadJavaPattern) {
  EXPECT_THROW(testRegexReplace("[]", "[a[b]]", ""), VeloxUserError);
  EXPECT_THROW(testRegexReplace("[]", "[a&&[b]]", ""), VeloxUserError);
  EXPECT_THROW(testRegexReplace("[]", "[a&&[^b]]", ""), VeloxUserError);
}

TEST_F(RegexFunctionsTest, regexReplaceInvalidUTF8) {
  EXPECT_THROW(
      testRegexReplace(std::string("\xA0") + "bcacbdefg", "", "", {2}),
      VeloxUserError);
}

TEST_F(RegexFunctionsTest, regexReplacePosition) {
  std::string output1 = "abc";
  std::string output2 = "bc";
  std::string output3 = "aaaaa";
  auto result1 = testRegexReplace("abca", "a", "", {2});
  auto result2 = testRegexReplace("abca", "a", "", {1});
  auto result3 = testRegexReplace("abca", "bc", "aaa", {1});
  EXPECT_EQ(result1, output1);
  EXPECT_EQ(result2, output2);
  EXPECT_EQ(result3, output3);
}

TEST_F(RegexFunctionsTest, regexReplaceNegativePosition) {
  EXPECT_THROW(testRegexReplace("abc", "a", "", {-1}), VeloxUserError);
}

TEST_F(RegexFunctionsTest, regexReplaceZeroPosition) {
  EXPECT_THROW(testRegexReplace("abc", "a", "", {0}), VeloxUserError);
}

TEST_F(RegexFunctionsTest, regexReplacePositionTooLarge) {
  std::string output = "abc";
  auto result1 = testRegexReplace("abc", "a", "", {1000});
  auto result2 = testRegexReplace("abc", "a", "", {4});
  EXPECT_EQ(result1, output);
  EXPECT_EQ(result2, output);
}

TEST_F(RegexFunctionsTest, regexReplaceSpecialCharacters) {
  std::string output = "abca";
  auto result = testRegexReplace("a.b.c.a", "\\.", "");
  EXPECT_EQ(result, output);
}

TEST_F(RegexFunctionsTest, regexReplaceNoReplacement) {
  std::string output = "abcde";
  auto result = testRegexReplace("abcde", "f", "z");
  EXPECT_EQ(result, output);
}

TEST_F(RegexFunctionsTest, regexReplaceMultipleMatches) {
  std::string output = "bb";
  auto result = testRegexReplace("aa", "a", "b");
  EXPECT_EQ(result, output);
}

TEST_F(RegexFunctionsTest, regexReplaceWord) {
  std::string output = "I like cake";
  auto result = testRegexReplace("I like pie", "pie", "cake");
}

TEST_F(RegexFunctionsTest, regexReplaceEscapedCharacters) {
  std::string output = "abcde";
  auto result = testRegexReplace("abc\\de", "\\\\", "");
  EXPECT_EQ(result, output);
}

TEST_F(RegexFunctionsTest, regexReplacePatternBeforePosition) {
  std::string output = "abcdef";
  auto result = testRegexReplace("abcdef", "d", "z", {5});
  EXPECT_EQ(result, output);
}

TEST_F(RegexFunctionsTest, regexReplaceConstantPattern) {
  std::vector<int32_t> positions = {1, 2};
  const std::vector<std::string> outputVector = {
      "the sky was blue", "coding isn't fun"};
  auto result = testingRegexReplaceConstantPattern(
      {"the sky is blue", "coding is fun"}, "is", {"was", "isn't"}, positions);
  auto output = convertOutput(outputVector, 1);
  assertEqualVectors(result, output);
}

TEST_F(RegexFunctionsTest, regexReplaceDataframe) {
  // Basic Replacement
  std::vector<int32_t> positions = {1, 2};
  const std::vector<std::string> outputVector = {"hi world", "coding was fun"};
  auto result = testingRegexReplaceRows(
      {"hello world", "coding is fun"},
      {"hello", " is"},
      {"hi", " was"},
      positions);
  auto output = convertOutput(outputVector, 1);
  assertEqualVectors(result, output);
}
TEST_F(RegexFunctionsTest, regexReplaceDataframeMultiple) {
  // Multiple matches
  std::vector<int32_t> positions = {1, 1};
  const std::vector<std::string> outputVector = {
      "fruit fruit fruit", "fruit fruit fruit"};
  auto result = testingRegexReplaceRows(
      {"apple apple apple", "banana banana banana"},
      {"apple", "banana"},
      {"fruit", "fruit"},
      positions);
  auto output = convertOutput(outputVector, 1);
  assertEqualVectors(result, output);
}
TEST_F(RegexFunctionsTest, regexReplaceDataframeSpecial) {
  // Special characters
  std::vector<int32_t> positions = {1, 1};
  const std::vector<std::string> outputVector = {"a-b-c", "coding"};
  auto result = testingRegexReplaceRows(
      {"a.b.c", "[coding]"}, {R"(\.)", R"(\[|\])"}, {"-", ""}, positions);
  auto output = convertOutput(outputVector, 1);
  assertEqualVectors(result, output);
}
TEST_F(RegexFunctionsTest, regexReplaceDataframeSizes) {
  // Replacement with different sizes
  std::vector<int32_t> positions = {1, 1};
  const std::vector<std::string> outputVector = {"fantastic day", "short"};
  auto result = testingRegexReplaceRows(
      {"good day", "shorter"},
      {"good", "shorter"},
      {"fantastic", "short"},
      positions);
  auto output = convertOutput(outputVector, 1);
  assertEqualVectors(result, output);
}
TEST_F(RegexFunctionsTest, regexReplaceDataframeNoMatches) {
  // No matches
  std::vector<int32_t> positions = {1, 1};
  const std::vector<std::string> outputVector = {"apple", "banana"};
  auto result = testingRegexReplaceRows(
      {"apple", "banana"}, {"orange", "grape"}, {"fruit", "fruit"}, positions);
  auto output = convertOutput(outputVector, 1);
  assertEqualVectors(result, output);
}
TEST_F(RegexFunctionsTest, regexReplaceDataframeOffsetPosition) {
  // Offset position
  std::vector<int32_t> positions = {9, 6};
  const std::vector<std::string> outputVector = {
      "apple pie fruit", "grape fruit grape"};
  auto result = testingRegexReplaceRows(
      {"apple pie apple", "grape banana grape"},
      {"apple", "banana"},
      {"fruit", "fruit"},
      positions);
  auto output = convertOutput(outputVector, 1);
  assertEqualVectors(result, output);
}
TEST_F(RegexFunctionsTest, regexReplaceEmptyStringsAndPatterns) {
  // Empty strings and patterns
  std::vector<int32_t> positions = {1, 1};
  const std::vector<std::string> outputVector = {"prefix ", "he world"};
  auto result = testingRegexReplaceRows(
      {"", "hello"}, {"", "llo"}, {"prefix ", " world"}, positions);
  auto output = convertOutput(outputVector, 1);
  assertEqualVectors(result, output);
}
TEST_F(RegexFunctionsTest, regexReplaceDataframeCharacterTypes) {
  // Multiple character types
  std::vector<int32_t> positions = {1, 1};
  const std::vector<std::string> outputVector = {"XXXABC", "YYY"};
  auto result = testingRegexReplaceRows(
      {"123ABC", "!@#"}, {R"(\d)", R"(\W)"}, {"X", "Y"}, positions);
  auto output = convertOutput(outputVector, 1);
  assertEqualVectors(result, output);
}
TEST_F(RegexFunctionsTest, regexReplaceDataframeBadPosition) {
  // Larger offsets than string size
  std::vector<int32_t> positions = {10, 15};
  const std::vector<std::string> outputVector = {"apple", "banana"};
  auto result = testingRegexReplaceRows(
      {"apple", "banana"}, {"apple", "banana"}, {"fruit", "fruit"}, positions);
  auto output = convertOutput(outputVector, 1);
  assertEqualVectors(result, output);
}

TEST_F(RegexFunctionsTest, regexReplaceDataframeLastCharacter) {
  std::vector<int32_t> positions = {5, 6};
  const std::vector<std::string> outputVector = {"apple", "banana"};
  auto result = testingRegexReplaceRows(
      {"applez", "bananaz"}, {"z", "z"}, {"", ""}, positions);
  auto output = convertOutput(outputVector, 1);
  assertEqualVectors(result, output);
}

// Test to match
// https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/StringFunctionsSuite.scala#L180-L184
// This test is the crux of why regex_replace needed to support non-constant
// parameters. Used position {0,0} out of convenience, ideally we create another
// function that does not pass a position parameter.
TEST_F(RegexFunctionsTest, regexReplaceMatchSparkSqlTest) {
  std::vector<int32_t> positions = {1, 1, 1};
  const std::vector<std::string> outputVector = {"300", "400", "400-400"};
  auto result = testingRegexReplaceRows(
      {"100-200", "100-200", "100-200"},
      {"(\\d+)-(\\d+)", "(\\d+)-(\\d+)", "(\\d+)"},
      {"300", "400", "400"},
      positions);
  auto output = convertOutput(outputVector, 1);
  assertEqualVectors(result, output);
}
TEST_F(RegexFunctionsTest, regexReplaceRowsNoPosition) {
  const std::vector<std::string> outputVector = {"300", "400", "400-400"};
  auto result = testingRegexReplaceRows(
      {"100-200", "100-200", "100-200"},
      {"(\\d+)-(\\d+)", "(\\d+)-(\\d+)", "(\\d+)"},
      {"300", "400", "400"});
  auto output = convertOutput(outputVector, 1);
  assertEqualVectors(result, output);
}

TEST_F(RegexFunctionsTest, regexReplaceMassiveVectors) {
  std::vector<int32_t> positions = {1, 1, 1};
  const std::vector<std::string> outputVector = {"300", "400", "400-400"};
  auto result = testingRegexReplaceRows(
      {"100-200", "100-200", "100-200"},
      {"(\\d+)-(\\d+)", "(\\d+)-(\\d+)", "(\\d+)"},
      {"300", "400", "400"},
      positions,
      100000);
  auto output = convertOutput(outputVector, 100000);
  assertEqualVectors(result, output);
}

TEST_F(RegexFunctionsTest, regexReplaceCacheLimitTest) {
  std::vector<std::string> patterns;
  std::vector<std::string> strings;
  std::vector<std::string> replaces;
  std::vector<std::string> expectedOutputs;

  for (int i = 0; i <= kMaxCompiledRegexes; ++i) {
    patterns.push_back("\\d" + std::to_string(i) + "-\\d" + std::to_string(i));
    strings.push_back("1" + std::to_string(i) + "-2" + std::to_string(i));
    replaces.push_back("X" + std::to_string(i) + "-Y" + std::to_string(i));
    expectedOutputs.push_back(
        "X" + std::to_string(i) + "-Y" + std::to_string(i));
  }

  EXPECT_THROW(
      testingRegexReplaceRows(strings, patterns, replaces), VeloxUserError);
}

TEST_F(RegexFunctionsTest, regexReplaceCacheMissLimit) {
  std::vector<std::string> patterns;
  std::vector<std::string> strings;
  std::vector<std::string> replaces;
  std::vector<std::string> expectedOutputs;
  std::vector<int32_t> positions;

  for (int i = 0; i <= kMaxCompiledRegexes - 1; ++i) {
    patterns.push_back("\\d" + std::to_string(i) + "-\\d" + std::to_string(i));
    strings.push_back("1" + std::to_string(i) + "-2" + std::to_string(i));
    replaces.push_back("X" + std::to_string(i) + "-Y" + std::to_string(i));
    expectedOutputs.push_back(
        "X" + std::to_string(i) + "-Y" + std::to_string(i));
    positions.push_back(1);
  }

  auto result =
      testingRegexReplaceRows(strings, patterns, replaces, positions, 50000);
  auto output = convertOutput(expectedOutputs, 50000);
  assertEqualVectors(result, output);
}

} // namespace
} // namespace facebook::velox::functions::sparksql
