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

using namespace facebook::velox::test;
namespace facebook::velox::functions::sparksql::test {
namespace {

class SplitTest : public SparkFunctionBaseTest {
 protected:
  void testSplit(
      const std::vector<std::string>& input,
      const std::string& delim,
      std::optional<int32_t> limit,
      size_t numRows,
      const std::vector<std::vector<std::string>>& expected) {
    auto strings = makeFlatVector(input);
    auto delims = makeConstant(StringView{delim}, numRows);
    auto expectedResult = makeArrayVector(expected);
    VectorPtr result;
    if (limit.has_value()) {
      auto limits = makeConstant<int32_t>(limit.value(), numRows);
      auto input = makeRowVector({strings, delims, limits});
      result = evaluate("split(c0, c1, c2)", input);
    } else {
      auto input = makeRowVector({strings, delims});
      result = evaluate("split(c0, c1)", input);
    }
    assertEqualVectors(expectedResult, result);
  }

  void testSplitConstantDelim(
      const std::vector<std::string>& input,
      const std::string& delim,
      std::optional<int32_t> limit,
      size_t numRows,
      const std::vector<std::vector<std::string>>& expected) {
    auto strings = makeFlatVector(input);
    auto expectedResult = makeArrayVector(expected);
    VectorPtr result;
    if (limit.has_value()) {
      auto limits = makeConstant<int32_t>(limit.value(), numRows);
      auto input = makeRowVector({strings, limits});
      result = evaluate("split(c0, '" + delim + "', c1)", input);
    } else {
      auto input = makeRowVector({strings});
      result = evaluate("split(c0, '" + delim + "')", input);
    }
    assertEqualVectors(expectedResult, result);
  }
};

TEST_F(SplitTest, basic) {
  auto numRows = 4;
  auto input = std::vector<std::string>{
      {"I,he,she,they"}, // Simple
      {"one,,,four,"}, // Empty strings
      {"a,\xED,\xA0,123"}, // Not a well-formed UTF-8 string
      {""}, // The whole string is empty
  };
  auto expected = std::vector<std::vector<std::string>>({
      {"I", "he", "she", "they"},
      {"one", "", "", "four", ""},
      {"a", "\xED", "\xA0", "123"},
      {""},
  });

  // No limit provided.
  testSplit(input, ",", std::nullopt, numRows, expected);

  // Limit <= 0.
  testSplit(input, ",", -1, numRows, expected);

  // High limit, the limit greater than the input string size.
  testSplit(input, ",", 10, numRows, expected);

  // Small limit, the limit is smaller than or equals to input string size.
  expected = {
      {"I", "he", "she,they"},
      {"one", "", ",four,"},
      {"a", "\xED", "\xA0,123"},
      {""},
  };
  testSplit(input, ",", 3, numRows, expected);

  // limit = 1, the resulting array only has one entry to contain all input.
  expected = {
      {"I,he,she,they"},
      {"one,,,four,"},
      {"a,\xED,\xA0,123"},
      {""},
  };
  testSplit(input, ",", 1, numRows, expected);

  // Non-ascii delimiter.
  input = {
      {"синяя сливаలేదా赤いトマトలేదా黃苹果లేదాbrown pear"},
      {"зелёное небоలేదాలేదాలేదా緑の空లేదా"},
      {"aలేదా\xEDలేదా\xA0లేదా123"},
      {""},
  };
  expected = {
      {"синяя слива", "赤いトマト", "黃苹果", "brown pear"},
      {"зелёное небо", "", "", "緑の空", ""},
      {"a", "\xED", "\xA0", "123"},
      {""},
  };
  testSplit(input, "లేదా", std::nullopt, numRows, expected);
  testSplit(input, "లేదా", -1, numRows, expected);
  testSplit(input, "లేదా", 10, numRows, expected);
  expected = {
      {"синяя слива", "赤いトマト", "黃苹果లేదాbrown pear"},
      {"зелёное небо", "", "లేదా緑の空లేదా"},
      {"a", "\xED", "\xA0లేదా123"},
      {""},
  };
  testSplit(input, "లేదా", 3, numRows, expected);
  expected = {
      {"синяя сливаలేదా赤いトマトలేదా黃苹果లేదాbrown pear"},
      {"зелёное небоలేదాలేదాలేదా緑の空లేదా"},
      {"aలేదా\xEDలేదా\xA0లేదా123"},
      {""},
  };
  testSplit(input, "లేదా", 1, numRows, expected);

  // Cover case that delimiter not exists in input string.
  testSplit(input, "A", std::nullopt, numRows, expected);
}

TEST_F(SplitTest, emptyDelimiter) {
  auto numRows = 4;
  auto input = std::vector<std::string>{
      {"I,he,she,they"}, // Simple
      {"one,,,four,"}, // Empty strings
      {"a\xED\xA0@123"}, // Not a well-formed UTF-8 string
      {""}, // The whole string is empty
  };
  auto expected = std::vector<std::vector<std::string>>({
      {"I", ",", "h", "e", ",", "s", "h", "e", ",", "t", "h", "e", "y"},
      {"o", "n", "e", ",", ",", ",", "f", "o", "u", "r", ","},
      {"a", "\xED\xA0", "@", "1", "2", "3"},
      {""},
  });

  // No limit provided.
  testSplit(input, "", std::nullopt, numRows, expected);

  // Limit <= 0.
  testSplit(input, "", -1, numRows, expected);

  // High limit, the limit greater than the input string size.
  testSplit(input, "", 20, numRows, expected);

  // Small limit, the limit is smaller than or equals to input string size.
  expected = {
      {"I", ",", "h"},
      {"o", "n", "e"},
      {"a", "\xED\xA0", "@"},
      {""},
  };
  testSplit(input, "", 3, numRows, expected);

  // limit = 1.
  expected = {
      {"I"},
      {"o"},
      {"a"},
      {""},
  };
  testSplit(input, "", 1, numRows, expected);

  // Non-ascii, empty delimiter.
  input = std::vector<std::string>{
      {"синяя赤いトマト緑の"},
      {"Hello世界🙂"},
      {"a\xED\xA0@123"}, // Not a well-formed UTF-8 string
      {""},
  };
  expected = {
      {"с", "и", "н", "я", "я", "赤", "い", "ト", "マ", "ト", "緑", "の"},
      {"H", "e", "l", "l", "o", "世", "界", "🙂"},
      {"a", "\xED\xA0", "@", "1", "2", "3"},
      {""},
  };
  testSplit(input, "", std::nullopt, numRows, expected);

  expected = {
      {"с", "и"},
      {"H", "e"},
      {"a", "\xED\xA0"},
      {""},
  };
  testSplit(input, "", 2, numRows, expected);
}

TEST_F(SplitTest, regexDelimiter) {
  auto input = std::vector<std::string>{
      "1a 2b \xA0 14m",
      "1a 2b \xA0 14",
      "",
      "a123b",
  };
  auto numRows = 4;
  auto expected = std::vector<std::vector<std::string>>({
      {"1", "2", "\xA0 14", ""},
      {"1", "2", "\xA0 14"},
      {""},
      {"", "123", ""},
  });

  // No limit.
  testSplit(input, "\\s*[a-z]+\\s*", std::nullopt, numRows, expected);
  // Limit < 0.
  testSplit(input, "\\s*[a-z]+\\s*", -1, numRows, expected);
  // High limit.
  testSplit(input, "\\s*[a-z]+\\s*", 10, numRows, expected);
  // Small limit.
  expected = {
      {"1", "2", "\xA0 14m"},
      {"1", "2", "\xA0 14"},
      {""},
      {"", "123", ""},
  };
  testSplit(input, "\\s*[a-z]+\\s*", 3, numRows, expected);
  expected = {
      {"1a 2b \xA0 14m"},
      {"1a 2b \xA0 14"},
      {""},
      {"a123b"},
  };
  testSplit(input, "\\s*[a-z]+\\s*", 1, numRows, expected);

  numRows = 3;
  input = std::vector<std::string>{
      {"синя🙂赤トマト🙂緑の"},
      {"Hello🙂世界\xED🙂"},
      {""},
  };
  expected = std::vector<std::vector<std::string>>({
      {"с", "и", "н", "я", "🙂", "赤", "ト", "マ", "ト", "🙂", "緑", "の", ""},
      {"H", "e", "l", "l", "o", "🙂", "世", "界", "\xED", "🙂", ""},
      {""},
  });

  testSplit(input, "A|", std::nullopt, numRows, expected);

  expected = {
      {"с", "иня🙂赤トマト🙂緑の"},
      {"H", "ello🙂世界\xED🙂"},
      {""},
  };
  testSplit(input, "A|", 2, numRows, expected);

  expected = {
      {"синя", "赤トマト", "緑の"},
      {"Hello", "世界\xED", ""},
      {""},
  };
  testSplit(input, "🙂", -1, numRows, expected);
}

TEST_F(SplitTest, fastPath) {
  auto input = std::vector<std::string>{
      {"I$he$she$they"}, // Simple
      {"one$$$four$"}, // Empty strings
      {"a$\xED$\xA0$123"}, // Not a well-formed UTF-8 string
      {""}, // The whole string is empty
  };
  auto expected = std::vector<std::vector<std::string>>({
      {"I", "he", "she", "they"},
      {"one", "", "", "four", ""},
      {"a", "\xED", "\xA0", "123"},
      {""},
  });

  // Escaped delimiter.
  testSplit(input, "\\$", std::nullopt, 1, expected);
  testSplitConstantDelim(input, "\\$", std::nullopt, 1, expected);

  // Octal string delimiter.
  testSplit(input, "\\044", std::nullopt, 1, expected);
  testSplitConstantDelim(input, "\\044", std::nullopt, 1, expected);

  input = std::vector<std::string>{
      {"I<he<she<they"}, // Simple
      {"one<<<four<"}, // Empty strings
      {"a<\xED<\xA0<123"}, // Not a well-formed UTF-8 string
      {""}, // The whole string is empty
  };

  // Single character delimiter.
  testSplit(input, "<", std::nullopt, 1, expected);
  testSplitConstantDelim(input, "<", std::nullopt, 1, expected);

  input = std::vector<std::string>{
      {"I©he©she©they"}, // Simple
      {"one©©©four©"}, // Empty strings
      {"a©\xED©\xA0©123"}, // Not a well-formed UTF-8 string
      {""}, // The whole string is empty
  };

  // Octal string delimiter that is outside the range of ASCII characters.
  testSplit(input, "\\251", std::nullopt, 1, expected);
  testSplitConstantDelim(input, "\\251", std::nullopt, 1, expected);
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
