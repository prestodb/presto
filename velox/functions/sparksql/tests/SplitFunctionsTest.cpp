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

#include <optional>
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {

using namespace facebook::velox::test;
namespace {

class SplitTest : public SparkFunctionBaseTest {
 protected:
  void testSplitCharacter(
      const std::vector<std::optional<std::string>>& input,
      std::optional<char> pattern,
      const std::vector<std::optional<std::vector<std::string>>>& output);
};

void SplitTest::testSplitCharacter(
    const std::vector<std::optional<std::string>>& input,
    std::optional<char> pattern,
    const std::vector<std::optional<std::vector<std::string>>>& output) {
  auto valueAt = [&input](vector_size_t row) {
    return input[row] ? StringView(*input[row]) : StringView();
  };

  // Creating vectors for input strings
  auto nullAt = [&input](vector_size_t row) { return !input[row].has_value(); };

  auto result = [&] {
    auto inputString =
        makeFlatVector<StringView>(input.size(), valueAt, nullAt);
    auto rowVector = makeRowVector({inputString});

    // Evaluating the function for each input and seed
    std::string patternString =
        pattern.has_value() ? std::string(", '") + pattern.value() + "'" : "";
    std::string expressionString =
        std::string("split(c0") + patternString + ")";
    return evaluate<ArrayVector>(expressionString, rowVector);
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

TEST_F(SplitTest, TestReallocationAndCornerCases) {
  testSplitCharacter(
      {"boo:and:foo", "abcfd", "abcfd:", "", ":ab::cfd::::"},
      ':',
      {{{"boo", "and", "foo"}},
       {{"abcfd"}},
       {{"abcfd", ""}},
       {{""}},
       {{"", "ab", "", "cfd", "", "", "", ""}}});
}

TEST_F(SplitTest, Nulls) {
  testSplitCharacter(
      {std::nullopt, "abcfd", "abcfd:", std::nullopt, ":ab::cfd::::"},
      ':',
      {{std::nullopt},
       {{"abcfd"}},
       {{"abcfd", ""}},
       {{std::nullopt}},
       {{"", "ab", "", "cfd", "", "", "", ""}}});
}

TEST_F(SplitTest, DefaultArguments) {
  testSplitCharacter(
      {"boo:and:foo", "abcfd"}, ':', {{{"boo", "and", "foo"}}, {{"abcfd"}}});
}

TEST_F(SplitTest, LongStrings) {
  testSplitCharacter(
      {"abcdefghijklkmnopqrstuvwxyz"},
      ',',
      {{{"abcdefghijklkmnopqrstuvwxyz"}}});
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
