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

using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {
class StringToMapTest : public SparkFunctionBaseTest {
 protected:
  VectorPtr evaluateStringToMap(const std::vector<StringView>& inputs) {
    const std::string expr =
        fmt::format("str_to_map(c0, '{}', '{}')", inputs[1], inputs[2]);
    return evaluate<MapVector>(
        expr, makeRowVector({makeFlatVector<StringView>({inputs[0]})}));
  }

  void testStringToMap(
      const std::vector<StringView>& inputs,
      const std::vector<std::pair<StringView, std::optional<StringView>>>&
          expect) {
    auto result = evaluateStringToMap(inputs);
    auto expectVector = makeMapVector<StringView, StringView>({expect});
    assertEqualVectors(result, expectVector);
  }
};

TEST_F(StringToMapTest, basic) {
  testStringToMap(
      {"a:1,b:2,c:3", ",", ":"}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});
  testStringToMap({"a: ,b:2", ",", ":"}, {{"a", " "}, {"b", "2"}});
  testStringToMap({"a:,b:2", ",", ":"}, {{"a", ""}, {"b", "2"}});
  testStringToMap({"", ",", ":"}, {{"", std::nullopt}});
  testStringToMap({"a", ",", ":"}, {{"a", std::nullopt}});
  testStringToMap(
      {"a=1,b=2,c=3", ",", "="}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});
  testStringToMap({"", ",", "="}, {{"", std::nullopt}});
  testStringToMap(
      {"a::1,b::2,c::3", ",", "c"},
      {{"a::1", std::nullopt}, {"b::2", std::nullopt}, {"", "::3"}});
  testStringToMap(
      {"a:1_b:2_c:3", "_", ":"}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});

  // Same delimiters.
  testStringToMap(
      {"a:1,b:2,c:3", ",", ","},
      {{"a:1", std::nullopt}, {"b:2", std::nullopt}, {"c:3", std::nullopt}});
  testStringToMap(
      {"a:1_b:2_c:3", "_", "_"},
      {{"a:1", std::nullopt}, {"b:2", std::nullopt}, {"c:3", std::nullopt}});

  // Exception for illegal delimiters.
  // Empty string is used.
  VELOX_ASSERT_THROW(
      evaluateStringToMap({"a:1,b:2", "", ":"}),
      "entryDelimiter's size should be 1.");
  VELOX_ASSERT_THROW(
      evaluateStringToMap({"a:1,b:2", ",", ""}),
      "keyValueDelimiter's size should be 1.");
  // Delimiter's length > 1.
  VELOX_ASSERT_THROW(
      evaluateStringToMap({"a:1,b:2", ";;", ":"}),
      "entryDelimiter's size should be 1.");
  VELOX_ASSERT_THROW(
      evaluateStringToMap({"a:1,b:2", ",", "::"}),
      "keyValueDelimiter's size should be 1.");
  // Unicode character is used.
  VELOX_ASSERT_THROW(
      evaluateStringToMap({"a:1,b:2", "å", ":"}),
      "entryDelimiter's size should be 1.");
  VELOX_ASSERT_THROW(
      evaluateStringToMap({"a:1,b:2", ",", "æ"}),
      "keyValueDelimiter's size should be 1.");

  // Exception for duplicated keys.
  VELOX_ASSERT_THROW(
      evaluateStringToMap({"a:1,b:2,a:3", ",", ":"}),
      "Duplicate keys are not allowed: 'a'.");
  VELOX_ASSERT_THROW(
      evaluateStringToMap({":1,:2", ",", ":"}),
      "Duplicate keys are not allowed: ''.");
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
