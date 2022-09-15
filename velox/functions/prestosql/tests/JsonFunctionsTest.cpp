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

#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/JsonType.h"

namespace facebook::velox::functions::prestosql {

namespace {

class JsonFunctionsTest : public functions::test::FunctionBaseTest {
 public:
  std::optional<bool> is_json_scalar(std::optional<std::string> json) {
    return evaluateOnce<bool>("is_json_scalar(c0)", json);
  }

  std::optional<int64_t> json_array_length(std::optional<std::string> json) {
    return evaluateOnce<int64_t>("json_array_length(c0)", json);
  }

  template <typename T>
  std::optional<bool> json_array_contains(
      std::optional<std::string> json,
      std::optional<T> value) {
    return evaluateOnce<bool>("json_array_contains(c0, c1)", json, value);
  }
};

TEST_F(JsonFunctionsTest, isJsonScalar) {
  // Scalars.
  EXPECT_EQ(is_json_scalar(R"(1)"), true);
  EXPECT_EQ(is_json_scalar(R"(123456)"), true);
  EXPECT_EQ(is_json_scalar(R"("hello")"), true);
  EXPECT_EQ(is_json_scalar(R"("thefoxjumpedoverthefence")"), true);
  EXPECT_EQ(is_json_scalar(R"(1.1)"), true);
  EXPECT_EQ(is_json_scalar(R"("")"), true);
  EXPECT_EQ(is_json_scalar(R"(true)"), true);

  // Lists and maps
  EXPECT_EQ(is_json_scalar(R"([1,2])"), false);
  EXPECT_EQ(is_json_scalar(R"({"k1":"v1"})"), false);
  EXPECT_EQ(is_json_scalar(R"({"k1":[0,1,2]})"), false);
  EXPECT_EQ(is_json_scalar(R"({"k1":""})"), false);
}

TEST_F(JsonFunctionsTest, jsonArrayLength) {
  EXPECT_EQ(json_array_length(R"([])"), 0);
  EXPECT_EQ(json_array_length(R"([1])"), 1);
  EXPECT_EQ(json_array_length(R"([1, 2, 3])"), 3);
  EXPECT_EQ(
      json_array_length(
          R"([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20])"),
      20);

  EXPECT_EQ(json_array_length(R"(1)"), std::nullopt);
  EXPECT_EQ(json_array_length(R"("hello")"), std::nullopt);
  EXPECT_EQ(json_array_length(R"("")"), std::nullopt);
  EXPECT_EQ(json_array_length(R"(true)"), std::nullopt);
  EXPECT_EQ(json_array_length(R"({"k1":"v1"})"), std::nullopt);
  EXPECT_EQ(json_array_length(R"({"k1":[0,1,2]})"), std::nullopt);
  EXPECT_EQ(json_array_length(R"({"k1":[0,1,2], "k2":"v1"})"), std::nullopt);
}

TEST_F(JsonFunctionsTest, jsonArrayContainsBool) {
  EXPECT_EQ(json_array_contains<bool>(R"([])", true), false);
  EXPECT_EQ(json_array_contains<bool>(R"([1, 2, 3])", false), false);
  EXPECT_EQ(json_array_contains<bool>(R"([1.2, 2.3, 3.4])", true), false);
  EXPECT_EQ(
      json_array_contains<bool>(R"(["hello", "presto", "world"])", false),
      false);
  EXPECT_EQ(json_array_contains<bool>(R"(1)", true), std::nullopt);
  EXPECT_EQ(
      json_array_contains<bool>(R"("thefoxjumpedoverthefence")", false),
      std::nullopt);
  EXPECT_EQ(json_array_contains<bool>(R"("")", false), std::nullopt);
  EXPECT_EQ(json_array_contains<bool>(R"(true)", true), std::nullopt);
  EXPECT_EQ(
      json_array_contains<bool>(R"({"k1":[0,1,2], "k2":"v1"})", true),
      std::nullopt);

  EXPECT_EQ(json_array_contains<bool>(R"([true, false])", true), true);
  EXPECT_EQ(json_array_contains<bool>(R"([true, true])", false), false);
  EXPECT_EQ(
      json_array_contains<bool>(R"([123, 123.456, true, "abc"])", true), true);
  EXPECT_EQ(
      json_array_contains<bool>(R"([123, 123.456, true, "abc"])", false),
      false);
  EXPECT_EQ(
      json_array_contains<bool>(
          R"([false, false, false, false, false, false, false,
false, false, false, false, false, false, true, false, false, false, false])",
          true),
      true);
  EXPECT_EQ(
      json_array_contains<bool>(
          R"([true, true, true, true, true, true, true,
true, true, true, true, true, true, true, true, true, true, true])",
          false),
      false);
}

TEST_F(JsonFunctionsTest, jsonArrayContainsInt) {
  EXPECT_EQ(json_array_contains<int64_t>(R"([])", 0), false);
  EXPECT_EQ(json_array_contains<int64_t>(R"([1.2, 2.3, 3.4])", 2), false);
  EXPECT_EQ(json_array_contains<int64_t>(R"([1.2, 2.0, 3.4])", 2), false);
  EXPECT_EQ(
      json_array_contains<int64_t>(R"(["hello", "presto", "world"])", 2),
      false);
  EXPECT_EQ(
      json_array_contains<int64_t>(R"([false, false, false])", 17), false);
  EXPECT_EQ(json_array_contains<int64_t>(R"(1)", 1), std::nullopt);
  EXPECT_EQ(
      json_array_contains<int64_t>(R"("thefoxjumpedoverthefence")", 1),
      std::nullopt);
  EXPECT_EQ(json_array_contains<int64_t>(R"("")", 1), std::nullopt);
  EXPECT_EQ(json_array_contains<int64_t>(R"(true)", 1), std::nullopt);
  EXPECT_EQ(
      json_array_contains<int64_t>(R"({"k1":[0,1,2], "k2":"v1"})", 1),
      std::nullopt);

  EXPECT_EQ(json_array_contains<int64_t>(R"([1, 2, 3])", 1), true);
  EXPECT_EQ(json_array_contains<int64_t>(R"([1, 2, 3])", 4), false);
  EXPECT_EQ(
      json_array_contains<int64_t>(R"([123, 123.456, true, "abc"])", 123),
      true);
  EXPECT_EQ(
      json_array_contains<int64_t>(R"([123, 123.456, true, "abc"])", 456),
      false);
  EXPECT_EQ(
      json_array_contains<int64_t>(
          R"([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20])",
          17),
      true);
  EXPECT_EQ(
      json_array_contains<int64_t>(
          R"([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20])",
          23),
      false);
}

TEST_F(JsonFunctionsTest, jsonArrayContainsDouble) {
  EXPECT_EQ(json_array_contains<double>(R"([])", 2.3), false);
  EXPECT_EQ(json_array_contains<double>(R"([1, 2, 3])", 2.3), false);
  EXPECT_EQ(json_array_contains<double>(R"([1, 2, 3])", 2.0), false);
  EXPECT_EQ(
      json_array_contains<double>(R"(["hello", "presto", "world"])", 2.3),
      false);
  EXPECT_EQ(
      json_array_contains<double>(R"([false, false, false])", 2.3), false);
  EXPECT_EQ(json_array_contains<double>(R"(1)", 2.3), std::nullopt);
  EXPECT_EQ(
      json_array_contains<double>(R"("thefoxjumpedoverthefence")", 2.3),
      std::nullopt);
  EXPECT_EQ(json_array_contains<double>(R"("")", 2.3), std::nullopt);
  EXPECT_EQ(json_array_contains<double>(R"(true)", 2.3), std::nullopt);
  EXPECT_EQ(
      json_array_contains<double>(R"({"k1":[0,1,2], "k2":"v1"})", 2.3),
      std::nullopt);

  EXPECT_EQ(json_array_contains<double>(R"([1.2, 2.3, 3.4])", 2.3), true);
  EXPECT_EQ(json_array_contains<double>(R"([1.2, 2.3, 3.4])", 2.4), false);
  EXPECT_EQ(
      json_array_contains<double>(R"([123, 123.456, true, "abc"])", 123.456),
      true);
  EXPECT_EQ(
      json_array_contains<double>(R"([123, 123.456, true, "abc"])", 456.789),
      false);
  EXPECT_EQ(
      json_array_contains<double>(
          R"([1.2, 2.3, 3.4, 4.5, 1.2, 2.3, 3.4, 4.5, 1.2, 2.3, 3.4, 4.5, 1.2, 2.3, 3.4, 4.5, 1.2, 2.3, 3.4, 4.5])",
          4.5),
      true);
  EXPECT_EQ(
      json_array_contains<double>(
          R"([1.2, 2.3, 3.4, 4.5, 1.2, 2.3, 3.4, 4.5, 1.2, 2.3, 3.4, 4.5, 1.2, 2.3, 3.4, 4.5, 1.2, 2.3, 3.4, 4.5])",
          4.3),
      false);
}

TEST_F(JsonFunctionsTest, jsonArrayContainsString) {
  EXPECT_EQ(json_array_contains<std::string>(R"([])", ""), false);
  EXPECT_EQ(json_array_contains<std::string>(R"([1, 2, 3])", "1"), false);
  EXPECT_EQ(
      json_array_contains<std::string>(R"([1.2, 2.3, 3.4])", "2.3"), false);
  EXPECT_EQ(
      json_array_contains<std::string>(R"([true, false])", R"("true")"), false);
  EXPECT_EQ(json_array_contains<std::string>(R"(1)", "1"), std::nullopt);
  EXPECT_EQ(
      json_array_contains<std::string>(R"("thefoxjumpedoverthefence")", "1"),
      std::nullopt);
  EXPECT_EQ(json_array_contains<std::string>(R"("")", "1"), std::nullopt);
  EXPECT_EQ(json_array_contains<std::string>(R"(true)", "1"), std::nullopt);
  EXPECT_EQ(
      json_array_contains<std::string>(R"({"k1":[0,1,2], "k2":"v1"})", "1"),
      std::nullopt);

  EXPECT_EQ(
      json_array_contains<std::string>(
          R"(["hello", "presto", "world"])", "presto"),
      true);
  EXPECT_EQ(
      json_array_contains<std::string>(
          R"(["hello", "presto", "world"])", "nation"),
      false);
  EXPECT_EQ(
      json_array_contains<std::string>(R"([123, 123.456, true, "abc"])", "abc"),
      true);
  EXPECT_EQ(
      json_array_contains<std::string>(R"([123, 123.456, true, "abc"])", "def"),
      false);
  EXPECT_EQ(
      json_array_contains<std::string>(
          R"(["hello", "presto", "world", "hello", "presto", "world", "hello", "presto", "world", "hello",
"presto", "world", "hello", "presto", "world", "hello", "presto", "world"])",
          "hello"),
      true);
  EXPECT_EQ(
      json_array_contains<std::string>(
          R"(["hello", "presto", "world", "hello", "presto", "world", "hello", "presto", "world", "hello",
"presto", "world", "hello", "presto", "world", "hello", "presto", "world"])",
          "hola"),
      false);
  EXPECT_EQ(
      json_array_contains<std::string>(
          R"(["hello", "presto", "world", 1, 2, 3, true, false, 1.2, 2.3, {"k1":[0,1,2], "k2":"v1"}])",
          "world"),
      true);
  EXPECT_EQ(
      json_array_contains<std::string>(
          R"(["the fox jumped over the fence", "hello presto world"])",
          "hello velox world"),
      false);
  EXPECT_EQ(
      json_array_contains<std::string>(
          R"(["the fox jumped over the fence", "hello presto world"])",
          "the fox jumped over the fence"),
      true);
}

} // namespace

} // namespace facebook::velox::functions::prestosql
