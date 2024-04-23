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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/JsonType.h"

namespace facebook::velox::functions::prestosql {

namespace {

class JsonExtractScalarTest : public functions::test::FunctionBaseTest {
 protected:
  VectorPtr makeVector(std::optional<std::string> json, const TypePtr& type) {
    std::optional<StringView> s = json.has_value()
        ? std::make_optional(StringView(json.value()))
        : std::nullopt;
    return makeNullableFlatVector<StringView>({s}, type);
  }

  std::optional<std::string> jsonExtractScalar(
      std::optional<std::string> json,
      std::optional<std::string> path) {
    auto jsonVector = makeVector(json, JSON());
    auto pathVector = makeNullableFlatVector<std::string>({path});
    auto jsonResult = evaluateOnce<std::string>(
        "json_extract_scalar(c0, c1)", makeRowVector({jsonVector, pathVector}));

    auto varcharVector = makeVector(json, VARCHAR());
    auto varcharResult = evaluateOnce<std::string>(
        "json_extract_scalar(c0, c1)",
        makeRowVector({varcharVector, pathVector}));

    EXPECT_EQ(jsonResult, varcharResult);
    return jsonResult;
  }

  void assertResults(
      const std::vector<std::optional<StringView>>& json,
      const std::vector<std::optional<StringView>>& path,
      const std::vector<std::optional<StringView>>& expected) {
    auto jsonVector = makeNullableFlatVector<StringView>(json, JSON());
    auto pathVector = makeNullableFlatVector<StringView>(path);
    auto expectedVector = makeNullableFlatVector<StringView>(expected);

    velox::test::assertEqualVectors(
        expectedVector,
        evaluate<SimpleVector<StringView>>(
            "json_extract_scalar(c0, c1)",
            makeRowVector({jsonVector, pathVector})));

    auto varcharVector = makeNullableFlatVector<StringView>(json, VARCHAR());

    velox::test::assertEqualVectors(
        expectedVector,
        evaluate<SimpleVector<StringView>>(
            "json_extract_scalar(c0, c1)",
            makeRowVector({varcharVector, pathVector})));
  }
};

TEST_F(JsonExtractScalarTest, simple) {
  // Scalars.
  EXPECT_EQ(jsonExtractScalar(R"(1)", "$"), "1");
  EXPECT_EQ(jsonExtractScalar(R"(123456)", "$"), "123456");
  EXPECT_EQ(jsonExtractScalar(R"("hello")", "$"), "hello");
  EXPECT_EQ(jsonExtractScalar(R"(1.1)", "$"), "1.1");
  EXPECT_EQ(jsonExtractScalar(R"("")", "$"), "");
  EXPECT_EQ(jsonExtractScalar(R"(true)", "$"), "true");

  // Simple lists.
  EXPECT_EQ(jsonExtractScalar(R"([1,2])", "$[0]"), "1");
  EXPECT_EQ(jsonExtractScalar(R"([1,2])", "$[1]"), "2");
  EXPECT_EQ(jsonExtractScalar(R"([1,2])", "$[2]"), std::nullopt);
  EXPECT_EQ(jsonExtractScalar(R"([1,2])", "$[999]"), std::nullopt);

  // Simple maps.
  EXPECT_EQ(jsonExtractScalar(R"({"k1":"v1"})", "$.k1"), "v1");
  EXPECT_EQ(jsonExtractScalar(R"({"k1":"v1"})", "$.k2"), std::nullopt);
  EXPECT_EQ(jsonExtractScalar(R"({"k1":"v1"})", "$.k1.k3"), std::nullopt);
  EXPECT_EQ(jsonExtractScalar(R"({"k1":[0,1,2]})", "$.k1"), std::nullopt);
  EXPECT_EQ(jsonExtractScalar(R"({"k1":""})", "$.k1"), "");

  // Nested
  EXPECT_EQ(jsonExtractScalar(R"({"k1":{"k2": 999}})", "$.k1.k2"), "999");
  EXPECT_EQ(jsonExtractScalar(R"({"k1":[1,2,3]})", "$.k1[0]"), "1");
  EXPECT_EQ(jsonExtractScalar(R"({"k1":[1,2,3]})", "$.k1[2]"), "3");
  EXPECT_EQ(jsonExtractScalar(R"([{"k1":"v1"}, 2])", "$[0].k1"), "v1");
  EXPECT_EQ(jsonExtractScalar(R"([{"k1":"v1"}, 2])", "$[1]"), "2");
  EXPECT_EQ(
      jsonExtractScalar(
          R"([{"k1":[{"k2": ["v1", "v2"]}]}])", "$[0].k1[0].k2[1]"),
      "v2");

  // Paths without leading '$'.
  EXPECT_EQ(jsonExtractScalar(R"({"k1":"v1"})", "k1"), "v1");
  EXPECT_EQ(jsonExtractScalar(R"({"k1":{"k2": 999}})", "k1.k2"), "999");

  // Paths with redundant '.'s.
  auto json = "[1, 2, [10, 20, [100, 200, 300]]]";
  EXPECT_EQ(jsonExtractScalar(json, "$[2][2][2]"), "300");
  EXPECT_EQ(jsonExtractScalar(json, "$.[2].[2].[2]"), "300");
  EXPECT_EQ(jsonExtractScalar(json, "$[2].[2].[2]"), "300");
  EXPECT_EQ(jsonExtractScalar(json, "$[2][2].[2]"), "300");
}

TEST_F(JsonExtractScalarTest, jsonType) {
  // Scalars.
  assertResults(
      {R"(1)"_sv,
       R"(123456)"_sv,
       R"("hello")"_sv,
       R"(1.1)"_sv,
       R"("")"_sv,
       R"(true)"},
      {"$"_sv, "$"_sv, "$"_sv, "$"_sv, "$"_sv, "$"_sv},
      {"1"_sv, "123456"_sv, "hello"_sv, "1.1"_sv, ""_sv, "true"_sv});

  // Simple lists.
  assertResults(
      {R"([1,2])"_sv, R"([1,2])"_sv, R"([1,2])"_sv, R"([1,2])"_sv},
      {"$[0]"_sv, "$[1]"_sv, "$[2]"_sv, "$[999]"_sv},
      {"1"_sv, "2"_sv, std::nullopt, std::nullopt});

  // Simple maps.
  assertResults(
      {R"({"k1":"v1"})"_sv,
       R"({"k1":"v1"})"_sv,
       R"({"k1":"v1"})"_sv,
       R"({"k1":[0,1,2]})"_sv,
       R"({"k1":""})"_sv},
      {"$.k1"_sv, "$.k2"_sv, "$.k1.k3"_sv, "$.k1"_sv, "$.k1"_sv},
      {"v1"_sv, std::nullopt, std::nullopt, std::nullopt, ""_sv});

  // Nested
  assertResults(
      {R"({"k1":{"k2": 999}})"_sv,
       R"({"k1":[1,2,3]})"_sv,
       R"({"k1":[1,2,3]})"_sv,
       R"([{"k1":"v1"}, 2])"_sv,
       R"([{"k1":"v1"}, 2])"_sv,
       R"([{"k1":[{"k2": ["v1", "v2"]}]}])"_sv},
      {"$.k1.k2"_sv,
       "$.k1[0]"_sv,
       "$.k1[2]"_sv,
       "$[0].k1"_sv,
       "$[1]"_sv,
       "$[0].k1[0].k2[1]"_sv},
      {"999"_sv, "1"_sv, "3"_sv, "v1"_sv, "2"_sv, "v2"_sv});
}

TEST_F(JsonExtractScalarTest, utf8) {
  EXPECT_EQ(
      jsonExtractScalar(R"({"k1":"I \u2665 UTF-8"})", "$.k1"),
      "I \u2665 UTF-8");
  EXPECT_EQ(
      jsonExtractScalar("{\"k1\":\"I \u2665 UTF-8\"}", "$.k1"),
      "I \u2665 UTF-8");

  EXPECT_EQ(
      jsonExtractScalar("{\"k1\":\"I \U0001D11E playing in G-clef\"}", "$.k1"),
      "I \U0001D11E playing in G-clef");
}

TEST_F(JsonExtractScalarTest, invalidPath) {
  VELOX_ASSERT_THROW(jsonExtractScalar(R"([0,1,2])", ""), "Invalid JSON path");
  VELOX_ASSERT_THROW(
      jsonExtractScalar(R"([0,1,2])", "$[]"), "Invalid JSON path");
  VELOX_ASSERT_THROW(
      jsonExtractScalar(R"([0,1,2])", "$[-1]"), "Invalid JSON path");
  VELOX_ASSERT_THROW(
      jsonExtractScalar(R"({"k1":"v1"})", "$k1"), "Invalid JSON path");
  VELOX_ASSERT_THROW(
      jsonExtractScalar(R"({"k1":"v1"})", "$.k1."), "Invalid JSON path");
  VELOX_ASSERT_THROW(
      jsonExtractScalar(R"({"k1":"v1"})", "$.k1]"), "Invalid JSON path");
  VELOX_ASSERT_THROW(
      jsonExtractScalar(R"({"k1":"v1)", "$.k1]"), "Invalid JSON path");

  // Paths without leading '$'.
  VELOX_ASSERT_THROW(jsonExtractScalar(R"([1,2])", "[0]"), "Invalid JSON path");
  VELOX_ASSERT_THROW(
      jsonExtractScalar(R"([1,2])", ".[0]"), "Invalid JSON path");
  VELOX_ASSERT_THROW(
      jsonExtractScalar(R"({"k1":"v1"})", ".k1"), "Invalid JSON path");
  VELOX_ASSERT_THROW(
      jsonExtractScalar(R"({"k1":{"k2": 999}})", ".k1.k2"),
      "Invalid JSON path");
}

// simdjson, like Presto java, returns the large number as-is as a string,
// without trying to convert it to an integer.
TEST_F(JsonExtractScalarTest, overflow) {
  EXPECT_EQ(
      jsonExtractScalar(
          R"(184467440737095516151844674407370955161518446744073709551615)",
          "$"),
      "184467440737095516151844674407370955161518446744073709551615");
}

// TODO: When there is a wildcard in the json path, Presto's behavior is to
// always extract an array of selected items, and hence json_extract_scalar()
// always return NULL in this situation. But some internal customers are
// relying on the current behavior of returning the selected item itself if
// there is exactly one selected. We'll fix json_extract_scalar() to follow
// Presto's behavior after internal customers clear the dependence on this
// diverged semantic. This unit test makes sure we don't break their workloads
// before they clear the dependency.
TEST_F(JsonExtractScalarTest, wildcardSelect) {
  EXPECT_EQ(
      jsonExtractScalar(R"({"tags":{"a":["b"],"c":["d"]}})", "$.tags.c[*]"),
      "d");
  EXPECT_EQ(
      jsonExtractScalar(R"({"tags":{"a":["b"],"c":["d"]}})", "$[tags][c][*]"),
      "d");
  EXPECT_EQ(
      jsonExtractScalar(R"({"tags":{"a":["b"],"c":["d","e"]}})", "$.tags.c[*]"),
      std::nullopt);
  EXPECT_EQ(
      jsonExtractScalar(R"({"tags":{"a":["b"],"c":[]}})", "$.tags.c[*]"),
      std::nullopt);
}

} // namespace

} // namespace facebook::velox::functions::prestosql
