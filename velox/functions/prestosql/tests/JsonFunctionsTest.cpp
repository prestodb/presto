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
#include "velox/functions/FunctionRegistry.h"
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

  std::optional<int64_t> json_size(
      std::optional<std::string> json,
      std::optional<std::string> path) {
    return evaluateOnce<int64_t>("json_size(c0, c1)", json, path);
  }

  static std::unordered_set<std::string> getSignatureStrings(
      const std::string& functionName) {
    auto allSignatures = getFunctionSignatures();
    const auto& signatures = allSignatures.at(functionName);

    std::unordered_set<std::string> signatureStrings;
    for (const auto& signature : signatures) {
      signatureStrings.insert(signature->toString());
    }
    return signatureStrings;
  }
};

TEST_F(JsonFunctionsTest, jsonFormat) {
  const auto jsonFormat = [&](std::optional<std::string> value) {
    return evaluateOnce<std::string, std::string>(
        "json_format(c0)", {value}, {JSON()});
  };

  EXPECT_EQ(jsonFormat(std::nullopt), std::nullopt);
  EXPECT_EQ(jsonFormat(R"(true)"), "true");
  EXPECT_EQ(jsonFormat(R"(null)"), "null");
  EXPECT_EQ(jsonFormat(R"(42)"), "42");
  EXPECT_EQ(jsonFormat(R"("abc")"), R"("abc")");
  EXPECT_EQ(jsonFormat(R"([1, 2, 3])"), "[1, 2, 3]");
  EXPECT_EQ(jsonFormat(R"({"k1":"v1"})"), R"({"k1":"v1"})");

  auto data = makeRowVector({makeFlatVector<StringView>(
      {"This is a long sentence", "This is some other sentence"})});

  auto result = evaluate("json_format(c0)", data);
  auto expected = makeFlatVector<StringView>(
      {"This is a long sentence", "This is some other sentence"});
  facebook::velox::test::assertEqualVectors(expected, result);

  data = makeRowVector({makeConstant("apple", 2)});
  result = evaluate("json_format(c0)", data);
  expected = makeFlatVector<StringView>({{"apple", "apple"}});

  facebook::velox::test::assertEqualVectors(expected, result);

  data = makeRowVector(
      {makeFlatVector<bool>({true, false}),
       makeFlatVector<StringView>(
           {"This is a long sentence", "This is some other sentence"})});

  result = evaluate("if(c0, 'foo', json_format(c1))", data);
  expected = makeFlatVector<StringView>({"foo", "This is some other sentence"});
  facebook::velox::test::assertEqualVectors(expected, result);

  result = evaluate("if(c0, json_format(c1), 'bar')", data);
  expected = makeFlatVector<StringView>({"This is a long sentence", "bar"});
  facebook::velox::test::assertEqualVectors(expected, result);
}

TEST_F(JsonFunctionsTest, jsonParse) {
  const auto jsonParse = [&](std::optional<std::string> value) {
    return evaluateOnce<StringView>("json_parse(c0)", value);
  };

  const auto jsonParseWithTry = [&](std::optional<std::string> value) {
    return evaluateOnce<StringView>("try(json_parse(c0))", value);
  };

  EXPECT_EQ(jsonParse(std::nullopt), std::nullopt);
  EXPECT_EQ(jsonParse(R"(true)"), "true");
  EXPECT_EQ(jsonParse(R"(null)"), "null");
  EXPECT_EQ(jsonParse(R"(42)"), "42");
  EXPECT_EQ(jsonParse(R"("abc")"), R"("abc")");
  EXPECT_EQ(jsonParse(R"([1, 2, 3])"), "[1, 2, 3]");
  EXPECT_EQ(jsonParse(R"({"k1":"v1"})"), R"({"k1":"v1"})");
  EXPECT_EQ(jsonParse(R"(["k1", "v1"])"), R"(["k1", "v1"])");

  VELOX_ASSERT_THROW(jsonParse(R"({"k1":})"), "expected json value");
  VELOX_ASSERT_THROW(jsonParse(R"({:"k1"})"), "expected json value");
  VELOX_ASSERT_THROW(jsonParse(R"(not_json)"), "expected json value");

  EXPECT_EQ(jsonParseWithTry(R"(not_json)"), std::nullopt);
  EXPECT_EQ(jsonParseWithTry(R"({"k1":})"), std::nullopt);
  EXPECT_EQ(jsonParseWithTry(R"({:"k1"})"), std::nullopt);

  auto elementVector = makeNullableFlatVector<StringView>(
      {R"("abc")", R"(42)", R"({"k1":"v1"})", R"({"k1":})", R"({:"k1"})"});
  auto resultVector =
      evaluate("try(json_parse(c0))", makeRowVector({elementVector}));

  auto expectedVector = makeNullableFlatVector<StringView>(
      {R"("abc")", "42", R"({"k1":"v1"})", std::nullopt, std::nullopt});
  facebook::velox::test::assertEqualVectors(expectedVector, resultVector);

  auto data = makeRowVector({makeConstant(R"("k1":)", 2)});
  expectedVector =
      makeNullableFlatVector<StringView>({std::nullopt, std::nullopt});
  facebook::velox::test::assertEqualVectors(
      expectedVector, evaluate("try(json_parse(c0))", data));

  VELOX_ASSERT_THROW(
      evaluate("json_parse(c0)", data),
      "json parse error on line 0 near `:': parsing didn't consume all input");

  data = makeRowVector({makeFlatVector<StringView>(
      {R"("This is a long sentence")", R"("This is some other sentence")"})});

  auto result = evaluate("json_parse(c0)", data);
  auto expected = makeFlatVector<StringView>(
      {R"("This is a long sentence")", R"("This is some other sentence")"});
  facebook::velox::test::assertEqualVectors(expected, result);

  data = makeRowVector({makeConstant(R"("apple")", 2)});
  result = evaluate("json_parse(c0)", data);
  expected = makeFlatVector<StringView>({{R"("apple")", R"("apple")"}});

  facebook::velox::test::assertEqualVectors(expected, result);

  data = makeRowVector(
      {makeFlatVector<bool>({true, false}),
       makeFlatVector<StringView>(
           {R"("This is a long sentence")",
            R"("This is some other sentence")"})});

  result = evaluate("if(c0, json_parse(c1), json_parse(c1))", data);
  expected = makeFlatVector<StringView>(
      {R"("This is a long sentence")", R"("This is some other sentence")"});
  facebook::velox::test::assertEqualVectors(expected, result);
}

TEST_F(JsonFunctionsTest, isJsonScalarSignatures) {
  auto signatures = getSignatureStrings("is_json_scalar");
  ASSERT_EQ(1, signatures.size());

  ASSERT_EQ(1, signatures.count("(json) -> boolean"));
}

TEST_F(JsonFunctionsTest, jsonArrayLengthSignatures) {
  auto signatures = getSignatureStrings("json_array_length");
  ASSERT_EQ(1, signatures.size());

  ASSERT_EQ(1, signatures.count("(json) -> bigint"));
}

TEST_F(JsonFunctionsTest, jsonExtractScalarSignatures) {
  auto signatures = getSignatureStrings("json_extract_scalar");
  ASSERT_EQ(1, signatures.size());

  ASSERT_EQ(1, signatures.count("(json,varchar) -> varchar"));
}

TEST_F(JsonFunctionsTest, jsonArrayContainsSignatures) {
  auto signatures = getSignatureStrings("json_array_contains");
  ASSERT_EQ(4, signatures.size());

  ASSERT_EQ(1, signatures.count("(json,varchar) -> boolean"));
  ASSERT_EQ(1, signatures.count("(json,bigint) -> boolean"));
  ASSERT_EQ(1, signatures.count("(json,double) -> boolean"));
  ASSERT_EQ(1, signatures.count("(json,boolean) -> boolean"));
}

TEST_F(JsonFunctionsTest, jsonSizeSignatures) {
  auto signatures = getSignatureStrings("json_size");
  ASSERT_EQ(1, signatures.size());

  ASSERT_EQ(1, signatures.count("(json,varchar) -> bigint"));
}

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

TEST_F(JsonFunctionsTest, jsonSize) {
  EXPECT_EQ(json_size(R"({"k1":{"k2": 999}, "k3": 1})", "$.k1.k2"), 0);
  EXPECT_EQ(json_size(R"({"k1":{"k2": 999}, "k3": 1})", "$.k1"), 1);
  EXPECT_EQ(json_size(R"({"k1":{"k2": 999}, "k3": 1})", "$"), 2);
  EXPECT_EQ(json_size(R"({"k1":{"k2": 999}, "k3": 1})", "$.k3"), 0);
  EXPECT_EQ(json_size(R"({"k1":{"k2": 999}, "k3": [1, 2, 3, 4]})", "$.k3"), 4);
  EXPECT_EQ(json_size(R"({"k1":{"k2": 999}, "k3": 1})", "$.k4"), std::nullopt);
  EXPECT_EQ(json_size(R"({"k1":{"k2": 999}, "k3"})", "$.k4"), std::nullopt);
  EXPECT_EQ(json_size(R"({"k1":{"k2": 999}, "k3": true})", "$.k3"), 0);
  EXPECT_EQ(json_size(R"({"k1":{"k2": 999}, "k3": null})", "$.k3"), 0);
  EXPECT_EQ(
      json_size(
          R"({"k1":{"k2": 999, "k3": [{"k4": [1, 2, 3]}]}})", "$.k1.k3[0].k4"),
      3);
}

TEST_F(JsonFunctionsTest, invalidPath) {
  EXPECT_THROW(json_size(R"([0,1,2])", ""), VeloxUserError);
  EXPECT_THROW(json_size(R"([0,1,2])", "$[]"), VeloxUserError);
  EXPECT_THROW(json_size(R"([0,1,2])", "$[-1]"), VeloxUserError);
  EXPECT_THROW(json_size(R"({"k1":"v1"})", "$k1"), VeloxUserError);
  EXPECT_THROW(json_size(R"({"k1":"v1"})", "$.k1."), VeloxUserError);
  EXPECT_THROW(json_size(R"({"k1":"v1"})", "$.k1]"), VeloxUserError);
  EXPECT_THROW(json_size(R"({"k1":"v1)", "$.k1]"), VeloxUserError);
}

} // namespace

} // namespace facebook::velox::functions::prestosql
