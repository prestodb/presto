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

const std::string kJson = R"(
    {
        "store": {
            "book": [
                {
                    "category": "reference",
                    "author": "Nigel Rees",
                    "title": "Sayings of the Century",
                    "price": 8.95
                },
                {
                    "category": "fiction",
                    "author": "Evelyn Waugh",
                    "title": "Sword of Honour",
                    "price": 12.99
                },
                {
                    "category": "fiction",
                    "author": "Herman Melville",
                    "title": "Moby Dick",
                    "isbn": "0-553-21311-3",
                    "price": 8.99
                },
                {
                    "category": "fiction",
                    "author": "J. R. R. Tolkien",
                    "title": "The Lord of the Rings",
                    "isbn": "0-395-19395-8",
                    "price": 22.99
                }
            ],
            "bicycle": {
                "color": "red",
                "price": 19.95
            }
        },
        "expensive": 10
    }
    )";

class JsonFunctionsTest : public functions::test::FunctionBaseTest {
 protected:
  VectorPtr makeJsonVector(std::optional<std::string> json) {
    std::optional<StringView> s = json.has_value()
        ? std::make_optional(StringView(json.value()))
        : std::nullopt;
    return makeNullableFlatVector<StringView>({s}, JSON());
  }

  std::pair<VectorPtr, VectorPtr> makeVectors(std::optional<std::string> json) {
    std::optional<StringView> s = json.has_value()
        ? std::make_optional(StringView(json.value()))
        : std::nullopt;
    return {
        makeNullableFlatVector<StringView>({s}, JSON()),
        makeNullableFlatVector<StringView>({s}, VARCHAR())};
  }

  std::optional<bool> isJsonScalar(std::optional<std::string> json) {
    auto [jsonVector, varcharVector] = makeVectors(json);
    auto jsonResult =
        evaluateOnce<bool>("is_json_scalar(c0)", makeRowVector({jsonVector}));
    auto varcharResult = evaluateOnce<bool>(
        "is_json_scalar(c0)", makeRowVector({varcharVector}));

    EXPECT_EQ(jsonResult, varcharResult);
    return jsonResult;
  }

  std::optional<int64_t> jsonArrayLength(std::optional<std::string> json) {
    auto [jsonVector, varcharVector] = makeVectors(json);
    auto jsonResult = evaluateOnce<int64_t>(
        "json_array_length(c0)", makeRowVector({jsonVector}));
    auto varcharResult = evaluateOnce<int64_t>(
        "json_array_length(c0)", makeRowVector({varcharVector}));

    EXPECT_EQ(jsonResult, varcharResult);
    return jsonResult;
  }

  template <typename T>
  std::optional<bool> jsonArrayContains(
      std::optional<std::string> json,
      std::optional<T> value) {
    auto [jsonVector, varcharVector] = makeVectors(json);
    auto valueVector = makeNullableFlatVector<T>({value});

    auto jsonResult = evaluateOnce<bool>(
        "json_array_contains(c0, c1)",
        makeRowVector({jsonVector, valueVector}));
    auto varcharResult = evaluateOnce<bool>(
        "json_array_contains(c0, c1)",
        makeRowVector({varcharVector, valueVector}));

    EXPECT_EQ(jsonResult, varcharResult);
    return jsonResult;
  }

  std::optional<int64_t> jsonSize(
      std::optional<std::string> json,
      const std::string& path) {
    auto [jsonVector, varcharVector] = makeVectors(json);
    auto pathVector = makeFlatVector<std::string>({path});

    auto jsonResult = evaluateOnce<int64_t>(
        "json_size(c0, c1)", makeRowVector({jsonVector, pathVector}));
    auto varcharResult = evaluateOnce<int64_t>(
        "json_size(c0, c1)", makeRowVector({varcharVector, pathVector}));

    EXPECT_EQ(jsonResult, varcharResult);
    return jsonResult;
  }
};

TEST_F(JsonFunctionsTest, jsonFormat) {
  const auto jsonFormat = [&](std::optional<std::string> value) {
    return evaluateOnce<std::string>(
        "json_format(c0)", makeRowVector({makeJsonVector(value)}));
  };

  EXPECT_EQ(jsonFormat(std::nullopt), std::nullopt);
  EXPECT_EQ(jsonFormat(R"(true)"), "true");
  EXPECT_EQ(jsonFormat(R"(null)"), "null");
  EXPECT_EQ(jsonFormat(R"(42)"), "42");
  EXPECT_EQ(jsonFormat(R"("abc")"), R"("abc")");
  EXPECT_EQ(jsonFormat(R"([1, 2, 3])"), "[1, 2, 3]");
  EXPECT_EQ(jsonFormat(R"({"k1":"v1"})"), R"({"k1":"v1"})");

  auto data = makeRowVector({makeFlatVector<StringView>(
      {"This is a long sentence", "This is some other sentence"}, JSON())});

  auto result = evaluate("json_format(c0)", data);
  auto expected = makeFlatVector<StringView>(
      {"This is a long sentence", "This is some other sentence"});
  velox::test::assertEqualVectors(expected, result);

  data = makeRowVector({makeConstant("apple", 2, JSON())});
  result = evaluate("json_format(c0)", data);
  expected = makeFlatVector<StringView>({{"apple", "apple"}});

  velox::test::assertEqualVectors(expected, result);

  data = makeRowVector(
      {makeFlatVector<bool>({true, false}),
       makeFlatVector<StringView>(
           {"This is a long sentence", "This is some other sentence"},
           JSON())});

  result = evaluate("if(c0, 'foo', json_format(c1))", data);
  expected = makeFlatVector<StringView>({"foo", "This is some other sentence"});
  velox::test::assertEqualVectors(expected, result);

  result = evaluate("if(c0, json_format(c1), 'bar')", data);
  expected = makeFlatVector<StringView>({"This is a long sentence", "bar"});
  velox::test::assertEqualVectors(expected, result);
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

  VELOX_ASSERT_THROW(
      jsonParse(R"({"k1":})"), "The JSON document has an improper structure");
  VELOX_ASSERT_THROW(
      jsonParse(R"({:"k1"})"), "The JSON document has an improper structure");
  VELOX_ASSERT_THROW(jsonParse(R"(not_json)"), "Problem while parsing an atom");
  VELOX_ASSERT_THROW(
      jsonParse("[1"),
      "JSON document ended early in the middle of an object or array");
  VELOX_ASSERT_THROW(jsonParse(""), "no JSON found");

  EXPECT_EQ(jsonParseWithTry(R"(not_json)"), std::nullopt);
  EXPECT_EQ(jsonParseWithTry(R"({"k1":})"), std::nullopt);
  EXPECT_EQ(jsonParseWithTry(R"({:"k1"})"), std::nullopt);

  auto elementVector = makeNullableFlatVector<StringView>(
      {R"("abc")", R"(42)", R"({"k1":"v1"})", R"({"k1":})", R"({:"k1"})"});
  auto resultVector =
      evaluate("try(json_parse(c0))", makeRowVector({elementVector}));

  auto expectedVector = makeNullableFlatVector<StringView>(
      {R"("abc")", "42", R"({"k1":"v1"})", std::nullopt, std::nullopt}, JSON());
  velox::test::assertEqualVectors(expectedVector, resultVector);

  auto data = makeRowVector({makeConstant(R"("k1":)", 2)});
  expectedVector =
      makeNullableFlatVector<StringView>({std::nullopt, std::nullopt}, JSON());
  velox::test::assertEqualVectors(
      expectedVector, evaluate("try(json_parse(c0))", data));

  VELOX_ASSERT_THROW(
      evaluate("json_parse(c0)", data),
      "Unexpected trailing content in the JSON input");

  data = makeRowVector({makeFlatVector<StringView>(
      {R"("This is a long sentence")", R"("This is some other sentence")"})});

  auto result = evaluate("json_parse(c0)", data);
  auto expected = makeFlatVector<StringView>(
      {R"("This is a long sentence")", R"("This is some other sentence")"},
      JSON());
  velox::test::assertEqualVectors(expected, result);

  data = makeRowVector({makeConstant(R"("apple")", 2)});
  result = evaluate("json_parse(c0)", data);
  expected = makeFlatVector<StringView>({{R"("apple")", R"("apple")"}}, JSON());

  velox::test::assertEqualVectors(expected, result);

  data = makeRowVector({makeFlatVector<StringView>({"233897314173811950000"})});
  result = evaluate("json_parse(c0)", data);
  expected = makeFlatVector<StringView>({{"233897314173811950000"}}, JSON());
  velox::test::assertEqualVectors(expected, result);

  data =
      makeRowVector({makeFlatVector<StringView>({"[233897314173811950000]"})});
  result = evaluate("json_parse(c0)", data);
  expected = makeFlatVector<StringView>({{"[233897314173811950000]"}}, JSON());
  velox::test::assertEqualVectors(expected, result);

  data = makeRowVector(
      {makeFlatVector<bool>({true, false}),
       makeFlatVector<StringView>(
           {R"("This is a long sentence")",
            R"("This is some other sentence")"})});

  result = evaluate("if(c0, json_parse(c1), json_parse(c1))", data);
  expected = makeFlatVector<StringView>(
      {R"("This is a long sentence")", R"("This is some other sentence")"},
      JSON());
  velox::test::assertEqualVectors(expected, result);

  try {
    jsonParse(R"({"k1":})");
    FAIL() << "Error expected";
  } catch (const VeloxUserError& e) {
    ASSERT_EQ(e.context(), "json_parse(c0)");
  }
}

TEST_F(JsonFunctionsTest, isJsonScalarSignatures) {
  auto signatures = getSignatureStrings("is_json_scalar");
  ASSERT_EQ(2, signatures.size());

  ASSERT_EQ(1, signatures.count("(json) -> boolean"));
  ASSERT_EQ(1, signatures.count("(varchar) -> boolean"));
}

TEST_F(JsonFunctionsTest, jsonArrayLengthSignatures) {
  auto signatures = getSignatureStrings("json_array_length");
  ASSERT_EQ(2, signatures.size());

  ASSERT_EQ(1, signatures.count("(json) -> bigint"));
  ASSERT_EQ(1, signatures.count("(varchar) -> bigint"));
}

TEST_F(JsonFunctionsTest, jsonExtractScalarSignatures) {
  auto signatures = getSignatureStrings("json_extract_scalar");
  ASSERT_EQ(2, signatures.size());

  ASSERT_EQ(1, signatures.count("(json,varchar) -> varchar"));
  ASSERT_EQ(1, signatures.count("(varchar,varchar) -> varchar"));
}

TEST_F(JsonFunctionsTest, jsonArrayContainsSignatures) {
  auto signatures = getSignatureStrings("json_array_contains");
  ASSERT_EQ(8, signatures.size());

  ASSERT_EQ(1, signatures.count("(json,varchar) -> boolean"));
  ASSERT_EQ(1, signatures.count("(json,bigint) -> boolean"));
  ASSERT_EQ(1, signatures.count("(json,double) -> boolean"));
  ASSERT_EQ(1, signatures.count("(json,boolean) -> boolean"));

  ASSERT_EQ(1, signatures.count("(varchar,varchar) -> boolean"));
  ASSERT_EQ(1, signatures.count("(varchar,bigint) -> boolean"));
  ASSERT_EQ(1, signatures.count("(varchar,double) -> boolean"));
  ASSERT_EQ(1, signatures.count("(varchar,boolean) -> boolean"));
}

TEST_F(JsonFunctionsTest, jsonSizeSignatures) {
  auto signatures = getSignatureStrings("json_size");
  ASSERT_EQ(2, signatures.size());

  ASSERT_EQ(1, signatures.count("(json,varchar) -> bigint"));
  ASSERT_EQ(1, signatures.count("(varchar,varchar) -> bigint"));
}

TEST_F(JsonFunctionsTest, isJsonScalar) {
  // Scalars.
  EXPECT_EQ(isJsonScalar(R"(1)"), true);
  EXPECT_EQ(isJsonScalar(R"(123456)"), true);
  EXPECT_EQ(isJsonScalar(R"("hello")"), true);
  EXPECT_EQ(isJsonScalar(R"("thefoxjumpedoverthefence")"), true);
  EXPECT_EQ(isJsonScalar(R"(1.1)"), true);
  EXPECT_EQ(isJsonScalar(R"("")"), true);
  EXPECT_EQ(isJsonScalar(R"(true)"), true);

  // Lists and maps
  EXPECT_EQ(isJsonScalar(R"([1,2])"), false);
  EXPECT_EQ(isJsonScalar(R"({"k1":"v1"})"), false);
  EXPECT_EQ(isJsonScalar(R"({"k1":[0,1,2]})"), false);
  EXPECT_EQ(isJsonScalar(R"({"k1":""})"), false);
}

TEST_F(JsonFunctionsTest, jsonArrayLength) {
  EXPECT_EQ(jsonArrayLength(R"([])"), 0);
  EXPECT_EQ(jsonArrayLength(R"([1])"), 1);
  EXPECT_EQ(jsonArrayLength(R"([1, 2, 3])"), 3);
  EXPECT_EQ(
      jsonArrayLength(
          R"([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20])"),
      20);

  EXPECT_EQ(jsonArrayLength(R"(1)"), std::nullopt);
  EXPECT_EQ(jsonArrayLength(R"("hello")"), std::nullopt);
  EXPECT_EQ(jsonArrayLength(R"("")"), std::nullopt);
  EXPECT_EQ(jsonArrayLength(R"(true)"), std::nullopt);
  EXPECT_EQ(jsonArrayLength(R"({"k1":"v1"})"), std::nullopt);
  EXPECT_EQ(jsonArrayLength(R"({"k1":[0,1,2]})"), std::nullopt);
  EXPECT_EQ(jsonArrayLength(R"({"k1":[0,1,2], "k2":"v1"})"), std::nullopt);
}

TEST_F(JsonFunctionsTest, jsonArrayContainsBool) {
  EXPECT_EQ(jsonArrayContains<bool>(R"([])", true), false);
  EXPECT_EQ(jsonArrayContains<bool>(R"([1, 2, 3])", false), false);
  EXPECT_EQ(jsonArrayContains<bool>(R"([1.2, 2.3, 3.4])", true), false);
  EXPECT_EQ(
      jsonArrayContains<bool>(R"(["hello", "presto", "world"])", false), false);
  EXPECT_EQ(jsonArrayContains<bool>(R"(1)", true), std::nullopt);
  EXPECT_EQ(
      jsonArrayContains<bool>(R"("thefoxjumpedoverthefence")", false),
      std::nullopt);
  EXPECT_EQ(jsonArrayContains<bool>(R"("")", false), std::nullopt);
  EXPECT_EQ(jsonArrayContains<bool>(R"(true)", true), std::nullopt);
  EXPECT_EQ(
      jsonArrayContains<bool>(R"({"k1":[0,1,2], "k2":"v1"})", true),
      std::nullopt);

  EXPECT_EQ(jsonArrayContains<bool>(R"([true, false])", true), true);
  EXPECT_EQ(jsonArrayContains<bool>(R"([true, true])", false), false);
  EXPECT_EQ(
      jsonArrayContains<bool>(R"([123, 123.456, true, "abc"])", true), true);
  EXPECT_EQ(
      jsonArrayContains<bool>(R"([123, 123.456, true, "abc"])", false), false);
  EXPECT_EQ(
      jsonArrayContains<bool>(
          R"([false, false, false, false, false, false, false,
false, false, false, false, false, false, true, false, false, false, false])",
          true),
      true);
  EXPECT_EQ(
      jsonArrayContains<bool>(
          R"([true, true, true, true, true, true, true,
true, true, true, true, true, true, true, true, true, true, true])",
          false),
      false);

  // Test errors of getting the specified type of json value.
  // Error code is "INCORRECT_TYPE".
  EXPECT_EQ(jsonArrayContains<bool>(R"([truet])", false), false);
  EXPECT_EQ(jsonArrayContains<bool>(R"([truet, false])", false), true);
}

TEST_F(JsonFunctionsTest, jsonArrayContainsBigint) {
  EXPECT_EQ(jsonArrayContains<int64_t>(R"([])", 0), false);
  EXPECT_EQ(jsonArrayContains<int64_t>(R"([1.2, 2.3, 3.4])", 2), false);
  EXPECT_EQ(jsonArrayContains<int64_t>(R"([1.2, 2.0, 3.4])", 2), false);
  EXPECT_EQ(
      jsonArrayContains<int64_t>(R"(["hello", "presto", "world"])", 2), false);
  EXPECT_EQ(jsonArrayContains<int64_t>(R"([false, false, false])", 17), false);
  EXPECT_EQ(jsonArrayContains<int64_t>(R"(1)", 1), std::nullopt);
  EXPECT_EQ(
      jsonArrayContains<int64_t>(R"("thefoxjumpedoverthefence")", 1),
      std::nullopt);
  EXPECT_EQ(jsonArrayContains<int64_t>(R"("")", 1), std::nullopt);
  EXPECT_EQ(jsonArrayContains<int64_t>(R"(true)", 1), std::nullopt);
  EXPECT_EQ(
      jsonArrayContains<int64_t>(R"({"k1":[0,1,2], "k2":"v1"})", 1),
      std::nullopt);

  EXPECT_EQ(jsonArrayContains<int64_t>(R"([1, 2, 3])", 1), true);
  EXPECT_EQ(jsonArrayContains<int64_t>(R"([1, 2, 3])", 4), false);
  EXPECT_EQ(
      jsonArrayContains<int64_t>(R"([123, 123.456, true, "abc"])", 123), true);
  EXPECT_EQ(
      jsonArrayContains<int64_t>(R"([123, 123.456, true, "abc"])", 456), false);
  EXPECT_EQ(
      jsonArrayContains<int64_t>(
          R"([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20])",
          17),
      true);
  EXPECT_EQ(
      jsonArrayContains<int64_t>(
          R"([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20])",
          23),
      false);
  EXPECT_EQ(jsonArrayContains<int64_t>(R"([92233720368547758071])", -9), false);

  // Test errors of getting the specified type of json value.
  // Error code is "INCORRECT_TYPE".
  EXPECT_EQ(jsonArrayContains<int64_t>(R"([-9223372036854775809])", -9), false);
  EXPECT_EQ(
      jsonArrayContains<int64_t>(R"([-9223372036854775809,-9])", -9), true);
  // Error code is "NUMBER_ERROR".
  EXPECT_EQ(jsonArrayContains<int64_t>(R"([01])", 4), false);
  EXPECT_EQ(jsonArrayContains<int64_t>(R"([01, 4])", 4), true);
}

TEST_F(JsonFunctionsTest, jsonArrayContainsDouble) {
  EXPECT_EQ(jsonArrayContains<double>(R"([])", 2.3), false);
  EXPECT_EQ(jsonArrayContains<double>(R"([1, 2, 3])", 2.3), false);
  EXPECT_EQ(jsonArrayContains<double>(R"([1, 2, 3])", 2.0), false);
  EXPECT_EQ(
      jsonArrayContains<double>(R"(["hello", "presto", "world"])", 2.3), false);
  EXPECT_EQ(jsonArrayContains<double>(R"([false, false, false])", 2.3), false);
  EXPECT_EQ(jsonArrayContains<double>(R"(1)", 2.3), std::nullopt);
  EXPECT_EQ(
      jsonArrayContains<double>(R"("thefoxjumpedoverthefence")", 2.3),
      std::nullopt);
  EXPECT_EQ(jsonArrayContains<double>(R"("")", 2.3), std::nullopt);
  EXPECT_EQ(jsonArrayContains<double>(R"(true)", 2.3), std::nullopt);
  EXPECT_EQ(
      jsonArrayContains<double>(R"({"k1":[0,1,2], "k2":"v1"})", 2.3),
      std::nullopt);

  EXPECT_EQ(jsonArrayContains<double>(R"([1.2, 2.3, 3.4])", 2.3), true);
  EXPECT_EQ(jsonArrayContains<double>(R"([1.2, 2.3, 3.4])", 2.4), false);
  EXPECT_EQ(
      jsonArrayContains<double>(R"([123, 123.456, true, "abc"])", 123.456),
      true);
  EXPECT_EQ(
      jsonArrayContains<double>(R"([123, 123.456, true, "abc"])", 456.789),
      false);
  EXPECT_EQ(
      jsonArrayContains<double>(
          R"([1.2, 2.3, 3.4, 4.5, 1.2, 2.3, 3.4, 4.5, 1.2, 2.3, 3.4, 4.5, 1.2, 2.3, 3.4, 4.5, 1.2, 2.3, 3.4, 4.5])",
          4.5),
      true);
  EXPECT_EQ(
      jsonArrayContains<double>(
          R"([1.2, 2.3, 3.4, 4.5, 1.2, 2.3, 3.4, 4.5, 1.2, 2.3, 3.4, 4.5, 1.2, 2.3, 3.4, 4.5, 1.2, 2.3, 3.4, 4.5])",
          4.3),
      false);

  // Test errors of getting the specified type of json value.
  // Error code is "NUMBER_ERROR".
  EXPECT_EQ(jsonArrayContains<double>(R"([9.6E400])", 4.2), false);
  EXPECT_EQ(jsonArrayContains<double>(R"([9.6E400,4.2])", 4.2), true);
}

TEST_F(JsonFunctionsTest, jsonArrayContainsString) {
  EXPECT_EQ(jsonArrayContains<std::string>(R"([])", ""), false);
  EXPECT_EQ(jsonArrayContains<std::string>(R"([1, 2, 3])", "1"), false);
  EXPECT_EQ(jsonArrayContains<std::string>(R"([1.2, 2.3, 3.4])", "2.3"), false);
  EXPECT_EQ(
      jsonArrayContains<std::string>(R"([true, false])", R"("true")"), false);
  EXPECT_EQ(jsonArrayContains<std::string>(R"(1)", "1"), std::nullopt);
  EXPECT_EQ(
      jsonArrayContains<std::string>(R"("thefoxjumpedoverthefence")", "1"),
      std::nullopt);
  EXPECT_EQ(jsonArrayContains<std::string>(R"("")", "1"), std::nullopt);
  EXPECT_EQ(jsonArrayContains<std::string>(R"(true)", "1"), std::nullopt);
  EXPECT_EQ(
      jsonArrayContains<std::string>(R"({"k1":[0,1,2], "k2":"v1"})", "1"),
      std::nullopt);

  EXPECT_EQ(
      jsonArrayContains<std::string>(
          R"(["hello", "presto", "world"])", "presto"),
      true);
  EXPECT_EQ(
      jsonArrayContains<std::string>(
          R"(["hello", "presto", "world"])", "nation"),
      false);
  EXPECT_EQ(
      jsonArrayContains<std::string>(R"([123, 123.456, true, "abc"])", "abc"),
      true);
  EXPECT_EQ(
      jsonArrayContains<std::string>(R"([123, 123.456, true, "abc"])", "def"),
      false);
  EXPECT_EQ(
      jsonArrayContains<std::string>(
          R"(["hello", "presto", "world", "hello", "presto", "world", "hello", "presto", "world", "hello",
"presto", "world", "hello", "presto", "world", "hello", "presto", "world"])",
          "hello"),
      true);
  EXPECT_EQ(
      jsonArrayContains<std::string>(
          R"(["hello", "presto", "world", "hello", "presto", "world", "hello", "presto", "world", "hello",
"presto", "world", "hello", "presto", "world", "hello", "presto", "world"])",
          "hola"),
      false);
  EXPECT_EQ(
      jsonArrayContains<std::string>(
          R"(["hello", "presto", "world", 1, 2, 3, true, false, 1.2, 2.3, {"k1":[0,1,2], "k2":"v1"}])",
          "world"),
      true);
  EXPECT_EQ(
      jsonArrayContains<std::string>(
          R"(["the fox jumped over the fence", "hello presto world"])",
          "hello velox world"),
      false);
  EXPECT_EQ(
      jsonArrayContains<std::string>(
          R"(["the fox jumped over the fence", "hello presto world"])",
          "the fox jumped over the fence"),
      true);
}

TEST_F(JsonFunctionsTest, jsonSize) {
  EXPECT_EQ(jsonSize(R"({"k1":{"k2": 999}, "k3": 1})", "$.k1.k2"), 0);
  EXPECT_EQ(jsonSize(R"({"k1":{"k2": 999}, "k3": 1})", "$.k1"), 1);
  EXPECT_EQ(jsonSize(R"({"k1":{"k2": 999}, "k3": 1})", "$"), 2);
  EXPECT_EQ(jsonSize(R"({"k1":{"k2": 999}, "k3": 1})", "$.k3"), 0);
  EXPECT_EQ(jsonSize(R"({"k1":{"k2": 999}, "k3": [1, 2, 3, 4]})", "$.k3"), 4);
  EXPECT_EQ(jsonSize(R"({"k1":{"k2": 999}, "k3": 1})", "$.k4"), std::nullopt);
  EXPECT_EQ(jsonSize(R"({"k1":{"k2": 999}, "k3"})", "$.k4"), std::nullopt);
  EXPECT_EQ(jsonSize(R"({"k1":{"k2": 999}, "k3": true})", "$.k3"), 0);
  EXPECT_EQ(jsonSize(R"({"k1":{"k2": 999}, "k3": null})", "$.k3"), 0);
  EXPECT_EQ(
      jsonSize(
          R"({"k1":{"k2": 999, "k3": [{"k4": [1, 2, 3]}]}})", "$.k1.k3[0].k4"),
      3);
}

TEST_F(JsonFunctionsTest, invalidPath) {
  VELOX_ASSERT_THROW(jsonSize(R"([0,1,2])", ""), "Invalid JSON path");
  VELOX_ASSERT_THROW(jsonSize(R"([0,1,2])", "$[]"), "Invalid JSON path");
  VELOX_ASSERT_THROW(jsonSize(R"([0,1,2])", "$[-1]"), "Invalid JSON path");
  VELOX_ASSERT_THROW(jsonSize(R"({"k1":"v1"})", "$k1"), "Invalid JSON path");
  VELOX_ASSERT_THROW(jsonSize(R"({"k1":"v1"})", "$.k1."), "Invalid JSON path");
  VELOX_ASSERT_THROW(jsonSize(R"({"k1":"v1"})", "$.k1]"), "Invalid JSON path");
  VELOX_ASSERT_THROW(jsonSize(R"({"k1":"v1)", "$.k1]"), "Invalid JSON path");
}

TEST_F(JsonFunctionsTest, jsonExtract) {
  auto jsonExtract = [&](std::optional<std::string> json,
                         const std::string& path) {
    return evaluateOnce<std::string>(
        "json_extract(c0, c1)",
        makeRowVector(
            {makeJsonVector(json), makeFlatVector<std::string>({path})}));
  };

  EXPECT_EQ(
      "{\"x\": {\"a\" : 1, \"b\" : 2} }",
      jsonExtract("{\"x\": {\"a\" : 1, \"b\" : 2} }", "$"));
  EXPECT_EQ(
      "{\"a\" : 1, \"b\" : 2}",
      jsonExtract("{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x"));
  EXPECT_EQ("1", jsonExtract("{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.a"));
  EXPECT_EQ(
      std::nullopt, jsonExtract("{\"x\": {\"a\" : 1, \"b\" : 2} }", "$.x.c"));
  EXPECT_EQ(
      "3", jsonExtract("{\"x\": {\"a\" : 1, \"b\" : [2, 3]} }", "$.x.b[1]"));
  EXPECT_EQ("2", jsonExtract("[1,2,3]", "$[1]"));
  EXPECT_EQ("null", jsonExtract("[1,null,3]", "$[1]"));
  EXPECT_EQ(std::nullopt, jsonExtract("INVALID_JSON", "$"));
  VELOX_ASSERT_THROW(jsonExtract("{\"\":\"\"}", ""), "Invalid JSON path");

  EXPECT_EQ(
      "[\"0-553-21311-3\",\"0-395-19395-8\"]",
      jsonExtract(kJson, "$.store.book[*].isbn"));
  EXPECT_EQ("\"Evelyn Waugh\"", jsonExtract(kJson, "$.store.book[1].author"));

  // Paths without leading '$'.
  auto json = R"({"x": {"a": 1, "b": [10, 11, 12]} })";
  EXPECT_EQ(R"({"a": 1, "b": [10, 11, 12]})", jsonExtract(json, "x"));
  EXPECT_EQ("1", jsonExtract(json, "x.a"));
  EXPECT_EQ("[10, 11, 12]", jsonExtract(json, "x.b"));
  EXPECT_EQ("12", jsonExtract(json, "x.b[2]"));
  EXPECT_EQ(std::nullopt, jsonExtract(json, "x.c"));
  EXPECT_EQ(std::nullopt, jsonExtract(json, "x.b[20]"));

  // Paths with redundant '.'s.
  json = R"([[[{"a": 1, "b": [1, 2, 3]}]]])";
  EXPECT_EQ("1", jsonExtract(json, "$.[0][0][0].a"));
  EXPECT_EQ("[1, 2, 3]", jsonExtract(json, "$.[0].[0].[0].b"));
  EXPECT_EQ("[1, 2, 3]", jsonExtract(json, "$[0][0].[0].b"));
  EXPECT_EQ("3", jsonExtract(json, "$[0][0][0].b.[2]"));
  EXPECT_EQ("3", jsonExtract(json, "$.[0].[0][0].b.[2]"));

  // Definite vs. non-definite paths.
  EXPECT_EQ("[123]", jsonExtract(R"({"a": [{"b": 123}]})", "$.a[*].b"));
  EXPECT_EQ("123", jsonExtract(R"({"a": [{"b": 123}]})", "$.a[0].b"));

  EXPECT_EQ("[]", jsonExtract(R"({"a": [{"b": 123}]})", "$.a[*].c"));
  EXPECT_EQ(std::nullopt, jsonExtract(R"({"a": [{"b": 123}]})", "$.a[0].c"));

  // TODO The following paths are supported by Presto via Jayway, but do not
  // work in Velox yet. Figure out how to add support for these.
  VELOX_ASSERT_THROW(jsonExtract(kJson, "$..price"), "Invalid JSON path");
  VELOX_ASSERT_THROW(
      jsonExtract(kJson, "$.store.book[?(@.price < 10)].title"),
      "Invalid JSON path");
  VELOX_ASSERT_THROW(jsonExtract(kJson, "max($..price)"), "Invalid JSON path");
  VELOX_ASSERT_THROW(
      jsonExtract(kJson, "concat($..category)"), "Invalid JSON path");
  VELOX_ASSERT_THROW(jsonExtract(kJson, "$.store.keys()"), "Invalid JSON path");
}

} // namespace

} // namespace facebook::velox::functions::prestosql
