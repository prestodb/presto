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

#include "folly/Unicode.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/OptionalEmpty.h"
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

  void testJsonParse(std::string json, std::string expectedJson) {
    auto data = makeRowVector({makeFlatVector<std::string>({json})});
    auto result = evaluate("json_parse(c0)", data);
    auto expected = makeFlatVector<std::string>({expectedJson}, JSON());
    velox::test::assertEqualVectors(expected, result);
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

  void checkInternalFn(
      const std::string& functionName,
      const TypePtr& returnType,
      const RowVectorPtr& data,
      const VectorPtr& expected) {
    auto inputFeild =
        std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c0");

    auto expression = std::make_shared<core::CallTypedExpr>(
        returnType, std::vector<core::TypedExprPtr>{inputFeild}, functionName);

    SelectivityVector rows(data->size());
    std::vector<VectorPtr> result(1);
    exec::ExprSet exprSet({expression}, &execCtx_);
    exec::EvalCtx evalCtx(&execCtx_, &exprSet, data.get());

    exprSet.eval(rows, evalCtx, result);
    velox::test::assertEqualVectors(expected, result[0]);
  };

  std::optional<std::string>
  jsonExtract(VectorPtr json, VectorPtr path, bool wrapInTry = false) {
    return evaluateJsonVectorFunction("json_extract", json, path, wrapInTry);
  }

  std::optional<std::string>
  jsonArrayGet(VectorPtr json, VectorPtr index, bool wrapInTry = false) {
    return evaluateJsonVectorFunction("json_array_get", json, index, wrapInTry);
  }

 private:
  // Utility function to evaluate a function both with and without constant
  // inputs. Ensures that the results are same in both cases. 'wrapInTry' is
  // used to test the cases where function throws a user error and verify
  // that they are captured by the TRY operator. The inputs are expected to have
  // only one row.
  std::optional<std::string> evaluateJsonVectorFunction(
      std::string function,
      VectorPtr firstInput,
      VectorPtr secondInput,
      bool wrapInTry = false) {
    std::string expr =
        !wrapInTry ? function + "(c0, c1)" : "try(" + function + "(c0, c1))";

    auto result = evaluateOnce<std::string>(
        expr, makeRowVector({firstInput, secondInput}));
    auto resultConstantInput = evaluateOnce<std::string>(
        expr,
        makeRowVector(
            {BaseVector::wrapInConstant(1, 0, firstInput),
             BaseVector::wrapInConstant(1, 0, secondInput)}));
    EXPECT_EQ(result, resultConstantInput)
        << "Equal results expected for constant and non constant inputs";
    return result;
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
  // Spaces before and after.
  EXPECT_EQ(jsonParse(R"( "abc"       )"), R"("abc")");
  EXPECT_EQ(jsonParse(R"(true)"), "true");
  EXPECT_EQ(jsonParse(R"(null)"), "null");
  EXPECT_EQ(jsonParse(R"(42)"), "42");
  EXPECT_EQ(jsonParse(R"("abc")"), R"("abc")");
  EXPECT_EQ(jsonParse("\"abc\u4FE1\""), "\"abc\u4FE1\"");
  auto utf32cp = folly::codePointToUtf8(U'😀');
  testJsonParse(fmt::format("\"{}\"", utf32cp), R"("\uD83D\uDE00")");
  EXPECT_EQ(jsonParse(R"([1, 2, 3])"), "[1,2,3]");
  EXPECT_EQ(jsonParse(R"({"k1": "v1" })"), R"({"k1":"v1"})");
  EXPECT_EQ(jsonParse(R"(["k1", "v1"])"), R"(["k1","v1"])");
  testJsonParse(R"({ "abc" : "\/"})", R"({"abc":"/"})");
  testJsonParse(R"({ "abc" : "\\/"})", R"({"abc":"\\/"})");
  testJsonParse("{\"\\\\\":null, \"\\\\\":null}", R"({"\\":null,"\\":null})");
  testJsonParse(R"({ "abc" : [1, 2, 3, 4    ]})", R"({"abc":[1,2,3,4]})");
  // Test out with unicodes and empty keys.
  testJsonParse(
      R"({"4":0.1,"\"":0.14, "自社在庫":0.1, "٢": 2.0, "١": 1.0, "१": 1.0, "": 3.5})",
      R"({"":3.5,"\"":0.14,"4":0.1,"١":1.0,"٢":2.0,"१":1.0,"自社在庫":0.1})");
  testJsonParse(
      R"({"error":"Falha na configura\u00e7\u00e3o do pagamento"})",
      R"({"error":"Falha na configuração do pagamento"})");
  // Test unicode in key and surogate pairs in values.
  testJsonParse(
      R"({"utf\u4FE1": "\u4FE1 \uD83D\uDE00 \/ \n abc a\uD834\uDD1Ec \u263Acba \u0002 \u001F \u0020"})",
      R"({"utf信":"信 \uD83D\uDE00 / \n abc a\uD834\uDD1Ec ☺cba \u0002 \u001F  "})");
  testJsonParse(
      R"({"v\u06ecfzo-\u04fbyw\u25d6#\u2adc\u27e6\u0494\u090e":0.74,"\u042d\u25eb\u03fe)\u044c\u25cb\u2184e":0.89})",
      R"({"v۬fzo-ӻyw◖#⫝̸⟦Ҕऎ":0.74,"Э◫Ͼ)ь○ↄe":0.89})");
  // Test special unicode characters.
  testJsonParse(
      R"({"utf\u4FE1": "\u0002 \u001F \u0020"})",
      R"({"utf信":"\u0002 \u001F  "})");
  // Test casing
  testJsonParse(
      R"("Items for D \ud835\udc52\ud835\udcc1 ")",
      R"("Items for D \uD835\uDC52\uD835\uDCC1 ")");

  // Test bad unicode characters
  testJsonParse("\"Hello \xc0\xaf World\"", "\"Hello �� World\"");
  // The below tests fail if simdjson.doc.get_string() is called
  // without specifying replacement for bad characters in simdjson.
  testJsonParse(R"("\uDE2Dau")", R"("\uDE2Dau")");
  testJsonParse(
      R"([{"response": "[\"fusil a peinture\",\"\ufffduD83E\\uDE2Dau bois\"]"}])",
      R"([{"response":"[\"fusil a peinture\",\"�uD83E\\uDE2Dau bois\"]"}])");

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

  VELOX_ASSERT_THROW(evaluate("json_parse(c0)", data), "TRAILING_CONTENT");

  data = makeRowVector({makeFlatVector<StringView>(
      {R"("This is a long sentence")", R"("This is some other sentence")"})});

  auto result = evaluate("json_parse(c0)", data);
  auto expected = makeFlatVector<StringView>(
      {R"("This is a long sentence")", R"("This is some other sentence")"},
      JSON());
  velox::test::assertEqualVectors(expected, result);

  // ':' are placed below to make parser think its a key and not a value.
  // when processing the next string.
  auto svData = {
      "\"SomeVerylargeStringThatIsUsedAaaBbbService::someSortOfImpressions\""_sv,
      "\"SomeBusinessClusterImagesSignal::genValue\""_sv,
      "\"SomeVerylargeStringThatIsUsedAaaBbbCc::Service::someSortOfImpressions\""_sv,
      "\"SomePreviewUtils::genMediaComponent\""_sv};

  data = makeRowVector({makeFlatVector<StringView>(svData)});
  expected = makeFlatVector<StringView>(svData, JSON());
  result = evaluate("json_parse(c0)", data);
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
    ASSERT_EQ(e.context(), "Top-level Expression: json_parse(c0)");
  }

  // Test partial escape sequences.
  VELOX_ASSERT_USER_THROW(
      jsonParse("{\"k1\\"), "Invalid escape sequence at the end of string");
  VELOX_ASSERT_USER_THROW(
      jsonParse("{\"k1\\u"), "Invalid escape sequence at the end of string");

  // Ensure state is cleared after invalid json
  {
    data = makeRowVector({makeFlatVector<StringView>({
        R"({"key":1578377,"name":"Alto Ma\\u00e9 \\"A\\"","type":"cities"})", // invalid json
        R"([{"k1": "v1" }, {"k2": "v2" }])" // valid json
    })});

    result = evaluate("try(json_parse(c0))", data);

    expected = makeNullableFlatVector<StringView>(
        {std::nullopt, R"([{"k1":"v1"},{"k2":"v2"}])"}, JSON());

    velox::test::assertEqualVectors(expected, result);
  }

  // Test try with invalid json unicode sequences.
  {
    data = makeRowVector({makeFlatVector<StringView>({
        // The logic for sorting keys checks the validity of escape sequences
        // and will throw a user error if unicode sequences are invalid.
        R"({"k\\i":"abc","k2":"xyz\u4FE"})", // invalid json
        // Add a second value to ensure the state is cleared.
        R"([{"k1": "v1" }, {"k2": "v2" }])" // valid json
    })});

    result = evaluate("try(json_parse(c0))", data);

    expected = makeNullableFlatVector<StringView>(
        {std::nullopt, R"([{"k1":"v1"},{"k2":"v2"}])"}, JSON());

    velox::test::assertEqualVectors(expected, result);
  }

  // Test try going through fast path for
  // constants.
  {
    data = makeRowVector(
        {makeConstant(R"({\"k\\i\":\"abc\",\"k2\":\"xyz\u4FE\"})", 3)});

    result = evaluate("try(json_parse(c0))", data);

    expected = makeNullableFlatVector<StringView>(
        {std::nullopt, std::nullopt, std::nullopt}, JSON());

    velox::test::assertEqualVectors(expected, result);
  }

  // Test reusing ExprSet after user exception thrown.
  {
    // A batch with an invalid value that will throw a user error.
    auto data1 = makeRowVector(
        {makeConstant(R"({\"k\\i\":\"abc\",\"k2\":\"xyz\u4FE\"})", 3)});

    // A batch with valid values.
    auto data2 = makeRowVector({makeFlatVector<StringView>(
        {R"([{"k1": "v1" }, {"k2": "v2" }])",
         R"([{"k3": "v3" }, {"k4": "v4" }])"})});

    auto typedExpr = makeTypedExpr("json_parse(c0)", asRowType(data1->type()));
    exec::ExprSet exprSet({typedExpr}, &execCtx_);

    VELOX_ASSERT_USER_THROW(evaluate(exprSet, data1), "Invalid escape digit");

    expected = makeNullableFlatVector<StringView>(
        {R"([{"k1":"v1"},{"k2":"v2"}])", R"([{"k3":"v3"},{"k4":"v4"}])"},
        JSON());

    result = evaluate(exprSet, data2);
    velox::test::assertEqualVectors(expected, result);
  }

  // Test reusing ExprSet after user exception thrown in fast path for
  // constants.
  {
    // A batch with an invalid value that will throw a user error.
    auto data1 = makeRowVector({makeFlatVector<StringView>({
        // The logic for sorting keys checks the validity of escape sequences
        // and will throw a user error if unicode sequences are invalid.
        R"({"k\\i":"abc","k2":"xyz\u4FE"})", // invalid json
        // Add a second value to ensure the state is cleared.
        R"([{"k1": "v1" }, {"k2": "v2" }])" // valid json
    })});

    // A batch with valid values.
    auto data2 = makeRowVector({makeFlatVector<StringView>(
        {R"([{"k1": "v1" }, {"k2": "v2" }])",
         R"([{"k3": "v3" }, {"k4": "v4" }])"})});

    auto typedExpr = makeTypedExpr("json_parse(c0)", asRowType(data1->type()));
    exec::ExprSet exprSet({typedExpr}, &execCtx_);

    VELOX_ASSERT_USER_THROW(evaluate(exprSet, data1), "Invalid escape digit");

    expected = makeNullableFlatVector<StringView>(
        {R"([{"k1":"v1"},{"k2":"v2"}])", R"([{"k3":"v3"},{"k4":"v4"}])"},
        JSON());

    result = evaluate(exprSet, data2);
    velox::test::assertEqualVectors(expected, result);
  }

  // Test with escape sequences in keys.
  {
    // Invalid escape sequence in key should be ignored.
    testJsonParse(
        R"({"\\&page=20": "a", "\\&page=26": "c"})",
        R"({"\\&page=20":"a","\\&page=26":"c"})");

    // Valid escape sequence in key should be parsed correctly.
    testJsonParse(
        R"({"\b&page=20": "a", "\\&page=26": "c"})",
        R"({"\b&page=20":"a","\\&page=26":"c"})");

    testJsonParse(
        R"({"\/&page=20": "a", "\/&page=26": "c"})",
        R"({"/&page=20":"a","/&page=26":"c"})");

    testJsonParse(
        R"({"\/&\"\f\r\n": "a", "\/&page=26": "c"})",
        R"({"/&\"\f\r\n":"a","/&page=26":"c"})");
  }

  // Test with incomplete unicode escape sequences.
  VELOX_ASSERT_USER_THROW(
      jsonParse("\"\\u1234\\u89\""),
      "Invalid escape sequence at the end of string");
}

TEST_F(JsonFunctionsTest, canonicalization) {
  auto json = R"({
  "menu": {
      "id": "file",
      "value": "File",
      "popup": {
          "menuitem": [
              {
                  "value": "New",
                  "onclick": "CreateNewDoc() "
              },
              {
                  "value": "Open",
                  "onclick": "OpenDoc() "
              },
              {
                  "value": "Close",
                  "onclick": "CloseDoc() "
              }
          ]
  }
  }
  })";

  auto expectedJson =
      R"({"menu":{"id":"file","popup":{"menuitem":[{"onclick":"CreateNewDoc() ","value":"New"},{"onclick":"OpenDoc() ","value":"Open"},{"onclick":"CloseDoc() ","value":"Close"}]},"value":"File"}})";
  testJsonParse(json, expectedJson);

  json =
      "{\n"
      "  \"name\": \"John Doe\",\n"
      "  \"address\": {\n"
      "    \"street\": \"123 Main St\",\n"
      "    \"city\": \"Anytown\",\n"
      "    \"state\": \"CA\",\n"
      "    \"zip\": \"12345\"\n"
      "  },\n"
      "  \"phoneNumbers\": [\n"
      "    {\n"
      "      \"type\": \"home\",\n"
      "      \"number\": \"555-1234\"\n"
      "    },\n"
      "    {\n"
      "      \"type\": \"work\",\n"
      "      \"number\": \"555-5678\"\n"
      "    }\n"
      "  ],\n"
      "  \"familyMembers\": [\n"
      "    {\n"
      "      \"name\": \"Jane Doe\",\n"
      "      \"relationship\": \"wife\"\n"
      "    },\n"
      "    {\n"
      "      \"name\": \"Jimmy Doe\",\n"
      "      \"relationship\": \"son\"\n"
      "    }\n"
      "  ],\n"
      "  \"hobbies\": [\"golf\", \"reading\", \"traveling\"]\n"
      "}";
  expectedJson =
      R"({"address":{"city":"Anytown","state":"CA","street":"123 Main St","zip":"12345"},"familyMembers":[{"name":"Jane Doe","relationship":"wife"},{"name":"Jimmy Doe","relationship":"son"}],"hobbies":["golf","reading","traveling"],"name":"John Doe","phoneNumbers":[{"number":"555-1234","type":"home"},{"number":"555-5678","type":"work"}]})";
  testJsonParse(json, expectedJson);

  // Json with spaces in keys
  json = R"({
  "menu": {
      "id": "file",
      "value": "File",
      "emptyArray": [],
      "popup": {
          "menuitem": [
              {
                  "value ": "New ",
                  "onclick": "CreateNewDoc() ",
                  " value ": " Space "
              }
           ]
  }
  }
  })";

  expectedJson =
      R"({"menu":{"emptyArray":[],"id":"file","popup":{"menuitem":[{" value ":" Space ","onclick":"CreateNewDoc() ","value ":"New "}]},"value":"File"}})";
  testJsonParse(json, expectedJson);

  json =
      R"({"stars":[{"updated_deferred_payout_state":null,"onboard_surface":"MTA_ON_MOBILE","entry_point":"FROM_STARS","task_name":null,"event":"START_APPLICATION","time":1678975122,"user_id":123456789123456},{"updated_deferred_payout_state":null,"onboard_surface":"MTA_ON_MOBILE","entry_point":"FROM_STARS","task_name":"STARS_SIGN_TOS","event":"START_TASK","time":1678975122,"user_id":123456789123456},{"updated_deferred_payout_state":null,"onboard_surface":"MTA_ON_MOBILE","entry_point":"FROM_STARS","task_name":"STARS_SIGN_TOS","event":"COMPLETE_TASK","time":1678975128,"user_id":123456789123456},{"error":null,"updated_deferred_payout_state":null,"onboard_surface":"MTA_ON_MOBILE","entry_point":"FROM_MOBILE_PRO_DASH","task_name":null,"event":"START_APPLICATION","time":1706866395,"user_id":123456789123456},{"error":null,"updated_deferred_payout_state":null,"onboard_surface":"MTA_ON_MOBILE","entry_point":"FROM_MOBILE_PRO_DASH","task_name":"STARS_DEFERRED_PAYOUT_WITH_TOS","event":"START_TASK","time":1706866395,"user_id":123456789123456},{"error":null,"updated_deferred_payout_state":"PAYOUT_SETUP_DEFERRED","onboard_surface":"MTA_ON_MOBILE","entry_point":"FROM_MOBILE_PRO_DASH","task_name":"STARS_DEFERRED_PAYOUT_WITH_TOS","event":"COMPLETE_TASK","time":1706866402,"user_id":123456789123456},{"error":null,"updated_deferred_payout_state":null,"onboard_surface":"MTA_ON_MOBILE","entry_point":"FROM_MOBILE_PRO_DASH","task_name":null,"event":"SUBMIT_APPLICATION","time":1706866402,"user_id":123456789123456},{"error":null,"updated_deferred_payout_state":null,"onboard_surface":"MTA_ON_MOBILE","entry_point":"FROM_MOBILE_PRO_DASH","task_name":null,"event":"APPLICATION_APPROVED","time":1706866402,"user_id":123456789123456}]})";

  expectedJson =
      R"({"stars":[{"entry_point":"FROM_STARS","event":"START_APPLICATION","onboard_surface":"MTA_ON_MOBILE","task_name":null,"time":1678975122,"updated_deferred_payout_state":null,"user_id":123456789123456},{"entry_point":"FROM_STARS","event":"START_TASK","onboard_surface":"MTA_ON_MOBILE","task_name":"STARS_SIGN_TOS","time":1678975122,"updated_deferred_payout_state":null,"user_id":123456789123456},{"entry_point":"FROM_STARS","event":"COMPLETE_TASK","onboard_surface":"MTA_ON_MOBILE","task_name":"STARS_SIGN_TOS","time":1678975128,"updated_deferred_payout_state":null,"user_id":123456789123456},{"entry_point":"FROM_MOBILE_PRO_DASH","error":null,"event":"START_APPLICATION","onboard_surface":"MTA_ON_MOBILE","task_name":null,"time":1706866395,"updated_deferred_payout_state":null,"user_id":123456789123456},{"entry_point":"FROM_MOBILE_PRO_DASH","error":null,"event":"START_TASK","onboard_surface":"MTA_ON_MOBILE","task_name":"STARS_DEFERRED_PAYOUT_WITH_TOS","time":1706866395,"updated_deferred_payout_state":null,"user_id":123456789123456},{"entry_point":"FROM_MOBILE_PRO_DASH","error":null,"event":"COMPLETE_TASK","onboard_surface":"MTA_ON_MOBILE","task_name":"STARS_DEFERRED_PAYOUT_WITH_TOS","time":1706866402,"updated_deferred_payout_state":"PAYOUT_SETUP_DEFERRED","user_id":123456789123456},{"entry_point":"FROM_MOBILE_PRO_DASH","error":null,"event":"SUBMIT_APPLICATION","onboard_surface":"MTA_ON_MOBILE","task_name":null,"time":1706866402,"updated_deferred_payout_state":null,"user_id":123456789123456},{"entry_point":"FROM_MOBILE_PRO_DASH","error":null,"event":"APPLICATION_APPROVED","onboard_surface":"MTA_ON_MOBILE","task_name":null,"time":1706866402,"updated_deferred_payout_state":null,"user_id":123456789123456}]})";

  testJsonParse(json, expectedJson);
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

  // Malformed Json.
  EXPECT_EQ(jsonArrayLength(R"((})"), std::nullopt);
}

TEST_F(JsonFunctionsTest, jsonArrayGet) {
  auto arrayGet = [&](const std::optional<StringView>& json,
                      int64_t index,
                      TypePtr jsontype) {
    std::optional<StringView> s = json.has_value()
        ? std::make_optional(StringView(json.value()))
        : std::nullopt;
    auto jsonInput = makeNullableFlatVector<std::string>({s}, jsontype);
    auto indexInput = makeFlatVector<int64_t>(std::vector<int64_t>{index});
    return JsonFunctionsTest::jsonArrayGet(jsonInput, indexInput);
  };

  for (TypePtr& type : std::vector<TypePtr>{JSON(), VARCHAR()}) {
    EXPECT_FALSE(arrayGet("{}", 1, type).has_value());
    EXPECT_FALSE(arrayGet("[]", 1, type).has_value());

    // Malformed json.
    EXPECT_FALSE(arrayGet("([1]})", 0, type).has_value());

    EXPECT_EQ(arrayGet("[1, 2, 3]", 0, type), "1");
    EXPECT_EQ(arrayGet("[1, 2, 3]", 1, type), "2");
    EXPECT_EQ(arrayGet("[1, 2, 3]", 2, type), "3");
    EXPECT_FALSE(arrayGet("[1, 2, 3]", 3, type).has_value());

    EXPECT_EQ(arrayGet("[1, 2, 3]", -1, type), "3");
    EXPECT_EQ(arrayGet("[1, 2, 3]", -2, type), "2");
    EXPECT_EQ(arrayGet("[1, 2, 3]", -3, type), "1");
    EXPECT_FALSE(arrayGet("[1, 2, 3]", -4, type).has_value());
    EXPECT_EQ(arrayGet("[[1, 2], [3, 4], []]", 2, type), "[]");

    if (type == VARCHAR()) {
      // Ensure the result is canonicalized before returning.
      EXPECT_EQ(arrayGet("[[1, 2], [3, 4], []]", 0, type), "[1,2]");
      EXPECT_EQ(
          arrayGet("[{\"foo\": 123}, {\"foo\": 456}]", 1, type),
          "{\"foo\":456}");
      EXPECT_EQ(
          arrayGet("[{\"foo\": 123}, {\"foo\": 456 , \"bar\": 789}]", 1, type),
          "{\"bar\":789,\"foo\":456}");
      EXPECT_EQ(arrayGet(R"([{ "abc" : "\/"}])", 0, type), R"({"abc":"/"})");
    } else {
      EXPECT_EQ(arrayGet("[[1,2],[3,4],[]]", 0, type), "[1,2]");
      EXPECT_EQ(
          arrayGet("[{\"foo\":123}, {\"foo\":456}]", 1, type), "{\"foo\":456}");
    }
    EXPECT_FALSE(arrayGet("[1, 2, ...", 1, type).has_value());
    EXPECT_FALSE(arrayGet("not json", 1, type).has_value());
  }
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

  EXPECT_EQ(jsonArrayContains<int64_t>(R"([1, 2, 3,...)", 2), std::nullopt);
  EXPECT_EQ(jsonArrayContains<int64_t>(R"([1, 2, 3,...)", 5), std::nullopt);

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

  static const double kNan = std::numeric_limits<double>::quiet_NaN();
  static const double kInf = std::numeric_limits<double>::infinity();
  EXPECT_EQ(jsonArrayContains<double>(R"([1.1, 2.2, 3.3])", kNan), false);
  EXPECT_EQ(jsonArrayContains<double>(R"([1.1, 2.2, 3.3])", kInf), false);
  EXPECT_EQ(jsonArrayContains<double>(R"([1.1, 2.2, 3.3...)", kNan), false);
  EXPECT_EQ(jsonArrayContains<double>(R"([1.1, 2.2, 3.3...)", kInf), false);

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

TEST_F(JsonFunctionsTest, jsonArrayContainsMalformed) {
  auto [jsonVector, _] = makeVectors(R"([]})");
  EXPECT_EQ(
      evaluateOnce<bool>(
          "json_array_contains(c0, 'a')", makeRowVector({jsonVector})),
      std::nullopt);
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
  VELOX_ASSERT_THROW(jsonSize(R"([0,1,2])", "$-1"), "Invalid JSON path");
  VELOX_ASSERT_THROW(jsonSize(R"({"k1":"v1"})", "$k1"), "Invalid JSON path");
  VELOX_ASSERT_THROW(jsonSize(R"({"k1":"v1"})", "$.k1."), "Invalid JSON path");
  VELOX_ASSERT_THROW(jsonSize(R"({"k1":"v1"})", "$.k1["), "Invalid JSON path");
  VELOX_ASSERT_THROW(jsonSize(R"({"k1":"v1)", "$.k1["), "Invalid JSON path");
}

TEST_F(JsonFunctionsTest, jsonExtract) {
  auto jsonExtract = [&](std::optional<std::string> json,
                         const std::string& path,
                         bool wrapInTry = false) {
    auto jsonInput = makeJsonVector(json);
    auto pathInput = makeFlatVector<std::string>({path});
    return JsonFunctionsTest::jsonExtract(jsonInput, pathInput, wrapInTry);
  };

  EXPECT_EQ(
      R"({"x": {"a" : 1, "b" : 2} })",
      jsonExtract(R"({"x": {"a" : 1, "b" : 2} })", "$"));
  EXPECT_EQ(
      R"({"a" : 1, "b" : 2})",
      jsonExtract(R"({"x": {"a" : 1, "b" : 2} })", "$.x"));
  EXPECT_EQ("1", jsonExtract(R"({"x": {"a" : 1, "b" : 2} })", "$.x.a"));
  EXPECT_EQ(
      std::nullopt, jsonExtract(R"({"x": {"a" : 1, "b" : 2} })", "$.x.c"));
  EXPECT_EQ("3", jsonExtract(R"({"x": {"a" : 1, "b" : [2, 3]} })", "$.x.b[1]"));
  EXPECT_EQ(
      "1", jsonExtract(R"({"x": {"a" : 1, "b" : 2} })", R"($['x']["a"])"));

  EXPECT_EQ("2", jsonExtract("[1, 2, 3]", "$[1]"));
  EXPECT_EQ("null", jsonExtract("[1, null, 3]", "$[1]"));
  EXPECT_EQ(std::nullopt, jsonExtract("[1, 2, 3]", "$[10]"));

  EXPECT_EQ("3", jsonExtract("[1, 2, 3]", "$[-1]"));
  EXPECT_EQ("null", jsonExtract("[1, null, 3]", "$[-2]"));
  EXPECT_EQ(std::nullopt, jsonExtract("[1, 2, 3]", "$[-10]"));

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

  // Wildcard on empty object and array
  EXPECT_EQ("[]", jsonExtract("{\"a\": {}", "$.a.[*]"));
  EXPECT_EQ("[]", jsonExtract("{\"a\": []}", "$.a.[*]"));

  // Calling wildcard on a scalar
  EXPECT_EQ("[]", jsonExtract(R"({"a": {"b": [123, 456]}})", "$.a.b.[0].[*]"));
  EXPECT_EQ("[]", jsonExtract("1", "$.*"));

  // Scalar json
  EXPECT_EQ("1", jsonExtract("1", "$"));
  EXPECT_EQ(std::nullopt, jsonExtract("1", "$.foo"));
  EXPECT_EQ(std::nullopt, jsonExtract("1", "$.[0]"));
  // Scalar 'null' json
  EXPECT_EQ("null", jsonExtract("null", "$"));
  EXPECT_EQ(std::nullopt, jsonExtract("null", "$.foo"));
  EXPECT_EQ(std::nullopt, jsonExtract("null", "$.[0]"));

  // Recursive opearator
  EXPECT_EQ("[8.95,12.99,8.99,22.99,19.95]", jsonExtract(kJson, "$..price"));
  EXPECT_EQ(
      "[8.95,12.99,8.99,22.99,19.95,8.95,12.99,8.99,22.99,19.95,8.95,12.99,8.99,22.99]",
      jsonExtract(kJson, "$..*..price"));
  EXPECT_EQ("[]", jsonExtract(kJson, "$..nonExistentKey"));
  EXPECT_EQ(std::nullopt, jsonExtract(kJson, "$.nonExistentKey..price"));
  EXPECT_EQ(
      std::nullopt, jsonExtract(R"({"a": {"b": [123, 456]}})", "$.a.c..[0]"));

  // Calling Recursive opearator on a scalar
  EXPECT_EQ("[]", jsonExtract(R"({"a": {"b": [123, 456]}})", "$.a.b.[0]..[0]"));
  EXPECT_EQ("[]", jsonExtract("1", "$..key"));

  // non-definite paths that end up being evaluated vs. not evaluated
  EXPECT_EQ(
      "[123,456]", jsonExtract(R"({"a": {"b": [123, 456]}})", "$.a.b[*]"));
  EXPECT_EQ(
      std::nullopt, jsonExtract(R"({"a": {"b": [123, 456]}})", "$.a.c[*]"));
  EXPECT_EQ(
      "[[123, 456]]", jsonExtract(R"({"a": {"b": [123, 456]}})", "$.a..b"));
  EXPECT_EQ("[]", jsonExtract(R"({"a": {"b": [123, 456]}})", "$.a..c"));
  EXPECT_EQ(std::nullopt, jsonExtract(R"({"a": {"b": [123, 456]}})", "$.c..b"));

  // Unicode keys
  EXPECT_EQ(R"({"á": 1, "â": 2})", jsonExtract(R"({"á": 1, "â": 2})", "$"));
  EXPECT_EQ("1", jsonExtract(R"({"á": 1, "â": 2})", "$.á"));
  EXPECT_EQ("1", jsonExtract(R"({"á": 1, "â": 2})", "$.[\"á\"]"));
  EXPECT_EQ("1", jsonExtract(R"({"á": 1, "â": 2})", "$.[\"\u00E1\"]"));

  // TODO The following paths are supported by Presto via Jayway, but do not
  // work in Velox yet. Figure out how to add support for these.
  VELOX_ASSERT_THROW(
      jsonExtract(kJson, "$.store.book[?(@.price < 10)].title"),
      "Invalid JSON path");
  VELOX_ASSERT_THROW(jsonExtract(kJson, "max($..price)"), "Invalid JSON path");
  VELOX_ASSERT_THROW(
      jsonExtract(kJson, "concat($..category)"), "Invalid JSON path");
  VELOX_ASSERT_THROW(jsonExtract(kJson, "$.store.keys()"), "Invalid JSON path");

  // Ensure User errors are captured in try() and not thrown.
  EXPECT_EQ(
      std::nullopt,
      jsonExtract(kJson, "$.store.book[?(@.price <10)].title", true));
  EXPECT_EQ(std::nullopt, jsonExtract(kJson, "max($..price)", true));
  EXPECT_EQ(std::nullopt, jsonExtract(kJson, "concat($..category)", true));
  EXPECT_EQ(std::nullopt, jsonExtract(kJson, "$.store.keys()", true));
}

TEST_F(JsonFunctionsTest, jsonExtractVarcharInput) {
  auto jsonExtract = [&](std::optional<std::string> json,
                         const std::string& path,
                         bool wrapInTry = false) {
    std::optional<StringView> s = json.has_value()
        ? std::make_optional(StringView(json.value()))
        : std::nullopt;
    auto varcharInput = makeNullableFlatVector<std::string>({s}, VARCHAR());
    auto pathInput = makeFlatVector<std::string>({path});
    return JsonFunctionsTest::jsonExtract(varcharInput, pathInput, wrapInTry);
  };

  // Valid json
  EXPECT_EQ(
      R"({"x":{"a":1,"b":2}})",
      jsonExtract(R"({"x": {"a" : 1, "b" : 2} })", "$"));
  EXPECT_EQ(
      R"({"a":1,"b":2})", jsonExtract(R"({"x": {"a" : 1, "b" : 2} })", "$.x"));

  // Invalid JSON
  EXPECT_EQ(std::nullopt, jsonExtract(R"({"x": {"a" : 1, "b" : "2""} })", "$"));
  // Non-canonicalized json
  EXPECT_EQ(
      R"({"x":{"a":1,"b":2}})",
      jsonExtract(R"({"x": {"b" : 2, "a" : 1} })", "$"));
  // Input has escape characters
  EXPECT_EQ(
      R"({"x":{"a":"/1","b":"/2"}})",
      jsonExtract(R"({"x": {"a" : "\/1", "b" : "\/2"} })", "$"));
  // Invalid path
  VELOX_ASSERT_THROW(jsonExtract(kJson, "$.book[1:2]"), "Invalid JSON path");
  // Ensure User error is captured in try() and not thrown.
  EXPECT_EQ(std::nullopt, jsonExtract(kJson, "$.book[1:2]", true));
}

// The following tests ensure that the internal json functions
// $internal$json_string_to_array/map/row_cast can be invoked without issues
// from Prestissimo. The actual functionality is tested in JsonCastTest.

TEST_F(JsonFunctionsTest, jsonStringToArrayCast) {
  // Array of strings.
  auto data = makeRowVector({makeNullableFlatVector<std::string>(
      {R"(["red","blue"])"_sv,
       R"([null,null,"purple"])"_sv,
       "[]"_sv,
       "null"_sv})});
  auto expected = makeNullableArrayVector<StringView>(
      {{{"red"_sv, "blue"_sv}},
       {{std::nullopt, std::nullopt, "purple"_sv}},
       common::testutil::optionalEmpty,
       std::nullopt});

  checkInternalFn(
      "$internal$json_string_to_array_cast", ARRAY(VARCHAR()), data, expected);

  // Array of integers.
  data = makeRowVector({makeNullableFlatVector<std::string>(
      {R"(["10212","1015353"])"_sv, R"(["10322","285000"])"})});
  expected =
      makeNullableArrayVector<int64_t>({{10212, 1015353}, {10322, 285000}});

  checkInternalFn(
      "$internal$json_string_to_array_cast", ARRAY(BIGINT()), data, expected);
}

TEST_F(JsonFunctionsTest, jsonStringToMapCast) {
  // Map of strings.
  auto data = makeRowVector({makeFlatVector<std::string>(
      {R"({"red":1,"blue":2})"_sv,
       R"({"green":3,"magenta":4})"_sv,
       R"({"violet":1,"blue":2})"_sv,
       R"({"yellow":1,"blue":2})"_sv,
       R"({"purple":10,"cyan":5})"_sv})});

  auto expected = makeMapVector<StringView, int64_t>(
      {{{"red"_sv, 1}, {"blue"_sv, 2}},
       {{"green"_sv, 3}, {"magenta"_sv, 4}},
       {{"violet"_sv, 1}, {"blue"_sv, 2}},
       {{"yellow"_sv, 1}, {"blue"_sv, 2}},
       {{"purple"_sv, 10}, {"cyan"_sv, 5}}});

  checkInternalFn(
      "$internal$json_string_to_map_cast",
      MAP(VARCHAR(), BIGINT()),
      data,
      expected);
}

TEST_F(JsonFunctionsTest, jsonStringToRowCast) {
  // Row of strings.
  auto data = makeRowVector({makeFlatVector<std::string>(
      {R"({"red":1,"blue":2})"_sv,
       R"({"red":3,"blue":4})"_sv,
       R"({"red":1,"blue":2})"_sv,
       R"({"red":1,"blue":2})"_sv,
       R"({"red":10,"blue":5})"_sv})});

  auto expected = makeRowVector(
      {makeFlatVector<int64_t>({1, 3, 1, 1, 10}),
       makeFlatVector<int64_t>({2, 4, 2, 2, 5})});

  checkInternalFn(
      "$internal$json_string_to_row_cast",
      ROW({{"red", BIGINT()}, {"blue", BIGINT()}}),
      data,
      expected);
}

} // namespace

} // namespace facebook::velox::functions::prestosql
