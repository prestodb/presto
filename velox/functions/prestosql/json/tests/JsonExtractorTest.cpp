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

#include "velox/functions/prestosql/json/JsonExtractor.h"

#include "folly/json.h"
#include "gtest/gtest.h"
#include "velox/common/base/VeloxException.h"

using facebook::velox::VeloxUserError;
using facebook::velox::functions::jsonExtract;
using facebook::velox::functions::jsonExtractScalar;
using folly::json::parse_error;
using namespace std::string_literals;

#define EXPECT_SCALAR_VALUE_EQ(json, path, ret) \
  {                                             \
    auto val = jsonExtractScalar(json, path);   \
    EXPECT_TRUE(val.hasValue());                \
    EXPECT_EQ(val.value(), ret);                \
  }

#define EXPECT_SCALAR_VALUE_NULL(json, path) \
  EXPECT_FALSE(jsonExtractScalar(json, path).hasValue())

#define EXPECT_JSON_VALUE_EQ(json, path, ret)        \
  {                                                  \
    auto val = json_format(jsonExtract(json, path)); \
    EXPECT_TRUE(val.hasValue());                     \
    EXPECT_EQ(val.value(), ret);                     \
  }

#define EXPECT_JSON_VALUE_NULL(json, path) \
  EXPECT_FALSE(json_format(jsonExtract(json, path)).hasValue())

#define EXPECT_THROW_INVALID_ARGUMENT(json, path) \
  EXPECT_THROW(jsonExtract(json, path), VeloxUserError)

namespace {
folly::Optional<std::string> json_format(
    const folly::Optional<folly::dynamic>& json) {
  if (json.has_value()) {
    return folly::toJson(json.value());
  }
  return folly::none;
}
} // namespace

TEST(JsonExtractorTest, generalJsonTest) {
  // clang-format off
  std::string json = R"DELIM(
      {"store":
        {"fruit":[
          {"weight":8, "type":"apple"},
          {"weight":9, "type":"pear"}],
         "basket":[[1,2,{"b":"y","a":"x"}],[3,4],[5,6]],
         "book":[
            {"author":"Nigel Rees",
             "title":"Sayings of the Century",
             "category":"reference",
             "price":8.95},
            {"author":"Herman Melville",
             "title":"Moby Dick",
             "category":"fiction",
             "price":8.99,
             "isbn":"0-553-21311-3"},
            {"author":"J. R. R. Tolkien",
             "title":"The Lord of the Rings",
             "category":"fiction",
             "reader":[
                {"age":25,
                 "name":"bob"},
                {"age":26,
                 "name":"jack"}],
             "price":22.99,
             "isbn":"0-395-19395-8"}],
          "bicycle":{"price":19.95, "color":"red"}},
        "e mail":"amy@only_for_json_udf_test.net",
        "owner":"amy"})DELIM";
  // clang-format on
  std::replace(json.begin(), json.end(), '\'', '\"');
  auto res = json_format(jsonExtract(json, "$.store.fruit[0].weight"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("8", res.value());
  res = json_format(jsonExtract(json, "$.store.fruit[1].weight"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("9", res.value());
  EXPECT_FALSE(
      json_format(jsonExtract(json, "$.store.fruit[2].weight"s)).has_value());
  res = json_format(jsonExtract(json, "$.store.fruit[*].weight"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("[8,9]", res.value());
  res = json_format(jsonExtract(json, "$.store.fruit[*].type"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("[\"apple\",\"pear\"]", res.value());
  res = json_format(jsonExtract(json, "$.store.book[0].price"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("8.95", res.value());
  res = json_format(jsonExtract(json, "$.store.book[2].category"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("\"fiction\"", res.value());
  res = json_format(jsonExtract(json, "$.store.basket[1]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("[3,4]", res.value());
  res = json_format(jsonExtract(json, "$.store.basket[0]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ(
      folly::parseJson("[1,2,{\"a\":\"x\",\"b\":\"y\"}]"),
      folly::parseJson(res.value()));
  EXPECT_FALSE(
      json_format(jsonExtract(json, "$.store.baskets[1]"s)).has_value());
  res = json_format(jsonExtract(json, "$[\"e mail\"]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("\"amy@only_for_json_udf_test.net\"", res.value());
  res = json_format(jsonExtract(json, "$.owner"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("\"amy\"", res.value());
  res = json_format(
      jsonExtract("[[1.1,[2.1,2.2]],2,{\"a\":\"b\"}]"s, "$[0][1][1]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("2.2", res.value());
  res = json_format(jsonExtract("[1,2,{\"a\":\"b\"}]"s, "$[1]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("2", res.value());
  res = json_format(jsonExtract("[1,2,{\"a\":\"b\"}]"s, "$[2]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("{\"a\":\"b\"}", res.value());
  EXPECT_FALSE(
      json_format(jsonExtract("[1,2,{\"a\":\"b\"}]"s, "$[3]"s)).has_value());
  res = json_format(jsonExtract("[{\"a\":\"b\"}]"s, "$[0]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("{\"a\":\"b\"}", res.value());
  EXPECT_FALSE(
      json_format(jsonExtract("[{\"a\":\"b\"}]"s, "$[2]"s)).has_value());
  res = json_format(jsonExtract("{\"a\":\"b\"}"s, " $ "s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("{\"a\":\"b\"}", res.value());

  std::string json2 =
      "[[{\"key\": 1, \"value\": 2},"
      "{\"key\": 2, \"value\": 4}],"
      "[{\"key\": 3, \"value\": 6},"
      "{\"key\": 4, \"value\": 8},"
      "{\"key\": 5, \"value\": 10}]]";

  // Key value pair order of a JsonMap may be changed after folly::toJson
  std::string expected("[[{\"key\":1,\"value\":2},{\"key\":2,\"value\":4}],");
  expected.append("[{\"key\":3,\"value\":6},{\"key\":4,\"value\":8},")
      .append("{\"key\":5,\"value\":10}]]");
  auto expectedJson = folly::parseJson(expected);
  res = json_format(jsonExtract(json2, "$[*]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ(expectedJson, folly::parseJson(res.value()));

  expected.clear();
  expected.append("[{\"key\":1,\"value\":2},{\"key\":2,\"value\":4},")
      .append("{\"key\":3,\"value\":6},")
      .append("{\"key\":4,\"value\":8},{\"value\":10,\"key\":5}]");
  expectedJson = folly::parseJson(expected);
  res = json_format(jsonExtract(json2, "$[*][*]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ(expectedJson, folly::parseJson(res.value()));

  res = json_format(jsonExtract(json2, "$[*][*].key"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("[1,2,3,4,5]", res.value());

  expected = "[{\"key\":1,\"value\":2},{\"key\":3,\"value\":6}]";
  expectedJson = folly::parseJson(expected);
  res = json_format(jsonExtract(json2, "$[*][0]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ(expectedJson, folly::parseJson(res.value()));

  expected = "[{\"key\":5,\"value\":10}]";
  expectedJson = folly::parseJson(expected);
  res = json_format(jsonExtract(json2, "$[*][2]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ(expectedJson, folly::parseJson(res.value()));

  // TEST WiteSpaces in Path and Json
  res = json_format(jsonExtract(json2, "$[*][*].key"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("[1,2,3,4,5]", res.value());
  res = json_format(
      jsonExtract(" [ [1.1,[2.1,2.2]],2, {\"a\": \"b\"}]"s, " $[0][1][1]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("2.2", res.value());
  EXPECT_THROW_INVALID_ARGUMENT(json2, "  \t\n "s);
}

// Test compatibility with Presto
// Reference: from https://github.com/prestodb/presto
// presto-main/src/test/java/com/facebook/presto/operator/scalar/TestJsonExtract.java
TEST(JsonExtractorTest, scalarValueTest) {
  EXPECT_SCALAR_VALUE_EQ("123"s, "$"s, "123"s);
  EXPECT_SCALAR_VALUE_EQ("-1"s, "$"s, "-1"s);
  EXPECT_SCALAR_VALUE_EQ("\"abc\""s, "$"s, "abc"s);
  EXPECT_SCALAR_VALUE_EQ("\"\""s, "$"s, ""s);
  EXPECT_SCALAR_VALUE_NULL("null"s, "$"s);

  // Test character escaped values
  EXPECT_SCALAR_VALUE_EQ("\"ab\\u0001c\""s, "$"s, "ab\001c"s);
  EXPECT_SCALAR_VALUE_EQ("\"ab\\u0002c\""s, "$"s, "ab\002c"s);

  // Complex types should return null
  EXPECT_SCALAR_VALUE_NULL("[1, 2, 3]"s, "$"s);
  EXPECT_SCALAR_VALUE_NULL("{\"a\": 1}"s, "$"s);
}

TEST(JsonExtractorTest, jsonValueTest) {
  // Check scalar values
  EXPECT_JSON_VALUE_EQ("123"s, "$"s, "123"s);
  EXPECT_JSON_VALUE_EQ("-1"s, "$"s, "-1"s);
  EXPECT_JSON_VALUE_EQ("0.01"s, "$"s, "0.01"s);
  EXPECT_JSON_VALUE_EQ("\"abc\""s, "$"s, "\"abc\""s);
  EXPECT_JSON_VALUE_EQ("\"\""s, "$"s, "\"\""s);
  EXPECT_JSON_VALUE_EQ("null"s, "$"s, "null"s);

  // Test character escaped values
  EXPECT_JSON_VALUE_EQ("\"ab\\u0001c\""s, "$"s, "\"ab\\u0001c\""s);
  EXPECT_JSON_VALUE_EQ("\"ab\\u0002c\""s, "$"s, "\"ab\\u0002c\""s);

  // Complex types should return json values
  EXPECT_JSON_VALUE_EQ("[1, 2, 3]"s, "$"s, "[1,2,3]"s);
  EXPECT_JSON_VALUE_EQ("{\"a\": 1}"s, "$"s, "{\"a\":1}"s);
}

TEST(JsonExtractorTest, arrayJsonValueTest) {
  EXPECT_JSON_VALUE_NULL("[]"s, "$[0]"s);
  EXPECT_JSON_VALUE_EQ("[1, 2, 3]"s, "$[0]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("[1, 2]"s, "$[1]"s, "2"s);
  EXPECT_JSON_VALUE_EQ("[1, null]"s, "$[1]"s, "null"s);
  // Out of bounds
  EXPECT_JSON_VALUE_NULL("[1]"s, "$[1]"s);
  // Check skipping complex structures
  EXPECT_JSON_VALUE_EQ("[{\"a\": 1}, 2, 3]"s, "$[1]"s, "2"s);
}

TEST(JsonExtractorTest, objectJsonValueTest) {
  EXPECT_JSON_VALUE_NULL("{}"s, "$.fuu"s);
  EXPECT_JSON_VALUE_NULL("{\"a\": 1}"s, "$.fuu"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": 1}"s, "$.fuu"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"a\": 0, \"fuu\": 1}"s, "$.fuu"s, "1"s);
  // Check skipping complex structures
  EXPECT_JSON_VALUE_EQ("{\"a\": [1, 2, 3], \"fuu\": 1}"s, "$.fuu"s, "1"s);
}

TEST(JsonExtractorTest, wildcardSelect) {
  std::string json =
      R"([{"c1":"v1","c2":"v2","c3":"v3","c4":[{"k41":"v41"},{"k42":"v42"},{"k41":"v43"}]}])";

  auto res = json_format(jsonExtract(json, "$[*].c3"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ(R"(["v3"])", res.value());

  res = json_format(jsonExtract(json, "$[*].c4"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ(R"([[{"k41":"v41"},{"k42":"v42"},{"k41":"v43"}]])", res.value());

  res = json_format(jsonExtract(json, "$[*].c4[0]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ(R"([{"k41":"v41"}])", res.value());

  res = json_format(jsonExtract(json, "$[*].c4[*].k41"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ(R"(["v41","v43"])", res.value());
}

TEST(JsonExtractorTest, fullScalarTest) {
  EXPECT_SCALAR_VALUE_NULL("{}"s, "$"s);
  EXPECT_SCALAR_VALUE_NULL(
      "{\"fuu\": {\"bar\": 1}}"s, "$.fuu"s); // Null b/c value is complex type
  EXPECT_SCALAR_VALUE_EQ("{\"fuu\": 1}"s, "$.fuu"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ("{\"fuu\": 1}"s, "$[fuu]"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ("{\"fuu\": 1}"s, "$[\"fuu\"]"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ(
      "{\"ab\\\"cd\\\"ef\": 2}"s, "$[\"ab\\\"cd\\\"ef\"]"s, "2"s);
  EXPECT_SCALAR_VALUE_NULL("{\"fuu\": null}"s, "$.fuu"s);
  EXPECT_SCALAR_VALUE_NULL("{\"fuu\": 1}"s, "$.bar"s);
  EXPECT_SCALAR_VALUE_EQ(
      "{\"fuu\": [\"\\u0001\"]}"s,
      "$.fuu[0]"s,
      "\001"s); // Test escaped characters
  EXPECT_SCALAR_VALUE_EQ("{\"fuu\": 1, \"bar\": \"abc\"}"s, "$.bar"s, "abc"s);
  EXPECT_SCALAR_VALUE_EQ("{\"fuu\": [0.1, 1, 2]}"s, "$.fuu[0]"s, "0.1"s);
  EXPECT_SCALAR_VALUE_NULL(
      "{\"fuu\": [0, [100, 101], 2]}"s,
      "$.fuu[1]"s); // Null b/c value is complex type
  EXPECT_SCALAR_VALUE_EQ(
      "{\"fuu\": [0, [100, 101], 2]}"s, "$.fuu[1][1]"s, "101"s);
  EXPECT_SCALAR_VALUE_EQ(
      "{\"fuu\": [0, {\"bar\": {\"key\" : [\"value\"]}}, 2]}"s,
      "$.fuu[1].bar.key[0]"s,
      "value"s);

  // Test non-object extraction
  EXPECT_SCALAR_VALUE_EQ("[0, 1, 2]"s, "$[0]"s, "0"s);
  EXPECT_SCALAR_VALUE_EQ("\"abc\""s, "$"s, "abc"s);
  EXPECT_SCALAR_VALUE_EQ("123"s, "$"s, "123"s);
  EXPECT_SCALAR_VALUE_NULL("null"s, "$"s);

  // Test numeric path expression matches arrays and objects
  EXPECT_SCALAR_VALUE_EQ("[0, 1, 2]"s, "$.1"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ("[0, 1, 2]"s, "$[1]"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ("[0, 1, 2]"s, "$[\"1\"]"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ("{\"0\" : 0, \"1\" : 1, \"2\" : 2 }"s, "$.1"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ("{\"0\" : 0, \"1\" : 1, \"2\" : 2 }"s, "$[1]"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ(
      "{\"0\" : 0, \"1\" : 1, \"2\" : 2 }"s, "$[\"1\"]"s, "1"s);

  // Test fields starting with a digit
  EXPECT_SCALAR_VALUE_EQ(
      "{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }"s, "$.30day"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ(
      "{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }"s, "$[30day]"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ(
      "{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }"s, "$[\"30day\"]"s, "1"s);
}

TEST(JsonExtractorTest, fullJsonValueTest) {
  EXPECT_JSON_VALUE_EQ("{}"s, "$"s, "{}"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": {\"bar\": 1}}"s, "$.fuu"s, "{\"bar\":1}"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": 1}"s, "$.fuu"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": 1}"s, "$[fuu]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": 1}"s, "$[\"fuu\"]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": null}"s, "$.fuu"s, "null"s);
  EXPECT_JSON_VALUE_NULL("{\"fuu\": 1}"s, "$.bar"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": [\"\\u0001\"]}"s,
      "$.fuu[0]"s,
      "\"\\u0001\""s); // Test escaped characters
  EXPECT_JSON_VALUE_EQ("{\"fuu\": 1, \"bar\": \"abc\"}"s, "$.bar"s, "\"abc\""s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": [0.1, 1, 2]}"s, "$.fuu[0]"s, "0.1"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": [0, [100, 101], 2]}"s, "$.fuu[1]"s, "[100,101]"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": [0, [100, 101], 2]}"s, "$.fuu[1][1]"s, "101"s);

  // Test non-object extraction
  EXPECT_JSON_VALUE_EQ("[0, 1, 2]"s, "$[0]"s, "0"s);
  EXPECT_JSON_VALUE_EQ("\"abc\""s, "$"s, "\"abc\""s);
  EXPECT_JSON_VALUE_EQ("123"s, "$"s, "123"s);
  EXPECT_JSON_VALUE_EQ("null"s, "$"s, "null"s);

  // Test extraction using bracket json path
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": {\"bar\": 1}}"s, "$[\"fuu\"]"s, "{\"bar\":1}"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": {\"bar\": 1}}"s, "$[\"fuu\"][\"bar\"]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": 1}"s, "$[\"fuu\"]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": null}"s, "$[\"fuu\"]"s, "null"s);
  EXPECT_JSON_VALUE_NULL("{\"fuu\": 1}"s, "$[\"bar\"]"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": [\"\\u0001\"]}"s,
      "$[\"fuu\"][0]"s,
      "\"\\u0001\""s); // Test escaped characters
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": 1, \"bar\": \"abc\"}"s, "$[\"bar\"]"s, "\"abc\""s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": [0.1, 1, 2]}"s, "$[\"fuu\"][0]"s, "0.1"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": [0, [100, 101], 2]}"s, "$[\"fuu\"][1]"s, "[100,101]"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": [0, [100, 101], 2]}"s, "$[\"fuu\"][1][1]"s, "101"s);

  // Test extraction using bracket json path with special json characters in
  // path
  EXPECT_JSON_VALUE_EQ(
      "{\"@$fuu\": {\".b.ar\": 1}}"s, "$[\"@$fuu\"]"s, "{\".b.ar\":1}"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu..\": 1}"s, "$[\"fuu..\"]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"fu*u\": null}"s, "$[\"fu*u\"]"s, "null"s);
  EXPECT_JSON_VALUE_NULL("{\",fuu\": 1}"s, "$[\"bar\"]"s);
  EXPECT_JSON_VALUE_EQ(
      "{\",fuu\": [\"\\u0001\"]}"s,
      "$[\",fuu\"][0]"s,
      "\"\\u0001\""s); // Test escaped characters
  EXPECT_JSON_VALUE_EQ(
      "{\":fu:u:\": 1, \":b:ar:\": \"abc\"}"s, "$[\":b:ar:\"]"s, "\"abc\""s);
  EXPECT_JSON_VALUE_EQ(
      "{\"?()fuu\": [0.1, 1, 2]}"s, "$[\"?()fuu\"][0]"s, "0.1"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"f?uu\": [0, [100, 101], 2]}"s, "$[\"f?uu\"][1]"s, "[100,101]"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu()\": [0, [100, 101], 2]}"s, "$[\"fuu()\"][1][1]"s, "101"s);

  // Test extraction using mix of bracket and dot notation json path
  EXPECT_JSON_VALUE_EQ("{\"fuu\": {\"bar\": 1}}"s, "$[\"fuu\"].bar"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": {\"bar\": 1}}"s, "$.fuu[\"bar\"]"s, "1"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": [\"\\u0001\"]}"s,
      "$[\"fuu\"][0]"s,
      "\"\\u0001\""s); // Test escaped characters
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": [\"\\u0001\"]}"s,
      "$.fuu[0]"s,
      "\"\\u0001\""s); // Test escaped characters

  // Test extraction using  mix of bracket and dot notation json path with
  // special json characters in path
  EXPECT_JSON_VALUE_EQ("{\"@$fuu\": {\"bar\": 1}}"s, "$[\"@$fuu\"].bar"s, "1"s);
  EXPECT_JSON_VALUE_EQ(
      "{\",fuu\": {\"bar\": [\"\\u0001\"]}}"s,
      "$[\",fuu\"].bar[0]"s,
      "\"\\u0001\""s); // Test escaped characters

  // Test numeric path expression matches arrays and objects
  EXPECT_JSON_VALUE_EQ("[0, 1, 2]"s, "$.1"s, "1"s);
  EXPECT_JSON_VALUE_EQ("[0, 1, 2]"s, "$[1]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("[0, 1, 2]"s, "$[\"1\"]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"0\" : 0, \"1\" : 1, \"2\" : 2 }"s, "$.1"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"0\" : 0, \"1\" : 1, \"2\" : 2 }"s, "$[1]"s, "1"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"0\" : 0, \"1\" : 1, \"2\" : 2 }"s, "$[\"1\"]"s, "1"s);

  // Test fields starting with a digit
  EXPECT_JSON_VALUE_EQ(
      "{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }"s, "$.30day"s, "1"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }"s, "$[30day]"s, "1"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }"s, "$[\"30day\"]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"a\\\\b\": 4}"s, "$[\"a\\\\b\"]"s, "4"s);
  EXPECT_JSON_VALUE_NULL("{\"fuu\" : null}"s, "$.a.b"s);
}

TEST(JsonExtractorTest, invalidJsonPathTest) {
  EXPECT_THROW_INVALID_ARGUMENT(""s, ""s);
  EXPECT_THROW_INVALID_ARGUMENT("{}"s, "$.bar[2][-1]"s);
  EXPECT_THROW_INVALID_ARGUMENT("{}"s, "$.fuu..bar"s);
  EXPECT_THROW_INVALID_ARGUMENT("{}"s, "$."s);
  EXPECT_THROW_INVALID_ARGUMENT(""s, "$$"s);
  EXPECT_THROW_INVALID_ARGUMENT(""s, " "s);
  EXPECT_THROW_INVALID_ARGUMENT(""s, "."s);
  EXPECT_THROW_INVALID_ARGUMENT(
      "{ \"store\": { \"book\": [{ \"title\": \"title\" }] } }"s,
      "$.store.book["s);
}

TEST(JsonExtractorTest, reextractJsonTest) {
  std::string json = R"DELIM(
      {"store":
        {"fruit":[
          {"weight":8, "type":"apple"},
          {"weight":9, "type":"pear"}],
         "basket":[[1,2,{"b":"y","a":"x"}],[3,4],[5,6]],
         "book":[
            {"author":"Nigel Rees",
             "title":"Sayings of the Century",
             "category":"reference",
             "price":8.95},
            {"author":"Herman Melville",
             "title":"Moby Dick",
             "category":"fiction",
             "price":8.99,
             "isbn":"0-553-21311-3"},
            {"author":"J. R. R. Tolkien",
             "title":"The Lord of the Rings",
             "category":"fiction",
             "reader":[
                {"age":25,
                 "name":"bob"},
                {"age":26,
                 "name":"jack"}],
             "price":22.99,
             "isbn":"0-395-19395-8"}],
          "bicycle":{"price":19.95, "color":"red"}},
        "e mail":"amy@only_for_json_udf_test.net",
        "owner":"amy"})DELIM";
  auto originalJsonObj = jsonExtract(json, "$");
  // extract the same json json by giving the root path
  auto reExtractedJsonObj = jsonExtract(originalJsonObj.value(), "$");
  ASSERT_TRUE(reExtractedJsonObj.hasValue());
  // expect the re-extracted json object to be the same as the original jsonObj
  EXPECT_EQ(originalJsonObj.value(), reExtractedJsonObj.value());
}

TEST(JsonExtractorTest, jsonMultipleExtractsTest) {
  std::string json = R"DELIM(
      {"store":
        {"fruit":[
          {"weight":8, "type":"apple"},
          {"weight":9, "type":"pear"}],
         "basket":[[1,2,{"b":"y","a":"x"}],[3,4],[5,6]],
         "book":[
            {"author":"Nigel Rees",
             "title":"Sayings of the Century",
             "category":"reference",
             "price":8.95},
            {"author":"Herman Melville",
             "title":"Moby Dick",
             "category":"fiction",
             "price":8.99,
             "isbn":"0-553-21311-3"},
            {"author":"J. R. R. Tolkien",
             "title":"The Lord of the Rings",
             "category":"fiction",
             "reader":[
                {"age":25,
                 "name":"bob"},
                {"age":26,
                 "name":"jack"}],
             "price":22.99,
             "isbn":"0-395-19395-8"}],
          "bicycle":{"price":19.95, "color":"red"}},
        "e mail":"amy@only_for_json_udf_test.net",
        "owner":"amy"})DELIM";
  auto extract1 = jsonExtract(json, "$.store");
  ASSERT_TRUE(extract1.hasValue());
  auto extract2 = jsonExtract(extract1.value(), "$.fruit");
  ASSERT_TRUE(extract2.hasValue());
  EXPECT_EQ(jsonExtract(json, "$.store.fruit").value(), extract2.value());
}
