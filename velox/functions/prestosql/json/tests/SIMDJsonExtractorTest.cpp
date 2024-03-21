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

#include "velox/functions/prestosql/json/SIMDJsonExtractor.h"
#include <optional>
#include <string>

#include "folly/json.h"
#include "gtest/gtest.h"
#include "velox/common/base/VeloxException.h"

namespace {
using facebook::velox::VeloxUserError;
using facebook::velox::functions::simdJsonExtract;

class SIMDJsonExtractorTest : public testing::Test {
 public:
  void expectThrowInvalidArgument(
      const std::string& json,
      const std::string& path) {
    EXPECT_THROW(testExtract(json, path, std::nullopt), VeloxUserError);
  }

  void testExtract(
      const std::string& json,
      const std::string& path,
      const std::string& expected) {
    testExtract(json, path, std::vector<std::string>{expected});
  }

  void testExtract(
      const std::string& json,
      const std::string& path,
      const std::optional<std::vector<std::string>>& expected) {
    std::vector<std::string> res;
    auto consumer = [&res](auto& v) {
      SIMDJSON_ASSIGN_OR_RAISE(auto jsonStr, simdjson::to_json_string(v));
      res.emplace_back(jsonStr);
      return simdjson::SUCCESS;
    };

    EXPECT_EQ(simdJsonExtract(json, path, consumer), simdjson::SUCCESS)
        << "with json " << json << " and path " << path;

    if (!expected) {
      EXPECT_EQ(0, res.size());
      return;
    }

    EXPECT_EQ(expected->size(), res.size())
        << "with json " << json << " and path " << path;
    for (int i = 0; i < res.size(); i++) {
      EXPECT_EQ(folly::parseJson(expected->at(i)), folly::parseJson(res.at(i)))
          << "Encountered different values at position " << i << " with json "
          << json << " and path " << path;
    }
  }

  void testExtractScalar(
      const std::string& json,
      const std::string& path,
      const std::optional<std::string>& expected) {
    bool resultPopulated = false;
    std::optional<std::string> actual;
    auto consumer = [&actual, &resultPopulated](auto& v) {
      if (resultPopulated) {
        // We expect a single value, if consumer gets called multiple times,
        // e.g. the path contains [*], return null.
        actual = std::nullopt;
        return simdjson::SUCCESS;
      }

      resultPopulated = true;

      SIMDJSON_ASSIGN_OR_RAISE(auto vtype, v.type());
      switch (vtype) {
        case simdjson::ondemand::json_type::boolean: {
          SIMDJSON_ASSIGN_OR_RAISE(bool vbool, v.get_bool());
          actual = vbool ? "true" : "false";
          break;
        }
        case simdjson::ondemand::json_type::string: {
          SIMDJSON_ASSIGN_OR_RAISE(actual, v.get_string());
          break;
        }
        case simdjson::ondemand::json_type::object:
        case simdjson::ondemand::json_type::array:
        case simdjson::ondemand::json_type::null:
          // Do nothing.
          break;
        default: {
          SIMDJSON_ASSIGN_OR_RAISE(actual, simdjson::to_json_string(v));
        }
      }
      return simdjson::SUCCESS;
    };

    EXPECT_EQ(simdJsonExtract(json, path, consumer), simdjson::SUCCESS)
        << "with json " << json << " and path " << path;

    EXPECT_EQ(expected, actual) << "with json " << json << " and path " << path;
  }

 private:
  simdjson::ondemand::parser parser_;
};

TEST_F(SIMDJsonExtractorTest, generalJsonTest) {
  std::string json = R"DELIM(
      {"store":
          {"fruit":[
          {"weight":8, "type":"apple"},
          {"weight":9, "type":"pear"}],
          "basket":[[1,2,{"b":"y","a":"x"}],[3,4],[5,6]],
          "book":[
              {"author":"Nigel Rees",
              "title":"ayings of the Century",
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
  std::replace(json.begin(), json.end(), '\'', '\"');
  testExtract(json, "$.store.fruit[0].weight", "8");
  testExtract(json, "$.store.fruit[1].weight", "9");
  testExtract(json, "$.store.fruit[2].weight", std::nullopt);
  testExtract(
      json, "$.store.fruit[*].weight", std::vector<std::string>{"8", "9"});
  testExtract(
      json,
      "$.store.fruit[*].type",
      std::vector<std::string>{"\"apple\"", "\"pear\""});
  testExtract(json, "$.store.book[0].price", "8.95");
  testExtract(json, "$.store.book[2].category", "\"fiction\"");
  testExtract(json, "$.store.basket[1]", "[3,4]");
  testExtract(json, "$.store.basket[0]", "[1,2,{\"a\":\"x\",\"b\":\"y\"}]");
  testExtract(json, "$.store.baskets[1]", std::nullopt);
  testExtract(json, "$[\"e mail\"]", "\"amy@only_for_json_udf_test.net\"");
  testExtract(json, "$.owner", "\"amy\"");

  testExtract("[[1.1,[2.1,2.2]],2,{\"a\":\"b\"}]", "$[0][1][1]", "2.2");

  json = "[1,2,{\"a\":\"b\"}]";
  testExtract(json, "$[1]", "2");
  testExtract(json, "$[2]", "{\"a\":\"b\"}");
  testExtract(json, "$[3]", std::nullopt);

  json = "[{\"a\":\"b\"}]";
  testExtract(json, "$[0]", "{\"a\":\"b\"}");
  testExtract(json, "$[2]", std::nullopt);

  testExtract("{\"a\":\"b\"}", " $ ", "{\"a\":\"b\"}");

  json =
      "[[{\"key\": 1, \"value\": 2},"
      "{\"key\": 2, \"value\": 4}],"
      "[{\"key\": 3, \"value\": 6},"
      "{\"key\": 4, \"value\": 8},"
      "{\"key\": 5, \"value\": 10}]]";
  testExtract(
      json,
      "$[*]",
      std::vector<std::string>{
          "[{\"key\": 1, \"value\": 2},"
          "{\"key\": 2, \"value\": 4}]",
          "[{\"key\": 3, \"value\": 6},"
          "{\"key\": 4, \"value\": 8},"
          "{\"key\": 5, \"value\": 10}]"});
  testExtract(
      json,
      "$[*][*]",
      std::vector<std::string>{
          "{\"key\": 1, \"value\": 2}",
          "{\"key\": 2, \"value\": 4}",
          "{\"key\": 3, \"value\": 6}",
          "{\"key\": 4, \"value\": 8}",
          "{\"key\": 5, \"value\": 10}"});
  testExtract(
      json, "$[*][*].key", std::vector<std::string>{"1", "2", "3", "4", "5"});
  testExtract(
      json,
      "$[*][0]",
      std::vector<std::string>{
          "{\"key\":1,\"value\":2}", "{\"key\":3,\"value\":6}"});
  testExtract(json, "$[*][2]", "{\"key\":5,\"value\":10}");

  json = " [ [1.1,[2.1,2.2]],2, {\"a\": \"b\"}]";
  testExtract(json, " $[0][1][1]", "2.2");
  expectThrowInvalidArgument(json, "  \t\n ");
}

// Test compatibility with Presto
// Reference: from https://github.com/prestodb/presto
// presto-main/src/test/java/com/facebook/presto/operator/scalar/TestJsonExtract.java
TEST_F(SIMDJsonExtractorTest, scalarValueTest) {
  testExtractScalar("123", "$", "123");
  testExtractScalar("-1", "$", "-1");
  testExtractScalar("\"abc\"", "$", "abc");
  testExtractScalar("\"\"", "$", "");
  testExtractScalar("null", "$", std::nullopt);

  // Test character escaped values
  testExtractScalar("\"ab\\u0001c\"", "$", "ab\001c");
  testExtractScalar("\"ab\\u0002c\"", "$", "ab\002c");

  // Complex types should return null
  testExtractScalar("[1, 2, 3]", "$", std::nullopt);
  testExtractScalar("{\"a\": 1}", "$", std::nullopt);
}

TEST_F(SIMDJsonExtractorTest, jsonValueTest) {
  // Check scalar values
  testExtract("123", "$", "123");
  testExtract("-1", "$", "-1");
  testExtract("0.01", "$", "0.01");
  testExtract("\"abc\"", "$", "\"abc\"");
  testExtract("\"\"", "$", "\"\"");
  testExtract("null", "$", "null");

  // Test character escaped values
  testExtract("\"ab\\u0001c\"", "$", "\"ab\\u0001c\"");
  testExtract("\"ab\\u0002c\"", "$", "\"ab\\u0002c\"");

  // Complex types should return json values
  testExtract("[1, 2, 3]", "$", "[1,2,3]");
  testExtract("{\"a\": 1}", "$", "{\"a\":1}");
}

TEST_F(SIMDJsonExtractorTest, arrayJsonValueTest) {
  testExtract("[]", "$[0]", std::nullopt);
  testExtract("[1, 2, 3]", "$[0]", "1");
  testExtract("[1, 2]", "$[1]", "2");
  testExtract("[1, null]", "$[1]", "null");
  // Out of bounds
  testExtract("[1]", "$[1]", std::nullopt);
  // Check skipping complex structures
  testExtract("[{\"a\": 1}, 2, 3]", "$[1]", "2");
}

TEST_F(SIMDJsonExtractorTest, objectJsonValueTest) {
  testExtractScalar("{}", "$.fuu", std::nullopt);
  testExtractScalar("{\"a\": 1}", "$.fuu", std::nullopt);
  testExtractScalar("{\"fuu\": 1}", "$.fuu", "1");
  testExtractScalar("{\"a\": 0, \"fuu\": 1}", "$.fuu", "1");
  // Check skipping complex structures
  testExtractScalar("{\"a\": [1, 2, 3], \"fuu\": 1}", "$.fuu", "1");
}

TEST_F(SIMDJsonExtractorTest, fullScalarTest) {
  testExtractScalar("{}", "$", std::nullopt);
  testExtractScalar(
      "{\"fuu\": {\"bar\": 1}}",
      "$.fuu",
      std::nullopt); // Null b/c value is complex
  testExtractScalar("{\"fuu\": 1}", "$.fuu", "1");
  testExtractScalar("{\"fuu\": 1}", "$[fuu]", "1");
  testExtractScalar("{\"fuu\": 1}", "$[\"fuu\"]", "1");
  testExtractScalar("{\"ab\\\"cd\\\"ef\": 2}", "$[\"ab\\\"cd\\\"ef\"]", "2");
  testExtractScalar("{\"fuu\": null}", "$.fuu", std::nullopt);
  testExtractScalar("{\"fuu\": 1}", "$.bar", std::nullopt);
  testExtractScalar(
      "{\"fuu\": [\"\\u0001\"]}",
      "$.fuu[0]",
      "\001"); // Test escaped characters
  testExtractScalar("{\"fuu\": 1, \"bar\": \"abc\"}", "$.bar", "abc");
  testExtractScalar("{\"fuu\": [0.1, 1, 2]}", "$.fuu[0]", "0.1");
  testExtractScalar(
      "{\"fuu\": [0, [100, 101], 2]}",
      "$.fuu[1]",
      std::nullopt); // Null b/c value is complex type
  testExtractScalar("{\"fuu\": [0, [100, 101], 2]}", "$.fuu[1][1]", "101");
  testExtractScalar(
      "{\"fuu\": [0, {\"bar\": {\"key\" : [\"value\"]}}, 2]}",
      "$.fuu[1].bar.key[0]",
      "value");

  // Test non-object extraction
  testExtractScalar("[0, 1, 2]", "$[0]", "0");
  testExtractScalar("\"abc\"", "$", "abc");
  testExtractScalar("123", "$", "123");
  testExtractScalar("null", "$", std::nullopt);

  // Test numeric path expression matches arrays and objects
  testExtractScalar("[0, 1, 2]", "$.1", "1");
  testExtractScalar("[0, 1, 2]", "$[1]", "1");
  testExtractScalar("[0, 1, 2]", "$[\"1\"]", "1");
  testExtractScalar("{\"0\" : 0, \"1\" : 1, \"2\" : 2 }", "$.1", "1");
  testExtractScalar("{\"0\" : 0, \"1\" : 1, \"2\" : 2 }", "$[1]", "1");
  testExtractScalar("{\"0\" : 0, \"1\" : 1, \"2\" : 2 }", "$[\"1\"]", "1");

  // Test fields starting with a digit
  testExtractScalar(
      "{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }", "$.30day", "1");
  testExtractScalar(
      "{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }", "$[30day]", "1");
  testExtractScalar(
      "{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }", "$[\"30day\"]", "1");
}

TEST_F(SIMDJsonExtractorTest, fullJsonValueTest) {
  testExtract("{}", "$", "{}");
  testExtract("{\"fuu\": {\"bar\": 1}}", "$.fuu", "{\"bar\":1}");
  testExtract("{\"fuu\": 1}", "$.fuu", "1");
  testExtract("{\"fuu\": 1}", "$[fuu]", "1");
  testExtract("{\"fuu\": 1}", "$[\"fuu\"]", "1");
  testExtract("{\"fuu\": null}", "$.fuu", "null");
  testExtract("{\"fuu\": 1}", "$.bar", std::nullopt);
  testExtract(
      "{\"fuu\": [\"\\u0001\"]}",
      "$.fuu[0]",
      "\"\\u0001\""); // Test escaped characters
  testExtract("{\"fuu\": 1, \"bar\": \"abc\"}", "$.bar", "\"abc\"");
  testExtract("{\"fuu\": [0.1, 1, 2]}", "$.fuu[0]", "0.1");
  testExtract("{\"fuu\": [0, [100, 101], 2]}", "$.fuu[1]", "[100,101]");
  testExtract("{\"fuu\": [0, [100, 101], 2]}", "$.fuu[1][1]", "101");

  // Test non-object extraction
  testExtract("[0, 1, 2]", "$[0]", "0");
  testExtract("\"abc\"", "$", "\"abc\"");
  testExtract("123", "$", "123");
  testExtract("null", "$", "null");

  // Test extraction using bracket json path
  testExtract("{\"fuu\": {\"bar\": 1}}", "$[\"fuu\"]", "{\"bar\":1}");
  testExtract("{\"fuu\": {\"bar\": 1}}", "$[\"fuu\"][\"bar\"]", "1");
  testExtract("{\"fuu\": 1}", "$[\"fuu\"]", "1");
  testExtract("{\"fuu\": null}", "$[\"fuu\"]", "null");
  testExtract("{\"fuu\": 1}", "$[\"bar\"]", std::nullopt);
  testExtract(
      "{\"fuu\": [\"\\u0001\"]}",
      "$[\"fuu\"][0]",
      "\"\\u0001\""); // Test escaped characters
  testExtract("{\"fuu\": 1, \"bar\": \"abc\"}", "$[\"bar\"]", "\"abc\"");
  testExtract("{\"fuu\": [0.1, 1, 2]}", "$[\"fuu\"][0]", "0.1");
  testExtract("{\"fuu\": [0, [100, 101], 2]}", "$[\"fuu\"][1]", "[100,101]");
  testExtract("{\"fuu\": [0, [100, 101], 2]}", "$[\"fuu\"][1][1]", "101");

  // Test extraction using bracket json path with special json characters in
  // path
  testExtract("{\"@$fuu\": {\".b.ar\": 1}}", "$[\"@$fuu\"]", "{\".b.ar\":1}");
  testExtract("{\"fuu..\": 1}", "$[\"fuu..\"]", "1");
  testExtract("{\"fu*u\": null}", "$[\"fu*u\"]", "null");
  testExtract("{\",fuu\": 1}", "$[\"bar\"]", std::nullopt);
  testExtract(
      "{\",fuu\": [\"\\u0001\"]}",
      "$[\",fuu\"][0]",
      "\"\\u0001\""); // Test escaped characters
  testExtract(
      "{\":fu:u:\": 1, \":b:ar:\": \"abc\"}", "$[\":b:ar:\"]", "\"abc\"");
  testExtract("{\"?()fuu\": [0.1, 1, 2]}", "$[\"?()fuu\"][0]", "0.1");
  testExtract("{\"f?uu\": [0, [100, 101], 2]}", "$[\"f?uu\"][1]", "[100,101]");
  testExtract("{\"fuu()\": [0, [100, 101], 2]}", "$[\"fuu()\"][1][1]", "101");

  // Test extraction using mix of bracket and dot notation json path
  testExtract("{\"fuu\": {\"bar\": 1}}", "$[\"fuu\"].bar", "1");
  testExtract("{\"fuu\": {\"bar\": 1}}", "$.fuu[\"bar\"]", "1");
  testExtract(
      "{\"fuu\": [\"\\u0001\"]}",
      "$[\"fuu\"][0]",
      "\"\\u0001\""); // Test escaped characters
  testExtract(
      "{\"fuu\": [\"\\u0001\"]}",
      "$.fuu[0]",
      "\"\\u0001\""); // Test escaped characters

  // Test extraction using  mix of bracket and dot notation json path with
  // special json characters in path
  testExtract("{\"@$fuu\": {\"bar\": 1}}", "$[\"@$fuu\"].bar", "1");
  testExtract(
      "{\",fuu\": {\"bar\": [\"\\u0001\"]}}",
      "$[\",fuu\"].bar[0]",
      "\"\\u0001\""); // Test escaped characters

  // Test numeric path expression matches arrays and objects
  testExtract("[0, 1, 2]", "$.1", "1");
  testExtract("[0, 1, 2]", "$[1]", "1");
  testExtract("[0, 1, 2]", "$[\"1\"]", "1");
  testExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2 }", "$.1", "1");
  testExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2 }", "$[1]", "1");
  testExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2 }", "$[\"1\"]", "1");

  // Test fields starting with a digit
  testExtract("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }", "$.30day", "1");
  testExtract(
      "{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }", "$[30day]", "1");
  testExtract(
      "{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }", "$[\"30day\"]", "1");
  testExtract("{\"a\\\\b\": 4}", "$[\"a\\\\b\"]", "4");
  testExtract("{\"fuu\" : null}", "$.a.b", std::nullopt);
}

TEST_F(SIMDJsonExtractorTest, invalidJsonPathTest) {
  expectThrowInvalidArgument("", "");
  expectThrowInvalidArgument("{}", "$.bar[2][-1]");
  expectThrowInvalidArgument("{}", "$.fuu..bar");
  expectThrowInvalidArgument("{}", "$.");
  expectThrowInvalidArgument("", "$$");
  expectThrowInvalidArgument("", " ");
  expectThrowInvalidArgument("", ".");
  expectThrowInvalidArgument(
      "{ \"store\": { \"book\": [{ \"title\": \"title\" }] } }",
      "$.store.book[");
}

TEST_F(SIMDJsonExtractorTest, reextractJsonTest) {
  std::string json = R"DELIM(
      {"store":
        {"fruit":[
          {"weight":8, "type":"apple"},
          {"weight":9, "type":"pear"}],
         "basket":[[1,2,{"b":"y","a":"x"}],[3,4],[5,6]],
         "book":[
            {"author":"Nigel Rees",
             "title":"ayings of the Century",
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
  std::string extract;
  std::string ret;
  auto consumer = [&ret](auto& v) {
    SIMDJSON_ASSIGN_OR_RAISE(ret, simdjson::to_json_string(v));
    return simdjson::SUCCESS;
  };

  simdJsonExtract(json, "$", consumer);
  // extract the same json json by giving the root path
  extract.swap(ret);
  simdJsonExtract(extract, "$", consumer);
  // expect the re-extracted json object to be the same as the original
  EXPECT_EQ(ret, extract);
}

TEST_F(SIMDJsonExtractorTest, jsonMultipleExtractsTest) {
  std::string json = R"DELIM(
      {"store":
        {"fruit":[
          {"weight":8, "type":"apple"},
          {"weight":9, "type":"pear"}],
         "basket":[[1,2,{"b":"y","a":"x"}],[3,4],[5,6]],
         "book":[
            {"author":"Nigel Rees",
             "title":"ayings of the Century",
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
  std::string extract1;
  std::string extract2;
  std::string ret;
  auto consumer = [&ret](auto& v) {
    SIMDJSON_ASSIGN_OR_RAISE(ret, simdjson::to_json_string(v));
    return simdjson::SUCCESS;
  };

  simdJsonExtract(json, "$.store", consumer);
  extract1.swap(ret);
  simdJsonExtract(extract1, "$.fruit", consumer);
  extract2.swap(ret);
  simdJsonExtract(json, "$.store.fruit", consumer);
  EXPECT_EQ(ret, extract2);
}

TEST_F(SIMDJsonExtractorTest, invalidJson) {
  // No-op consumer.
  auto consumer = [](auto& /* unused */) { return simdjson::SUCCESS; };

  // Object key is invalid.
  std::string json = "{\"foo: \"bar\"}";
  EXPECT_NE(simdJsonExtract(json, "$.foo", consumer), simdjson::SUCCESS);
  // Object value is invalid.
  json = "{\"foo\": \"bar}";
  EXPECT_NE(simdJsonExtract(json, "$.foo", consumer), simdjson::SUCCESS);
  // Value in array is invalid.
  // Inner object is invalid.
  json = "{\"foo\": [\"bar\", \"baz]}";
  EXPECT_NE(simdJsonExtract(json, "$.foo[0]", consumer), simdjson::SUCCESS);
}
} // namespace
