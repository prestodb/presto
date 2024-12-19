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
#include <stdint.h>
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/type/Type.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class GetJsonObjectTest : public SparkFunctionBaseTest {
 protected:
  std::optional<std::string> getJsonObject(
      const std::string& json,
      const std::string& jsonPath) {
    return evaluateOnce<std::string>(
        "get_json_object(c0, c1)",
        std::optional<std::string>(json),
        std::optional<std::string>(jsonPath));
  }
};

TEST_F(GetJsonObjectTest, basic) {
  EXPECT_EQ(getJsonObject(R"({"hello": "3.5"})", "$.hello"), "3.5");
  EXPECT_EQ(getJsonObject(R"({"hello": 3.5})", "$.hello"), "3.5");
  EXPECT_EQ(getJsonObject(R"({"hello": 292222730})", "$.hello"), "292222730");
  EXPECT_EQ(getJsonObject(R"({"hello": -292222730})", "$.hello"), "-292222730");
  EXPECT_EQ(getJsonObject(R"({"my": {"hello": 3.5}})", "$.my.hello"), "3.5");
  EXPECT_EQ(getJsonObject(R"({"my": {"hello": true}})", "$.my.hello"), "true");
  EXPECT_EQ(getJsonObject(R"({"hello": ""})", "$.hello"), "");
  EXPECT_EQ(
      "0.0215434648799772",
      getJsonObject(R"({"score":0.0215434648799772})", "$.score"));
  // Returns input json if json path is "$".
  EXPECT_EQ(
      getJsonObject(R"({"name": "Alice", "age": 5, "id": "001"})", "$"),
      R"({"name": "Alice", "age": 5, "id": "001"})");
  EXPECT_EQ(
      getJsonObject(R"({"name": "Alice", "age": 5, "id": "001"})", "$.age"),
      "5");
  EXPECT_EQ(
      getJsonObject(R"({"name": "Alice", "age": 5, "id": "001"})", "$.id"),
      "001");
  EXPECT_EQ(
      getJsonObject(
          R"([{"my": {"info": {"name": "Alice", "age": "5", "id": "001"}}}, {"other": "v1"}])",
          "$[0]['my']['info']['age']"),
      "5");
  EXPECT_EQ(
      getJsonObject(
          R"([{"my": {"info": {"name": "Alice", "age": "5", "id": "001"}}}, {"other": "v1"}])",
          "$[0].my.info.age"),
      "5");

  // Json object as result.
  EXPECT_EQ(
      getJsonObject(
          R"({"my": {"info": {"name": "Alice", "age": "5", "id": "001"}}})",
          "$.my.info"),
      R"({"name": "Alice", "age": "5", "id": "001"})");
  EXPECT_EQ(
      getJsonObject(
          R"({"my": {"info": {"name": "Alice", "age": "5", "id": "001"}}})",
          "$['my']['info']"),
      R"({"name": "Alice", "age": "5", "id": "001"})");

  // Array as result.
  EXPECT_EQ(
      getJsonObject(
          R"([{"my": {"info": {"name": "Alice"}}}, {"other": ["v1", "v2"]}])",
          "$[1].other"),
      R"(["v1", "v2"])");
  // Array element as result.
  EXPECT_EQ(
      getJsonObject(
          R"([{"my": {"info": {"name": "Alice"}}}, {"other": ["v1", "v2"]}])",
          "$[1].other[0]"),
      "v1");
  EXPECT_EQ(
      getJsonObject(
          R"([{"my": {"info": {"name": "Alice"}}}, {"other": ["v1", "v2"]}])",
          "$[1].other[1]"),
      "v2");
}

TEST_F(GetJsonObjectTest, nullResult) {
  // Field not found.
  EXPECT_EQ(getJsonObject(R"({"hello": "3.5"})", "$.hi"), std::nullopt);

  // Illegal json.
  EXPECT_EQ(getJsonObject(R"({"hello"-3.5})", "$.hello"), std::nullopt);
  EXPECT_EQ(getJsonObject(R"({"a": bad, "b": string})", "$.a"), std::nullopt);

  // Illegal json path.
  EXPECT_EQ(getJsonObject(R"({"hello": "3.5"})", "$hello"), std::nullopt);
  EXPECT_EQ(getJsonObject(R"({"hello": "3.5"})", "$."), std::nullopt);
  // The first char is not '$'.
  EXPECT_EQ(getJsonObject(R"({"hello": "3.5"})", ".hello"), std::nullopt);
  // Constains '$' not in the first position.
  EXPECT_EQ(getJsonObject(R"({"hello": "3.5"})", "$.$hello"), std::nullopt);

  // Invalid ending character.
  EXPECT_EQ(
      getJsonObject(
          R"([{"my": {"info": {"name": "Alice"quoted""}}}, {"other": ["v1", "v2"]}])",
          "$[0].my.info.name"),
      std::nullopt);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
