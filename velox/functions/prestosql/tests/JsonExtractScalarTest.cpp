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
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;

class JsonExtractScalarTest : public functions::test::FunctionBaseTest {};

TEST_F(JsonExtractScalarTest, constJsonPath) {
  std::vector<std::string> testJsonData = {
      "{\"key1\":{\"key3\": 456}, \"key2\": 123}",
      "{\"key1\":\"value1\", \"key2\":\"value2\"}"};
  auto result = evaluate<FlatVector<StringView>>(
      "json_extract_scalar(c0, '$.key1.key3')",
      makeRowVector({makeFlatVector(testJsonData)}));
  EXPECT_EQ(result->valueAt(0).str(), "456") << "at 0";
  EXPECT_TRUE(result->isNullAt(1)) << "at 1";
}

TEST_F(JsonExtractScalarTest, nonConstJsonPath) {
  std::vector<std::string> testJsonData = {
      "{\"key1\":\"value1\", \"key2\":\"value2\"}",
      "{\"key1\":\"value1\", \"key2\": 123}",
      "{\"key1\":\"value1\", \"key2\": [1, 2, 3]}",
      "{\"key1\":{\"key3\": 456}, \"key2\": 123}",
      "{\"key1\":{\"key3\": 456}, \"key2\": 123}"};
  std::vector<std::string> testJsonPathData = {
      "$.key1", "$.key3", "$.key2", "$.key1.key3", "$.key1"};

  auto row = makeRowVector(
      {makeFlatVector(testJsonData), makeFlatVector(testJsonPathData)});
  auto result =
      evaluate<FlatVector<StringView>>("json_extract_scalar(c0, c1)", row);
  EXPECT_EQ(result->valueAt(0).str(), "value1") << "at 0";
  EXPECT_TRUE(result->isNullAt(1)) << "at 1";
  EXPECT_TRUE(result->isNullAt(2)) << "at 2";
  EXPECT_EQ(result->valueAt(3).str(), "456") << "at 3";
  EXPECT_TRUE(result->isNullAt(4)) << "at 4";
}

TEST_F(JsonExtractScalarTest, utf8Value) {
  std::vector<std::string> testJsonData = {
      "{\"key1\":\"I \\u2665 UTF-8\"}",
      u8"{\"key1\":\"I \u2665 UTF-8\"}",
      "{\"key1\":\"I \\uD834\\uDD1E playing in G-clef\"}",
      u8"{\"key1\":\"I \U0001D11E playing in G-clef\"}"};
  std::vector<std::string> testJsonPathData = {
      "$.key1", "$.key1", "$.key1", "$.key1"};

  auto row = makeRowVector(
      {makeFlatVector(testJsonData), makeFlatVector(testJsonPathData)});
  auto result =
      evaluate<FlatVector<StringView>>("json_extract_scalar(c0, c1)", row);
  EXPECT_EQ(result->valueAt(0).str(), u8"I \u2665 UTF-8") << "at 0";
  EXPECT_EQ(result->valueAt(1).str(), u8"I \u2665 UTF-8") << "at 1";
  EXPECT_EQ(result->valueAt(2).str(), u8"I \U0001D11E playing in G-clef")
      << "at 2";
  EXPECT_EQ(result->valueAt(3).str(), u8"I \U0001D11E playing in G-clef")
      << "at 3";
}

TEST_F(JsonExtractScalarTest, invalidJsonPath) {
  std::vector<std::string> testJsonData = {
      "{\"key1\":\"value1\", \"key2\":\"value2\"}"};

  auto row = makeRowVector({makeFlatVector(testJsonData)});
  EXPECT_THROW(
      evaluate<FlatVector<StringView>>("json_extract_scalar(c0, '')", row),
      VeloxUserError);
  EXPECT_THROW(
      evaluate<FlatVector<StringView>>(
          "json_extract_scalar(c0, '$.key1[')", row),
      VeloxUserError);
  EXPECT_THROW(
      evaluate<FlatVector<StringView>>("json_extract_scalar(c0, '$.]')", row),
      VeloxUserError);
}

TEST_F(JsonExtractScalarTest, jsonParseError) {
  std::vector<std::string> testJsonData = {"", "invalidJson"};

  auto row = makeRowVector({makeFlatVector(testJsonData)});
  auto result = evaluate<FlatVector<StringView>>(
      "json_extract_scalar(c0, '$.key1')", row);
  EXPECT_TRUE(result->isNullAt(0)) << "at 0";
  EXPECT_TRUE(result->isNullAt(1)) << "at 1";
}
