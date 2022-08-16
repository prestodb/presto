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

} // namespace

} // namespace facebook::velox::functions::prestosql
