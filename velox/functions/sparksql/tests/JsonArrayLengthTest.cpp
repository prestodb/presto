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
#include <optional>
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class JsonArrayLengthTest : public SparkFunctionBaseTest {
 protected:
  const auto jsonArrayLength(const std::string& json) {
    auto varcharVector = makeFlatVector<std::string>({json});
    return evaluateOnce<int32_t>(
        "json_array_length(c0)", makeRowVector({varcharVector}));
  }
};

TEST_F(JsonArrayLengthTest, basic) {
  EXPECT_EQ(4, jsonArrayLength(R"([1,2,3,4])"));
  EXPECT_EQ(5, jsonArrayLength(R"([1,2,3,{"f1":1,"f2":[5,6]},4])"));
  // Malformed Json.
  EXPECT_EQ(std::nullopt, jsonArrayLength(R"([1,2)"));
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
