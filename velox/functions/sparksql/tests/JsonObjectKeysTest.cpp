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
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class JsonObjectKeysTest : public SparkFunctionBaseTest {
 protected:
  VectorPtr jsonObjectKeys(const std::string& json) {
    auto varcharVector = makeFlatVector<std::string>({json});
    return evaluate("json_object_keys(c0)", makeRowVector({varcharVector}));
  }
};

TEST_F(JsonObjectKeysTest, basic) {
  auto expected =
      makeArrayVectorFromJson<std::string>({"[\"name\",\"age\",\"id\"]"});
  assertEqualVectors(
      jsonObjectKeys(R"({"name": "Alice", "age": 5, "id": "001"})"), expected);

  expected = makeArrayVectorFromJson<std::string>({"[]"});
  assertEqualVectors(jsonObjectKeys(R"({})"), expected);

  expected = makeNullableArrayVector<std::string>({std::nullopt});
  assertEqualVectors(jsonObjectKeys(R"(1)"), expected);
  assertEqualVectors(jsonObjectKeys(R"("hello")"), expected);
  assertEqualVectors(jsonObjectKeys(R"("")"), expected);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
