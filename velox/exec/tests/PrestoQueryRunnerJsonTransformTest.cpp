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

#include "velox/exec/fuzzer/PrestoQueryRunnerIntermediateTypeTransforms.h"
#include "velox/exec/tests/PrestoQueryRunnerIntermediateTypeTransformTestBase.h"
#include "velox/functions/prestosql/types/JsonType.h"

namespace facebook::velox::exec::test {
namespace {
class PrestoQueryRunnerJsonTransformTest
    : public PrestoQueryRunnerIntermediateTypeTransformTestBase {};

TEST_F(PrestoQueryRunnerJsonTransformTest, verifyTransformExpression) {
  core::PlanNodePtr plan =
      PlanBuilder()
          .values({makeRowVector(
              {"c0"},
              {transformIntermediateTypes(makeNullableFlatVector(
                  std::vector<std::optional<StringView>>{}, JSON()))})})
          .projectExpressions({getProjectionsToIntermediateTypes(
              JSON(),
              std::make_shared<core::FieldAccessExpr>(
                  "c0",
                  std::nullopt,
                  std::vector<core::ExprPtr>{
                      std::make_shared<core::InputExpr>()}),
              "c0")})
          .planNode();

  VELOX_CHECK_EQ(
      plan->toString(true, false),
      "-- Project[1][expressions: (c0:JSON, try(json_parse(ROW[\"c0\"])))] -> c0:JSON\n");
  AssertQueryBuilder(plan).assertTypeAndNumRows(JSON(), 0);
}

TEST_F(PrestoQueryRunnerJsonTransformTest, roundTrip) {
  std::vector<std::optional<StringView>> no_nulls{"1", "2", "3"};
  test(makeNullableFlatVector(no_nulls, JSON()));

  std::vector<std::optional<StringView>> some_nulls{"1", std::nullopt, "3"};
  test(makeNullableFlatVector(some_nulls, JSON()));

  std::vector<std::optional<StringView>> all_nulls{
      std::nullopt, std::nullopt, std::nullopt};
  test(makeNullableFlatVector(all_nulls, JSON()));
}

TEST_F(PrestoQueryRunnerJsonTransformTest, isIntermediateOnlyType) {
  ASSERT_TRUE(isIntermediateOnlyType(JSON()));
  ASSERT_TRUE(isIntermediateOnlyType(ARRAY(JSON())));
  ASSERT_TRUE(isIntermediateOnlyType(MAP(JSON(), SMALLINT())));
  ASSERT_TRUE(isIntermediateOnlyType(MAP(VARBINARY(), JSON())));
  ASSERT_TRUE(isIntermediateOnlyType(ROW({JSON(), SMALLINT()})));
  ASSERT_TRUE(isIntermediateOnlyType(ROW({BOOLEAN(), ARRAY(JSON())})));
  ASSERT_TRUE(isIntermediateOnlyType(
      ROW({SMALLINT(), TIMESTAMP(), ARRAY(ROW({MAP(VARCHAR(), JSON())}))})));
}

TEST_F(PrestoQueryRunnerJsonTransformTest, transformArray) {
  std::vector<std::optional<StringView>> valid_json{
      "1",
      "2",
      "3",
      "\"{}\"",
      "[1,2,3]",
      "{\"name\":\"John\"}",
      "{\"product\":\"laptop\"}",
      "{\"temperature\":23.5}",
      "{\"isActive\":true}",
      "{\"coordinates\":{\"latitude\":40.7128}}",
      "{\"colors\":[\"red\"]}",
      "{\"user\":{\"id\":123}}",
      "{\"order\":{\"id\":456}}",
      "{\"event\":\"concert\"}",
      "{\"settings\":{\"volume\":75}}",
      "{\"company\":{\"name\":\"TechCorp\"}}",
      "{\"university\":{\"departments\":[{\"name\":\"ComputerScience\"}]}}",
      "{\"library\":{\"books\":[{\"title\":\"1984\"}]}}",
      "{\"restaurant\":{\"menu\":{\"appetizers\":[\"salad\"]}}}",
      "{\"project\":{\"name\":\"Apollo\"}}",
  };
  auto input = makeNullableFlatVector(valid_json, JSON());
  testArray(input);
}

TEST_F(PrestoQueryRunnerJsonTransformTest, transformMap) {
  std::vector<std::optional<StringView>> valid_json_keys{
      "\"key1\"",
      "\"key2\"",
      "\"key3\"",
      "{\"address\":{\"city\":\"New York\"}}",
      "{\"company\":{\"name\":\"TechCorp\"}}",
      "{\"product\":\"Laptop\"}",
      "\"key7\"",
      "\"key8\"",
      "\"key9\"",
      "\"key10\""};
  auto keys = makeNullableFlatVector(valid_json_keys, JSON());
  std::vector<std::optional<StringView>> valid_json_values{
      "{\"name\":\"Alice\"}",
      "{\"age\":30}",
      "{\"address\":{\"city\":\"New York\"}}",
      "{\"company\":{\"name\":\"TechCorp\"}}",
      "{\"product\":\"Laptop\"}",
      "{\"price\":999.99}",
      "{\"user\":{\"id\":123}}",
      "{\"order\":{\"id\":456}}",
      "{\"event\":{\"name\":\"Conference\"}}",
      "{\"library\":{\"books\":[{\"title\":\"1984\"}]}}"};
  auto values = makeNullableFlatVector(valid_json_values, JSON());
  testMap(keys, values);
}

TEST_F(PrestoQueryRunnerJsonTransformTest, transformRow) {
  std::vector<std::optional<StringView>> valid_json{
      "1",
      "2",
      "3",
      "\"{}\"",
      "[1,2,3]",
      "{\"name\":\"John\"}",
      "{\"product\":\"laptop\"}",
      "{\"temperature\":23.5}",
      "{\"isActive\":true}",
      "{\"coordinates\":{\"latitude\":40.7128}}",
      "{\"colors\":[\"red\"]}",
      "{\"user\":{\"id\":123}}",
      "{\"order\":{\"id\":456}}",
      "{\"event\":\"concert\"}",
      "{\"settings\":{\"volume\":75}}",
      "{\"company\":{\"name\":\"TechCorp\"}}",
      "{\"university\":{\"departments\":[{\"name\":\"ComputerScience\"}]}}",
      "{\"library\":{\"books\":[{\"title\":\"1984\"}]}}",
      "{\"restaurant\":{\"menu\":{\"appetizers\":[\"salad\"]}}}",
      "{\"project\":{\"name\":\"Apollo\"}}",
  };
  auto input = makeNullableFlatVector(valid_json, JSON());
  testRow({input}, {"row"});
}

} // namespace
} // namespace facebook::velox::exec::test
