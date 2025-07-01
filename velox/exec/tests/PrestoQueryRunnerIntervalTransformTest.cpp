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

namespace facebook::velox::exec::test {
namespace {

class PrestoQueryRunnerIntervalTransformTest
    : public PrestoQueryRunnerIntermediateTypeTransformTestBase {};

TEST_F(PrestoQueryRunnerIntervalTransformTest, isIntermediateOnlyType) {
  ASSERT_TRUE(isIntermediateOnlyType(INTERVAL_DAY_TIME()));
  ASSERT_TRUE(isIntermediateOnlyType(ARRAY(INTERVAL_DAY_TIME())));
  ASSERT_TRUE(isIntermediateOnlyType(MAP(INTERVAL_DAY_TIME(), SMALLINT())));
  ASSERT_TRUE(isIntermediateOnlyType(MAP(VARBINARY(), INTERVAL_DAY_TIME())));
  ASSERT_TRUE(isIntermediateOnlyType(ROW({INTERVAL_DAY_TIME(), SMALLINT()})));
  ASSERT_TRUE(isIntermediateOnlyType(ROW(
      {SMALLINT(),
       TIMESTAMP(),
       ARRAY(ROW({MAP(VARCHAR(), INTERVAL_DAY_TIME())}))})));
}

TEST_F(PrestoQueryRunnerIntervalTransformTest, roundTrip) {
  std::vector<std::optional<int64_t>> no_nulls{0, 1, 2, 3};
  test(makeNullableFlatVector(no_nulls, INTERVAL_DAY_TIME()));

  std::vector<std::optional<int64_t>> some_nulls{0, 1, std::nullopt, 3};
  test(makeNullableFlatVector(some_nulls, INTERVAL_DAY_TIME()));

  std::vector<std::optional<int64_t>> all_nulls{
      std::nullopt, std::nullopt, std::nullopt};
  test(makeNullableFlatVector(all_nulls, INTERVAL_DAY_TIME()));
}

TEST_F(PrestoQueryRunnerIntervalTransformTest, negative) {
  auto vector = makeNullableFlatVector(
      std::vector<std::optional<int64_t>>{
          -1,
          -2,
          -3,
          -4,
          -5,
      },
      INTERVAL_DAY_TIME());
  const auto colName = "col";
  const auto input =
      makeRowVector({colName}, {transformIntermediateTypes(vector)});

  auto expr = getProjectionsToIntermediateTypes(
      vector->type(),
      std::make_shared<core::FieldAccessExpr>(
          colName,
          std::nullopt,
          std::vector<core::ExprPtr>{std::make_shared<core::InputExpr>()}),
      colName);

  core::PlanNodePtr plan =
      PlanBuilder().values({input}).projectExpressions({expr}).planNode();

  AssertQueryBuilder(plan).assertResults(makeRowVector(
      {colName},
      {makeNullableFlatVector(
          std::vector<std::optional<int64_t>>{
              std::nullopt,
              std::nullopt,
              std::nullopt,
              std::nullopt,
              std::nullopt,
          },
          INTERVAL_DAY_TIME())}));
}

TEST_F(PrestoQueryRunnerIntervalTransformTest, transformArray) {
  auto input = makeNullableFlatVector(
      std::vector<std::optional<int64_t>>{
          0,
          1,
          12,
          123,
          1234,
          12345,
          123456,
          1234567,
          12345678,
          123456789,
          1234567890,
          12345678901,
          123456789012,
          1234567890123,
          12345678901234,
          123456789012345,
          1234567890123456,
          12345678901234567,
          123456789012345678,
          1234567890123456789,
      },
      INTERVAL_DAY_TIME());
  testArray(input);

  input = makeNullableFlatVector(
      std::vector<std::optional<int64_t>>{
          993296205767471,
          101271764434518,
          166587109740908,
          210274651317771,
          276381323443199,
          283617048324990,
          519099922518052,
          530020098439118,
          604149362180160,
          622016152847258},
      INTERVAL_DAY_TIME());
  testArray(input);
}

TEST_F(PrestoQueryRunnerIntervalTransformTest, transformMap) {
  auto keys = makeNullableFlatVector(
      std::vector<std::optional<int64_t>>{
          1,
          12,
          123,
          1234,
          12345,
          123456,
          1234567,
          12345678,
          123456789,
          1234567890,
      },
      INTERVAL_DAY_TIME());

  auto values = makeNullableFlatVector(
      std::vector<std::optional<int64_t>>{
          993296205767471,
          101271764434518,
          166587109740908,
          210274651317771,
          276381323443199,
          283617048324990,
          519099922518052,
          530020098439118,
          604149362180160,
          622016152847258},
      INTERVAL_DAY_TIME());

  testMap(keys, values);
}

TEST_F(PrestoQueryRunnerIntervalTransformTest, transformRow) {
  auto input = makeNullableFlatVector(
      std::vector<std::optional<int64_t>>{
          993296205767471,
          101271764434518,
          166587109740908,
          210274651317771,
          276381323443199,
          283617048324990,
          519099922518052,
          530020098439118,
          604149362180160,
          622016152847258},
      INTERVAL_DAY_TIME());
  testRow({input}, {"row"});
}

} // namespace
} // namespace facebook::velox::exec::test
