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
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

namespace {

class MapAggTest : public AggregationTestBase {};

TEST_F(MapAggTest, finalGroupBy) {
  vector_size_t num = 10;

  auto vectors = {makeRowVector(
      {makeFlatVector<int32_t>(num, [](vector_size_t row) { return row / 3; }),
       makeFlatVector<int32_t>(num, [](vector_size_t row) { return row; }),
       makeFlatVector<double>(
           num, [](vector_size_t row) { return row + 0.05; })})};

  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({"c0"}, {"map_agg(c1, c2)"})
                .finalAggregation()
                .planNode();

  static std::array<int32_t, 10> keys{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  vector_size_t keyIndex{0};
  vector_size_t valIndex{0};

  auto expectedResult = {makeRowVector(
      {makeFlatVector<int32_t>({0, 1, 2, 3}),
       makeMapVector<int32_t, double>(
           4,
           [&](vector_size_t row) { return (row == 3) ? 1 : 3; },
           [&](vector_size_t row) { return keys[keyIndex++]; },
           [&](vector_size_t row) { return keys[valIndex++] + 0.05; })})};

  exec::test::assertQuery(op, expectedResult);
}

TEST_F(MapAggTest, groupBy) {
  vector_size_t size = 90;

  auto vectors = {makeRowVector(
      {makeFlatVector<int32_t>(size, [](vector_size_t row) { return row / 3; }),
       makeFlatVector<int32_t>(size, [](vector_size_t row) { return row; }),
       makeFlatVector<double>(
           size, [](vector_size_t row) { return row + 0.05; }, nullEvery(7))})};

  auto op = PlanBuilder()
                .values(vectors)
                .singleAggregation({"c0"}, {"map_agg(c1, c2)"})
                .planNode();

  auto expectedResult = {makeRowVector(
      {makeFlatVector<int32_t>(30, [](vector_size_t row) { return row; }),
       makeMapVector<int32_t, double>(
           30,
           [](vector_size_t /*row*/) { return 3; },
           [](vector_size_t row) { return row; },
           [](vector_size_t row) { return row + 0.05; },
           nullptr,
           nullEvery(7))})};

  exec::test::assertQuery(op, expectedResult);

  // Add local exchange before intermediate aggregation.
  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();

  CursorParameters params;
  params.planNode =
      PlanBuilder(planNodeIdGenerator)
          .localPartition(
              {"c0"},
              {PlanBuilder(planNodeIdGenerator)
                   .values(vectors)
                   .partialAggregation({"c0"}, {"map_agg(c1, c2)"})
                   .planNode()})
          .intermediateAggregation()
          .planNode();
  params.maxDrivers = 2;

  exec::test::assertQuery(params, expectedResult);
}

TEST_F(MapAggTest, global) {
  vector_size_t size = 10;

  std::vector<RowVectorPtr> vectors = {makeRowVector(
      {makeFlatVector<int32_t>(size, [](vector_size_t row) { return row; }),
       makeFlatVector<double>(
           size, [](vector_size_t row) { return row + 0.05; }, nullEvery(7))})};

  std::map<velox::variant, velox::variant> expected;
  for (auto i = 0; i < size; i++) {
    if (i % 7 == 0) {
      expected.insert({i, velox::variant(TypeKind::DOUBLE)});
    } else {
      expected.insert({i, i + 0.05});
    }
  }
  const velox::variant mapExpected{velox::variant::map(expected)};

  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({}, {"map_agg(c0, c1)"})
                .planNode();
  ASSERT_EQ(mapExpected, readSingleValue(op));

  op = PlanBuilder()
           .values(vectors)
           .partialAggregation({}, {"map_agg(c0, c1)"})
           .finalAggregation()
           .planNode();
  ASSERT_EQ(mapExpected, readSingleValue(op));

  // Add local exchange before intermediate aggregation. Expect the same result.
  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  op = PlanBuilder(planNodeIdGenerator)
           .localPartition(
               {},
               {PlanBuilder(planNodeIdGenerator)
                    .localPartitionRoundRobin(
                        {PlanBuilder(planNodeIdGenerator)
                             .values(vectors)
                             .partialAggregation({}, {"map_agg(c0, c1)"})
                             .planNode()})
                    .intermediateAggregation()
                    .planNode()})
           .finalAggregation()
           .planNode();

  ASSERT_EQ(mapExpected, readSingleValue(op, 2));
}

TEST_F(MapAggTest, globalNoData) {
  std::vector<RowVectorPtr> vectors = {
      vectorMaker_.rowVector(ROW({"c0", "c1"}, {INTEGER(), DOUBLE()}), 0)};

  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({}, {"map_agg(c0, c1)"})
                .planNode();
  ASSERT_EQ(velox::variant::map({}), readSingleValue(op));

  op = PlanBuilder()
           .values(vectors)
           .partialAggregation({}, {"map_agg(c0, c1)"})
           .finalAggregation()
           .planNode();
  ASSERT_EQ(velox::variant::map({}), readSingleValue(op));
}

TEST_F(MapAggTest, globalDuplicateKeys) {
  vector_size_t size = 10;

  std::vector<RowVectorPtr> vectors = {makeRowVector(
      {makeFlatVector<int32_t>(size, [](vector_size_t row) { return row / 2; }),
       makeFlatVector<double>(
           size, [](vector_size_t row) { return row + 0.05; }, nullEvery(7))})};

  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({}, {"map_agg(c0, c1)"})
                .planNode();

  auto value = readSingleValue(op);

  std::map<velox::variant, velox::variant> expected;
  for (auto i = 0; i < size; i += 2) {
    if (i % 7 == 0) {
      expected.insert({i / 2, velox::variant(TypeKind::DOUBLE)});
    } else {
      expected.insert({i / 2, i + 0.05});
    }
  }
  ASSERT_EQ(velox::variant::map(expected), value);
}

} // namespace
} // namespace facebook::velox::aggregate::test
