/*
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
#include "velox/aggregates/tests/AggregationTestBase.h"
#include "velox/exec/tests/PlanBuilder.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

namespace {

class MapAggTest : public AggregationTestBase {};

TEST_F(MapAggTest, groupBy) {
  vector_size_t size = 90;

  auto vectors = {makeRowVector(
      {makeFlatVector<int32_t>(size, [](vector_size_t row) { return row / 3; }),
       makeFlatVector<int32_t>(size, [](vector_size_t row) { return row; }),
       makeFlatVector<double>(
           size, [](vector_size_t row) { return row + 0.05; }, nullEvery(7))})};

  auto op = PlanBuilder()
                .values(vectors)
                .singleAggregation({0}, {"map_agg(c1, c2)"})
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
}

TEST_F(MapAggTest, global) {
  vector_size_t size = 10;

  std::vector<RowVectorPtr> vectors = {makeRowVector(
      {makeFlatVector<int32_t>(size, [](vector_size_t row) { return row; }),
       makeFlatVector<double>(
           size, [](vector_size_t row) { return row + 0.05; }, nullEvery(7))})};

  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({}, {"map_agg(c0, c1)"})
                .planNode();

  auto value = readSingleValue(op);

  std::map<velox::variant, velox::variant> expected;
  for (auto i = 0; i < size; i++) {
    if (i % 7 == 0) {
      expected.insert({i, velox::variant(TypeKind::DOUBLE)});
    } else {
      expected.insert({i, i + 0.05});
    }
  }
  ASSERT_EQ(velox::variant::map(expected), value);
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
