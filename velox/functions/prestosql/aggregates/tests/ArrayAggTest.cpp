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
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

namespace {

class ArrayAggTest : public AggregationTestBase {};

TEST_F(ArrayAggTest, groupBy) {
  constexpr int32_t kNumGroups = 10;
  std::vector<RowVectorPtr> batches;
  // We make 10 groups. each with 10 arrays. Each array consists of n
  // arrays of varchar. These 10 groups are repeated 10 times. The
  // expected result is that there is, for each key, an array of 100
  // elements with, for key k, batch[k[, batch[k + 10], ... batch[k +
  // 90], repeated 10 times.
  batches.push_back(
      std::static_pointer_cast<RowVector>(velox::test::BatchMaker::createBatch(
          ROW({"C0", "a"}, {INTEGER(), ARRAY(VARCHAR())}), 100, *pool_)));
  // We divide the rows into 10 groups.
  auto keys = batches[0]->childAt(0)->as<FlatVector<int32_t>>();
  for (auto i = 0; i < keys->size(); ++i) {
    if (i % 10 == 0) {
      keys->setNull(i, true);
    } else {
      keys->set(i, i % kNumGroups);
    }
  }
  // We make 10 repeats of the first batch.
  for (auto i = 0; i < 9; ++i) {
    batches.push_back(batches[0]);
  }
  CursorParameters params;
  params.planNode = PlanBuilder()
                        .values(batches)
                        .partialAggregation({0}, {"array_agg(A)"})
                        .finalAggregation()
                        .planNode();

  auto pair = readCursor(params, [](Task*) {});
  auto reference = batches[0]->as<RowVector>()->childAt(1);
  for (auto rowVector : pair.second) {
    auto resultKeys = rowVector->childAt(0)->as<FlatVector<int32_t>>();
    auto array = rowVector->childAt(1)->as<ArrayVector>();
    auto elements = array->elements()->as<ArrayVector>();
    for (auto i = 0; i < rowVector->size(); ++i) {
      auto group = resultKeys->isNullAt(i) ? 0 : resultKeys->valueAt(i);
      auto offset = array->offsetAt(i);
      auto size = array->sizeAt(i);
      EXPECT_EQ(size, 100);
      // We check that group n has a result that repeats elements of
      // reference at n, n+ 10, ... n+90 ten times.
      for (auto index = 0; index < size; ++index) {
        EXPECT_TRUE(reference->equalValueAt(
            elements,
            group + (i * kNumGroups) % reference->size(),
            offset + i));
      }
    }
  }

  // Add local exchange before intermediate aggregation. Expect the same result.
  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  params.planNode = PlanBuilder(planNodeIdGenerator)
                        .localPartition(
                            {0},
                            {PlanBuilder(planNodeIdGenerator)
                                 .values(batches)
                                 .partialAggregation({0}, {"array_agg(A)"})
                                 .planNode()})
                        .intermediateAggregation()
                        .planNode();
  params.maxDrivers = 2;

  exec::test::assertQuery(params, pair.second);
}

TEST_F(ArrayAggTest, global) {
  vector_size_t size = 10;

  std::vector<RowVectorPtr> vectors = {makeRowVector({makeFlatVector<int32_t>(
      size, [](vector_size_t row) { return row * 2; }, nullEvery(3))})};

  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({}, {"array_agg(c0)"})
                .finalAggregation()
                .planNode();

  auto value = readSingleValue(op);

  std::vector<velox::variant> expected;
  for (auto i = 0; i < size; i++) {
    if (i % 3 == 0) {
      expected.emplace_back(TypeKind::INTEGER);
    } else {
      expected.emplace_back(i * 2);
    }
  }
  ASSERT_EQ(velox::variant::array(expected), value);
}

} // namespace
} // namespace facebook::velox::aggregate::test
