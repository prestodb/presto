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
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/exec/tests/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace {

class MinMaxTest : public aggregate::test::AggregationTestBase {};

TEST_F(MinMaxTest, maxTinyint) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), TINYINT()});
  auto vectors = makeVectors(rowType, 1000, 10);
  createDuckDbTable(vectors);

  // Global partial aggregation.
  auto agg = PlanBuilder()
                 .values(vectors)
                 .partialAggregation({}, {"max(c1)"})
                 .planNode();
  assertQuery(agg, "SELECT max(c1) FROM tmp");

  // Global final aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .partialAggregation({}, {"max(c1)"})
            .finalAggregation({}, {"max(a0)"})
            .planNode();
  assertQuery(agg, "SELECT max(c1) FROM tmp");

  // Group by partial aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .project({"c0 % 10", "c1"}, {"c0 % 10", "c1"})
            .partialAggregation({0}, {"max(c1)"})
            .planNode();
  assertQuery(agg, "SELECT c0 % 10, max(c1) FROM tmp GROUP BY 1");

  // Group by final aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .project({"c0 % 10", "c1"}, {"c0 % 10", "c1"})
            .partialAggregation({0}, {"max(c1)"})
            .finalAggregation({0}, {"max(a0)"})
            .planNode();
  assertQuery(agg, "SELECT c0 % 10, max(c1) FROM tmp GROUP BY 1");

  // encodings: use filter to wrap aggregation inputs in a dictionary.
  agg = PlanBuilder()
            .values(vectors)
            .filter("c0 % 2 = 0")
            .project({"c0 % 11", "c1"}, {"c0_mod_11", "c1"})
            .partialAggregation({0}, {"max(c1)"})
            .planNode();

  assertQuery(
      agg, "SELECT c0 % 11, max(c1) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1");

  agg = PlanBuilder()
            .values(vectors)
            .filter("c0 % 2 = 0")
            .partialAggregation({}, {"max(c1)"})
            .planNode();
  assertQuery(agg, "SELECT max(c1) FROM tmp WHERE c0 % 2 = 0");
}

TEST_F(MinMaxTest, minTinyint) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), TINYINT()});
  auto vectors = makeVectors(rowType, 1000, 10);
  createDuckDbTable(vectors);

  // Global partial aggregation.
  auto agg = PlanBuilder()
                 .values(vectors)
                 .partialAggregation({}, {"min(c1)"})
                 .planNode();
  assertQuery(agg, "SELECT min(c1) FROM tmp");

  // Global final aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .partialAggregation({}, {"min(c1)"})
            .finalAggregation({}, {"min(a0)"})
            .planNode();
  assertQuery(agg, "SELECT min(c1) FROM tmp");

  // Group by partial aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .project({"c0 % 10", "c1"}, {"c0 % 10", "c1"})
            .partialAggregation({0}, {"min(c1)"})
            .planNode();
  assertQuery(agg, "SELECT c0 % 10, min(c1) FROM tmp GROUP BY 1");

  // Group by final aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .project({"c0 % 10", "c1"}, {"c0 % 10", "c1"})
            .partialAggregation({0}, {"min(c1)"})
            .finalAggregation({0}, {"min(a0)"})
            .planNode();
  assertQuery(agg, "SELECT c0 % 10, min(c1) FROM tmp GROUP BY 1");

  // encodings: use filter to wrap aggregation inputs in a dictionary.
  agg = PlanBuilder()
            .values(vectors)
            .filter("c0 % 2 = 0")
            .project({"c0 % 11", "c1"}, {"c0_mod_11", "c1"})
            .partialAggregation({0}, {"min(c1)"})
            .planNode();

  assertQuery(
      agg, "SELECT c0 % 11, min(c1) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1");

  agg = PlanBuilder()
            .values(vectors)
            .filter("c0 % 2 = 0")
            .partialAggregation({}, {"min(c1)"})
            .planNode();
  assertQuery(agg, "SELECT min(c1) FROM tmp WHERE c0 % 2 = 0");
}

TEST_F(MinMaxTest, maxVarchar) {
  auto rowType = ROW({"c0", "c1"}, {INTEGER(), VARCHAR()});
  auto vector = std::dynamic_pointer_cast<RowVector>(
      test::BatchMaker::createBatch(rowType, 10'000, *pool_));
  std::vector<RowVectorPtr> vectors = {vector};
  createDuckDbTable(vectors);

  auto op = PlanBuilder()
                .values(vectors)
                .project({"c0 % 11", "c1"}, {"c0_mod_11", "c1"})
                .partialAggregation({0}, {"max(c1)"})
                .planNode();

  assertQuery(op, "SELECT c0 % 11, max(c1) FROM tmp GROUP BY 1");

  op = PlanBuilder()
           .values(vectors)
           .partialAggregation({}, {"max(c1)"})
           .planNode();

  assertQuery(op, "SELECT max(c1) FROM tmp");

  // encodings: use filter to wrap aggregation inputs in a dictionary
  op = PlanBuilder()
           .values(vectors)
           .filter("c0 % 2 = 0")
           .project({"c0 % 11", "c1"}, {"c0_mod_11", "c1"})
           .partialAggregation({0}, {"max(c1)"})
           .planNode();

  assertQuery(
      op, "SELECT c0 % 11, max(c1) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1");

  op = PlanBuilder()
           .values(vectors)
           .filter("c0 % 2 = 0")
           .partialAggregation({}, {"max(c1)"})
           .planNode();
  assertQuery(op, "SELECT max(c1) FROM tmp WHERE c0 % 2 = 0");
}

TEST_F(MinMaxTest, minVarchar) {
  auto rowType = ROW({"c0", "c1"}, {INTEGER(), VARCHAR()});
  auto vector = std::dynamic_pointer_cast<RowVector>(
      test::BatchMaker::createBatch(rowType, 10'000, *pool_));
  std::vector<RowVectorPtr> vectors = {vector};
  createDuckDbTable(vectors);

  auto op = PlanBuilder()
                .values(vectors)
                .project({"c0 % 17", "c1"}, {"c0_mod_17", "c1"})
                .partialAggregation({0}, {"min(c1)"})
                .planNode();

  assertQuery(op, "SELECT c0 % 17, min(c1) FROM tmp GROUP BY 1");

  op = PlanBuilder()
           .values(vectors)
           .partialAggregation({}, {"min(c1)"})
           .planNode();

  assertQuery(op, "SELECT min(c1) FROM tmp");

  // encodings: use filter to wrap aggregation inputs in a dictionary
  op = PlanBuilder()
           .values(vectors)
           .filter("c0 % 2 = 0")
           .project({"c0 % 17", "c1"}, {"c0_mod_17", "c1"})
           .partialAggregation({0}, {"min(c1)"})
           .planNode();

  assertQuery(
      op, "SELECT c0 % 17, min(c1) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1");

  op = PlanBuilder()
           .values(vectors)
           .filter("c0 % 2 = 0")
           .partialAggregation({}, {"min(c1)"})
           .planNode();
  assertQuery(op, "SELECT min(c1) FROM tmp WHERE c0 % 2 = 0");
}

TEST_F(MinMaxTest, constVarchar) {
  auto constVectors = {
      makeRowVector({BaseVector::createConstant("apple", 1'000, pool_.get())}),
      makeRowVector(
          {BaseVector::createConstant("banana", 1'000, pool_.get())})};
  auto op = PlanBuilder()
                .values({constVectors})
                .singleAggregation({}, {"min(c0)", "max(c0)"})
                .planNode();
  assertQuery(op, "SELECT 'apple', 'banana'");
}

TEST_F(MinMaxTest, minMaxTimestamp) {
  auto rowType = ROW({"c0"}, {TIMESTAMP()});
  auto vectors = makeVectors(rowType, 1'000, 10);
  createDuckDbTable(vectors);

  auto agg = PlanBuilder()
                 .values(vectors)
                 .partialAggregation({}, {"min(c0)", "max(c0)"})
                 .finalAggregation({}, {"min(a0)", "max(a1)"})
                 .planNode();
  assertQuery(agg, "SELECT min(c0), max(c0) FROM tmp");
}

TEST_F(MinMaxTest, initialValue) {
  // Ensures that no groups are default initialized (to 0) in
  // aggregate::SimpleNumericAggregate.
  auto rowType = ROW({"c0", "c1"}, {TINYINT(), TINYINT()});
  auto arrayVectorC0 = makeFlatVector<int8_t>({1, 1, 1, 1});
  auto arrayVectorC1 = makeFlatVector<int8_t>({-1, -1, -1, -1});

  std::vector<VectorPtr> vec = {arrayVectorC0, arrayVectorC1};
  auto row = std::make_shared<RowVector>(
      pool_.get(), rowType, nullptr, vec.front()->size(), vec, 0);

  // Test min of {1, 1, ...} and max {-1, -1, ..}.
  // Make sure they are not zero.
  auto agg = PlanBuilder()
                 .values({row})
                 .partialAggregation({}, {"min(c0)"})
                 .finalAggregation({}, {"min(a0)"})
                 .planNode();
  assertQuery(agg, "SELECT 1");

  agg = PlanBuilder()
            .values({row})
            .partialAggregation({}, {"max(c1)"})
            .finalAggregation({}, {"max(a0)"})
            .planNode();
  assertQuery(agg, "SELECT -1");
}

} // namespace
