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
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace {

std::string min(const std::string& column) {
  return fmt::format("min({})", column);
}

std::string max(const std::string& column) {
  return fmt::format("max({})", column);
}

class MinMaxTest : public aggregate::test::AggregationTestBase {
 protected:
  template <typename TAgg>
  void testIntegralType(TAgg agg, const TypePtr& inputType) {
    auto rowType = ROW({"c0", "c1"}, {BIGINT(), inputType});
    auto vectors = makeVectors(rowType, 1000, 10);
    createDuckDbTable(vectors);

    static const std::string c0 = "c0";
    static const std::string c1 = "c1";
    static const std::string a0 = "a0";

    // Global partial aggregation.
    auto op = PlanBuilder()
                  .values(vectors)
                  .partialAggregation({}, {agg(c1)})
                  .planNode();
    assertQuery(op, fmt::format("SELECT {} FROM tmp", agg(c1)));

    // Global final aggregation.
    op = PlanBuilder()
             .values(vectors)
             .partialAggregation({}, {agg(c1)})
             .finalAggregation()
             .planNode();
    assertQuery(op, fmt::format("SELECT {} FROM tmp", agg(c1)));

    // Group by partial aggregation.
    op = PlanBuilder()
             .values(vectors)
             .project({"c0 % 10", "c1"})
             .partialAggregation({"p0"}, {agg(c1)})
             .planNode();
    assertQuery(
        op, fmt::format("SELECT c0 % 10, {} FROM tmp GROUP BY 1", agg(c1)));

    // Group by final aggregation.
    op = PlanBuilder()
             .values(vectors)
             .project({"c0 % 10", "c1"})
             .partialAggregation({"p0"}, {agg(c1)})
             .finalAggregation()
             .planNode();
    assertQuery(
        op, fmt::format("SELECT c0 % 10, {} FROM tmp GROUP BY 1", agg(c1)));

    // encodings: use filter to wrap aggregation inputs in a dictionary.
    op = PlanBuilder()
             .values(vectors)
             .filter("c0 % 2 = 0")
             .project({"c0 % 11", "c1"})
             .partialAggregation({"p0"}, {agg(c1)})
             .planNode();

    assertQuery(
        op,
        fmt::format(
            "SELECT c0 % 11, {} FROM tmp WHERE c0 % 2 = 0 GROUP BY 1",
            agg(c1)));

    op = PlanBuilder()
             .values(vectors)
             .filter("c0 % 2 = 0")
             .partialAggregation({}, {agg(c1)})
             .planNode();
    assertQuery(
        op, fmt::format("SELECT {} FROM tmp WHERE c0 % 2 = 0", agg(c1)));
  }
};

TEST_F(MinMaxTest, maxTinyint) {
  testIntegralType(max, TINYINT());
}

TEST_F(MinMaxTest, maxSmallint) {
  testIntegralType(max, SMALLINT());
}

TEST_F(MinMaxTest, maxInteger) {
  testIntegralType(max, INTEGER());
}

TEST_F(MinMaxTest, maxBigint) {
  testIntegralType(max, BIGINT());
}

TEST_F(MinMaxTest, minTinyint) {
  testIntegralType(min, TINYINT());
}

TEST_F(MinMaxTest, minSmallint) {
  testIntegralType(min, SMALLINT());
}

TEST_F(MinMaxTest, minInteger) {
  testIntegralType(min, INTEGER());
}

TEST_F(MinMaxTest, minBigint) {
  testIntegralType(min, BIGINT());
}

TEST_F(MinMaxTest, maxVarchar) {
  auto rowType = ROW({"c0", "c1"}, {INTEGER(), VARCHAR()});
  auto vector = std::dynamic_pointer_cast<RowVector>(
      test::BatchMaker::createBatch(rowType, 10'000, *pool_));
  std::vector<RowVectorPtr> vectors = {vector};
  createDuckDbTable(vectors);

  auto op = PlanBuilder()
                .values(vectors)
                .project({"c0 % 11", "c1"})
                .partialAggregation({"p0"}, {"max(c1)"})
                .planNode();
  assertQuery(op, "SELECT c0 % 11, max(c1) FROM tmp GROUP BY 1");

  op = PlanBuilder()
           .values(vectors)
           .partialAggregation({}, {"max(c1)"})
           .planNode();
  assertQuery(op, "SELECT max(c1) FROM tmp");

  // Encodings: use filter to wrap aggregation inputs in a dictionary
  op = PlanBuilder()
           .values(vectors)
           .filter("c0 % 2 = 0")
           .project({"c0 % 11", "c1"})
           .partialAggregation({"p0"}, {"max(c1)"})
           .planNode();
  assertQuery(
      op, "SELECT c0 % 11, max(c1) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1");

  op = PlanBuilder()
           .values(vectors)
           .filter("c0 % 2 = 0")
           .project({"c0 % 11", "c1"})
           .partialAggregation({"p0"}, {"max(c1)"})
           .finalAggregation()
           .planNode();
  assertQuery(
      op, "SELECT c0 % 11, max(c1) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1");

  op = PlanBuilder()
           .values(vectors)
           .filter("c0 % 2 = 0")
           .partialAggregation({}, {"max(c1)"})
           .planNode();
  assertQuery(op, "SELECT max(c1) FROM tmp WHERE c0 % 2 = 0");

  op = PlanBuilder()
           .values(vectors)
           .filter("c0 % 2 = 0")
           .partialAggregation({}, {"max(c1)"})
           .finalAggregation()
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
                .project({"c0 % 17", "c1"})
                .partialAggregation({"p0"}, {"min(c1)"})
                .planNode();
  assertQuery(op, "SELECT c0 % 17, min(c1) FROM tmp GROUP BY 1");

  op = PlanBuilder()
           .values(vectors)
           .partialAggregation({}, {"min(c1)"})
           .planNode();
  assertQuery(op, "SELECT min(c1) FROM tmp");

  // Encodings: use filter to wrap aggregation inputs in a dictionary
  op = PlanBuilder()
           .values(vectors)
           .filter("c0 % 2 = 0")
           .project({"c0 % 17", "c1"})
           .partialAggregation({"p0"}, {"min(c1)"})
           .planNode();
  assertQuery(
      op, "SELECT c0 % 17, min(c1) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1");

  op = PlanBuilder()
           .values(vectors)
           .filter("c0 % 2 = 0")
           .project({"c0 % 17", "c1"})
           .partialAggregation({"p0"}, {"min(c1)"})
           .finalAggregation()
           .planNode();
  assertQuery(
      op, "SELECT c0 % 17, min(c1) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1");

  op = PlanBuilder()
           .values(vectors)
           .filter("c0 % 2 = 0")
           .partialAggregation({}, {"min(c1)"})
           .planNode();
  assertQuery(op, "SELECT min(c1) FROM tmp WHERE c0 % 2 = 0");

  op = PlanBuilder()
           .values(vectors)
           .filter("c0 % 2 = 0")
           .partialAggregation({}, {"min(c1)"})
           .finalAggregation()
           .planNode();
  assertQuery(op, "SELECT min(c1) FROM tmp WHERE c0 % 2 = 0");
}

TEST_F(MinMaxTest, constVarchar) {
  // Create two batches of the source data for the aggregation:
  // Column c0 with 1K of "apple" and 1K of "banana".
  // Column c1 with 1K of nulls and 1K of nulls.
  auto constVectors = {
      makeRowVector(
          {makeConstant("apple", 1'000),
           makeNullConstant(TypeKind::VARCHAR, 1'000)}),
      makeRowVector({
          makeConstant("banana", 1'000),
          makeConstant(TypeKind::VARCHAR, 1'000),
      })};
  auto op =
      PlanBuilder()
          .values({constVectors})
          .singleAggregation({}, {"min(c0)", "max(c0)", "min(c1)", "max(c1)"})
          .planNode();
  assertQuery(op, "SELECT 'apple', 'banana', null, null");
}

TEST_F(MinMaxTest, minMaxTimestamp) {
  auto rowType = ROW({"c0"}, {TIMESTAMP()});
  auto vectors = makeVectors(rowType, 1'000, 10);
  createDuckDbTable(vectors);

  auto agg = PlanBuilder()
                 .values(vectors)
                 .partialAggregation({}, {"min(c0)", "max(c0)"})
                 .finalAggregation()
                 .planNode();
  assertQuery(
      agg,
      "SELECT date_trunc('millisecond', min(c0)), date_trunc('millisecond', max(c0)) FROM tmp");
}

TEST_F(MinMaxTest, minMaxDate) {
  auto rowType = ROW({"c0"}, {DATE()});
  auto vectors = makeVectors(rowType, 1'000, 10);
  createDuckDbTable(vectors);

  auto agg = PlanBuilder()
                 .values(vectors)
                 .partialAggregation({}, {"min(c0)", "max(c0)"})
                 .finalAggregation()
                 .planNode();
  assertQuery(agg, "SELECT min(c0), max(c0) from tmp");
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
                 .finalAggregation()
                 .planNode();
  assertQuery(agg, "SELECT 1");

  agg = PlanBuilder()
            .values({row})
            .partialAggregation({}, {"max(c1)"})
            .finalAggregation()
            .planNode();
  assertQuery(agg, "SELECT -1");
}

} // namespace
