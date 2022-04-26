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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

class CovarianceAggregationTest
    : public virtual AggregationTestBase,
      public testing::WithParamInterface<std::string> {
 protected:
  void testGroupBy(const std::string& aggName, const RowVectorPtr& data) {
    auto partialAgg = fmt::format("{}(c1, c2)", aggName);
    auto sql = fmt::format(
        "SELECT c0, round({}(c1, c2), 2) FROM tmp GROUP BY 1", aggName);

    auto op = PlanBuilder()
                  .values({data})
                  .partialAggregation({"c0"}, {partialAgg})
                  .finalAggregation()
                  .project({"c0", "round(a0, cast(2 as integer))"})
                  .planNode();

    assertQuery(op, sql);

    op = PlanBuilder()
             .values({data})
             .singleAggregation({"c0"}, {partialAgg})
             .project({"c0", "round(a0, cast(2 as integer))"})
             .planNode();

    assertQuery(op, sql);

    op = PlanBuilder()
             .values({data})
             .partialAggregation({"c0"}, {partialAgg})
             .planNode();

    auto partialResults = getResults(op);

    op = PlanBuilder()
             .values({partialResults})
             .finalAggregation(
                 {"c0"}, {fmt::format("{}(a0)", aggName)}, {DOUBLE()})
             .project({"c0", "round(a0, cast(2 as integer))"})
             .planNode();
    assertQuery(op, sql);
  }

  void testGlobalAgg(const std::string& aggName, const RowVectorPtr& data) {
    auto partialAgg = fmt::format("{}(c1, c2)", aggName);
    auto sql = fmt::format("SELECT round({}(c1, c2), 2) FROM tmp", aggName);

    auto op = PlanBuilder()
                  .values({data})
                  .partialAggregation({}, {partialAgg})
                  .finalAggregation()
                  .project({"round(a0, cast(2 as integer))"})
                  .planNode();

    assertQuery(op, sql);

    op = PlanBuilder()
             .values({data})
             .singleAggregation({}, {partialAgg})
             .project({"round(a0, cast(2 as integer))"})
             .planNode();

    assertQuery(op, sql);

    op = PlanBuilder()
             .values({data})
             .partialAggregation({}, {partialAgg})
             .planNode();

    auto partialResults = getResults(op);

    op = PlanBuilder()
             .values({partialResults})
             .finalAggregation({}, {fmt::format("{}(a0)", aggName)}, {DOUBLE()})
             .project({"round(a0, cast(2 as integer))"})
             .planNode();

    assertQuery(op, sql);
  }
};

TEST_P(CovarianceAggregationTest, doubleNoNulls) {
  vector_size_t size = 1'000;
  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
      makeFlatVector<double>(size, [](auto row) { return row * 0.1; }),
      makeFlatVector<double>(size, [](auto row) { return row * 0.2; }),
  });

  createDuckDbTable({data});

  auto aggName = GetParam();
  testGlobalAgg(aggName, data);
  testGroupBy(aggName, data);
}

TEST_P(CovarianceAggregationTest, doubleSomeNulls) {
  vector_size_t size = 1'000;
  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
      makeFlatVector<double>(
          size, [](auto row) { return row * 0.1; }, nullEvery(11)),
      makeFlatVector<double>(
          size, [](auto row) { return row * 0.2; }, nullEvery(17)),
  });

  createDuckDbTable({data});

  auto aggName = GetParam();
  testGlobalAgg(aggName, data);
  testGroupBy(aggName, data);
}

TEST_P(CovarianceAggregationTest, floatNoNulls) {
  vector_size_t size = 1'000;
  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
      makeFlatVector<float>(size, [](auto row) { return row * 0.1; }),
      makeFlatVector<float>(size, [](auto row) { return row * 0.2; }),
  });

  createDuckDbTable({data});

  auto aggName = GetParam();
  testGlobalAgg(aggName, data);
  testGroupBy(aggName, data);
}

TEST_P(CovarianceAggregationTest, floatSomeNulls) {
  vector_size_t size = 1'000;
  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
      makeFlatVector<float>(
          size, [](auto row) { return row * 0.1; }, nullEvery(11)),
      makeFlatVector<float>(
          size, [](auto row) { return row * 0.2; }, nullEvery(17)),
  });

  createDuckDbTable({data});

  auto aggName = GetParam();
  testGlobalAgg(aggName, data);
  testGroupBy(aggName, data);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    CovarianceAggregationTest,
    CovarianceAggregationTest,
    testing::Values("covar_samp", "covar_pop", "corr"));
} // namespace facebook::velox::aggregate::test
