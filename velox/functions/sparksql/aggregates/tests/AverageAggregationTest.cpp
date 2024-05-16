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
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::functions::aggregate::sparksql::test {

namespace {

class AverageAggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerAggregateFunctions("spark_");
  }
};

TEST_F(AverageAggregationTest, avgAllNulls) {
  vector_size_t size = 1'000;
  // Have two row vectors a least as it triggers different code paths.
  std::vector<RowVectorPtr> vectors = {
      makeRowVector({
          makeAllNullFlatVector<int64_t>(size),
      }),
      makeRowVector({
          makeAllNullFlatVector<int64_t>(size),
      }),
  };
  testAggregations(vectors, {}, {"spark_avg(c0)"}, "SELECT NULL");

  auto plan = PlanBuilder()
                  .values(vectors)
                  .partialAggregation({}, {"spark_avg(c0)"})
                  .planNode();
  assertQuery(plan, "SELECT row(0, 0)");

  // Average with grouping key.
  // Have at least two row vectors as it triggers different code paths.
  vectors = {
      makeRowVector({
          makeNullableFlatVector<int64_t>({0, 1, 2, 0, 1, 2, 0, 1, 2, 0}),
          makeNullableFlatVector<int64_t>(
              {std::nullopt,
               std::nullopt,
               2,
               std::nullopt,
               10,
               9,
               std::nullopt,
               25,
               12,
               std::nullopt}),
      }),
      makeRowVector({
          makeNullableFlatVector<int64_t>({0, 1, 2, 0, 1, 2, 0, 1, 2, 0}),
          makeNullableFlatVector<int64_t>(
              {std::nullopt,
               10,
               20,
               std::nullopt,
               std::nullopt,
               25,
               std::nullopt,
               16,
               21,
               std::nullopt}),
      }),
  };
  createDuckDbTable(vectors);
  testAggregations(
      vectors,
      {"c0"},
      {"spark_avg(c1)"},
      "SELECT c0, avg(c1) FROM tmp GROUP BY c0");

  plan = PlanBuilder()
             .values(vectors)
             .partialAggregation({"c0"}, {"spark_avg(c1)"})
             .planNode();
  auto expected = makeRowVector(
      {"c0", "c1"},
      {
          makeFlatVector<int64_t>({0, 1, 2}),
          makeRowVector(
              {"sum", "count"},
              {
                  makeFlatVector<double>({0, 61, 89}),
                  makeFlatVector<int64_t>({0, 4, 6}),
              }),
      });
  assertQuery(plan, expected);
}

} // namespace
} // namespace facebook::velox::functions::aggregate::sparksql::test
