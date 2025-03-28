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
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::functions::aggregate::sparksql::test {

namespace {
class CentralMomentsAggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerAggregateFunctions("spark_");
  }

  void testCenteralMomentsAggResult(
      const std::string& agg,
      const RowVectorPtr& input,
      const RowVectorPtr& expected,
      bool legacy = false) {
    auto plan = PlanBuilder()
                    .values({input})
                    .singleAggregation({}, {fmt::format("spark_{}(c0)", agg)})
                    .planNode();
    AssertQueryBuilder(plan)
        .config(
            core::QueryConfig::kSparkLegacyStatisticalAggregate,
            legacy ? "true" : "false")
        .assertResults({expected});
  }
};

TEST_F(CentralMomentsAggregationTest, skewnessHasResult) {
  auto agg = "skewness";
  auto input = makeRowVector({makeFlatVector<double>({1, 2})});
  // Even when the count is 2, Spark still produces output.
  auto expected =
      makeRowVector({makeFlatVector<double>(std::vector<double>{0.0})});
  testCenteralMomentsAggResult(agg, input, expected);

  input = makeRowVector({makeFlatVector<double>({1, 1})});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCenteralMomentsAggResult(agg, input, expected);

  // Output NULL when m2 equals 0.
  input = makeRowVector({makeFlatVector<double>({1, 1})});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCenteralMomentsAggResult(agg, input, expected);

  // Output NaN when m2 equals 0 for legacy aggregate.
  input = makeRowVector({makeFlatVector<double>({1, 1})});
  expected = makeRowVector({makeFlatVector<double>(
      std::vector<double>{std::numeric_limits<double>::quiet_NaN()})});
  testCenteralMomentsAggResult(agg, input, expected, true);
}

TEST_F(CentralMomentsAggregationTest, pearsonKurtosis) {
  auto agg = "kurtosis";
  auto input = makeRowVector({makeFlatVector<double>({1, 10, 100, 10, 1})});
  auto expected = makeRowVector(
      {makeFlatVector<double>(std::vector<double>{0.19432323191699075})});
  testCenteralMomentsAggResult(agg, input, expected);

  input = makeRowVector({makeFlatVector<double>({-10, -20, 100, 1000})});
  expected = makeRowVector(
      {makeFlatVector<double>(std::vector<double>{-0.7014368047529627})});
  testCenteralMomentsAggResult(agg, input, expected);

  // Even when the count is 2, Spark still produces non-null result.
  input = makeRowVector({makeFlatVector<double>({1, 2})});
  expected = makeRowVector({makeFlatVector<double>(std::vector<double>{-2.0})});
  testCenteralMomentsAggResult(agg, input, expected);

  // Output NULL when m2 equals 0.
  input = makeRowVector({makeFlatVector<double>({1, 1})});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCenteralMomentsAggResult(agg, input, expected);

  // Output NaN when m2 equals 0 for legacy aggregate.
  input = makeRowVector({makeFlatVector<double>({1, 1})});
  expected = makeRowVector({makeFlatVector<double>(
      std::vector<double>{std::numeric_limits<double>::quiet_NaN()})});
  testCenteralMomentsAggResult(agg, input, expected, true);
}

} // namespace
} // namespace facebook::velox::functions::aggregate::sparksql::test
