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

class VarianceAggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerAggregateFunctions("spark_");
  }

  void testStatisticalAggregate(
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

  void testVarianceAggregate(const std::string& agg) {
    auto input = makeRowVector({makeFlatVector<double>({2, 4, 4, 4})});
    auto expected =
        makeRowVector({makeFlatVector<double>(std::vector<double>{1.0})});
    testStatisticalAggregate(agg, input, expected);

    input = makeRowVector({makeFlatVector<double>({1, 1})});
    expected =
        makeRowVector({makeFlatVector<double>(std::vector<double>{0.0})});
    testStatisticalAggregate(agg, input, expected);

    // Output NULL when count equals 0.
    input = makeRowVector({makeNullableFlatVector<double>(
        std::vector<std::optional<double>>{std::nullopt})});
    expected = makeRowVector({makeNullableFlatVector<double>(
        std::vector<std::optional<double>>{std::nullopt})});
    testStatisticalAggregate(agg, input, expected);

    // Output NULL when count equals 1.
    input = makeRowVector({makeFlatVector<double>(1)});
    expected = makeRowVector({makeNullableFlatVector<double>(
        std::vector<std::optional<double>>{std::nullopt})});
    testStatisticalAggregate(agg, input, expected);

    // Output NaN when m2 equals 1 for legacy aggregate.
    input = makeRowVector({makeFlatVector<double>(1)});
    expected = makeRowVector({makeFlatVector<double>(
        std::vector<double>{std::numeric_limits<double>::quiet_NaN()})});
    testStatisticalAggregate(agg, input, expected, true);

    // Output NULL when count equals 0 for legacy aggregate.
    input = makeRowVector({makeNullableFlatVector<double>(
        std::vector<std::optional<double>>{std::nullopt})});
    expected = makeRowVector({makeNullableFlatVector<double>(
        std::vector<std::optional<double>>{std::nullopt})});
    testStatisticalAggregate(agg, input, expected, true);
  }
};

TEST_F(VarianceAggregationTest, stddev) {
  testVarianceAggregate("stddev");
}

TEST_F(VarianceAggregationTest, stddev_samp) {
  testVarianceAggregate("stddev_samp");
}

TEST_F(VarianceAggregationTest, variance) {
  testVarianceAggregate("variance");
}

TEST_F(VarianceAggregationTest, var_samp) {
  testVarianceAggregate("var_samp");
}

} // namespace
} // namespace facebook::velox::functions::aggregate::sparksql::test
