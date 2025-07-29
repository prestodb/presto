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

class CovarianceAggregatesTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerAggregateFunctions("spark_");
  }

  void testCovarianceAggResult(
      const std::string& agg,
      const RowVectorPtr& input,
      const RowVectorPtr& expected,
      bool legacy = false) {
    auto plan =
        PlanBuilder()
            .values({input})
            .singleAggregation({}, {fmt::format("spark_{}(c0, c1)", agg)})
            .planNode();
    AssertQueryBuilder(plan)
        .config(
            core::QueryConfig::kSparkLegacyStatisticalAggregate,
            legacy ? "true" : "false")
        .assertResults({expected});
  }
};

TEST_F(CovarianceAggregatesTest, corr) {
  auto agg = "corr";
  auto input = makeRowVector(
      {makeFlatVector<double>({1, 2, 3}), makeFlatVector<double>({1, 2, 3})});
  auto expected =
      makeRowVector({makeFlatVector<double>(std::vector<double>{1.0})});
  testCovarianceAggResult(agg, input, expected);

  input = makeRowVector(
      {makeFlatVector<double>({1, 3, 5}), makeFlatVector<double>({2, 4, 6})});
  expected = makeRowVector({makeFlatVector<double>(std::vector<double>{1.0})});
  testCovarianceAggResult(agg, input, expected);

  // Output NULL when m2x equals 0.
  input = makeRowVector(
      {makeFlatVector<double>({1, 1, 1}), makeFlatVector<double>({2, 2, 2})});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected);

  // Output NULL when count equals 1.
  input = makeRowVector({makeFlatVector<double>(1), makeFlatVector<double>(1)});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected);

  // Output NULL when count equals 0.
  input = makeRowVector(
      {makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt}),
       makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt})});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected);

  // Output NULL when m2x equals 0 for legacy aggregate.
  input = makeRowVector(
      {makeFlatVector<double>({1, 1, 1}), makeFlatVector<double>({2, 2, 2})});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected, true);

  // Output NaN when count equals 1 for legacy aggregate.
  input = makeRowVector({makeFlatVector<double>(1), makeFlatVector<double>(1)});
  expected = makeRowVector({makeFlatVector<double>(
      std::vector<double>{std::numeric_limits<double>::quiet_NaN()})});
  testCovarianceAggResult(agg, input, expected, true);

  // Output NaN when m2x or m2y equals 0 but c2 is NaN.
  input = makeRowVector(
      {makeFlatVector<double>({0, 0, 0, 0, 0, 0}),
       makeFlatVector<double>(
           {std::numeric_limits<double>::quiet_NaN(), 0, 0, 0, 0, 0})});

  expected = makeRowVector({makeFlatVector<double>(
      std::vector<double>{std::numeric_limits<double>::quiet_NaN()})});
  testCovarianceAggResult(agg, input, expected);
  testCovarianceAggResult(agg, input, expected, true);

  // Output NULL when count equals 0 for legacy aggregate.
  input = makeRowVector(
      {makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt}),
       makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt})});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected, true);
}

TEST_F(CovarianceAggregatesTest, covarSamp) {
  auto agg = "covar_samp";
  auto input = makeRowVector(
      {makeFlatVector<double>({1, 2, 3}), makeFlatVector<double>({1, 2, 3})});
  auto expected =
      makeRowVector({makeFlatVector<double>(std::vector<double>{1.0})});
  testCovarianceAggResult(agg, input, expected);

  input = makeRowVector(
      {makeFlatVector<double>({1, 3, 5}), makeFlatVector<double>({2, 4, 6})});
  expected = makeRowVector({makeFlatVector<double>(std::vector<double>{4.0})});
  testCovarianceAggResult(agg, input, expected);

  // Output NULL when count equals 0.
  input = makeRowVector(
      {makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt}),
       makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt})});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected);

  // Output NULL when count equals 1.
  input = makeRowVector({makeFlatVector<double>(1), makeFlatVector<double>(1)});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected);

  // Output NULL when count equals 0 for legacy aggregate.
  input = makeRowVector(
      {makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt}),
       makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt})});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected, true);

  // Output NaN when m2 equals 1 for legacy aggregate.
  input = makeRowVector({makeFlatVector<double>(1), makeFlatVector<double>(1)});
  expected = makeRowVector({makeFlatVector<double>(
      std::vector<double>{std::numeric_limits<double>::quiet_NaN()})});
  testCovarianceAggResult(agg, input, expected, true);

  // Output NaN when c2 is ±inf.
  input = makeRowVector(
      {makeFlatVector<double>({22, std::numeric_limits<double>::infinity()}),
       makeFlatVector<double>({0.688, 0.225})});
  expected = makeRowVector({makeFlatVector<double>(
      std::vector<double>{std::numeric_limits<double>::quiet_NaN()})});
  testCovarianceAggResult(agg, input, expected);
  testCovarianceAggResult(agg, input, expected, true);
}

} // namespace
} // namespace facebook::velox::functions::aggregate::sparksql::test
