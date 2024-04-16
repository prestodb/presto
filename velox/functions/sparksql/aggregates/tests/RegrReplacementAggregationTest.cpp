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

#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::functions::aggregate::sparksql::test {

namespace {
class RegrReplacementAggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerAggregateFunctions("spark_");
  }
};

TEST_F(RegrReplacementAggregationTest, groupBy) {
  vector_size_t size = 1'000;
  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 10; }),
      makeFlatVector<double>(size, [](auto row) { return row * 0.1; }),
  });

  createDuckDbTable({data});
  // m2 is equal to the population variance divided by the size of the group.
  testAggregations(
      {data},
      {"c0"},
      {"spark_regr_replacement(c1)"},
      {"c0", "a0 / 100.0"},
      "select c0, var_pop(c1) from tmp group by c0");
  testAggregationsWithCompanion(
      {data},
      [](auto& /*builder*/) {},
      {"c0"},
      {"spark_regr_replacement(c1)"},
      {{DOUBLE()}},
      {"c0", "a0 / 100.0"},
      "select c0, var_pop(c1) from tmp group by c0");
}

TEST_F(RegrReplacementAggregationTest, global) {
  vector_size_t size = 1'000;
  auto data = makeRowVector({
      makeFlatVector<double>(size, [](auto row) { return row * 0.3; }),
  });

  createDuckDbTable({data});
  // m2 is equal to the population variance divided by the size of the group.
  testAggregations(
      {data},
      {},
      {"spark_regr_replacement(c0)"},
      {"a0 / 1000.0"},
      "select var_pop(c0) from tmp");
  testAggregationsWithCompanion(
      {data},
      [](auto& /*builder*/) {},
      {},
      {"spark_regr_replacement(c0)"},
      {{DOUBLE()}},
      {"a0 / 1000.0"},
      "select var_pop(c0) from tmp");
}

} // namespace
} // namespace facebook::velox::functions::aggregate::sparksql::test
