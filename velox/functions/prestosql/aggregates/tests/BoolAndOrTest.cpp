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

using facebook::velox::exec::test::PlanBuilder;

namespace facebook::velox::aggregate::test {

namespace {

struct TestParams {
  std::string veloxName;
  std::string duckDbName;

  std::string toString() const {
    return veloxName;
  }
};

class BoolAndOrTest : public virtual AggregationTestBase,
                      public testing::WithParamInterface<TestParams> {};

TEST_P(BoolAndOrTest, basic) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), BOOLEAN()});
  auto vectors = makeVectors(rowType, 1000, 10);
  createDuckDbTable(vectors);

  const auto veloxName = GetParam().veloxName;
  const auto duckDbName = GetParam().duckDbName;

  const auto partialAgg = fmt::format("{}(c1)", veloxName);

  // Global partial aggregation.
  auto agg = PlanBuilder()
                 .values(vectors)
                 .partialAggregation({}, {partialAgg})
                 .planNode();
  assertQuery(agg, fmt::format("SELECT {}(c1::TINYINT) FROM tmp", duckDbName));

  // Global final aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .partialAggregation({}, {partialAgg})
            .finalAggregation()
            .planNode();
  assertQuery(agg, fmt::format("SELECT {}(c1::TINYINT) FROM tmp", duckDbName));

  // Group by partial aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .project({"c0 % 10", "c1"})
            .partialAggregation({0}, {partialAgg})
            .planNode();
  assertQuery(
      agg,
      fmt::format(
          "SELECT c0 % 10, {}(c1::TINYINT) FROM tmp GROUP BY 1", duckDbName));

  // Group by final aggregation.
  agg = PlanBuilder()
            .values(vectors)
            .project({"c0 % 10", "c1"})
            .partialAggregation({0}, {partialAgg})
            .finalAggregation()
            .planNode();
  assertQuery(
      agg,
      fmt::format(
          "SELECT c0 % 10, {}(c1::TINYINT) FROM tmp GROUP BY 1", duckDbName));

  // encodings: use filter to wrap aggregation inputs in a dictionary.
  agg = PlanBuilder()
            .values(vectors)
            .filter("c0 % 2 = 0")
            .project({"c0 % 11", "c1"})
            .partialAggregation({0}, {partialAgg})
            .planNode();

  assertQuery(
      agg,
      fmt::format(
          "SELECT c0 % 11, {}(c1::TINYINT) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1",
          duckDbName));

  agg = PlanBuilder()
            .values(vectors)
            .filter("c0 % 2 = 0")
            .partialAggregation({}, {partialAgg})
            .planNode();
  assertQuery(
      agg,
      fmt::format(
          "SELECT {}(c1::TINYINT) FROM tmp WHERE c0 % 2 = 0", duckDbName));
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    BoolAndOrTest,
    BoolAndOrTest,
    testing::Values(
        TestParams{"bool_and", "bit_and"},
        TestParams{"every", "bit_and"},
        TestParams{"bool_or", "bit_or"}),
    [](auto p) { return p.param.toString(); });

} // namespace
} // namespace facebook::velox::aggregate::test
