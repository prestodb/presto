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
#include "velox/functions/prestosql/window/tests/WindowTestBase.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

namespace {

static const std::vector<std::string> kAggregateFunctions = {
    std::string("sum(c2)"),
    std::string("min(c2)"),
    std::string("max(c2)"),
    std::string("count(c2)"),
    std::string("avg(c2)"),
    std::string("sum(1)")};

// This AggregateWindowTestBase class is used to instantiate parameterized
// aggregate window function tests. The parameters are (function, over clause).
// The window function is tested for the over clause and all combinations of
// frame clauses. Doing so helps to construct input vectors and DuckDB table
// only once for the (function, over clause) combination over all frame clauses.
struct AggregateWindowTestParam {
  const std::string function;
  const std::string overClause;
};

class AggregateWindowTestBase : public WindowTestBase {
 protected:
  explicit AggregateWindowTestBase(const AggregateWindowTestParam& testParam)
      : function_(testParam.function), overClause_(testParam.overClause) {}

  void testWindowFunction(const std::vector<RowVectorPtr>& vectors) {
    WindowTestBase::testWindowFunction(
        vectors, function_, {overClause_}, kFrameClauses);
  }

  const std::string function_;
  const std::string overClause_;
};

std::vector<AggregateWindowTestParam> getAggregateTestParams() {
  std::vector<AggregateWindowTestParam> params;
  for (auto function : kAggregateFunctions) {
    for (auto overClause : kOverClauses) {
      params.push_back({function, overClause});
    }
  }
  return params;
}

class SimpleAggregatesTest
    : public AggregateWindowTestBase,
      public testing::WithParamInterface<AggregateWindowTestParam> {
 public:
  SimpleAggregatesTest() : AggregateWindowTestBase(GetParam()) {}
};

// Tests function with a dataset with uniform partitions.
TEST_P(SimpleAggregatesTest, basic) {
  testWindowFunction({makeSimpleVector(10)});
}

// Tests function with a dataset with a single partition containing all the
// rows.
TEST_P(SimpleAggregatesTest, singlePartition) {
  testWindowFunction({makeSinglePartitionVector(100)});
}

// Tests function with a dataset with a single partition but 2 input row
// vectors.
TEST_P(SimpleAggregatesTest, multiInput) {
  testWindowFunction(
      {makeSinglePartitionVector(250), makeSinglePartitionVector(50)});
}

// Tests function with a dataset where all partitions have a single row.
TEST_P(SimpleAggregatesTest, singleRowPartitions) {
  testWindowFunction({makeSingleRowPartitionsVector(50)});
}

// Tests function with a randomly generated input dataset.
TEST_P(SimpleAggregatesTest, randomInput) {
  testWindowFunction({makeRandomInputVector(50)});
}

// Instantiate all the above tests for each combination of aggregate function
// and over clause.
VELOX_INSTANTIATE_TEST_SUITE_P(
    AggregatesTestInstantiation,
    SimpleAggregatesTest,
    testing::ValuesIn(getAggregateTestParams()));

class StringAggregatesTest : public WindowTestBase {};

// Test for an aggregate function with strings that needs out of line storage.
TEST_F(StringAggregatesTest, nonFixedWidthAggregate) {
  auto size = 10;
  auto input = {makeRowVector({
      makeRandomInputVector(BIGINT(), size, 0.2),
      makeRandomInputVector(SMALLINT(), size, 0.2),
      makeRandomInputVector(VARCHAR(), size, 0.3),
      makeRandomInputVector(VARCHAR(), size, 0.3),
  })};

  testWindowFunction(input, "min(c2)", kOverClauses);
  testWindowFunction(input, "max(c2)", kOverClauses);
}

}; // namespace
}; // namespace facebook::velox::window::test
