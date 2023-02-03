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

struct TestParam {
  const std::string aggregateFunction;
  const std::string frameClause;
};

std::vector<TestParam> getTestParams(
    const std::vector<std::string>& frameClauses) {
  std::vector<TestParam> params;
  for (auto aggregateFunction : kAggregateFunctions) {
    for (auto frameClause : frameClauses) {
      params.push_back({aggregateFunction, frameClause});
    }
  }
  return params;
}

class SimpleAggregatesTest : public WindowTestBase {
 protected:
  SimpleAggregatesTest(const TestParam& testParam)
      : function_(testParam.aggregateFunction),
        frameClause_(testParam.frameClause) {}

  RowVectorPtr makeBasicVectors(vector_size_t size) {
    return makeRowVector({
        makeFlatVector<int32_t>(size, [](auto row) { return row % 10; }),
        // These columns are used for k PRECEDING/FOLLOWING frame bounds. So
        // they should have values >= 1.
        makeFlatVector<int64_t>(size, [](auto row) { return row % 7 + 1; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row + 1; }),
    });
  }

  RowVectorPtr makeSinglePartitionVector(vector_size_t size) {
    return makeRowVector({
        makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
        // These columns are used for k PRECEDING/FOLLOWING frame bounds. So
        // they should have values >= 1.
        makeFlatVector<int64_t>(size, [](auto row) { return row % 50 + 1; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row + 1; }),
    });
  }

  void testWindowFunction(
      const std::vector<RowVectorPtr>& vectors,
      const std::vector<std::string>& overClauses) {
    WindowTestBase::testWindowFunction(
        vectors, function_, overClauses, frameClause_);
  }

  const std::string function_;
  const std::string frameClause_;
};

class MultiAggregatesTest : public SimpleAggregatesTest,
                            public testing::WithParamInterface<TestParam> {
 public:
  MultiAggregatesTest() : SimpleAggregatesTest(GetParam()) {}
};

TEST_P(MultiAggregatesTest, basic) {
  SimpleAggregatesTest::testWindowFunction(
      {makeBasicVectors(10)}, kBasicOverClauses);
}

TEST_P(MultiAggregatesTest, basicWithSortOrders) {
  SimpleAggregatesTest::testWindowFunction(
      {makeBasicVectors(10)}, kSortOrderBasedOverClauses);
}

TEST_P(MultiAggregatesTest, singlePartition) {
  // Test all input rows in a single partition.
  SimpleAggregatesTest::testWindowFunction(
      {makeSinglePartitionVector(100)}, kBasicOverClauses);
}

TEST_P(MultiAggregatesTest, singlePartitionWithSortOrders) {
  SimpleAggregatesTest::testWindowFunction(
      {makeSinglePartitionVector(100)}, kSortOrderBasedOverClauses);
}

TEST_P(MultiAggregatesTest, randomInput) {
  auto size = 50;
  std::vector<VectorPtr> vectors;
  // Columns c1, c2 are used for k PRECEDING/FOLLOWING frame bounds. So they
  // should have values >= 1.
  auto input = makeRowVector(
      {makeFlatFuzzVector(BIGINT(), size),
       makeFlatVector<int64_t>(size, [](auto row) { return row % 5 + 1; }),
       makeFlatVector<int64_t>(size, [](auto row) { return row + 1; }),
       makeFlatFuzzVector(INTEGER(), size),
       makeFlatFuzzVector(BIGINT(), size)});

  createDuckDbTable({input});
  // Add columns c3, c4 in sort order in overClauses to impose a deterministic
  // output row order in the tests.
  testWindowFunction(
      {input}, addSuffixToClauses(", c3, c4", kBasicOverClauses));
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    SimpleAggregatesTest,
    MultiAggregatesTest,
    testing::ValuesIn(getTestParams(kFrameClauses)));

class StringAggregatesTest : public WindowTestBase {};

TEST_F(StringAggregatesTest, nonFixedWidthAggregate) {
  auto vectors = makeFuzzVectors(
      ROW({"c0", "c1", "c2"}, {BIGINT(), SMALLINT(), VARCHAR()}), 10, 2);
  createDuckDbTable(vectors);
  testWindowFunction(vectors, "min(c2)", kBasicOverClauses);
  testWindowFunction(vectors, "max(c2)", kSortOrderBasedOverClauses);
}

}; // namespace
}; // namespace facebook::velox::window::test
