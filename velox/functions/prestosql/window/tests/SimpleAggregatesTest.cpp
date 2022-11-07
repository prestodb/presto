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

class SimpleAggregatesTest : public WindowTestBase {
 protected:
  SimpleAggregatesTest(const std::string& function) : function_(function) {}

  RowVectorPtr makeBasicVectors(vector_size_t size) {
    return makeRowVector({
        makeFlatVector<int32_t>(
            size, [](auto row) -> int32_t { return row % 10; }),
        makeFlatVector<int32_t>(
            size, [](auto row) -> int32_t { return row % 7; }),
        makeFlatVector<int32_t>(size, [](auto row) -> int32_t { return row; }),
    });
  }

  RowVectorPtr makeSinglePartitionVector(vector_size_t size) {
    return makeRowVector({
        makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
        makeFlatVector<int32_t>(size, [](auto row) { return row % 50; }),
        makeFlatVector<int32_t>(size, [](auto row) -> int32_t { return row; }),
    });
  }

  void testWindowFunction(
      const std::vector<RowVectorPtr>& vectors,
      const std::vector<std::string>& overClauses) {
    WindowTestBase::testWindowFunction(vectors, function_, overClauses);
  }

  const std::string function_;
};

class MultiAggregatesTest : public SimpleAggregatesTest,
                            public testing::WithParamInterface<std::string> {
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
  auto vectors = makeFuzzVectors(
      ROW({"c0", "c1", "c2", "c3"},
          {BIGINT(), SMALLINT(), INTEGER(), BIGINT()}),
      10,
      2);
  createDuckDbTable(vectors);

  std::vector<std::string> overClauses = {
      "partition by c0 order by c1, c2, c3",
      "partition by c1 order by c0, c2, c3",
      "partition by c0 order by c1 desc, c2, c3",
      "partition by c1 order by c0 desc, c2, c3",
      "partition by c0 order by c1",
      "partition by c0 order by c2",
      "partition by c0 order by c3",
      "partition by c1 order by c0 desc",
      "partition by c0, c1 order by c2, c3",
      "partition by c0, c1 order by c2",
      "partition by c0, c1 order by c2 desc",
      "order by c0, c1, c2, c3",
      "partition by c0, c1, c2, c3",
  };

  testWindowFunction(vectors, overClauses);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    SimpleAggregatesTest,
    MultiAggregatesTest,
    testing::ValuesIn({
        std::string("sum(c2)"),
        std::string("min(c2)"),
        std::string("max(c2)"),
        std::string("count(c2)"),
        std::string("avg(c2)"),
    }));

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
