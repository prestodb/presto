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

#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec::test;

class MarkDistinctTest : public OperatorTestBase {
 public:
  void runBasicTest(const VectorPtr& base) {
    const vector_size_t size = base->size() * 2;
    auto indices = makeIndices(size, [](auto row) { return row / 2; });
    auto data = wrapInDictionary(indices, size, base);

    auto isDistinct =
        makeFlatVector<bool>(size, [&](auto row) { return row % 2 == 0; });

    auto expectedResults = makeRowVector({data, isDistinct});

    auto plan = PlanBuilder()
                    .values({makeRowVector({data})})
                    .markDistinct("c0_distinct", {"c0"})
                    .planNode();

    auto results = AssertQueryBuilder(plan).copyResults(pool());
    assertEqualVectors(expectedResults, results);
  }
};

template <typename T>
class MarkDistinctPODTest : public MarkDistinctTest {};

using MyTypes = ::testing::Types<int16_t, int32_t, int64_t, float, double>;
TYPED_TEST_SUITE(MarkDistinctPODTest, MyTypes);

TYPED_TEST(MarkDistinctPODTest, basic) {
  auto data = VectorTestBase::makeFlatVector<TypeParam>(
      1'000, [](auto row) { return row; }, [](auto row) { return row == 7; });

  MarkDistinctTest::runBasicTest(data);
}

TEST_F(MarkDistinctTest, tinyint) {
  auto data = makeFlatVector<int8_t>(
      256, [](auto row) { return row; }, [](auto row) { return row == 7; });

  runBasicTest(data);
}

TEST_F(MarkDistinctTest, boolean) {
  auto data = makeNullableFlatVector<bool>({true, false, std::nullopt});

  runBasicTest(data);
}

TEST_F(MarkDistinctTest, varchar) {
  auto base = makeFlatVector<StringView>({
      "{1, 2, 3, 4, 5}",
      "{1, 2, 3}",
  });
  runBasicTest(base);
}

TEST_F(MarkDistinctTest, array) {
  auto base = makeArrayVector<int64_t>({
      {1, 2, 3, 4, 5},
      {1, 2, 3},
  });
  runBasicTest(base);
}

TEST_F(MarkDistinctTest, map) {
  auto base = makeMapVector<int8_t, int32_t>(
      {{{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}}, {{1, 1}, {1, 1}, {1, 1}}});
  runBasicTest(base);
}

TEST_F(MarkDistinctTest, row) {
  auto base = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3, 4, 5},
          {1, 2, 3},
      }),
      makeMapVector<int8_t, int32_t>({
          {{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}},
          {{1, 1}, {1, 1}, {1, 1}},
      }),
  });
  runBasicTest(base);
}

TEST_F(MarkDistinctTest, aggregation) {
  // Simulate the input over 3 splits.
  std::vector<RowVectorPtr> vectors = {
      makeRowVector({
          makeFlatVector<int32_t>({1, 1}),
          makeFlatVector<int32_t>({1, 1}),
          makeFlatVector<int32_t>({1, 2}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({1, 2}),
          makeFlatVector<int32_t>({1, 1}),
          makeFlatVector<int32_t>({1, 2}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({2, 2}),
          makeFlatVector<int32_t>({2, 3}),
          makeFlatVector<int32_t>({1, 2}),
      }),
  };

  createDuckDbTable(vectors);

  auto plan =
      PlanBuilder()
          .values(vectors)
          .markDistinct("c1_distinct", {"c0", "c1"})
          .markDistinct("c2_distinct", {"c0", "c2"})
          .singleAggregation(
              {"c0"}, {"sum(c1)", "sum(c2)"}, {"c1_distinct", "c2_distinct"})
          .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .assertResults(
          "SELECT c0, sum(distinct c1), sum(distinct c2) FROM tmp GROUP BY 1");
}
