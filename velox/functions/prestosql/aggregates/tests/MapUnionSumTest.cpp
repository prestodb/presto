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
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/functions/lib/aggregates/tests/AggregationTestBase.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {

namespace {

class MapUnionSumTest : public AggregationTestBase {};

TEST_F(MapUnionSumTest, global) {
  auto data = makeRowVector({
      makeNullableMapVector<int64_t, int64_t>({
          {{}}, // empty map
          std::nullopt, // null map
          {{{1, 10}, {2, 20}}},
          {{{1, 11}, {3, 30}, {4, 40}}},
          {{{3, 30}, {5, 50}, {1, 12}}},
      }),
  });

  auto expected = makeRowVector({
      makeMapVector<int64_t, int64_t>({
          {{1, 33}, {2, 20}, {3, 60}, {4, 40}, {5, 50}},
      }),
  });

  testAggregations({data}, {}, {"map_union_sum(c0)"}, {expected});
}

TEST_F(MapUnionSumTest, nullAndEmptyMaps) {
  auto allEmptyMaps = makeRowVector({
      makeMapVector<int64_t, int64_t>({
          {},
          {},
          {},
      }),
  });

  auto expectedEmpty = makeRowVector({
      makeMapVector<int64_t, int64_t>({
          {},
      }),
  });

  testAggregations({allEmptyMaps}, {}, {"map_union_sum(c0)"}, {expectedEmpty});

  auto allNullMaps = makeRowVector({
      makeNullableMapVector<int64_t, int64_t>({
          std::nullopt,
          std::nullopt,
          std::nullopt,
      }),
  });

  auto expectedNull = makeRowVector({
      makeNullableMapVector<int64_t, int64_t>({
          std::nullopt,
      }),
  });

  testAggregations({allNullMaps}, {}, {"map_union_sum(c0)"}, {expectedNull});

  auto emptyAndNullMaps = makeRowVector({
      makeNullableMapVector<int64_t, int64_t>({
          std::nullopt,
          {{}},
          std::nullopt,
          {{}},
      }),
  });

  testAggregations(
      {emptyAndNullMaps}, {}, {"map_union_sum(c0)"}, {expectedEmpty});
}

TEST_F(MapUnionSumTest, tinyintOverflow) {
  auto data = makeRowVector({
      makeNullableMapVector<int64_t, int8_t>({
          {{{1, 10}, {2, 20}}},
          {{{1, 100}, {3, 30}, {4, 40}}},
          {{{3, 30}, {5, 50}, {1, 30}}},
      }),
  });

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation({}, {"map_union_sum(c0)"})
                  .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()),
      "integer overflow: 110 + 30");
}

TEST_F(MapUnionSumTest, smallintOverflow) {
  const int16_t largeValue = std::numeric_limits<int16_t>::max() - 20;
  auto data = makeRowVector({
      makeNullableMapVector<int64_t, int16_t>({
          {{{1, 10}, {2, 20}}},
          {{{1, largeValue}, {3, 30}, {4, 40}}},
          {{{3, 30}, {5, 50}, {1, 30}}},
      }),
  });

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation({}, {"map_union_sum(c0)"})
                  .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()),
      "integer overflow: 32757 + 30");
}

TEST_F(MapUnionSumTest, integerOverflow) {
  const int32_t largeValue = std::numeric_limits<int32_t>::max() - 20;
  auto data = makeRowVector({
      makeNullableMapVector<int64_t, int32_t>({
          {{{1, 10}, {2, 20}}},
          {{{1, largeValue}, {3, 30}, {4, 40}}},
          {{{3, 30}, {5, 50}, {1, 30}}},
      }),
  });

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation({}, {"map_union_sum(c0)"})
                  .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()),
      "integer overflow: 2147483637 + 30");
}

TEST_F(MapUnionSumTest, bigintOverflow) {
  const int64_t largeValue = std::numeric_limits<int64_t>::max() - 20;
  auto data = makeRowVector({
      makeNullableMapVector<int64_t, int64_t>({
          {{{1, 10}, {2, 20}}},
          {{{1, largeValue}, {3, 30}, {4, 40}}},
          {{{3, 30}, {5, 50}, {1, 30}}},
      }),
  });

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation({}, {"map_union_sum(c0)"})
                  .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()),
      "integer overflow: 9223372036854775797 + 30");
}

TEST_F(MapUnionSumTest, floatNan) {
  constexpr float kInf = std::numeric_limits<float>::infinity();
  constexpr float kNan = std::numeric_limits<float>::quiet_NaN();

  auto data = makeRowVector({
      makeNullableMapVector<int64_t, float>({
          {{{1, 10}, {2, 20}}},
          {{{1, kNan}, {3, 30}, {5, 50}}},
          {{{3, 30}, {5, kInf}, {1, 30}}},
      }),
  });

  auto expected = makeRowVector({
      makeMapVector<int64_t, float>({
          {{1, kNan}, {2, 20}, {3, 60}, {5, kInf}},
      }),
  });

  testAggregations({data}, {}, {"map_union_sum(c0)"}, {expected});
}

TEST_F(MapUnionSumTest, doubleNan) {
  constexpr float kInf = std::numeric_limits<double>::infinity();
  constexpr float kNan = std::numeric_limits<double>::quiet_NaN();

  auto data = makeRowVector({
      makeNullableMapVector<int64_t, double>({
          {{{1, 10}, {2, 20}}},
          {{{1, kNan}, {3, 30}, {5, 50}}},
          {{{3, 30}, {5, kInf}, {1, 30}}},
      }),
  });

  auto expected = makeRowVector({
      makeMapVector<int64_t, double>({
          {{1, kNan}, {2, 20}, {3, 60}, {5, kInf}},
      }),
  });

  testAggregations({data}, {}, {"map_union_sum(c0)"}, {expected});
}

TEST_F(MapUnionSumTest, groupBy) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 1, 2, 1}),
      makeNullableMapVector<int64_t, int64_t>({
          {}, // empty map
          std::nullopt, // null map
          {{{1, 10}, {2, 20}}},
          {{{1, 11}, {3, 30}, {4, 40}}},
          {{{3, 30}, {5, 50}, {1, 12}}},
      }),
  });

  auto expected = makeRowVector({
      makeFlatVector<int64_t>({1, 2}),
      makeMapVector<int64_t, int64_t>({
          {{1, 22}, {2, 20}, {3, 30}, {5, 50}},
          {{1, 11}, {3, 30}, {4, 40}},
      }),
  });

  testAggregations({data}, {"c0"}, {"map_union_sum(c1)"}, {expected});
}

} // namespace
} // namespace facebook::velox::aggregate::test
