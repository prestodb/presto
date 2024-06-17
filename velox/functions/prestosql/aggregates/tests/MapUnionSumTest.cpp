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
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

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

TEST_F(MapUnionSumTest, globalVarcharKey) {
  std::vector<std::string> keyStrings = {
      "Tall mountains",
      "Wide rivers",
      "Deep oceans",
      "Thick dark forests",
      "Expansive vistas",
  };
  std::vector<StringView> keys;
  for (const auto& key : keyStrings) {
    keys.push_back(StringView(key));
  }

  auto data = makeRowVector({
      makeNullableMapVector<StringView, int64_t>({
          {{}}, // empty map
          std::nullopt, // null map
          {{{keys[0], 10}, {keys[1], 20}}},
          {{{keys[0], 11}, {keys[2], 30}, {keys[3], 40}}},
          {{{keys[2], 30}, {keys[4], 50}, {keys[0], 12}}},
      }),
  });

  auto expected = makeRowVector({
      makeMapVector<StringView, int64_t>({
          {{keys[0], 33},
           {keys[1], 20},
           {keys[2], 60},
           {keys[3], 40},
           {keys[4], 50}},
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
      AssertQueryBuilder(plan).copyResults(pool()), "Value 140 exceeds 127");

  data = makeRowVector({
      makeNullableMapVector<int64_t, int8_t>({
          {{{1, -10}, {2, -20}}},
          {{{1, -100}, {3, -30}, {4, -40}}},
          {{{3, -30}, {5, -50}, {1, -30}}},
      }),
  });

  plan = PlanBuilder()
             .values({data})
             .singleAggregation({}, {"map_union_sum(c0)"})
             .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()),
      "Value -140 is less than -128");
}

TEST_F(MapUnionSumTest, smallintOverflow) {
  const int16_t largeValue = std::numeric_limits<int16_t>::max() - 20;
  const int16_t smallValue = std::numeric_limits<int16_t>::min() + 20;
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
      "Value 32787 exceeds 32767");

  data = makeRowVector({
      makeNullableMapVector<int64_t, int16_t>({
          {{{1, -10}, {2, -20}}},
          {{{1, smallValue}, {3, -30}, {4, -40}}},
          {{{3, -30}, {5, -50}, {1, -30}}},
      }),
  });

  plan = PlanBuilder()
             .values({data})
             .singleAggregation({}, {"map_union_sum(c0)"})
             .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()),
      "Value -32788 is less than -32768");
}

TEST_F(MapUnionSumTest, integerOverflow) {
  const int32_t largeValue = std::numeric_limits<int32_t>::max() - 20;
  const int32_t smallValue = std::numeric_limits<int32_t>::min() + 20;
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
      "Value 2147483667 exceeds 2147483647");

  data = makeRowVector({
      makeNullableMapVector<int64_t, int32_t>({
          {{{1, -10}, {2, -20}}},
          {{{1, smallValue}, {3, -30}, {4, -40}}},
          {{{3, -30}, {5, -50}, {1, -30}}},
      }),
  });

  plan = PlanBuilder()
             .values({data})
             .singleAggregation({}, {"map_union_sum(c0)"})
             .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()),
      "Value -2147483668 is less than -2147483648");
}

TEST_F(MapUnionSumTest, bigintOverflow) {
  const int64_t largeValue = std::numeric_limits<int64_t>::max() - 20;
  const int64_t smallValue = std::numeric_limits<int64_t>::min() + 20;
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
      "Value 9223372036854775827 exceeds 9223372036854775807");

  data = makeRowVector({
      makeNullableMapVector<int64_t, int64_t>({
          {{{1, -10}, {2, -20}}},
          {{{1, smallValue}, {3, -30}, {4, -40}}},
          {{{3, -30}, {5, -50}, {1, -30}}},
      }),
  });

  plan = PlanBuilder()
             .values({data})
             .singleAggregation({}, {"map_union_sum(c0)"})
             .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()),
      "Value -9223372036854775828 is less than -9223372036854775808");
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

TEST_F(MapUnionSumTest, groupByVarcharKey) {
  std::vector<std::string> keyStrings = {
      "Tall mountains",
      "Wide rivers",
      "Deep oceans",
      "Thick dark forests",
      "Expansive vistas",
  };
  std::vector<StringView> keys;
  for (const auto& key : keyStrings) {
    keys.push_back(StringView(key));
  }

  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 1, 2, 1}),
      makeNullableMapVector<StringView, int64_t>({
          {}, // empty map
          std::nullopt, // null map
          {{{keys[0], 10}, {keys[1], 20}}},
          {{{keys[0], 11}, {keys[2], 30}, {keys[3], 40}}},
          {{{keys[2], 30}, {keys[4], 50}, {keys[0], 12}}},
      }),
  });

  auto expected = makeRowVector({
      makeFlatVector<int64_t>({1, 2}),
      makeMapVector<StringView, int64_t>({
          {{keys[0], 22}, {keys[1], 20}, {keys[2], 30}, {keys[4], 50}},
          {{keys[0], 11}, {keys[2], 30}, {keys[3], 40}},
      }),
  });

  testAggregations({data}, {"c0"}, {"map_union_sum(c1)"}, {expected});
}

TEST_F(MapUnionSumTest, floatingPointKeys) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 1, 2, 1, 1, 2, 2}),
      makeMapVectorFromJson<float, int64_t>({
          "{1.1: 10, 1.2: 20, 1.3: 30}",
          "{2.1: 10, 1.2: 20, 2.3: 30}",
          "{3.1: 10, 1.2: 20, 2.3: 30}",
          "{}",
          "null",
          "{4.1: 10, 4.2: 20, 2.3: 30}",
          "{5.1: 10, 4.2: 20, 2.3: 30}",
          "{6.1: 10, 6.2: 20, 6.3: 30}",
      }),
  });

  auto expected = makeRowVector({
      makeMapVectorFromJson<float, int64_t>({
          "{1.1: 10, 1.2: 60, 1.3: 30, 2.1: 10, 2.3: 120, 3.1: 10, 4.1: 10, "
          "4.2: 40, 5.1: 10, 6.1: 10, 6.2: 20, 6.3: 30}",
      }),
  });

  testAggregations({data}, {}, {"map_union_sum(c1)"}, {expected});
}

TEST_F(MapUnionSumTest, nanKeys) {
  // Verify that NaNs with different binary representations are considered equal
  // and deduplicated when used as keys in the output map.
  constexpr double kNan = std::numeric_limits<double>::quiet_NaN();
  constexpr double kSNaN = std::numeric_limits<double>::signaling_NaN();

  // Global aggregation.
  auto data = makeRowVector(
      {makeNullableMapVector<double, int32_t>({
           {{{kSNaN, 10}, {2, 20}}},
           {{{kNan, 1}, {3, 30}, {5, 50}}},
           {{{3, 30}, {kSNaN, 4}, {1, 30}}},
       }),
       makeFlatVector<int32_t>({1, 1, 2})});

  auto expected = makeRowVector({
      makeMapVector<double, int32_t>({
          {{1, 30}, {2, 20}, {3, 60}, {5, 50}, {kNan, 15}},
      }),
  });

  testAggregations({data}, {}, {"map_union_sum(c0)"}, {expected});

  // Group by aggregation.
  expected = makeRowVector(
      {makeMapVector<double, int32_t>({
           {{2, 20}, {3, 30}, {5, 50}, {kNan, 11}},
           {{1, 30}, {3, 30}, {kNan, 4}},
       }),
       makeFlatVector<int32_t>({1, 2})});

  testAggregations(
      {data}, {"c1"}, {"map_union_sum(c0)"}, {"a0", "c1"}, {expected});
}

} // namespace
} // namespace facebook::velox::aggregate::test
