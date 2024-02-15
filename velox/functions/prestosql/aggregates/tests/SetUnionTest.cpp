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
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {

namespace {

constexpr int64_t kLongMax = std::numeric_limits<int64_t>::max();
constexpr int64_t kLongMin = std::numeric_limits<int64_t>::min();
constexpr int128_t kHugeMax = std::numeric_limits<int128_t>::max();
constexpr int128_t kHugeMin = std::numeric_limits<int128_t>::min();

class SetUnionTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }
};

TEST_F(SetUnionTest, global) {
  auto data = makeRowVector({
      makeArrayVector<int32_t>({
          {},
          {1, 2, 3},
          {1, 2},
          {2, 3, 4, 5},
          {6, 7},
      }),
  });

  auto expected = makeRowVector({
      makeArrayVector<int32_t>({
          {1, 2, 3, 4, 5, 6, 7},
      }),
  });

  testAggregations(
      {data}, {}, {"set_union(c0)"}, {"array_sort(a0)"}, {expected});

  // Null inputs.
  data = makeRowVector({
      makeArrayVectorFromJson<int32_t>({
          "[]",
          "[1, 2, null, 3]",
          "[1, 2]",
          "null",
          "[2, 3, 4, null, 5]",
          "[6, 7]",
      }),
  });

  expected = makeRowVector({
      makeArrayVectorFromJson<int32_t>({"[1, 2, 3, 4, 5, 6, 7, null]"}),
  });

  testAggregations(
      {data}, {}, {"set_union(c0)"}, {"array_sort(a0)"}, {expected});

  // All nulls arrays.
  data = makeRowVector({
      makeAllNullArrayVector(10, INTEGER()),
  });

  expected = makeRowVector({
      // Empty array: [].
      makeArrayVector<int32_t>({{}}),
  });

  testAggregations(
      {data}, {}, {"set_union(c0)"}, {"array_sort(a0)"}, {expected});

  // All nulls elements.
  data = makeRowVector({
      makeArrayVectorFromJson<int32_t>({
          "[]",
          "[null, null, null, null]",
          "[null, null, null]",
      }),
  });

  expected = makeRowVector({
      makeArrayVectorFromJson<int32_t>({"[null]"}),
  });

  testAggregations(
      {data}, {}, {"set_union(c0)"}, {"array_sort(a0)"}, {expected});
}

TEST_F(SetUnionTest, groupBy) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 2, 1, 2, 1, 2, 1}),
      makeArrayVector<int32_t>({
          {},
          {1, 2, 3}, // masked out
          {10, 20},
          {20, 30, 40},
          {10, 50}, // masked out
          {4, 2, 1, 5}, // masked out
          {60, 20},
          {5, 6},
          {}, // masked out
          {},
      }),
      makeFlatVector<bool>(
          {true, false, true, true, false, false, true, true, false, true}),
  });

  auto expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVector<int32_t>({
          {1, 2, 3, 4, 5, 6},
          {10, 20, 30, 40, 50, 60},
      }),
  });

  testAggregations(
      {data, data, data},
      {"c0"},
      {"set_union(c1)"},
      {"c0", "array_sort(a0)"},
      {expected});

  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVector<int32_t>({
          {5, 6},
          {10, 20, 30, 40, 60},
      }),
  });

  testAggregations(
      {data, data, data},
      {"c0"},
      {"set_union(c1) filter (where c2)"},
      {"c0", "array_sort(a0)"},
      {expected});

  // Null inputs.
  data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 2, 1, 2, 1, 2, 1}),
      makeArrayVectorFromJson<int32_t>({
          "[]",
          "[1, 2, 3]",
          "[10, null, 20]",
          "[20, 30, 40, null, 50]",
          "null",
          "[4, 2, 1, 5]",
          "[60, null, 20]",
          "[null, 5, 6]",
          "[]",
          "null",
      }),
  });

  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVectorFromJson<int32_t>({
          "[1, 2, 3, 4, 5, 6, null]",
          "[10, 20, 30, 40, 50, 60, null]",
      }),
  });

  testAggregations(
      {data, data, data},
      {"c0"},
      {"set_union(c1)"},
      {"c0", "array_sort(a0)"},
      {expected});

  // All null arrays for one group.
  std::vector<RowVectorPtr> multiBatchData = {
      makeRowVector({
          makeFlatVector<int16_t>({1, 1, 2, 2, 2, 1, 2}),
          makeArrayVectorFromJson<int32_t>({
              "null",
              "null",
              "[]",
              "[1, 2]",
              "[1, 2, 3]",
              "null",
              "null",
          }),
      }),
      makeRowVector({
          makeFlatVector<int16_t>({3, 3, 3, 2, 3}),
          makeArrayVectorFromJson<int32_t>({
              "null",
              "null",
              "null",
              "[2, 4, 5]",
              "null",
          }),
      }),
  };

  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 3}),
      makeArrayVector<int32_t>({
          {}, // Empty array: [].
          {1, 2, 3, 4, 5},
          {}, // Empty array: [].
      }),
  });

  testAggregations(
      multiBatchData,
      {"c0"},
      {"set_union(c1)"},
      {"c0", "array_sort(a0)"},
      {expected});
}

TEST_F(SetUnionTest, inputOrder) {
  // Presto preserves order of input.

  auto testInputOrder = [&](const RowVectorPtr& data,
                            const RowVectorPtr& expected) {
    auto plan = exec::test::PlanBuilder()
                    .values({data})
                    .singleAggregation({}, {"set_union(c0)"})
                    .planNode();
    assertQuery(plan, expected);
  };

  auto data = makeRowVector({
      makeArrayVectorFromJson<int32_t>({
          "[]",
          "[1, 2, 3]",
          "[1, 2, null]",
          "[2, 3, 4, 5]",
          "[6, 7]",
      }),
  });

  auto expected = makeRowVector({
      makeNullableArrayVector<int32_t>({
          {1, 2, 3, std::nullopt, 4, 5, 6, 7},
      }),
  });

  testInputOrder(data, expected);

  // Strings.
  data = makeRowVector({
      makeNullableArrayVector<StringView>({
          {},
          {"abc", "bxy", "cde"},
          {"abc", "bxy"},
          {"cdef", "hijk", std::nullopt},
          {"abc", "some very long string to test long strings"},
      }),
  });

  expected = makeRowVector({
      makeNullableArrayVector<StringView>({
          {"abc",
           "bxy",
           "cde",
           "cdef",
           "hijk",
           std::nullopt,
           "some very long string to test long strings"},
      }),
  });

  testInputOrder(data, expected);

  // Complex types.

  data = makeRowVector({makeNestedArrayVectorFromJson<int32_t>(
      {"[[1,2], [5, 6], null]", "[[3,4], [7, 8], null]"})});

  expected = makeRowVector({makeNestedArrayVectorFromJson<int32_t>(
      {"[[1,2], [5, 6], null, [3,4], [7, 8]]"})});

  testInputOrder(data, expected);

  // Group by.
  data = makeRowVector({
      makeFlatVector<int16_t>({
          1,
          2,
          1,
          2,
          1,
      }),
      makeArrayVectorFromJson<int32_t>({
          "[]",
          "[1, 2, 3]",
          "[1, 2, null]",
          "[2, 3, 4, 5]",
          "[6, 7]",
      }),
  });

  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVectorFromJson<int32_t>({
          "[1, 2, null, 6, 7]",
          "[1, 2, 3, 4, 5]",
      }),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data})
                  .singleAggregation({"c0"}, {"set_union(c1)"})
                  .planNode();

  assertQuery(plan, expected);
}

TEST_F(SetUnionTest, shortDecimal) {
  // Test with short decimal
  auto type = DECIMAL(6, 2);

  auto data = makeRowVector({
      makeArrayVector<int64_t>(
          {
              {},
              {kLongMin, 2000, 3000},
              {kLongMin, -2000},
              {2000, 3000, kLongMax, 5000},
              {6000, 7000, -5432},
          },
          type),
  });

  auto expected = makeRowVector({
      makeArrayVector<int64_t>(
          {
              {kLongMin, -5432, -2000, 2000, 3000, 5000, 6000, 7000, kLongMax},
          },
          type),
  });

  testAggregations(
      {data}, {}, {"set_union(c0)"}, {"array_sort(a0)"}, {expected});

  // Test with some NULL inputs (short decimals)
  data = makeRowVector({
      makeNullableArrayVector<int64_t>(
          {
              {},
              {-1000, std::nullopt, kLongMax, std::nullopt, 7000},
              {1000, kLongMax, std::nullopt, 4000, std::nullopt, std::nullopt},
              {kLongMin, -5923},
          },
          ARRAY(type)),
  });

  expected = makeRowVector({
      makeNullableArrayVector(
          std::vector<std::vector<std::optional<int64_t>>>{
              {kLongMin,
               -5923,
               -1000,
               1000,
               4000,
               7000,
               kLongMax,
               std::nullopt},
          },
          ARRAY(type)),
  });

  testAggregations(
      {data}, {}, {"set_union(c0)"}, {"array_sort(a0)"}, {expected});

  // Test with all NULL inputs (short decimals)
  data = makeRowVector({
      makeAllNullArrayVector(10, type),
  });

  expected = makeRowVector({
      makeArrayVector<int64_t>({{}}, type),
  });

  testAggregations(
      {data}, {}, {"set_union(c0)"}, {"array_sort(a0)"}, {expected});

  data = makeRowVector({
      makeNullableArrayVector<int64_t>(
          {
              {},
              {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
              {std::nullopt, std::nullopt, std::nullopt},
          },
          ARRAY(type)),
  });

  expected = makeRowVector({
      makeNullableArrayVector(
          std::vector<std::vector<std::optional<int64_t>>>{
              {std::nullopt},
          },
          ARRAY(type)),
  });

  testAggregations(
      {data}, {}, {"set_union(c0)"}, {"array_sort(a0)"}, {expected});
}

TEST_F(SetUnionTest, longDecimal) {
  // Test with long decimals
  auto type = DECIMAL(20, 2);

  auto data = makeRowVector({
      makeArrayVector<int128_t>(
          {
              {},
              {kHugeMax, -2000, 3000},
              {1000, 2000},
              {2000, kHugeMin, 3000, 4000, 5000},
              {-6363, 7000},
          },
          type),
  });

  auto expected = makeRowVector({
      makeArrayVector<int128_t>(
          {
              {kHugeMin,
               -6363,
               -2000,
               1000,
               2000,
               3000,
               4000,
               5000,
               7000,
               kHugeMax},
          },
          type),
  });

  testAggregations(
      {data}, {}, {"set_union(c0)"}, {"array_sort(a0)"}, {expected}, {}, false);

  // Test with some NULL inputs (long decimals).
  data = makeRowVector({
      makeNullableArrayVector<int128_t>(
          {
              {},
              {kHugeMin},
              {1000, std::nullopt, 3000, std::nullopt, 7000},
              {-1000, 3000, std::nullopt, 4000, std::nullopt, std::nullopt},
              {2000, kHugeMax, -1234},
          },
          ARRAY(type)),
  });

  expected = makeRowVector({
      makeNullableArrayVector(
          std::vector<std::vector<std::optional<int128_t>>>{
              {kHugeMin,
               -1234,
               -1000,
               1000,
               2000,
               3000,
               4000,
               7000,
               kHugeMax,
               std::nullopt},
          },
          ARRAY(type)),
  });

  testAggregations(
      {data}, {}, {"set_union(c0)"}, {"array_sort(a0)"}, {expected}, {}, false);

  // Test with all NULL inputs (long decimals).
  data = makeRowVector({
      makeAllNullArrayVector(10, type),
  });

  expected = makeRowVector({
      makeArrayVector<int128_t>({{}}, type),
  });

  testAggregations(
      {data}, {}, {"set_union(c0)"}, {"array_sort(a0)"}, {expected}, {}, false);
}
} // namespace
} // namespace facebook::velox::aggregate::test
