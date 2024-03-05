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

class MapUnionTest : public AggregationTestBase {};

/**
 * This test checks single, partial, intermediate, final aggregates
 * with and without local exchange when
 * there are no duplicates in the keys of the map.
 *
 * Takes as input a table that contains 2 columns.
 * First column: five 0's followed by five 1's.
 * Second column: A map of size one which contains
 * consecutive numbers, where the key is NULL for
 * every 4th entry (num % 4 == 0) and the value is NULL
 * for every 7th entry (num % 7 == 0).
 *
 * The expected output is to GROUP BY the first column
 * and the map size is to be of size 3 for the first row
 * while of size 4 for the second row, where each map
 * has a list of consecutive numbers and the value of the
 * map is NULL for every 7th entry (num % 7 == 0).
 */
TEST_F(MapUnionTest, groupByWithoutDuplicates) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({0, 0, 0, 0, 0, 1, 1, 1, 1, 1}),
      makeNullableMapVector<int32_t, double>({
          // Group 0.
          std::nullopt,
          {{{1, std::nullopt}}},
          {{{2, 2.05}}},
          {{{3, 3.05}}},
          std::nullopt,
          // Group 1.
          {{{5, 5.05}}},
          {{{6, std::nullopt}}},
          {{{7, 7.05}}},
          std::nullopt,
          {{{9, 9.05}}},
      }),
      makeFlatVector<bool>(10, [](auto row) { return row % 2 == 0; }),
  });

  auto expectedResult = makeRowVector({
      makeFlatVector<int32_t>({0, 1}),
      makeNullableMapVector<int32_t, double>({
          // Group 0.
          {{{1, std::nullopt}, {2, 2.05}, {3, 3.05}}},
          // Group 1.
          {{{5, 5.05}, {6, std::nullopt}, {7, 7.05}, {9, 9.05}}},
      }),
  });

  testAggregations({data}, {"c0"}, {"map_union(c1)"}, {expectedResult});
  testAggregations(split(data), {"c0"}, {"map_union(c1)"}, {expectedResult});

  expectedResult = makeRowVector({
      makeFlatVector<int32_t>({0, 1}),
      makeNullableMapVector<int32_t, double>({
          // Group 0.
          {{{2, 2.05}}},
          // Group 1.
          {{{6, std::nullopt}}},
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"map_union(c1) filter (where c2)"}, {expectedResult});

  testAggregations(
      split(data),
      {"c0"},
      {"map_union(c1) filter (where c2)"},
      {expectedResult});
}

/**
 * This test checks single, partial, intermediate, final aggregates
 * with and without local exchange when
 * there are duplicates in the keys of the map.
 *
 * Takes as input a table that contains 2 columns.
 * First column: five 0's followed by five 1's
 * Second column: A map of size one which contains
 * (Key, Value) as (1, 1.05).
 *
 * The expected output is to GROUP BY the first column
 * and for each row, the map size is to be of size 1 which
 * contains (Key, Value) as (1, 1.05).
 */
TEST_F(MapUnionTest, groupByWithDuplicates) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(10, [](vector_size_t row) { return row / 5; }),
      makeMapVector<int32_t, double>(
          10,
          [&](vector_size_t /*row*/) { return 1; },
          [&](vector_size_t /*row*/) { return 1; },
          [&](vector_size_t /*row*/) { return 1.05; }),
      makeFlatVector<bool>(10, [](auto row) { return row % 2 == 0; }),
  });

  auto expectedResult = {makeRowVector(
      {makeFlatVector<int32_t>({0, 1}),
       makeMapVector<int32_t, double>(
           2,
           [&](vector_size_t /*row*/) { return 1; },
           [&](vector_size_t /*row*/) { return 1; },
           [&](vector_size_t /*row*/) { return 1.05; })})};

  testAggregations(
      {data, data, data}, {"c0"}, {"map_union(c1)"}, expectedResult);

  testAggregations(
      {data, data, data},
      {"c0"},
      {"map_union(c1) filter (where c2)"},
      expectedResult);
}

/**
 * This test checks single, partial, intermediate, final aggregates
 * with and without local exchange when input is empty.
 */
TEST_F(MapUnionTest, groupByNoData) {
  auto inputVectors = {makeRowVector(
      {makeFlatVector<int32_t>({}), makeMapVector<int32_t, double>({})})};
  auto expectedResult = inputVectors;

  testAggregations(inputVectors, {"c0"}, {"map_union(c1)"}, expectedResult);
}

/**
 * This test checks global aggregate when
 * with and without local exchange when
 * there are no duplicates in the keys of the map.
 *
 * Takes as input a table that contains 1 column i.e.
 * a map of size one which contains consecutive numbers,
 * where the key is NULL for every 4th entry
 * (num % 4 == 0) and the value is NULL
 * for every 7th entry (num % 7 == 0).
 *
 * The expected output is a map of all the non-NULL keys.
 */
TEST_F(MapUnionTest, globalWithoutDuplicates) {
  auto inputVectors = {makeRowVector({makeMapVector<int32_t, double>(
      10,
      [&](vector_size_t /*row*/) { return 1; },
      [&](vector_size_t row) { return row; },
      [&](vector_size_t row) { return row + 0.05; },
      nullEvery(4),
      nullEvery(7))})};
  auto expectedResult = {makeRowVector({makeMapVector<int32_t, double>(
      1,
      [&](vector_size_t /*row*/) { return 7; },
      [&](vector_size_t row) { return row; },
      [&](vector_size_t row) { return row + 0.05; },
      nullptr,
      nullEvery(7))})};

  testAggregations(inputVectors, {}, {"map_union(c0)"}, expectedResult);
}

/**
 * This test checks global aggregate when
 * with and without local exchange when
 * there are duplicates in the keys of the map.
 *
 * Takes as input a table that contains 1 column i.e.
 * a map of size one which contains
 * (Key, Value) as (1, 1.05).
 *
 * The expected output a map which
 * contains (Key, Value) as (1, 1.05).
 */
TEST_F(MapUnionTest, globalWithDuplicates) {
  auto inputVectors = {makeRowVector({makeMapVector<int32_t, double>(
      10,
      [&](vector_size_t /*row*/) { return 1; },
      [&](vector_size_t /*row*/) { return 1; },
      [&](vector_size_t /*row*/) { return 1.05; })})};
  auto expectedResult = {makeRowVector({makeMapVector<int32_t, double>(
      1,
      [&](vector_size_t /*row*/) { return 1; },
      [&](vector_size_t /*row*/) { return 1; },
      [&](vector_size_t /*row*/) { return 1.05; })})};

  testAggregations(inputVectors, {}, {"map_union(c0)"}, expectedResult);
}

/**
 * This test checks global aggregate when
 * the input is empty.
 */
TEST_F(MapUnionTest, globalNoData) {
  auto data = makeRowVector(ROW({"c0"}, {MAP(BIGINT(), VARCHAR())}), 0);

  testAggregations({data}, {}, {"map_union(c0)"}, "SELECT null");
}

TEST_F(MapUnionTest, nulls) {
  auto data = makeRowVector(
      {"k", "m"},
      {
          makeFlatVector<int64_t>({1, 2, 1, 3, 3}),
          makeMapVector<int64_t, int64_t>({
              {{{1, 10}, {2, 20}}},
              {{123, 100}}, // to be null
              {{{3, 33}, {4, 44}, {5, 55}}},
              {{456, 1000}}, // to be null
              {}, // empty map
          }),
      });

  data->childAt(1)->setNull(1, true);
  data->childAt(1)->setNull(3, true);

  // Global aggregation.
  auto expected = makeRowVector({
      makeMapVector<int64_t, int64_t>({
          {{1, 10}, {2, 20}, {3, 33}, {4, 44}, {5, 55}},
      }),
  });

  testAggregations({data}, {}, {"map_union(m)"}, {expected});

  // Group-by aggregation.
  expected = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeNullableMapVector<int64_t, int64_t>({
          {{{1, 10}, {2, 20}, {3, 33}, {4, 44}, {5, 55}}},
          std::nullopt,
          {{}},
      }),
  });

  testAggregations({data}, {"k"}, {"map_union(m)"}, {expected});
}

TEST_F(MapUnionTest, unknownKeysAndValues) {
  // map_union over empty map(unknown, unknown) is allowed. Skip testing with
  // TableScan because unknown type is not supported in writers.
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 1}),
      makeMapVector<UnknownValue, UnknownValue>({{}, {}, {}}),
  });

  auto expectedGlobalResult = makeRowVector({
      makeMapVector<UnknownValue, UnknownValue>({{}}),
  });

  auto expectedGroupByResult = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
      makeMapVector<UnknownValue, UnknownValue>({{}, {}}),
  });

  testAggregations({data}, {}, {"map_union(c1)"}, {expectedGlobalResult});
  testAggregations({data}, {"c0"}, {"map_union(c1)"}, {expectedGroupByResult});

  // map_union over non-empty map(T, unknown) where T is not unknown is allowed.
  data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 1}),
      makeNullableMapVector<int32_t, UnknownValue>({
          {{{1, {std::nullopt}}, {2, {std::nullopt}}}},
          {{{3, {std::nullopt}}}},
          {{{3, {std::nullopt}}, {4, {std::nullopt}}}},
      }),
  });

  expectedGlobalResult = makeRowVector({
      makeNullableMapVector<int32_t, UnknownValue>({
          {{
              {1, {std::nullopt}},
              {2, {std::nullopt}},
              {3, {std::nullopt}},
              {4, {std::nullopt}},
          }},
      }),
  });

  expectedGroupByResult = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
      makeNullableMapVector<int32_t, UnknownValue>({
          {{
              {1, {std::nullopt}},
              {2, {std::nullopt}},
              {3, {std::nullopt}},
              {4, {std::nullopt}},
          }},
          {{
              {3, {std::nullopt}},
          }},
      }),
  });

  testAggregations({data}, {}, {"map_union(c1)"}, {expectedGlobalResult});
  testAggregations({data}, {"c0"}, {"map_union(c1)"}, {expectedGroupByResult});

  // map_union over non-emtpy map(unknown, T) is not allowed.
  data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 1}),
      // 3 map rows: {null, 100}, {null, 200}, {null, 300}.
      makeMapVector(
          {0, 1, 1},
          makeAllNullFlatVector<UnknownValue>(3),
          makeFlatVector<int32_t>({100, 200, 300})),
  });

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation({}, {"map_union(c1)"})
                  .planNode();
  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()), "map key cannot be null");

  plan = PlanBuilder()
             .values({data})
             .singleAggregation({"c0"}, {"map_union(c1)"})
             .planNode();
  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()), "map key cannot be null");
}

} // namespace
} // namespace facebook::velox::aggregate::test
