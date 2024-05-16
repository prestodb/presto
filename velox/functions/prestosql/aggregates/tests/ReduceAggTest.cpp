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
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using facebook::velox::exec::test::AssertQueryBuilder;
using facebook::velox::exec::test::PlanBuilder;

namespace facebook::velox::aggregate::prestosql {
namespace {

class ReduceAggTest : public functions::aggregate::test::AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    disableTestStreaming();
  }

  std::vector<RowVectorPtr> fuzzData() {
    VectorFuzzer::Options opts;
    opts.vectorSize = 10'000;
    opts.nullRatio = 0.1;
    VectorFuzzer fuzzer(opts, pool());

    auto rowType = ROW({{"c0", SMALLINT()}});
    std::vector<RowVectorPtr> data;
    for (auto i = 0; i < 10; ++i) {
      data.emplace_back(fuzzer.fuzzInputRow(rowType));
    }

    return data;
  }
};

TEST_F(ReduceAggTest, integersGlobal) {
  auto data = makeRowVector(
      {"c0", "c1", "m"},
      {
          makeFlatVector<int64_t>({1, 2, 3, 4, 5, 1, 1}),
          makeNullableFlatVector<int64_t>(
              {1, 2, std::nullopt, 0, 1, 3, std::nullopt}),
          makeFlatVector<bool>({true, false, true, true, false, false, true}),
      });

  // No nulls.
  int64_t product = 1 * 2 * 3 * 4 * 5 * 1 * 1;
  auto expected = makeRowVector({makeConstant(product, 1)});

  testAggregations(
      {data},
      {},
      {"reduce_agg(c0, 1, (x, y) -> (x * y), (x, y) -> (x * y))"},
      {expected});

  // No nulls. With mask.
  product = 1 * 3 * 4 * 1;
  expected = makeRowVector({makeConstant(product, 1)});

  testAggregations(
      {data},
      {},
      {"reduce_agg(c0, 1, (x, y) -> (x * y), (x, y) -> (x * y)) FILTER (WHERE m)"},
      {expected});

  // Some nulls.
  product = 1 * 2 * 0 * 1 * 3;
  expected = makeRowVector({makeConstant(product, 1)});

  testAggregations(
      {data},
      {},
      {"reduce_agg(c1, 1, (x, y) -> (x * y), (x, y) -> (x * y))"},
      {expected});

  // Same as above, but using a sum reduction.
  int64_t sum = 1 + 2 + 3 + 4 + 5 + 1 + 1;
  expected = makeRowVector({makeConstant(sum, 1)});

  testAggregations(
      {data},
      {},
      {"reduce_agg(c0, 0, (x, y) -> (x + y), (x, y) -> (x + y))"},
      {expected});

  sum = 1 + 2 + 0 + 1 + 3;
  expected = makeRowVector({makeConstant(sum, 1)});

  testAggregations(
      {data},
      {},
      {"reduce_agg(c1, 0, (x, y) -> (x + y), (x, y) -> (x + y))"},
      {expected});
}

TEST_F(ReduceAggTest, integersGroupBy) {
  auto data = makeRowVector(
      {"k", "c0", "m"},
      {
          makeFlatVector<int32_t>({1, 1, 2, 2, 1}),
          makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
          makeFlatVector<bool>({true, false, false, false, true}),
      });

  auto expected = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
      makeFlatVector<int64_t>({10, 12}),
  });

  testAggregations(
      {data},
      {"k"},
      {"reduce_agg(c0, 1, (x, y) -> (x * y), (x, y) -> (x * y))"},
      {expected});

  // With mask.
  expected = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
      makeNullableFlatVector<int64_t>({5, std::nullopt}),
  });

  testAggregations(
      {data},
      {"k"},
      {"reduce_agg(c0, 1, (x, y) -> (x * y), (x, y) -> (x * y)) FILTER (WHERE m)"},
      {expected});

  // Sum reduction.
  expected = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
      makeFlatVector<int64_t>({8, 7}),
  });

  testAggregations(
      {data},
      {"k"},
      {"reduce_agg(c0, 0, (x, y) -> (x + y), (x, y) -> (x + y))"},
      {expected});

  // With mask.
  expected = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
      makeNullableFlatVector<int64_t>({6, std::nullopt}),
  });

  testAggregations(
      {data},
      {"k"},
      {"reduce_agg(c0, 0, (x, y) -> (x + y), (x, y) -> (x + y)) FILTER (WHERE m)"},
      {expected});
}

TEST_F(ReduceAggTest, arraysGlobal) {
  auto data = makeRowVector(
      {"c0", "c1", "m"},
      {
          makeArrayVector<int64_t>({
              {1, 2, 3},
              {1, 4, 5},
              {},
              {2, 6},
              {3, 3, 7, 2},
          }),
          makeConstantArray<int64_t>(5, {}),
          makeFlatVector<bool>({true, false, true, false, true}),
      });

  auto expected = makeRowVector({makeArrayVector<int64_t>({{7, 6, 5}})});

  testAggregations(
      {data, data, data},
      {},
      {"reduce_agg(c0, c1, "
       "(a, b) -> slice(reverse(array_sort(array_distinct(concat(a, b)))), 1, 3), "
       "(a, b) -> slice(reverse(array_sort(array_distinct(concat(a, b)))), 1, 3))"},
      {expected});

  expected = makeRowVector({makeArrayVector<int64_t>({{7, 3, 2}})});

  testAggregations(
      {data, data, data},
      {},
      {"reduce_agg(c0, c1, "
       "(a, b) -> slice(reverse(array_sort(array_distinct(concat(a, b)))), 1, 3), "
       "(a, b) -> slice(reverse(array_sort(array_distinct(concat(a, b)))), 1, 3)) "
       "FILTER (WHERE m)"},
      {expected});
}

TEST_F(ReduceAggTest, arraysGroupBy) {
  auto data = makeRowVector(
      {"k", "c0", "c1", "m"},
      {
          makeFlatVector<int32_t>({1, 2, 1, 2, 1}),
          makeArrayVector<int64_t>({
              {1, 2, 3},
              {10, 20},
              {1, 2, 4, 5},
              {20, 30, 40},
              {3, 1, 1, 10},
          }),
          makeConstantArray<int64_t>(5, {}),
          makeFlatVector<bool>({false, true, false, true, false}),
      });

  auto expected = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
      makeArrayVector<int64_t>({
          {10, 5, 4},
          {40, 30, 20},
      }),
  });

  testAggregations(
      {data, data, data},
      {"k"},
      {"reduce_agg(c0, c1, "
       "(a, b) -> slice(reverse(array_sort(array_distinct(concat(a, b)))), 1, 3), "
       "(a, b) -> slice(reverse(array_sort(array_distinct(concat(a, b)))), 1, 3))"},
      {expected});

  expected = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
      makeNullableArrayVector<int64_t>({
          std::nullopt,
          {{40, 30, 20}},
      }),
  });

  testAggregations(
      {data, data, data},
      {"k"},
      {"reduce_agg(c0, c1, "
       "(a, b) -> slice(reverse(array_sort(array_distinct(concat(a, b)))), 1, 3), "
       "(a, b) -> slice(reverse(array_sort(array_distinct(concat(a, b)))), 1, 3)) "
       "FILTER (WHERE m)"},
      {expected});
}

TEST_F(ReduceAggTest, differentInputAndCombine) {
  auto data = makeRowVector(
      {"k", "c0", "c1", "m"},
      {
          makeFlatVector<int32_t>({1, 1, 1, 2, 2, 2, 1, 2}),
          makeNullableFlatVector<int32_t>(
              {1, 2, std::nullopt, 10, std::nullopt, 20, 3, 30}),
          makeConstantArray<int32_t>(8, {}),
          makeFlatVector<bool>(
              {true, false, true, false, true, false, true, false}),
      });

  auto expected = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
      makeArrayVector<int32_t>({{1, 2, 3}, {10, 20, 30}}),
  });

  testAggregations(
      {data, data, data},
      {"k"},
      {"reduce_agg(c0, c1, "
       "(s, x) -> array_sort(array_distinct(concat(s, array[x]))), "
       "(s, s2) -> array_sort(array_distinct(concat(s, s2))))"},
      {expected});

  expected = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
      makeNullableArrayVector<int32_t>({{{1, 3}}, std::nullopt}),
  });

  testAggregations(
      {data, data, data},
      {"k"},
      {"reduce_agg(c0, c1, "
       "(s, x) -> array_sort(array_distinct(concat(s, array[x]))), "
       "(s, s2) -> array_sort(array_distinct(concat(s, s2)))) "
       "FILTER (WHERE m)"},
      {expected});
}

TEST_F(ReduceAggTest, fuzzGlobalSum) {
  auto data = fuzzData();
  auto plan =
      PlanBuilder().values(data).singleAggregation({}, {"sum(c0)"}).planNode();

  auto sumResults = AssertQueryBuilder(plan).copyResults(pool());

  testAggregations(
      data,
      {},
      {"reduce_agg(c0, 0, (x, y) -> (x + y), (x, y) -> (x + y))"},
      {},
      {sumResults});
}

TEST_F(ReduceAggTest, fuzzGroupBySum) {
  auto data = fuzzData();
  auto plan = PlanBuilder()
                  .values(data)
                  .project({"c0 % 1234 as key", "c0"})
                  .singleAggregation({"key"}, {"sum(c0)"})
                  .planNode();

  auto sumResults = AssertQueryBuilder(plan).copyResults(pool());

  testAggregations(
      [&](PlanBuilder& builder) {
        builder.values(data).project({"c0 % 1234 as key", "c0"});
      },
      {"key"},
      {"reduce_agg(c0, 0, (x, y) -> (x + y), (x, y) -> (x + y))"},
      {},
      [&](auto& builder) { return builder.assertResults(sumResults); });
}

TEST_F(ReduceAggTest, fuzzGlobalAvg) {
  auto data = fuzzData();
  auto plan =
      PlanBuilder().values(data).singleAggregation({}, {"avg(c0)"}).planNode();

  auto avgResults = AssertQueryBuilder(plan).copyResults(pool());

  testAggregations(
      [&](PlanBuilder& builder) {
        builder.values(data).project({
            "c0",
            "cast(row_constructor(0, 0) as row(sum double, count bigint)) as c1",
        });
      },
      {},
      {"reduce_agg(c0, c1, "
       "(s, x) -> (row_constructor(s.sum + x, s.count + 1)), "
       "(s, s2) -> (row_constructor(s.sum + s2.sum, s.count + s2.count)))"},
      {"a0.sum / cast(a0.count as double)"},
      [&](auto& builder) { return builder.assertResults(avgResults); });
}

TEST_F(ReduceAggTest, fuzzGroupByAvg) {
  auto data = fuzzData();
  auto plan = PlanBuilder()
                  .values(data)
                  .project({"c0 % 1234 as key", "c0"})
                  .singleAggregation({"key"}, {"avg(c0)"})
                  .planNode();

  auto avgResults = AssertQueryBuilder(plan).copyResults(pool());

  testAggregations(
      [&](PlanBuilder& builder) {
        builder.values(data).project({
            "c0",
            "cast(row_constructor(0, 0) as row(sum double, count bigint)) as c1",
            "c0 % 1234 as key",
        });
      },
      {"key"},
      {"reduce_agg(c0, c1, "
       "(s, x) -> (row_constructor(s.sum + x, s.count + 1)), "
       "(s, s2) -> (row_constructor(s.sum + s2.sum, s.count + s2.count)))"},
      {"key", "a0.sum / cast(a0.count as double)"},
      [&](auto& builder) { return builder.assertResults(avgResults); });
}

} // namespace
} // namespace facebook::velox::aggregate::prestosql
