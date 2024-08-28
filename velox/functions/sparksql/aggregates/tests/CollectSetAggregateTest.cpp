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

#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/sparksql/Register.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::functions::aggregate::sparksql::test {

namespace {

class CollectSetAggregateTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerAggregateFunctions("");
    functions::sparksql::registerFunctions("spark_");
  }
};

TEST_F(CollectSetAggregateTest, global) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 3, 4, 2, 6, 7}),
  });

  auto expected = makeRowVector({
      makeArrayVectorFromJson<int32_t>({"[1, 2, 3, 4, 5, 6, 7]"}),
  });

  testAggregations(
      {data}, {}, {"collect_set(c0)"}, {"spark_array_sort(a0)"}, {expected});

  // Null inputs.
  data = makeRowVector({
      makeNullableFlatVector<int32_t>(
          {1, 2, std::nullopt, 4, 5, std::nullopt, 4, 2, 6, 7}),
  });

  expected = makeRowVector({
      makeArrayVectorFromJson<int32_t>({"[1, 2, 4, 5, 6, 7]"}),
  });

  testAggregations(
      {data}, {}, {"collect_set(c0)"}, {"spark_array_sort(a0)"}, {expected});

  // All inputs are null.
  data = makeRowVector({
      makeAllNullFlatVector<int32_t>(1'000),
  });

  expected = makeRowVector({
      makeArrayVectorFromJson<int32_t>({"[]"}),
  });

  testAggregations(
      {data}, {}, {"collect_set(c0)"}, {"spark_array_sort(a0)"}, {expected});

  data = makeRowVector({
      makeFlatVector<double>(
          {1,
           std::numeric_limits<double>::quiet_NaN(),
           std::nan("1"),
           std::nan("2")}),
  });

  expected = makeRowVector({
      makeArrayVector<double>({
          {1, std::numeric_limits<double>::quiet_NaN()},
      }),
  });

  testAggregations(
      {data}, {}, {"collect_set(c0)"}, {"spark_array_sort(a0)"}, {expected});
}

TEST_F(CollectSetAggregateTest, noInputRow) {
  // Empty input.
  auto data = makeRowVector({makeFlatVector<int32_t>({})});
  auto expected = makeRowVector({
      makeArrayVectorFromJson<int32_t>({"[]"}),
  });
  testAggregations({data}, {}, {"collect_set(c0)"}, {}, {expected});

  // No input row passes aggregate filter.
  const auto size = 1'000;
  const auto integers =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 10; });
  data = makeRowVector({
      integers,
      makeFlatVector<bool>(size, [](auto /*row*/) { return false; }),
  });

  auto op = exec::test::PlanBuilder()
                .values({data})
                .singleAggregation({}, {"collect_set(c0)"}, {"c1"})
                .planNode();
  assertQuery(op, expected);

  // Some groups have no input row passes aggregate filter.
  data = makeRowVector({
      integers,
      integers,
      makeFlatVector<bool>(size, [](auto row) { return row % 2 == 0; }),
  });
  expected = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
      makeArrayVectorFromJson<int32_t>(
          {"[0]", "[]", "[2]", "[]", "[4]", "[]", "[6]", "[]", "[8]", "[]"}),
  });
  op = exec::test::PlanBuilder()
           .values({data})
           .singleAggregation({"c0"}, {"collect_set(c1)"}, {"c2"})
           .planNode();
  assertQuery(op, expected);
}

TEST_F(CollectSetAggregateTest, groupBy) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 2, 1, 2, 1, 2, 1}),
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 3, 4, 2, 6, 7}),
  });

  auto expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVectorFromJson<int32_t>({"[1, 2, 3, 7]", "[3, 4, 5, 6]"}),
  });

  testAggregations(
      {data, data, data},
      {"c0"},
      {"collect_set(c1)"},
      {"c0", "spark_array_sort(a0)"},
      {expected});

  // Null inputs.
  data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 2, 1, 2, 1, 2, 1}),
      makeNullableFlatVector<int32_t>(
          {1,
           std::nullopt,
           3,
           std::nullopt,
           5,
           std::nullopt,
           3,
           std::nullopt,
           6,
           1}),
  });

  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVectorFromJson<int32_t>({
          "[1]",
          "[3, 5, 6]",
      }),
  });

  testAggregations(
      {data, data, data},
      {"c0"},
      {"collect_set(c1)"},
      {"c0", "spark_array_sort(a0)"},
      {expected});

  // All inputs are null for a group.
  data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 2, 1, 2, 1, 2, 1}),
      makeNullableFlatVector<int32_t>(
          {1,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           1}),
  });

  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVectorFromJson<int32_t>({
          "[1]",
          "[]",
      }),
  });

  testAggregations(
      {data, data, data},
      {"c0"},
      {"collect_set(c1)"},
      {"c0", "spark_array_sort(a0)"},
      {expected});

  data = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeMapVectorFromJson<int64_t, int64_t>({
          "{10: 10, 11: 11}",
          "{12: 12}",
      }),
  });

  testFailingAggregations(
      {data}, {"c0"}, {"collect_set(c1)"}, "Unsupported type MAP");
}

TEST_F(CollectSetAggregateTest, arrayWithNestedNulls) {
  auto data = makeRowVector({
      makeArrayVectorFromJson<int32_t>(
          {"[1, 2, 3]",
           "[4 ,5]",
           "[1, 2, 3]",
           "[3, 4, 2, 6, 7]",
           "[1, 2, 3]",
           "[4 ,5, null]"}),
  });

  auto expected = makeRowVector({makeNestedArrayVectorFromJson<int32_t>(
      {"[[1, 2, 3], [3, 4, 2, 6, 7], [4 ,5], [4 ,5, null]]"})});

  testAggregations(
      {data}, {}, {"collect_set(c0)"}, {"spark_array_sort(a0)"}, {expected});
}

TEST_F(CollectSetAggregateTest, rowWithNestedNull) {
  auto data = makeRowVector({makeRowVector({
      makeFlatVector<int32_t>({1, 2, 2, 1}),
      makeNullableFlatVector<int32_t>({1, std::nullopt, 2, 1}),
  })});
  auto expected = makeRowVector({
      makeArrayOfRowVector(
          ROW({INTEGER(), INTEGER()}),
          {
              {
                  variant::row({1, 1}),
                  variant::row({2, 2}),
                  variant::row({2, variant::null(TypeKind::INTEGER)}),
              },
          }),
  });

  testAggregations(
      {data}, {}, {"collect_set(c0)"}, {"spark_array_sort(a0)"}, {expected});
}

} // namespace
} // namespace facebook::velox::functions::aggregate::sparksql::test
