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
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::functions::aggregate::test;
using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

namespace {

class CountDistinctTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    prestosql::registerInternalAggregateFunctions("");
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }
};

TEST_F(CountDistinctTest, global) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 3, 4, 2, 6, 7}),
      makeFlatVector<StringView>(
          {"1", "2", "3", "4", "5", "3", "4", "2", "6", "7"}),
  });

  auto expected = makeRowVector({
      makeFlatVector<int64_t>(std::vector<int64_t>{7}),
  });

  testAggregations(
      {data}, {}, {"\"$internal$count_distinct\"(c0)"}, {expected});
  testAggregations(
      {data}, {}, {"\"$internal$count_distinct\"(c1)"}, {expected});

  // Null inputs.
  data = makeRowVector({
      makeNullableFlatVector<int32_t>(
          {1, 2, std::nullopt, 4, 5, std::nullopt, 4, 2, 6, 7}),
      makeNullableFlatVector<StringView>(
          {"1", "2", std::nullopt, "4", "5", std::nullopt, "4", "2", "6", "7"}),
  });

  expected = makeRowVector({
      makeFlatVector<int64_t>(std::vector<int64_t>{6}),
  });

  testAggregations(
      {data}, {}, {"\"$internal$count_distinct\"(c0)"}, {expected});
  testAggregations(
      {data}, {}, {"\"$internal$count_distinct\"(c1)"}, {expected});

  // All inputs are null.
  data = makeRowVector({
      makeAllNullFlatVector<int32_t>(1'000),
      makeAllNullFlatVector<StringView>(1'000),
  });

  expected = makeRowVector({
      makeFlatVector<int64_t>(std::vector<int64_t>{0}),
  });

  testAggregations(
      {data}, {}, {"\"$internal$count_distinct\"(c0)"}, {expected});
  testAggregations(
      {data}, {}, {"\"$internal$count_distinct\"(c1)"}, {expected});
}

TEST_F(CountDistinctTest, groupBy) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 2, 1, 2, 1, 2, 1}),
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 3, 4, 2, 6, 7}),
      makeFlatVector<StringView>(
          {"1", "2", "3", "4", "5", "3", "4", "2", "6", "7"}),
  });

  auto expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeFlatVector<int64_t>({4, 4}),
  });

  testAggregations(
      {data, data, data},
      {"c0"},
      {"\"$internal$count_distinct\"(c1)"},
      {expected});
  testAggregations(
      {data, data, data},
      {"c0"},
      {"\"$internal$count_distinct\"(c2)"},
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
      makeNullableFlatVector<StringView>(
          {"1",
           std::nullopt,
           "3",
           std::nullopt,
           "5",
           std::nullopt,
           "3",
           std::nullopt,
           "6",
           "1"}),
  });

  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeFlatVector<int64_t>({1, 3}),
  });

  testAggregations(
      {data, data, data},
      {"c0"},
      {"\"$internal$count_distinct\"(c1)"},
      {expected});
  testAggregations(
      {data, data, data},
      {"c0"},
      {"\"$internal$count_distinct\"(c2)"},
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
      makeNullableFlatVector<StringView>(
          {"1",
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           "1"}),
  });

  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeFlatVector<int64_t>({1, 0}),
  });

  testAggregations(
      {data, data, data},
      {"c0"},
      {"\"$internal$count_distinct\"(c1)"},
      {expected});
  testAggregations(
      {data, data, data},
      {"c0"},
      {"\"$internal$count_distinct\"(c2)"},
      {expected});
}

TEST_F(CountDistinctTest, globalArray) {
  auto data = makeRowVector({
      makeArrayVector<int32_t>({
          {1, 2, 3},
          {4, 5},
          {1, 2, 3},
          {3, 4, 2, 6, 7},
          {1, 2, 3},
          {4, 5},
      }),
  });

  auto expected = makeRowVector({
      makeFlatVector<int64_t>(std::vector<int64_t>{3}),
  });

  testAggregations(
      {data}, {}, {"\"$internal$count_distinct\"(c0)"}, {expected});
}

TEST_F(CountDistinctTest, groupByArray) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 1, 2, 2, 1}),
      makeArrayVector<int32_t>({
          {1, 2, 3},
          {4, 5},
          {1, 2, 3},
          {3, 4, 2, 6, 7},
          {1, 2, 3},
          {4, 5},
      }),
  });

  auto expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeFlatVector<int64_t>({2, 2}),
  });

  testAggregations(
      {data, data, data},
      {"c0"},
      {"\"$internal$count_distinct\"(c1)"},
      {expected});
}

} // namespace
} // namespace facebook::velox::aggregate::test
