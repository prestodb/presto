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
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {

namespace {

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
      makeNullableArrayVector<int32_t>({
          {{}},
          {{1, 2, std::nullopt, 3}},
          {{1, 2}},
          std::nullopt,
          {{2, 3, 4, std::nullopt, 5}},
          {{6, 7}},
      }),
  });

  expected = makeRowVector({
      makeNullableArrayVector<int32_t>({
          {1, 2, 3, 4, 5, 6, 7, std::nullopt},
      }),
  });

  testAggregations(
      {data}, {}, {"set_union(c0)"}, {"array_sort(a0)"}, {expected});

  // All nulls arrays.
  data = makeRowVector({
      makeAllNullArrayVector(10, INTEGER()),
  });

  expected = makeRowVector({
      makeAllNullArrayVector(1, INTEGER()),
  });

  testAggregations(
      {data}, {}, {"set_union(c0)"}, {"array_sort(a0)"}, {expected});

  // All nulls elements.
  data = makeRowVector({
      makeNullableArrayVector<int32_t>({
          {},
          {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
          {std::nullopt, std::nullopt, std::nullopt},
      }),
  });

  expected = makeRowVector({
      makeNullableArrayVector(std::vector<std::vector<std::optional<int32_t>>>{
          {std::nullopt},
      }),
  });

  testAggregations(
      {data}, {}, {"set_union(c0)"}, {"array_sort(a0)"}, {expected});
}

TEST_F(SetUnionTest, groupBy) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 2, 1, 2, 1, 2, 1}),
      makeArrayVector<int32_t>({
          {},
          {1, 2, 3},
          {10, 20},
          {20, 30, 40},
          {10, 50},
          {4, 2, 1, 5},
          {60, 20},
          {5, 6},
          {},
          {},
      }),
  });

  auto expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVector<int32_t>({
          {1, 2, 3, 4, 5, 6},
          {10, 20, 30, 40, 50, 60},
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"set_union(c1)"}, {"c0", "array_sort(a0)"}, {expected});

  // Null inputs.
  data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 2, 1, 2, 1, 2, 1}),
      makeNullableArrayVector<int32_t>({
          {{}},
          {{1, 2, 3}},
          {{10, std::nullopt, 20}},
          {{20, 30, 40, std::nullopt, 50}},
          std::nullopt,
          {{4, 2, 1, 5}},
          {{60, std::nullopt, 20}},
          {{std::nullopt, 5, 6}},
          {{}},
          std::nullopt,
      }),
  });

  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeNullableArrayVector<int32_t>({
          {1, 2, 3, 4, 5, 6, std::nullopt},
          {10, 20, 30, 40, 50, 60, std::nullopt},
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"set_union(c1)"}, {"c0", "array_sort(a0)"}, {expected});

  // All null arrays for one group.
  data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 2, 1, 2}),
      makeNullableArrayVector<int32_t>({
          std::nullopt,
          std::nullopt,
          {{}},
          {{1, 2}},
          {{1, 2, 3}},
          std::nullopt,
          std::nullopt,
      }),
  });

  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeNullableArrayVector<int32_t>({
          std::nullopt,
          {{1, 2, 3}},
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"set_union(c1)"}, {"c0", "array_sort(a0)"}, {expected});
}

} // namespace
} // namespace facebook::velox::aggregate::test
