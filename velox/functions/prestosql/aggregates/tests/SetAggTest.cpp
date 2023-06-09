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

class SetAggTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }
};

TEST_F(SetAggTest, global) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 3, 4, 2, 6, 7}),
  });

  auto expected = makeRowVector({
      makeArrayVector<int32_t>({
          {1, 2, 3, 4, 5, 6, 7},
      }),
  });

  testAggregations({data}, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});

  // Null inputs.
  data = makeRowVector({
      makeNullableFlatVector<int32_t>(
          {1, 2, std::nullopt, 4, 5, std::nullopt, 4, 2, 6, 7}),
  });

  expected = makeRowVector({
      makeNullableArrayVector<int32_t>({
          {1, 2, 4, 5, 6, 7, std::nullopt},
      }),
  });

  testAggregations({data}, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});

  // All inputs are null.
  data = makeRowVector({
      makeAllNullFlatVector<int32_t>(1'000),
  });

  expected = makeRowVector({
      makeNullableArrayVector(
          std::vector<std::vector<std::optional<int32_t>>>{{std::nullopt}}),
  });

  testAggregations({data}, {}, {"set_agg(c0)"}, {"array_sort(a0)"}, {expected});
}

TEST_F(SetAggTest, groupBy) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 2, 1, 2, 1, 2, 1}),
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 3, 4, 2, 6, 7}),
  });

  auto expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVector<int32_t>({
          {1, 2, 3, 7},
          {3, 4, 5, 6},
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"set_agg(c1)"}, {"c0", "array_sort(a0)"}, {expected});

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
      makeNullableArrayVector<int32_t>({
          {1, std::nullopt},
          {3, 5, 6, std::nullopt},
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"set_agg(c1)"}, {"c0", "array_sort(a0)"}, {expected});

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
      makeNullableArrayVector<int32_t>({
          {1, std::nullopt},
          {std::nullopt},
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"set_agg(c1)"}, {"c0", "array_sort(a0)"}, {expected});
}

} // namespace
} // namespace facebook::velox::aggregate::test
