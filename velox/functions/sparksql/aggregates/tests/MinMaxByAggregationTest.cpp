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

#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/AggregationTestBase.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::functions::aggregate::sparksql::test {

namespace {

class MinMaxByAggregateTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    AggregationTestBase::disallowInputShuffle();
    registerAggregateFunctions("spark_");
  }
};

TEST_F(MinMaxByAggregateTest, maxBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3}),
      makeFlatVector<int32_t>({11, 12, 12}),
  })};

  auto expected = {makeRowVector({
      makeFlatVector<int32_t>(std::vector<int32_t>({3})),
  })};

  testAggregations(vectors, {}, {"spark_max_by(c0, c1)"}, expected);
}

TEST_F(MinMaxByAggregateTest, minBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3}),
      makeFlatVector<int32_t>({12, 11, 11}),
  })};

  auto expected = {makeRowVector({
      makeFlatVector<int32_t>(std::vector<int32_t>({3})),
  })};

  testAggregations(vectors, {}, {"spark_min_by(c0, c1)"}, expected);
}

} // namespace
} // namespace facebook::velox::functions::aggregate::sparksql::test
