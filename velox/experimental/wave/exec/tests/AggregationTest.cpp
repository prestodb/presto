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

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/experimental/wave/exec/ToWave.h"

DEFINE_bool(benchmark, false, "");

namespace facebook::velox::wave {
namespace {

using namespace exec::test;

class AggregationTest : public OperatorTestBase {
 protected:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
    wave::registerWave();
  }
};

TEST_F(AggregationTest, singleKeySingleAggregate) {
  constexpr int kSize = 10;
  auto vector = makeRowVector({
      makeFlatVector<int64_t>(kSize, [](int i) { return i % 3; }),
      makeFlatVector<int64_t>(kSize, folly::identity),
  });
  auto plan = PlanBuilder()
                  .values({vector})
                  .singleAggregation({"c0"}, {"sum(c1)"})
                  .planNode();
  auto expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2}),
      makeFlatVector<int64_t>({18, 12, 15}),
  });
  AssertQueryBuilder(plan).assertResults(expected);
}

TEST_F(AggregationTest, singleKeyMultiAggregate) {
  constexpr int kSize = 10;
  auto vector = makeRowVector({
      makeFlatVector<int64_t>(kSize, [](int i) { return i % 3; }),
      makeFlatVector<int64_t>(kSize, folly::identity),
  });
  auto plan =
      PlanBuilder()
          .values({vector})
          .singleAggregation({"c0"}, {"count(c1)", "sum(c1)", "avg(c1)"})
          .planNode();
  auto expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2}),
      makeFlatVector<int64_t>({4, 3, 3}),
      makeFlatVector<int64_t>({18, 12, 15}),
      makeFlatVector<double>({18.0 / 4, 12.0 / 3, 15.0 / 3}),
  });
  AssertQueryBuilder(plan).assertResults(expected);
}

TEST_F(AggregationTest, multiKeySingleAggregate) {
  constexpr int kSize = 10;
  // 0 1 0 1 0 1 0 1 0 1
  // 0 1 2 0 1 2 0 1 2 0
  // 0 1 2 3 4 5 6 7 8 9
  auto vector = makeRowVector({
      makeFlatVector<int64_t>(kSize, [](int i) { return i % 2; }),
      makeFlatVector<int64_t>(kSize, [](int i) { return i % 3; }),
      makeFlatVector<int64_t>(kSize, folly::identity),
  });
  auto plan = PlanBuilder()
                  .values({vector})
                  .singleAggregation({"c0", "c1"}, {"sum(c2)"})
                  .planNode();
  auto expected = makeRowVector({
      makeFlatVector<int64_t>({0, 0, 0, 1, 1, 1}),
      makeFlatVector<int64_t>({0, 1, 2, 0, 1, 2}),
      makeFlatVector<int64_t>({6, 4, 10, 12, 8, 5}),
  });
  AssertQueryBuilder(plan).assertResults(expected);
}

TEST_F(AggregationTest, benchmark) {
  if (!FLAGS_benchmark) {
    return;
  }
  constexpr int kBatchSize = 1009;
  constexpr int kNumBatches = 100;
  auto vector = makeRowVector({
      makeFlatVector<int64_t>(kBatchSize, [](int i) { return i % 2; }),
      makeFlatVector<int64_t>(kBatchSize, [](int i) { return i % 3; }),
      makeFlatVector<int64_t>(kBatchSize, folly::identity),
  });
  auto plan =
      PlanBuilder()
          .values({vector}, false, kNumBatches)
          .singleAggregation({"c0", "c1"}, {"count(c2)", "sum(c2)", "avg(c2)"})
          .planNode();
}

} // namespace
} // namespace facebook::velox::wave

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init follyInit(&argc, &argv);
  return RUN_ALL_TESTS();
}
