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

#include <cuda_runtime.h> // @manual
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <random>
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/experimental/wave/exec/ToWave.h"

DEFINE_int32(batch_size, 1000, "");
DEFINE_int32(num_batches, 100, "");
DEFINE_int64(random_seed, -1, "");

namespace facebook::velox::wave {
namespace {

using namespace exec::test;

uint32_t randomSeed() {
  if (FLAGS_random_seed != -1) {
    return FLAGS_random_seed;
  }
  auto seed = std::random_device{}();
  LOG(INFO) << "Random seed: " << seed;
  return seed;
}

class AggregationTest : public OperatorTestBase {
 protected:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
    wave::registerWave();
  }

  void SetUp() override {
    GTEST_SKIP() << "Skipped until aggregation remodeling is complete";
    OperatorTestBase::SetUp();
    if (int device; cudaGetDevice(&device) != cudaSuccess) {
      GTEST_SKIP() << "No CUDA detected, skipping all tests";
    }
  }

  void TearDown() override {}
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

TEST_F(AggregationTest, tpchQ1) {
  // TODO: Use StringView instead of int64_t for keys.
  struct {
    std::uniform_int_distribution<> returnflag{0, 2};
    std::uniform_int_distribution<> linestatus{0, 1};
    std::uniform_int_distribution<> quantity{1, 10};
    std::uniform_real_distribution<> price{0, 1};
    std::uniform_real_distribution<> discount{0, 1};
    std::uniform_real_distribution<> tax{0, 1};
  } dist;
  std::default_random_engine gen(randomSeed());
  auto returnflag = makeFlatVector<int64_t>(
      FLAGS_batch_size, [&](int i) { return dist.returnflag(gen); });
  auto linestatus = makeFlatVector<int64_t>(
      FLAGS_batch_size, [&](int i) { return dist.linestatus(gen); });
  auto quantity = makeFlatVector<int64_t>(
      FLAGS_batch_size, [&](int i) { return dist.quantity(gen); });
  auto price = makeFlatVector<double>(
      FLAGS_batch_size, [&](int i) { return dist.price(gen); });
  auto discount = makeFlatVector<double>(
      FLAGS_batch_size, [&](int i) { return dist.discount(gen); });
  auto tax = makeFlatVector<double>(
      FLAGS_batch_size, [&](int i) { return dist.tax(gen); });
  auto input = makeRowVector({
      returnflag,
      linestatus,
      quantity,
      price,
      discount,
      makeFlatVector<double>(
          FLAGS_batch_size,
          [&](int i) {
            return price->valueAtFast(i) * (1 - discount->valueAtFast(i));
          }),
      makeFlatVector<double>(
          FLAGS_batch_size,
          [&](int i) {
            return price->valueAtFast(i) * (1 - discount->valueAtFast(i)) *
                (1 + tax->valueAtFast(i));
          }),
  });
  auto plan = PlanBuilder()
                  .values({input}, false, FLAGS_num_batches)
                  .singleAggregation(
                      {"c0", "c1"},
                      {"sum(c2)",
                       "sum(c3)",
                       "sum(c5)",
                       "sum(c6)",
                       "avg(c2)",
                       "avg(c3)",
                       "avg(c4)",
                       "count(c0)"})
                  .planNode();
  struct Accumulator {
    int64_t sumQuantity;
    double sumPrice;
    double sumDiscount;
    double sumDiscPrice;
    double sumCharge;
    int64_t count;
  };
  folly::F14FastMap<std::pair<int, int>, Accumulator> accumulators;
  for (int i = 0; i < FLAGS_batch_size; ++i) {
    std::pair<int, int> key(
        returnflag->valueAtFast(i), linestatus->valueAtFast(i));
    auto& a = accumulators[key];
    a.sumQuantity += quantity->valueAtFast(i);
    a.sumPrice += price->valueAtFast(i);
    a.sumDiscount += discount->valueAtFast(i);
    a.sumDiscPrice += input->childAt(5)->asFlatVector<double>()->valueAtFast(i);
    a.sumCharge += input->childAt(6)->asFlatVector<double>()->valueAtFast(i);
    ++a.count;
  }
  auto expected = makeRowVector({
      makeFlatVector<int64_t>(accumulators.size()),
      makeFlatVector<int64_t>(accumulators.size()),
      makeFlatVector<int64_t>(accumulators.size()),
      makeFlatVector<double>(accumulators.size()),
      makeFlatVector<double>(accumulators.size()),
      makeFlatVector<double>(accumulators.size()),
      makeFlatVector<double>(accumulators.size()),
      makeFlatVector<double>(accumulators.size()),
      makeFlatVector<double>(accumulators.size()),
      makeFlatVector<int64_t>(accumulators.size()),
  });
  int i = 0;
  for (auto& [k, a] : accumulators) {
    auto [c0, c1] = k;
    expected->childAt(0)->asFlatVector<int64_t>()->set(i, c0);
    expected->childAt(1)->asFlatVector<int64_t>()->set(i, c1);
    expected->childAt(2)->asFlatVector<int64_t>()->set(
        i, a.sumQuantity * FLAGS_num_batches);
    expected->childAt(3)->asFlatVector<double>()->set(
        i, a.sumPrice * FLAGS_num_batches);
    expected->childAt(4)->asFlatVector<double>()->set(
        i, a.sumDiscPrice * FLAGS_num_batches);
    expected->childAt(5)->asFlatVector<double>()->set(
        i, a.sumCharge * FLAGS_num_batches);
    expected->childAt(6)->asFlatVector<double>()->set(
        i, 1.0 * a.sumQuantity / a.count);
    expected->childAt(7)->asFlatVector<double>()->set(i, a.sumPrice / a.count);
    expected->childAt(8)->asFlatVector<double>()->set(
        i, a.sumDiscount / a.count);
    expected->childAt(9)->asFlatVector<int64_t>()->set(
        i, a.count * FLAGS_num_batches);
    ++i;
  }
  AssertQueryBuilder(plan).assertResults(expected);
}

} // namespace
} // namespace facebook::velox::wave
