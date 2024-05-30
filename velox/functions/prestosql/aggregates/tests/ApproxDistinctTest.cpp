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
#include "velox/common/hyperloglog/HllUtils.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {
namespace {
class ApproxDistinctTest : public AggregationTestBase {
 protected:
  static const std::vector<std::string> kFruits;
  static const std::vector<std::string> kVegetables;

  void testGlobalAgg(
      const VectorPtr& values,
      double maxStandardError,
      int64_t expectedResult) {
    auto vectors = makeRowVector({values});
    auto expected =
        makeRowVector({makeNullableFlatVector<int64_t>({expectedResult})});

    testAggregations(
        {vectors},
        {},
        {fmt::format("approx_distinct(c0, {})", maxStandardError)},
        {expected});
    testAggregationsWithCompanion(
        {vectors},
        [](auto& /*builder*/) {},
        {},
        {fmt::format("approx_distinct(c0, {})", maxStandardError)},
        {{values->type(), DOUBLE()}},
        {},
        {expected});

    testAggregations(
        {vectors},
        {},
        {fmt::format("approx_set(c0, {})", maxStandardError)},
        {"cardinality(a0)"},
        {expected});
  }

  void testGlobalAgg(
      const VectorPtr& values,
      int64_t expectedResult,
      bool testApproxSet = true) {
    auto vectors = makeRowVector({values});
    auto expected =
        makeRowVector({makeNullableFlatVector<int64_t>({expectedResult})});

    testAggregations({vectors}, {}, {"approx_distinct(c0)"}, {expected});
    testAggregationsWithCompanion(
        {vectors},
        [](auto& /*builder*/) {},
        {},
        {"approx_distinct(c0)"},
        {{values->type()}},
        {},
        {expected});

    if (testApproxSet) {
      testAggregations(
          {vectors}, {}, {"approx_set(c0)"}, {"cardinality(a0)"}, {expected});
    }
  }

  template <typename T, typename U>
  RowVectorPtr toRowVector(const std::unordered_map<T, U>& data) {
    std::vector<T> keys(data.size());
    transform(data.begin(), data.end(), keys.begin(), [](auto pair) {
      return pair.first;
    });

    std::vector<U> values(data.size());
    transform(data.begin(), data.end(), values.begin(), [](auto pair) {
      return pair.second;
    });

    return makeRowVector({makeFlatVector(keys), makeFlatVector(values)});
  }

  void testGroupByAgg(
      const VectorPtr& keys,
      const VectorPtr& values,
      const std::unordered_map<int32_t, int64_t>& expectedResults,
      bool testApproxSet = true) {
    auto vectors = makeRowVector({keys, values});
    auto expected = toRowVector(expectedResults);

    testAggregations({vectors}, {"c0"}, {"approx_distinct(c1)"}, {expected});
    testAggregationsWithCompanion(
        {vectors},
        [](auto& /*builder*/) {},
        {"c0"},
        {"approx_distinct(c1)"},
        {{values->type()}},
        {},
        {expected});

    if (testApproxSet) {
      testAggregations(
          {vectors},
          {"c0"},
          {"approx_set(c1)"},
          {"c0", "cardinality(a0)"},
          {expected});
    }
  }
};

const std::vector<std::string> ApproxDistinctTest::kFruits = {
    "apple",
    "banana",
    "cherry",
    "dragonfruit",
    "grapefruit",
    "melon",
    "orange",
    "pear",
    "pineapple",
    "unknown fruit with a very long name",
    "watermelon"};

const std::vector<std::string> ApproxDistinctTest::kVegetables = {
    "cucumber",
    "tomato",
    "potato",
    "squash",
    "unknown vegetable with a very long name"};

TEST_F(ApproxDistinctTest, groupByIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? row % 17 : row % 21 + 100; });

  testGroupByAgg(keys, values, {{0, 17}, {1, 21}});
}

TEST_F(ApproxDistinctTest, groupByStrings) {
  vector_size_t size = 1'000;

  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<StringView>(size, [&](auto row) {
    return StringView(
        row % 2 == 0 ? kFruits[row % kFruits.size()]
                     : kVegetables[row % kVegetables.size()]);
  });

  testGroupByAgg(keys, values, {{0, kFruits.size()}, {1, kVegetables.size()}});
}

TEST_F(ApproxDistinctTest, groupByHighCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  testGroupByAgg(keys, values, {{0, 488}, {1, 493}}, false);
  testAggregations(
      {makeRowVector({keys, values})},
      {"c0"},
      {"approx_set(c1)"},
      {"c0", "cardinality(a0)"},
      {toRowVector<int32_t, int64_t>({{0, 500}, {1, 500}})});
}

TEST_F(ApproxDistinctTest, groupByVeryLowCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? 27 : row % 3; });

  testGroupByAgg(keys, values, {{0, 1}, {1, 3}});
}

TEST_F(ApproxDistinctTest, groupByAllNulls) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? 27 : row % 3; }, nullEvery(2));

  auto vectors = makeRowVector({keys, values});
  auto expected = toRowVector<int32_t, int64_t>({{0, 0}, {1, 3}});

  testAggregations({vectors}, {"c0"}, {"approx_distinct(c1)"}, {expected});
  testAggregationsWithCompanion(
      {vectors},
      [](auto& /*builder*/) {},
      {"c0"},
      {"approx_distinct(c1)"},
      {{values->type()}},
      {},
      {expected});
}

TEST_F(ApproxDistinctTest, globalAggIntegers) {
  vector_size_t size = 1'000;
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 17; });

  testGlobalAgg(values, 17);
}

TEST_F(ApproxDistinctTest, globalAggStrings) {
  vector_size_t size = 1'000;

  auto values = makeFlatVector<StringView>(size, [&](auto row) {
    return StringView(kFruits[row % kFruits.size()]);
  });

  testGlobalAgg(values, kFruits.size());
}

TEST_F(ApproxDistinctTest, globalAggVarbinary) {
  auto values = makeFlatVector<std::string>(
      1'000,
      [&](auto row) { return kFruits[row % kFruits.size()]; },
      nullptr,
      VARBINARY());

  testGlobalAgg(values, kFruits.size());
}

TEST_F(ApproxDistinctTest, globalAggTimeStamp) {
  auto data = makeFlatVector<Timestamp>(
      1'000, [](auto row) { return Timestamp::fromMillis(row); });
  testGlobalAgg(data, 0.023, 1010);
}

TEST_F(ApproxDistinctTest, globalAggHighCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  testGlobalAgg(values, 977, false);
  testAggregations(
      {makeRowVector({values})},
      {},
      {"approx_set(c0)"},
      {"cardinality(a0)"},
      {makeRowVector({makeFlatVector<int64_t>(std::vector<int64_t>({997}))})});
}

TEST_F(ApproxDistinctTest, globalAggVeryLowCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto values = makeFlatVector<int32_t>(size, [](auto /*row*/) { return 27; });

  testGlobalAgg(values, 1);
}

TEST_F(ApproxDistinctTest, toIndexBitLength) {
  ASSERT_EQ(
      common::hll::toIndexBitLength(common::hll::kHighestMaxStandardError), 4);
  ASSERT_EQ(
      common::hll::toIndexBitLength(
          common::hll::kDefaultApproxDistinctStandardError),
      11);
  ASSERT_EQ(
      common::hll::toIndexBitLength(
          common::hll::kDefaultApproxSetStandardError),
      12);
  ASSERT_EQ(
      common::hll::toIndexBitLength(common::hll::kLowestMaxStandardError), 16);

  ASSERT_EQ(common::hll::toIndexBitLength(0.0325), 10);
  ASSERT_EQ(common::hll::toIndexBitLength(0.0324), 11);
  ASSERT_EQ(common::hll::toIndexBitLength(0.0230), 11);
  ASSERT_EQ(common::hll::toIndexBitLength(0.0229), 12);
  ASSERT_EQ(common::hll::toIndexBitLength(0.0163), 12);
  ASSERT_EQ(common::hll::toIndexBitLength(0.0162), 13);
  ASSERT_EQ(common::hll::toIndexBitLength(0.0115), 13);
  ASSERT_EQ(common::hll::toIndexBitLength(0.0114), 14);
  ASSERT_EQ(common::hll::toIndexBitLength(0.008125), 14);
  ASSERT_EQ(common::hll::toIndexBitLength(0.008124), 15);
  ASSERT_EQ(common::hll::toIndexBitLength(0.00575), 15);
  ASSERT_EQ(common::hll::toIndexBitLength(0.00574), 16);
}

TEST_F(ApproxDistinctTest, globalAggIntegersWithError) {
  vector_size_t size = 1'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  testGlobalAgg(values, common::hll::kLowestMaxStandardError, 1000);
  testGlobalAgg(values, 0.01, 1000);
  testGlobalAgg(values, 0.1, 951);
  testGlobalAgg(values, 0.2, 936);
  testGlobalAgg(values, common::hll::kHighestMaxStandardError, 929);

  values = makeFlatVector<int32_t>(50'000, folly::identity);
  testGlobalAgg(values, common::hll::kLowestMaxStandardError, 50043);
  testGlobalAgg(values, common::hll::kHighestMaxStandardError, 39069);
}

TEST_F(ApproxDistinctTest, globalAggAllNulls) {
  vector_size_t size = 1'000;
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row; }, nullEvery(1));

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .singleAggregation({}, {"approx_distinct(c0, 0.01)"})
                .planNode();
  EXPECT_EQ(readSingleValue(op), 0ll);

  op = PlanBuilder()
           .values({makeRowVector({values})})
           .partialAggregation({}, {"approx_distinct(c0, 0.01)"})
           .finalAggregation()
           .planNode();
  EXPECT_EQ(readSingleValue(op), 0ll);

  // approx_distinct over null inputs returns zero, but
  // cardinality(approx_set(x)) over null inputs returns null. See
  // https://github.com/prestodb/presto/issues/17465
  op = PlanBuilder()
           .values({makeRowVector({values})})
           .singleAggregation({}, {"approx_set(c0, 0.01)"})
           .project({"cardinality(a0)"})
           .planNode();
  EXPECT_TRUE(readSingleValue(op).isNull());

  op = PlanBuilder()
           .values({makeRowVector({values})})
           .partialAggregation({}, {"approx_set(c0, 0.01)"})
           .finalAggregation()
           .project({"cardinality(a0)"})
           .planNode();
  EXPECT_TRUE(readSingleValue(op).isNull());
}

TEST_F(ApproxDistinctTest, hugeInt) {
  auto hugeIntValues =
      makeFlatVector<int128_t>(50000, [](auto row) { return row; });
  testGlobalAgg(hugeIntValues, 49669, false);
  testAggregations(
      {makeRowVector({hugeIntValues})},
      {},
      {"approx_set(c0)"},
      {"cardinality(a0)"},
      {makeRowVector(
          {makeFlatVector<int64_t>(std::vector<int64_t>({49958}))})});
  testGlobalAgg(hugeIntValues, common::hll::kLowestMaxStandardError, 50110);
  testGlobalAgg(hugeIntValues, common::hll::kHighestMaxStandardError, 41741);
}

TEST_F(ApproxDistinctTest, streaming) {
  auto rawInput1 = makeFlatVector<int64_t>({1, 2, 3});
  auto rawInput2 = makeFlatVector<int64_t>(1000, folly::identity);
  auto result =
      testStreaming("approx_distinct", true, {rawInput1}, {rawInput2});
  ASSERT_EQ(result->size(), 1);
  ASSERT_EQ(result->asFlatVector<int64_t>()->valueAt(0), 1010);
  result = testStreaming("approx_distinct", false, {rawInput1}, {rawInput2});
  ASSERT_EQ(result->size(), 1);
  ASSERT_EQ(result->asFlatVector<int64_t>()->valueAt(0), 1010);
}

// Ensure that we convert to dense HLL during merge when necessary.
TEST_F(ApproxDistinctTest, memoryLeakInMerge) {
  constexpr int kSize = 500;
  auto nodeIdGen = std::make_shared<core::PlanNodeIdGenerator>();
  std::vector<core::PlanNodePtr> sources;
  for (int i = 0; i < 100; ++i) {
    auto c0 =
        makeFlatVector<int32_t>(kSize, [i](auto j) { return j + i * kSize; });
    sources.push_back(PlanBuilder(nodeIdGen)
                          .values({makeRowVector({c0})})
                          .partialAggregation({}, {"approx_distinct(c0, 0.01)"})
                          .planNode());
  }
  core::PlanNodeId finalAgg;
  auto op = PlanBuilder(nodeIdGen)
                .localMerge({}, std::move(sources))
                .finalAggregation()
                .capturePlanNodeId(finalAgg)
                .planNode();
  auto expected = makeFlatVector(std::vector<int64_t>({49810}));
  auto task = assertQuery(op, {makeRowVector({expected})});
  // Should be significantly smaller than 500KB (the number before the fix),
  // because we should be able to convert to DenseHll in the process.
  ASSERT_LT(
      toPlanStats(task->taskStats()).at(finalAgg).peakMemoryBytes, 180'000);
}

TEST_F(ApproxDistinctTest, mergeWithEmpty) {
  constexpr int kSize = 500;
  auto input = makeRowVector({
      makeFlatVector<int32_t>(kSize, [](auto i) { return std::min(i, 1); }),
      makeFlatVector<int32_t>(
          kSize, folly::identity, [](auto i) { return i == 0; }),
  });
  auto op = PlanBuilder()
                .values({input})
                .singleAggregation({"c0"}, {"approx_set(c1)"})
                .project({"coalesce(a0, empty_approx_set())"})
                .singleAggregation({}, {"merge(p0)"})
                .project({"cardinality(a0)"})
                .planNode();
  ASSERT_EQ(readSingleValue(op).value<TypeKind::BIGINT>(), 499);
}

TEST_F(ApproxDistinctTest, toIntermediate) {
  constexpr int kSize = 1000;
  auto input = makeRowVector({
      makeFlatVector<int32_t>(kSize, folly::identity),
      makeConstant<int64_t>(1, kSize),
  });
  auto plan = PlanBuilder()
                  .values({input})
                  .singleAggregation({"c0"}, {"approx_set(c1)"})
                  .planNode();
  auto digests = split(AssertQueryBuilder(plan).copyResults(pool()), 2);
  testAggregations(
      digests, {"c0"}, {"merge(a0)"}, {"c0", "cardinality(a0)"}, {input});
}

} // namespace
} // namespace facebook::velox::aggregate::test
