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
#include <folly/base64.h>

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
      int64_t expectedResult,
      bool testApproxSet = false,
      std::optional<int64_t> expectedApproxSetResult = std::nullopt) {
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

    if (testApproxSet) {
      if (expectedApproxSetResult.has_value()) {
        expected = makeRowVector(
            {makeNullableFlatVector<int64_t>({*expectedApproxSetResult})});
      }
      testAggregations(
          {vectors},
          {},
          {fmt::format("approx_set(c0, {})", maxStandardError)},
          {"cardinality(a0)"},
          {expected});
    }
  }

  void testGlobalAgg(
      const VectorPtr& values,
      int64_t expectedResult,
      bool testApproxSet = false) {
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
      bool testApproxSet = false) {
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
  auto values = makeFlatVector<int64_t>(
      size, [](auto row) { return row % 2 == 0 ? row % 17 : row % 21 + 100; });

  testGroupByAgg(keys, values, {{0, 17}, {1, 21}}, true);
}

TEST_F(ApproxDistinctTest, groupByStrings) {
  vector_size_t size = 1'000;

  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<StringView>(size, [&](auto row) {
    return StringView(
        row % 2 == 0 ? kFruits[row % kFruits.size()]
                     : kVegetables[row % kVegetables.size()]);
  });

  testGroupByAgg(
      keys, values, {{0, kFruits.size()}, {1, kVegetables.size()}}, true);
}

TEST_F(ApproxDistinctTest, groupByHighCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int64_t>(size, [](auto row) { return row; });

  testGroupByAgg(keys, values, {{0, 516}, {1, 507}}, false);
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
  auto values = makeFlatVector<int64_t>(
      size, [](auto row) { return row % 2 == 0 ? 27 : row % 3; });

  testGroupByAgg(keys, values, {{0, 1}, {1, 3}}, true);
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
      makeFlatVector<int64_t>(size, [](auto row) { return row % 17; });

  testGlobalAgg(values, 17);
}

TEST_F(ApproxDistinctTest, globalAggStrings) {
  vector_size_t size = 1'000;

  auto values = makeFlatVector<StringView>(size, [&](auto row) {
    return StringView(kFruits[row % kFruits.size()]);
  });

  testGlobalAgg(values, kFruits.size(), true);
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
  auto values = makeFlatVector<int64_t>(size, [](auto row) { return row; });

  testGlobalAgg(values, 1010, false);
  testAggregations(
      {makeRowVector({values})},
      {},
      {"approx_set(c0)"},
      {"cardinality(a0)"},
      {makeRowVector({makeFlatVector<int64_t>(std::vector<int64_t>({1005}))})});
}

TEST_F(ApproxDistinctTest, globalAggVeryLowCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto values = makeFlatVector<int64_t>(size, [](auto /*row*/) { return 27; });

  testGlobalAgg(values, 1, true);
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

  // Test approx_distinct with integer.
  {
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

  // Test approx_set with bigint.
  {
    auto values = makeFlatVector<int64_t>(size, [](auto row) { return row; });

    testGlobalAgg(values, common::hll::kLowestMaxStandardError, 1000, true);
    testGlobalAgg(values, 0.01, 1000, true);
    testGlobalAgg(values, 0.1, 1080, true, 945);
    testGlobalAgg(values, 0.2, 1340, true, 1028);
    testGlobalAgg(
        values, common::hll::kHighestMaxStandardError, 1814, true, 1034);

    values = makeFlatVector<int64_t>(50'000, folly::identity);
    testGlobalAgg(
        values, common::hll::kLowestMaxStandardError, 50060, true, 50284);
    testGlobalAgg(
        values, common::hll::kHighestMaxStandardError, 45437, true, 40037);
  }
}

TEST_F(ApproxDistinctTest, booleanValues) {
  vector_size_t size = 2'000;
  auto values =
      makeFlatVector<bool>(size, [](auto row) { return row % 2 == 0; });
  testGlobalAgg(values, 2, false);

  values = makeFlatVector<bool>(size, [](auto /*row*/) { return true; });
  testGlobalAgg(values, 1, false);

  values = makeFlatVector<bool>(size, [](auto /*row*/) { return false; });
  testGlobalAgg(values, 1, false);

  values = makeFlatVector<bool>(
      size, [](auto row) { return row % 2 == 0; }, nullEvery(3));
  testGlobalAgg(values, 2, false);

  values = makeFlatVector<bool>(
      size, [](auto row) { return row % 2 == 0; }, nullEvery(1));
  testGlobalAgg(values, 0, false);

  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto v = makeFlatVector<bool>(size, [](auto row) {
    return row % 2 == 0 ? true : (row % 3 == 0 ? true : false);
  });
  testGroupByAgg(keys, v, {{0, 1}, {1, 2}}, false);
}

TEST_F(ApproxDistinctTest, globalAggAllNulls) {
  vector_size_t size = 1'000;
  auto values =
      makeFlatVector<int64_t>(size, [](auto row) { return row; }, nullEvery(1));

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
  testGlobalAgg(hugeIntValues, 49669);
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
      makeFlatVector<int64_t>(
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

TEST_F(ApproxDistinctTest, unknownType) {
  constexpr int kSize = 10;
  auto input = makeRowVector({
      makeFlatVector<int32_t>(kSize, [](auto i) { return i % 2; }),
      makeAllNullFlatVector<UnknownValue>(kSize),
  });
  testAggregations(
      {input},
      {},
      {"approx_distinct(c1)", "approx_distinct(c1, 0.023)"},
      {makeRowVector(std::vector<VectorPtr>(2, makeConstant<int64_t>(0, 1)))});
  testAggregations(
      {input},
      {},
      {"approx_set(c1)", "approx_set(c1, 0.01625)"},
      {"cardinality(a0)", "cardinality(a1)"},
      {makeRowVector(
          std::vector<VectorPtr>(2, makeNullConstant(TypeKind::BIGINT, 1)))});
  testAggregations(
      {input},
      {"c0"},
      {"approx_distinct(c1)", "approx_distinct(c1, 0.023)"},
      {makeRowVector({
          makeFlatVector<int32_t>({0, 1}),
          makeFlatVector<int64_t>({0, 0}),
          makeFlatVector<int64_t>({0, 0}),
      })});
  testAggregations(
      {input},
      {"c0"},
      {"approx_set(c1)", "approx_set(c1, 0.01625)"},
      {"c0", "cardinality(a0)", "cardinality(a1)"},
      {makeRowVector({
          makeFlatVector<int32_t>({0, 1}),
          makeNullConstant(TypeKind::BIGINT, 2),
          makeNullConstant(TypeKind::BIGINT, 2),
      })});
}

TEST_F(ApproxDistinctTest, approxSetMatchJava) {
  auto bigintData = makeFlatVector<int64_t>({123, -321});
  auto op = PlanBuilder()
                .values({makeRowVector({bigintData})})
                .singleAggregation({}, {"approx_set(c0)"})
                .project({"to_base64(cast(a0 as varbinary))"})
                .planNode();
  auto result = readSingleValue(op);
  ASSERT_EQ(result.value<TypeKind::VARCHAR>(), "AgwCAEDjUEZAqnCk");

  bigintData =
      makeFlatVector<int64_t>(1000, [](auto row) { return -500 + row; });
  op = PlanBuilder()
           .values({makeRowVector({bigintData})})
           .singleAggregation({}, {"approx_set(c0)"})
           .project({"to_base64(cast(a0 as varbinary))"})
           .planNode();
  result = readSingleValue(op);
  ASSERT_EQ(
      result.value<TypeKind::VARCHAR>(),
      "AwwAAAAgAAAAAnADAAEAAAEAAQAAAQAAIAABABACAgAAAAAAAAAQAAABEAAAACAAAAAWMBAEAAYQEBAQAAACAAACAAAAAAAAAQAgACACQAIAAAAQAAECADAAABAAAAEAAQAAAABgAAAAIQADAAAgMAAAYCACAAAQAAAAAAAAABEAAgAAMBEhAAAgAAAAEAAAAAAAADAAAREAEAIBEAMAAQEAAAAAAAACAAAQMwAAIAAAAAAwAQAAAAARABAAAAACAAMAEAAlAAAAAQAAEAAAIAAFAAAhBQAgAAAABAIBAAAAARAQAAAAABEAADAAABAAIQAAEAEAIBEAAgAAAAAhAyAAAAAgAAAAAAAAEAAAAAIAAgACABAAAAEAAAAAAAAEAAAAABABEAAQAAAAAAAAEAAAIgAxAQAAAANAAAAAABAAACACAAEAAAABAAARIAAAEgAwABAAAAAAAAAAAQACAQEAUAABAQIAEAAAAAAAAAAAAAEAAhAAEQMABwEgAAEQEAAwAAAACAEAAkAgAGAAAAUAAAAREAIAAAEBACMBAAMAADAgAQAAAAAAAAAAAAAVAAAxEAACAAAAESAAAAAAAAAIJQAAIAABEAAAEAAQACAAAAAQABAAAgABAAAAAAABADAAAAMAIAAAAgAAEwAxEAAAQAEAAwBAAAQAEAAQEAECAAEAEAEAECAAAAAAIgAAAAAQAQABAAAAEAAAAABwAAAAAQAAARAAACAEAFAQAABQAAAAAAUAADEBAAARACAAEAACAAEDAAAAAAAAACIAMAAAAAEAIAAAABAAAAAAIAAAEQAABAAAAAAAIBAAMAAAAQAAAAAAABMAAAEAAQAAAAAEAAAQIBAAAiABAAMQAAEAAwAgACAAAQAQAQAAAAADIgEAQQAAAAAAAAIAAAAAAAAAEAAAAAAwAEAAIBMAAgAAAAAAAAAQAAAAEBAAAAAAIAAAAABDAAAAAAAQAQQAEAIAAAEAAQAAAkABAAAAAAEAZwAgAQAAAAAAAQAAAAITAAAAABAQAEAAMgIAAQAAAgACAAACEAAABQAAAQAFAAAhAAAAAAAAACABADAAAAEAAQAABwAAAAEEIAAAAQADAAIAEAAAACABAgAAAAAAABAAABAQAwAAUgAEADAgAAAAAAAAYQAgAAEAAAAAAAAAAQAwAAAAAAAAAQIAAQAABSAAAAAFAQAgACAEAAUAAAAgIAQAADAAAEIAAAAAAAAQAQACMAMAAAEDAAAAABAABQAQAAAAAAAAAAAAAAAAEQABAAAAEAAAAANAABACABAAAABgAAAAECACAAAAACIREAAAAAACAAAAAgEAAAYAEQAQABAAAAAAMAAAAAAwAAAAEAEBUAIAAQAAEAAAIxAAAEAgIQMAAQIAAAAAAAAAAAEAAAAAMBAQAAAAAzAAAAUQABAQAAAAAAAAAEAAARAABQIAAAUAAQAQAQAAACAABAAwAAADEDAAEAAAAAIAAQAAAAEAAFAAAAAAAiQAAAAAEAABEAAAAAIAAQABAAABAEAAAAEAIAAAAAAAAAAAAAAAAAAAEAAAEAABAAEAAAIgEAAAAAAAABAAAAABABAAERIAAgBAAAAQAQICACEAACASAAEIAAACAAEEIAAAAgAAABAAAAAAAAAAMBAAAAYAABAwEgACACABAAARAAAAUAAAAgABAQABAAEQAAAAAAAAQAAABQBgAAAAAAAAIAAAAAAAEAAgAAAAAAIGARAAABIAAQEAAEAAAQUgAAAAAQAAAiAIAAAAAAEAAAAwACAAADABAAAgACAAAAIAAiAQAQAQAAAAAAEAAAAQAAAQAAAAAAEAEAAQACAAAAAQAAAAAAEAACASAAACBwQGAQAAAAAiAAEwAAABEAAAAAAAAAAAAAQAEAAAAAIAIQAAAAAAAAAAEBEAAQAAIjAAAAAAASAQASAAASAAAAAAEDACAAAAAAAABgAQAAAAEAUAAAAAAAAAAAAAESABMAAAAAAAAAAQEAAAEAATAAAAAAABEBAwAAIAAAAAAAACAAEAEQAAAAAAAAAgAQAAABABADEgAAATAQAQAAAAAgABIAAAEABAAAADAQBAAAAAAAIAAAAAAAAAFAAQAQAAADAAAQMAAAAFAAQBAAAAAABBAQAAAAEAAAAQJAAABAAAAAIAAAARMAAAAwAAEQAABAMAACAAAAACAAAABAAAAQAwAQAAAAAAIAAAAAAwA0EABQAAAAAgBAAQAAAQIgAAARACAAAgAAAAEAAAUAAAAQAwAAAAAAABAAAAABABAAEQAwAABCEAQAAwAAAAACMAIAAAICAAEAIAAAAAMAAAAAICEAEAABAQECAAAAEAAAAAABAAACEAAEIAFAAAEAEAIAAAAAAgACAAABACAAEAAAAAABAAIAAwAAACABAAMAAAAgAAAAAAAwAAARAAJQAAACAAAAACAwIAAAAHAgBAAAAAAAAgAwFAAAEAMCAAAAAAAQAQAAADUAAAEAAAAAABAAAAARAAIBEgAAAgAAFDBQAAADAAACAAIAAQAyAAAAAAAAAAAAMQABIgAAEAAAAAAAIAAAAAAAIAAAAAAAAAEAEBABAAAAARAAM0AAAAAAADAAAAAAAAAwBRRCAQEAAABBIgAAAQABAAAAIQAgAAAxAAAAEAAAAAAgAAAgAAAAAAASAAIARgAAIAABACABAAAAAAAAIAAxAlAAEAAAAwAAAAIAADIAAAAAADAAAAAA==");

  auto stringData = makeFlatVector<std::string>({"123"});
  op = PlanBuilder()
           .values({makeRowVector({stringData})})
           .singleAggregation({}, {"approx_set(c0)"})
           .project({"to_base64(cast(a0 as varbinary))"})
           .planNode();
  result = readSingleValue(op);
  ASSERT_EQ(result.value<TypeKind::VARCHAR>(), "AgwBAAEtW5g=");

  stringData = makeFlatVector<std::string>(
      1000, [](auto row) { return std::to_string(row); });
  op = PlanBuilder()
           .values({makeRowVector({stringData})})
           .singleAggregation({}, {"approx_set(c0)"})
           .project({"to_base64(cast(a0 as varbinary))"})
           .planNode();
  result = readSingleValue(op);
  ASSERT_EQ(
      result.value<TypeKind::VARCHAR>(),
      "AwwAAAUAMABAAAAQAAABABAAMAAAAQAAAAABAAoAAAAAAAMyBAEAACAAQAAGACAAEAEAAEAAICAAAgEAABAhABAAAAAAAAACABAQAAAAAEMgAAAAAAIgABEAAgAAAABgACEAAAAAAAABAAAAABAAAAEAAAEAAAcAAAAAAAAQAAAAACAAACAhAAAAABAAAEAAABABAQECAWAEAAAAEAQAABACIAAAABAAADAQAAAAEAIAAAAAAAAANAAwAAABEhAAAAAAAAAAADAEEAAAACAAAwAAAgAQAAAAABAAAQAAICIAUAABAAIAAAAAAAAQADAgABEAEEEAAAAAAAAQAQAAAAEAAFAAIgAAAAABAAQAAAAAABAQBAQAABADAQEAADAgAAAAAAAAAAAAAgAAcgEQAAAAACAAAAAQAAAAEAABAAAAABAAAAAAAQADAAAAACIAAAEAABACAAAwEAAQABAAEDAGAgAAAAAQAgAwAAAAUAAAADEAAABAAAAAAAAAAAAAABEAAAAAAAAAAEAAAAUQACAAAAAAAAAAAAAQAAAAIAAAAAAQIAAAABACABAAAAAAIBAQAwAAAAAhADEAAAACAAAAAAABAAIAAAAAAAAAACBwABAAAAAAAAAAECAAEAACUAAAAAADIAAAAAIAIAAAAAQAAAAAIBACERAAAAAAAAAAQAEQBAAAAAAAAAIAAAABUAAAAAAxAAAAAwAQAAAAEAAAAAAAABAgAAAiAAAFEQAgBSACIBABIAAiAAAAAEAAAAEAAAIAEAABAAABABACAQEyAQAAAwAgEAEQAAAgEAAQAAQAECJQAAQBAwAAIAAAAgABAAEAIAAAAAABABABEAAQAAAAAgAABAAQAAIAIAAgBwAAAAABAAEAAgAQAAABAAAAAAACAAAQAAAAAAAAAAAAAAAAEgAAAAAAIAsBAAACAAAkAAAAQDABAAAAAAEAAAYAMAEAAAAAIBACAAAQMQAAAAAAMAAAAAAAAAAAAAUwACAgAAAAAAAQAAAAAQAAEAAQAAAAMCAQAAAAQAAAAEEAAAAgAAEAAAAAEQAAABAAIAACAFABEAABEBAAAwMAAAAAAAAAIAAAAAABAAAAABAAAQAAATIAEAABAAAAAQAAAQAQAAIAAAADEAAAEABAMAABAAAwAAAQAAAAIAAAAAABAAAAAwAQAAAAAAAAEBAEAQAAMCAAAAABEAABAAABAAERAAIwEGQAAwIAAAAAAAAAAAAQABQwAwAAAAACAyAQIAAAQgAwIRAgIQAiAgAQACEAERAAQAABAAABAAABAAAhEwAAEQAAEQAAAAAAAAAAAAAAEiFAAAIAAAIAAAAAAQEBAAAAEAAAIgEgASAAABAAEAAgABIgIBEAAAAAQQAwAEAAAAIAAAACAAAAAQAAAAAABwAQAAAAEAABIAAAABEAIAAQAjAAAgACAAASAAAAAAAAECACAhMAAAIAEAAAEQIBIBAAAAACAAAAAAAAAiAAEAAAAAAAAAEDAAAQAQASEAAAEAAAAwAAEAAAAQAAECABAAAgAAAAAAMBAAAQAAAAFAAAAAFjAQAAAAAAAAAAAAABAQAxAAAAEAAwABUAABAAMQABAAAAJgACEABQAAIAAAAAAQAAAAADAAAgAgABABAAABMAUAAAAwAAAAMQBAAAAAMAAAAAEgAAAwAAARAgAAAwEAACAAEAAAACADAAAREQAAAAABAAAAAAAAABAAAAAAAwMAAAAAAAAAABIRASAEAgAAAAAAAABAEwADAAAAEgAQAAAAAAAQAAAAAAAAAAAAADEDABAAAgAAASAQAAJAAAAAQAAAEhARAAAAIgIAAAIAAAAAARAAABBQAQAAIAABAFAARzAAEBAAIBAAEAAQAAABAAAEAAIAAAAAAAAQYBMAAQEAEAAQAAEwAAACAAADAAAAEjEAAAAAAAEDAAADEAAgAQAQAAABAAIAAgAAAQAAIDAHIAAAAAAAAAAAAAAAAAAAIDAgAAAAAgAAABAQAAAAAHAAEgAAAQIEIAAAAAAAAAASAwAwAAEAAAAAEAAgAAAQAAAAIgAwAAAFAAAQAwAQAAAAADAHAgEEAAAAAAgCEgAAMSAAAwAAAQMAAgEAAAIAQAAwAgAAEAAgUBAAAAAAAAIAUAQQQAACAAAAAAAAEQAEAAAAAAAAAAAAIAAAAAMAAEMAABAAACAAAAAhAAMBAgACABAAIAAQFQAAUCUAAQAhcAACABAAADAAIwABAAAAARAFAAAAAAJQAAEAAAAAEgQBQAAAACAAAAAAAAAAAAEAAAAAAAAAABAAAFEAAEAAABAAMAEAIAAAAAAEBVIAAAEDAABBUAEEEAAAAAASAAAAEAAAABAAAAAAAAAAIAMAIAAAAAAQAAQAACAAAAACAXAQIAECACABAgAAAAAwAAAAAAAAAwAAAAAAQgABAAAAABQAIDABAAAAAAADEAAAAQEAAAYDYAAAAAAAEgAAAQAAAAAgAgEQEAEAAAAAACACAQACEAAAEAAAIAAABAIAMAAAAAAAAAAQAAIAEBAAICAAAABQEBABACAAIBAAAAABAAAQAEABAwAAAAAAEAAAABUAECAAAAAAEAABEAABAAAAAAABAAABBQAAACAAABAAAAEDAAAAIEAAAAAQAAABAAAAMAEAAAEAABNQIAAQAAAAAQAAECAAESABMAAAQgADAQAAAAAAMDFwAiAgABAQABARAAAAACAAAAADAAAA==");

  auto doubleData = makeFlatVector<double>({123.1, -321.1});
  op = PlanBuilder()
           .values({makeRowVector({doubleData})})
           .singleAggregation({}, {"approx_set(c0)"})
           .project({"to_base64(cast(a0 as varbinary))"})
           .planNode();
  result = readSingleValue(op);
  ASSERT_EQ(result.value<TypeKind::VARCHAR>(), "AgwCAACW723A9ER8");

  doubleData =
      makeFlatVector<double>(1000, [](auto row) { return -500 + row + 0.1; });
  op = PlanBuilder()
           .values({makeRowVector({doubleData})})
           .singleAggregation({}, {"approx_set(c0)"})
           .project({"to_base64(cast(a0 as varbinary))"})
           .planNode();
  result = readSingleValue(op);
  ASSERT_EQ(
      result.value<TypeKind::VARCHAR>(),
      "AwwAAAAAAwAAQAAAAAASAAAAAAAAABAAAAAAAAAAAAIBAEAAAAAAAAAAAAkAAAMgMAAAAABQAAAAAQAAACIAAAMAAAAAAQBnIAAAAAABAAAAAREAACAAAAAgEAAAIAADEAAAAAMAAAEAADAAAhAAEAAgAAAAACAgCAAAAgEAkAAAIDAAADAiBAADMAAAAQEQAAAQAwAQAAABEhAAARAgARAgAAAAABAgAAABAAACAAAFEAAAAgAgAAAQACAABQAAABAAAAECAAAAAAEgEAAwAwAAABAAAAEAAAAQAAAAIAAEAAAQAAAAAAAAMBBwECAABQQAAQAgAgABAgFQMQEAEAAAAGEgAAAAAAAAAAAAAAAQICAAAAAAABASAFADJxAAABABAwAAEwAAAAMBAgAAQAABJAAwAQICAAADMCAAIwAAAAAAAAACACAAAAAAABAUABAAACAAEAAAAQAAADAAABIAAwAAMRAAAAAwAQAAAAAAAAEAAgIAAAACABAAABEAAAAAAQMAMAAgAFUAAAAAAAEAAAIBIAAAAAAAAxAAAAAAAAAAMAAAAAAAAAAAAAAAAAAABgAhEAACIAAAEAAQAAAAAAEAAAEDAAQAAAAAAwEAAAACAAACEAABAAABAAACAAAAIAABAAMwABAAIAEAAUABAjABAAQQAUAAAAQAAAAAABAAAAAQABAAAAADAAAAQBASACAAAAEAAAIiAAAAUgAAAABgAAAwAAIAACAgAAAAEEAAAAAQIAAAAAAkAQAAAAAQAGESAAAAAAACAUAAAAAAAgAQEAAAAAAAAAAEAAAAEBAAIAABEDAAEAAgMAAAAAAQAAAAAQAAAAAAACAAAAEgUAAAAAIwIgAQAAMBBQAAIAEAEAAAIAEAAABwAAAAAAEAYAAAAwABACBSAAEQBQAAMAACAAABASAkAAABAQAAAAAAEBNAEAEQAQABMAAgAAAAAAAAAAEAAQAAAQAAEBAgAAAUABAAEAAAAAMAAAIAAAAAEAACAwAAAAAAAAAAAyAQABIAAAAAAEEAExAAAAAAADAAABAAAhMBBAEgAAAAQAAyEEAAEBIAAQAAAEAAAAAAAQAAAAAAAAARAgACAAcAMBABAAAEAAAAAAABBRAQAgAAEAAAFAACAAAAEAAAABAQAwADAAAAIAAAABBgAAMAEBAAARAABgAAAgAAADEAACEAMAIAAAEABAIgAAAAAgAAAAAAAAAAIAAAAlEAAAAAAAAQAAEgAAAAAQAwACAhAAAQUAAAAAEgAiAAEAAAADAAAAAAAhACAAAAAAEQAQAiEBAAAAACAQAAAAAAEAAAAAAAAFABcAAwAEAAAAAAAAAAABEAEABAAAAAMAAAAAAAAAIBAwAQAAEDEAUCMAAAAAAQAAAAAAAAEAAQAAEAABAAABABMAACAAAQAEAkEAAAACAAACAAAAADAAASAAAEAAAAAAAQAAAAAQADAAAABQAAAEAAAAAgAAEAAgAAAAAAAAAAATAAAAAgEDAAEAIwAAAAAEAAABgAAAIAABEAAQECAEAAAAAAAQECAAADABAAABAAAgEAAQAAMAAAAAMAAAAAAAECAAAAAAIAAgA1AAAAEBABAAAAAHEAA1ABAAAAAAEAAAAAAAEAAAAAAAAAIAAAAAAAAAIBACAgAAAAAAAAAEACACACAAAAAAEQABEBAAAGAQECAAEAMAAAAAIAAAABAQADAAAQEAAAEBEAAQAwATABAgAAAAEDAREAAAADAAAAAAAwAAAAAAEAEAEBAAAAAAAgAQFBAAAAAgBAAgAAAQAHAAAAAAAAAQAAAAABAAAAAAAQAAAAAAAAIDAAAAAAAAAAMBAAAAAAAKEAAAAQAAEAAAAABRAAAAAAAQAAAAAAAAAhAAEjAQAAAAAAAQAAAAMAACAgIAAAAAAAAAAQABAAcQAAAAABABMwAAAAJjAAAAAAAAACAQAAABIAAAABACIAAAIAAAAwAQAAAAAAABAAAAAAAAABAAABACAEAQAABgAAACEAAAAABAAAABAAEAAhAgAAAAAwAAAAAkAAIAIAADAQEAAAAQAAAAAgACABAgUQAAMQAAACABAEAAEAAQEAEBAAABAAABcDAAAAACACEAAgAAACEjMAEBAgAAABAAAAAAAiAAABAQARAAMAEAEAAnAAABEAMDAAABAAAAAIAiQDIAABAxAAIAAAACsAAAAAAAAAAAAAAEAAdCAwAAAQAAABABAAABAAAAAAQAARAAAAAwEAEAACABEQIEAAAAAAAgAAAAABAAAAAAADIQBAAhABMAEDACAQAAAAAAAAEBMAIAAAAAAAABAAAAUAAAAQAAABAAAABCACEDMCBQAAQAAAAAAAAAEwAAEAAAAAMABAACAAAAEAABABACAAAAAAAAAAAAEQEAAQACACAAAAAAAQAAAAAAAHAAABAAAAAQAAEAAAAAAAAAAAEAACAAAAAAMAAQAAABADAAEAEQAAAhAAAAMAIAMwAAAAACAAAgAAAAAAACAAQAAQABAwAQAIADAgAAAAAAIAMDAAAAAAAAACAAAAAAAAAAAAAgAQAQAAAAAAAAAAAAEAEDAAEAAAARAEEQAFBDcAADABEQAAADAgAEExAAIAAAAAAAAAIAIAAAAQAgEAQABBAAJAAAADAAIAAAEAIAAgAAEiAAAAAAABAAEBABAAAQEAAQAAAAAAAAABAEAAAiAAABEQAAAAAAEAAAADAQAAADAAAA==");

  auto unknownData = makeNullConstant(TypeKind::UNKNOWN, 1000);
  op = PlanBuilder()
           .values({makeRowVector({unknownData})})
           .singleAggregation({}, {"approx_set(c0)"})
           .project({"to_base64(cast(a0 as varbinary))"})
           .planNode();
  result = readSingleValue(op);
  ASSERT_TRUE(result.isNull());
}

TEST_F(ApproxDistinctTest, mergeMatchJava) {
  // Base64-encoded HLLs from Presto Java.
  // Dense HLL: 0--999
  const std::string kHll1 =
      "AwwAMEAgAAAAAAAAAQEAAAAAEQAAAEEAAAAAABAQAgAQAQAAAAADAAMAAAAAACAAAFAGAQAUcREQEBAAAAICAAAgACAAAAMAAQAgADACQQIQAAAABDAAADAAAAAAABEAAQAAAABgAAAAIAAAAAAAMAADYAESAGAQACAAAAAAAAEAAgAAMAEhAAAAAAAAAQAAAAAAADAAABAAAAEABAAQAAEAAAAAAAACAAAAMAAAAAAAADAAAAAAAEAQAAAAAAAAAAABEAAAAQAAAQAAEAAAIFAWIAAAAAAgADAAAAIBAAAAABAAAAAAAAEAADBAABAAIDAAAgABAwEBAABAAQAgAABQAAAAIAAAAAAAEBAAAAIAABACABAAAAAAABAAAQADAAAAAAARAAAQAgAQIAAAEAAAAgABAQAQAABAADACAgAAAAACAAEAAAABAAARIBAQEAIAMAAAAQAAQAAAACAxAgAAUAACAQAAEAAAAAAAABABAAAAAAEgAQMAAAAgAAEQEAAgAAAAAAEAAEAAAGMAEAUAAQABAAAAAAIBAQAAAHMAAAAgAQAQAAAAECAAAAAQAAABABACABAAECAQADAAACAAIgAAIAAgEiACEAAQECAQAAABAAEAAAQBAAAAAQABEAAAAAMAIAAAAgAAAwADEAAAQAEAAwBAAAAAABAQAgACAAAAEAAAACEAAAAAIAAAAAAAAQAAABIAEAAAAAASAAAhAAAAIBQAACAEABAAAAAwAAAAAAAAADEBAEAAAiAAAAAiAAICAAAAAhIAQCIARQAAAAEQAAAAAAAAAAAAIQAAEQAABAAAAAAAABAAAAAQIQAAAAAAAAADAAAAARAAAAEQAAAQIAEAABABAAAAAAAAAwAAAAAAAQAQAQAAAAADAAEAQAAAAAABAAIQAAAQAAAAAAAABQBQEEQAIEMAAAABAQAAABAQAAAAAAAAAAAAIAAAEAFDAAAAMAAAAQAAAAIBAAkAAAAQAgAAAAABAAEAZxAAAgAAAAAAAAAAABATABAAABAAAgIQMgIgAQAAMAACAAECEAAhBQAAAAAAAAAhABAAACAAQAABADQBAAEAAQAABwAAAAAEIAAAAQETAAAAEAAAACAAAAACAAAAABAQAABQAQAAAQAAAwAgAAAAAAAAAAAAAAEAAAEgAAYAAQAAACAAAAAgAQIBAQAAUAAgAAAVAQAgAAAEADUAAAAgIAAAADAAMEAAAAAAEBAQAAAREAMQAAEAEAAAAAIQBQAQAAAAAAAAAAAAAAAhAAABAQACAAACAAAAABAAACABEABgAAAAECAAYAAAAAARAgAAAAASEAAAAgEAIAAAAQAAACAAABMAMAAAAAAAASAQEAABUAAAAQAAAAAAIBAAAgAAAAMAAQIAAAAAAAAAUAEAAAAQMAAAAAAAADACAAUBABAgAAAAAAAAAAAAABAABQEAAAAAQAAQAQAAIAEBBAIAAAADERAAEAAAAAKQAQAAAAEAAVAABwAAAgQAAAAAAAABAAAAEAAgAAABAAABAAAAACEAAEAQAAAAIAAAAAAAAAAAEAAAAAAAAgMCAAEgEAAAABAAEBAAAAAAABAAEQIAAABAAAAAAiIAECEgAAACABEQAAACADAEAAACAgAAABABAwEAAAAAABAAAAAAABAwEgAQICABEAAAAAAAUAAQAQABAQAAAAAAAAAAAAAAQAIABQBgAAAAAAAAAAAAAAAAAAAgAAAAAAIBEQEAABADAQAAAEAgAAUQAAAAATAAMCAAAAAwAAAAAgAAEAAQADAAAAAgAAAAACIAAAAQAQAAAAAAAAAAAAEQABAQAAAAAAEAEAATAAEAAAAQACAAAAAAYCECAAACAAQGAAAAAAAiAAAwAAAREAAAABAAAAAQAABQIAAAAAAAIDAAAAAAAAAAEAEAAQAAABAAAAAAASAQAAAAASAAAAAAFCACAAAAAAAAAgAQAAAAAwUAAAAAAEAAAQAAAQAAMAAAAAAQABAAAAAAYAAAAQACAAAAQBAAAQEAAAAAAAAAAAEAAAIBAAAAAAAgAQAhAAAhACEAACADAAAAAAABEgAAAAAAEABAABAAAAAAAiAAMAIAQAAAAAAQEABCAAAEADAAAAMAAwAAAgQBAAAAAgBBARAAAAEAAAIAFAAAAAAAAAABAAAAAAAAAAAAEAAABAAAAAAAAAAAAAAABAACAAAwMQAAACABIAAAAhAAAAECJQAAABAABAAQAAAAAgAAEBEAAAAgAAAAEBAAUAADIQAwAAAAEHABAAABABMhAAAAAwAABCADEAEwAAAAASAAAAAAICAAEAAAAABQMAAFAAACAwIEAAAQACEAACEAEAAAABAAACAAAEJQFAAQEAABIAAQAAAAASAAIBAAAAEAAAAAABABEQBAAAAAABAAMBAAAiAAAAAAAwAAAhAhBgEAIAAAAAACAwIAAAAAAABAAAAAAAAAAxEBAAAAQCIAAAAAARAQAAEQUAAAAAAAABAAAAAQBQEAIAEAIAAgEABABQAQADYBAwAAIAAAACAQAAIAAAIAAAAQABIgAAEAAAAAAAIAAAIAAAIAABAAAAAAEAABABABAAAhAQAwABAAAAAzAAAQAAAAAAABAEASEAAABBAhAgAAABAAAAJAEgEQEAAAAAEAAAAAAAAAMkAAAAAAAiAAAARgAAEAABBSACAAAAEEAAIAEQAhIAExAAAAYAAAIAEDIAAAABAAAAAAAA==";
  // Sparse HLL: -50--49
  const std::string kHll2 =
      "AgxkAIADRACAoGgEQOzJBsGR7QZERuYIAYAMCsBUdwoBLsQMxc8SDcDF1A+A3UsRADQvEgFe9RLAXfwSQDeXF8KhUBiA5xIbhh00G8CPWhvEo9gbAY9oJER5iCWArhUogGPfKABGriyAGHAwgSnhMAT9BjEAY644gtKHOQDytj9CLGBDAbAqRYAcTEXAXUZJQfw7T8BZt1BAwsdQAP3bUwBYPVtBUx5cQT6aXUEV010Bwcpggqk7YYNFsWaAqphowNuMaoKfnW2BsHBvgK2WckH+oXgBROt4AmIUfILMlXzBkEt9QaLygYLuKoRA3MqFAKryiYHUaopGdgaQwIjrkYORXZXC9+GVgH0Qm4C/qpuAuyCdQLLkoIEasKVAfQenQrpip4GeDKmDEmupgFj3rQHSGbTCV3a0gcOstgDl7LuAQ/q8QFWVvUSf+b6AveS/ANebwYG5cMdAs8zJAT8+y4B1d9qAGxrdgCAI3gCQJOKCDVnqgTkZ7EAOw+yBZeXtgeJJ8oN4k/NAVOz1QEC19wD8zvc=";
  // Sparse HLL: 2020--2021
  const std::string kHll3 = "AgwCAEJW8wAAseQ4";
  // Dense HLL: 1000--1999
  const std::string kHll4 =
      "AwwAAhEAAAABEAAAAAAAAAABACABAQAAAAMBABAAIAAAAQEAAAASABAAAAAAAAAAABAAAAAAAAAAAEABAAAHAAAAAAABAAAUAHABQGADAAIQAzACAAAAIQAAAAAAMAIEAAEAARAhIjAAAABAAAAQAAABAAMAAAEwAAIAAAAAAEAAAhABEQAAAAIQAAABABEAAAAAAYAAAAEDYQAAAAAEIBAAAAAAAgIAQBAAAAAAEAAQAAMAAAAAAwAgAAAAAAAABBUAAAEAAAAAEwEAAAAAEAEAAAAAAAMgAAAABCAAAAEQACEAQCAAAAACAhAAEEAAABABAAAAEiBFAAAFAwALAAADAgACAAAgAADQEwMAAAICAAAAAwAAEAERAAAAAAAAAhEAAAAAAAAAAAIQERMAAQIQAAAQAAAAAAAEAEAAAAAAAAAQAAMAIDAAAAACAABAAAAAABEyAAEAAAAwKSIAAAIAAAAAIQUCEAAAAAAAAAEAYCABAAAAACAAAAARABAAAAABAAEAABAAAAAAABAAAAQQEAEAAAAAACICIAAAAAAAICIFACAAAAAAAwAAAhEAAAAAEgMBEQAAAgFAAAAgADEAABAAAAARACAAAgAAAAAiAgIAAAEAAAAABCAwAAAQAAAAABYCAABgAAAAAQAAAAAAAAEAAQAAABEEAAAAAAEAAAAAABEAAAAQAAAgBAAAAAAAAQASAQEAAAIAADAAEAMkAAACAgQDEAAAEAAAAZAwIgAEMCAAAAMQEAAAADAQABAQIBAABAAAAgAAEBAAAAAQMEAwABAQAAAWMAAAAAEAMAAAAAAAAAQAAAAAAAAAABFAAAAAADAAEAAgAgAAAAAAAAAAAAAAUAAQQAAAAAAAIQMBAAIEEAMAAAAAAAIBAgAAIAIAAAAAAAAAEAAAAAAAEAEAAAAgIAAAACAAAAOwAAAABAAAAAAAEAABAQAzABICAAAAAAAAAAAAERABACACAAAAAAAAAAAAAjAAAQAAAAAAAQAAAAAAAAMBAgACAAEAAyEAEAMAAAAAAQFAMAEAAAAAAGEAMAAAAAACAAAAAAIAABAAEAADAFAAACAAAAAQAQAAQAABABIAACIQAwAAAAAAAAIgIhAgACAFIAECAAAgMgMwABAAAgAABgAQAAABMQAAIAAAAAEEAQEDEAABAAEBIAAwEAAAAQAAAAAAAAAAABAgMAACADAAEAMAAAAAAAEQBUAAAAAAACAQAAAQARACAAAAAQABAQABAEAAAAAxIQBAATAAAAAAAAAAAQAAAAAQABIAAAAAACABABAAMAABBgAFAAAAADAQAAADAAAAAAEAEAACAAAAEgAAAAAgAAEQAAAAECACAQAAAAACAQAAABUAEQAAADAgACAAAAARAAAAYAEBAQIwIAAQAAAAMAABAAAAAAAAAAAAAAAAAAEAEAAABAABAAMAAgAAAAAAIQAAAAAAAAACAAAAAAACAAABAAAAAAIwIAAAQAAAAAEgAAACEAMgAwAAEAAQAAACEAMAEAABEAEAACAAAAAgEDIAAAIAIAAAAAAAAwAAASEAACADAjABAAAAEAAAAAAAAAIAAANgAwAAAAAAABMAARAAAAAAAwAwAxAAEwAAAAAAIAAQAAAAADAAAAAgAAAAAAIEAAEAABAAAQAAAAAAAAEAABMAAAAAAAAAAAACAAAEAQMAEBAAAAEAAwAAAAAAAwAWAQAQAAAQAAAAAAAQAQMAYAAAAQAQICAAAAACAgAAIAACAAAABiAHAAAAAQAAQAABAQABBwAgACARAEMAAAAAEAAAAAAQAAAAIBEAAAQAAAAAEAAAEAAFAAAQAAAAUAADADAgAAABAAEAARAAAAEQACAAAAAAAAYAAAAAEAAAEAAAAAAAIQADAAAAABAAAAAAAAAyAAAAAAAAAQAAAAAAABAgAABBABAwADAAAgAAIAABAABwAAEABgEAAAAiAAMAAEAAAQABAAAAAAAAEAAAAAAAAAAAIAAAAAAQAAEAAAAAAAEAAQIAADAEACAGABAAABIQAwEAAAAAAAMBAAAAAAAAAAAwAAEAAAEAAAAwEAMAAAIQIDAAAQEAAQEAAgMAAAAQEAEQAQAAAAAAABIAAAAAAAEAABAAcAEAEQAAEQAAABAAAAAAADAAAQAAAAIAAAAAEAAAAQAAAAAAATAAAAABAAAAAwAAEQAABAAAAQAAAAAAAAAAAAAAACAAAAEAABAAAgBTAAAAEgAAAAIAAAEAAAABAAADAAAAAAAAAAAAABQAAAAAAFAAAAMAAgAAAAAAABEAQAIgABAQABEBBQAiADAAAAAAAAABAAAQAABxECEABwAQAAAAEAAAAAAAIAAAAAAAAAAAAAAAABAAEAAQAQAAABAAACACABAAAAAwAAADAAADAAAAEAACUDAAAgAgAAAAAAAAAAACAgAQAgAQAAAgAFEAAAAAARAAABMAAAAAAAEQAQMBAAABEAJAAAAAAAAAIQAQAgAAAAAAAAAAAAAAAwEgAgABAQEQAAAAAEIAAAAAAAAQIBIBIAAAMABBAAEAAAAABBAAAwACEAAAIQABAQEAAAAAAAAAABEAAREEAAUAAQAAAAABADAAAgIAAAEhFAAAIFADAAAAYIABASEAEAAAAAAAAAAQAAABABAQABAAAQAAAAAAABABADAABwAAQgEAAAAAEgAAAAAAEAACARAgAAEQAAIAAAAAAAAAMAAAAAEAEAAAAA==";

  // Check that Velox produces the same HLLs as Java.
  auto data = makeRowVector(
      {makeFlatVector<int64_t>(
           2102,
           [](auto row) {
             if (row < 1000) {
               return row;
             } else if (row < 1100) {
               return row - 1000 - 50;
             } else if (row < 1102) {
               return row - 1100 + 2020;
             } else {
               return row - 1102 + 1000;
             }
           }),
       makeFlatVector<int32_t>(2102, [](auto row) {
         if (row < 1000) {
           return 0;
         } else if (row < 1100) {
           return 1;
         } else if (row < 1102) {
           return 2;
         } else {
           return 3;
         }
       })});
  auto op = PlanBuilder()
                .values({data})
                .singleAggregation({"c1"}, {"approx_set(c0)"})
                .orderBy({"c1"}, false)
                .project({"to_base64(cast(a0 as varbinary))"})
                .planNode();
  auto resultVector = AssertQueryBuilder(op).copyResults(pool_.get());
  auto hllVector = resultVector->childAt(0);
  BaseVector::flattenVector(hllVector);
  ASSERT_EQ(hllVector->size(), 4);
  ASSERT_EQ(hllVector->asFlatVector<StringView>()->valueAt(0), kHll1);
  ASSERT_EQ(hllVector->asFlatVector<StringView>()->valueAt(1), kHll2);
  ASSERT_EQ(hllVector->asFlatVector<StringView>()->valueAt(2), kHll3);
  ASSERT_EQ(hllVector->asFlatVector<StringView>()->valueAt(3), kHll4);

  // Check that Velox merges the HLLs properly and produces the same HLL result
  // as Java.
  auto hll = makeFlatVector<std::string>({kHll1, kHll2, kHll3, kHll4});
  op = PlanBuilder()
           .values({makeRowVector({hll})})
           .project({"cast(from_base64(c0) as hyperloglog) as c0"})
           .singleAggregation({}, {"merge(c0)"})
           .project({"to_base64(cast(a0 as varbinary))"})
           .planNode();
  auto result = readSingleValue(op);
  ASSERT_EQ(
      result.value<TypeKind::VARCHAR>(),
      "AwwAMkEgAAABEAMAAQEAAAABESABAUEAAAMBABAQIgAQAQEAAAATABMAAAAAACAAAFAGAQAUcREQEEABAAIHAAAgACABAAMUAXAhQGADQQIQAzASBDACITAAAAAAMBIEAQEAARBhIjAAIABDAAAQMAADYAMSAGEwACIAAAAAAEEAAhABMQEhAAIQAAABEREAAAAAAYAAABEDYQIABAAUIBEAAAAAAgICQBAAMAAAEAAQADMAAAAAA0AgAAAAAAACBBUBEAEFAQAAEwEAEAAAIFEWIAAAAAMgADAABCIBAAEQASEAQCAAAAECAjBAEEAAIDABAgABEyFFAABFAwArAABTAgACIAAgAADQExMAAAICABACAxAAEAERABAAAQADAhEAAAAREAAQAgIQIRMAEQIQAgARAQAQAABEAEACAgAAAAASAAMAIDABAAASIBBAEAIAMBEyAQEAQAAwKSIxAgIAUAACIQUCEAAAAAAAABEBYCABAAEgASMAAAAhABEQEAAhAAEACBEAAEAgAGMAEAUQEQEBAAAAACICIQAAAHMAICIlASAQAAAAEyAAAhEQAAABEhMCERAAEiFAADAgADEAIhAAIAAhEiAiEgAQECAiAgIBAAEAAAQBBCAwAQAREAAAABYCIABgAgAAAwADEAAAQAEAAwBAABEEABAQAgECAAAAEBEAACEQAAAgJAAAAAAAAQASARIAEAIAADByEAMkAAACIhQDECAEEBAQAZAwIgAEMCAAADMREEAAAjAQABAiIBICBAAAAhIAQCIARQAQMEEwABAQAAAWMAAAIQEAMQAABAAAAAQAABAAAAAQIRFAAAAAADADEAAgAhAAAAEQAAAQIAEAUiARQAAAAAAAIwMBAAIEEQMQAQAAAAIDAgEAQAIAAAABAAIQEAAQAAAAEAEABQBQIEQAIEMAAAOxAQAABBAQAAAAEAABAQAzIBICEAFDAAAAMAAAERABACICAAkAAAAQAgAAAjABAQEAZxAAAgAAAAAAAAMBAhATABEAAyEAEgMQMgIgAQFAMAECAAECEGEhNQAAAAACAAAhABIAACAAQAADAFQBACEAAQAQBwAAQAAEIBIAASITAwAAEAAAACIhIhAiACAFIBESAABQMgMwERAAAwAgBgAQAAABMQAAIAEAAAEkAQYDEQABACEBIAAwEQIBAQAAVQAgAAAVARAgMAAEADUAEAMgIAAAADEQNUIAAAAAECAQAAARERMSAAEAEQABAQIRBUAQAAAxIQBAATAAAAAhAAABAQACAAASABJAABAAACABEBBgMAABFiAFYAAAADARAgADAAASEAEAEgECIAAAEgAAACAgABMQMAAAECACASAQEAACUQAAARUAEQAAIDAgAiAAAAMRAQIAYAEBAQIwUAEQAAAQMBABAAAAADACAAUBABAgAAEAEAAABAABABMABQEAAAAAQQAQAQAAIAECBAIAAAADERABEAAAAAKQIQAAQAEAAVEgBwACEgQgAwAAEAARAAACEAMgEAABEAEBACAAACEgIEIQAAIAIAAAAAAAAwAAESEAACADAjMCAAEgEAAAABAAEBIAAANgAxAAEQIAABNAARAAAiIAEyEwAxACExEQAAACIDAUAAACAjAAABAhAwEAAAIEABEAABYAARAwEgAQICEBEBMAAAAAUAAQAQACAQAEAQMAEBAAAAEAQwIABQBgAwAWAQAQIAAQAAAAAAAgAQMAYAIBEQEQICADAQACAkAgIAUSAAAABjAHMCAIAQAwQAABAgABFyAgADARAEMgAAAAECIAAAAQAQAQIBEAAAQAAAEQEBAQEAAFAAEQEAATUAEDADAgACABAAEAYSECAAESACQGAAAAAAYiAAAwEAAREAAAABAAIQATAABQIBAAAAAAIDAyAAAAAAAAEQEAAQAAAhAgAABBASAwADAAAiAAIAABFCByAAEABgEAAgAiAAMAA0UAAQABAEAAAQAAEQAAMAAAAAAQIBAAAAAQYAEAAQACAAEBQRIAATIEACAGABAAABIQAwIBAAAAAAMhAQAhAAAhACEwACEDAQEAAAAxEgMAAAIQIDBAAREAAQEAAiMAMAIQQAEQAQAQEABCABIEADAAAAMAAxAFcgQBEQAAEgBBARAAAAEAADIAFAAAAAIAAAABEAABAQAAAAAAETAABAABAAAAAwAAEQAABAACAQAwMQAAACABIAAAAiAAAAECJRAAAhBTBAAQEgAAAgIAEBEAAAAhAAADEBAAUAADIQAwABQAEHABAFABABMhAgAAAwAABCEDQAIwABAQASEBBQAiIDAAEAAAAABRMAAVAAJyEyIEBwAQACEAECEAEAAAIBAAACEAAEJQFAAQEBABIAAQAQAAASAAICACABEAAAAwABADEQBDAAAAEBACUDAAAiAgAAAAAwAAAhAiBgEQIgAQAAAiA1IAAAAAARBAABMAAAAAAxERAQMBQCIBEAJAARAQAAEQUQAQAgAAABABAAAQBQEAIwEgIgAhEQFABQAQAEYBAwAAIAAQICIRIAIAMAJBAAEQABIgBBEAAwACEAIAIQIBAQIAABAAAAAAEBEBAREEAAUhAQMwABABADAzAgIQAAEhFAABIFATEAAAZIAhAiEAEBAAAAJAEgEQEAABABEQABAAAQAAMkAAABABAjAABwRgQgEAABBSEiAAAAEEEAICERAhIAExAAIAYAAAIAEDMAAAABEAEAAAAA==");
}

} // namespace
} // namespace facebook::velox::aggregate::test
