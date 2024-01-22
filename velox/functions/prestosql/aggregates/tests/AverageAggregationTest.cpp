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
#include "velox/exec/tests/SimpleAggregateFunctionsRegistration.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/DecimalAggregate.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {

namespace {

class AverageAggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();

    registerSimpleAverageAggregate("simple_avg");
  }

  core::PlanNodePtr createAvgAggPlanNode(
      std::vector<VectorPtr> input,
      bool isSingle) {
    PlanBuilder builder;
    builder.values({makeRowVector(input)});
    if (isSingle) {
      builder.singleAggregation({}, {"avg(c0)"});
    } else {
      builder.partialAggregation({}, {"avg(c0)"});
    }
    return builder.planNode();
  }

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), SMALLINT(), INTEGER(), BIGINT(), REAL(), DOUBLE()})};
};

TEST_F(AverageAggregationTest, avgConst) {
  auto testFunction = [this](const std::string& functionName) {
    // Have two row vectors a lest as it triggers different code paths.
    auto vectors = {
        makeRowVector({
            makeFlatVector<int64_t>(
                10, [](vector_size_t row) { return row / 3; }),
            makeConstant(5, 10),
            makeConstant(6.0, 10),
        }),
        makeRowVector({
            makeFlatVector<int64_t>(
                10, [](vector_size_t row) { return row / 3; }),
            makeConstant(5, 10),
            makeConstant(6.0, 10),
        }),
    };

    createDuckDbTable(vectors);

    testAggregations(
        vectors,
        {},
        {fmt::format("{}(c1)", functionName),
         fmt::format("{}(c2)", functionName)},
        "SELECT avg(c1), avg(c2) FROM tmp");

    testAggregations(
        vectors,
        {"c0"},
        {fmt::format("{}(c1)", functionName),
         fmt::format("{}(c2)", functionName)},
        "SELECT c0, avg(c1), avg(c2) FROM tmp group by c0");

    testAggregations(
        vectors,
        {},
        {fmt::format("{}(c0)", functionName)},
        "SELECT avg(c0) FROM tmp");

    testAggregations(
        [&](auto& builder) {
          builder.values(vectors).project({"c0 % 2 AS c0_mod_2", "c0"});
        },
        {"c0_mod_2"},
        {fmt::format("{}(c0)", functionName)},
        "SELECT c0 % 2, avg(c0) FROM tmp group by 1");
  };

  testFunction("avg");
  testFunction("simple_avg");
}

TEST_F(AverageAggregationTest, avgConstNull) {
  auto testFunction = [this](const std::string& functionName) {
    // Have at least two row vectors as it triggers different code paths.
    auto vectors = {
        makeRowVector({
            makeNullableFlatVector<int64_t>({0, 1, 2, 0, 1, 2, 0, 1, 2, 0}),
            makeNullConstant(TypeKind::BIGINT, 10),
            makeNullConstant(TypeKind::DOUBLE, 10),
        }),
        makeRowVector({
            makeNullableFlatVector<int64_t>({0, 1, 2, 0, 1, 2, 0, 1, 2, 0}),
            makeNullConstant(TypeKind::BIGINT, 10),
            makeNullConstant(TypeKind::DOUBLE, 10),
        }),
    };

    createDuckDbTable(vectors);

    testAggregations(
        vectors,
        {},
        {fmt::format("{}(c1)", functionName),
         fmt::format("{}(c2)", functionName)},
        "SELECT avg(c1), avg(c2) FROM tmp");

    testAggregations(
        vectors,
        {"c0"},
        {fmt::format("{}(c1)", functionName),
         fmt::format("{}(c2)", functionName)},
        "SELECT c0, avg(c1), avg(c2) FROM tmp group by c0");

    testAggregations(
        vectors,
        {},
        {fmt::format("{}(c0)", functionName)},
        "SELECT avg(c0) FROM tmp");
  };

  testFunction("avg");
  testFunction("simple_avg");
}

TEST_F(AverageAggregationTest, avgNulls) {
  auto testFunction = [this](const std::string& functionName) {
    // Have two row vectors a lest as it triggers different code paths.
    auto vectors = {
        makeRowVector({
            makeNullableFlatVector<int64_t>({0, std::nullopt, 2, 0, 1}),
            makeNullableFlatVector<int64_t>({0, 1, std::nullopt, 3, 4}),
            makeNullableFlatVector<double>({0.1, 1.2, 2.3, std::nullopt, 4.4}),
        }),
        makeRowVector({
            makeNullableFlatVector<int64_t>({0, std::nullopt, 2, 0, 1}),
            makeNullableFlatVector<int64_t>({0, 1, std::nullopt, 3, 4}),
            makeNullableFlatVector<double>({0.1, 1.2, 2.3, std::nullopt, 4.4}),
        }),
    };

    createDuckDbTable(vectors);

    testAggregations(
        vectors,
        {},
        {fmt::format("{}(c1)", functionName),
         fmt::format("{}(c2)", functionName)},
        "SELECT avg(c1), avg(c2) FROM tmp");

    testAggregations(
        vectors,
        {"c0"},
        {fmt::format("{}(c1)", functionName),
         fmt::format("{}(c2)", functionName)},
        "SELECT c0, avg(c1), avg(c2) FROM tmp group by c0");
  };

  testFunction("avg");
  testFunction("simple_avg");
}

TEST_F(AverageAggregationTest, avg) {
  auto testFunction = [this](const std::string& functionName) {
    auto vectors = makeVectors(rowType_, 10, 100);
    createDuckDbTable(vectors);

    // global aggregation
    std::vector<std::string> aggregates = {
        fmt::format("{}(c1)", functionName),
        fmt::format("{}(c2)", functionName),
        fmt::format("{}(c4)", functionName),
        fmt::format("{}(c5)", functionName)};
    testAggregations(
        vectors,
        {},
        aggregates,
        "SELECT avg(c1), avg(c2), avg(c4), avg(c5) FROM tmp");
    testAggregationsWithCompanion(
        vectors,
        [](auto& /*builder*/) {},
        {},
        aggregates,
        {{rowType_->childAt(1)},
         {rowType_->childAt(2)},
         {rowType_->childAt(4)},
         {rowType_->childAt(5)}},
        {},
        "SELECT avg(c1), avg(c2), avg(c4), avg(c5) FROM tmp");

    // global aggregation; no input
    aggregates = {fmt::format("{}(c0)", functionName)};
    testAggregations(
        [&](auto& builder) { builder.values(vectors).filter("c0 % 2 = 5"); },
        {},
        aggregates,
        "SELECT null");
    testAggregationsWithCompanion(
        vectors,
        [&](auto& builder) { builder.filter("c0 % 2 = 5"); },
        {},
        aggregates,
        {{rowType_->childAt(0)}},
        {},
        "SELECT null");

    // global aggregation over filter
    aggregates = {fmt::format("{}(c1)", functionName)};
    testAggregations(
        [&](auto& builder) { builder.values(vectors).filter("c0 % 5 = 3"); },
        {},
        aggregates,
        "SELECT avg(c1) FROM tmp WHERE c0 % 5 = 3");
    testAggregationsWithCompanion(
        vectors,
        [&](auto& builder) { builder.filter("c0 % 5 = 3"); },
        {},
        aggregates,
        {{rowType_->childAt(1)}},
        {},
        "SELECT avg(c1) FROM tmp WHERE c0 % 5 = 3");

    // group by
    aggregates = {
        fmt::format("{}(c1)", functionName),
        fmt::format("{}(c2)", functionName),
        fmt::format("{}(c3)", functionName),
        fmt::format("{}(c4)", functionName),
        fmt::format("{}(c5)", functionName)};
    testAggregations(
        [&](auto& builder) {
          builder.values(vectors).project(
              {"c0 % 10 AS c0_mod_10", "c1", "c2", "c3", "c4", "c5"});
        },
        {"c0_mod_10"},
        aggregates,
        "SELECT c0 % 10, avg(c1), avg(c2), avg(c3::DOUBLE), "
        "avg(c4), avg(c5) FROM tmp GROUP BY 1");
    testAggregationsWithCompanion(
        vectors,
        [&](auto& builder) {
          builder.project(
              {"c0 % 10 AS c0_mod_10", "c1", "c2", "c3", "c4", "c5", "k0"});
        },
        {"c0_mod_10"},
        aggregates,
        {{rowType_->childAt(1)},
         {rowType_->childAt(2)},
         {rowType_->childAt(3)},
         {rowType_->childAt(4)},
         {rowType_->childAt(5)}},
        {},
        "SELECT c0 % 10, avg(c1), avg(c2), avg(c3::DOUBLE), "
        "avg(c4), avg(c5) FROM tmp GROUP BY 1");

    // group by; no input
    aggregates = {fmt::format("{}(c1)", functionName)};
    testAggregations(
        [&](auto& builder) {
          builder.values(vectors)
              .project({"c0 % 10 AS c0_mod_10", "c1"})
              .filter("c0_mod_10 > 10");
        },
        {"c0_mod_10"},
        aggregates,
        "");
    testAggregationsWithCompanion(
        vectors,
        [&](auto& builder) {
          builder.project({"c0 % 10 AS c0_mod_10", "c1", "k0"})
              .filter("c0_mod_10 > 10");
        },
        {"c0_mod_10"},
        aggregates,
        {{rowType_->childAt(1)}},
        {},
        "");

    // group by over filter
    aggregates = {fmt::format("{}(c1)", functionName)};
    testAggregations(
        [&](auto& builder) {
          builder.values(vectors)
              .filter("c2 % 5 = 3")
              .project({"c0 % 10 AS c0_mod_10", "c1"});
        },
        {"c0_mod_10"},
        aggregates,
        "SELECT c0 % 10, avg(c1) FROM tmp WHERE c2 % 5 = 3 GROUP BY 1");
    testAggregationsWithCompanion(
        vectors,
        [&](auto& builder) {
          builder.filter("c2 % 5 = 3")
              .project({"c0 % 10 AS c0_mod_10", "c1", "k0"});
        },
        {"c0_mod_10"},
        aggregates,
        {{rowType_->childAt(1)}},
        {},
        "SELECT c0 % 10, avg(c1) FROM tmp WHERE c2 % 5 = 3 GROUP BY 1");
  };

  testFunction("avg");
  testFunction("simple_avg");
}

TEST_F(AverageAggregationTest, overflow) {
  auto makeSingleAggregationPlan = [this](
                                       const std::string& functionName,
                                       bool singleGroup,
                                       const VectorPtr& vector) {
    return PlanBuilder()
        .values({makeRowVector({makeFlatVector<bool>({true, true}), vector})})
        .singleAggregation(
            singleGroup ? std::vector<std::string>{}
                        : std::vector<std::string>{"c0"},
            {fmt::format("{}(c1)", functionName)})
        .planNode();
  };

  auto makePlan = [this](
                      const std::string& functionName,
                      bool singleGroup,
                      const VectorPtr& vector) {
    return PlanBuilder()
        .values({makeRowVector({makeFlatVector<bool>({true, true}), vector})})
        .partialAggregation(
            singleGroup ? std::vector<std::string>{}
                        : std::vector<std::string>{"c0"},
            {fmt::format("{}(c1)", functionName)})
        .intermediateAggregation()
        .finalAggregation()
        .planNode();
  };

  auto testFunction = [&](const std::string& functionName, bool singleGroup) {
    auto vector = makeRowVector(
        {makeFlatVector<double>({100.0, 200.0}),
         makeFlatVector<int64_t>({8490071280492378624, 8490071280492378624})});
    auto constantVector = BaseVector::wrapInConstant(100, 0, vector);
    auto expected = makeRowVector(
        {makeNullableFlatVector<double>({std::nullopt, std::nullopt})});

    auto plan = makeSingleAggregationPlan(functionName, singleGroup, vector);
    VELOX_ASSERT_THROW(assertQuery(plan, expected), "integer overflow");

    plan = makeSingleAggregationPlan(functionName, singleGroup, constantVector);
    VELOX_ASSERT_THROW(assertQuery(plan, expected), "integer overflow");

    plan = makePlan(functionName, singleGroup, vector);
    VELOX_ASSERT_THROW(assertQuery(plan, expected), "integer overflow");

    plan = makePlan(functionName, singleGroup, constantVector);
    VELOX_ASSERT_THROW(assertQuery(plan, expected), "integer overflow");
  };
  testFunction("avg_merge", true);
  testFunction("avg_merge", false);
  testFunction("avg_merge_extract_real", true);
  testFunction("avg_merge_extract_real", false);
  testFunction("avg_merge_extract_double", true);
  testFunction("avg_merge_extract_double", false);
  testFunction("simple_avg_merge", true);
  testFunction("simple_avg_merge", false);
  testFunction("simple_avg_merge_extract_real", true);
  testFunction("simple_avg_merge_extract_real", false);
  testFunction("simple_avg_merge_extract_double", true);
  testFunction("simple_avg_merge_extract_double", false);
}

TEST_F(AverageAggregationTest, partialResults) {
  auto testFunction = [this](const std::string& functionName) {
    auto data = makeRowVector(
        {makeFlatVector<int64_t>(100, [](auto row) { return row; })});

    auto plan =
        PlanBuilder()
            .values({data})
            .partialAggregation({}, {fmt::format("{}(c0)", functionName)})
            .planNode();

    assertQuery(plan, "SELECT row(4950, 100)");
  };

  testFunction("avg");
  testFunction("simple_avg");
}

TEST_F(AverageAggregationTest, decimalAccumulator) {
  functions::aggregate::LongDecimalWithOverflowState accumulator;
  accumulator.sum = -1000;
  accumulator.count = 10;
  accumulator.overflow = -1;

  char* buffer = new char[accumulator.serializedSize()];
  StringView serialized(buffer, accumulator.serializedSize());
  accumulator.serialize(serialized);
  functions::aggregate::LongDecimalWithOverflowState mergedAccumulator;
  mergedAccumulator.mergeWith(serialized);

  ASSERT_EQ(mergedAccumulator.sum, accumulator.sum);
  ASSERT_EQ(mergedAccumulator.count, accumulator.count);
  ASSERT_EQ(mergedAccumulator.overflow, accumulator.overflow);

  // Merging again to same accumulator.
  memset(buffer, 0, accumulator.serializedSize());
  mergedAccumulator.serialize(serialized);
  mergedAccumulator.mergeWith(serialized);
  ASSERT_EQ(mergedAccumulator.sum, accumulator.sum * 2);
  ASSERT_EQ(mergedAccumulator.count, accumulator.count * 2);
  ASSERT_EQ(mergedAccumulator.overflow, accumulator.overflow * 2);
  delete[] buffer;
}

TEST_F(AverageAggregationTest, avgDecimal) {
  // Disable incremental aggregation tests because DecimalAggregate doesn't set
  // StringView::prefix when extracting accumulators, leaving the prefix field
  // undefined that fails the test.
  AggregationTestBase::disableTestIncremental();

  // Skip testing with TableScan because decimal is not supported in writers.
  auto shortDecimal = makeNullableFlatVector<int64_t>(
      {1'000, 2'000, 3'000, 4'000, 5'000, std::nullopt}, DECIMAL(10, 1));
  // Short decimal aggregation
  testAggregations(
      {makeRowVector({shortDecimal})},
      {},
      {"avg(c0)"},
      {},
      {makeRowVector(
          {makeNullableFlatVector<int64_t>({3'000}, DECIMAL(10, 1))})},
      /*config*/ {},
      /*testWithTableScan*/ false);

  // Long decimal aggregation
  testAggregations(
      {makeRowVector({makeNullableFlatVector<int128_t>(
          {HugeInt::build(10, 100),
           HugeInt::build(10, 200),
           HugeInt::build(10, 300),
           HugeInt::build(10, 400),
           HugeInt::build(10, 500),
           std::nullopt},
          DECIMAL(23, 4))})},
      {},
      {"avg(c0)"},
      {},
      {makeRowVector({makeFlatVector(
          std::vector<int128_t>{HugeInt::build(10, 300)}, DECIMAL(23, 4))})},
      /*config*/ {},
      /*testWithTableScan*/ false);
  // Round-up average.
  testAggregations(
      {makeRowVector(
          {makeFlatVector<int64_t>({100, 400, 510}, DECIMAL(3, 2))})},
      {},
      {"avg(c0)"},
      {},
      {makeRowVector(
          {makeFlatVector(std::vector<int64_t>{337}, DECIMAL(3, 2))})},
      /*config*/ {},
      /*testWithTableScan*/ false);

  // The total sum overflows the max int128_t limit.
  std::vector<int128_t> rawVector;
  for (int i = 0; i < 10; ++i) {
    rawVector.push_back(DecimalUtil::kLongDecimalMax);
  }
  testAggregations(
      {makeRowVector({makeFlatVector<int128_t>(rawVector, DECIMAL(38, 0))})},
      {},
      {"avg(c0)"},
      {},
      {makeRowVector({makeFlatVector(
          std::vector<int128_t>{DecimalUtil::kLongDecimalMax},
          DECIMAL(38, 0))})},
      /*config*/ {},
      /*testWithTableScan*/ false);
  // The total sum underflows the min int128_t limit.
  rawVector.clear();
  auto underFlowTestResult = makeFlatVector(
      std::vector<int128_t>{DecimalUtil::kLongDecimalMin}, DECIMAL(38, 0));
  for (int i = 0; i < 10; ++i) {
    rawVector.push_back(DecimalUtil::kLongDecimalMin);
  }
  testAggregations(
      {makeRowVector({makeFlatVector<int128_t>(rawVector, DECIMAL(38, 0))})},
      {},
      {"avg(c0)"},
      {},
      {makeRowVector({underFlowTestResult})},
      /*config*/ {},
      /*testWithTableScan*/ false);

  // Add more rows to show that average result is still accurate.
  for (int i = 0; i < 10; ++i) {
    rawVector.push_back(DecimalUtil::kLongDecimalMin);
  }
  AssertQueryBuilder assertQueryBuilder(createAvgAggPlanNode(
      {makeFlatVector<int128_t>(rawVector, DECIMAL(38, 0))}, true));
  auto result = assertQueryBuilder.copyResults(pool());

  auto actualResult = result->childAt(0)->asFlatVector<int128_t>();
  ASSERT_EQ(actualResult->valueAt(0), underFlowTestResult->valueAt(0));

  // Test constant vector.
  testAggregations(
      {makeRowVector({makeConstant<int64_t>(100, 10, DECIMAL(3, 2))})},
      {},
      {"avg(c0)"},
      {},
      {makeRowVector(
          {makeFlatVector(std::vector<int64_t>{100}, DECIMAL(3, 2))})},
      /*config*/ {},
      /*testWithTableScan*/ false);

  auto newSize = shortDecimal->size() * 2;
  auto indices = makeIndices(newSize, [&](int row) { return row / 2; });
  auto dictVector =
      VectorTestBase::wrapInDictionary(indices, newSize, shortDecimal);

  testAggregations(
      {makeRowVector({dictVector})},
      {},
      {"avg(c0)"},
      {},
      {makeRowVector(
          {makeFlatVector(std::vector<int64_t>{3'000}, DECIMAL(10, 1))})},
      /*config*/ {},
      /*testWithTableScan*/ false);

  // Decimal average aggregation with multiple groups.
  auto inputRows = {
      makeRowVector(
          {makeNullableFlatVector<int32_t>({1, 1}),
           makeFlatVector<int64_t>({37220, 53450}, DECIMAL(5, 2))}),
      makeRowVector(
          {makeNullableFlatVector<int32_t>({2, 2}),
           makeFlatVector<int64_t>({10410, 9250}, DECIMAL(5, 2))}),
      makeRowVector(
          {makeNullableFlatVector<int32_t>({3, 3}),
           makeFlatVector<int64_t>({-12783, 0}, DECIMAL(5, 2))}),
      makeRowVector(
          {makeNullableFlatVector<int32_t>({1, 2}),
           makeFlatVector<int64_t>({23178, 41093}, DECIMAL(5, 2))}),
      makeRowVector(
          {makeNullableFlatVector<int32_t>({2, 3}),
           makeFlatVector<int64_t>({-10023, 5290}, DECIMAL(5, 2))}),
  };

  auto expectedResult = {
      makeRowVector(
          {makeNullableFlatVector<int32_t>({1}),
           makeFlatVector(std::vector<int64_t>{37949}, DECIMAL(5, 2))}),
      makeRowVector(
          {makeNullableFlatVector<int32_t>({2}),
           makeFlatVector(std::vector<int64_t>{12683}, DECIMAL(5, 2))}),
      makeRowVector(
          {makeNullableFlatVector<int32_t>({3}),
           makeFlatVector(std::vector<int64_t>{-2498}, DECIMAL(5, 2))})};

  testAggregations(
      inputRows,
      {"c0"},
      {"avg(c1)"},
      expectedResult,
      /*config*/ {},
      /*testWithTableScan*/ false);

  AggregationTestBase::enableTestIncremental();
}

TEST_F(AverageAggregationTest, avgDecimalWithMultipleRowVectors) {
  AggregationTestBase::disableTestIncremental();

  auto inputRows = {
      makeRowVector({makeFlatVector<int64_t>({100, 200}, DECIMAL(5, 2))}),
      makeRowVector({makeFlatVector<int64_t>({300, 400}, DECIMAL(5, 2))}),
      makeRowVector({makeFlatVector<int64_t>({500, 600}, DECIMAL(5, 2))}),
  };

  auto expectedResult = {makeRowVector(
      {makeFlatVector(std::vector<int64_t>{350}, DECIMAL(5, 2))})};

  testAggregations(
      inputRows,
      {},
      {"avg(c0)"},
      expectedResult,
      /*config*/ {},
      /*testWithTableScan*/ false);

  AggregationTestBase::enableTestIncremental();
}

TEST_F(AverageAggregationTest, constantVectorOverflow) {
  auto rows = makeRowVector({makeConstant<int32_t>(1073741824, 100)});
  auto plan = PlanBuilder()
                  .values({rows})
                  .singleAggregation({}, {"avg(c0)"})
                  .planNode();
  assertQuery(plan, "SELECT 1073741824");
}

TEST_F(AverageAggregationTest, companionFunctionsWithNonFlatAndLazyInputs) {
  auto testFunction = [this](const std::string& functionName) {
    auto indices = makeIndices({0, 1, 2, 3, 4});
    VectorPtr row = makeRowVector(
        {BaseVector::wrapInDictionary(
             nullptr,
             indices,
             5,
             makeFlatVector<double>({1.0, 2.0, 3.0, 4.0, 5.0})),
         makeFlatVector<int64_t>({1, 1, 1, 1, 1})});
    // rowInDict is a Dictionary(Row(Dictionary(Flat), Flat)) vector.
    auto rowInDict = BaseVector::wrapInDictionary(nullptr, indices, 5, row);

    auto sumVector = std::make_shared<LazyVector>(
        pool(),
        DOUBLE(),
        5,
        std::make_unique<velox::test::SimpleVectorLoader>([&](auto /*rows*/) {
          return makeFlatVector<double>({1.0, 2.0, 3.0, 4.0, 5.0});
        }));
    // row2 is a Row(Lazy(Flat), Constant(Flat)) vector.
    VectorPtr row2 = makeRowVector({
        sumVector,
        BaseVector::wrapInDictionary(
            nullptr,
            indices,
            5,
            BaseVector::wrapInConstant(5, 0, makeFlatVector<int64_t>({1, 2}))),
    });
    auto key = makeFlatVector<bool>({true, false, true, false, true});
    auto input = makeRowVector({key, rowInDict, row2});

    // Test non-global aggregations.
    auto expected = makeRowVector(
        {makeFlatVector<bool>({false, true}),
         makeFlatVector<double>({3.0, 3.0}),
         makeFlatVector<double>({3.0, 3.0})});
    testAggregations(
        {input},
        {"c0"},
        {fmt::format("{}_merge_extract_double(c1)", functionName),
         fmt::format("{}_merge_extract_double(c2)", functionName)},
        {expected});
    testAggregations(
        {input},
        {"c0"},
        {fmt::format("{}_merge(c1)", functionName),
         fmt::format("{}_merge(c2)", functionName)},
        {"c0",
         fmt::format("{}_extract_double(a0)", functionName),
         fmt::format("{}_extract_double(a1)", functionName)},
        {expected});

    // Test global aggregations.
    expected = makeRowVector(
        {makeFlatVector<double>(std::vector<double>{3.0}),
         makeFlatVector<double>(std::vector<double>{3.0})});
    testAggregations(
        {input},
        {},
        {fmt::format("{}_merge_extract_double(c1)", functionName),
         fmt::format("{}_merge_extract_double(c2)", functionName)},
        {expected});
    testAggregations(
        {input},
        {},
        {fmt::format("{}_merge(c1)", functionName),
         fmt::format("{}_merge(c2)", functionName)},
        {fmt::format("{}_extract_double(a0)", functionName),
         fmt::format("{}_extract_double(a1)", functionName)},
        {expected});
  };

  testFunction("avg");
  testFunction("simple_avg");
}

} // namespace
} // namespace facebook::velox::aggregate::test
