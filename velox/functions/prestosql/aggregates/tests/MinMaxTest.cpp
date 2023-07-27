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
#include "velox/functions/lib/aggregates/tests/AggregationTestBase.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace {

std::string min(const std::string& column) {
  return fmt::format("min({})", column);
}

std::string max(const std::string& column) {
  return fmt::format("max({})", column);
}

class MinMaxTest : public functions::aggregate::test::AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }

  std::vector<RowVectorPtr> fuzzData(const RowTypePtr& rowType) {
    VectorFuzzer::Options options;
    options.vectorSize = 1'000;
    VectorFuzzer fuzzer(options, pool());
    std::vector<RowVectorPtr> vectors(10);
    for (auto i = 0; i < 10; ++i) {
      vectors[i] = fuzzer.fuzzInputRow(rowType);
    }
    return vectors;
  }

  template <typename TAgg>
  void doTest(TAgg agg, const TypePtr& inputType) {
    auto rowType = ROW({"c0", "c1"}, {BIGINT(), inputType});
    auto vectors = fuzzData(rowType);
    createDuckDbTable(vectors);

    static const std::string c0 = "c0";
    static const std::string c1 = "c1";
    static const std::string a0 = "a0";

    // Global aggregation.
    testAggregations(
        vectors, {}, {agg(c1)}, fmt::format("SELECT {} FROM tmp", agg(c1)));

    // Group by aggregation.
    testAggregations(
        [&](auto& builder) {
          builder.values(vectors).project({"c0 % 10", "c1"});
        },
        {"p0"},
        {agg(c1)},
        fmt::format("SELECT c0 % 10, {} FROM tmp GROUP BY 1", agg(c1)));

    // Encodings: use filter to wrap aggregation inputs in a dictionary.
    testAggregations(
        [&](auto& builder) {
          builder.values(vectors)
              .filter("c0 % 2 = 0")
              .project({"c0 % 11", "c1"});
        },
        {"p0"},
        {agg(c1)},
        fmt::format(
            "SELECT c0 % 11, {} FROM tmp WHERE c0 % 2 = 0 GROUP BY 1",
            agg(c1)));

    testAggregations(
        [&](auto& builder) { builder.values(vectors).filter("c0 % 2 = 0"); },
        {},
        {agg(c1)},
        fmt::format("SELECT {} FROM tmp WHERE c0 % 2 = 0", agg(c1)));
  }
};

TEST_F(MinMaxTest, maxTinyint) {
  doTest(max, TINYINT());
}

TEST_F(MinMaxTest, maxSmallint) {
  doTest(max, SMALLINT());
}

TEST_F(MinMaxTest, maxInteger) {
  doTest(max, INTEGER());
}

TEST_F(MinMaxTest, maxBigint) {
  doTest(max, BIGINT());
}

TEST_F(MinMaxTest, maxReal) {
  doTest(max, REAL());
}

TEST_F(MinMaxTest, maxDouble) {
  doTest(max, DOUBLE());
}

TEST_F(MinMaxTest, maxVarchar) {
  doTest(max, VARCHAR());
}

TEST_F(MinMaxTest, maxBoolean) {
  doTest(max, BOOLEAN());
}

TEST_F(MinMaxTest, maxInterval) {
  doTest(max, INTERVAL_DAY_TIME());
}

TEST_F(MinMaxTest, minTinyint) {
  doTest(min, TINYINT());
}

TEST_F(MinMaxTest, minSmallint) {
  doTest(min, SMALLINT());
}

TEST_F(MinMaxTest, minInteger) {
  doTest(min, INTEGER());
}

TEST_F(MinMaxTest, minBigint) {
  doTest(min, BIGINT());
}

TEST_F(MinMaxTest, minReal) {
  doTest(min, REAL());
}

TEST_F(MinMaxTest, minDouble) {
  doTest(min, DOUBLE());
}

TEST_F(MinMaxTest, minInterval) {
  doTest(min, INTERVAL_DAY_TIME());
}

TEST_F(MinMaxTest, minVarchar) {
  doTest(min, VARCHAR());
}

TEST_F(MinMaxTest, minBoolean) {
  doTest(min, BOOLEAN());
}

TEST_F(MinMaxTest, constVarchar) {
  // Create two batches of the source data for the aggregation:
  // Column c0 with 1K of "apple" and 1K of "banana".
  // Column c1 with 1K of nulls and 1K of nulls.
  auto constVectors = {
      makeRowVector(
          {makeConstant("apple", 1'000),
           makeNullConstant(TypeKind::VARCHAR, 1'000)}),
      makeRowVector({
          makeConstant("banana", 1'000),
          makeNullConstant(TypeKind::VARCHAR, 1'000),
      })};

  testAggregations(
      {constVectors},
      {},
      {"min(c0)", "max(c0)", "min(c1)", "max(c1)"},
      "SELECT 'apple', 'banana', null, null");
}

TEST_F(MinMaxTest, minMaxTimestamp) {
  auto rowType = ROW({"c0", "c1"}, {SMALLINT(), TIMESTAMP()});
  auto vectors = makeVectors(rowType, 1'000, 10);
  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {},
      {"min(c1)", "max(c1)"},
      "SELECT date_trunc('millisecond', min(c1)), "
      "date_trunc('millisecond', max(c1)) FROM tmp");

  testAggregations(
      [&](auto& builder) {
        builder.values(vectors).project({"c0 % 17 as k", "c1"});
      },
      {"k"},
      {"min(c1)", "max(c1)"},
      "SELECT c0 % 17, date_trunc('millisecond', min(c1)), "
      "date_trunc('millisecond', max(c1)) FROM tmp GROUP BY 1");
}

TEST_F(MinMaxTest, largeValuesDate) {
  auto vectors = {makeRowVector(
      {makeConstant(60577, 100, DATE()), makeConstant(-57604, 100, DATE())})};
  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {},
      {"min(c0)", "max(c0)", "min(c1)", "max(c1)"},
      "SELECT min(c0), max(c0), min(c1), max(c1) FROM tmp");
}

TEST_F(MinMaxTest, minMaxDate) {
  auto rowType = ROW({"c0", "c1"}, {SMALLINT(), DATE()});
  auto vectors = makeVectors(rowType, 1'000, 10);
  createDuckDbTable(vectors);

  testAggregations(
      vectors, {}, {"min(c1)", "max(c1)"}, "SELECT min(c1), max(c1) FROM tmp");

  testAggregations(
      [&](auto& builder) {
        builder.values(vectors).project({"c0 % 17 as k", "c1"});
      },
      {"k"},
      {"min(c1)", "max(c1)"},
      "SELECT c0 % 17, min(c1), max(c1) FROM tmp GROUP BY 1");
}

TEST_F(MinMaxTest, initialValue) {
  // Ensures that no groups are default initialized (to 0) in
  // aggregate::SimpleNumericAggregate.
  auto row = makeRowVector({
      makeFlatVector<int8_t>({1, 1, 1, 1}),
      makeFlatVector<int8_t>({-1, -1, -1, -1}),
      makeFlatVector<double>({1, 2, 3, 4}),
      makeFlatVector<double>({-1, -2, -3, -4}),
  });
  testAggregations(
      {row},
      {},
      {"min(c0)", "max(c1)", "min(c2)", "max(c3)"},
      "SELECT 1, -1, 1, -1");
}

TEST_F(MinMaxTest, maxShortDecimal) {
  doTest(max, DECIMAL(18, 3));
}

TEST_F(MinMaxTest, minShortDecimal) {
  doTest(min, DECIMAL(3, 1));
}

TEST_F(MinMaxTest, maxLongDecimal) {
  doTest(max, DECIMAL(20, 3));
}

TEST_F(MinMaxTest, minLongDecimal) {
  doTest(min, DECIMAL(38, 19));
}

class MinMaxNTest : public functions::aggregate::test::AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }

  template <typename T>
  void testNumericGlobal() {
    auto data = makeRowVector({
        makeFlatVector<T>({1, 10, 2, 9, 3, 8, 4, 7, 6, 5}),
    });

    // DuckDB doesn't support min(x, n) or max(x, n) functions.

    auto expected = makeRowVector({
        makeArrayVector<T>({
            {1, 2},
        }),
        makeArrayVector<T>({
            {1, 2, 3, 4, 5},
        }),
        makeArrayVector<T>({
            {10, 9, 8},
        }),
        makeArrayVector<T>({
            {10, 9, 8, 7, 6, 5, 4},
        }),
    });

    testAggregations(
        {data},
        {},
        {"min(c0, 2)", "min(c0, 5)", "max(c0, 3)", "max(c0, 7)"},
        {expected});

    // Add some nulls. Expect these to be ignored.
    data = makeRowVector({
        makeNullableFlatVector<T>(
            {1,
             std::nullopt,
             10,
             2,
             9,
             std::nullopt,
             3,
             8,
             4,
             7,
             6,
             5,
             std::nullopt}),
    });

    testAggregations(
        {data},
        {},
        {"min(c0, 2)", "min(c0, 5)", "max(c0, 3)", "max(c0, 7)"},
        {expected});

    // Test all null input.
    data = makeRowVector({
        makeAllNullFlatVector<T>(100),
    });

    expected = makeRowVector({
        makeAllNullArrayVector(1, data->childAt(0)->type()),
        makeAllNullArrayVector(1, data->childAt(0)->type()),
        makeAllNullArrayVector(1, data->childAt(0)->type()),
        makeAllNullArrayVector(1, data->childAt(0)->type()),
    });

    testAggregations(
        {data},
        {},
        {"min(c0, 2)", "min(c0, 5)", "max(c0, 3)", "max(c0, 7)"},
        {expected});
  }

  template <typename T>
  void testNumericGroupBy() {
    auto data = makeRowVector({
        makeFlatVector<int16_t>({1, 2, 1, 1, 2, 2, 1, 2}),
        makeFlatVector<T>({1, 2, 4, 3, 6, 5, 7, 8}),
    });

    auto expected = makeRowVector({
        makeFlatVector<int16_t>({1, 2}),
        makeArrayVector<T>({
            {1, 3},
            {2, 5},
        }),
        makeArrayVector<T>({
            {1, 3, 4, 7},
            {2, 5, 6, 8},
        }),
        makeArrayVector<T>({
            {7, 4, 3},
            {8, 6, 5},
        }),
        makeArrayVector<T>({
            {7, 4, 3, 1},
            {8, 6, 5, 2},
        }),
    });

    testAggregations(
        {data},
        {"c0"},
        {"min(c1, 2)", "min(c1, 5)", "max(c1, 3)", "max(c1, 7)"},
        {expected});

    // Add some nulls. Expect these to be ignored.
    data = makeRowVector({
        makeFlatVector<int16_t>({1, 2, 1, 1, 1, 2, 2, 2, 1, 2}),
        makeNullableFlatVector<T>(
            {1, 2, std::nullopt, 4, 3, 6, std::nullopt, 5, 7, 8}),
    });

    testAggregations(
        {data},
        {"c0"},
        {"min(c1, 2)", "min(c1, 5)", "max(c1, 3)", "max(c1, 7)"},
        {expected});

    // Test all null input.
    data = makeRowVector({
        makeFlatVector<int16_t>({1, 2, 1, 1, 1, 2, 2, 2, 1, 2}),
        makeNullableFlatVector<T>(
            {std::nullopt,
             2,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             6,
             std::nullopt,
             5,
             std::nullopt,
             8}),
    });

    expected = makeRowVector({
        makeFlatVector<int16_t>({1, 2}),
        makeNullableArrayVector<T>({
            std::nullopt,
            {{{2, 5}}},
        }),
        makeNullableArrayVector<T>({
            std::nullopt,
            {{{2, 5, 6, 8}}},
        }),
        makeNullableArrayVector<T>({
            std::nullopt,
            {{{8, 6, 5}}},
        }),
        makeNullableArrayVector<T>({
            std::nullopt,
            {{{8, 6, 5, 2}}},
        }),
    });

    testAggregations(
        {data},
        {"c0"},
        {"min(c1, 2)", "min(c1, 5)", "max(c1, 3)", "max(c1, 7)"},
        {expected});
  }
};

TEST_F(MinMaxNTest, tinyint) {
  testNumericGlobal<int8_t>();
  testNumericGroupBy<int8_t>();
}

TEST_F(MinMaxNTest, smallint) {
  testNumericGlobal<int16_t>();
  testNumericGroupBy<int16_t>();
}

TEST_F(MinMaxNTest, integer) {
  testNumericGlobal<int32_t>();
  testNumericGroupBy<int32_t>();
}

TEST_F(MinMaxNTest, bigint) {
  testNumericGlobal<int64_t>();
  testNumericGroupBy<int64_t>();
}

TEST_F(MinMaxNTest, real) {
  testNumericGlobal<float>();
  testNumericGroupBy<float>();
}

TEST_F(MinMaxNTest, double) {
  testNumericGlobal<double>();
  testNumericGroupBy<double>();
}

} // namespace
