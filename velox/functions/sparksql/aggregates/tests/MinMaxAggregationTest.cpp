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
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/sparksql/aggregates/Register.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace facebook::velox::functions::aggregate::sparksql::test {

namespace {

std::string min(const std::string& column) {
  return fmt::format("spark_min({})", column);
}

std::string max(const std::string& column) {
  return fmt::format("spark_max({})", column);
}

class MinMaxAggregationTest
    : public functions::aggregate::test::AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerAggregateFunctions("spark_");
  }

  std::vector<RowVectorPtr> fuzzData(const RowTypePtr& rowType) {
    VectorFuzzer::Options options;
    options.vectorSize = 1'000;
    options.nullRatio = 0.1;
    VectorFuzzer fuzzer(options, pool());
    std::vector<RowVectorPtr> vectors(10);
    for (auto i = 0; i < 10; ++i) {
      vectors[i] = fuzzer.fuzzInputRow(rowType);
    }
    return vectors;
  }

  void doTest(const TypePtr& inputType, bool testWithTableScan = true) {
    auto rowType = ROW({"c0", "c1", "mask"}, {BIGINT(), inputType, BOOLEAN()});
    auto vectors = fuzzData(rowType);
    createDuckDbTable(vectors);

    static const std::string c0 = "c0";
    static const std::string c1 = "c1";
    static const std::string a0 = "a0";

    // Global aggregation.
    testAggregations(
        vectors, {}, {min(c1), max(c1)}, "SELECT min(c1), max(c1) FROM tmp");

    // Group by aggregation.
    testAggregations(
        [&](auto& builder) {
          builder.values(vectors).project({"c0 % 10", "c1"});
        },
        {"p0"},
        {min(c1), max(c1)},
        "SELECT c0 % 10, min(c1), max(c1) FROM tmp GROUP BY 1");

    // Masked aggregations.
    auto minMaskedAgg = min(c1) + " filter (where mask)";
    testAggregations(
        vectors,
        {},
        {minMaskedAgg},
        "SELECT min(c1) filter (where mask) FROM tmp");

    testAggregations(
        [&](auto& builder) {
          builder.values(vectors).project({"c0 % 10", "c1", "mask"});
        },
        {"p0"},
        {minMaskedAgg},
        "SELECT c0 % 10, min(c1) filter (where mask) FROM tmp GROUP BY 1");

    auto maxMaskedAgg = max(c1) + " filter (where mask)";
    testAggregations(
        vectors,
        {},
        {maxMaskedAgg},
        "SELECT max(c1) filter (where mask) FROM tmp");

    testAggregations(
        [&](auto& builder) {
          builder.values(vectors).project({"c0 % 10", "c1", "mask"});
        },
        {"p0"},
        {maxMaskedAgg},
        "SELECT c0 % 10, max(c1) filter (where mask) FROM tmp GROUP BY 1");

    // Encodings: use filter to wrap aggregation inputs in a dictionary.
    testAggregations(
        [&](auto& builder) {
          builder.values(vectors)
              .filter("c0 % 2 = 0")
              .project({"c0 % 11", "c1"});
        },
        {"p0"},
        {min(c1), max(c1)},
        "SELECT c0 % 11, min(c1), max(c1) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1");

    testAggregations(
        [&](auto& builder) { builder.values(vectors).filter("c0 % 2 = 0"); },
        {},
        {min(c1), max(c1)},
        "SELECT min(c1), max(c1) FROM tmp WHERE c0 % 2 = 0");
  }
};

TEST_F(MinMaxAggregationTest, tinyint) {
  doTest(TINYINT());
}

TEST_F(MinMaxAggregationTest, smallint) {
  doTest(SMALLINT());
}

TEST_F(MinMaxAggregationTest, integer) {
  doTest(INTEGER());
}

TEST_F(MinMaxAggregationTest, bigint) {
  doTest(BIGINT());
}

TEST_F(MinMaxAggregationTest, real) {
  doTest(REAL());
}

TEST_F(MinMaxAggregationTest, double) {
  doTest(DOUBLE());
}

TEST_F(MinMaxAggregationTest, varchar) {
  doTest(VARCHAR());
}

TEST_F(MinMaxAggregationTest, boolean) {
  doTest(BOOLEAN());
}

TEST_F(MinMaxAggregationTest, interval) {
  doTest(INTERVAL_DAY_TIME());
}

TEST_F(MinMaxAggregationTest, shortDecimal) {
  doTest(DECIMAL(18, 3), false);
}

TEST_F(MinMaxAggregationTest, longDecimal) {
  doTest(DECIMAL(20, 3), false);
}

TEST_F(MinMaxAggregationTest, timestamp) {
  auto rowType = ROW({"c0", "c1"}, {SMALLINT(), TIMESTAMP()});
  auto vectors = makeVectors(rowType, 1'000, 10);
  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {},
      {min("c1"), max("c1")},
      "SELECT date_trunc('microsecond', min(c1)), "
      "date_trunc('microsecond', max(c1)) FROM tmp");

  testAggregations(
      [&](auto& builder) {
        builder.values(vectors).project({"c0 % 17 as k", "c1"});
      },
      {"k"},
      {min("c1"), max("c1")},
      "SELECT c0 % 17, date_trunc('microsecond', min(c1)), "
      "date_trunc('microsecond', max(c1)) FROM tmp GROUP BY 1");
}

TEST_F(MinMaxAggregationTest, array) {
  auto data = makeRowVector({
      makeNullableArrayVector<int64_t>({
          {1, 2, 3},
          {2, std::nullopt},
          {6, 7, 8},
      }),
  });

  auto expected = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
      }),
      makeArrayVector<int64_t>({
          {6, 7, 8},
      }),
  });

  testAggregations({data}, {}, {min("c0"), max("c0")}, {expected});

  data = makeRowVector({
      makeNullableArrayVector<int64_t>({
          {1, 2, 3},
          {3, 2},
          {6, 7, 8},
      }),
  });
  testAggregations({data}, {}, {min("c0"), max("c0")}, {expected});
}

TEST_F(MinMaxAggregationTest, row) {
  auto data = makeRowVector({
      makeRowVector({
          makeFlatVector<StringView>({
              "a"_sv,
              "b"_sv,
              "c"_sv,
          }),
          makeNullableFlatVector<StringView>({
              std::nullopt,
              "efg"_sv,
              "hij"_sv,
          }),
      }),
  });

  auto expected = makeRowVector({
      makeRowVector(
          {makeFlatVector<StringView>({"a"_sv}),
           makeNullableFlatVector<StringView>({std::nullopt})}),
      makeRowVector(
          {makeFlatVector<StringView>({"c"_sv}),
           makeFlatVector<StringView>({"hij"_sv})}),
  });

  testAggregations({data}, {}, {min("c0"), max("c0")}, {expected});

  data = makeRowVector({
      makeRowVector({
          makeFlatVector<StringView>({
              "a"_sv,
              "b"_sv,
              "c"_sv,
          }),
          makeNullableFlatVector<StringView>({
              "abc"_sv,
              "efg"_sv,
              "hij"_sv,
          }),
      }),
  });
  expected = makeRowVector({
      makeRowVector(
          {makeFlatVector<StringView>({"a"_sv}),
           makeFlatVector<StringView>({"abc"_sv})}),
      makeRowVector(
          {makeFlatVector<StringView>({"c"_sv}),
           makeFlatVector<StringView>({"hij"_sv})}),
  });
  testAggregations({data}, {}, {min("c0"), max("c0")}, {expected});
}

TEST_F(MinMaxAggregationTest, failOnUnorderableType) {
  auto data = makeRowVector({makeMapVectorFromJson<int32_t, double>({
      "{0: 0.05, 2: 2.05}",
      "{4: 4.05}",
      "{6: 6.05, 8: 8.05}",
  })});

  static const std::string kErrorMessage =
      "Aggregate function signature is not supported";
  for (const auto& expr : {min("c0"), max("c0")}) {
    {
      auto builder = PlanBuilder().values({data});
      VELOX_ASSERT_THROW(builder.singleAggregation({}, {expr}), kErrorMessage);
    }

    {
      auto builder = PlanBuilder().values({data});
      VELOX_ASSERT_THROW(
          builder.singleAggregation({"c1"}, {expr}), kErrorMessage);
    }
  }
}

} // namespace
} // namespace facebook::velox::functions::aggregate::sparksql::test
