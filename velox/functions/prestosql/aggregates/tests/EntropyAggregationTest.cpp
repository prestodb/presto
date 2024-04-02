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
#include <random>
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {

namespace {
// The test class.
class EntropyAggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }

  void testGroupByAgg(
      const std::vector<RowVectorPtr>& data,
      const std::string& veloxAggKey,
      const std::string& duckDBAggKey) {
    auto partialAgg = fmt::format("entropy({0})", veloxAggKey);
    std::string sql = fmt::format(
        "SELECT c0, entropy({0}) FROM tmp GROUP BY 1", duckDBAggKey);
    testAggregations(data, {"c0"}, {partialAgg}, sql);
  }

  void testGlobalAgg(
      const std::vector<RowVectorPtr>& data,
      const std::string& veloxAggKey,
      const std::string& duckDBAggKey) {
    auto partialAgg = fmt::format("entropy({0})", veloxAggKey);
    std::string sql = fmt::format("SELECT entropy({0}) FROM tmp", duckDBAggKey);

    testAggregations(data, {}, {partialAgg}, sql);
  }

  // The input of Velox entropy agg function is value count,
  // but the input of DuckDB is value itself. In order to check the correctness
  // of the result with DuckDB, we need to generate the input data of DuckDB
  // first, and then convert the input data of DuckDB to the input data of Velox
  // through count agg. This method will create an agg plan, and get the agg
  // results.
  RowVectorPtr getEntropyCounts(
      const RowVectorPtr& data,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& projects) {
    PlanBuilder builder(pool());
    builder.values({data});
    builder.partialAggregation(groupingKeys, aggregates).finalAggregation();
    if (!projects.empty()) {
      builder.project(projects);
    }
    return AssertQueryBuilder(builder.planNode()).copyResults(pool());
  }

  // Generate random value in some range, type T needs to be an integer type.
  template <typename T>
  static std::vector<T> generateRandomValues(
      const vector_size_t& size,
      const int32_t& min,
      const int32_t& max) {
    auto seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine gen(seed);
    std::uniform_int_distribution<T> dis(min, max);
    std::vector<T> numbers(size);
    for (auto i = 0; i < size; i++) {
      numbers[i] = dis(gen);
    }
    return numbers;
  }
};

TEST_F(EntropyAggregationTest, constCountGlobal) {
  // Velox data:  [5, 5, 5, ..., 5]
  // DuckDB data: [0, 0, 0, 0, 0, 1, 1, 1, 1, 1, ..., 19, 19, 19, 19, 19]
  auto everyValueCount = 5;
  auto counts = makeRowVector({makeConstant(everyValueCount, 10)});

  auto data = makeRowVector({makeFlatVector<int64_t>(
      100, [&everyValueCount](auto row) { return row / everyValueCount; })});

  createDuckDbTable({data});

  testGlobalAgg({counts, counts}, "c0", "c0");
}

TEST_F(EntropyAggregationTest, constCountGroupBy) {
  // Velox data:
  // c0: [0, 1, 2, 0, 1, 2, 0, 1, 2, 0]
  // c1: [3, 3, 3, 3, 3, 3, 3, 3, 3, 3]
  // DuckDB data:
  // c0: [0, 0, 0, 1, 1, 1, ..., 2, 2, 2, 0, 0, 0]
  // c1: [0, 0, 0, 1, 1, 1, ..., 8, 8, 8, 9, 9, 9]
  auto everyValueCount = 3;
  auto counts = makeRowVector(
      {makeFlatVector<int32_t>(10, [](auto row) { return row % 3; }),
       makeConstant(everyValueCount, 10)});

  auto data = makeRowVector(
      {makeFlatVector<int32_t>(
           30,
           [&everyValueCount](auto row) {
             return (row / everyValueCount) % 3;
           }),
       makeFlatVector<int32_t>(30, [&everyValueCount](auto row) {
         return row / everyValueCount;
       })});

  createDuckDbTable({data});
  testGroupByAgg({counts}, "c1", "c1");
}

TEST_F(EntropyAggregationTest, integerNoNulls) {
  std::vector<int32_t> randomValues = generateRandomValues<int32_t>(100, 1, 5);
  auto data = makeRowVector(
      {makeFlatVector<int32_t>(100, [](auto row) { return row / 7; }),
       makeFlatVector<int32_t>(randomValues)});
  createDuckDbTable({data});

  auto groupByCounts =
      getEntropyCounts(data, {"c0", "c1"}, {"count(c1)"}, {"c0", "a0"});
  testGroupByAgg({groupByCounts}, "a0", "c1");

  auto globalCounts = getEntropyCounts(data, {"c1"}, {"count(c1)"}, {});
  testGlobalAgg({globalCounts}, "a0", "c1");
}

TEST_F(EntropyAggregationTest, integerSomeNulls) {
  std::vector<int32_t> randomValues = generateRandomValues<int32_t>(100, 1, 5);
  auto data = makeRowVector(
      {makeFlatVector<int32_t>(
           100, [](auto row) { return row / 7; }, nullEvery(17)),
       makeFlatVector<int32_t>(randomValues)});
  createDuckDbTable({data});

  auto groupByCounts =
      getEntropyCounts(data, {"c0", "c1"}, {"count(c1)"}, {"c0", "a0"});
  testGroupByAgg({groupByCounts}, "a0", "c1");

  auto globalCounts = getEntropyCounts(data, {"c1"}, {"count(c1)"}, {});
  testGlobalAgg({globalCounts}, "a0", "c1");
}

TEST_F(EntropyAggregationTest, mayHaveNulls) {
  // c0: [1, 1, 1, 2, 2, 2, 3, 3, 3]
  // c1: [1, 1, null, 1, null, 2, null, 5, 6]
  auto data = makeRowVector(
      {makeFlatVector<int32_t>({1, 1, 1, 2, 2, 2, 3, 3, 3}),
       makeNullableFlatVector<int32_t>(
           {1, 1, std::nullopt, 1, std::nullopt, 2, std::nullopt, 5, 6})});
  auto expectedResult = makeRowVector(
      {makeFlatVector<double>({1.0, 0.9182958340544898, 0.9940302114769566})});
  testAggregations({data}, {"c0"}, {"entropy(c1)"}, {"a0"}, {expectedResult});
}

TEST_F(EntropyAggregationTest, allNulls) {
  auto data = makeRowVector({makeAllNullFlatVector<int64_t>(10)});
  auto expectedResult = makeRowVector({makeConstant(0.0, 1)});
  testAggregations({data}, {}, {"entropy(c0)"}, {expectedResult});

  data = makeRowVector(
      {makeFlatVector<int32_t>({1, 1, 1, 2, 2, 2}),
       makeNullableFlatVector<int32_t>(
           {1, 1, std::nullopt, std::nullopt, std::nullopt, std::nullopt})});
  expectedResult = makeRowVector({makeFlatVector<double>({1.0, 0.0})});
  testAggregations({data}, {"c0"}, {"entropy(c1)"}, {"a0"}, {expectedResult});
}

TEST_F(EntropyAggregationTest, largeConstant) {
  auto data =
      makeRowVector({makeConstant(std::numeric_limits<int64_t>::max(), 10)});

  auto expectedResult = makeRowVector(
      {makeFlatVector<double>(std::vector<double>{3.3219280948873693})});
  testAggregations({data}, {}, {"entropy(c0)"}, {expectedResult});

  data = makeRowVector(
      {makeFlatVector<int32_t>({1, 1, 1, 2, 2, 2}),
       makeConstant(std::numeric_limits<int64_t>::max(), 10)});
  expectedResult = makeRowVector(
      {makeFlatVector<double>({1.5849625007211632, 1.5849625007211632})});
  testAggregations({data}, {"c0"}, {"entropy(c1)"}, {"a0"}, {expectedResult});
}

} // namespace
} // namespace facebook::velox::aggregate::test
