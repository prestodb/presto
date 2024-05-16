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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {

class CentralMomentsAggregationTest
    : public virtual AggregationTestBase,
      public testing::WithParamInterface<std::string> {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
  }

  void testGroupBy(
      const std::string& aggName,
      const RowVectorPtr& data,
      int32_t groupSize) {
    auto partialAgg = fmt::format("{0}(c1)", aggName);

    double coefficient{1.0};
    if (aggName == "skewness") {
      // The skewness function in DuckDB and Presto have different calculation
      // formula for the final result. In order to get the same calculation
      // result, we can add a coefficient, which is only used to verify the
      // correctness of the result in UT.
      coefficient = duckDBCoefficient(groupSize);
    }
    auto sql = fmt::format(
        "SELECT c0, {0} * {1}(c1) FROM tmp GROUP BY 1", coefficient, aggName);

    testAggregations({data}, {"c0"}, {partialAgg}, sql);
  }

  void testGlobalAgg(
      const std::string& aggName,
      const RowVectorPtr& data,
      int32_t groupSize) {
    VELOX_CHECK(groupSize >= 3);
    auto partialAgg = fmt::format("{0}(c1)", aggName);

    double coefficient{1.0};
    if (aggName == "skewness") {
      coefficient = duckDBCoefficient(groupSize);
    }
    auto sql =
        fmt::format("SELECT {0} * {1}(c1) FROM tmp", coefficient, aggName);

    testAggregations({data}, {}, {partialAgg}, sql);
  }

  void testSingleColGlobalAgg(
      const std::string& aggName,
      const RowVectorPtr& data,
      const std::vector<RowVectorPtr>& expectedResult) {
    auto partialAgg = fmt::format("{0}(c0)", aggName);
    testAggregations({data}, {}, {partialAgg}, expectedResult);
  }

  template <typename T>
  static std::vector<T> generateNormalDistributionRandom(vector_size_t size) {
    // Generate normally distributed random numbers for testing
    auto seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::vector<T> numbers(size);
    std::default_random_engine gen(seed);
    std::normal_distribution<T> dis(50, 10);
    for (auto i = 0; i < size; i++) {
      numbers[i] = dis(gen);
    }
    return numbers;
  }

  static double duckDBCoefficient(int32_t n) {
    return (n - 2) / std::sqrt(n * (n - 1));
  }
};

TEST_P(CentralMomentsAggregationTest, doubleNoNulls) {
  vector_size_t size = 1'000;
  int32_t groupNum = 10;
  int32_t groupSize = size / groupNum;
  auto randomNums = generateNormalDistributionRandom<double>(size);

  auto data = makeRowVector(
      {makeFlatVector<int32_t>(
           size, [&groupNum](auto row) { return row % groupNum; }),
       makeFlatVector<double>(
           size, [&randomNums](auto row) { return randomNums[row]; })});

  createDuckDbTable({data});

  auto aggName = GetParam();
  testGlobalAgg(aggName, data, size);
  testGroupBy(aggName, data, groupSize);
}

TEST_P(CentralMomentsAggregationTest, doubleSomeNulls) {
  vector_size_t size = 1'000;
  int32_t groupNum = 10;
  int32_t groupSize = size / groupNum;
  int32_t totalNullValue = 200;
  int32_t nullValueInGroup = totalNullValue / groupNum;
  auto randomNums = generateNormalDistributionRandom<double>(size);

  // null needs to be evenly distributed among each group, otherwise the
  // coefficient required by each group cannot be calculated
  auto data = makeRowVector(
      {makeFlatVector<int32_t>(
           size, [&groupNum](auto row) { return row % groupNum; }),
       makeFlatVector<double>(
           size,
           [&randomNums](auto row) { return randomNums[row]; },
           [&size, &groupNum, &totalNullValue](auto row) {
             return (row / groupNum) % (size / totalNullValue) == 0;
           })});

  createDuckDbTable({data});

  auto aggName = GetParam();
  testGlobalAgg(aggName, data, size - totalNullValue);
  testGroupBy(aggName, data, groupSize - nullValueInGroup);
}

TEST_P(CentralMomentsAggregationTest, floatNoNulls) {
  vector_size_t size = 1'000;
  int32_t groupNum = 10;
  int32_t groupSize = size / groupNum;
  auto randomNums = generateNormalDistributionRandom<float>(size);

  auto data = makeRowVector(
      {makeFlatVector<int32_t>(
           size, [&groupNum](auto row) { return row % groupNum; }),
       makeFlatVector<float>(
           size, [&randomNums](auto row) { return randomNums[row]; })});

  createDuckDbTable({data});

  auto aggName = GetParam();
  testGlobalAgg(aggName, data, size);
  testGroupBy(aggName, data, groupSize);
}

TEST_P(CentralMomentsAggregationTest, floatSomeNulls) {
  vector_size_t size = 1'000;
  int32_t groupNum = 10;
  int32_t groupSize = size / groupNum;
  int32_t totalNullValue = 200;
  int32_t nullValueInGroup = totalNullValue / groupNum;
  auto randomNums = generateNormalDistributionRandom<float>(size);

  // null needs to be evenly distributed among each group, otherwise the
  // coefficient required by each group cannot be calculated
  auto data = makeRowVector(
      {makeFlatVector<int32_t>(
           size, [&groupNum](auto row) { return row % groupNum; }),
       makeFlatVector<float>(
           size,
           [&randomNums](auto row) { return randomNums[row]; },
           [&size, &groupNum, &totalNullValue](auto row) {
             return (row / groupNum) % (size / totalNullValue) == 0;
           })});

  createDuckDbTable({data});

  auto aggName = GetParam();
  testGlobalAgg(aggName, data, size - totalNullValue);
  testGroupBy(aggName, data, groupSize - nullValueInGroup);
}

TEST_P(CentralMomentsAggregationTest, integerNoNulls) {
  vector_size_t size = 1'000;
  int32_t groupNum = 10;
  int32_t groupSize = size / groupNum;
  auto floatRandomNums = generateNormalDistributionRandom<float>(size);
  std::vector<int32_t> randomNums(size);
  for (auto i = 0; i < size; i++) {
    randomNums[i] = (int32_t)std::round(floatRandomNums[i]);
  }

  auto data = makeRowVector(
      {makeFlatVector<int32_t>(
           size, [&groupNum](auto row) { return row % groupNum; }),
       makeFlatVector<int32_t>(
           size, [&randomNums](auto row) { return randomNums[row]; })});

  createDuckDbTable({data});

  auto aggName = GetParam();
  testGlobalAgg(aggName, data, size);
  testGroupBy(aggName, data, groupSize);
}

TEST_P(CentralMomentsAggregationTest, integerSomeNulls) {
  vector_size_t size = 1'000;
  int32_t groupNum = 10;
  int32_t groupSize = size / groupNum;
  int32_t totalNullValue = 200;
  int32_t nullValueInGroup = totalNullValue / groupNum;
  auto floatRandomNums = generateNormalDistributionRandom<float>(size);
  std::vector<int32_t> randomNums(size);
  for (auto i = 0; i < size; i++) {
    randomNums[i] = (int32_t)std::round(floatRandomNums[i]);
  }

  // null needs to be evenly distributed among each group, otherwise the
  // coefficient required by each group cannot be calculated
  auto data = makeRowVector(
      {makeFlatVector<int32_t>(
           size, [&groupNum](auto row) { return row % groupNum; }),
       makeFlatVector<int32_t>(
           size,
           [&randomNums](auto row) { return randomNums[row]; },
           [&size, &groupNum, &totalNullValue](auto row) {
             return (row / groupNum) % (size / totalNullValue) == 0;
           })});

  createDuckDbTable({data});

  auto aggName = GetParam();
  testGlobalAgg(aggName, data, size - totalNullValue);
  testGroupBy(aggName, data, groupSize - nullValueInGroup);
}

TEST_P(CentralMomentsAggregationTest, notEnoughCount) {
  auto data =
      makeRowVector({makeFlatVector<double>({1.0f, 0.0f, 0.0f, 3.0f, 0.0f})});
  for (auto i = 0; i < 5; i++) {
    if (data->childAt(0)->asFlatVector<double>()->valueAt(i) == 0.0f) {
      data->childAt(0)->setNull(i, true);
    }
  }
  createDuckDbTable({data});
  auto aggName = GetParam();
  auto expectedResult = makeRowVector({makeAllNullFlatVector<double>(1)});
  testSingleColGlobalAgg(aggName, data, {expectedResult});
}

TEST_P(CentralMomentsAggregationTest, constantInput) {
  constexpr double kNan = std::numeric_limits<double>::quiet_NaN();

  vector_size_t size = 10;
  auto data = makeRowVector(
      {makeFlatVector<int32_t>(size, [](auto row) { return 5; })});
  createDuckDbTable({data});
  auto aggName = GetParam();
  std::vector<double> nanVec = {kNan};
  auto expectedResult = makeRowVector({makeFlatVector<double>(nanVec)});
  testSingleColGlobalAgg(aggName, data, {expectedResult});
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    CentralMomentsAggregationTest,
    CentralMomentsAggregationTest,
    testing::Values("kurtosis", "skewness"));
} // namespace facebook::velox::aggregate::test
