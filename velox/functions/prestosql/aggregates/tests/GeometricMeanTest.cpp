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

#include <cmath>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {

namespace {

class GeometricMeanTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
  }
};

template <typename T>
T geometricMean(
    int32_t start,
    int32_t end,
    int32_t steps,
    std::function<T(int32_t)> valueConverter) {
  double logSum = 0;
  int64_t count = 0;
  for (int32_t i = start; i < end; i += steps) {
    logSum += std::log(valueConverter(i));
    count++;
  }
  return static_cast<T>(std::exp(logSum / count));
}

TEST_F(GeometricMeanTest, globalEmpty) {
  auto data = makeRowVector({
      makeFlatVector(std::vector<int64_t>{}),
  });

  testAggregations({data}, {}, {"geometric_mean(c0)"}, "SELECT NULL");
}

TEST_F(GeometricMeanTest, globalNulls) {
  auto data = makeRowVector({
      makeAllNullFlatVector<int64_t>(100),
      makeFlatVector<int64_t>(
          100, [](auto row) { return row; }, nullEvery(2)),
  });

  // All nulls.
  testAggregations({data}, {}, {"geometric_mean(c0)"}, "SELECT NULL");

  auto expected = makeRowVector({
      makeConstant(geometricMean<double>(1, 100, 2, folly::identity), 1),
  });

  testAggregations({data}, {}, {"geometric_mean(c1)"}, {expected});
}

TEST_F(GeometricMeanTest, globalIntegers) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>(100, [](auto row) { return row / 7; }),
  });

  auto expected = makeRowVector({
      makeFlatVector(std::vector<double>{
          geometricMean<double>(1, 100, 1, [](int32_t i) { return i / 7; }),
      }),
  });

  testAggregations({data}, {}, {"geometric_mean(c0)"}, {expected});
}

TEST_F(GeometricMeanTest, groupByNulls) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(100, [](auto row) { return row / 10; }),
      makeFlatVector<int64_t>(
          100,
          [](auto row) { return row; },
          [](auto row) {
            // All values in group 3 are null.
            if (row / 10 == 3) {
              return true;
            }

            // Every other value is null.
            return row % 2 == 0;
          }),
  });

  auto expected = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
      makeFlatVector<double>(
          10,
          [](auto row) {
            return geometricMean<double>(
                1, 10, 2, [&](int32_t i) { return row * 10 + i; });
          },
          [](auto row) { return row == 3; }),
  });

  testAggregations({data}, {"c0"}, {"geometric_mean(c1)"}, {expected});
}

TEST_F(GeometricMeanTest, groupByIntegers) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(100, [](auto row) { return row / 10; }),
      makeFlatVector<int64_t>(100, [](auto row) { return row; }),
  });

  auto expected = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
      makeFlatVector<double>(
          10,
          [](auto row) {
            return geometricMean<double>(
                0, 10, 1, [&](int32_t i) { return row * 10 + i; });
          }),
  });

  testAggregations({data}, {"c0"}, {"geometric_mean(c1)"}, {expected});
}

TEST_F(GeometricMeanTest, globalDoubles) {
  auto data = makeRowVector({
      makeFlatVector<double>(100, [](auto row) { return row * 0.1 / 7; }),
  });

  auto expected = makeRowVector({
      makeFlatVector(std::vector<double>{geometricMean<double>(
          0, 100, 1, [&](int32_t i) { return i * 0.1 / 7; })}),
  });

  testAggregations({data}, {}, {"geometric_mean(c0)"}, {expected});
}

TEST_F(GeometricMeanTest, groupByDoubles) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(100, [](auto row) { return row / 10; }),
      makeFlatVector<double>(100, [](auto row) { return row * 0.1; }),
  });

  auto expected = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
      makeFlatVector<double>(
          10,
          [](auto row) {
            return geometricMean<double>(
                0, 10, 1, [&](int32_t i) { return row + i * 0.1; });
          }),
  });

  testAggregations({data}, {"c0"}, {"geometric_mean(c1)"}, {expected});
}

TEST_F(GeometricMeanTest, globalReals) {
  auto data = makeRowVector({
      makeFlatVector<float>(100, [](auto row) { return row * 0.1 / 7; }),
  });

  auto expected = makeRowVector({
      makeFlatVector(std::vector<float>{geometricMean<float>(
          0, 100, 1, [&](int32_t i) { return i * 0.1 / 7; })}),
  });

  testAggregations({data}, {}, {"geometric_mean(c0)"}, {expected});
}

TEST_F(GeometricMeanTest, groupByReals) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(100, [](auto row) { return row / 10; }),
      makeFlatVector<float>(100, [](auto row) { return row * 0.1; }),
  });

  auto expected = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
      makeFlatVector<float>(
          10,
          [](auto row) {
            return geometricMean<float>(
                0, 10, 1, [&](int32_t i) { return row + i * 0.1; });
          }),
  });

  testAggregations({data}, {"c0"}, {"geometric_mean(c1)"}, {expected});
}

TEST_F(GeometricMeanTest, groupByMultipleBatches) {
  auto data = {
      makeRowVector({
          makeFlatVector<int32_t>(50, [](auto row) { return row / 10; }),
          makeFlatVector<int64_t>(50, [](auto row) { return row; }),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(50, [](auto row) { return (row + 50) / 10; }),
          makeFlatVector<int64_t>(50, [](auto row) { return row + 50; }),
      })};

  auto expected = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
      makeFlatVector<double>(
          10,
          [](auto row) {
            return geometricMean<double>(
                0, 10, 1, [&](int32_t i) { return row * 10 + i; });
          }),
  });

  testAggregations(data, {"c0"}, {"geometric_mean(c1)"}, {expected});
}

} // namespace
} // namespace facebook::velox::aggregate::test
