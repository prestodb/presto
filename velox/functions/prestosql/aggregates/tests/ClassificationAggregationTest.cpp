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
#include "velox/common/testutil/OptionalEmpty.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {
namespace {

class ClassificationAggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
  }
};

TEST_F(ClassificationAggregationTest, basic) {
  auto runTest = [&](const std::string& expression,
                     RowVectorPtr input,
                     RowVectorPtr expected) {
    testAggregations({input}, {}, {expression}, {expected});
  };

  /// Test without any nulls.
  auto input = makeRowVector({
      makeNullableFlatVector<bool>(
          {true, false, true, false, false, false, false, false, true, false}),
      makeNullableFlatVector<double>(
          {0.1, 0.2, 0.3, 0.3, 0.3, 0.3, 0.7, 1.0, 0.5, 0.5}),
  });

  /// Fallout test.
  auto expected = makeRowVector({
      makeArrayVector<double>({{1.0, 1.0, 3.0 / 7}}),
  });
  runTest("classification_fall_out(5, c0, c1)", input, expected);

  /// Precision test.
  expected = makeRowVector({
      makeArrayVector<double>({{0.3, 2.0 / 9, 0.25}}),
  });
  runTest("classification_precision(5, c0, c1)", input, expected);

  /// Recall test.
  expected = makeRowVector({
      makeArrayVector<double>({{1.0, 2.0 / 3, 1.0 / 3}}),
  });
  runTest("classification_recall(5, c0, c1)", input, expected);

  /// Miss rate test.
  expected = makeRowVector({
      makeArrayVector<double>({{0, 1.0 / 3, 2.0 / 3}}),
  });
  runTest("classification_miss_rate(5, c0, c1)", input, expected);

  /// Thresholds test.
  expected = makeRowVector({
      makeArrayVector<double>({{0, 0.2, 0.4}}),
  });
  runTest("classification_thresholds(5, c0, c1)", input, expected);

  /// Test with some nulls.
  input = makeRowVector({
      makeNullableFlatVector<bool>(
          {std::nullopt,
           false,
           true,
           false,
           false,
           false,
           false,
           false,
           std::nullopt,
           false}),
      makeNullableFlatVector<double>(
          {0.1, 0.2, 0.3, 0.3, 0.3, 0.3, 0.7, 1.0, std::nullopt, std::nullopt}),
  });

  /// Fallout test.
  expected = makeRowVector({makeArrayVector<double>({{1.0, 1.0}})});
  runTest("classification_fall_out(5, c0, c1)", input, expected);

  /// Precision test.
  expected = makeRowVector({makeArrayVector<double>({{1.0 / 7, 1.0 / 7}})});
  runTest("classification_precision(5, c0, c1)", input, expected);

  /// Recall test.
  expected = makeRowVector({makeArrayVector<double>({{1, 1}})});
  runTest("classification_recall(5, c0, c1)", input, expected);

  /// Miss rate test.
  expected = makeRowVector({makeArrayVector<double>({{0, 0}})});
  runTest("classification_miss_rate(5, c0, c1)", input, expected);

  /// Thresholds test.
  expected = makeRowVector({makeArrayVector<double>({{0, 0.2}})});
  runTest("classification_thresholds(5, c0, c1)", input, expected);

  /// Test with all nulls.
  input = makeRowVector({
      makeNullableFlatVector<bool>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<double>({std::nullopt, std::nullopt}),
  });

  expected = makeRowVector({makeNullableArrayVector<double>(
      std::vector<std::optional<std::vector<std::optional<double>>>>{
          common::testutil::optionalEmpty})});
  runTest("classification_fall_out(5, c0, c1)", input, expected);
  runTest("classification_precision(5, c0, c1)", input, expected);
  runTest("classification_recall(5, c0, c1)", input, expected);
  runTest("classification_miss_rate(5, c0, c1)", input, expected);
  runTest("classification_thresholds(5, c0, c1)", input, expected);

  /// Test invalid bucket count test
  input = makeRowVector({
      makeNullableFlatVector<bool>({true}),
      makeNullableFlatVector<double>({1.0}),
  });

  constexpr std::array<const char*, 5> functions = {
      "classification_fall_out",
      "classification_precision",
      "classification_recall",
      "classification_miss_rate",
      "classification_thresholds"};

  /// Test invalid bucket count.
  constexpr std::array<int, 2> invalidBuckets = {0, 1};
  for (const auto bucket : invalidBuckets) {
    for (const auto function : functions) {
      VELOX_ASSERT_THROW(
          runTest(
              fmt::format("{}({}, {}, {})", function, bucket, "c0", "c1"),
              input,
              expected),
          "Bucket count must be at least 2.0");
    }
  }

  /// Test invalid threshold.
  for (const auto function : functions) {
    VELOX_ASSERT_THROW(
        runTest(
            fmt::format("{}(5, {}, {}, {})", function, "c0", "c1", -0.1),
            input,
            expected),
        "Weight must be non-negative.");
  }

  /// Test invalid predictions. Note, a prediction of > 1
  /// will never actually be hit because convert the pred = std::min(pred,
  /// 0.99999999999)
  for (const auto function : functions) {
    VELOX_ASSERT_THROW(
        runTest(
            fmt::format("{}({}, {}, {})", function, 5, "c0", -0.1),
            input,
            expected),
        "Prediction value must be between 0 and 1");
  }
}

TEST_F(ClassificationAggregationTest, weights) {
  auto runTest = [&](const std::string& expression,
                     RowVectorPtr input,
                     RowVectorPtr expected) {
    testAggregations({input}, {}, {expression}, {expected});
  };

  /// This test detects memory access bugs in Accumulator::extractValues, where
  /// iteration stops due to the number of used buckets rather than weight sum.
  /// This discrepancy occurs because the weight sum is inaccurate, caused by a
  /// double precision issue when summing in different orders.
  auto input = makeRowVector({
      makeNullableFlatVector<bool>(
          {false, false, true, false, true, true, false, false}),
      makeNullableFlatVector<double>({0.1, 0.0, 0.3, 0.0, 0.0, 0.1, 0.3, 0.8}),
      makeNullableFlatVector<double>({0.0, 0.5, 0.4, 0.0, 0.6, 0.3, 0.0, 0.2}),
  });

  /// Fallout test.
  auto expected = makeRowVector({makeArrayVector<double>({{1.0, 2. / 7}})});
  runTest("classification_fall_out(5, c0, c1, c2)", input, expected);

  /// Precision test.
  expected = makeRowVector({makeArrayVector<double>({{0.65, 2. / 3}})});
  runTest("classification_precision(5, c0, c1, c2)", input, expected);

  /// Recall test.
  expected = makeRowVector({makeArrayVector<double>({{1.0, 4. / 13}})});
  runTest("classification_recall(5, c0, c1, c2)", input, expected);

  /// Miss rate test.
  expected = makeRowVector({makeArrayVector<double>({{0.0, 9. / 13}})});
  runTest("classification_miss_rate(5, c0, c1, c2)", input, expected);

  /// Thresholds test.
  expected = makeRowVector({makeArrayVector<double>({{0.0, 0.2}})});
  runTest("classification_thresholds(5, c0, c1, c2)", input, expected);
}

TEST_F(ClassificationAggregationTest, groupBy) {
  auto input = makeRowVector({
      makeNullableFlatVector<int32_t>({0, 0, 1, 1, 1, 2, 2, 2, 2, 3, 3}),
      makeNullableFlatVector<bool>(
          {true,
           false,
           true,
           false,
           false,
           false,
           false,
           false,
           true,
           true,
           false}),
      makeNullableFlatVector<double>(
          {0.1, 0.2, 0.3, 0.3, 0.3, 0.3, 0.7, 1.0, 1.0, 0.5, 0.5}),
  });

  auto runTest = [this](
                     const std::string& expression,
                     RowVectorPtr input,
                     RowVectorPtr expected) {
    testAggregations({input}, {"c0"}, {expression}, {expected});
  };
  auto keys = makeFlatVector<int32_t>({0, 1, 2, 3});
  runTest(
      "classification_fall_out(5, c1, c2)",
      input,
      makeRowVector({
          keys,
          makeArrayVector<double>(
              {{{1}, {1, 1}, {1, 1, 2.0 / 3, 2.0 / 3, 1.0 / 3}, {1, 1, 1}}}),
      }));
  runTest(
      "classification_precision(5, c1, c2)",
      input,
      makeRowVector({
          keys,
          makeArrayVector<double>(
              {{{0.5},
                {1.0 / 3, 1.0 / 3},
                {0.25, 0.25, 1.0 / 3, 1.0 / 3, 0.5},
                {0.5, 0.5, 0.5}}}),
      }));
  runTest(
      "classification_recall(5, c1, c2)",
      input,
      makeRowVector({
          keys,
          makeArrayVector<double>({{{1}, {1, 1}, {1, 1, 1, 1, 1}, {1, 1, 1}}}),
      }));
  runTest(
      "classification_miss_rate(5, c1, c2)",
      input,
      makeRowVector({
          keys,
          makeArrayVector<double>({{{0}, {0, 0}, {0, 0, 0, 0, 0}, {0, 0, 0}}}),
      }));
  runTest(
      "classification_thresholds(5, c1, c2)",
      input,
      makeRowVector({
          keys,
          makeArrayVector<double>(
              {{{0}, {0, 0.2}, {0, 0.2, 0.4, 0.6, 0.8}, {0, 0.2, 0.4}}}),
      }));
}

} // namespace
} // namespace facebook::velox::aggregate::test
