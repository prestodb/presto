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
#include <optional>
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

namespace {

class WidthBucketArrayTest : public FunctionBaseTest {
 protected:
  void testFailure(
      const double operand,
      const std::vector<std::vector<double>>& bins,
      const std::string& expected_message) {
    auto binsVector = makeArrayVector<double>(bins);
    assertUserInvalidArgument(
        [&]() {
          evaluate<SimpleVector<int64_t>>(
              "width_bucket(c0, c1)",
              makeRowVector(
                  {makeConstant(operand, binsVector->size()), binsVector}));
        },
        expected_message);
  }

  void testFailureForConstant(
      const double operand,
      const std::string& bins,
      const std::string& expected_message) {
    assertUserInvalidArgument(
        [&]() {
          evaluate<SimpleVector<int64_t>>(
              fmt::format("width_bucket(c0, {})", bins),
              makeRowVector({makeConstant(operand, 1)}));
        },
        expected_message);
  }
};

TEST_F(WidthBucketArrayTest, success) {
  auto binsVector = makeArrayVector<double>({{0.0, 2.0, 4.0}, {0.0}});

  auto testWidthBucketArray = [&](const double operand,
                                  const std::vector<int64_t>& expected) {
    auto result = evaluate<SimpleVector<int64_t>>(
        "width_bucket(c0, c1)",
        makeRowVector({
            makeConstant(operand, binsVector->size()),
            binsVector,
        }));

    assertEqualVectors(makeFlatVector<int64_t>(expected), result);
  };

  testWidthBucketArray(3.14, {2, 1});
  testWidthBucketArray(std::numeric_limits<double>::infinity(), {3, 1});
  testWidthBucketArray(-1, {0, 0});
}

TEST_F(WidthBucketArrayTest, failure) {
  testFailure(0, {{}}, "Bins cannot be an empty array");
  testFailure(
      std::numeric_limits<double>::quiet_NaN(), {{0}}, "Operand cannot be NaN");
  testFailure(
      1,
      {{0, std::numeric_limits<double>::infinity()}},
      "Bin value must be finite");
  testFailure(
      1,
      {{0, std::numeric_limits<double>::quiet_NaN()}},
      "Bin values are not sorted in ascending order");
  testFailure(2, {{1, 0}}, "Bin values are not sorted in ascending order");
}

TEST_F(WidthBucketArrayTest, successForConstantArray) {
  auto testWidthBucketArray = [&](const double operand,
                                  const std::string& bins,
                                  const int64_t expected) {
    auto expression = fmt::format("width_bucket(c0, {})", bins);
    auto oneRowResult = evaluate<SimpleVector<int64_t>>(
        expression, makeRowVector({makeConstant(operand, 1)}));
    assertEqualVectors(
        makeFlatVector<int64_t>(std::vector<int64_t>({expected})),
        oneRowResult);

    auto twoRowsResult = evaluate<SimpleVector<int64_t>>(
        expression, makeRowVector({makeConstant(operand, 2)}));
    assertEqualVectors(
        makeFlatVector<int64_t>(std::vector<int64_t>({expected, expected})),
        twoRowsResult);
  };

  testWidthBucketArray(3.14, "ARRAY[0.0, 2.0, 4.0]", 2);
  testWidthBucketArray(
      std::numeric_limits<double>::infinity(), "ARRAY[0.0, 2.0, 4.0]", 3);
  testWidthBucketArray(-1, "ARRAY[0.0, 2.0, 4.0]", 0);
  testWidthBucketArray(3.14, "ARRAY[0.0]", 1);
  testWidthBucketArray(
      std::numeric_limits<double>::infinity(), "ARRAY[0.0]", 1);
  testWidthBucketArray(-1, "ARRAY[0.0]", 0);
}

TEST_F(WidthBucketArrayTest, failureForConstant) {
  // TODO: Add tests for empty bin and bins that contains infinity(), nan()
  //       once corresponding casting and non-constant array literal element is
  //       supported.
  testFailureForConstant(
      std::numeric_limits<double>::quiet_NaN(),
      "ARRAY[0.0]",
      "Operand cannot be NaN");
  testFailureForConstant(
      2, "ARRAY[1.0, 0.0]", "Bin values are not sorted in ascending order");
}

} // namespace
