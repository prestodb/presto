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
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class ArrayFlattenTest : public SparkFunctionBaseTest {
 protected:
  void testExpression(
      const std::string& expression,
      const std::vector<VectorPtr>& input,
      const VectorPtr& expected) {
    const auto result = evaluate(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }
};

// Flatten integer arrays.
TEST_F(ArrayFlattenTest, intArrays) {
  const auto arrayOfArrays = makeNestedArrayVectorFromJson<int64_t>({
      "[[1, 1], [2, 2], [3, 3]]",
      "[[4, 4]]",
      "[[5, 5], [6, 6]]",
  });

  const auto expected = makeArrayVectorFromJson<int64_t>(
      {"[1, 1, 2, 2, 3, 3]", "[4, 4]", "[5, 5, 6, 6]"});

  testExpression("flatten(c0)", {arrayOfArrays}, expected);
}

// Flatten arrays with null.
TEST_F(ArrayFlattenTest, nullArray) {
  const auto arrayOfArrays = makeNestedArrayVectorFromJson<int64_t>({
      "[[1, 1], null, [3, 3]]",
      "null",
      "[[5, null], [null, 6], [null, null], []]",
  });

  const auto expected = makeArrayVectorFromJson<int64_t>(
      {"null", "null", "[5, null, null, 6, null, null]"});

  testExpression("flatten(c0)", {arrayOfArrays}, expected);
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
