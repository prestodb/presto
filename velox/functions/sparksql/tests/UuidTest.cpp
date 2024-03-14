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
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class UuidTest : public SparkFunctionBaseTest {
 protected:
  std::optional<std::string> uuidOne(int64_t seed, int32_t partitionIndex) {
    setSparkPartitionId(partitionIndex);
    return evaluateOnce<std::string>(
        fmt::format("uuid({})", seed), makeRowVector(ROW({}), 1));
  }

  VectorPtr uuidMany(int64_t seed, int32_t partitionIndex, int32_t batchSize) {
    setSparkPartitionId(partitionIndex);
    auto exprSet = compileExpression(fmt::format("uuid({})", seed), ROW({}));
    return evaluate(*exprSet, makeRowVector(ROW({}), batchSize));
  }
};

TEST_F(UuidTest, basic) {
  // With same partitionIndex, same seed always produces same result.
  EXPECT_EQ(uuidOne(100, 1), uuidOne(100, 1));
  EXPECT_EQ(uuidOne(2, 1234), uuidOne(2, 1234));

  EXPECT_NE(uuidOne(9, 1), uuidOne(100, 1));
  EXPECT_NE(uuidOne(9, 1), uuidOne(9, 2));
  EXPECT_NE(uuidOne(100, 1), uuidOne(99, 20));

  // Expected result comes from Spark.
  EXPECT_EQ(uuidOne(0, 0), std::string("8c7f0aac-97c4-4a2f-b716-a675d821ccc0"));

  velox::test::assertEqualVectors(
      uuidMany(123, 1233, 100), uuidMany(123, 1233, 100));
  velox::test::assertEqualVectors(
      uuidMany(321, 1233, 33), uuidMany(321, 1233, 33));
}

TEST_F(UuidTest, nonConstantSeed) {
  setSparkPartitionId(0);

  VELOX_ASSERT_THROW(
      evaluateOnce<std::string>("uuid(c0)", std::optional<int64_t>(123)),
      "seed argument must be constant");
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
