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

class SparkPartitionIdTest : public SparkFunctionBaseTest {
 protected:
  void setSparkPartitionId(int32_t partitionId) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kSparkPartitionId, std::to_string(partitionId)}});
  }

  void testSparkPartitionId(int32_t partitionId, int32_t vectorSize) {
    setSparkPartitionId(partitionId);
    auto result =
        evaluate("spark_partition_id()", makeRowVector(ROW({}), vectorSize));
    ASSERT_TRUE(result->isConstantEncoding());
    velox::test::assertEqualVectors(
        makeConstant(partitionId, vectorSize), result);
  }
};

TEST_F(SparkPartitionIdTest, basic) {
  testSparkPartitionId(0, 1);
  testSparkPartitionId(100, 1);
  testSparkPartitionId(0, 100);
  testSparkPartitionId(100, 100);
}

TEST_F(SparkPartitionIdTest, error) {
  auto rowVector = makeRowVector(ROW({}), 1);

  queryCtx_->testingOverrideConfigUnsafe({{}});
  VELOX_ASSERT_THROW(
      evaluate("spark_partition_id()", rowVector),
      "Spark partition id is not set");

  setSparkPartitionId(-1);
  VELOX_ASSERT_THROW(
      evaluate("spark_partition_id()", rowVector),
      "Invalid Spark partition id");
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
