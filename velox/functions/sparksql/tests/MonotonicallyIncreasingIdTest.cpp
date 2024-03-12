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

class MonotonicallyIncreasingIdTest : public SparkFunctionBaseTest {
 protected:
  void testMonotonicallyIncreasingId(
      int32_t partitionId,
      int32_t vectorSize,
      const std::vector<int64_t>& expected) {
    setSparkPartitionId(partitionId);
    auto result = evaluate(
        "monotonically_increasing_id()", makeRowVector(ROW({}), vectorSize));
    ASSERT_FALSE(result->isConstantEncoding());
    velox::test::assertEqualVectors(
        {makeFlatVector<int64_t>(expected)}, result);
  }
};

TEST_F(MonotonicallyIncreasingIdTest, basic) {
  testMonotonicallyIncreasingId(0, 1, {0});
  testMonotonicallyIncreasingId(2, 2, {17179869184, 17179869185});
  testMonotonicallyIncreasingId(5, 3, {42949672960, 42949672961, 42949672962});
  testMonotonicallyIncreasingId(
      100, 4, {858993459200, 858993459201, 858993459202, 858993459203});
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
