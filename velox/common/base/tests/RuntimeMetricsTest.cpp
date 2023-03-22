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

#include "velox/common/base/RuntimeMetrics.h"
#include <gtest/gtest.h>

namespace facebook::velox {

class RuntimeMetricsTest : public testing::Test {
 protected:
  static void testMetric(
      const RuntimeMetric& rm1,
      int64_t expectedSum,
      int64_t expectedCount,
      int64_t expectedMin = std::numeric_limits<int64_t>::max(),
      int64_t expectedMax = std::numeric_limits<int64_t>::min()) {
    EXPECT_EQ(expectedSum, rm1.sum);
    EXPECT_EQ(expectedCount, rm1.count);
    EXPECT_EQ(expectedMin, rm1.min);
    EXPECT_EQ(expectedMax, rm1.max);
  }
};

TEST_F(RuntimeMetricsTest, basic) {
  RuntimeMetric rm1;
  testMetric(rm1, 0, 0);

  rm1.addValue(5);
  testMetric(rm1, 5, 1, 5, 5);

  rm1.addValue(11);
  testMetric(rm1, 16, 2, 5, 11);

  rm1.addValue(3);
  testMetric(rm1, 19, 3, 3, 11);

  EXPECT_EQ(
      fmt::format(
          "sum:{}, count:{}, min:{}, max:{}",
          rm1.sum,
          rm1.count,
          rm1.min,
          rm1.max),
      rm1.toString());

  RuntimeMetric rm2;

  rm1.merge(rm2);
  testMetric(rm1, 19, 3, 3, 11);

  rm2.addValue(53);
  rm1.merge(rm2);
  testMetric(rm1, 72, 4, 3, 53);

  rm1.aggregate();
  testMetric(rm1, 72, 1, 72, 72);

  RuntimeMetric rm3;
  rm3.aggregate();
  testMetric(rm3, 0, 0, 0, 0);
};

} // namespace facebook::velox
