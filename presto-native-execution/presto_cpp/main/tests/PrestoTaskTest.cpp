/*
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
#include "presto_cpp/main/PrestoTask.h"
#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::presto;

using facebook::presto::PrestoTaskId;

class PrestoTaskTest : public testing::Test {};

TEST_F(PrestoTaskTest, basicTaskId) {
  PrestoTaskId id("20201107_130540_00011_wrpkw.1.2.3");

  EXPECT_EQ(id.queryId(), "20201107_130540_00011_wrpkw");
  EXPECT_EQ(id.stageId(), 1);
  EXPECT_EQ(id.stageExecutionId(), 2);
  EXPECT_EQ(id.id(), 3);
}

TEST_F(PrestoTaskTest, malformedTaskId) {
  ASSERT_THROW(PrestoTaskId(""), std::invalid_argument);
  ASSERT_THROW(
      PrestoTaskId("20201107_130540_00011_wrpkw."), std::invalid_argument);
  ASSERT_THROW(PrestoTaskId("q.1.2"), std::invalid_argument);
}

TEST_F(PrestoTaskTest, runtimeMetricConversion) {
  RuntimeMetric veloxMetric;
  veloxMetric.unit = RuntimeCounter::Unit::kBytes;
  veloxMetric.sum = 101;
  veloxMetric.count = 17;
  veloxMetric.min = 62;
  veloxMetric.max = 79;

  const std::string metricName{"my_name"};
  const auto prestoMetric = toRuntimeMetric(metricName, veloxMetric);
  EXPECT_EQ(metricName, prestoMetric.name);
  EXPECT_EQ(protocol::RuntimeUnit::BYTE, prestoMetric.unit);
  EXPECT_EQ(veloxMetric.sum, prestoMetric.sum);
  EXPECT_EQ(veloxMetric.count, prestoMetric.count);
  EXPECT_EQ(veloxMetric.max, prestoMetric.max);
  EXPECT_EQ(veloxMetric.min, prestoMetric.min);
}
