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
#include "velox/common/base/tests/GTestUtils.h"

DECLARE_bool(velox_memory_leak_check_enabled);

using namespace facebook::velox;
using namespace facebook::presto;

using facebook::presto::PrestoTaskId;

class PrestoTaskTest : public testing::Test {
  void SetUp() override {
    FLAGS_velox_memory_leak_check_enabled = true;
  }
};

TEST_F(PrestoTaskTest, basicTaskId) {
  const std::string taskIdStr("20201107_130540_00011_wrpkw.1.2.3.4");
  PrestoTaskId id(taskIdStr);
  ASSERT_EQ(id.queryId(), "20201107_130540_00011_wrpkw");
  ASSERT_EQ(id.stageId(), 1);
  ASSERT_EQ(id.stageExecutionId(), 2);
  ASSERT_EQ(id.id(), 3);
  ASSERT_EQ(id.attemptNumber(), 4);
  ASSERT_EQ(id.toString(), taskIdStr);
}

TEST_F(PrestoTaskTest, malformedTaskId) {
  VELOX_ASSERT_THROW(PrestoTaskId(""), "Malformed task ID: ");
  VELOX_ASSERT_THROW(
      PrestoTaskId("20201107_130540_00011_wrpkw."),
      "Malformed task ID: 20201107_130540_00011_wrpkw.");
  VELOX_ASSERT_THROW(PrestoTaskId("q.1.2"), "Malformed task ID: q.1.2");
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
