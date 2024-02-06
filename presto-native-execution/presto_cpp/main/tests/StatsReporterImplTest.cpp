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

#include "presto_cpp/main/common/StatsReporterImpl.h"
#include <gtest/gtest.h>
#include <cstring>

namespace facebook::presto {

class StatsReporterImplTest : public testing::Test {
  void SetUp() override {}

  void TearDown() override {}
};

/// Tests addStatType and addStats functions.
TEST_F(StatsReporterImplTest, prometheusSerializer) {
  auto reporter =
      std::make_shared<StatsReporterImpl>("test_cluster", "test_worker_pod");

  reporter->registerMetricExportType("key1", facebook::velox::StatType::COUNT);
  reporter->registerMetricExportType("key2", facebook::velox::StatType::AVG);

  EXPECT_EQ(
      facebook::velox::StatType::COUNT,
      reporter->getRegisteredStatType("key1"));
  EXPECT_EQ(
      facebook::velox::StatType::AVG, reporter->getRegisteredStatType("key2"));

  std::vector<size_t> testData = {10, 11, 15};
  for (auto i : testData) {
    reporter->addMetricValue("key1", i);
    reporter->addMetricValue("key2", i + 1000);
  }
  // Uses default value of 1 for second parameter.
  reporter->addMetricValue("key1");
  prometheus::PrometheusSerializer serializer(prometheus::Labels{
      {"cluster", "test_cluster"}, {"worker", "test_worker_pod"}});
  auto prometheusFormat = reporter->getMetrics(serializer);
  const std::string expected[] = {
      "# HELP key2",
      "# TYPE key2 gauge",
      "key2{cluster=\"test_cluster\",worker=\"test_worker_pod\"} 1015",
      "# HELP key1",
      "# TYPE key1 counter",
      "key1{cluster=\"test_cluster\",worker=\"test_worker_pod\"} 37"};

  auto i = 0;
  auto pos = prometheusFormat.find("\n");
  while (pos != std::string::npos) {
    auto line = prometheusFormat.substr(0, pos);
    EXPECT_NE(line.find(expected[i++]), std::string::npos);
    prometheusFormat = prometheusFormat.substr(pos + 1);
    pos = prometheusFormat.find("\n");
  }
};
} // namespace facebook::presto
