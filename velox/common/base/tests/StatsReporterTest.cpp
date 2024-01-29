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

#include "velox/common/base/StatsReporter.h"
#include <folly/Singleton.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <cstdint>
#include <unordered_map>

namespace facebook::velox {

class StatsReporterTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

class TestReporter : public BaseStatsReporter {
 public:
  mutable std::unordered_map<std::string, size_t> counterMap;
  mutable std::unordered_map<std::string, StatType> statTypeMap;
  mutable std::unordered_map<std::string, std::vector<int32_t>>
      histogramPercentilesMap;

  void registerMetricExportType(const char* key, StatType statType)
      const override {
    statTypeMap[key] = statType;
  }

  void registerMetricExportType(folly::StringPiece key, StatType statType)
      const override {
    statTypeMap[key.str()] = statType;
  }

  void registerHistogramMetricExportType(
      const char* key,
      int64_t /* bucketWidth */,
      int64_t /* min */,
      int64_t /* max */,
      const std::vector<int32_t>& pcts) const override {
    histogramPercentilesMap[key] = pcts;
  }

  void registerHistogramMetricExportType(
      folly::StringPiece key,
      int64_t /* bucketWidth */,
      int64_t /* min */,
      int64_t /* max */,
      const std::vector<int32_t>& pcts) const override {
    histogramPercentilesMap[key.str()] = pcts;
  }

  void addMetricValue(const std::string& key, const size_t value)
      const override {
    counterMap[key] += value;
  }

  void addMetricValue(const char* key, const size_t value) const override {
    counterMap[key] += value;
  }

  void addMetricValue(folly::StringPiece key, size_t value) const override {
    counterMap[key.str()] += value;
  }

  void addHistogramMetricValue(const std::string& key, size_t value)
      const override {
    counterMap[key] = std::max(counterMap[key], value);
  }

  void addHistogramMetricValue(const char* key, size_t value) const override {
    counterMap[key] = std::max(counterMap[key], value);
  }

  void addHistogramMetricValue(folly::StringPiece key, size_t value)
      const override {
    counterMap[key.str()] = std::max(counterMap[key.str()], value);
  }
};

TEST_F(StatsReporterTest, trivialReporter) {
  auto reporter = std::dynamic_pointer_cast<TestReporter>(
      folly::Singleton<BaseStatsReporter>::try_get());

  DEFINE_METRIC("key1", StatType::COUNT);
  DEFINE_METRIC("key2", StatType::SUM);
  DEFINE_METRIC("key3", StatType::RATE);
  DEFINE_HISTOGRAM_METRIC("key4", 10, 0, 100, 50, 99, 100);

  EXPECT_EQ(StatType::COUNT, reporter->statTypeMap["key1"]);
  EXPECT_EQ(StatType::SUM, reporter->statTypeMap["key2"]);
  EXPECT_EQ(StatType::RATE, reporter->statTypeMap["key3"]);
  std::vector<int32_t> expected = {50, 99, 100};
  EXPECT_EQ(expected, reporter->histogramPercentilesMap["key4"]);
  EXPECT_TRUE(
      reporter->statTypeMap.find("key5") == reporter->statTypeMap.end());

  RECORD_METRIC_VALUE("key1", 10);
  RECORD_METRIC_VALUE("key1", 11);
  RECORD_METRIC_VALUE("key1", 15);
  RECORD_METRIC_VALUE("key2", 1001);
  RECORD_METRIC_VALUE("key2", 1200);
  RECORD_METRIC_VALUE("key3");
  RECORD_METRIC_VALUE("key3", 1100);
  RECORD_HISTOGRAM_METRIC_VALUE("key4", 50);
  RECORD_HISTOGRAM_METRIC_VALUE("key4", 100);

  EXPECT_EQ(36, reporter->counterMap["key1"]);
  EXPECT_EQ(2201, reporter->counterMap["key2"]);
  EXPECT_EQ(1101, reporter->counterMap["key3"]);
  EXPECT_EQ(100, reporter->counterMap["key4"]);
};

// Registering to folly Singleton with intended reporter type
folly::Singleton<BaseStatsReporter> reporter([]() {
  return new TestReporter();
});

} // namespace facebook::velox

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  facebook::velox::BaseStatsReporter::registered = true;
  return RUN_ALL_TESTS();
}
