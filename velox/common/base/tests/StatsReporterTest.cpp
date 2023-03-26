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

  void addStatExportType(const char* key, StatType statType) const override {
    statTypeMap[key] = statType;
  }

  void addStatExportType(folly::StringPiece key, StatType statType)
      const override {
    statTypeMap[key.str()] = statType;
  }

  void addHistogramExportPercentiles(
      const char* key,
      int64_t /* bucketWidth */,
      int64_t /* min */,
      int64_t /* max */,
      const std::vector<int32_t>& pcts) const override {
    histogramPercentilesMap[key] = pcts;
  }

  void addHistogramExportPercentiles(
      folly::StringPiece key,
      int64_t /* bucketWidth */,
      int64_t /* min */,
      int64_t /* max */,
      const std::vector<int32_t>& pcts) const override {
    histogramPercentilesMap[key.str()] = pcts;
  }

  void addStatValue(const std::string& key, const size_t value) const override {
    counterMap[key] += value;
  }

  void addStatValue(const char* key, const size_t value) const override {
    counterMap[key] += value;
  }

  void addStatValue(folly::StringPiece key, size_t value) const override {
    counterMap[key.str()] += value;
  }

  void addHistogramValue(const std::string& key, size_t value) const override {
    counterMap[key] = std::max(counterMap[key], value);
  }

  void addHistogramValue(const char* key, size_t value) const override {
    counterMap[key] = std::max(counterMap[key], value);
  }

  void addHistogramValue(folly::StringPiece key, size_t value) const override {
    counterMap[key.str()] = std::max(counterMap[key.str()], value);
  }
};

TEST_F(StatsReporterTest, trivialReporter) {
  auto reporter = std::dynamic_pointer_cast<TestReporter>(
      folly::Singleton<BaseStatsReporter>::try_get());

  REPORT_ADD_STAT_EXPORT_TYPE("key1", StatType::COUNT);
  REPORT_ADD_STAT_EXPORT_TYPE("key2", StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE("key3", StatType::RATE);
  REPORT_ADD_HISTOGRAM_EXPORT_PERCENTILE("key4", 10, 0, 100, 50, 99, 100);

  EXPECT_EQ(StatType::COUNT, reporter->statTypeMap["key1"]);
  EXPECT_EQ(StatType::SUM, reporter->statTypeMap["key2"]);
  EXPECT_EQ(StatType::RATE, reporter->statTypeMap["key3"]);
  std::vector<int32_t> expected = {50, 99, 100};
  EXPECT_EQ(expected, reporter->histogramPercentilesMap["key4"]);
  EXPECT_TRUE(
      reporter->statTypeMap.find("key5") == reporter->statTypeMap.end());

  REPORT_ADD_STAT_VALUE("key1", 10);
  REPORT_ADD_STAT_VALUE("key1", 11);
  REPORT_ADD_STAT_VALUE("key1", 15);
  REPORT_ADD_STAT_VALUE("key2", 1001);
  REPORT_ADD_STAT_VALUE("key2", 1200);
  REPORT_ADD_STAT_VALUE("key3");
  REPORT_ADD_STAT_VALUE("key3", 1100);
  REPORT_ADD_HISTOGRAM_VALUE("key4", 50);
  REPORT_ADD_HISTOGRAM_VALUE("key4", 100);

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
  folly::init(&argc, &argv, false);
  facebook::velox::BaseStatsReporter::registered = true;
  return RUN_ALL_TESTS();
}
