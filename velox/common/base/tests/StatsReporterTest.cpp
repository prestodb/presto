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
  mutable std::unordered_map<std::string, StatType> counterTypeMap;

  void addStatExportType(const char* key, StatType statType) const override {
    counterTypeMap[key] = statType;
  }

  void addStatExportType(folly::StringPiece key, StatType statType)
      const override {
    counterTypeMap[key.str()] = statType;
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
};

TEST_F(StatsReporterTest, trivialReporter) {
  auto reporter = std::dynamic_pointer_cast<TestReporter>(
      folly::Singleton<BaseStatsReporter>::try_get());

  REPORT_ADD_STAT_EXPORT_TYPE("key1", StatType::COUNT);
  REPORT_ADD_STAT_EXPORT_TYPE("key2", StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE("key3", StatType::RATE);

  EXPECT_EQ(StatType::COUNT, reporter->counterTypeMap["key1"]);
  EXPECT_EQ(StatType::SUM, reporter->counterTypeMap["key2"]);
  EXPECT_EQ(StatType::RATE, reporter->counterTypeMap["key3"]);
  EXPECT_TRUE(
      reporter->counterTypeMap.find("key4") == reporter->counterTypeMap.end());

  REPORT_ADD_STAT_VALUE("key1", 10);
  REPORT_ADD_STAT_VALUE("key1", 11);
  REPORT_ADD_STAT_VALUE("key1", 15);
  REPORT_ADD_STAT_VALUE("key2", 1001);
  REPORT_ADD_STAT_VALUE("key2", 1200);
  REPORT_ADD_STAT_VALUE("key3");
  REPORT_ADD_STAT_VALUE("key3", 1100);

  EXPECT_EQ(36, reporter->counterMap["key1"]);
  EXPECT_EQ(2201, reporter->counterMap["key2"]);
  EXPECT_EQ(1101, reporter->counterMap["key3"]);
};

// Registering to folly Singleton with intended reporter type
folly::Singleton<BaseStatsReporter> reporter([]() {
  return new TestReporter();
});

} // namespace facebook::velox

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
