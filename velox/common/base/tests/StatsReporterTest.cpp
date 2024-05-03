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
#include <unordered_set>
#include "velox/common/base/Counters.h"
#include "velox/common/base/PeriodicStatsReporter.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox {

class TestReporter : public BaseStatsReporter {
 public:
  mutable std::unordered_map<std::string, size_t> counterMap;
  mutable std::unordered_map<std::string, StatType> statTypeMap;
  mutable std::unordered_map<std::string, std::vector<int32_t>>
      histogramPercentilesMap;

  void clear() {
    counterMap.clear();
    statTypeMap.clear();
    histogramPercentilesMap.clear();
  }

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

class StatsReporterTest : public testing::Test {
 protected:
  void SetUp() override {
    reporter_ = std::dynamic_pointer_cast<TestReporter>(
        folly::Singleton<BaseStatsReporter>::try_get());
  }
  void TearDown() override {
    reporter_->clear();
  }

  std::shared_ptr<TestReporter> reporter_;
};

TEST_F(StatsReporterTest, trivialReporter) {
  DEFINE_METRIC("key1", StatType::COUNT);
  DEFINE_METRIC("key2", StatType::SUM);
  DEFINE_METRIC("key3", StatType::RATE);
  DEFINE_HISTOGRAM_METRIC("key4", 10, 0, 100, 50, 99, 100);

  EXPECT_EQ(StatType::COUNT, reporter_->statTypeMap["key1"]);
  EXPECT_EQ(StatType::SUM, reporter_->statTypeMap["key2"]);
  EXPECT_EQ(StatType::RATE, reporter_->statTypeMap["key3"]);
  std::vector<int32_t> expected = {50, 99, 100};
  EXPECT_EQ(expected, reporter_->histogramPercentilesMap["key4"]);
  EXPECT_TRUE(
      reporter_->statTypeMap.find("key5") == reporter_->statTypeMap.end());

  RECORD_METRIC_VALUE("key1", 10);
  RECORD_METRIC_VALUE("key1", 11);
  RECORD_METRIC_VALUE("key1", 15);
  RECORD_METRIC_VALUE("key2", 1001);
  RECORD_METRIC_VALUE("key2", 1200);
  RECORD_METRIC_VALUE("key3");
  RECORD_METRIC_VALUE("key3", 1100);
  RECORD_HISTOGRAM_METRIC_VALUE("key4", 50);
  RECORD_HISTOGRAM_METRIC_VALUE("key4", 100);

  EXPECT_EQ(36, reporter_->counterMap["key1"]);
  EXPECT_EQ(2201, reporter_->counterMap["key2"]);
  EXPECT_EQ(1101, reporter_->counterMap["key3"]);
  EXPECT_EQ(100, reporter_->counterMap["key4"]);
};

class PeriodicStatsReporterTest : public StatsReporterTest {};

class TestStatsReportMemoryArbitrator : public memory::MemoryArbitrator {
 public:
  explicit TestStatsReportMemoryArbitrator(
      memory::MemoryArbitrator::Stats stats)
      : memory::MemoryArbitrator({}), stats_(stats) {}

  ~TestStatsReportMemoryArbitrator() override = default;

  void updateStats(memory::MemoryArbitrator::Stats stats) {
    stats_ = stats;
  }

  std::string kind() const override {
    return "test";
  }

  uint64_t growCapacity(memory::MemoryPool* /*unused*/, uint64_t /*unused*/)
      override {
    return 0;
  }

  bool growCapacity(
      memory::MemoryPool* /*unused*/,
      const std::vector<std::shared_ptr<memory::MemoryPool>>& /*unused*/,
      uint64_t /*unused*/) override {
    return false;
  }

  uint64_t shrinkCapacity(memory::MemoryPool* /*unused*/, uint64_t /*unused*/)
      override {
    return 0;
  }

  uint64_t shrinkCapacity(
      const std::vector<std::shared_ptr<memory::MemoryPool>>& /*unused*/,
      uint64_t /*unused*/,
      bool /*unused*/,
      bool /*unused*/) override {
    return 0;
  }

  Stats stats() const override {
    return stats_;
  }

  std::string toString() const override {
    return "TestStatsReportMemoryArbitrator::toString()";
  }

 private:
  memory::MemoryArbitrator::Stats stats_;
};

TEST_F(PeriodicStatsReporterTest, basic) {
  TestStatsReportMemoryArbitrator arbitrator({});
  PeriodicStatsReporter::Options options;
  options.arbitrator = &arbitrator;
  options.arbitratorStatsIntervalMs = 4'000;
  PeriodicStatsReporter periodicReporter(options);

  periodicReporter.start();
  std::this_thread::sleep_for(std::chrono::milliseconds(2'000));
  // Stop right after sufficient wait to ensure the following reads from main
  // thread does not trigger TSAN failures.
  periodicReporter.stop();

  const auto& counterMap = reporter_->counterMap;
  ASSERT_EQ(counterMap.size(), 2);
  ASSERT_EQ(counterMap.count(kMetricArbitratorFreeCapacityBytes.str()), 1);
  ASSERT_EQ(
      counterMap.count(kMetricArbitratorFreeReservedCapacityBytes.str()), 1);
}

TEST_F(PeriodicStatsReporterTest, globalInstance) {
  TestStatsReportMemoryArbitrator arbitrator({});
  PeriodicStatsReporter::Options options;
  options.arbitrator = &arbitrator;
  options.arbitratorStatsIntervalMs = 4'000;
  VELOX_ASSERT_THROW(
      stopPeriodicStatsReporter(), "No periodic stats reporter to stop.");
  ASSERT_NO_THROW(startPeriodicStatsReporter(options));
  VELOX_ASSERT_THROW(
      startPeriodicStatsReporter(options),
      "The periodic stats reporter has already started.");
  ASSERT_NO_THROW(stopPeriodicStatsReporter());
}

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
