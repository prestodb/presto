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

#include "presto_cpp/main/MemorySampling.h"

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <folly/Singleton.h>
#include <gtest/gtest.h>

#include "presto_cpp/main/PeriodicMemoryChecker.h"
#include "presto_cpp/main/common/Counters.h"
#include "velox/common/base/StatsReporter.h"

using namespace facebook::velox;

namespace facebook::presto {
namespace {

// StatsReporter that captures the latest value recorded against each metric
// so a test can assert what sampleSystemMemoryAndRecord emitted. Only the
// addMetricValue hook is used; the rest are no-ops.
class CapturingStatsReporter : public BaseStatsReporter {
 public:
  void registerMetricExportType(const char* /*key*/, StatType /*type*/)
      const override {}
  void registerMetricExportType(folly::StringPiece /*key*/, StatType /*type*/)
      const override {}
  void registerHistogramMetricExportType(
      const char* /*key*/,
      int64_t /*bucket*/,
      int64_t /*min*/,
      int64_t /*max*/,
      const std::vector<int32_t>& /*pcts*/) const override {}
  void registerHistogramMetricExportType(
      folly::StringPiece /*key*/,
      int64_t /*bucket*/,
      int64_t /*min*/,
      int64_t /*max*/,
      const std::vector<int32_t>& /*pcts*/) const override {}

  void addMetricValue(const std::string& key, size_t value = 1) const override {
    std::lock_guard<std::mutex> lock(mu_);
    values_[key] = static_cast<int64_t>(value);
    ++recordCount_;
  }
  void addMetricValue(const char* key, size_t value = 1) const override {
    addMetricValue(std::string(key), value);
  }
  void addMetricValue(folly::StringPiece key, size_t value = 1) const override {
    addMetricValue(key.str(), value);
  }
  void addHistogramMetricValue(
      const std::string& /*key*/,
      size_t /*value*/) const override {}
  void addHistogramMetricValue(const char* /*key*/, size_t /*value*/)
      const override {}
  void addHistogramMetricValue(folly::StringPiece /*key*/, size_t /*value*/)
      const override {}

  std::string fetchMetrics() override {
    return "";
  }

  int64_t lastValueFor(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = values_.find(key);
    return it == values_.end() ? -1 : it->second;
  }

  size_t recordCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return recordCount_;
  }

  void reset() {
    std::lock_guard<std::mutex> lock(mu_);
    values_.clear();
    recordCount_ = 0;
  }

 private:
  mutable std::mutex mu_;
  mutable std::unordered_map<std::string, int64_t> values_;
  mutable size_t recordCount_{0};
};

// Minimal PeriodicMemoryChecker stub exposing a settable sample. Modeled on
// TestPeriodicMemoryChecker in PeriodicMemoryCheckerTest.cpp.
class StubMemoryChecker : public PeriodicMemoryChecker {
 public:
  explicit StubMemoryChecker(int64_t initial)
      : PeriodicMemoryChecker(PeriodicMemoryChecker::Config{}) {
    cachedSystemUsedMemoryBytes_.store(initial, std::memory_order_relaxed);
  }

 protected:
  void refreshSystemMemoryUsageLocked() override {}
  int64_t mallocBytes() const override {
    return 0;
  }
  void periodicCb() override {}
  bool heapDumpCb(const std::string& /*filePath*/) const override {
    return true;
  }
  void removeDumpFile(const std::string& /*filePath*/) const override {}
};

folly::Singleton<BaseStatsReporter> reporter([]() {
  return new CapturingStatsReporter();
});

CapturingStatsReporter& reporterRef() {
  auto handle = folly::Singleton<BaseStatsReporter>::try_get();
  return *static_cast<CapturingStatsReporter*>(handle.get());
}

} // namespace

class MemorySamplingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    BaseStatsReporter::registered = true;
    reporterRef().reset();
  }
  void TearDown() override {
    BaseStatsReporter::registered = false;
    reporterRef().reset();
  }
};

TEST_F(MemorySamplingTest, nullCheckerReturnsEmptyAndSkipsMetric) {
  // When no checker is installed the helper must return an empty shared_ptr
  // and must NOT record the metric. Scrapers rely on the time series being
  // absent, not zero, so dashboards can tell "not sampled" from "idle".
  const auto result = sampleSystemMemoryAndRecord(nullptr);

  ASSERT_EQ(result, nullptr);
  ASSERT_EQ(reporterRef().recordCount(), 0);
  ASSERT_EQ(
      reporterRef().lastValueFor(
          std::string(kCounterMemoryCheckerUsedBytes)),
      -1);
}

TEST_F(MemorySamplingTest, nonNullCheckerReturnsSampleAndRecordsMetric) {
  // The stub exposes a known cached sample; the helper must return a
  // non-null shared_ptr holding that value and must record the same value
  // against kCounterMemoryCheckerUsedBytes exactly once per call.
  constexpr int64_t kSample = 12'345'678;
  StubMemoryChecker checker(kSample);

  const auto result = sampleSystemMemoryAndRecord(&checker);

  ASSERT_NE(result, nullptr);
  ASSERT_EQ(*result, kSample);
  ASSERT_EQ(reporterRef().recordCount(), 1);
  ASSERT_EQ(
      reporterRef().lastValueFor(
          std::string(kCounterMemoryCheckerUsedBytes)),
      kSample);
}

TEST_F(MemorySamplingTest, metricReflectsEachCallsSample) {
  // Each call must publish the current reading, so a monitor scraping the
  // metric sees the latest sample rather than a stale one.
  StubMemoryChecker first(100);
  ASSERT_EQ(*sampleSystemMemoryAndRecord(&first), 100);
  ASSERT_EQ(
      reporterRef().lastValueFor(
          std::string(kCounterMemoryCheckerUsedBytes)),
      100);

  StubMemoryChecker second(999);
  ASSERT_EQ(*sampleSystemMemoryAndRecord(&second), 999);
  ASSERT_EQ(
      reporterRef().lastValueFor(
          std::string(kCounterMemoryCheckerUsedBytes)),
      999);
  ASSERT_EQ(reporterRef().recordCount(), 2);
}

} // namespace facebook::presto
