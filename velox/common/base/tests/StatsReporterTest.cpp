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
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/CacheTTLController.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/memory/MmapAllocator.h"

namespace facebook::velox {

class TestReporter : public BaseStatsReporter {
 public:
  mutable std::mutex m;
  mutable std::map<std::string, size_t> counterMap;
  mutable std::unordered_map<std::string, StatType> statTypeMap;
  mutable std::unordered_map<std::string, std::vector<int32_t>>
      histogramPercentilesMap;

  void clear() {
    std::lock_guard<std::mutex> l(m);
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
    std::lock_guard<std::mutex> l(m);
    counterMap[key] += value;
  }

  void addMetricValue(const char* key, const size_t value) const override {
    std::lock_guard<std::mutex> l(m);
    counterMap[key] += value;
  }

  void addMetricValue(folly::StringPiece key, size_t value) const override {
    std::lock_guard<std::mutex> l(m);
    counterMap[key.str()] += value;
  }

  void addHistogramMetricValue(const std::string& key, size_t value)
      const override {
    std::lock_guard<std::mutex> l(m);
    counterMap[key] = std::max(counterMap[key], value);
  }

  void addHistogramMetricValue(const char* key, size_t value) const override {
    std::lock_guard<std::mutex> l(m);
    counterMap[key] = std::max(counterMap[key], value);
  }

  void addHistogramMetricValue(folly::StringPiece key, size_t value)
      const override {
    std::lock_guard<std::mutex> l(m);
    counterMap[key.str()] = std::max(counterMap[key.str()], value);
  }

  std::string fetchMetrics() override {
    std::stringstream ss;
    ss << "[";
    auto sep = "";
    for (const auto& [key, value] : counterMap) {
      ss << sep << key << ":" << value;
      sep = ",";
    }
    ss << "]";
    return ss.str();
  }
};

class StatsReporterTest : public testing::Test {
 protected:
  void SetUp() override {
    reporter_ = std::dynamic_pointer_cast<TestReporter>(
        folly::Singleton<BaseStatsReporter>::try_get());
    reporter_->clear();
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

  EXPECT_EQ(
      "[key1:36,key2:2201,key3:1101,key4:100]", reporter_->fetchMetrics());
};

class PeriodicStatsReporterTest : public StatsReporterTest {};

class TestStatsReportMmapAllocator : public memory::MmapAllocator {
 public:
  TestStatsReportMmapAllocator(
      memory::MachinePageCount numMapped,
      memory::MachinePageCount numAllocated,
      memory::MachinePageCount numMallocBytes,
      memory::MachinePageCount numExternalMapped)
      : memory::MmapAllocator({.capacity = 1024}),
        numMapped_(numMapped),
        numAllocated_(numAllocated),
        numMallocBytes_(numMallocBytes),
        numExternalMapped_(numExternalMapped) {}

  memory::MachinePageCount numMapped() const override {
    return numMapped_;
  }

  memory::MachinePageCount numAllocated() const override {
    return numAllocated_;
  }

  uint64_t numMallocBytes() const {
    return numMallocBytes_;
  }

  memory::MachinePageCount numExternalMapped() const {
    return numExternalMapped_;
  }

 private:
  memory::MachinePageCount numMapped_;
  memory::MachinePageCount numAllocated_;
  memory::MachinePageCount numMallocBytes_;
  memory::MachinePageCount numExternalMapped_;
};

class TestStatsReportAsyncDataCache : public cache::AsyncDataCache {
 public:
  TestStatsReportAsyncDataCache(cache::CacheStats stats)
      : cache::AsyncDataCache(nullptr, nullptr), stats_(stats) {}

  cache::CacheStats refreshStats() const override {
    std::lock_guard<std::mutex> l(mutex_);
    return stats_;
  }

  void updateStats(cache::CacheStats stats) {
    std::lock_guard<std::mutex> l(mutex_);
    stats_ = stats;
  }

 private:
  mutable std::mutex mutex_;
  cache::CacheStats stats_;
};

class TestStatsReportMemoryArbitrator : public memory::MemoryArbitrator {
 public:
  explicit TestStatsReportMemoryArbitrator(
      memory::MemoryArbitrator::Stats stats)
      : memory::MemoryArbitrator({}), stats_(stats) {}

  ~TestStatsReportMemoryArbitrator() override = default;

  void updateStats(memory::MemoryArbitrator::Stats stats) {
    std::lock_guard<std::mutex> l(mutex_);
    stats_ = stats;
  }

  std::string kind() const override {
    return "test";
  }

  void addPool(const std::shared_ptr<memory::MemoryPool>& /*unused*/) override {
  }

  void removePool(memory::MemoryPool* /*unused*/) override {}

  bool growCapacity(memory::MemoryPool* /*unused*/, uint64_t /*unused*/)
      override {
    return false;
  }

  uint64_t shrinkCapacity(memory::MemoryPool* /*unused*/, uint64_t /*unused*/)
      override {
    return 0;
  }

  uint64_t shrinkCapacity(uint64_t /*unused*/, bool /*unused*/, bool /*unused*/)
      override {
    return 0;
  }

  Stats stats() const override {
    std::lock_guard<std::mutex> l(mutex_);
    return stats_;
  }

  std::string toString() const override {
    return "TestStatsReportMemoryArbitrator::toString()";
  }

 private:
  mutable std::mutex mutex_;
  memory::MemoryArbitrator::Stats stats_;
};

class TestMemoryPool : public memory::MemoryPool {
 public:
  explicit TestMemoryPool() : MemoryPool("", Kind::kAggregate, nullptr, {}) {}

  void* allocate(int64_t size) override {
    return nullptr;
  }

  void* allocateZeroFilled(int64_t /* unused */, int64_t /* unused */)
      override {
    return nullptr;
  }

  void* reallocate(
      void* /* unused */,
      int64_t /* unused */,
      int64_t /* unused */) override {
    return nullptr;
  }

  void free(void* /* unused */, int64_t /* unused */) override {}

  void allocateNonContiguous(
      memory::MachinePageCount /* unused */,
      memory::Allocation& /* unused */,
      memory::MachinePageCount /* unused */) override {}

  void freeNonContiguous(memory::Allocation& /* unused */) override {}

  memory::MachinePageCount largestSizeClass() const override {
    return 0;
  }

  const std::vector<memory::MachinePageCount>& sizeClasses() const override {
    static std::vector<memory::MachinePageCount> sizeClasses;
    return sizeClasses;
  }

  void allocateContiguous(
      memory::MachinePageCount /* unused */,
      memory::ContiguousAllocation& /* unused */,
      memory::MachinePageCount /* unused */) override {}

  void freeContiguous(memory::ContiguousAllocation& /* unused */) override {}

  void growContiguous(
      memory::MachinePageCount /* unused */,
      memory::ContiguousAllocation& /* unused */) override {}

  int64_t capacity() const override {
    return 0;
  }

  int64_t usedBytes() const override {
    return 0;
  }

  int64_t peakBytes() const override {
    return 0;
  }

  int64_t availableReservation() const override {
    return 0;
  }

  int64_t releasableReservation() const override {
    return 0;
  }

  int64_t reservedBytes() const override {
    return 0;
  }

  bool maybeReserve(uint64_t /* unused */) override {
    return false;
  }

  void release() override {}

  uint64_t freeBytes() const override {
    return 0;
  }

  uint64_t shrink(uint64_t /* unused */) override {
    return 0;
  }

  bool grow(uint64_t /* unused */, uint64_t /* unused */) override {
    return false;
  }

  void setReclaimer(
      std::unique_ptr<memory::MemoryReclaimer> /* unused */) override {}
  memory::MemoryReclaimer* reclaimer() const override {
    return nullptr;
  }

  void enterArbitration() override {}

  void leaveArbitration() noexcept override {}

  std::optional<uint64_t> reclaimableBytes() const override {
    return std::nullopt;
  }

  uint64_t reclaim(
      uint64_t /* unused */,
      uint64_t /* unused */,
      memory::MemoryReclaimer::Stats& /* unused */) override {
    return 0;
  }

  void abort(const std::exception_ptr& /* unused */) override {}

  bool aborted() const override {
    return false;
  }

  std::string toString() const override {
    return "";
  }

  std::string treeMemoryUsage(bool /* unused */) const override {
    return "";
  }

  std::shared_ptr<MemoryPool> genChild(
      std::shared_ptr<MemoryPool> /* unused */,
      const std::string& /* unused */,
      Kind /* unused */,
      bool /* unused */,
      std::unique_ptr<memory::MemoryReclaimer> /* unused */) override {
    return nullptr;
  }

  Stats stats() const override {
    return Stats();
  }
};

TEST_F(PeriodicStatsReporterTest, basic) {
  TestStatsReportMmapAllocator allocator(1, 1, 1, 1);
  TestStatsReportAsyncDataCache cache(
      {.ssdStats = std::make_shared<cache::SsdCacheStats>()});
  cache::CacheTTLController::create(cache);
  TestStatsReportMemoryArbitrator arbitrator({});
  TestMemoryPool spillMemoryPool;
  PeriodicStatsReporter::Options options;
  options.cache = &cache;
  options.cacheStatsIntervalMs = 4'000;
  options.allocator = &allocator;
  options.allocatorStatsIntervalMs = 4'000;
  options.arbitrator = &arbitrator;
  options.arbitratorStatsIntervalMs = 4'000;
  options.spillMemoryPool = &spillMemoryPool;
  options.spillStatsIntervalMs = 4'000;
  PeriodicStatsReporter periodicReporter(options);

  periodicReporter.start();
  std::this_thread::sleep_for(std::chrono::milliseconds(2'000));

  // Check snapshot stats
  const auto& counterMap = reporter_->counterMap;
  {
    std::lock_guard<std::mutex> l(reporter_->m);
    ASSERT_EQ(counterMap.count(kMetricArbitratorFreeCapacityBytes.str()), 1);
    ASSERT_EQ(
        counterMap.count(kMetricArbitratorFreeReservedCapacityBytes.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumEntries.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumEmptyEntries.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumSharedEntries.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumExclusiveEntries.str()), 1);
    ASSERT_EQ(
        counterMap.count(kMetricMemoryCacheNumPrefetchedEntries.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheTotalTinyBytes.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheTotalLargeBytes.str()), 1);
    ASSERT_EQ(
        counterMap.count(kMetricMemoryCacheTotalTinyPaddingBytes.str()), 1);
    ASSERT_EQ(
        counterMap.count(kMetricMemoryCacheTotalLargePaddingBytes.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheTotalPrefetchBytes.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheCachedEntries.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheCachedRegions.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheCachedBytes.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricCacheMaxAgeSecs.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMappedMemoryBytes.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricAllocatedMemoryBytes.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMmapDelegatedAllocBytes.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMmapExternalMappedBytes.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSpillMemoryBytes.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSpillPeakMemoryBytes.str()), 1);
    // Check deltas are not reported
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumHits.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheHitBytes.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumNew.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumEvicts.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumSavableEvicts.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumEvictChecks.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumWaitExclusive.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumAllocClocks.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumAgedOutEntries.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheSumEvictScore.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheReadEntries.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheReadBytes.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheWrittenEntries.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheWrittenBytes.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheOpenSsdErrors.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheOpenCheckpointErrors.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheOpenLogErrors.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheDeleteCheckpointErrors.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheGrowFileErrors.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheWriteSsdErrors.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheWriteSsdDropped.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheWriteCheckpointErrors.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheReadSsdErrors.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheReadCorruptions.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheReadCheckpointErrors.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheCheckpointsRead.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheCheckpointsWritten.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheRegionsEvicted.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheAgedOutEntries.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheAgedOutRegions.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheRecoveredEntries.str()), 0);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheReadWithoutChecksum.str()), 0);
    ASSERT_EQ(counterMap.size(), 22);
  }

  // Update stats
  auto newSsdStats = std::make_shared<cache::SsdCacheStats>();
  newSsdStats->entriesWritten = 10;
  newSsdStats->bytesWritten = 10;
  newSsdStats->checkpointsWritten = 10;
  newSsdStats->entriesRead = 10;
  newSsdStats->bytesRead = 10;
  newSsdStats->checkpointsRead = 10;
  newSsdStats->entriesAgedOut = 10;
  newSsdStats->regionsAgedOut = 10;
  newSsdStats->regionsEvicted = 10;
  newSsdStats->numPins = 10;
  newSsdStats->openFileErrors = 10;
  newSsdStats->openCheckpointErrors = 10;
  newSsdStats->openLogErrors = 10;
  newSsdStats->deleteCheckpointErrors = 10;
  newSsdStats->growFileErrors = 10;
  newSsdStats->writeSsdErrors = 10;
  newSsdStats->writeSsdDropped = 10;
  newSsdStats->writeCheckpointErrors = 10;
  newSsdStats->readSsdErrors = 10;
  newSsdStats->readSsdCorruptions = 10;
  newSsdStats->readCheckpointErrors = 10;
  newSsdStats->readWithoutChecksumChecks = 10;
  newSsdStats->entriesRecovered = 10;
  cache.updateStats(
      {.numHit = 10,
       .hitBytes = 10,
       .numNew = 10,
       .numEvict = 10,
       .numSavableEvict = 10,
       .numEvictChecks = 10,
       .numWaitExclusive = 10,
       .numAgedOut = 10,
       .allocClocks = 10,
       .sumEvictScore = 10,
       .ssdStats = newSsdStats});
  arbitrator.updateStats(memory::MemoryArbitrator::Stats(
      10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10));
  std::this_thread::sleep_for(std::chrono::milliseconds(4'000));

  // Stop right after sufficient wait to ensure the following reads from main
  // thread does not trigger TSAN failures.
  periodicReporter.stop();

  // Check delta stats are reported
  {
    std::lock_guard<std::mutex> l(reporter_->m);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumHits.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheHitBytes.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumNew.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumEvicts.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumSavableEvicts.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumEvictChecks.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumWaitExclusive.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumAllocClocks.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheNumAgedOutEntries.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricMemoryCacheSumEvictScore.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheReadEntries.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheReadBytes.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheWrittenEntries.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheWrittenBytes.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheOpenSsdErrors.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheOpenCheckpointErrors.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheOpenLogErrors.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheDeleteCheckpointErrors.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheGrowFileErrors.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheWriteSsdErrors.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheWriteSsdDropped.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheWriteCheckpointErrors.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheReadSsdErrors.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheReadCorruptions.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheReadCheckpointErrors.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheCheckpointsRead.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheCheckpointsWritten.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheRegionsEvicted.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheAgedOutEntries.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheAgedOutRegions.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheRecoveredEntries.str()), 1);
    ASSERT_EQ(counterMap.count(kMetricSsdCacheReadWithoutChecksum.str()), 1);
    ASSERT_EQ(counterMap.size(), 54);
  }
}

TEST_F(PeriodicStatsReporterTest, globalInstance) {
  TestStatsReportMemoryArbitrator arbitrator({});
  PeriodicStatsReporter::Options options;
  PeriodicStatsReporter periodicReporter(options);
  ASSERT_NO_THROW(periodicReporter.start());
  std::this_thread::sleep_for(std::chrono::milliseconds(4'000));
  ASSERT_NO_THROW(periodicReporter.stop());
}

TEST_F(PeriodicStatsReporterTest, allNullOption) {
  PeriodicStatsReporter::Options options;
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
