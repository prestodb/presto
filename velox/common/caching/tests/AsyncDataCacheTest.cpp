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

#include "folly/experimental/EventCount.h"
#include "velox/common/base/Semaphore.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/caching/CacheTTLController.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/common/testutil/ScopedTestTime.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/executors/QueuedImmediateExecutor.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/types.h>

using namespace facebook::velox;
using namespace facebook::velox::cache;
using namespace facebook::velox::common::testutil;

using facebook::velox::memory::MemoryAllocator;

// Represents a planned load from a file. Many of these constitute a load plan.
struct Request {
  Request(uint64_t _offset, uint64_t _size) : offset(_offset), size(_size) {}

  uint64_t offset;
  uint64_t size;
  SsdPin ssdPin;
};

struct TestParam {
  bool checksumEnabled;
  bool checksumVerificationEnabled;
};

class AsyncDataCacheTest : public ::testing::TestWithParam<TestParam> {
 public:
  static std::vector<TestParam> getTestParams() {
    static std::vector<TestParam> testParams = {
        {false, false}, {true, false}, {true, true}};
    return testParams;
  }

  // Deterministically fills 'allocation' based on 'sequence'
  static void initializeContents(int64_t sequence, memory::Allocation& alloc) {
    for (int32_t i = 0; i < alloc.numRuns(); ++i) {
      memory::Allocation::PageRun run = alloc.runAt(i);
      int64_t* ptr = reinterpret_cast<int64_t*>(run.data());
      const int32_t numWords =
          memory::AllocationTraits::pageBytes(run.numPages()) / sizeof(void*);
      for (int32_t offset = 0; offset < numWords; ++offset) {
        ptr[offset] = sequence + offset;
      }
    }
  }

 protected:
  static constexpr int32_t kNumFiles = 100;

  static void SetUpTestCase() {
    TestValue::enable();
  }

  void SetUp() override {
    filesystems::registerLocalFileSystem();
  }

  void TearDown() override {
    if (cache_) {
      cache_->shutdown();
      auto* ssdCache = cache_->ssdCache();
      if (ssdCache) {
        ssdCache->testingDeleteFiles();
      }
    }
    if (executor_) {
      executor_->join();
    }
    filenames_.clear();
    CacheTTLController::testingClear();
    fileIds().testingReset();
  }

  void waitForPendingLoads() {
    while (numPendingLoads_ > 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(2000)); // NOLINT
    }
  }

  void initializeCache(
      uint64_t maxBytes,
      int64_t ssdBytes = 0,
      uint64_t checkpointIntervalBytes = 0,
      AsyncDataCache::Options cacheOptions = {}) {
    if (cache_ != nullptr) {
      cache_->shutdown();
    }
    cache_.reset();

    std::unique_ptr<SsdCache> ssdCache;
    if (ssdBytes > 0) {
      // tmpfs does not support O_DIRECT, so turn this off for testing.
      FLAGS_ssd_odirect = false;
      // Make a new tempDirectory only if one is not already set. The
      // second creation of cache must find the checkpoint of the
      // previous one.
      if (tempDirectory_ == nullptr) {
        tempDirectory_ = exec::test::TempDirectoryPath::create();
      }
      SsdCache::Config config(
          fmt::format("{}/cache", tempDirectory_->getPath()),
          ssdBytes,
          4,
          executor(),
          checkpointIntervalBytes > 0 ? checkpointIntervalBytes : ssdBytes / 20,
          false,
          GetParam().checksumEnabled,
          GetParam().checksumVerificationEnabled);
      ssdCache = std::make_unique<SsdCache>(config);
    }

    memory::MemoryManagerOptions options;
    options.useMmapAllocator = true;
    options.allocatorCapacity = maxBytes;
    options.arbitratorCapacity = maxBytes;
    options.arbitratorReservedCapacity = 0;
    options.trackDefaultUsage = true;
    manager_ = std::make_unique<memory::MemoryManager>(options);
    allocator_ = static_cast<memory::MmapAllocator*>(manager_->allocator());
    cache_ =
        AsyncDataCache::create(allocator_, std::move(ssdCache), cacheOptions);
    if (filenames_.empty()) {
      for (auto i = 0; i < kNumFiles; ++i) {
        auto name = fmt::format("testing_file_{}", i);
        filenames_.push_back(StringIdLease(fileIds(), name));
      }
    }
    ASSERT_EQ(cache_->allocator()->kind(), MemoryAllocator::Kind::kMmap);
    ASSERT_EQ(MemoryAllocator::kindString(cache_->allocator()->kind()), "MMAP");
  }

  // Finds one entry from RAM, SSD or storage. Throws if the data
  // cannot be read or 'injectError' is true. Checks the data with
  // verifyHook and discards the pin.
  void loadOne(uint64_t fileNum, Request& request, bool injectError);

  // Brings the data for the ranges in 'requests' into cache. The individual
  // entries should be accessed with loadOne(). 'requests' are handled with one
  // TestingCoalescedSsdLoad and one TestingCoalescedLoad. Call
  // semaphore.acquire() twice if needing to wait for the two loads to finish.
  void loadBatch(
      uint64_t fileNum,
      std::vector<Request>& requests,
      bool injectError,
      Semaphore* semaphore = nullptr);

  // Gets a pin on each of 'requests' individually. This checks the contents via
  // cache_'s verifyHook.
  void checkBatch(
      uint64_t fileNum,
      std::vector<Request>& requests,
      bool injectError) {
    for (auto& request : requests) {
      loadOne(fileNum, request, injectError);
    }
  }

  void loadNFiles(int32_t numFiles, std::vector<int64_t> offsets);

  // Loads a sequence of entries from a number of files. Looks up a
  // number of entries, then loads the ones that nobody else is
  // loading. Stops after loading 'loadBytes' worth of entries. If
  // 'errorEveryNBatches' is non-0, every nth load batch will have a
  // bad read and wil be dropped. The entries of the failed batch read
  // will still be accessed one by one. If 'largeEveryNBatches' is
  // non-0, allocates and frees a single allocation of 'largeBytes'
  // every so many batches. This creates extra memory pressure, as
  // happens when allocating large hash tables in queries.
  void loadLoop(
      int64_t startOffset,
      int64_t loadBytes,
      int32_t errorEveryNBatches = 0,
      int32_t largeEveryNBatches = 0,
      int32_t largeBytes = 0);

  // Calls func on 'numThreads' in parallel.
  template <typename Func>
  void runThreads(int32_t numThreads, Func func) {
    std::vector<std::thread> threads;
    threads.reserve(numThreads);
    for (int32_t i = 0; i < numThreads; ++i) {
      threads.push_back(std::thread([i, func]() { func(i); }));
    }
    for (auto& thread : threads) {
      thread.join();
    }
  }

  // Checks that the contents are consistent with what is set in
  // initializeContents.
  static void checkContents(const AsyncDataCacheEntry& entry) {
    const auto& alloc = entry.data();
    const int32_t numBytes = entry.size();
    const int64_t expectedSequence = entry.key().fileNum.id() + entry.offset();
    int32_t bytesChecked = sizeof(int64_t);
    for (int32_t i = 0; i < alloc.numRuns(); ++i) {
      const memory::Allocation::PageRun run = alloc.runAt(i);
      int64_t* ptr = reinterpret_cast<int64_t*>(run.data());
      const int32_t numWords =
          memory::AllocationTraits::pageBytes(run.numPages()) / sizeof(void*);
      for (int32_t offset = 0; offset < numWords; ++offset) {
        ASSERT_EQ(ptr[offset], expectedSequence + offset) << fmt::format(
            "{} {} + {}", entry.toString(), expectedSequence + offset, offset);
        bytesChecked += sizeof(int64_t);
        if (bytesChecked >= numBytes) {
          return;
        }
      }
    }
  }

  static void waitForSsdWriteToFinish(const SsdCache* ssdCache) {
    while (ssdCache->writeInProgress()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100)); // NOLINT
    }
  }

  CachePin newEntry(uint64_t offset, int32_t size) {
    folly::SemiFuture<bool> wait(false);
    try {
      RawFileCacheKey key{filenames_[0].id(), offset};
      auto pin = cache_->findOrCreate(key, size, &wait);
      EXPECT_FALSE(pin.empty());
      EXPECT_TRUE(pin.entry()->isExclusive());
      pin.entry()->setPrefetch();
      return pin;
    } catch (const VeloxException& e) {
      return CachePin();
    };
  }

  folly::IOThreadPoolExecutor* executor() {
    static std::mutex mutex;
    std::lock_guard<std::mutex> l(mutex);
    if (!executor_) {
      // We have up to 20 threads. Some tests run at max 16 threads so
      // that there are threads left over for SSD background write.
      executor_ = std::make_unique<folly::IOThreadPoolExecutor>(20);
    }
    return executor_.get();
  }

  void clearAllocations(std::deque<memory::Allocation>& allocations) {
    while (!allocations.empty()) {
      allocator_->freeNonContiguous(allocations.front());
      allocations.pop_front();
    }
  }

  std::shared_ptr<exec::test::TempDirectoryPath> tempDirectory_;
  std::unique_ptr<memory::MemoryManager> manager_;
  memory::MemoryAllocator* allocator_;
  std::shared_ptr<AsyncDataCache> cache_;
  std::vector<StringIdLease> filenames_;
  std::unique_ptr<folly::IOThreadPoolExecutor> executor_;
  int32_t numLargeRetries_{0};
  std::atomic_int64_t numPendingLoads_{0};
};

class TestingCoalescedLoad : public CoalescedLoad {
 public:
  TestingCoalescedLoad(
      std::vector<RawFileCacheKey> keys,
      std::vector<int32_t> sizes,
      const std::shared_ptr<AsyncDataCache>& cache,
      bool injectError)
      : CoalescedLoad(std::move(keys), std::move(sizes)),
        cache_(cache),
        injectError_(injectError) {}

  std::vector<CachePin> loadData(bool /*isPrefetch*/) override {
    std::vector<CachePin> pins;
    cache_->makePins(
        keys_,
        [&](int32_t index) { return sizes_[index]; },
        [&](int32_t /*index*/, CachePin pin) {
          pins.push_back(std::move(pin));
        });
    for (const auto& pin : pins) {
      auto& buffer = pin.entry()->data();
      AsyncDataCacheTest::initializeContents(
          pin.entry()->key().offset + pin.entry()->key().fileNum.id(), buffer);
    }
    VELOX_CHECK(!injectError_, "Testing error");
    return pins;
  }

  int64_t size() const override {
    int64_t sum = 0;
    for (auto& request : requests_) {
      sum += request.size;
    }
    return sum;
  }

 protected:
  const std::shared_ptr<AsyncDataCache> cache_;
  const std::vector<Request> requests_;
  const bool injectError_{false};
};

class TestingCoalescedSsdLoad : public TestingCoalescedLoad {
 public:
  TestingCoalescedSsdLoad(
      std::vector<RawFileCacheKey> keys,
      std::vector<int32_t> sizes,
      std::vector<SsdPin> ssdPins,
      const std::shared_ptr<AsyncDataCache>& cache,
      bool injectError)
      : TestingCoalescedLoad(
            std::move(keys),
            std::move(sizes),
            cache,
            injectError),
        ssdPins_(std::move(ssdPins)) {}

  std::vector<CachePin> loadData(bool /*isPrefetch*/) override {
    const auto fileNum = keys_[0].fileNum;
    auto& file = cache_->ssdCache()->file(fileNum);
    std::vector<CachePin> pins;
    std::vector<SsdPin> toLoad;
    // We make pins for the new load but leave out the entries that may have
    // been loaded between constructing 'this' and now.
    cache_->makePins(
        keys_,
        [&](int32_t index) { return sizes_[index]; },
        [&](int32_t index, CachePin pin) {
          pins.push_back(std::move(pin));
          toLoad.push_back(std::move(ssdPins_[index]));
        });

    // This is an illustration of discarding SSD entries that could not be read.
    try {
      file.load(toLoad, pins);
      VELOX_CHECK(!injectError_, "Testing error");
    } catch (std::exception& e) {
      try {
        for (const auto& ssdPin : toLoad) {
          file.erase(RawFileCacheKey{fileNum, ssdPin.run().offset()});
        }
      } catch (const std::exception& e2) {
        // Ignore error.
      }
      throw;
    }
    return pins;
  }

 private:
  std::vector<SsdPin> ssdPins_;
};

namespace {
int64_t sizeAtOffset(int64_t offset) {
  return offset % 100'000;
}
} // namespace

void AsyncDataCacheTest::loadOne(
    uint64_t fileNum,
    Request& request,
    bool injectError) {
  // Pattern for loading one pin.
  RawFileCacheKey key{fileNum, request.offset};
  for (;;) {
    folly::SemiFuture<bool> loadFuture(false);
    auto pin = cache_->findOrCreate(key, request.size, &loadFuture);
    if (pin.empty()) {
      // The pin was exclusive on another thread. Wait until it is no longer so
      // and retry.
      auto& exec = folly::QueuedImmediateExecutor::instance();
      std::move(loadFuture).via(&exec).wait();
      continue;
    }
    auto* entry = pin.checkedEntry();
    if (entry->isShared()) {
      // Already in RAM. Check the data.
      checkContents(*entry);
      VELOX_CHECK(!injectError, "Testing error");
      return;
    }
    // We have an uninitialized entry in exclusive mode. We fill it with data
    // and set it to shared. If we release this pin while still in exclusive
    // mode, the entry will be erased.
    if (cache_->ssdCache() != nullptr) {
      auto& ssdFile = cache_->ssdCache()->file(key.fileNum);
      auto ssdPin = ssdFile.find(key);
      if (!ssdPin.empty()) {
        std::vector<CachePin> pins;
        std::vector<SsdPin> ssdPins;
        // pin is exclusive and not copiable, so std::move.
        pins.push_back(std::move(pin));
        ssdPins.push_back(std::move(ssdPin));
        ssdFile.load(ssdPins, pins);
        entry->setExclusiveToShared();
        return;
      }
    }
    // Load from storage.
    initializeContents(
        entry->key().offset + entry->key().fileNum.id(), entry->data());
    entry->setExclusiveToShared();
    return;
  }
}

void AsyncDataCacheTest::loadBatch(
    uint64_t fileNum,
    std::vector<Request>& requests,
    bool injectError,
    Semaphore* semaphore) {
  // Pattern for loading a set of buffers from a file: Divide the requested
  // ranges between already loaded and loadable from storage.
  std::vector<Request*> fromStorage;
  std::vector<Request*> fromSsd;
  for (auto& request : requests) {
    RawFileCacheKey key{fileNum, request.offset};
    if (cache_->exists(key)) {
      continue;
    }
    // Schedule a CoalescedLoad with other keys that need loading from the same
    // source.
    if (cache_->ssdCache() != nullptr) {
      auto& file = cache_->ssdCache()->file(key.fileNum);
      request.ssdPin = file.find(key);
      if (!request.ssdPin.empty()) {
        fromSsd.push_back(&request);
        continue;
      }
    }
    fromStorage.push_back(&request);
  }

  // Make CoalescedLoads for pins from different sources.
  if (!fromStorage.empty()) {
    std::vector<RawFileCacheKey> keys;
    std::vector<int32_t> sizes;
    for (auto request : fromStorage) {
      keys.push_back(RawFileCacheKey{fileNum, request->offset});
      sizes.push_back(request->size);
    }
    auto load = std::make_shared<TestingCoalescedLoad>(
        std::move(keys), std::move(sizes), cache_, injectError);
    ++numPendingLoads_;
    executor()->add([this, load, semaphore]() {
      SCOPE_EXIT {
        --numPendingLoads_;
      };
      try {
        load->loadOrFuture(nullptr);
      } catch (const std::exception& e) {
        // Expecting error, ignore.
      };
      if (semaphore) {
        semaphore->release();
      }
    });
  } else if (semaphore) {
    semaphore->release();
  }

  if (!fromSsd.empty()) {
    std::vector<SsdPin> ssdPins;
    std::vector<RawFileCacheKey> keys;
    std::vector<int32_t> sizes;
    for (auto* request : fromSsd) {
      keys.push_back(RawFileCacheKey{fileNum, request->offset});
      sizes.push_back(request->size);
      ssdPins.push_back(std::move(request->ssdPin));
    }
    auto load = std::make_shared<TestingCoalescedSsdLoad>(
        std::move(keys),
        std::move(sizes),
        std::move(ssdPins),
        cache_,
        injectError);
    ++numPendingLoads_;
    executor()->add([this, load, semaphore]() {
      SCOPE_EXIT {
        --numPendingLoads_;
      };
      try {
        load->loadOrFuture(nullptr);
      } catch (const std::exception& e) {
        // Expecting error, ignore.
      };
      if (semaphore) {
        semaphore->release();
      }
    });
  } else if (semaphore) {
    semaphore->release();
  }
}

void AsyncDataCacheTest::loadNFiles(
    int32_t numFiles,
    std::vector<int64_t> offsets) {
  Semaphore semaphore(0);

  std::vector<Request> batch;
  int32_t numLoads = 0;
  for (auto file = 0; file < numFiles; ++file) {
    auto fileNum = filenames_[file].id();
    if (auto instance = CacheTTLController::getInstance()) {
      instance->addOpenFileInfo(fileNum);
    }
    for (auto i = 0; i < offsets.size() - 1; i++) {
      batch.emplace_back(offsets[i], offsets[i + 1] - offsets[i]);
      if (batch.size() == 8 || i == (offsets.size() - 2)) {
        loadBatch(fileNum, batch, false, &semaphore);
        batch.clear();
        numLoads +=
            2; // One TestingCoalescedSsdLoad and one TestingCoalescedLoad.
      }
    }
  }

  for (auto i = 0; i < numLoads; i++) {
    semaphore.acquire();
  }
}

void AsyncDataCacheTest::loadLoop(
    int64_t startOffset,
    int64_t loadBytes,
    int32_t errorEveryNBatches,
    int32_t largeEveryNBatches,
    int32_t largeAllocSize) {
  const int64_t maxOffset =
      std::max<int64_t>(100'000, (startOffset + loadBytes) / filenames_.size());
  int64_t skippedBytes = 0;
  int32_t errorCounter = 0;
  int32_t largeCounter = 0;
  std::vector<Request> batch;
  for (auto i = 0; i < filenames_.size(); ++i) {
    const auto fileNum = filenames_[i].id();
    for (uint64_t offset = 100; offset < maxOffset;
         offset += sizeAtOffset(offset)) {
      const auto size = sizeAtOffset(offset);
      if (skippedBytes < startOffset) {
        skippedBytes += size;
        continue;
      }

      batch.emplace_back(offset, size);
      if (batch.size() >= 8) {
        for (;;) {
          if (largeEveryNBatches > 0 &&
              largeCounter++ % largeEveryNBatches == 0) {
            // Many threads will allocate a single large chunk at the
            // same time. Some are expected to fail. All will
            // eventually succeed because whoever gets the allocation
            // frees it soon and without deadlocking with others..
            memory::ContiguousAllocation large;
            // Explicitly free 'large' on exit. Do not use MemoryPool for that
            // because we test the allocator's limits, not the pool/memory
            // manager  limits.
            auto guard =
                folly::makeGuard([&]() { allocator_->freeContiguous(large); });
            while (!allocator_->allocateContiguous(
                memory::AllocationTraits::numPages(largeAllocSize),
                nullptr,
                large)) {
              ++numLargeRetries_;
            }
            std::this_thread::sleep_for(
                std::chrono::microseconds(2000)); // NOLINT
          }
          const bool injectError = (errorEveryNBatches > 0) &&
              (++errorCounter % errorEveryNBatches == 0);
          loadBatch(fileNum, batch, injectError);
          try {
            checkBatch(fileNum, batch, injectError);
          } catch (std::exception& e) {
            continue;
          }
          batch.clear();
          break;
        }
      }
    }
  }
}

TEST_P(AsyncDataCacheTest, pin) {
  constexpr int64_t kSize = 25000;
  initializeCache(1 << 20);
  auto& exec = folly::QueuedImmediateExecutor::instance();

  StringIdLease file(fileIds(), std::string_view("testingfile"));
  uint64_t offset = 1000;
  folly::SemiFuture<bool> wait(false);
  RawFileCacheKey key{file.id(), offset};
  auto pin = cache_->findOrCreate(key, kSize, &wait);
  EXPECT_FALSE(pin.empty());
  EXPECT_TRUE(wait.isReady());
  EXPECT_TRUE(pin.entry()->isExclusive());
  pin.entry()->setPrefetch();
  EXPECT_LE(kSize, pin.entry()->data().byteSize());
  EXPECT_LT(0, cache_->incrementPrefetchPages(0));
  auto stats = cache_->refreshStats();
  EXPECT_EQ(1, stats.numExclusive);
  EXPECT_LE(kSize, stats.largeSize);

  CachePin otherPin;
  EXPECT_THROW(otherPin = pin, VeloxException);
  EXPECT_TRUE(otherPin.empty());

  // Second reference to an exclusive entry.
  otherPin = cache_->findOrCreate(key, kSize, &wait);
  EXPECT_FALSE(wait.isReady());
  EXPECT_TRUE(otherPin.empty());
  bool noLongerExclusive = false;
  std::move(wait).via(&exec).thenValue([&](bool) { noLongerExclusive = true; });
  initializeContents(key.fileNum + key.offset, pin.checkedEntry()->data());
  pin.checkedEntry()->setExclusiveToShared();
  pin.clear();
  EXPECT_TRUE(pin.empty());

  EXPECT_TRUE(noLongerExclusive);

  pin = cache_->findOrCreate(key, kSize, &wait);
  EXPECT_TRUE(pin.entry()->isShared());
  EXPECT_TRUE(pin.entry()->getAndClearFirstUseFlag());
  EXPECT_FALSE(pin.entry()->getAndClearFirstUseFlag());
  checkContents(*pin.entry());
  otherPin = pin;
  EXPECT_EQ(2, pin.entry()->numPins());
  EXPECT_FALSE(pin.entry()->isPrefetch());
  auto largerPin = cache_->findOrCreate(key, kSize * 2, &wait);
  // We expect a new uninitialized entry with a larger size to displace the
  // previous one.

  EXPECT_TRUE(largerPin.checkedEntry()->isExclusive());
  largerPin.checkedEntry()->setExclusiveToShared();
  largerPin.clear();
  pin.clear();
  otherPin.clear();
  stats = cache_->refreshStats();
  EXPECT_LE(kSize * 2, stats.largeSize);
  EXPECT_EQ(1, stats.numEntries);
  EXPECT_EQ(0, stats.numShared);
  EXPECT_EQ(0, stats.numExclusive);

  cache_->testingClear();
  stats = cache_->refreshStats();
  EXPECT_EQ(0, stats.largeSize);
  EXPECT_EQ(0, stats.numEntries);
  EXPECT_EQ(0, cache_->incrementPrefetchPages(0));
}

TEST_P(AsyncDataCacheTest, replace) {
  constexpr int64_t kMaxBytes = 64 << 20;
  FLAGS_velox_exception_user_stacktrace_enabled = false;
  initializeCache(kMaxBytes);
  // Load 10x the max size, inject an error every 21 batches.
  loadLoop(0, kMaxBytes * 10, 21);
  if (executor_) {
    executor_->join();
  }
  auto stats = cache_->refreshStats();
  EXPECT_LT(0, stats.numHit);
  EXPECT_LT(0, stats.hitBytes);
  EXPECT_LT(0, stats.numEvict);
  EXPECT_GE(
      kMaxBytes / memory::AllocationTraits::kPageSize,
      cache_->incrementCachedPages(0));
}

TEST_P(AsyncDataCacheTest, evictAccounting) {
  constexpr int64_t kMaxBytes = 64 << 20;
  FLAGS_velox_exception_user_stacktrace_enabled = false;
  initializeCache(kMaxBytes);
  auto pool = manager_->addLeafPool("test");

  // We make allocations that we exchange for larger ones later. This will evict
  // cache. We check that the evictions are not counted on the pool even if they
  // occur as a result of action on the pool.
  memory::Allocation allocation;
  memory::ContiguousAllocation large;
  pool->allocateNonContiguous(1200, allocation);
  pool->allocateContiguous(1200, large);
  EXPECT_EQ(memory::AllocationTraits::pageBytes(2400), pool->usedBytes());
  loadLoop(0, kMaxBytes * 1.1);
  waitForPendingLoads();
  pool->allocateNonContiguous(2400, allocation);
  pool->allocateContiguous(2400, large);
  EXPECT_EQ(memory::AllocationTraits::pageBytes(4800), pool->usedBytes());
  auto stats = cache_->refreshStats();
  EXPECT_LT(0, stats.numEvict);
}

TEST_P(AsyncDataCacheTest, largeEvict) {
  constexpr int64_t kMaxBytes = 256 << 20;
  constexpr int32_t kNumThreads = 24;
  FLAGS_velox_exception_user_stacktrace_enabled = false;
  initializeCache(kMaxBytes);
  // Load 10x the max size, inject an allocation of 1/8 the capacity every 4
  // batches.
  runThreads(kNumThreads, [&](int32_t /*i*/) {
    loadLoop(0, kMaxBytes * 1.2, 0, 1, kMaxBytes / 4);
  });
  if (executor_) {
    executor_->join();
  }
  auto stats = cache_->refreshStats();
  EXPECT_LT(0, stats.numEvict);
  EXPECT_GE(
      kMaxBytes / memory::AllocationTraits::kPageSize,
      cache_->incrementCachedPages(0));
  LOG(INFO) << "Reties after failed evict: " << numLargeRetries_;
}

TEST_P(AsyncDataCacheTest, outOfCapacity) {
  const int64_t kMaxBytes = 64
      << 20; // 64MB as MmapAllocator's min size is 64MB
  const int32_t kSize = 16 << 10;
  const int32_t kSizeInPages = memory::AllocationTraits::numPages(kSize);
  std::deque<CachePin> pins;
  std::deque<memory::Allocation> allocations;
  initializeCache(kMaxBytes);
  // We pin 2 16K entries and unpin 1. Eventually the whole capacity
  // is pinned and we fail making a ew entry.

  uint64_t offset = 0;
  for (;;) {
    pins.push_back(newEntry(++offset, kSize));
    pins.push_back(newEntry(++offset, kSize));
    if (pins.back().empty()) {
      break;
    }
    pins.pop_front();
  }
  memory::Allocation allocation;
  ASSERT_FALSE(allocator_->allocateNonContiguous(kSizeInPages, allocation));
  // One 4 page entry below the max size of 4K 4 page entries in 16MB of
  // capacity.
  ASSERT_EQ(16384, cache_->incrementCachedPages(0));
  ASSERT_EQ(16384, cache_->incrementPrefetchPages(0));
  pins.clear();

  // We allocate the full capacity and expect the cache entries to go.
  for (;;) {
    if (!allocator_->allocateNonContiguous(kSizeInPages, allocation)) {
      break;
    }
    allocations.push_back(std::move(allocation));
  }
  EXPECT_EQ(0, cache_->incrementCachedPages(0));
  EXPECT_EQ(0, cache_->incrementPrefetchPages(0));
  EXPECT_EQ(16384, allocator_->numAllocated());
  clearAllocations(allocations);
}

namespace {
// Cuts off the last 1/10th of file at 'path'.
void corruptFile(const std::string& path) {
  const auto fd = ::open(path.c_str(), O_WRONLY);
  const auto size = ::lseek(fd, 0, SEEK_END);
  const auto rc = ftruncate(fd, size / 10 * 9);
  ASSERT_EQ(rc, 0);
}
} // namespace

TEST_P(AsyncDataCacheTest, DISABLED_ssd) {
#ifdef TSAN_BUILD
  // NOTE: scale down the test data set to prevent tsan tester from running out
  // of memory.
  constexpr uint64_t kRamBytes = 16 << 20;
  constexpr uint64_t kSsdBytes = 256UL << 20;
#else
  constexpr uint64_t kRamBytes = 32 << 20;
  constexpr uint64_t kSsdBytes = 512UL << 20;
#endif
  FLAGS_velox_exception_user_stacktrace_enabled = false;
  initializeCache(kRamBytes, kSsdBytes);
  cache_->setVerifyHook(
      [&](const AsyncDataCacheEntry& entry) { checkContents(entry); });

  // Read back all writes. This increases the chance of writes falling behind
  // new entry creation.
  FLAGS_ssd_verify_write = true;

  // We read kSsdBytes worth of data on 16 threads. The same data will be hit by
  // all threads. The expectation is that most of the data ends up on SSD. All
  // data may not get written if reading is faster than writing. Error out once
  // every 11 load batches.
  //
  // Note that executor() must have more threads so that background
  // write does not wait for the workload.
  runThreads(16, [&](int32_t /*i*/) { loadLoop(0, kSsdBytes, 11); });
  LOG(INFO) << "Stats after first pass: " << cache_->toString();
  auto ssdStats = cache_->ssdCache()->stats();
  ASSERT_LE(kRamBytes, ssdStats.bytesWritten);

  // We allow writes to proceed faster.
  FLAGS_ssd_verify_write = false;
  // We read the data back. The verify hook checks correct values. Error every
  // 13 batch loads.
  runThreads(16, [&](int32_t /*i*/) { loadLoop(0, kSsdBytes, 13); });
  LOG(INFO) << "Stats after second pass:" << cache_->toString();
  ssdStats = cache_->ssdCache()->stats();
  ASSERT_LE(kRamBytes, ssdStats.bytesRead);

  // We re-read the second half and add another half capacity of new entries. We
  // expect some of the oldest entries to get evicted. Error every 17 batch
  // loads.
  runThreads(
      16, [&](int32_t /*i*/) { loadLoop(kSsdBytes / 2, kSsdBytes * 1.5, 17); });
  LOG(INFO) << "Stats after third pass:" << cache_->toString();

  // Wait for writes to finish and make a checkpoint.
  cache_->ssdCache()->shutdown();
  auto ssdStatsAfterShutdown = cache_->ssdCache()->stats();
  ASSERT_GT(ssdStatsAfterShutdown.bytesWritten, ssdStats.bytesWritten);
  ASSERT_GT(ssdStatsAfterShutdown.bytesRead, ssdStats.bytesRead);

  // Check that no pins are leaked.
  ASSERT_EQ(ssdStatsAfterShutdown.numPins, 0);

  auto ramStats = cache_->refreshStats();
  ASSERT_EQ(ramStats.numShared, 0);
  ASSERT_EQ(ramStats.numExclusive, 0);

  cache_->ssdCache()->testingClear();
  // We cut the tail off one of the cache shards.
  corruptFile(fmt::format("{}/cache0.cpt", tempDirectory_->getPath()));
  // We open the cache from checkpoint. Reading checks the data integrity, here
  // we check that more data was read than written.
  initializeCache(kRamBytes, kSsdBytes);
  runThreads(16, [&](int32_t /*i*/) {
    loadLoop(kSsdBytes / 2, kSsdBytes * 1.5, 113);
  });
  LOG(INFO) << "State after starting 3/4 shards from checkpoint: "
            << cache_->toString();
  const auto ssdStatsFromCP = cache_->ssdCache()->stats();
  ASSERT_EQ(ssdStatsFromCP.readCheckpointErrors, 1);
}

TEST_P(AsyncDataCacheTest, invalidSsdPath) {
  auto testPath = "hdfs:/test/prefix_";
  uint64_t ssdBytes = 256UL << 20;
  SsdCache::Config config(testPath, ssdBytes, 4, executor(), ssdBytes / 20);
  VELOX_ASSERT_THROW(
      SsdCache(config),
      fmt::format(
          "Ssd path '{}' does not start with '/' that points to local file system.",
          testPath));
}

TEST_P(AsyncDataCacheTest, cacheStats) {
  CacheStats stats;
  stats.tinySize = 234;
  stats.largeSize = 1024;
  stats.tinyPadding = 23;
  stats.largePadding = 1344;
  stats.numEntries = 100;
  stats.numExclusive = 20;
  stats.numShared = 30;
  stats.sharedPinnedBytes = 10 << 20;
  stats.exclusivePinnedBytes = 10 << 20;
  stats.numEmptyEntries = 20;
  stats.numPrefetch = 30;
  stats.prefetchBytes = 100;
  stats.numHit = 46;
  stats.hitBytes = 1374;
  stats.numNew = 2041;
  stats.numEvict = 463;
  stats.numEvictChecks = 348;
  stats.numWaitExclusive = 244;
  stats.numAgedOut = 10;
  stats.allocClocks = 1320;
  stats.sumEvictScore = 123;
  ASSERT_EQ(
      stats.toString(),
      "Cache size: 2.56KB tinySize: 257B large size: 2.31KB\n"
      "Cache entries: 100 read pins: 30 write pins: 20 pinned shared: 10.00MB pinned exclusive: 10.00MB\n"
      " num write wait: 244 empty entries: 20\n"
      "Cache access miss: 2041 hit: 46 hit bytes: 1.34KB eviction: 463 eviction checks: 348 aged out: 10\n"
      "Prefetch entries: 30 bytes: 100B\n"
      "Alloc Megaclocks 0");

  constexpr uint64_t kRamBytes = 32 << 20;
  constexpr uint64_t kSsdBytes = 512UL << 20;
  initializeCache(kRamBytes, kSsdBytes);
  const std::string expectedDetailedCacheOutput =
      "AsyncDataCache:\n"
      "Cache size: 0B tinySize: 0B large size: 0B\n"
      "Cache entries: 0 read pins: 0 write pins: 0 pinned shared: 0B pinned exclusive: 0B\n"
      " num write wait: 0 empty entries: 0\n"
      "Cache access miss: 0 hit: 0 hit bytes: 0B eviction: 0 eviction checks: 0 aged out: 0\n"
      "Prefetch entries: 0 bytes: 0B\n"
      "Alloc Megaclocks 0\n"
      "Allocated pages: 0 cached pages: 0\n"
      "Backing: Memory Allocator[MMAP total capacity 64.00MB free capacity 64.00MB allocated pages 0 mapped pages 0 external mapped pages 0\n"
      "[size 1: 0(0MB) allocated 0 mapped]\n"
      "[size 2: 0(0MB) allocated 0 mapped]\n"
      "[size 4: 0(0MB) allocated 0 mapped]\n"
      "[size 8: 0(0MB) allocated 0 mapped]\n"
      "[size 16: 0(0MB) allocated 0 mapped]\n"
      "[size 32: 0(0MB) allocated 0 mapped]\n"
      "[size 64: 0(0MB) allocated 0 mapped]\n"
      "[size 128: 0(0MB) allocated 0 mapped]\n"
      "[size 256: 0(0MB) allocated 0 mapped]\n"
      "]\n"
      "SSD: Ssd cache IO: Write 0B read 0B Size 512.00MB Occupied 0B 0K entries.\n"
      "GroupStats: <dummy FileGroupStats>";
  ASSERT_EQ(cache_->toString(), expectedDetailedCacheOutput);
  ASSERT_EQ(cache_->toString(true), expectedDetailedCacheOutput);
  const std::string expectedShortCacheOutput =
      "AsyncDataCache:\n"
      "Cache size: 0B tinySize: 0B large size: 0B\n"
      "Cache entries: 0 read pins: 0 write pins: 0 pinned shared: 0B pinned exclusive: 0B\n"
      " num write wait: 0 empty entries: 0\n"
      "Cache access miss: 0 hit: 0 hit bytes: 0B eviction: 0 eviction checks: 0 aged out: 0\n"
      "Prefetch entries: 0 bytes: 0B\n"
      "Alloc Megaclocks 0\n"
      "Allocated pages: 0 cached pages: 0\n";
  ASSERT_EQ(cache_->toString(false), expectedShortCacheOutput);
}

TEST_P(AsyncDataCacheTest, shrinkCache) {
  constexpr uint64_t kRamBytes = 128UL << 20;
  constexpr uint64_t kSsdBytes = 512UL << 20;
  constexpr int kTinyDataSize = AsyncDataCacheEntry::kTinyDataSize - 1;
  const int numEntries{10};
  constexpr int kLargeDataSize = kTinyDataSize * 2;
  ASSERT_LE(numEntries * (kTinyDataSize + kLargeDataSize), kRamBytes);

  std::vector<RawFileCacheKey> tinyCacheKeys;
  std::vector<RawFileCacheKey> largeCacheKeys;
  std::vector<StringIdLease> fileLeases;
  for (int i = 0; i < numEntries; ++i) {
    fileLeases.emplace_back(
        StringIdLease(fileIds(), fmt::format("shrinkCacheFile{}", i)));
    tinyCacheKeys.emplace_back(RawFileCacheKey{fileLeases.back().id(), 0});
    largeCacheKeys.emplace_back(
        RawFileCacheKey{fileLeases.back().id(), kLargeDataSize});
  }

  struct {
    bool shrinkAll;
    bool hasSsd;
    bool releaseAll;

    std::string debugString() const {
      return fmt::format(
          "shrinkAll {}, hasSsd {}, releaseAll {}",
          shrinkAll,
          hasSsd,
          releaseAll);
    }
  } testSettings[] = {
      {true, false, false},
      {true, true, false},
      {true, false, true},
      {true, true, true},
      {false, false, true},
      {false, true, true},
      {false, false, false},
      {false, true, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    initializeCache(kRamBytes, testData.hasSsd ? kSsdBytes : 0);
    std::vector<CachePin> pins;
    for (int i = 0; i < numEntries; ++i) {
      auto tinyPin = cache_->findOrCreate(tinyCacheKeys[i], kTinyDataSize);
      ASSERT_FALSE(tinyPin.empty());
      ASSERT_TRUE(tinyPin.entry()->tinyData() != nullptr);
      ASSERT_TRUE(tinyPin.entry()->data().empty());
      ASSERT_FALSE(tinyPin.entry()->isPrefetch());
      ASSERT_FALSE(tinyPin.entry()->ssdSaveable());
      pins.push_back(std::move(tinyPin));
      auto largePin = cache_->findOrCreate(largeCacheKeys[i], kLargeDataSize);
      ASSERT_FALSE(largePin.entry()->tinyData() != nullptr);
      ASSERT_FALSE(largePin.entry()->data().empty());
      ASSERT_FALSE(largePin.entry()->isPrefetch());
      ASSERT_FALSE(largePin.entry()->ssdSaveable());
      pins.push_back(std::move(largePin));
    }
    auto stats = cache_->refreshStats();
    ASSERT_EQ(stats.numEntries, numEntries * 2);
    ASSERT_EQ(stats.numEmptyEntries, 0);
    ASSERT_EQ(stats.numExclusive, numEntries * 2);
    ASSERT_EQ(stats.numEvict, 0);
    ASSERT_EQ(stats.numHit, 0);
    ASSERT_EQ(stats.tinySize, kTinyDataSize * numEntries);
    ASSERT_EQ(stats.largeSize, kLargeDataSize * numEntries);
    ASSERT_EQ(stats.sharedPinnedBytes, 0);
    ASSERT_GE(
        stats.exclusivePinnedBytes,
        (kTinyDataSize + kLargeDataSize) * numEntries);
    ASSERT_EQ(stats.prefetchBytes, 0);
    ASSERT_EQ(stats.numPrefetch, 0);

    const auto numMappedPagesBeforeShrink = allocator_->numMapped();
    ASSERT_GT(numMappedPagesBeforeShrink, 0);

    // Everything gets pinged in memory.
    VELOX_ASSERT_THROW(cache_->shrink(0), "");
    ASSERT_EQ(cache_->shrink(testData.shrinkAll ? kRamBytes : 1), 0);

    if (!testData.releaseAll) {
      for (auto& pin : pins) {
        pin.entry()->setExclusiveToShared();
      }
      pins.clear();
      if (testData.shrinkAll) {
        ASSERT_GE(
            cache_->shrink(kRamBytes),
            (kLargeDataSize + kTinyDataSize) * numEntries);
      } else {
        ASSERT_GE(cache_->shrink(2 * kTinyDataSize), kTinyDataSize);
      }
    } else {
      pins.clear();
      // We expect everything has been freed.
      ASSERT_EQ(
          cache_->shrink(testData.shrinkAll ? kRamBytes : 2 * kTinyDataSize),
          0);
    }
    stats = cache_->refreshStats();
    const auto numMappedPagesAfterShrink = allocator_->numMapped();
    if (testData.shrinkAll || testData.releaseAll) {
      ASSERT_EQ(stats.numEntries, 0);
      ASSERT_EQ(stats.numEmptyEntries, 2 * numEntries);
      ASSERT_EQ(stats.numExclusive, 0);
      ASSERT_EQ(stats.numEvict, 2 * numEntries);
      ASSERT_EQ(stats.numHit, 0);
      ASSERT_EQ(stats.tinySize, 0);
      ASSERT_EQ(stats.largeSize, 0);
      ASSERT_EQ(stats.sharedPinnedBytes, 0);
      ASSERT_GE(stats.exclusivePinnedBytes, 0);
      ASSERT_EQ(stats.prefetchBytes, 0);
      ASSERT_EQ(stats.numPrefetch, 0);
      if (testData.shrinkAll) {
        ASSERT_EQ(numMappedPagesAfterShrink, 0);
      } else {
        ASSERT_LT(numMappedPagesAfterShrink, numMappedPagesBeforeShrink);
      }
    } else {
      ASSERT_LT(stats.numEntries, 2 * numEntries);
      ASSERT_GT(stats.numEntries, 0);
      ASSERT_GE(stats.numEmptyEntries, 1);
      ASSERT_EQ(stats.numExclusive, 0);
      ASSERT_GE(stats.numEvict, 1);
      ASSERT_EQ(stats.numHit, 0);
      ASSERT_GT(stats.tinySize, 0);
      ASSERT_GT(stats.largeSize, 0);
      ASSERT_EQ(stats.sharedPinnedBytes, 0);
      ASSERT_GE(stats.exclusivePinnedBytes, 0);
      ASSERT_EQ(stats.prefetchBytes, 0);
      ASSERT_EQ(stats.numPrefetch, 0);
      ASSERT_LT(numMappedPagesAfterShrink, numMappedPagesBeforeShrink);
    }
  }
}

TEST_P(AsyncDataCacheTest, shutdown) {
  constexpr uint64_t kRamBytes = 16 << 20;
  constexpr uint64_t kSsdBytes = 64UL << 20;

  for (const auto asyncShutdown : {false, true}) {
    SCOPED_TRACE(fmt::format("asyncShutdown {}", asyncShutdown));
    // Initialize cache with a big checkpointIntervalBytes, giving eviction log
    // chance to survive.
    initializeCache(kRamBytes, kSsdBytes, kSsdBytes * 10);
    ASSERT_EQ(cache_->ssdCache()->stats().openCheckpointErrors, 4);

    // Write large amount of data, making sure eviction is triggered and the log
    // file is not zero.
    loadLoop(0, 16 * kSsdBytes);
    ASSERT_GT(cache_->ssdCache()->stats().regionsEvicted, 0);
    ASSERT_GT(cache_->ssdCache()->testingTotalLogEvictionFilesSize(), 0);

    // Shutdown cache.
    if (!asyncShutdown) {
      waitForSsdWriteToFinish(cache_->ssdCache());
    }
    const uint64_t bytesWrittenBeforeShutdown =
        cache_->ssdCache()->stats().bytesWritten;
    cache_->ssdCache()->shutdown();
    const uint64_t bytesWrittenAfterShutdown =
        cache_->ssdCache()->stats().bytesWritten;

    if (asyncShutdown) {
      // The written bytes before shutdown is not larger than before shutdown.
      ASSERT_LE(bytesWrittenBeforeShutdown, bytesWrittenAfterShutdown);
    } else {
      // No new data has been written after shutdown.
      ASSERT_EQ(bytesWrittenBeforeShutdown, bytesWrittenAfterShutdown);
    }
    // Eviction log files have been truncated.
    ASSERT_EQ(cache_->ssdCache()->testingTotalLogEvictionFilesSize(), 0);

    // Shutdown again making sure no issue is triggered.
    cache_->ssdCache()->shutdown();

    // New cache write attempt is blocked and triggers exception.
    VELOX_ASSERT_THROW(
        cache_->ssdCache()->startWrite(),
        "Unexpected write after SSD cache has been shutdown");

    // Re-initialize cache.
    cache_->ssdCache()->testingClear();
    initializeCache(kRamBytes, kSsdBytes, kSsdBytes * 10);
    // Checkpoint files are intact and readable.
    ASSERT_EQ(cache_->ssdCache()->stats().openCheckpointErrors, 0);
    ASSERT_EQ(cache_->ssdCache()->stats().readCheckpointErrors, 0);
    cache_->ssdCache()->testingDeleteCheckpoints();
  }
}

DEBUG_ONLY_TEST_P(AsyncDataCacheTest, shrinkWithSsdWrite) {
  constexpr uint64_t kRamBytes = 128UL << 20;
  constexpr uint64_t kSsdBytes = 512UL << 20;
  constexpr int kDataSize = 4096;
  initializeCache(kRamBytes, kSsdBytes);
  const int numEntries{10};
  std::vector<CachePin> cachePins;
  uint64_t offset = 0;
  for (int i = 0; i < numEntries; ++i) {
    cachePins.push_back(newEntry(offset, kDataSize));
    offset += kDataSize;
  }
  for (auto& pin : cachePins) {
    pin.entry()->setExclusiveToShared();
  }

  std::atomic_bool writeStartFlag{false};
  folly::EventCount writeStartWait;
  std::atomic_bool writeWaitFlag{true};
  folly::EventCount writeWait;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::cache::SsdCache::write",
      std::function<void(const SsdCache*)>(([&](const SsdCache* cache) {
        writeStartFlag = true;
        writeStartWait.notifyAll();
        writeWait.await([&]() { return !writeWaitFlag.load(); });
      })));

  // Starts a write thread running at background.
  std::thread ssdWriteThread([&]() {
    cache_->ssdCache()->startWrite();
    cache_->saveToSsd();
  });

  // Wait for the write thread to start, and block it while do cache shrink.
  writeStartWait.await([&]() { return writeStartFlag.load(); });
  ASSERT_TRUE(cache_->ssdCache()->writeInProgress());

  cachePins.clear();
  cache_->shrink(kRamBytes);
  auto stats = cache_->refreshStats();
  // Shrink can only reclaim some entries but not all as some of the cache
  // entries have been pickup for ssd write which is not evictable.
  ASSERT_LT(stats.numEntries, numEntries);
  ASSERT_GT(stats.numEmptyEntries, 0);
  ASSERT_GT(stats.numEvict, 0);
  ASSERT_GT(stats.numShared, 0);
  ASSERT_EQ(stats.numExclusive, 0);
  ASSERT_EQ(stats.numWaitExclusive, 0);

  // Wait for write to complete.
  writeWaitFlag = false;
  writeWait.notifyAll();
  ssdWriteThread.join();
  waitForSsdWriteToFinish(cache_->ssdCache());

  stats = cache_->refreshStats();
  ASSERT_GT(stats.numEntries, stats.numEmptyEntries);

  ASSERT_GT(cache_->shrink(kRamBytes), 0);
  stats = cache_->refreshStats();
  ASSERT_EQ(stats.numEntries, 0);
  ASSERT_EQ(stats.numEmptyEntries, numEntries);
}

DEBUG_ONLY_TEST_P(AsyncDataCacheTest, ttl) {
  constexpr uint64_t kRamBytes = 32 << 20;
  constexpr uint64_t kSsdBytes = 128UL << 20;

  initializeCache(kRamBytes, kSsdBytes);
  CacheTTLController::create(*cache_);

  std::vector<int64_t> offsets(32);
  std::generate(offsets.begin(), offsets.end(), [&, n = 0]() mutable {
    return n += (kRamBytes / kNumFiles / offsets.size());
  });

  ScopedTestTime stt;
  auto loadTime1 = getCurrentTimeSec();
  auto loadTime2 = loadTime1 + 100;

  stt.setCurrentTestTimeSec(loadTime1);
  loadNFiles(filenames_.size() * 2 / 3, offsets);
  waitForSsdWriteToFinish(cache_->ssdCache());
  auto statsT1 = cache_->refreshStats();

  stt.setCurrentTestTimeSec(loadTime2);
  loadNFiles(filenames_.size(), offsets);

  runThreads(2, [&](int32_t /*i*/) {
    CacheTTLController::getInstance()->applyTTL(
        getCurrentTimeSec() - loadTime1 - 2);
  });

  auto statsTtl = cache_->refreshStats();
  EXPECT_EQ(statsTtl.numAgedOut, statsT1.numEntries);
  EXPECT_EQ(statsTtl.ssdStats->entriesAgedOut, statsT1.ssdStats->entriesCached);
}

TEST_P(AsyncDataCacheTest, makeEvictable) {
  constexpr uint64_t kRamBytes = 128UL << 20;
  constexpr uint64_t kSsdBytes = 512UL << 20;
  constexpr int kDataSize = 4096;
  for (const bool evictable : {false, true}) {
    SCOPED_TRACE(fmt::format("evictable: {}", evictable));
    initializeCache(kRamBytes, kSsdBytes);
    const int numEntries{10};
    std::vector<CachePin> cachePins;
    uint64_t offset = 0;
    for (int i = 0; i < numEntries; ++i) {
      cachePins.push_back(newEntry(offset, kDataSize));
      offset += kDataSize;
    }
    for (auto& pin : cachePins) {
      pin.entry()->setExclusiveToShared(!evictable);
    }
    if (evictable) {
      std::vector<RawFileCacheKey> keys;
      keys.reserve(cachePins.size());
      for (const auto& pin : cachePins) {
        keys.push_back(RawFileCacheKey{
            pin.checkedEntry()->key().fileNum.id(),
            pin.checkedEntry()->key().offset});
      }
      cachePins.clear();
      for (const auto& key : keys) {
        cache_->makeEvictable(key);
      }
    }
    const auto cacheEntries = cache_->testingCacheEntries();
    for (const auto& cacheEntry : cacheEntries) {
      ASSERT_EQ(cacheEntry->ssdSaveable(), !evictable);
      ASSERT_EQ(cacheEntry->testingAccessStats().numUses, 0);
      if (evictable) {
        ASSERT_EQ(cacheEntry->testingAccessStats().lastUse, 0);
      } else {
        ASSERT_NE(cacheEntry->testingAccessStats().lastUse, 0);
      }
    }
    auto* ssdCache = cache_->ssdCache();
    if (ssdCache == nullptr) {
      continue;
    }
    ssdCache->testingWaitForWriteToFinish();
    if (evictable) {
      ASSERT_EQ(ssdCache->stats().entriesCached, 0);
    } else {
      if (cache_->testingSsdSavable() == 0) {
        ASSERT_GT(ssdCache->stats().entriesCached, 0);
      } else {
        // Ssd write only gets triggered after a certain ssd space usage
        // threshold.
        ASSERT_GE(ssdCache->stats().entriesCached, 0);
      }
    }
  }
}

TEST_P(AsyncDataCacheTest, ssdWriteOptions) {
  constexpr uint64_t kRamBytes = 16UL << 20; // 16 MB
  constexpr uint64_t kSsdBytes = 64UL << 20; // 64 MB

  // Test if ssd write behavior with different settings.
  struct {
    double maxWriteRatio;
    double ssdSavableRatio;
    int32_t minSsdSavableBytes;
    bool expectedSaveToSsd;

    std::string debugString() const {
      return fmt::format(
          "maxWriteRatio {}, ssdSavableRatio {}, minSsdSavableBytes {}, expectedSaveToSsd {}",
          maxWriteRatio,
          ssdSavableRatio,
          minSsdSavableBytes,
          expectedSaveToSsd);
    }
  } testSettings[] = {
      {0.8, 0.95, 32UL << 20, false},
      {0.8, 0.95, 4UL << 20, false},
      {0.8, 0.3, 32UL << 20, false},
      {0.8, 0.3, 4UL << 20, true},
      {0.001, 0.3, 4UL << 20, true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    initializeCache(
        kRamBytes,
        kSsdBytes,
        0,
        {testData.maxWriteRatio,
         testData.ssdSavableRatio,
         testData.minSsdSavableBytes});
    // Load data half of the in-memory capacity.
    loadLoop(0, kRamBytes / 2);
    waitForPendingLoads();
    auto stats = cache_->refreshStats();
    if (testData.expectedSaveToSsd) {
      EXPECT_GE(stats.ssdStats->entriesWritten, 0);
    } else {
      EXPECT_EQ(stats.ssdStats->entriesWritten, 0);
    }
    if (testData.maxWriteRatio < 0.005) {
      // SSD cache write rate is so small that it stops right after the
      // first entry in each shard.
      EXPECT_EQ(stats.ssdStats->entriesWritten, 4);
    }
  }
}

// TODO: add concurrent fuzzer test.

INSTANTIATE_TEST_SUITE_P(
    AsyncDataCacheTest,
    AsyncDataCacheTest,
    ::testing::ValuesIn(AsyncDataCacheTest::getTestParams()));
