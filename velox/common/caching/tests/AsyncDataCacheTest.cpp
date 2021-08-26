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

#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileIds.h"

#include <folly/executors/QueuedImmediateExecutor.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::cache;

using facebook::velox::memory::MappedMemory;

class AsyncDataCacheTest : public testing::Test {
 protected:
  static constexpr int32_t kNumFiles = 100;
  void initializeCache(int64_t maxBytes) {
    cache_ =
        std::make_shared<AsyncDataCache>(MappedMemory::getInstance(), maxBytes);
    for (auto i = 0; i < kNumFiles; ++i) {
      auto name = fmt::format("testing_file_{}", i);
      filenames_.push_back(StringIdLease(fileIds(), name));
    }
  }

  // Loads a sequence of entries from a number of files. Looks up a
  // number of entries, then loads the ones that nobody else is
  // loading.
  void loadLoop();

 public:
  // Deterministically fills 'allocation'  based on 'sequence'
  static void initializeContents(
      int64_t sequence,
      MappedMemory::Allocation& alloc) {
    bool first = true;
    for (int32_t i = 0; i < alloc.numRuns(); ++i) {
      MappedMemory::PageRun run = alloc.runAt(i);
      void** ptr = reinterpret_cast<void**>(run.data());
      int32_t numWords =
          run.numPages() * MappedMemory::kPageSize / sizeof(void*);
      for (int32_t offset = 0; offset < numWords; offset++) {
        if (first) {
          ptr[offset] = reinterpret_cast<void*>(sequence);
          first = false;
        } else {
          ptr[offset] = ptr + offset + sequence;
        }
      }
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

 protected:
  // Checks that the contents are consistent with what is set in
  // initializeContents.
  static void checkContents(MappedMemory::Allocation& alloc) {
    bool first = true;
    long sequence;
    for (int32_t i = 0; i < alloc.numRuns(); ++i) {
      MappedMemory::PageRun run = alloc.runAt(i);
      void** ptr = reinterpret_cast<void**>(run.data());
      int32_t numWords =
          run.numPages() * MappedMemory::kPageSize / sizeof(void*);
      for (int32_t offset = 0; offset < numWords; offset++) {
        if (first) {
          sequence = reinterpret_cast<long>(ptr[offset]);
          first = false;
        } else {
          ASSERT_EQ(ptr[offset], ptr + offset + sequence);
        }
      }
    }
  }

  std::shared_ptr<AsyncDataCache> cache_;
  std::vector<StringIdLease> filenames_;
};

class TestingFusedLoad : public FusedLoad {
 public:
  void loadData() override {
    for (auto& pin : pins_) {
      auto& buffer = pin.entry()->data();
      AsyncDataCacheTest::initializeContents(
          pin.entry()->key().offset + pin.entry()->key().fileNum.id(), buffer);
      pin.entry()->setValid();
    }
  }
};

void AsyncDataCacheTest::loadLoop() {
  constexpr int32_t kBatch = 8;
  std::vector<CachePin> batch;
  for (auto file = 0; file < filenames_.size(); ++file) {
    for (uint64_t offset = 100; offset < 10000000; offset += 100000) {
      RawFileCacheKey key{filenames_[file].id(), offset};
      for (;;) {
        folly::SemiFuture<bool> wait(false);
        auto pin = cache_->findOrCreate(key, offset % 1000000, &wait);
        if (pin.empty()) {
          // Another thread has the pin as exclusive.
          auto& exec = folly::QueuedImmediateExecutor::instance();
          std::move(wait).via(&exec).wait();
          continue;
        }
        if (pin.entry()->isExclusive()) {
          // Entry is new. This thread has it as exclusive.
          batch.push_back(std::move(pin));
          if (batch.size() < kBatch) {
            break; // will load when we have enough entries for a batch.
          }
          auto load = std::make_shared<TestingFusedLoad>();
          load->initialize(std::move(batch));
          // The batch is now loadable. Re-get so it loads.
          continue;
        } else {
          // the pin is in shared mode. Ensure the data is loaded.
          pin.entry()->ensureLoaded(true);
          break;
        }
      }
    }
  }
  // Drop possibly unloaded exclusive entries.
  batch.clear();
}

TEST_F(AsyncDataCacheTest, pin) {
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

  std::vector<CachePin> pins;
  pins.push_back(std::move(pin));
  EXPECT_TRUE(pin.empty());

  auto load = std::make_shared<TestingFusedLoad>();
  load->initialize(std::move(pins));
  EXPECT_TRUE(noLongerExclusive);

  pin.clear();
  pin = cache_->findOrCreate(key, kSize, &wait);
  EXPECT_FALSE(pin.entry()->dataValid());
  EXPECT_TRUE(pin.entry()->isShared());
  pin.entry()->ensureLoaded(true);
  checkContents(pin.entry()->data());
  otherPin = pin;
  EXPECT_EQ(2, pin.entry()->numPins());
  EXPECT_FALSE(pin.entry()->isPrefetch());
  pin.entry()->setValid(false);
  pin.clear();
  otherPin.clear();

  // Since the pins were cleared with the data not valid, the entry is expected
  // to be gone.
  stats = cache_->refreshStats();
  EXPECT_EQ(0, stats.largeSize);
  EXPECT_EQ(0, stats.numEntries);
  EXPECT_EQ(0, cache_->incrementPrefetchPages(0));
}

TEST_F(AsyncDataCacheTest, replace) {
  constexpr int64_t kMaxBytes = 16 << 20;
  initializeCache(kMaxBytes);
  loadLoop();
  auto stats = cache_->refreshStats();
  EXPECT_LT(0, stats.numEvict);
  EXPECT_GE(
      kMaxBytes / memory::MappedMemory::kPageSize,
      cache_->incrementCachedPages(0));
}

TEST_F(AsyncDataCacheTest, outOfCapacity) {
  constexpr int64_t kMaxBytes = 16 << 20;
  constexpr int32_t kSize = 16 << 10;
  constexpr int32_t kSizeInPages = kSize / MappedMemory::kPageSize;
  std::deque<CachePin> pins;
  std::deque<MappedMemory::Allocation> allocations;
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
  MappedMemory::Allocation allocation(cache_.get());
  EXPECT_FALSE(cache_->allocate(kSizeInPages, 0, allocation));
  // One 4 page entry below the max size of 4K 4 page entries in 16MB of
  // capacity.
  EXPECT_EQ(4092, cache_->incrementCachedPages(0));
  EXPECT_EQ(4092, cache_->incrementPrefetchPages(0));
  pins.clear();

  // We allocate the full capacity and expect the cache entries to go.
  for (;;) {
    MappedMemory::Allocation(cache_.get());
    if (!cache_->allocate(kSizeInPages, 0, allocation)) {
      break;
    }
    allocations.push_back(std::move(allocation));
  }
  EXPECT_EQ(0, cache_->incrementCachedPages(0));
  EXPECT_EQ(0, cache_->incrementPrefetchPages(0));
  EXPECT_EQ(4092, cache_->numAllocated());
}
