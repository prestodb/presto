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

#include "presto_cpp/main/PeriodicMemoryChecker.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/caching/StringIdMap.h"
#include "velox/common/memory/Memory.h"

using namespace facebook::velox;

namespace facebook::presto {
class PeriodicMemoryCheckerTest : public testing::Test {
 protected:
  class TestPeriodicMemoryChecker : public PeriodicMemoryChecker {
   public:
    explicit TestPeriodicMemoryChecker(
        PeriodicMemoryChecker::Config config,
        int64_t systemUsedMemoryBytes = 0,
        int64_t mallocBytes = 0,
        std::function<void()>&& periodicCb = nullptr,
        std::function<bool(const std::string&)>&& heapDumpCb = nullptr)
        : PeriodicMemoryChecker(config),
          mallocBytes_(mallocBytes),
          periodicCb_(std::move(periodicCb)),
          heapDumpCb_(std::move(heapDumpCb)) {
      cachedSystemUsedMemoryBytes_.store(
          systemUsedMemoryBytes, std::memory_order_relaxed);
    }

    ~TestPeriodicMemoryChecker() override = default;

    void setMallocBytes(int64_t mallocBytes) {
      mallocBytes_ = mallocBytes;
    }

    void setLoadSystemMemoryUsageCb(std::function<int64_t()> callback) {
      loadSystemMemoryUsageCb_ = std::move(callback);
    }

    void setPushbackMemoryCb(std::function<void()> callback) {
      pushbackMemoryCb_ = std::move(callback);
    }

    // Runs the base-class pushback directly, bypassing the schedulers, to
    // regression-test the trigger-revalidation logic deterministically.
    void invokeBasePushbackForTest() {
      PeriodicMemoryChecker::pushbackMemory();
    }

   protected:
    void loadSystemMemoryUsage() override {
      if (loadSystemMemoryUsageCb_) {
        cachedSystemUsedMemoryBytes_.store(
            loadSystemMemoryUsageCb_(), std::memory_order_relaxed);
      }
    }

    int64_t mallocBytes() const override {
      return mallocBytes_;
    }

    void periodicCb() override {
      if (periodicCb_) {
        periodicCb_();
      }
    }

    bool heapDumpCb(const std::string& filePath) const override {
      if (heapDumpCb_) {
        return heapDumpCb_(filePath);
      }
      return false;
    }

    void removeDumpFile(const std::string& filePath) const override {}

    void pushbackMemory() override {
      if (pushbackMemoryCb_) {
        pushbackMemoryCb_();
      } else {
        PeriodicMemoryChecker::pushbackMemory();
      }
    }

   private:
    int64_t mallocBytes_{0};
    std::function<void()> periodicCb_;
    std::function<bool(const std::string&)> heapDumpCb_;
    std::function<int64_t()> loadSystemMemoryUsageCb_;
    std::function<void()> pushbackMemoryCb_;
  };
};

TEST_F(PeriodicMemoryCheckerTest, basic) {
  // Default config
  ASSERT_NO_THROW(TestPeriodicMemoryChecker(PeriodicMemoryChecker::Config{}));

  ASSERT_NO_THROW(TestPeriodicMemoryChecker(
      PeriodicMemoryChecker::Config{
          1'000, true, 1024, 32, true, 5, "/path/to/dir", "prefix", 5, 512}));
  VELOX_ASSERT_THROW(
      TestPeriodicMemoryChecker(
          PeriodicMemoryChecker::Config{
              1'000, true, 0, 32, true, 5, "/path/to/dir", "prefix", 5, 512}),
      "(0 vs. 0)");
  VELOX_ASSERT_THROW(
      TestPeriodicMemoryChecker(
          PeriodicMemoryChecker::Config{
              1'000, true, 1024, 32, true, 5, "", "prefix", 5, 512}),
      "heapDumpLogDir cannot be empty when heap dump is enabled.");
  VELOX_ASSERT_THROW(
      TestPeriodicMemoryChecker(
          PeriodicMemoryChecker::Config{
              1'000, true, 1024, 32, true, 5, "/path/to/dir", "", 5, 512}),
      "heapDumpFilePrefix cannot be empty when heap dump is enabled.");
  TestPeriodicMemoryChecker memChecker(
      PeriodicMemoryChecker::Config{
          1'000, false, 0, 0, false, 5, "/path/to/dir", "prefix", 5, 512});
  ASSERT_NO_THROW(memChecker.start());
  VELOX_ASSERT_THROW(memChecker.start(), "start() called more than once");
  ASSERT_NO_THROW(memChecker.stop());
}

TEST_F(PeriodicMemoryCheckerTest, periodicCb) {
  auto testPeriodicCb = [](bool pushbackEnabled, bool heapDumpEnabled) {
    std::atomic_bool periodicCbInvoked{false};
    TestPeriodicMemoryChecker memChecker(
        PeriodicMemoryChecker::Config{
            1'000,
            pushbackEnabled,
            512,
            32,
            heapDumpEnabled,
            5,
            "/path/to/dir",
            "prefix",
            5,
            512},
        pushbackEnabled ? 768 : 256,
        128,
        [&]() { periodicCbInvoked = true; });
    memChecker.start();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    memChecker.stop();
    ASSERT_TRUE(periodicCbInvoked.load());
  };
  testPeriodicCb(true, true);
  testPeriodicCb(true, false);
  testPeriodicCb(false, true);
  testPeriodicCb(false, false);
}

TEST_F(PeriodicMemoryCheckerTest, samplingContinuesWhilePushbackIsBlocked) {
  const auto numSamples = std::make_shared<std::atomic<int32_t>>(0);
  const auto pushbackStarted = std::make_shared<std::atomic_bool>(false);
  const auto unblockPushback = std::make_shared<std::promise<void>>();
  const std::shared_future<void> unblockPushbackFuture =
      unblockPushback->get_future().share();

  PeriodicMemoryChecker::Config config;
  config.memoryCheckerIntervalMs = 10;
  config.systemMemPushbackEnabled = true;
  config.systemMemLimitBytes = 2;
  config.systemMemShrinkBytes = 1;
  TestPeriodicMemoryChecker memoryChecker(config, 0);
  memoryChecker.setLoadSystemMemoryUsageCb([numSamples]() noexcept {
    numSamples->fetch_add(1, std::memory_order_relaxed);
    return 3;
  });
  memoryChecker.setPushbackMemoryCb([pushbackStarted, unblockPushbackFuture]() {
    pushbackStarted->store(true, std::memory_order_release);
    unblockPushbackFuture.wait();
  });

  memoryChecker.start();
  const std::chrono::steady_clock::time_point pushbackDeadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{1};
  while (!pushbackStarted->load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < pushbackDeadline) {
    std::this_thread::yield();
  }
  const bool pushbackObserved =
      pushbackStarted->load(std::memory_order_acquire);
  const int32_t numSamplesAtPushbackStart =
      numSamples->load(std::memory_order_relaxed);
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{1};
  while (numSamples->load(std::memory_order_relaxed) <=
             numSamplesAtPushbackStart &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  const bool samplingContinued =
      numSamples->load(std::memory_order_relaxed) > numSamplesAtPushbackStart;
  unblockPushback->set_value();
  memoryChecker.stop();

  ASSERT_TRUE(pushbackObserved);
  EXPECT_TRUE(samplingContinued);
}

TEST_F(PeriodicMemoryCheckerTest, heapdump) {
  // Malloc bytes less than dump threshold. Expect no dump trigger.
  {
    std::atomic_bool heapdumpCbCalled{false};
    TestPeriodicMemoryChecker memChecker(
        PeriodicMemoryChecker::Config{
            1'000, false, 0, 0, true, 5, "/path/to/dir", "prefix", 5, 512},
        1024,
        256,
        []() {},
        [&](const std::string& filePath) {
          heapdumpCbCalled = true;
          return false;
        });
    memChecker.start();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    memChecker.stop();
    ASSERT_FALSE(heapdumpCbCalled.load());
  }

  // Dump file count greater than max allowed, while malloc size is smaller than
  // current smallest dump. Expect no dump trigger.
  {
    std::atomic_bool heapdumpCbCalled{false};
    TestPeriodicMemoryChecker memChecker(
        PeriodicMemoryChecker::Config{
            1'000, false, 0, 0, true, 1, "/path/to/dir", "prefix", 2, 512},
        1024,
        768,
        []() {},
        [&](const std::string& filePath) {
          heapdumpCbCalled = true;
          return true;
        });
    memChecker.start();
    std::this_thread::sleep_for(std::chrono::seconds(3));
    memChecker.setMallocBytes(513);
    heapdumpCbCalled = false;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_FALSE(heapdumpCbCalled.load());
    memChecker.stop();
  }
}

TEST_F(PeriodicMemoryCheckerTest, pushbackMemory) {
  memory::MemoryManager::Options options;
  options.allocatorCapacity = 32L << 20;
  memory::MemoryManager::testingSetInstance(options);
  auto asyncDataCache =
      cache::AsyncDataCache::create(memory::memoryManager()->allocator());
  cache::AsyncDataCache::setInstance(asyncDataCache.get());
  StringIdLease stringIdLease(fileIds(), "cache_file_name");

  // Create a cache and drop the pin to make evictable memory
  {
    auto cachePin = cache::AsyncDataCache::getInstance()->findOrCreate(
        {stringIdLease.id(), 0}, 32L << 20);
    auto& allocation = cachePin.entry()->nonContiguousData();
    for (int32_t i = 0; i < allocation.numRuns(); ++i) {
      memory::Allocation::PageRun run = allocation.runAt(i);
      int64_t* ptr = reinterpret_cast<int64_t*>(run.data());
      std::memset(ptr, 'x', run.numBytes());
    }
    cachePin.entry()->setExclusiveToShared();
  }
  ASSERT_EQ(memory::memoryManager()->getTotalBytes(), 32L << 20);

  TestPeriodicMemoryChecker memChecker(
      PeriodicMemoryChecker::Config{
          1'000,
          true,
          16L << 20,
          8L << 20,
          false,
          1,
          "/path/to/dir",
          "prefix",
          2,
          512},
      32L << 20,
      0,
      []() {},
      [&](const std::string& filePath) { return true; });
  memChecker.start();
  std::this_thread::sleep_for(std::chrono::seconds(2));
  memChecker.stop();
  ASSERT_EQ(memory::memoryManager()->getTotalBytes(), 0);

  // Shutdown global memory setups
  asyncDataCache->shutdown();
  cache::AsyncDataCache::setInstance(nullptr);
  memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
}

TEST_F(PeriodicMemoryCheckerTest, pushbackNoopOnStaleTrigger) {
  // A stale scheduler trigger (cached usage dropped below the pushback target
  // after the guard fired) must no-op instead of tripping a fatal CHECK.
  TestPeriodicMemoryChecker belowTarget(
      PeriodicMemoryChecker::Config{
          1'000, true, 100, 10, false, 5, "/path/to/dir", "prefix", 5, 512},
      /*systemUsedMemoryBytes=*/50);
  ASSERT_NO_THROW(belowTarget.invokeBasePushbackForTest());

  VELOX_ASSERT_THROW(
      TestPeriodicMemoryChecker(
          PeriodicMemoryChecker::Config{
              1'000,
              true,
              100,
              200,
              false,
              5,
              "/path/to/dir",
              "prefix",
              5,
              512}),
      "systemMemShrinkBytes must be less than systemMemLimitBytes");
}
} // namespace facebook::presto
