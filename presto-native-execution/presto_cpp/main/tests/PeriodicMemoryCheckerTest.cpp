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
#include <gtest/gtest.h>
#include "velox/common/base/VeloxException.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/memory/Memory.h"

using namespace facebook::velox;
using namespace facebook::presto;

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
          systemUsedMemoryBytes_(systemUsedMemoryBytes),
          mallocBytes_(mallocBytes),
          periodicCb_(std::move(periodicCb)),
          heapDumpCb_(std::move(heapDumpCb)) {}

    void setMallocBytes(int64_t mallocBytes) {
      mallocBytes_ = mallocBytes;
    }

   protected:
    int64_t systemUsedMemoryBytes() const override {
      return systemUsedMemoryBytes_;
    }

    int64_t mallocBytes() const override {
      return mallocBytes_;
    }

    void periodicCb() const override {
      if (periodicCb_) {
        periodicCb_();
      }
    }

    bool heapDumpCb(const std::string& filePath) const {
      if (heapDumpCb_) {
        return heapDumpCb_(filePath);
      }
      return false;
    }

   private:
    int64_t systemUsedMemoryBytes_{0};
    int64_t mallocBytes_{0};
    std::function<void()> periodicCb_;
    std::function<bool(const std::string&)> heapDumpCb_;
  };
};

TEST_F(PeriodicMemoryCheckerTest, basic) {
  // Default config
  ASSERT_NO_THROW(TestPeriodicMemoryChecker(PeriodicMemoryChecker::Config{}));

  ASSERT_NO_THROW(TestPeriodicMemoryChecker(PeriodicMemoryChecker::Config{
      1, true, 1024, 32, true, 5, "/path/to/dir", "prefix", 5, 512}));
  ASSERT_THROW(
      TestPeriodicMemoryChecker(PeriodicMemoryChecker::Config{
          1, true, 0, 32, true, 5, "/path/to/dir", "prefix", 5, 512}),
      VeloxException);
  ASSERT_THROW(
      TestPeriodicMemoryChecker(PeriodicMemoryChecker::Config{
          1, true, 1024, 32, true, 5, "", "prefix", 5, 512}),
      VeloxException);
  ASSERT_THROW(
      TestPeriodicMemoryChecker(PeriodicMemoryChecker::Config{
          1, true, 1024, 32, true, 5, "/path/to/dir", "", 5, 512}),
      VeloxException);
  TestPeriodicMemoryChecker memChecker(PeriodicMemoryChecker::Config{
      1, false, 0, 0, false, 5, "/path/to/dir", "prefix", 5, 512});
  ASSERT_NO_THROW(memChecker.start());
  ASSERT_THROW(memChecker.start(), VeloxException);
  ASSERT_NO_THROW(memChecker.stop());
}

TEST_F(PeriodicMemoryCheckerTest, periodicCb) {
  auto testPeriodicCb = [](bool pushbackEnabled, bool heapDumpEnabled) {
    std::atomic_bool periodicCbInvoked{false};
    TestPeriodicMemoryChecker memChecker(
        PeriodicMemoryChecker::Config{
            1,
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

TEST_F(PeriodicMemoryCheckerTest, heapdump) {
  // Malloc bytes less than dump threshold. Expect no dump trigger.
  {
    std::atomic_bool heapdumpCbCalled{false};
    TestPeriodicMemoryChecker memChecker(
        PeriodicMemoryChecker::Config{
            1, false, 0, 0, true, 5, "/path/to/dir", "prefix", 5, 512},
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
            1, false, 0, 0, true, 1, "/path/to/dir", "prefix", 2, 512},
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
  memory::MemoryManagerOptions options;
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
    auto& allocation = cachePin.entry()->data();
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
          1,
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
}
