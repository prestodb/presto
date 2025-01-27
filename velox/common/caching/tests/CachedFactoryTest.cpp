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

#include "velox/common/caching/CachedFactory.h"

#include "folly/Random.h"
#include "folly/executors/EDFThreadPoolExecutor.h"
#include "folly/executors/thread_factory/NamedThreadFactory.h"
#include "folly/synchronization/Latch.h"
#include "gtest/gtest.h"
#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::velox;

namespace {

struct DoublerGenerator {
  std::unique_ptr<int> operator()(
      const int& value,
      const void* properties = nullptr) {
    ++generated;
    return std::make_unique<int>(value * 2);
  }
  std::atomic<int> generated = 0;
};

struct IdentityGenerator {
  std::unique_ptr<int> operator()(
      const int& value,
      const void* properties = nullptr) {
    return std::make_unique<int>(value);
  }
};
} // namespace

TEST(CachedFactoryTest, basicGeneration) {
  auto generator = std::make_unique<DoublerGenerator>();
  auto* generated = &generator->generated;
  CachedFactory<int, int, DoublerGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(1000), std::move(generator));
  ASSERT_EQ(factory.maxSize(), 1000);
  ASSERT_EQ(factory.currentSize(), 0);

  {
    auto val1 = factory.generate(1);
    ASSERT_EQ(*val1, 2);
    ASSERT_EQ(*generated, 1);
    ASSERT_FALSE(val1.fromCache());
    auto val2 = factory.generate(1);
    ASSERT_EQ(*val2, 2);
    ASSERT_EQ(*generated, 1);
    ASSERT_TRUE(val2.fromCache());
    ASSERT_EQ(factory.currentSize(), 1);
    ASSERT_EQ(factory.cacheStats().pinnedSize, 1);
  }
  ASSERT_EQ(factory.cacheStats().pinnedSize, 0);

  {
    auto val3 = factory.generate(1);
    ASSERT_EQ(*val3, 2);
    ASSERT_EQ(*generated, 1);
    ASSERT_TRUE(val3.fromCache());
    auto val4 = factory.generate(2);
    ASSERT_EQ(*val4, 4);
    ASSERT_EQ(*generated, 2);
    ASSERT_FALSE(val4.fromCache());
    auto val5 = factory.generate(3);
    ASSERT_EQ(*val5, 6);
    ASSERT_EQ(*generated, 3);
    ASSERT_FALSE(val5.fromCache());
    ASSERT_EQ(factory.currentSize(), 3);
    ASSERT_EQ(factory.cacheStats().pinnedSize, 3);
  }
  ASSERT_EQ(factory.cacheStats().pinnedSize, 0);

  {
    auto val6 = factory.generate(1);
    ASSERT_EQ(*val6, 2);
    ASSERT_EQ(*generated, 3);
    ASSERT_TRUE(val6.fromCache());
    auto val7 = factory.generate(4);
    ASSERT_EQ(*val7, 8);
    ASSERT_EQ(*generated, 4);
    ASSERT_FALSE(val7.fromCache());
    auto val8 = factory.generate(3);
    ASSERT_EQ(*val8, 6);
    ASSERT_EQ(*generated, 4);
    ASSERT_TRUE(val8.fromCache());
    ASSERT_EQ(factory.currentSize(), 4);
    ASSERT_EQ(factory.cacheStats().pinnedSize, 3);
  }
  ASSERT_EQ(factory.cacheStats().pinnedSize, 0);

  factory.clearCache();
  ASSERT_EQ(factory.currentSize(), 0);
  ASSERT_EQ(factory.cacheStats().curSize, 0);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 0);
}

struct DoublerWithExceptionsGenerator {
  std::unique_ptr<int> operator()(
      const int& value,
      const void* properties = nullptr) {
    if (value == 3) {
      VELOX_FAIL("3 is bad");
    }
    ++generated;
    return std::make_unique<int>(value * 2);
  }
  int generated = 0;
};

TEST(CachedFactoryTest, clearCache) {
  auto generator = std::make_unique<DoublerGenerator>();
  CachedFactory<int, int, DoublerGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(1000), std::move(generator));
  ASSERT_EQ(factory.maxSize(), 1000);
  {
    auto val1 = factory.generate(1);
    ASSERT_FALSE(val1.fromCache());
  }

  factory.clearCache();
  ASSERT_EQ(factory.currentSize(), 0);

  ASSERT_FALSE(factory.generate(1).fromCache());
  auto cachedValue = factory.generate(1);
  ASSERT_TRUE(cachedValue.fromCache());
  ASSERT_FALSE(factory.generate(2).fromCache());
  ASSERT_EQ(factory.cacheStats().pinnedSize, 1);
  ASSERT_EQ(factory.cacheStats().curSize, 2);

  factory.clearCache();
  ASSERT_EQ(factory.currentSize(), 1);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 1);

  cachedValue.testingClear();
  ASSERT_EQ(factory.currentSize(), 1);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 0);

  factory.clearCache();
  ASSERT_EQ(factory.currentSize(), 0);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 0);
}

TEST(CachedFactoryTest, basicExceptionHandling) {
  auto generator = std::make_unique<DoublerWithExceptionsGenerator>();
  int* generated = &generator->generated;
  CachedFactory<int, int, DoublerWithExceptionsGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(1000), std::move(generator));
  auto val1 = factory.generate(1);
  ASSERT_EQ(*val1, 2);
  ASSERT_EQ(*generated, 1);
  VELOX_ASSERT_THROW(factory.generate(3), "3 is bad");

  val1 = factory.generate(4);
  ASSERT_EQ(*val1, 8);
  ASSERT_EQ(*generated, 2);
  val1 = factory.generate(1);
  ASSERT_EQ(*val1, 2);
  ASSERT_EQ(*generated, 2);
}

TEST(CachedFactoryTest, multiThreadedGeneration) {
  auto generator = std::make_unique<DoublerGenerator>();
  auto* generated = &generator->generated;
  CachedFactory<int, int, DoublerGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(1000), std::move(generator));
  folly::EDFThreadPoolExecutor pool(
      100, std::make_shared<folly::NamedThreadFactory>("test_pool"));
  const int numValues = 5;
  const int requestsPerValue = 10;
  folly::Latch latch(numValues * requestsPerValue);
  for (int i = 0; i < requestsPerValue; ++i) {
    for (int j = 0; j < numValues; ++j) {
      pool.add([&, j]() {
        auto value = factory.generate(j);
        CHECK_EQ(*value, 2 * j);
        latch.count_down();
      });
    }
  }
  latch.wait();
  ASSERT_EQ(*generated, numValues);
}

// Same as above, but we keep the returned CachedPtrs till the end of the
// function.
TEST(CachedFactoryTest, multiThreadedGenerationAgain) {
  auto generator = std::make_unique<DoublerGenerator>();
  auto* generated = &generator->generated;
  CachedFactory<int, int, DoublerGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(1000), std::move(generator));
  folly::EDFThreadPoolExecutor pool(
      100, std::make_shared<folly::NamedThreadFactory>("test_pool"));
  const int numValues = 5;
  const int requestsPerValue = 10;
  std::vector<CachedPtr<int, int>> cachedValues(numValues * requestsPerValue);
  folly::Latch latch(numValues * requestsPerValue);
  for (int i = 0; i < requestsPerValue; i++) {
    for (int j = 0; j < numValues; j++) {
      pool.add([&factory, &latch, &cachedValues, i, j]() {
        cachedValues[i * numValues + j] = factory.generate(j);
        latch.count_down();
      });
    }
  }
  latch.wait();
  ASSERT_EQ(*generated, numValues);
  for (int i = 0; i < requestsPerValue; i++) {
    for (int j = 0; j < numValues; j++) {
      ASSERT_EQ(*cachedValues[i * numValues + j], 2 * j);
    }
  }
}

TEST(CachedFactoryTest, lruCacheEviction) {
  auto generator = std::make_unique<DoublerGenerator>();
  CachedFactory<int, int, DoublerGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(3), std::move(generator));
  ASSERT_EQ(factory.maxSize(), 3);
  ASSERT_EQ(factory.currentSize(), 0);

  auto val1 = factory.generate(1);
  ASSERT_FALSE(val1.fromCache());
  ASSERT_TRUE(val1.cached());
  auto val2 = factory.generate(2);
  ASSERT_FALSE(val2.fromCache());
  ASSERT_TRUE(val2.cached());
  auto val3 = factory.generate(3);
  ASSERT_FALSE(val3.fromCache());
  ASSERT_TRUE(val3.cached());
  ASSERT_EQ(factory.currentSize(), 3);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 3);
  auto val4 = factory.generate(4);
  ASSERT_FALSE(val4.fromCache());
  ASSERT_FALSE(val4.cached());

  {
    auto val = factory.generate(4);
    ASSERT_FALSE(val.fromCache());
    ASSERT_FALSE(val.cached());
    val = factory.generate(1);
    ASSERT_TRUE(val.fromCache());
    ASSERT_TRUE(val.cached());
    val = factory.generate(2);
    ASSERT_TRUE(val.fromCache());
    ASSERT_TRUE(val.cached());
    val = factory.generate(3);
    ASSERT_TRUE(val.fromCache());
    ASSERT_TRUE(val.cached());
  }
  {
    auto val = factory.generate(1);
    ASSERT_TRUE(val.fromCache());
    ASSERT_TRUE(val.cached());
  }
  val1.testingClear();
  val2.testingClear();
  val3.testingClear();

  val4 = factory.generate(4);
  ASSERT_FALSE(val4.fromCache());
  ASSERT_TRUE(val4.cached());
  ASSERT_EQ(factory.cacheStats().curSize, 3);
  {
    auto val = factory.generate(4);
    ASSERT_TRUE(val.fromCache());
    ASSERT_TRUE(val.cached());
    val = factory.generate(1);
    ASSERT_TRUE(val.fromCache());
    ASSERT_TRUE(val.cached());
    // Cache entry 2 should be selected for eviction.
    val = factory.generate(2);
    ASSERT_FALSE(val.fromCache());
    ASSERT_TRUE(val.cached());
    // Cache entry 2 insertion caused cache entry 3 eviction.
    val = factory.generate(3);
    ASSERT_FALSE(val.fromCache());
    ASSERT_TRUE(val.cached());
  }
  ASSERT_EQ(factory.currentSize(), 3);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 1);
}

TEST(CachedFactoryTest, cacheExpiration) {
  auto generator = std::make_unique<DoublerGenerator>();
  CachedFactory<int, int, DoublerGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(3, 1'000),
      std::move(generator));
  ASSERT_EQ(factory.maxSize(), 3);
  ASSERT_EQ(factory.currentSize(), 0);

  auto val1 = factory.generate(1);
  ASSERT_FALSE(val1.fromCache());
  ASSERT_TRUE(val1.cached());
  auto val2 = factory.generate(2);
  ASSERT_FALSE(val2.fromCache());
  ASSERT_TRUE(val2.cached());
  auto val3 = factory.generate(3);
  ASSERT_FALSE(val3.fromCache());
  ASSERT_TRUE(val3.cached());
  ASSERT_EQ(factory.currentSize(), 3);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 3);
  auto val4 = factory.generate(4);
  ASSERT_FALSE(val4.fromCache());
  ASSERT_FALSE(val4.cached());

  std::this_thread::sleep_for(std::chrono::milliseconds{1'500});
  ASSERT_EQ(factory.currentSize(), 3);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 3);

  val4 = factory.generate(4);
  ASSERT_FALSE(val4.fromCache());
  ASSERT_FALSE(val4.cached());
  ASSERT_EQ(factory.currentSize(), 3);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 3);

  val1.testingClear();
  ASSERT_EQ(factory.currentSize(), 2);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 2);

  val4 = factory.generate(4);
  ASSERT_FALSE(val4.fromCache());
  ASSERT_TRUE(val4.cached());
  ASSERT_EQ(factory.currentSize(), 3);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 3);

  val2.testingClear();
  val3.testingClear();
  ASSERT_EQ(factory.currentSize(), 1);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 1);
  val4.testingClear();
  ASSERT_EQ(factory.currentSize(), 1);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 0);

  std::this_thread::sleep_for(std::chrono::milliseconds{1'500});

  val1 = factory.generate(1);
  ASSERT_FALSE(val1.fromCache());
  ASSERT_TRUE(val1.cached());
  ASSERT_EQ(factory.currentSize(), 1);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 1);
}

TEST(CachedFactoryTest, retrievedCached) {
  auto generator = std::make_unique<DoublerGenerator>();
  auto* generated = &generator->generated;
  CachedFactory<int, int, DoublerGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(1000), std::move(generator));
  for (int i = 0; i < 10; i += 2) {
    factory.generate(i);
  }
  ASSERT_EQ(*generated, 5);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 0);
  ASSERT_EQ(factory.cacheStats().curSize, 5);

  std::vector<int> keys(10);
  for (int i = 0; i < 10; ++i) {
    keys[i] = i;
    if (i % 2 == 0) {
      ASSERT_EQ(*factory.get(keys[i]), i * 2) << i;
    } else {
      ASSERT_EQ(factory.get(keys[i]).get(), nullptr);
    }
  }
  std::vector<std::pair<int, CachedPtr<int, int>>> cached;
  std::vector<int> missing;
  factory.retrieveCached(keys, cached, missing);
  ASSERT_EQ(cached.size(), 5);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 5);
  ASSERT_EQ(factory.cacheStats().curSize, 5);

  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(cached[i].first, 2 * i);
    ASSERT_EQ(*cached[i].second, 4 * i);
    ASSERT_TRUE(cached[i].second.fromCache());
  }
  ASSERT_EQ(missing.size(), 5);

  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(missing[i], 2 * i + 1);
  }
  ASSERT_EQ(*generated, 5);
}

TEST(CachedFactoryTest, clearCacheWithManyEntries) {
  auto generator = std::make_unique<DoublerGenerator>();
  CachedFactory<int, int, DoublerGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(1000), std::move(generator));
  for (auto i = 0; i < 1000; ++i) {
    factory.generate(i);
  }
  std::vector<int> keys(500);
  for (int i = 0; i < 500; ++i) {
    keys[i] = i;
  }
  {
    std::vector<std::pair<int, CachedPtr<int, int>>> cached;
    std::vector<int> missing;
    factory.retrieveCached(keys, cached, missing);
    ASSERT_EQ(cached.size(), 500);
    auto cacheStats = factory.clearCache();
    ASSERT_EQ(cacheStats.numElements, 500);
    ASSERT_EQ(cacheStats.pinnedSize, 500);
  }
  auto cacheStats = factory.cacheStats();
  ASSERT_EQ(cacheStats.numElements, 500);
  ASSERT_EQ(cacheStats.pinnedSize, 0);

  cacheStats = factory.clearCache();
  ASSERT_EQ(cacheStats.numElements, 0);
  ASSERT_EQ(cacheStats.pinnedSize, 0);
}

TEST(CachedFactoryTest, disableCache) {
  auto generator = std::make_unique<DoublerGenerator>();
  auto* generated = &generator->generated;
  CachedFactory<int, int, DoublerGenerator> factory(std::move(generator));

  auto val1 = factory.generate(1);
  ASSERT_FALSE(val1.fromCache());
  ASSERT_EQ(*generated, 1);
  ASSERT_EQ(factory.currentSize(), 0);
  ASSERT_EQ(factory.cacheStats().curSize, 0);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 0);

  auto val2 = factory.generate(1);
  ASSERT_FALSE(val2.fromCache());
  EXPECT_EQ(*generated, 2);
  ASSERT_EQ(factory.currentSize(), 0);
  ASSERT_EQ(factory.cacheStats().curSize, 0);
  ASSERT_EQ(factory.cacheStats().pinnedSize, 0);
  ASSERT_EQ(factory.cacheStats().expireDurationMs, 0);

  ASSERT_EQ(factory.maxSize(), 0);

  EXPECT_EQ(factory.cacheStats(), SimpleLRUCacheStats{});

  EXPECT_EQ(factory.clearCache(), SimpleLRUCacheStats{});

  std::vector<int> keys(10);
  for (int i = 0; i < 10; ++i) {
    keys[i] = i;
  }

  std::vector<std::pair<int, CachedPtr<int, int>>> cached;
  std::vector<int> missing;
  factory.retrieveCached(keys, cached, missing);
  ASSERT_EQ(cached.size(), 0);
  ASSERT_EQ(missing.size(), 10);
  for (int i = 0; i < 10; ++i) {
    ASSERT_EQ(missing[i], i);
  }
}

TEST(CachedFactoryTest, fuzzer) {
  const int numThreads = 32;
  const int testDurationMs = 5'000;
  const size_t expirationDurationMs = 1;
  for (const bool expireCache : {false, true}) {
    SCOPED_TRACE(fmt::format("expireCache: {}", expireCache));
    auto generator = std::make_unique<IdentityGenerator>();
    CachedFactory<int, int, IdentityGenerator> factory(
        std::make_unique<SimpleLRUCache<int, int>>(
            128, expireCache ? expirationDurationMs : 0),
        std::move(generator));

    std::vector<std::thread> threads;
    threads.reserve(numThreads);
    for (int i = 0; i < numThreads; ++i) {
      threads.emplace_back([&factory, i]() {
        folly::Random::DefaultGenerator rng(23 + i);
        const auto startTimeMs = getCurrentTimeMs();
        while (startTimeMs + testDurationMs > getCurrentTimeMs()) {
          const auto key = folly::Random::rand32(rng) % 256;
          const auto val = factory.generate(key);
          if (val.fromCache()) {
            ASSERT_TRUE(val.cached());
            ASSERT_EQ(*val, key);
          }
          if (folly::Random::oneIn(4)) {
            std::this_thread::sleep_for(std::chrono::microseconds{100});
          }
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    ASSERT_EQ(factory.cacheStats().pinnedSize, 0);
    ASSERT_LE(factory.cacheStats().curSize, 128);
    ASSERT_LE(factory.cacheStats().numElements, 128);
    ASSERT_GT(factory.cacheStats().numHits, 0);
    ASSERT_GT(factory.cacheStats().numLookups, 0);
  }
}
