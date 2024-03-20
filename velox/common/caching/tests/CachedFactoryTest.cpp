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

#include "folly/executors/EDFThreadPoolExecutor.h"
#include "folly/executors/thread_factory/NamedThreadFactory.h"
#include "folly/synchronization/Latch.h"
#include "gtest/gtest.h"

using namespace facebook::velox;
namespace {

struct DoublerGenerator {
  int operator()(const int& value) {
    ++generated_;
    return value * 2;
  }
  std::atomic<int> generated_ = 0;
};

template <typename T>
T getCachedValue(std::pair<bool, T>& value) {
  return value.second;
}

template <typename T>
bool isCached(std::pair<bool, T>& value) {
  return value.first;
}

template <typename T>
std::pair<bool, T> cacheHit(const T& value) {
  return std::make_pair(true, value);
}

template <typename T>
std::pair<bool, T> cacheMiss(const T& value) {
  return std::make_pair(false, value);
}

} // namespace

TEST(CachedFactoryTest, basicGeneration) {
  auto generator = std::make_unique<DoublerGenerator>();
  auto* generated = &generator->generated_;
  CachedFactory<int, int, DoublerGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(1000), std::move(generator));
  EXPECT_EQ(factory.maxSize(), 1000);
  {
    auto val1 = factory.generate(1);
    EXPECT_EQ(val1, cacheMiss(2));
    EXPECT_EQ(*generated, 1);

    auto val2 = factory.generate(1);
    EXPECT_EQ(val2, cacheHit(2));
    EXPECT_EQ(*generated, 1);
    EXPECT_EQ(factory.currentSize(), 1);
  }
  {
    auto val3 = factory.generate(1);
    EXPECT_EQ(val3, cacheHit(2));
    EXPECT_EQ(*generated, 1);

    auto val4 = factory.generate(2);
    EXPECT_EQ(val4, cacheMiss(4));
    EXPECT_EQ(*generated, 2);

    auto val5 = factory.generate(3);
    EXPECT_EQ(val5, cacheMiss(6));
    EXPECT_EQ(*generated, 3);
    EXPECT_EQ(factory.currentSize(), 3);
  }

  auto val6 = factory.generate(1);
  EXPECT_EQ(val6, cacheHit(2));
  EXPECT_EQ(*generated, 3);

  auto val7 = factory.generate(4);
  EXPECT_EQ(val7, cacheMiss(8));
  EXPECT_EQ(*generated, 4);

  auto val8 = factory.generate(3);
  EXPECT_EQ(val8, cacheHit(6));
  EXPECT_EQ(*generated, 4);
  EXPECT_EQ(factory.currentSize(), 4);
}

struct DoublerWithExceptionsGenerator {
  int operator()(const int& value) {
    if (value == 3) {
      throw std::invalid_argument("3 is bad");
    }
    ++generated_;
    return value * 2;
  }
  int generated_ = 0;
};

TEST(CachedFactoryTest, clearCache) {
  auto generator = std::make_unique<DoublerGenerator>();
  CachedFactory<int, int, DoublerGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(1000), std::move(generator));
  EXPECT_EQ(factory.maxSize(), 1000);
  {
    auto val1 = factory.generate(1);
    EXPECT_EQ(val1, cacheMiss(2));
  }

  factory.clearCache();
  EXPECT_EQ(factory.currentSize(), 0);
  EXPECT_EQ(factory.generate(1), cacheMiss(2));
}

TEST(CachedFactoryTest, basicExceptionHandling) {
  auto generator = std::make_unique<DoublerWithExceptionsGenerator>();
  int* generated = &generator->generated_;
  CachedFactory<int, int, DoublerWithExceptionsGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(1000), std::move(generator));
  auto val1 = factory.generate(1);
  EXPECT_EQ(getCachedValue(val1), 2);
  EXPECT_EQ(*generated, 1);
  try {
    auto val2 = factory.generate(3);
    FAIL() << "Factory generation should have failed";
  } catch (const std::invalid_argument&) {
    // Expected.
  }
  val1 = factory.generate(4);
  EXPECT_EQ(getCachedValue(val1), 8);
  EXPECT_EQ(*generated, 2);
  val1 = factory.generate(1);
  EXPECT_EQ(getCachedValue(val1), 2);
  EXPECT_EQ(*generated, 2);
}

TEST(CachedFactoryTest, multiThreadedGeneration) {
  auto generator = std::make_unique<DoublerGenerator>();
  auto* generated = &generator->generated_;
  CachedFactory<int, int, DoublerGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(1000), std::move(generator));
  folly::EDFThreadPoolExecutor pool(
      100, std::make_shared<folly::NamedThreadFactory>("test_pool"));
  const int numValues = 5;
  const int requestsPerValue = 10;
  folly::Latch latch(numValues * requestsPerValue);
  for (int i = 0; i < requestsPerValue; i++) {
    for (int j = 0; j < numValues; j++) {
      pool.add([&, j]() {
        auto value = factory.generate(j);
        EXPECT_EQ(getCachedValue(value), 2 * j);
        latch.count_down();
      });
    }
  }
  latch.wait();
  EXPECT_EQ(*generated, numValues);
}

// Same as above, but we keep the returned CachedPtrs till the end
// of the function.
TEST(CachedFactoryTest, multiThreadedGenerationAgain) {
  auto generator = std::make_unique<DoublerGenerator>();
  auto* generated = &generator->generated_;
  CachedFactory<int, int, DoublerGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(1000), std::move(generator));
  folly::EDFThreadPoolExecutor pool(
      100, std::make_shared<folly::NamedThreadFactory>("test_pool"));
  const int numValues = 5;
  const int requestsPerValue = 10;
  std::vector<std::pair<bool, int>> cachedValues(numValues * requestsPerValue);
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
      EXPECT_EQ(getCachedValue(cachedValues[i * numValues + j]), 2 * j);
    }
  }
}

TEST(CachedFactoryTest, retrievedCached) {
  auto generator = std::make_unique<DoublerGenerator>();
  auto* generated = &generator->generated_;
  CachedFactory<int, int, DoublerGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(1000), std::move(generator));
  for (int i = 0; i < 10; i += 2)
    factory.generate(i);
  EXPECT_EQ(*generated, 5);
  std::vector<int> keys(10);
  for (int i = 0; i < 10; i += 1)
    keys[i] = i;
  std::vector<std::pair<int, int>> cached;
  std::vector<int> missing;
  factory.retrieveCached(keys, &cached, &missing);
  ASSERT_EQ(5, cached.size());
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(cached[i].first, 2 * i);
    EXPECT_EQ(cached[i].second, 4 * i);
  }
  ASSERT_EQ(5, missing.size());
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(missing[i], 2 * i + 1);
  }
  EXPECT_EQ(*generated, 5);
}

TEST(CachedFactoryTest, disableCache) {
  auto generator = std::make_unique<DoublerGenerator>();
  auto* generated = &generator->generated_;
  CachedFactory<int, int, DoublerGenerator> factory(
      nullptr, std::move(generator));

  auto val1 = factory.generate(1);
  EXPECT_EQ(val1, cacheMiss(2));
  EXPECT_EQ(*generated, 1);

  auto val2 = factory.generate(1);
  EXPECT_EQ(val2, cacheMiss(2));
  EXPECT_EQ(*generated, 2);

  EXPECT_EQ(factory.currentSize(), 0);

  EXPECT_EQ(factory.maxSize(), 0);

  EXPECT_EQ(factory.cacheStats(), SimpleLRUCacheStats(0, 0, 0, 0));

  EXPECT_EQ(factory.clearCache(), SimpleLRUCacheStats(0, 0, 0, 0));

  std::vector<int> keys(10);
  for (int i = 0; i < 10; i += 1) {
    keys[i] = i;
  }
  std::vector<std::pair<int, int>> cached;
  std::vector<int> missing;
  factory.retrieveCached(keys, &cached, &missing);
  ASSERT_EQ(0, cached.size());
  ASSERT_EQ(10, missing.size());
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(missing[i], i);
  }
}
