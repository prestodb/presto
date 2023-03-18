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
#include "gtest/gtest.h"

using namespace facebook::velox;

struct DoublerGenerator {
  std::unique_ptr<int> operator()(const int& value) {
    ++generated_;
    return std::make_unique<int>(value * 2);
  }
  std::atomic<int> generated_ = 0;
};

TEST(CachedFactoryTest, basicGeneration) {
  auto generator = std::make_unique<DoublerGenerator>();
  auto* generated = &generator->generated_;
  CachedFactory<int, int, DoublerGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(1000), std::move(generator));
  ASSERT_EQ(factory.maxSize(), 1000);
  {
    auto val1 = factory.generate(1);
    ASSERT_EQ(*val1, 2);
    ASSERT_EQ(*generated, 1);
    ASSERT_FALSE(val1.wasCached());
    auto val2 = factory.generate(1);
    ASSERT_EQ(*val2, 2);
    ASSERT_EQ(*generated, 1);
    ASSERT_TRUE(val2.wasCached());
    ASSERT_EQ(factory.currentSize(), 1);
  }
  {
    auto val3 = factory.generate(1);
    ASSERT_EQ(*val3, 2);
    ASSERT_EQ(*generated, 1);
    ASSERT_TRUE(val3.wasCached());
    auto val4 = factory.generate(2);
    ASSERT_EQ(*val4, 4);
    ASSERT_EQ(*generated, 2);
    ASSERT_FALSE(val4.wasCached());
    auto val5 = factory.generate(3);
    ASSERT_EQ(*val5, 6);
    ASSERT_EQ(*generated, 3);
    ASSERT_FALSE(val5.wasCached());
    ASSERT_EQ(factory.currentSize(), 3);
  }
  auto val6 = factory.generate(1);
  ASSERT_EQ(*val6, 2);
  ASSERT_EQ(*generated, 3);
  ASSERT_TRUE(val6.wasCached());
  auto val7 = factory.generate(4);
  ASSERT_EQ(*val7, 8);
  ASSERT_EQ(*generated, 4);
  ASSERT_FALSE(val7.wasCached());
  auto val8 = factory.generate(3);
  ASSERT_EQ(*val8, 6);
  ASSERT_EQ(*generated, 4);
  ASSERT_TRUE(val8.wasCached());
  ASSERT_EQ(factory.currentSize(), 4);
}

struct DoublerWithExceptionsGenerator {
  std::unique_ptr<int> operator()(const int& value) {
    if (value == 3) {
      throw std::invalid_argument("3 is bad");
    }
    ++generated_;
    return std::make_unique<int>(value * 2);
  }
  int generated_ = 0;
};

TEST(CachedFactoryTest, basicExceptionHandling) {
  auto generator = std::make_unique<DoublerWithExceptionsGenerator>();
  int* generated = &generator->generated_;
  CachedFactory<int, int, DoublerWithExceptionsGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(1000), std::move(generator));
  auto val1 = factory.generate(1);
  ASSERT_EQ(*val1, 2);
  ASSERT_EQ(*generated, 1);
  try {
    auto val2 = factory.generate(3);
    CHECK(false);
  } catch (const std::invalid_argument& e) {
    // Expected.
  }
  val1 = factory.generate(4);
  ASSERT_EQ(*val1, 8);
  ASSERT_EQ(*generated, 2);
  val1 = factory.generate(1);
  ASSERT_EQ(*val1, 2);
  ASSERT_EQ(*generated, 2);
}

namespace {

// TODO: Folly probably already has something like this. Perhaps
// LifoSem, but its not clear to me how to use it. Replace this later, or
// make it into a proper header if its actually unique/useful.
class BlockingCounter {
 public:
  explicit BlockingCounter(int64_t count) : count_(count) {}

  // Blocks until decrement has been called the number of times *this was
  // constructed with. Exactly 1 thread should call wait().
  void wait() {
    std::unique_lock<std::mutex> l(mu_);
    if (count_ > 0) {
      cv_.wait(l, [&]() { return count_ <= 0; });
    }
  }

  // Threadsafe.
  void decrement() {
    std::unique_lock<std::mutex> l(mu_);
    --count_;
    if (count_ == 0) {
      cv_.notify_one();
    }
  }

 private:
  int64_t count_;
  std::mutex mu_;
  std::condition_variable cv_;
};

} // namespace

TEST(CachedFactoryTest, multiThreadedGeneration) {
  auto generator = std::make_unique<DoublerGenerator>();
  auto* generated = &generator->generated_;
  CachedFactory<int, int, DoublerGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(1000), std::move(generator));
  folly::EDFThreadPoolExecutor pool(
      100, std::make_shared<folly::NamedThreadFactory>("test_pool"));
  const int numValues = 5;
  const int requestsPerValue = 10;
  BlockingCounter counter(numValues * requestsPerValue);
  for (int i = 0; i < requestsPerValue; i++) {
    for (int j = 0; j < numValues; j++) {
      pool.add([&, j]() {
        auto value = factory.generate(j);
        CHECK_EQ(*value, 2 * j);
        counter.decrement();
      });
    }
  }
  counter.wait();
  ASSERT_EQ(*generated, numValues);
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
  std::vector<CachedPtr<int, int>> cachedValues(numValues * requestsPerValue);
  BlockingCounter counter(numValues * requestsPerValue);
  for (int i = 0; i < requestsPerValue; i++) {
    for (int j = 0; j < numValues; j++) {
      pool.add([&factory, &counter, &cachedValues, i, j]() {
        cachedValues[i * numValues + j] = factory.generate(j);
        counter.decrement();
      });
    }
  }
  counter.wait();
  ASSERT_EQ(*generated, numValues);
  for (int i = 0; i < requestsPerValue; i++) {
    for (int j = 0; j < numValues; j++) {
      ASSERT_EQ(*cachedValues[i * numValues + j], 2 * j);
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
  ASSERT_EQ(*generated, 5);
  std::vector<int> keys(10);
  for (int i = 0; i < 10; i += 1)
    keys[i] = i;
  std::vector<std::pair<int, CachedPtr<int, int>>> cached;
  std::vector<int> missing;
  factory.retrieveCached(keys, &cached, &missing);
  ASSERT_EQ(5, cached.size());
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(cached[i].first, 2 * i);
    ASSERT_EQ(*cached[i].second, 4 * i);
    ASSERT_TRUE(cached[i].second.wasCached());
  }
  ASSERT_EQ(5, missing.size());
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(missing[i], 2 * i + 1);
  }
  ASSERT_EQ(*generated, 5);
}

TEST(CachedFactoryTest, clearCache) {
  auto generator = std::make_unique<DoublerGenerator>();
  CachedFactory<int, int, DoublerGenerator> factory(
      std::make_unique<SimpleLRUCache<int, int>>(1000), std::move(generator));
  for (auto i = 0; i < 1000; i++) {
    factory.generate(i);
  }
  std::vector<int> keys(500);
  for (int i = 0; i < 500; i++) {
    keys[i] = i;
  }
  {
    std::vector<std::pair<int, CachedPtr<int, int>>> cached;
    std::vector<int> missing;
    factory.retrieveCached(keys, &cached, &missing);
    EXPECT_EQ(cached.size(), 500);
    auto cacheStats = factory.clearCache();
    EXPECT_EQ(cacheStats.numElements, 500);
    EXPECT_EQ(cacheStats.pinnedSize, 500);
  }
  auto cacheStats = factory.cacheStats();
  EXPECT_EQ(cacheStats.numElements, 500);
  EXPECT_EQ(cacheStats.pinnedSize, 0);

  cacheStats = factory.clearCache();
  EXPECT_EQ(cacheStats.numElements, 0);
  EXPECT_EQ(cacheStats.pinnedSize, 0);
}
