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

#include "velox/common/caching/SimpleLRUCache.h"

#include "gtest/gtest.h"

using namespace facebook::velox;

TEST(SimpleLRUCache, basicCaching) {
  SimpleLRUCache<int, int> cache(1000);

  ASSERT_TRUE(cache.add(1, new int(11), 1));
  int* value = cache.get(1);
  ASSERT_NE(value, nullptr);
  ASSERT_EQ(*value, 11);
  cache.release(1);

  int* secondValue = new int(22);
  ASSERT_TRUE(cache.addPinned(2, secondValue, 1));
  *secondValue += 5;
  cache.release(2);

  value = cache.get(1);
  ASSERT_NE(value, nullptr);
  ASSERT_EQ(*value, 11);
  cache.release(1);

  value = cache.get(2);
  ASSERT_NE(value, nullptr);
  ASSERT_EQ(*value, 27);
  cache.release(2);

  value = cache.get(1);
  ASSERT_NE(value, nullptr);
  ASSERT_EQ(*value, 11);
  secondValue = cache.get(1);
  ASSERT_EQ(value, secondValue);
  cache.release(1);
  cache.release(1);

  ASSERT_EQ(
      cache.stats().toString(),
      "{\n  maxSize: 1000\n  expireDurationMs: 0\n  curSize: 2\n  pinnedSize: 0\n  numElements: 2\n  numHits: 5\n  numLookups: 5\n}\n");
}

TEST(SimpleLRUCache, lruEviction) {
  SimpleLRUCache<int, int> cache(3);

  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(cache.add(i, new int(i), 1));
  }
  ASSERT_EQ(cache.stats().numElements, 3);
  ASSERT_EQ(*cache.get(0), 0);
  cache.release(0);

  ASSERT_TRUE(cache.add(3, new int(3), 1));
  ASSERT_EQ(*cache.get(0), 0);
  cache.release(0);
  ASSERT_EQ(cache.get(1), nullptr);
  ASSERT_EQ(*cache.get(3), 3);
  cache.release(3);
  ASSERT_EQ(cache.stats().numElements, 3);
}

TEST(SimpleLRUCache, eviction) {
  SimpleLRUCache<int, int> cache(1000);

  for (int i = 0; i < 1010; ++i) {
    ASSERT_TRUE(cache.add(i, new int(i), 1));
  }

  for (int i = 0; i < 10; ++i) {
    ASSERT_EQ(cache.get(i), nullptr);
  }
  for (int i = 10; i < 1010; ++i) {
    int* value = cache.get(i);
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(*value, i);
    cache.release(i);
  }
}

TEST(SimpleLRUCache, pinnedEviction) {
  SimpleLRUCache<int, int> cache(100);

  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(cache.addPinned(i, new int(i), 1));
  }
  for (int i = 10; i < 110; ++i) {
    ASSERT_TRUE(cache.add(i, new int(i), 1));
  }

  for (int i = 0; i < 10; ++i) {
    int* value = cache.get(i);
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(*value, i);
    cache.release(i);
    cache.release(i); // Release the original pin too.
  }
  for (int i = 10; i < 20; ++i) {
    ASSERT_EQ(cache.get(i), nullptr);
  }
  for (int i = 20; i < 110; ++i) {
    int* value = cache.get(i);
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(*value, i);
    cache.release(i);
  }
}

TEST(SimpleLRUCache, fullyPinned) {
  SimpleLRUCache<int, int> cache(10);

  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(cache.addPinned(i, new int(i), 1));
  }
  for (int i = 10; i < 20; ++i) {
    int* value = new int(i);
    ASSERT_FALSE(cache.add(i, value, 1));
    delete value;
  }
  for (int i = 20; i < 30; ++i) {
    int* value = new int(i);
    ASSERT_FALSE(cache.addPinned(i, value, 1));
    delete value;
  }

  for (int i = 0; i < 10; ++i) {
    int* value = cache.get(i);
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(*value, i);
    cache.release(i);
    cache.release(i); // Release the original pin too.
  }
  for (int i = 10; i < 30; ++i) {
    ASSERT_EQ(cache.get(i), nullptr);
  }
}

TEST(SimpleLRUCache, size) {
  SimpleLRUCache<int, int> cache(10);
  ASSERT_EQ(cache.maxSize(), 10);

  for (int i = 0; i < 5; ++i) {
    ASSERT_TRUE(cache.addPinned(i, new int(i), 2));
    ASSERT_EQ(cache.currentSize(), 2 * (i + 1));
  }
  int* value = new int(5);
  ASSERT_FALSE(cache.addPinned(5, value, 1));

  for (int i = 0; i < 5; ++i) {
    cache.release(i);
  }
  ASSERT_TRUE(cache.addPinned(5, value, 10));
  ASSERT_EQ(cache.currentSize(), 10);

  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(cache.get(i), nullptr);
  }
  cache.release(5);
}

TEST(SimpleLRUCache, insertLargerThanCacheFails) {
  SimpleLRUCache<int, int> cache(10);

  int* value = new int(42);
  ASSERT_FALSE(cache.add(123, value, 11));
  delete value;
}

TEST(SimpleLRUCache, expiredCacheEntries) {
  SimpleLRUCache<int, int> cache(100, 1'000);

  // Expires on insert new entry.
  int* value1 = new int(42);
  ASSERT_TRUE(cache.add(123, value1, 11));
  ASSERT_EQ(cache.currentSize(), 11);
  ASSERT_EQ(cache.get(123), value1);
  cache.release(123);

  std::this_thread::sleep_for(std::chrono::seconds{2});
  ASSERT_EQ(cache.currentSize(), 11);

  int* value2 = new int(32);
  ASSERT_TRUE(cache.add(122, value2, 22));
  ASSERT_EQ(cache.currentSize(), 22);
  ASSERT_EQ(cache.get(123), nullptr);
  ASSERT_EQ(cache.get(122), value2);
  cache.release(122);

  // Expires when get cache entry.
  std::this_thread::sleep_for(std::chrono::seconds{2});
  ASSERT_EQ(cache.currentSize(), 22);
  ASSERT_EQ(cache.get(123), nullptr);
  ASSERT_EQ(cache.currentSize(), 0);
  ASSERT_EQ(cache.get(122), nullptr);
  ASSERT_EQ(cache.currentSize(), 0);

  // Expires when get the same cache entry.
  value2 = new int(33);
  ASSERT_TRUE(cache.add(124, value2, 11));
  ASSERT_EQ(cache.currentSize(), 11);
  ASSERT_EQ(cache.get(124), value2);
  cache.release(124);
  ASSERT_EQ(cache.currentSize(), 11);
  std::this_thread::sleep_for(std::chrono::seconds{2});
  ASSERT_EQ(cache.currentSize(), 11);
  ASSERT_EQ(cache.get(124), nullptr);
  ASSERT_EQ(cache.currentSize(), 0);

  // Adds multiple entries.
  int expectedCacheSize{0};
  for (int i = 0; i < 10; ++i) {
    int* value = new int(i);
    ASSERT_TRUE(cache.add(i, value, i));
    ASSERT_EQ(cache.get(i), value);
    cache.release(i);
    expectedCacheSize += i;
    ASSERT_EQ(cache.currentSize(), expectedCacheSize);
  }
  std::this_thread::sleep_for(std::chrono::seconds{2});
  ASSERT_EQ(cache.currentSize(), expectedCacheSize);
  ASSERT_EQ(cache.get(0), nullptr);
  ASSERT_EQ(cache.currentSize(), 0);

  // Expire on release.
  value2 = new int(64);
  ASSERT_TRUE(cache.addPinned(124, value2, 11));
  ASSERT_EQ(cache.currentSize(), 11);
  std::this_thread::sleep_for(std::chrono::seconds{2});
  ASSERT_EQ(cache.currentSize(), 11);
  ASSERT_EQ(*cache.get(124), 64);
  cache.release(124);
  ASSERT_EQ(cache.currentSize(), 11);
  cache.release(124);
  ASSERT_EQ(cache.currentSize(), 0);
  ASSERT_EQ(cache.get(124), nullptr);
}
