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
#include <optional>

#include "gtest/gtest.h"

using namespace facebook::velox;

namespace {
void verifyCacheStats(
    const SimpleLRUCacheStats& actual,
    size_t maxSize,
    size_t curSize,
    size_t numHits,
    size_t numLookups) {
  SimpleLRUCacheStats expectedStats{maxSize, curSize, numHits, numLookups};
  EXPECT_EQ(actual, expectedStats) << " Actual " << actual.toString()
                                   << " Expected " << expectedStats.toString();
}
} // namespace

TEST(SimpleLRUCache, basicCaching) {
  SimpleLRUCache<int, int> cache(1000);

  EXPECT_FALSE(cache.get(1).has_value());
  EXPECT_FALSE(cache.get(2).has_value());

  verifyCacheStats(cache.getStats(), 1000, 0, 0, 2);

  int firstValue = 11;
  ASSERT_TRUE(cache.add(1, firstValue));
  auto value = cache.get(1);
  ASSERT_EQ(value, std::make_optional(11));

  int secondValue = 22;
  ASSERT_TRUE(cache.add(2, secondValue));

  verifyCacheStats(cache.getStats(), 1000, 2, 1, 3);

  value = cache.get(1);
  ASSERT_EQ(value, std::make_optional(11));

  value = cache.get(2);
  ASSERT_EQ(value, std::make_optional(22));

  value = cache.get(1);
  ASSERT_EQ(value, std::make_optional(11));

  value = cache.get(2);
  ASSERT_EQ(value, std::make_optional(22));
  verifyCacheStats(cache.getStats(), 1000, 2, 5, 7);

  cache.clear();
  verifyCacheStats(cache.getStats(), 1000, 0, 5, 7);
  EXPECT_FALSE(cache.get(1).has_value());
  EXPECT_FALSE(cache.get(2).has_value());
}

TEST(SimpleLRUCache, eviction) {
  SimpleLRUCache<int, int> cache(1000);

  for (int i = 0; i < 1010; ++i) {
    ASSERT_TRUE(cache.add(i, i));
  }

  for (int i = 0; i < 10; ++i) {
    ASSERT_FALSE(cache.get(i).has_value());
  }

  for (int i = 10; i < 1010; ++i) {
    auto value = cache.get(i);
    ASSERT_EQ(value, std::make_optional(i));
  }
}
