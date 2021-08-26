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

#include "velox/common/caching/DataCache.h"

#include "gtest/gtest.h"

using namespace facebook::velox;

TEST(SimpleLRUDataCache, basicUsage) {
  SimpleLRUDataCache cache(1000);
  ASSERT_EQ(cache.maxSize(), 1000);
  ASSERT_EQ(cache.currentSize(), 0);

  ASSERT_TRUE(cache.put("foo", "boo!"));
  std::string buf(4, 0);
  ASSERT_TRUE(cache.get("foo", 4, buf.data()));
  ASSERT_EQ(buf, "boo!");
  const int64_t currentSize = cache.currentSize();
  ASSERT_GT(currentSize, 0);

  ASSERT_FALSE(cache.get("wuz", 4, buf.data()));
  ASSERT_FALSE(cache.get("foo", 3, buf.data()));

  std::string snarf;
  ASSERT_TRUE(cache.get("foo", &snarf));
  ASSERT_EQ(snarf, "boo!");
  ASSERT_EQ(cache.currentSize(), currentSize);

  ASSERT_TRUE(cache.put("waka", "waka!!"));

  ASSERT_TRUE(cache.get("foo", 4, buf.data()));
  ASSERT_EQ(buf, "boo!");

  buf.resize(6);
  ASSERT_TRUE(cache.get("waka", 6, buf.data()));
  ASSERT_EQ(buf, "waka!!");
  ASSERT_GT(cache.currentSize(), currentSize);
}
