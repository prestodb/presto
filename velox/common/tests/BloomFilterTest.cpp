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

#include "velox/common/base/BloomFilter.h"
#include <folly/Random.h>
#include <unordered_set>

#include <gtest/gtest.h>

using namespace facebook::velox;

TEST(BloomFilterTest, basic) {
  constexpr int32_t kSize = 1024;
  BloomFilter bloom;
  bloom.reset(kSize);
  for (auto i = 0; i < kSize; ++i) {
    bloom.insert(i);
  }
  int32_t numFalsePositives = 0;
  for (auto i = 0; i < kSize; ++i) {
    EXPECT_TRUE(bloom.mayContain(i));
    numFalsePositives += bloom.mayContain(i + kSize);
    numFalsePositives += bloom.mayContain((i + kSize) * 123451);
  }
  EXPECT_GT(2, 100 * numFalsePositives / kSize);
}
