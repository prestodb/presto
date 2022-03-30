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

#include "velox/common/base/BitSet.h"

#include <gtest/gtest.h>

using namespace facebook::velox;

TEST(BitSetTest, basic) {
  constexpr int64_t kMin = 12;
  BitSet bits(kMin);
  bits.insert(1);
  EXPECT_FALSE(bits.contains(1));
  EXPECT_EQ(bits.max(), kMin - 1);
  for (auto i = kMin; i < 1000; i += 2) {
    bits.insert(i);
  }

  EXPECT_EQ(998, bits.max());
  EXPECT_FALSE(bits.contains(0));
  EXPECT_FALSE(bits.contains(10000));
  EXPECT_FALSE(bits.contains(bits::roundUp(998 - kMin, 64) + kMin));
  EXPECT_FALSE(bits.contains(-100));
  for (auto i = kMin; i < 1000; i += 2) {
    EXPECT_TRUE(bits.contains(i));
    EXPECT_FALSE(bits.contains(i + 1));
  }
}
