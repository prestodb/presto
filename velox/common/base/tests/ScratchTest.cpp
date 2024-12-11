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

#include "velox/common/base/Scratch.h"

#include <gtest/gtest.h>

using namespace facebook::velox;

TEST(ScratchTest, basic) {
  Scratch scratch;
  {
    ScratchPtr<int32_t> ints(scratch);
    ScratchPtr<int64_t> longs(scratch);
    auto tempInts = ints.get(1000);
    auto tempLongs = longs.get(2000);
    std::fill(tempInts, tempInts + 1000, -1);
    std::fill(tempLongs, tempLongs + 2000, -1);
    EXPECT_EQ(0, scratch.retainedSize());
  }
  EXPECT_EQ(20352, scratch.retainedSize());
  {
    ScratchPtr<int32_t> ints(scratch);
    ScratchPtr<int64_t> longs(scratch);
    auto tempLongs = longs.get(2000);
    auto tempInts = ints.get(1000);
    std::fill(tempInts, tempInts + 1000, -1);
    std::fill(tempInts, tempInts + 2000, -1);
    EXPECT_EQ(0, scratch.retainedSize());
  }
  // The scratch vectors were acquired in a different order, so the smaller got
  // resized to the larger size.
  EXPECT_EQ(32640, scratch.retainedSize());
  scratch.trim();
  EXPECT_EQ(0, scratch.retainedSize());
  {
    ScratchPtr<int32_t, 10> ints(scratch);
    // The size is the inline size, nothing gets returned to 'scratch'.
    auto temp = ints.get(10);
    temp[0] = 1;
  }
  EXPECT_EQ(0, scratch.retainedSize());
}

TEST(ScratchTest, large) {
  constexpr int32_t kSize = 100;
  Scratch scratch;
  std::vector<std::unique_ptr<ScratchPtr<int32_t>>> pointers;
  for (auto i = 0; i < kSize; ++i) {
    pointers.push_back(std::make_unique<ScratchPtr<int32_t>>(scratch));
    pointers.back()->get(1000);
  }
  pointers.clear();
  // 100 times 1000 bytes returned.
  EXPECT_LT(100'000, scratch.retainedSize());
  for (auto i = 0; i < kSize; ++i) {
    pointers.push_back(std::make_unique<ScratchPtr<int32_t>>(scratch));
    pointers.back()->get(1000);
  }
  EXPECT_EQ(0, scratch.retainedSize());
}
