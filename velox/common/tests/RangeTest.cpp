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

#include "velox/common/base/Range.h"

#include <gtest/gtest.h>

namespace facebook {
namespace velox {

TEST(RangeTest, ranges) {
  std::vector<uint64_t> bits(10);
  uint64_t* data = &bits[0];
  Range<bool> readable(data, 11, 511);
  MutableRange<bool> writable(data, 9, 509);
  Range<uint8_t> readableBytes(&bits[0], 1, 79);
  MutableRange<uint8_t> writableBytes(&bits[0], 0, 80);
  // Bit 13 appears as bit 2 in readable and as bit 4 in writable.
  bits::setBit(data, 13);
  EXPECT_TRUE(readable[2]);
  EXPECT_TRUE(writable[4]);
  writable[13] = true;
  EXPECT_TRUE(readable[11]);
  writable[13] = false;
  EXPECT_FALSE(readable[11]);

  writableBytes[10] = 123;
  EXPECT_EQ(readableBytes[9], 123);
  // Bit 80 is set.
  EXPECT_TRUE(readable[69]);
}

} // namespace velox
} // namespace facebook
