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

#include <gtest/gtest.h>
#include <limits>

#include "velox/common/base/BitUtil.h"
#include "velox/dwio/dwrf/utils/BitIterator.h"

using namespace ::testing;

namespace facebook::velox::dwrf::utils {

TEST(BulkBitIterator, Basic) {
  std::vector<char> charBuffer1{
      std::numeric_limits<char>::max(), 102, 40, -120};
  std::vector<char> charBuffer2{125, 85, 42, std::numeric_limits<char>::min()};

  BulkBitIterator<char> bulkIter{};
  bulkIter.addRawByteBuffer(charBuffer1.data());
  bulkIter.addRawByteBuffer(charBuffer2.data());

  for (size_t i = 0; i < 32; ++i) {
    bulkIter.loadNext();
    EXPECT_EQ(bits::isBitSet(charBuffer1.data(), i), bulkIter.hasValueAt(0))
        << "Mismatch for first iter at " << i << "-th element";
    EXPECT_EQ(bits::isBitSet(charBuffer2.data(), i), bulkIter.hasValueAt(1))
        << "Mismatch for second iter at " << i << "-th element";
  }

  std::vector<long> longBuffer1{
      12790, std::numeric_limits<long>::max(), 45, -1288};
  std::vector<long> longBuffer2{
      125, 85098, std::numeric_limits<long>::min(), -98009};

  BulkBitIterator<long> longBulkIter{};
  longBulkIter.addRawByteBuffer(longBuffer1.data());
  longBulkIter.addRawByteBuffer(longBuffer2.data());

  for (size_t i = 0; i < 256; ++i) {
    longBulkIter.loadNext();
    EXPECT_EQ(bits::isBitSet(longBuffer1.data(), i), longBulkIter.hasValueAt(0))
        << "Mismatch for first iter at " << i << "-th element";
    EXPECT_EQ(bits::isBitSet(longBuffer2.data(), i), longBulkIter.hasValueAt(1))
        << "Mismatch for second iter at " << i << "-th element";
  }
}
} // namespace facebook::velox::dwrf::utils
