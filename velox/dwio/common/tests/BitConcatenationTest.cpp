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

#include "velox/dwio/common/BitConcatenation.h"

#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;

TEST(BitConcatenationTests, basic) {
  auto pool = facebook::velox::memory::getDefaultScopedMemoryPool();
  BitConcatenation bits(*pool);
  BufferPtr result;

  std::vector<uint64_t> oneBits(10, ~0UL);
  std::vector<uint64_t> zeroBits(10, 0UL);

  // add only one bits, expect nullptr.
  bits.reset(result);
  bits.appendOnes(34);
  bits.append(oneBits.data(), 3, 29);
  EXPECT_EQ(34 + (29 - 3), bits.numBits());
  EXPECT_TRUE(!result);
  EXPECT_TRUE(!bits.buffer());

  // Add ones, then zeros and then ones. Expect bitmap.
  bits.reset(result);
  bits.append(oneBits.data(), 0, 29);
  bits.append(zeroBits.data(), 3, 29);
  bits.append(oneBits.data(), 6, 29);
  // Expecting  29 ones, 26 zeros and 23 zeros.
  EXPECT_EQ(29 + 26 + 23, bits.numBits());
  auto data = result->as<uint64_t>();
  EXPECT_TRUE(bits::isAllSet(data, 0, 29, true));
  EXPECT_TRUE(bits::isAllSet(data, 29, 29 + 26, false));
  EXPECT_TRUE(bits::isAllSet(data, 29 + 26, 29 + 26 + 23, true));

  // Add only one bits, expect nullptr, even though 'result' has a value.
  bits.reset(result);
  bits.appendOnes(24);
  bits.append(oneBits.data(), 3, 29);
  EXPECT_EQ(24 + (29 - 3), bits.numBits());
  EXPECT_TRUE(!bits.buffer());
  EXPECT_FALSE(!result);
}
