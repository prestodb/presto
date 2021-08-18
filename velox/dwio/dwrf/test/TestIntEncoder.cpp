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

#include <folly/Varint.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "velox/dwio/dwrf/common/IntEncoder.h"

using namespace ::testing;

namespace facebook::velox::dwrf {

class VarintPairs {
 public:
  const uint64_t value;
  const std::vector<uint8_t> varInt;

  VarintPairs(uint64_t value, std::vector<uint8_t> varInt)
      : value{value}, varInt{varInt} {}
};

TEST(TestIntEncoder, TestVarIntEncoder) {
  auto values = std::vector<VarintPairs>{
      {0, {0x0}},
      {(1ul << 7) - 1, {0x7F}},
      {(1ul << 7), {0x80, 0x1}},
      {(1ul << 14) - 1, {0xFF, 0x7F}},
      {(1ul << 14), {0x80, 0x80, 0x01}},
      {(1ul << 21) - 1, {0xFF, 0xFF, 0x7F}},
      {(1ul << 21), {0x80, 0x80, 0x80, 0x01}},
      {(1ul << 28) - 1, {0xFF, 0xFF, 0xFF, 0x7F}},
      {(1ul << 28), {0x80, 0x80, 0x80, 0x80, 0x01}},
      {(1ul << 35) - 1, {0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
      {(1ul << 35), {0x80, 0x80, 0x80, 0x80, 0x80, 0x01}},
      {(1ul << 42) - 1, {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
      {(1ul << 42), {0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}},
      {(1ul << 49) - 1, {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
      {(1ul << 49), {0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}},
      {(1ul << 56) - 1, {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
      {(1ul << 56), {0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}},
      {(1ul << 63) - 1, {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
      {(1ul << 63),
       {0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}},
      {UINT64_MAX,
       {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}},

  };

  char outBuff[folly::kMaxVarintLength64];

  for (auto& pair : values) {
    auto result = IntEncoder<true>::write64Varint(pair.value, outBuff);
    EXPECT_EQ(result, pair.varInt.size()) << " value " << pair.value;
    ASSERT_THAT(pair.varInt, ElementsAreArray(outBuff, result))
        << " value " << pair.value;
  }
}

} // namespace facebook::velox::dwrf
