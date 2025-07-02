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
#include "velox/common/encode/Coding.h"
#include "velox/type/HugeInt.h"

namespace facebook::velox::encode::test {

class ZigZagTest : public ::testing::Test {};

TEST_F(ZigZagTest, hugeInt) {
  auto assertZigZag = [](int128_t value) {
    auto encoded = ZigZag::encodeInt128(value);
    auto decoded = ZigZag::decode(encoded);
    EXPECT_EQ(value, decoded);
  };

  assertZigZag(0);
  assertZigZag(HugeInt::parse("1234567890123456789"));
  assertZigZag(HugeInt::parse("-1234567890123456789"));
  assertZigZag(HugeInt::parse(std::string(38, '9')));
  assertZigZag(std::numeric_limits<__int128_t>::max());
  assertZigZag(std::numeric_limits<__int128_t>::min());
}

} // namespace facebook::velox::encode::test
