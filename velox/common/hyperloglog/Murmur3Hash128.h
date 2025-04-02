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

#pragma once

#include <cstdint>

#include "velox/common/base/BitUtil.h"

namespace facebook::velox::common::hll {

/// This method implements the Murmur3Hash128::hash64 methods to match Presto
/// Java.
class Murmur3Hash128 {
 public:
  /// This method implements
  /// https://github.com/airlift/slice/blob/44896889dcef7a16a2c14800a4a392934909c2cc/src/main/java/io/airlift/slice/Murmur3Hash128.java#L152.
  static int64_t hash64(const void* data, int32_t length, int64_t seed);

  /// This method implements
  /// https://github.com/airlift/slice/blob/44896889dcef7a16a2c14800a4a392934909c2cc/src/main/java/io/airlift/slice/Murmur3Hash128.java#L248.
  static int64_t hash64ForLong(int64_t data, int64_t seed) {
    uint64_t h2 = seed ^ sizeof(int64_t);
    uint64_t h1 = h2 + (h2 ^ (bits::rotateLeft64(data * C1, 31) * C2));
    return static_cast<int64_t>(mix64(h1) + mix64(h1 + h2));
  }

  static void
  hash(const void* key, const int32_t len, const uint32_t seed, void* out);

 private:
  static constexpr uint64_t C1 = 0x87c37b91114253d5L;
  static constexpr uint64_t C2 = 0x4cf5ad432745937fL;

  static uint64_t mix64(uint64_t k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= k >> 33;

    return k;
  }
};

} // namespace facebook::velox::common::hll
