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

#include "velox/functions/lib/Murmur3Hash32Base.h"
#include "velox/common/base/BitUtil.h"

namespace facebook::velox::functions {

uint32_t Murmur3Hash32Base::hashInt64(uint64_t input, uint32_t seed) {
  uint32_t low = input;
  uint32_t high = input >> 32;

  uint32_t k1 = mixK1(low);
  uint32_t h1 = mixH1(seed, k1);

  k1 = mixK1(high);
  h1 = mixH1(h1, k1);

  return fmix(h1, 8);
}

uint32_t Murmur3Hash32Base::mixK1(uint32_t k1) {
  k1 *= 0xcc9e2d51;
  k1 = bits::rotateLeft(k1, 15);
  k1 *= 0x1b873593;
  return k1;
}

uint32_t Murmur3Hash32Base::mixH1(uint32_t h1, uint32_t k1) {
  h1 ^= k1;
  h1 = bits::rotateLeft(h1, 13);
  h1 = h1 * 5 + 0xe6546b64;
  return h1;
}

uint32_t Murmur3Hash32Base::fmix(uint32_t h1, uint32_t length) {
  h1 ^= length;
  h1 ^= h1 >> 16;
  h1 *= 0x85ebca6b;
  h1 ^= h1 >> 13;
  h1 *= 0xc2b2ae35;
  h1 ^= h1 >> 16;
  return h1;
}
} // namespace facebook::velox::functions
