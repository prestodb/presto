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

#include "velox/common/hyperloglog/Murmur3Hash128.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::common::hll {

int64_t getLong(const void* data, int32_t offset) {
  return folly::loadUnaligned<int64_t>(static_cast<const char*>(data) + offset);
}

char getByte(const void* data, int32_t offset) {
  return *(static_cast<const char*>(data) + offset);
}

// static
int64_t Murmur3Hash128::hash64(const void* data, int32_t length, int64_t seed) {
  VELOX_DCHECK_NOT_NULL(data);
  const int32_t fastLimit =
      static_cast<int32_t>(length - (2 * sizeof(int64_t)) + 1);

  uint64_t h1 = seed;
  uint64_t h2 = seed;

  int32_t current = 0;
  while (current < fastLimit) {
    VELOX_DCHECK_LE(current + 2 * sizeof(int64_t), length);
    int64_t k1 = getLong(data, current);
    current += sizeof(int64_t);

    int64_t k2 = getLong(data, current);
    current += sizeof(int64_t);

    k1 = static_cast<int64_t>(k1 * C1);
    k1 = static_cast<int64_t>(bits::rotateLeft64(k1, 31));
    k1 = static_cast<int64_t>(k1 * C2);
    h1 ^= k1;

    h1 = bits::rotateLeft64(h1, 27);
    h1 = h1 + h2;
    h1 = h1 * 5 + 0x52dce729L;

    k2 = static_cast<int64_t>(k2 * C2);
    k2 = static_cast<int64_t>(bits::rotateLeft64(k2, 33));
    k2 = static_cast<int64_t>(k2 * C1);
    h2 ^= k2;

    h2 = bits::rotateLeft64(h2, 31);
    h2 = h2 + h1;
    h2 = h2 * 5 + 0x38495ab5L;
  }

  int64_t k1 = 0;
  int64_t k2 = 0;

  VELOX_DCHECK_LE(current + (length & 15), length);
  switch (length & 15) {
    case 15:
      k2 ^= (static_cast<int64_t>(getByte(data, current + 14))) << 48;
      [[fallthrough]];
    case 14:
      k2 ^= (static_cast<int64_t>(getByte(data, current + 13))) << 40;
      [[fallthrough]];
    case 13:
      k2 ^= (static_cast<int64_t>(getByte(data, current + 12))) << 32;
      [[fallthrough]];
    case 12:
      k2 ^= (static_cast<int64_t>(getByte(data, current + 11))) << 24;
      [[fallthrough]];
    case 11:
      k2 ^= (static_cast<int64_t>(getByte(data, current + 10))) << 16;
      [[fallthrough]];
    case 10:
      k2 ^= (static_cast<int64_t>(getByte(data, current + 9))) << 8;
      [[fallthrough]];
    case 9:
      k2 ^= (static_cast<int64_t>(getByte(data, current + 8))) << 0;

      k2 = static_cast<int64_t>(k2 * C2);
      k2 = static_cast<int64_t>(bits::rotateLeft64(k2, 33));
      k2 = static_cast<int64_t>(k2 * C1);
      h2 ^= k2;
      [[fallthrough]];

    case 8:
      k1 ^= (static_cast<int64_t>(getByte(data, current + 7))) << 56;
      [[fallthrough]];
    case 7:
      k1 ^= (static_cast<int64_t>(getByte(data, current + 6))) << 48;
      [[fallthrough]];
    case 6:
      k1 ^= (static_cast<int64_t>(getByte(data, current + 5))) << 40;
      [[fallthrough]];
    case 5:
      k1 ^= (static_cast<int64_t>(getByte(data, current + 4))) << 32;
      [[fallthrough]];
    case 4:
      k1 ^= (static_cast<int64_t>(getByte(data, current + 3))) << 24;
      [[fallthrough]];
    case 3:
      k1 ^= (static_cast<int64_t>(getByte(data, current + 2))) << 16;
      [[fallthrough]];
    case 2:
      k1 ^= (static_cast<int64_t>(getByte(data, current + 1))) << 8;
      [[fallthrough]];
    case 1:
      k1 ^= (static_cast<int64_t>(getByte(data, current + 0))) << 0;

      k1 = static_cast<int64_t>(k1 * C1);
      k1 = static_cast<int64_t>(bits::rotateLeft64(k1, 31));
      k1 = static_cast<int64_t>(k1 * C2);
      h1 ^= k1;
      break;
    default:
      break;
  }

  h1 ^= length;
  h2 ^= length;

  h1 = h1 + h2;
  h2 = h2 + h1;

  h1 = mix64(h1);
  h2 = mix64(h2);

  return static_cast<int64_t>(h1 + h2);
}

void Murmur3Hash128::hash(
    const void* key,
    const int32_t len,
    const uint32_t seed,
    void* out) {
  VELOX_DCHECK_NOT_NULL(key);
  VELOX_DCHECK_NOT_NULL(out);
  const uint8_t* data = static_cast<const uint8_t*>(key);
  const int32_t nblocks = len / 16;
  uint64_t h1 = seed;
  uint64_t h2 = seed;
  // Body
  for (int32_t i = 0; i < nblocks; ++i) {
    uint64_t k1 = getLong(data, i * 16);
    uint64_t k2 = getLong(data, i * 16 + 8);
    k1 *= C1;
    k1 = bits::rotateLeft64(k1, 31);
    k1 *= C2;
    h1 ^= k1;
    h1 = bits::rotateLeft64(h1, 27);
    h1 += h2;
    h1 = h1 * 5 + 0x52dce729;
    k2 *= C2;
    k2 = bits::rotateLeft64(k2, 33);
    k2 *= C1;
    h2 ^= k2;
    h2 = bits::rotateLeft64(h2, 31);
    h2 += h1;
    h2 = h2 * 5 + 0x38495ab5;
  }
  // Tail
  const uint8_t* tail = data + nblocks * 16;
  uint64_t k1 = 0;
  uint64_t k2 = 0;
  switch (len & 15) {
    case 15:
      k2 ^= uint64_t(tail[14]) << 48;
      [[fallthrough]];
    case 14:
      k2 ^= uint64_t(tail[13]) << 40;
      [[fallthrough]];
    case 13:
      k2 ^= uint64_t(tail[12]) << 32;
      [[fallthrough]];
    case 12:
      k2 ^= uint64_t(tail[11]) << 24;
      [[fallthrough]];
    case 11:
      k2 ^= uint64_t(tail[10]) << 16;
      [[fallthrough]];
    case 10:
      k2 ^= uint64_t(tail[9]) << 8;
      [[fallthrough]];
    case 9:
      k2 ^= uint64_t(tail[8]) << 0;
      k2 *= C2;
      k2 = bits::rotateLeft64(k2, 33);
      k2 *= C1;
      h2 ^= k2;
      [[fallthrough]];
    case 8:
      k1 ^= uint64_t(tail[7]) << 56;
      [[fallthrough]];
    case 7:
      k1 ^= uint64_t(tail[6]) << 48;
      [[fallthrough]];
    case 6:
      k1 ^= uint64_t(tail[5]) << 40;
      [[fallthrough]];
    case 5:
      k1 ^= uint64_t(tail[4]) << 32;
      [[fallthrough]];
    case 4:
      k1 ^= uint64_t(tail[3]) << 24;
      [[fallthrough]];
    case 3:
      k1 ^= uint64_t(tail[2]) << 16;
      [[fallthrough]];
    case 2:
      k1 ^= uint64_t(tail[1]) << 8;
      [[fallthrough]];
    case 1:
      k1 ^= uint64_t(tail[0]) << 0;
      k1 *= C1;
      k1 = bits::rotateLeft64(k1, 31);
      k1 *= C2;
      h1 ^= k1;
  }
  // Finalization
  h1 ^= len;
  h2 ^= len;
  h1 += h2;
  h2 += h1;
  h1 = mix64(h1);
  h2 = mix64(h2);
  h1 += h2;
  h2 += h1;
  // Store the result in the output buffer
  uint64_t* out64 = static_cast<uint64_t*>(out);
  out64[0] = h1;
  out64[1] = h2;
}

} // namespace facebook::velox::common::hll
