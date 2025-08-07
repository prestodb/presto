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

#include "velox/functions/iceberg/Murmur3Hash32.h"
#include "velox/common/base/BitUtil.h"

namespace facebook::velox::functions::iceberg {

namespace {

constexpr int32_t kSeed = 0;
} // namespace

int32_t Murmur3Hash32::hashInt64(uint64_t input) {
  return Murmur3Hash32Base::hashInt64(input, kSeed);
}

int32_t Murmur3Hash32::hashBytes(const char* input, uint32_t len) {
  const uint8_t* data = reinterpret_cast<const uint8_t*>(input);
  const int32_t nblocks = len / 4;

  uint32_t h1 = kSeed;

  // Process 4-byte chunks.
  for (auto i = 0; i < nblocks; i++) {
    uint32_t k1 = *reinterpret_cast<const uint32_t*>(data + i * 4);
    k1 = mixK1(k1);
    h1 = mixH1(h1, k1);
  }

  // Process remaining bytes.
  const uint8_t* tail = data + nblocks * 4;
  uint32_t k1 = 0;
  switch (len & 3) {
    case 3:
      k1 ^= static_cast<uint32_t>(tail[2]) << 16;
      [[fallthrough]];
    case 2:
      k1 ^= static_cast<uint32_t>(tail[1]) << 8;
      [[fallthrough]];
    case 1:
      k1 ^= static_cast<uint32_t>(tail[0]);
      h1 ^= mixK1(k1);
  }

  return fmix(h1, len);
}

} // namespace facebook::velox::functions::iceberg
