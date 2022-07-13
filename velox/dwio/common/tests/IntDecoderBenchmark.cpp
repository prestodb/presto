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

#include <limits>
#include "folly/Benchmark.h"
#include "folly/CpuId.h"
#include "folly/Random.h"
#include "folly/Varint.h"
#include "folly/init/Init.h"
#include "folly/lang/Bits.h"
#include "velox/common/base/BitUtil.h"
#include "velox/dwio/common/IntCodecCommon.h"
#include "velox/dwio/common/exception/Exception.h"

using namespace facebook::velox::dwio;
namespace bits = facebook::velox::bits;

const size_t kNumElements = 1000000;

static size_t len_u16 = 0;
std::vector<uint16_t> randomInts_u16;
std::vector<uint64_t> randomInts_u16_result;
std::vector<char> buffer_u16;

static size_t len_u32 = 0;
std::vector<uint32_t> randomInts_u32;
std::vector<uint64_t> randomInts_u32_result;
std::vector<char> buffer_u32;

static size_t len_u64 = 0;
std::vector<uint64_t> randomInts_u64;
std::vector<uint64_t> randomInts_u64_result;
std::vector<char> buffer_u64;

uint64_t readVuLong(const char* buffer, size_t& len) {
  if (LIKELY(len >= folly::kMaxVarintLength64)) {
    const char* p = buffer;
    uint64_t val;
    do {
      int64_t b;
      b = *p++;
      len--;
      val = (b & 0x7f);
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      len--;
      val |= (b & 0x7f) << 7;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      len--;
      val |= (b & 0x7f) << 14;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      len--;
      val |= (b & 0x7f) << 21;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      len--;
      val |= (b & 0x7f) << 28;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      len--;
      val |= (b & 0x7f) << 35;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      len--;
      val |= (b & 0x7f) << 42;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      len--;
      val |= (b & 0x7f) << 49;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      len--;
      val |= (b & 0x7f) << 56;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      len--;
      val |= (b & 0x01) << 63;
      if (LIKELY(b >= 0)) {
        break;
      } else {
        throw std::runtime_error{"invalid encoding: likely corrupt data"};
      }
    } while (false);
    return val;
  } else {
    int64_t result = 0;
    int64_t offset = 0;
    signed char ch;
    size_t pos = 0;
    do {
      ch = buffer[pos++];
      result |= (ch & facebook::velox::dwio::common::BASE_128_MASK) << offset;
      offset += 7;
      len--;
    } while (ch < 0);
    return result;
  }
}

const char* readVuLongOptimized(uint64_t n, const char* pos, uint64_t* output) {
  static bool has_bmi2 = folly::CpuId().bmi2();
  DWIO_ENSURE(has_bmi2, "bmi2 is not eabled");
  constexpr uint64_t mask = 0x0000808080808080;
  // Note that we could of course use a mask_len of up to 8. But I found
  // that with mask_len > 6 we start to spill out of the l1i cache in
  // opt mode and that counterbalances the gain. Plus the first run and/or
  // small n are more expensive as we have to load more instructions.
  constexpr int32_t mask_len = 6;
  uint64_t carryover = 0;
  int32_t carryover_bits = 0;
  pos -= mask_len;
  // Also note that a handful of these cases are impossible for 32-bit varints.
  // We coould save a tiny bit of program size by pruning them out.
  while (n >= 8) {
    pos += mask_len;
    const auto word = folly::loadUnaligned<uint64_t>(pos);
    const uint64_t control_bits = bits::extractBits<uint64_t>(word, mask);
    switch (control_bits) {
      case 0ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000000000007f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f00000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 6;
        continue;
      }
      case 1ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f00000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 5;
        continue;
      }
      case 2ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f00000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 5;
        continue;
      }
      case 3ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f00000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 4;
        continue;
      }
      case 4ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000000000007f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f00000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 5;
        continue;
      }
      case 5ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f00000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 4;
        continue;
      }
      case 6ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f7f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f00000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 4;
        continue;
      }
      case 7ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000007f7f7f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f00000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 3;
        continue;
      }
      case 8ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000000000007f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 5;
        continue;
      }
      case 9ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 4;
        continue;
      }
      case 10ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 4;
        continue;
      }
      case 11ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 3;
        continue;
      }
      case 12ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000000000007f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f7f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 4;
        continue;
      }
      case 13ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f7f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 3;
        continue;
      }
      case 14ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f7f7f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 3;
        continue;
      }
      case 15ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000007f7f7f7f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 2;
        continue;
      }
      case 16ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000000000007f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 5;
        continue;
      }
      case 17ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 4;
        continue;
      }
      case 18ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 4;
        continue;
      }
      case 19ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 3;
        continue;
      }
      case 20ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000000000007f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 4;
        continue;
      }
      case 21ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 3;
        continue;
      }
      case 22ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f7f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 3;
        continue;
      }
      case 23ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000007f7f7f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 2;
        continue;
      }
      case 24ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000000000007f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 4;
        continue;
      }
      case 25ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 3;
        continue;
      }
      case 26ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 3;
        continue;
      }
      case 27ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 2;
        continue;
      }
      case 28ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000000000007f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f7f7f0000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 3;
        continue;
      }
      case 29ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f7f7f0000ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 2;
        continue;
      }
      case 30ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f7f7f7f00ULL);
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 2;
        continue;
      }
      case 31ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x00007f7f7f7f7f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        carryover = 0ULL;
        carryover_bits = 0;
        n -= 1;
        continue;
      }
      case 32ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000000000007f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f00000000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover_bits = 7;
        n -= 5;
        continue;
      }
      case 33ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f00000000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover_bits = 7;
        n -= 4;
        continue;
      }
      case 34ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f00000000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover_bits = 7;
        n -= 4;
        continue;
      }
      case 35ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f000000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f00000000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover_bits = 7;
        n -= 3;
        continue;
      }
      case 36ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000000000007f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f00000000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover_bits = 7;
        n -= 4;
        continue;
      }
      case 37ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f00000000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover_bits = 7;
        n -= 3;
        continue;
      }
      case 38ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f7f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f00000000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover_bits = 7;
        n -= 3;
        continue;
      }
      case 39ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000007f7f7f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f00000000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover_bits = 7;
        n -= 2;
        continue;
      }
      case 40ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000000000007f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover_bits = 7;
        n -= 4;
        continue;
      }
      case 41ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover_bits = 7;
        n -= 3;
        continue;
      }
      case 42ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover_bits = 7;
        n -= 3;
        continue;
      }
      case 43ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover_bits = 7;
        n -= 2;
        continue;
      }
      case 44ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000000000007f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f7f0000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover_bits = 7;
        n -= 3;
        continue;
      }
      case 45ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f7f0000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover_bits = 7;
        n -= 2;
        continue;
      }
      case 46ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f7f7f00ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover_bits = 7;
        n -= 2;
        continue;
      }
      case 47ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000007f7f7f7f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        carryover = bits::extractBits<uint64_t>(word, 0x00007f0000000000ULL);
        carryover_bits = 7;
        n -= 1;
        continue;
      }
      case 48ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000000000007f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f000000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
        carryover_bits = 14;
        n -= 4;
        continue;
      }
      case 49ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f0000ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f000000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
        carryover_bits = 14;
        n -= 3;
        continue;
      }
      case 50ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f000000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
        carryover_bits = 14;
        n -= 3;
        continue;
      }
      case 51ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f000000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
        carryover_bits = 14;
        n -= 2;
        continue;
      }
      case 52ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000000000007f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
        carryover_bits = 14;
        n -= 3;
        continue;
      }
      case 53ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
        carryover_bits = 14;
        n -= 2;
        continue;
      }
      case 54ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f7f00ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
        carryover_bits = 14;
        n -= 2;
        continue;
      }
      case 55ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000007f7f7f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        carryover = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
        carryover_bits = 14;
        n -= 1;
        continue;
      }
      case 56ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000000000007f00ULL);
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f0000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
        carryover_bits = 21;
        n -= 3;
        continue;
      }
      case 57ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f0000ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
        carryover_bits = 21;
        n -= 2;
        continue;
      }
      case 58ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
        carryover_bits = 21;
        n -= 2;
        continue;
      }
      case 59ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        carryover = bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
        carryover_bits = 21;
        n -= 1;
        continue;
      }
      case 60ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        *output++ = bits::extractBits<uint64_t>(word, 0x0000000000007f00ULL);
        carryover = bits::extractBits<uint64_t>(word, 0x00007f7f7f7f0000ULL);
        carryover_bits = 28;
        n -= 2;
        continue;
      }
      case 61ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        carryover = bits::extractBits<uint64_t>(word, 0x00007f7f7f7f0000ULL);
        carryover_bits = 28;
        n -= 1;
        continue;
      }
      case 62ULL: {
        const uint64_t first_value =
            bits::extractBits<uint64_t>(word, 0x000000000000007fULL);
        *output++ = (first_value << carryover_bits) | carryover;
        carryover = bits::extractBits<uint64_t>(word, 0x00007f7f7f7f7f00ULL);
        carryover_bits = 35;
        n -= 1;
        continue;
      }
      case 63ULL: {
        carryover |= bits::extractBits<uint64_t>(word, 0x00007f7f7f7f7f7fULL)
            << carryover_bits;
        carryover_bits += 42;
        continue;
      }
      default: {
        throw std::logic_error(
            "It should be impossible for control bits to be > 31.");
      }
    }
  }
  pos += mask_len;
  if (n > 0) {
    constexpr size_t START_LEN = 10;
    size_t len = START_LEN;
    *output++ = (readVuLong(pos, len) << carryover_bits) | carryover;
    pos += (START_LEN - len);
    for (uint64_t i = 1; i < n; ++i) {
      len = START_LEN;
      *output++ = readVuLong(pos, len);
      pos += (START_LEN - len);
    }
  }
  return pos;
}

size_t writeVulongToBuffer(uint64_t val, char* buffer, size_t pos) {
  while (true) {
    if ((val & ~0x7f) == 0) {
      buffer[pos++] = static_cast<char>(val);
      return pos;
    } else {
      buffer[pos++] = static_cast<char>(
          0x80 | (val & facebook::velox::dwio::common::BASE_128_MASK));
      // cast val to unsigned so as to force 0-fill right shift
      val = (static_cast<uint64_t>(val) >> 7);
    }
  }
  return pos;
}

BENCHMARK(decodeOld_16) {
  size_t currentLen = len_u16;
  const size_t startingLen = len_u16;
  // Preserve the buffer, but change the start position of the buffer being
  // passed into readVuLong
  while (currentLen != 0) {
    auto result =
        readVuLong(buffer_u16.data() + (startingLen - currentLen), currentLen);
    folly::doNotOptimizeAway(result);
  }
}

BENCHMARK_RELATIVE(decodeNew_16) {
  readVuLongOptimized(
      randomInts_u16.size(), buffer_u16.data(), randomInts_u16_result.data());
}

BENCHMARK(decodeOld_32) {
  size_t currentLen = len_u32;
  const size_t startingLen = len_u32;
  int32_t i = 0;
  while (currentLen != 0) {
    auto result =
        readVuLong(buffer_u32.data() + (startingLen - currentLen), currentLen);
    folly::doNotOptimizeAway(result);
  }
}

BENCHMARK_RELATIVE(decodeNew_32) {
  readVuLongOptimized(
      randomInts_u32.size(), buffer_u32.data(), randomInts_u32_result.data());
}

BENCHMARK(decodeOld_64) {
  size_t currentLen = len_u64;
  const size_t startingLen = len_u64;
  while (currentLen != 0) {
    auto result =
        readVuLong(buffer_u64.data() + (startingLen - currentLen), currentLen);
    folly::doNotOptimizeAway(result);
  }
}

BENCHMARK_RELATIVE(decodeNew_64) {
  readVuLongOptimized(
      randomInts_u64.size(), buffer_u64.data(), randomInts_u64_result.data());
}

int32_t main(int32_t argc, char* argv[]) {
  folly::init(&argc, &argv);

  // Populate uint16 buffer
  buffer_u16.resize(kNumElements);
  size_t pos = 0;
  for (int32_t i = 0; i < 300000; i++) {
    auto randomInt = static_cast<uint16_t>(folly::Random::rand32());
    randomInts_u16.push_back(randomInt);
    pos = writeVulongToBuffer(randomInt, buffer_u16.data(), pos);
  }
  randomInts_u16_result.resize(randomInts_u16.size());
  len_u16 = pos;

  // Populate uint32 buffer
  buffer_u32.resize(kNumElements);
  pos = 0;
  for (int32_t i = 0; i < 200000; i++) {
    auto randomInt = folly::Random::rand32();
    randomInts_u32.push_back(randomInt);
    pos = writeVulongToBuffer(randomInt, buffer_u32.data(), pos);
  }
  randomInts_u32_result.resize(randomInts_u32.size());
  len_u32 = pos;

  // Populate uint32 buffer
  buffer_u64.resize(kNumElements);
  pos = 0;
  for (int32_t i = 0; i < 100000; i++) {
    auto randomInt = folly::Random::rand64();
    randomInts_u64.push_back(randomInt);
    pos = writeVulongToBuffer(randomInt, buffer_u64.data(), pos);
  }
  randomInts_u64_result.resize(randomInts_u64.size());
  len_u64 = pos;

  folly::runBenchmarks();
  return 0;
}
