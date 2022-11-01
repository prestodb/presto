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

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/BitPackDecoder.h"

#include <folly/Benchmark.h>
#include <folly/Random.h>
#include <folly/init/Init.h>

using namespace folly;
using namespace facebook::velox;

using RowSet = folly::Range<const facebook::velox::vector_size_t*>;

// Array of bit packed representations of randomInts_u32. The array at index i
// is packed i bits wide and the values come from the low bits of
std::vector<std::vector<uint64_t>> bitPackedData;

std::vector<uint32_t> result32;

std::vector<int32_t> allRowNumbers;
std::vector<int32_t> oddRowNumbers;
RowSet allRows;
RowSet oddRows;

static size_t len_u32 = 0;
std::vector<uint32_t> randomInts_u32;
std::vector<uint64_t> randomInts_u32_result;

static size_t len_u64 = 0;
std::vector<uint64_t> randomInts_u64;
std::vector<uint64_t> randomInts_u64_result;
std::vector<char> buffer_u64;

// Naive unpacking, original version of IntDecoder::unpack.
template <typename T>
void naiveDecodeBitsLE(
    const uint64_t* FOLLY_NONNULL bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    T* FOLLY_NONNULL result) {
  uint64_t mask = bits::lowMask(bitWidth);
  auto numRows = rows.size();
  if (bitWidth > 56) {
    for (auto i = 0; i < numRows; ++i) {
      auto bit = bitOffset + (rows[i] - rowBias) * bitWidth;
      result[i] = bits::detail::loadBits<T>(bits, bit, bitWidth) & mask;
    }
    return;
  }
  auto FOLLY_NONNULL lastSafe = bufferEnd - sizeof(uint64_t);
  int32_t numSafeRows = numRows;
  bool anyUnsafe = false;
  if (bufferEnd) {
    const char* endByte = reinterpret_cast<const char*>(bits) +
        bits::roundUp(bitOffset + (rows.back() - rowBias + 1) * bitWidth, 8) /
            8;
    // redzone is the number of bytes at the end of the accessed range that
    // could overflow the buffer if accessed 64 its wide.
    int64_t redZone =
        sizeof(uint64_t) - static_cast<int64_t>(bufferEnd - endByte);
    if (redZone > 0) {
      anyUnsafe = true;
      auto numRed = (redZone + 1) * 8 / bitWidth;
      int32_t lastSafeIndex = rows.back() - numRed;
      --numSafeRows;
      for (; numSafeRows >= 1; --numSafeRows) {
        if (rows[numSafeRows - 1] < lastSafeIndex) {
          break;
        }
      }
    }
  }
  for (auto i = 0; i < numSafeRows; ++i) {
    auto bit = bitOffset + (rows[i] - rowBias) * bitWidth;
    auto byte = bit / 8;
    auto shift = bit & 7;
    result[i] = (*reinterpret_cast<const uint64_t*>(
                     reinterpret_cast<const char*>(bits) + byte) >>
                 shift) &
        mask;
  }
  if (anyUnsafe) {
    auto lastSafeWord = bufferEnd - sizeof(uint64_t);
    VELOX_DCHECK(lastSafeWord);
    for (auto i = numSafeRows; i < numRows; ++i) {
      auto bit = bitOffset + (rows[i] - rowBias) * bitWidth;
      auto byte = bit / 8;
      auto shift = bit & 7;
      result[i] = facebook::velox::dwio::common::safeLoadBits(
                      reinterpret_cast<const char*>(bits) + byte,
                      shift,
                      bitWidth,
                      lastSafeWord) &
          mask;
    }
  }
}

template <typename T>
void unpackNaive(RowSet rows, uint8_t bitWidth, T* result) {
  auto data = bitPackedData[bitWidth].data();
  auto numBytes = bits::roundUp((rows.back() + 1) * bitWidth, 8) / 8;
  auto end = reinterpret_cast<const char*>(data) + numBytes;
  naiveDecodeBitsLE(data, 0, rows, 0, bitWidth, end, result32.data());
}

template <typename T>
void unpackFast(RowSet rows, uint8_t bitWidth, T* result) {
  auto data = bitPackedData[bitWidth].data();
  auto numBytes = bits::roundUp((rows.back() + 1) * bitWidth, 8) / 8;
  auto end = reinterpret_cast<const char*>(data) + numBytes;
  facebook::velox::dwio::common::unpack(
      data,
      0,
      rows,
      0,
      bitWidth,
      end,
      reinterpret_cast<int32_t*>(result32.data()));
}

#define BIT_BM_CASE_32(width)                       \
  BENCHMARK(unpackNaive##width##_32) {              \
    unpackNaive(allRows, width, result32.data());   \
  }                                                 \
                                                    \
  BENCHMARK_RELATIVE(unpackFast##width##_32) {      \
    unpackFast(allRows, width, result32.data());    \
  }                                                 \
                                                    \
  BENCHMARK_RELATIVE(unpackNaive##width##_32_odd) { \
    unpackNaive(oddRows, width, result32.data());   \
  }                                                 \
                                                    \
  BENCHMARK_RELATIVE(unpackFast##width##_32_odd) {  \
    unpackFast(oddRows, 7, result32.data());        \
  }

BIT_BM_CASE_32(7)
BIT_BM_CASE_32(8)
BIT_BM_CASE_32(13)
BIT_BM_CASE_32(16)
BIT_BM_CASE_32(22)
BIT_BM_CASE_32(24)
BIT_BM_CASE_32(31)

void populateBitPacked() {
  bitPackedData.resize(32);
  for (auto bitWidth = 2; bitWidth < 32; ++bitWidth) {
    auto numWords = bits::roundUp(randomInts_u32.size() * bitWidth, 64) / 64;
    bitPackedData[bitWidth].resize(numWords);
    auto source = reinterpret_cast<uint64_t*>(randomInts_u32.data());
    auto destination =
        reinterpret_cast<uint64_t*>(bitPackedData[bitWidth].data());
    for (auto i = 0; i < randomInts_u32.size(); ++i) {
      bits::copyBits(source, i * 32, destination, i * bitWidth, bitWidth);
    }
  }
  allRowNumbers.resize(randomInts_u32.size());
  std::iota(allRowNumbers.begin(), allRowNumbers.end(), 0);
  oddRowNumbers.resize(randomInts_u32.size() / 2);
  for (auto i = 0; i < oddRowNumbers.size(); i++) {
    oddRowNumbers[i] = i * 2 + 1;
  }
  allRows = RowSet(allRowNumbers);
  oddRows = RowSet(oddRowNumbers);
}

int32_t main(int32_t argc, char* argv[]) {
  folly::init(&argc, &argv);

  // Populate uint32 buffer
  for (int32_t i = 0; i < 200000; i++) {
    auto randomInt = folly::Random::rand32();
    randomInts_u32.push_back(randomInt);
  }
  randomInts_u32_result.resize(randomInts_u32.size());

  populateBitPacked();
  result32.resize(randomInts_u32.size());

  randomInts_u64_result.resize(randomInts_u64.size());

  folly::runBenchmarks();
  return 0;
}

#if 0
============================================================================
[...]mon/tests/BitPackDecoderBenchmark.cpp     relative  time/iter   iters/s
============================================================================
unpackNaive7_32                                             1.69ms    592.84
unpackFast7_32                                  98.914%     1.71ms    586.40
unpackNaive7_32_odd                             202.08%   834.74us     1.20K
unpackFast7_32_odd                              210.19%   802.51us     1.25K
unpackNaive8_32                                             1.53ms    652.69
unpackFast8_32                                  105.86%     1.45ms    690.93
unpackNaive8_32_odd                             209.97%   729.67us     1.37K
unpackFast8_32_odd                              215.89%   709.69us     1.41K
unpackNaive13_32                                            1.48ms    674.62
unpackFast13_32                                 101.05%     1.47ms    681.73
unpackNaive13_32_odd                             197.3%   751.28us     1.33K
unpackFast13_32_odd                             208.79%   709.95us     1.41K
unpackNaive16_32                                            1.46ms    683.77
unpackFast16_32                                 103.09%     1.42ms    704.92
unpackNaive16_32_odd                            191.29%   764.54us     1.31K
unpackFast16_32_odd                             206.14%   709.45us     1.41K
unpackNaive22_32                                            1.52ms    655.79
unpackFast22_32                                 107.48%     1.42ms    704.85
unpackNaive22_32_odd                            205.88%   740.67us     1.35K
unpackFast22_32_odd                             214.91%   709.56us     1.41K
unpackNaive24_32                                            1.47ms    682.47
unpackFast24_32                                  101.1%     1.45ms    689.99
unpackNaive24_32_odd                            198.33%   738.79us     1.35K
unpackFast24_32_odd                             206.57%   709.35us     1.41K
unpackNaive31_32                                            1.53ms    652.18
unpackFast31_32                                 102.87%     1.49ms    670.92
unpackNaive31_32_odd                            202.34%   757.79us     1.32K
unpackFast31_32_odd                             207.25%   739.84us     1.35K
#endif
