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

#ifdef __BMI2__
#include "velox/dwio/common/tests/Lemire/bmipacking32.h"
#endif

#include "velox/dwio/common/tests/Lemire/FastPFor/bitpackinghelpers.h"

#include <arrow/util/rle_encoding.h>
#include <folly/Benchmark.h>
#include <folly/Random.h>
#include <folly/init/Init.h>

#include <duckdb.hpp> // @manual

using namespace folly;
using namespace facebook::velox;

using RowSet = folly::Range<const facebook::velox::vector_size_t*>;

static const uint64_t kNumValues = 1024768 * 8;

namespace facebook::velox::parquet {

class ByteBuffer { // on to the 10 thousandth impl
 public:
  ByteBuffer() {}
  ByteBuffer(char* ptr, uint64_t len) : ptr(ptr), len(len) {}

  char* ptr = nullptr;
  uint64_t len = 0;

 public:
  void inc(uint64_t increment) {
    available(increment);
    len -= increment;
    ptr += increment;
  }

  template <class T>
  T read() {
    T val = get<T>();
    inc(sizeof(T));
    return val;
  }

  template <class T>
  T get() {
    available(sizeof(T));
    T val = duckdb::Load<T>((duckdb::data_ptr_t)ptr);
    return val;
  }

  void copy_to(char* dest, uint64_t len) {
    available(len);
    std::memcpy(dest, ptr, len);
  }

  void zero() {
    std::memset(ptr, 0, len);
  }

  void available(uint64_t req_len) {
    if (req_len > len) {
      throw std::runtime_error("Out of buffer");
    }
  }
};

class ParquetDecodeUtils {
 public:
  template <class T>
  static T ZigzagToInt(const T& n) {
    return (n >> 1) ^ -(n & 1);
  }

  static const uint64_t BITPACK_MASKS[];
  static const uint64_t BITPACK_MASKS_SIZE;
  static const uint8_t BITPACK_DLEN;

  template <typename T>
  static uint32_t BitUnpack(
      ByteBuffer& buffer,
      uint8_t& bitpack_pos,
      T* dest,
      uint32_t count,
      uint8_t width) {
    if (width >= ParquetDecodeUtils::BITPACK_MASKS_SIZE) {
      throw duckdb::InvalidInputException(
          "The width (%d) of the bitpacked data exceeds the supported max width (%d), "
          "the file might be corrupted.",
          width,
          ParquetDecodeUtils::BITPACK_MASKS_SIZE);
    }
    auto mask = BITPACK_MASKS[width];

    for (uint32_t i = 0; i < count; i++) {
      T val = (buffer.get<uint8_t>() >> bitpack_pos) & mask;
      bitpack_pos += width;
      while (bitpack_pos > BITPACK_DLEN) {
        buffer.inc(1);
        val |= (T(buffer.get<uint8_t>())
                << T(BITPACK_DLEN - (bitpack_pos - width))) &
            mask;
        bitpack_pos -= BITPACK_DLEN;
      }
      dest[i] = val;
    }
    return count;
  }

  template <class T>
  static T VarintDecode(ByteBuffer& buf) {
    T result = 0;
    uint8_t shift = 0;
    while (true) {
      auto byte = buf.read<uint8_t>();
      result |= T(byte & 127) << shift;
      if ((byte & 128) == 0) {
        break;
      }
      shift += 7;
      if (shift > sizeof(T) * 8) {
        throw std::runtime_error("Varint-decoding found too large number");
      }
    }
    return result;
  }
};
} // namespace facebook::velox::parquet

const uint64_t facebook::velox::parquet::ParquetDecodeUtils::BITPACK_MASKS[] = {
    0,
    1,
    3,
    7,
    15,
    31,
    63,
    127,
    255,
    511,
    1023,
    2047,
    4095,
    8191,
    16383,
    32767,
    65535,
    131071,
    262143,
    524287,
    1048575,
    2097151,
    4194303,
    8388607,
    16777215,
    33554431,
    67108863,
    134217727,
    268435455,
    536870911,
    1073741823,
    2147483647,
    4294967295,
    8589934591,
    17179869183,
    34359738367,
    68719476735,
    137438953471,
    274877906943,
    549755813887,
    1099511627775,
    2199023255551,
    4398046511103,
    8796093022207,
    17592186044415,
    35184372088831,
    70368744177663,
    140737488355327,
    281474976710655,
    562949953421311,
    1125899906842623,
    2251799813685247,
    4503599627370495,
    9007199254740991,
    18014398509481983,
    36028797018963967,
    72057594037927935,
    144115188075855871,
    288230376151711743,
    576460752303423487,
    1152921504606846975,
    2305843009213693951,
    4611686018427387903,
    9223372036854775807,
    18446744073709551615ULL};

const uint64_t
    facebook::velox::parquet::ParquetDecodeUtils::BITPACK_MASKS_SIZE =
        sizeof(ParquetDecodeUtils::BITPACK_MASKS) / sizeof(uint64_t);

const uint8_t facebook::velox::parquet::ParquetDecodeUtils::BITPACK_DLEN = 8;

// Array of bit packed representations of randomInts_u32. The array at index i
// is packed i bits wide and the values come from the low bits of
std::vector<std::vector<uint64_t>> bitPackedData;

std::vector<uint8_t> result8;
std::vector<uint16_t> result16;
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

#define BYTES(numValues, bitWidth) (numValues * bitWidth + 7) / 8

template <typename T>
void naiveDecodeBitsLE(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    T* result);

template <typename T>
void legacyUnpackNaive(RowSet rows, uint8_t bitWidth, T* result) {
  auto data = bitPackedData[bitWidth].data();
  auto numBytes = bits::divRoundUp((rows.back() + 1) * bitWidth, 8);
  auto end = reinterpret_cast<const char*>(data) + numBytes;
  naiveDecodeBitsLE(data, 0, rows, 0, bitWidth, end, result32.data());
}

template <typename T>
void legacyUnpackFast(RowSet rows, uint8_t bitWidth, T* result) {
  auto data = bitPackedData[bitWidth].data();
  auto numBytes = bits::divRoundUp((rows.back() + 1) * bitWidth, 8);
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

template <typename T>
void veloxBitUnpack(uint8_t bitWidth, T* result) {
  const uint8_t* inputIter =
      reinterpret_cast<const uint8_t*>(bitPackedData[bitWidth].data());
  facebook::velox::dwio::common::unpack<T>(
      inputIter, BYTES(kNumValues, bitWidth), kNumValues, bitWidth, result);
}

template <typename T>
void fastpforlib(uint8_t bitWidth, T* result) {
  uint64_t numBatches = kNumValues / 32;
  auto inputBuffer = reinterpret_cast<const T*>(bitPackedData[bitWidth].data());
  for (auto i = 0; i < numBatches; i++) {
    // Read 4 bytes and unpack 32 values
    velox::fastpforlib::fastunpack(
        inputBuffer + i * 4, result + i * 32, bitWidth);
  }
}

void lemirebmi2(uint8_t bitWidth, uint32_t* result) {
  const uint8_t* inputIter =
      reinterpret_cast<const uint8_t*>(bitPackedData[bitWidth].data());

#ifdef __BMI2__
  bmiunpack32(inputIter, kNumValues, bitWidth, result);
#else
  VELOX_FAIL("bmiunpack32 is not supported on this platform.");
#endif
}

template <typename T>
void arrowBitUnpack(uint8_t bitWidth, T* result) {
  arrow::bit_util::BitReader bitReader(
      reinterpret_cast<const uint8_t*>(bitPackedData[bitWidth].data()),
      BYTES(kNumValues, bitWidth));
  bitReader.GetBatch<T>(bitWidth, result, kNumValues);
}

template <typename T>
void duckdbBitUnpack(uint8_t bitWidth, T* result) {
  facebook::velox::parquet::ByteBuffer duckInputBuffer(
      reinterpret_cast<char*>(bitPackedData[bitWidth].data()),
      BYTES(kNumValues, bitWidth));
  uint8_t bitpack_pos = 0;
  facebook::velox::parquet::ParquetDecodeUtils::BitUnpack<T>(
      duckInputBuffer, bitpack_pos, result, kNumValues, bitWidth);
}

#define BENCHMARK_UNPACK_FULLROWS_CASE_8(width)                  \
  BENCHMARK(velox_unpack_fullrows_##width##_8) {                 \
    veloxBitUnpack<uint8_t>(width, result8.data());              \
  }                                                              \
  BENCHMARK_RELATIVE(legacy_unpack_naive_fullrows_##width##_8) { \
    legacyUnpackNaive<uint8_t>(allRows, width, result8.data());  \
  }                                                              \
  BENCHMARK_RELATIVE(legacy_unpack_fast_fullrows_##width##_8) {  \
    legacyUnpackFast<uint8_t>(allRows, width, result8.data());   \
  }                                                              \
  BENCHMARK_RELATIVE(fastpforlib_unpack_fullrows_##width##_8) {  \
    fastpforlib<uint8_t>(width, result8.data());                 \
  }                                                              \
  BENCHMARK_RELATIVE(arrow_unpack_fullrows_##width##_8) {        \
    arrowBitUnpack<uint8_t>(width, result8.data());              \
  }                                                              \
  BENCHMARK_RELATIVE(duckdb_unpack_fullrows_##width##_8) {       \
    duckdbBitUnpack<uint8_t>(width, result8.data());             \
  }                                                              \
  BENCHMARK_DRAW_LINE();

#define BENCHMARK_UNPACK_FULLROWS_CASE_16(width)                  \
  BENCHMARK(velox_unpack_fullrows_##width##_16) {                 \
    veloxBitUnpack<uint16_t>(width, result16.data());             \
  }                                                               \
  BENCHMARK_RELATIVE(legacy_unpack_naive_fullrows_##width##_16) { \
    legacyUnpackNaive<uint16_t>(allRows, width, result16.data()); \
  }                                                               \
  BENCHMARK_RELATIVE(legacy_unpack_fast_fullrows_##width##_16) {  \
    legacyUnpackFast<uint16_t>(allRows, width, result16.data());  \
  }                                                               \
  BENCHMARK_RELATIVE(fastpforlib_unpack_fullrows_##width##_16) {  \
    fastpforlib<uint16_t>(width, result16.data());                \
  }                                                               \
  BENCHMARK_RELATIVE(arrow_unpack_fullrows_##width##_16) {        \
    arrowBitUnpack<uint16_t>(width, result16.data());             \
  }                                                               \
  BENCHMARK_RELATIVE(duckdb_unpack_fullrows_##width##_16) {       \
    duckdbBitUnpack<uint16_t>(width, result16.data());            \
  }                                                               \
  BENCHMARK_DRAW_LINE();

#define BENCHMARK_UNPACK_FULLROWS_CASE_32(width)                  \
  BENCHMARK(velox_unpack_fullrows_##width##_32) {                 \
    veloxBitUnpack<uint32_t>(width, result32.data());             \
  }                                                               \
  BENCHMARK_RELATIVE(legacy_unpack_naive_fullrows_##width##_32) { \
    legacyUnpackNaive<uint32_t>(allRows, width, result32.data()); \
  }                                                               \
  BENCHMARK_RELATIVE(legacy_unpack_fast_fullrows_##width##_32) {  \
    legacyUnpackFast<uint32_t>(allRows, width, result32.data());  \
  }                                                               \
  BENCHMARK_RELATIVE(lemirebmi_unpack_fullrows_##width##_32) {    \
    lemirebmi2(width, result32.data());                           \
  }                                                               \
  BENCHMARK_RELATIVE(fastpforlib_unpack_fullrows_##width##_32) {  \
    fastpforlib<uint32_t>(width, result32.data());                \
  }                                                               \
  BENCHMARK_RELATIVE(arrow_unpack_fullrows_##width##_32) {        \
    arrowBitUnpack<uint32_t>(width, result32.data());             \
  }                                                               \
  BENCHMARK_RELATIVE(duckdb_unpack_fullrows_##width##_32) {       \
    duckdbBitUnpack<uint32_t>(width, result32.data());            \
  }                                                               \
  BENCHMARK_DRAW_LINE();

#define BENCHMARK_UNPACK_ODDROWS_CASE_8(width)                  \
  BENCHMARK_RELATIVE(legacy_unpack_naive_oddrows_##width##_8) { \
    legacyUnpackNaive<uint8_t>(oddRows, width, result8.data()); \
  }                                                             \
  BENCHMARK_RELATIVE(legacy_unpack_fast_oddrows_##width##_8) {  \
    legacyUnpackFast<uint8_t>(oddRows, width, result8.data());  \
  }                                                             \
  BENCHMARK_DRAW_LINE();

#define BENCHMARK_UNPACK_ODDROWS_CASE_16(width)                   \
  BENCHMARK_RELATIVE(legacy_unpack_naive_oddrows_##width##_16) {  \
    legacyUnpackNaive<uint16_t>(oddRows, width, result16.data()); \
  }                                                               \
  BENCHMARK_RELATIVE(legacy_unpack_fast_oddrows_##width##_16) {   \
    legacyUnpackFast<uint16_t>(oddRows, width, result16.data());  \
  }                                                               \
  BENCHMARK_DRAW_LINE();

#define BENCHMARK_UNPACK_ODDROWS_CASE_32(width)                   \
  BENCHMARK_RELATIVE(legacy_unpack_naive_oddrows_##width##_32) {  \
    legacyUnpackNaive<uint32_t>(oddRows, width, result32.data()); \
  }                                                               \
  BENCHMARK_RELATIVE(legacy_unpack_fast_oddrows_##width##_32) {   \
    legacyUnpackFast<uint32_t>(oddRows, width, result32.data());  \
  }                                                               \
  BENCHMARK_DRAW_LINE();

BENCHMARK_UNPACK_FULLROWS_CASE_8(1)
BENCHMARK_UNPACK_FULLROWS_CASE_8(2)
BENCHMARK_UNPACK_FULLROWS_CASE_8(3)
BENCHMARK_UNPACK_FULLROWS_CASE_8(4)
BENCHMARK_UNPACK_FULLROWS_CASE_8(5)
BENCHMARK_UNPACK_FULLROWS_CASE_8(6)
BENCHMARK_UNPACK_FULLROWS_CASE_8(7)
BENCHMARK_UNPACK_FULLROWS_CASE_8(8)

BENCHMARK_DRAW_LINE();

BENCHMARK_UNPACK_FULLROWS_CASE_16(1)
BENCHMARK_UNPACK_FULLROWS_CASE_16(2)
BENCHMARK_UNPACK_FULLROWS_CASE_16(3)
BENCHMARK_UNPACK_FULLROWS_CASE_16(4)
BENCHMARK_UNPACK_FULLROWS_CASE_16(5)
BENCHMARK_UNPACK_FULLROWS_CASE_16(6)
BENCHMARK_UNPACK_FULLROWS_CASE_16(7)
BENCHMARK_UNPACK_FULLROWS_CASE_16(8)
BENCHMARK_UNPACK_FULLROWS_CASE_16(9)
BENCHMARK_UNPACK_FULLROWS_CASE_16(10)
BENCHMARK_UNPACK_FULLROWS_CASE_16(11)
BENCHMARK_UNPACK_FULLROWS_CASE_16(12)
BENCHMARK_UNPACK_FULLROWS_CASE_16(13)
BENCHMARK_UNPACK_FULLROWS_CASE_16(14)
BENCHMARK_UNPACK_FULLROWS_CASE_16(15)
BENCHMARK_UNPACK_FULLROWS_CASE_16(16)

BENCHMARK_DRAW_LINE();

BENCHMARK_UNPACK_FULLROWS_CASE_32(1)
BENCHMARK_UNPACK_FULLROWS_CASE_32(2)
BENCHMARK_UNPACK_FULLROWS_CASE_32(3)
BENCHMARK_UNPACK_FULLROWS_CASE_32(4)
BENCHMARK_UNPACK_FULLROWS_CASE_32(5)
BENCHMARK_UNPACK_FULLROWS_CASE_32(6)
BENCHMARK_UNPACK_FULLROWS_CASE_32(7)
BENCHMARK_UNPACK_FULLROWS_CASE_32(8)
BENCHMARK_UNPACK_FULLROWS_CASE_32(9)
BENCHMARK_UNPACK_FULLROWS_CASE_32(10)
BENCHMARK_UNPACK_FULLROWS_CASE_32(11)
BENCHMARK_UNPACK_FULLROWS_CASE_32(13)
BENCHMARK_UNPACK_FULLROWS_CASE_32(15)
BENCHMARK_UNPACK_FULLROWS_CASE_32(17)
BENCHMARK_UNPACK_FULLROWS_CASE_32(19)
BENCHMARK_UNPACK_FULLROWS_CASE_32(21)
BENCHMARK_UNPACK_FULLROWS_CASE_32(24)
BENCHMARK_UNPACK_FULLROWS_CASE_32(28)
BENCHMARK_UNPACK_FULLROWS_CASE_32(30)
BENCHMARK_UNPACK_FULLROWS_CASE_32(32)

BENCHMARK_DRAW_LINE();

BENCHMARK_UNPACK_ODDROWS_CASE_8(1)
BENCHMARK_UNPACK_ODDROWS_CASE_8(2)
BENCHMARK_UNPACK_ODDROWS_CASE_8(4)
BENCHMARK_UNPACK_ODDROWS_CASE_8(8)

BENCHMARK_DRAW_LINE();

BENCHMARK_UNPACK_ODDROWS_CASE_16(1)
BENCHMARK_UNPACK_ODDROWS_CASE_16(2)
BENCHMARK_UNPACK_ODDROWS_CASE_16(4)
BENCHMARK_UNPACK_ODDROWS_CASE_16(8)
BENCHMARK_UNPACK_ODDROWS_CASE_16(10)
BENCHMARK_UNPACK_ODDROWS_CASE_16(12)
BENCHMARK_UNPACK_ODDROWS_CASE_16(14)
BENCHMARK_UNPACK_ODDROWS_CASE_16(16)

BENCHMARK_DRAW_LINE();

BENCHMARK_UNPACK_ODDROWS_CASE_32(1)
BENCHMARK_UNPACK_ODDROWS_CASE_32(2)
BENCHMARK_UNPACK_ODDROWS_CASE_32(4)
BENCHMARK_UNPACK_ODDROWS_CASE_32(7)
BENCHMARK_UNPACK_ODDROWS_CASE_32(8)
BENCHMARK_UNPACK_ODDROWS_CASE_32(13)
BENCHMARK_UNPACK_ODDROWS_CASE_32(16)
BENCHMARK_UNPACK_ODDROWS_CASE_32(22)
BENCHMARK_UNPACK_ODDROWS_CASE_32(24)
BENCHMARK_UNPACK_ODDROWS_CASE_32(31)

void populateBitPacked() {
  bitPackedData.resize(33);
  for (auto bitWidth = 1; bitWidth <= 32; ++bitWidth) {
    auto numWords = bits::divRoundUp(randomInts_u32.size() * bitWidth, 64);
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

// Naive unpacking, original version of IntDecoder::unpack.
template <typename T>
void naiveDecodeBitsLE(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    T* result) {
  uint64_t mask = bits::lowMask(bitWidth);
  auto numRows = rows.size();
  if (bitWidth > 56) {
    for (auto i = 0; i < numRows; ++i) {
      auto bit = bitOffset + (rows[i] - rowBias) * bitWidth;
      result[i] = bits::detail::loadBits<T>(bits, bit, bitWidth) & mask;
    }
    return;
  }
  auto lastSafe = bufferEnd - sizeof(uint64_t);
  int32_t numSafeRows = numRows;
  bool anyUnsafe = false;
  if (bufferEnd) {
    const char* endByte = reinterpret_cast<const char*>(bits) +
        bits::divRoundUp(bitOffset + (rows.back() - rowBias + 1) * bitWidth, 8);
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

int32_t main(int32_t argc, char* argv[]) {
  folly::Init init{&argc, &argv};

  // Populate uint32 buffer

  for (int32_t i = 0; i < kNumValues; i++) {
    auto randomInt = folly::Random::rand32();
    randomInts_u32.push_back(randomInt);
  }
  randomInts_u32_result.resize(randomInts_u32.size());

  populateBitPacked();

  result8.resize(randomInts_u32.size());
  result16.resize(randomInts_u32.size());
  result32.resize(randomInts_u32.size());

  randomInts_u64_result.resize(randomInts_u64.size());

  folly::runBenchmarks();
  return 0;
}

/*
============================================================================
[...]mon/tests/BitPackDecoderBenchmark.cpp     relative  time/iter   iters/s
============================================================================
velox_unpack_fullrows_1_8                                 314.64us     3.18K
legacy_unpack_naive_fullrows_1_8                2.7926%    11.27ms     88.76
legacy_unpack_fast_fullrows_1_8                 8.3402%     3.77ms    265.07
fastpforlib_unpack_fullrows_1_8                  5.585%     5.63ms    177.51
arrow_unpack_fullrows_1_8                       10.285%     3.06ms    326.90
duckdb_unpack_fullrows_1_8                      2.3676%    13.29ms     75.25
----------------------------------------------------------------------------
velox_unpack_fullrows_2_8                                 434.85us     2.30K
legacy_unpack_naive_fullrows_2_8                3.5301%    12.32ms     81.18
legacy_unpack_fast_fullrows_2_8                 12.563%     3.46ms    288.91
fastpforlib_unpack_fullrows_2_8                 9.2229%     4.71ms    212.09
arrow_unpack_fullrows_2_8                       15.439%     2.82ms    355.04
duckdb_unpack_fullrows_2_8                      3.4825%    12.49ms     80.09
----------------------------------------------------------------------------
velox_unpack_fullrows_3_8                                 323.24us     3.09K
legacy_unpack_naive_fullrows_3_8                3.2279%    10.01ms     99.86
legacy_unpack_fast_fullrows_3_8                 9.1099%     3.55ms    281.83
fastpforlib_unpack_fullrows_3_8                 6.8442%     4.72ms    211.74
arrow_unpack_fullrows_3_8                       11.318%     2.86ms    350.16
duckdb_unpack_fullrows_3_8                      2.3649%    13.67ms     73.16
----------------------------------------------------------------------------
velox_unpack_fullrows_4_8                                 356.01us     2.81K
legacy_unpack_naive_fullrows_4_8                3.6289%     9.81ms    101.93
legacy_unpack_fast_fullrows_4_8                 10.344%     3.44ms    290.55
fastpforlib_unpack_fullrows_4_8                 9.7341%     3.66ms    273.42
arrow_unpack_fullrows_4_8                       12.441%     2.86ms    349.44
duckdb_unpack_fullrows_4_8                      2.4215%    14.70ms     68.02
----------------------------------------------------------------------------
velox_unpack_fullrows_5_8                                 358.09us     2.79K
legacy_unpack_naive_fullrows_5_8                3.6498%     9.81ms    101.92
legacy_unpack_fast_fullrows_5_8                 10.219%     3.50ms    285.38
fastpforlib_unpack_fullrows_5_8                 7.6934%     4.65ms    214.84
arrow_unpack_fullrows_5_8                       11.396%     3.14ms    318.24
duckdb_unpack_fullrows_5_8                      2.2256%    16.09ms     62.15
----------------------------------------------------------------------------
velox_unpack_fullrows_6_8                                 429.08us     2.33K
legacy_unpack_naive_fullrows_6_8                4.3034%     9.97ms    100.29
legacy_unpack_fast_fullrows_6_8                 11.773%     3.64ms    274.36
fastpforlib_unpack_fullrows_6_8                 9.8848%     4.34ms    230.37
arrow_unpack_fullrows_6_8                         13.3%     3.23ms    309.96
duckdb_unpack_fullrows_6_8                      2.4617%    17.43ms     57.37
----------------------------------------------------------------------------
velox_unpack_fullrows_7_8                                 472.86us     2.11K
legacy_unpack_naive_fullrows_7_8                4.7892%     9.87ms    101.28
legacy_unpack_fast_fullrows_7_8                 13.528%     3.50ms    286.09
fastpforlib_unpack_fullrows_7_8                  8.671%     5.45ms    183.37
arrow_unpack_fullrows_7_8                       14.401%     3.28ms    304.55
duckdb_unpack_fullrows_7_8                      2.5374%    18.64ms     53.66
----------------------------------------------------------------------------
velox_unpack_fullrows_8_8                                 420.84us     2.38K
legacy_unpack_naive_fullrows_8_8                4.2062%    10.01ms     99.95
legacy_unpack_fast_fullrows_8_8                 12.264%     3.43ms    291.42
fastpforlib_unpack_fullrows_8_8                 12.819%     3.28ms    304.61
arrow_unpack_fullrows_8_8                       11.558%     3.64ms    274.64
duckdb_unpack_fullrows_8_8                      1.7907%    23.50ms     42.55
----------------------------------------------------------------------------
----------------------------------------------------------------------------
velox_unpack_fullrows_1_16                                668.27us     1.50K
legacy_unpack_naive_fullrows_1_16               5.6819%    11.76ms     85.02
legacy_unpack_fast_fullrows_1_16                19.364%     3.45ms    289.76
fastpforlib_unpack_fullrows_1_16                23.448%     2.85ms    350.88
arrow_unpack_fullrows_1_16                       25.77%     2.59ms    385.62
duckdb_unpack_fullrows_1_16                     6.2749%    10.65ms     93.90
----------------------------------------------------------------------------
velox_unpack_fullrows_2_16                                725.07us     1.38K
legacy_unpack_naive_fullrows_2_16               7.1637%    10.12ms     98.80
legacy_unpack_fast_fullrows_2_16                22.684%     3.20ms    312.86
fastpforlib_unpack_fullrows_2_16                30.943%     2.34ms    426.76
arrow_unpack_fullrows_2_16                      27.889%     2.60ms    384.64
duckdb_unpack_fullrows_2_16                      5.935%    12.22ms     81.85
----------------------------------------------------------------------------
velox_unpack_fullrows_3_16                                768.55us     1.30K
legacy_unpack_naive_fullrows_3_16               7.7823%     9.88ms    101.26
legacy_unpack_fast_fullrows_3_16                21.779%     3.53ms    283.38
fastpforlib_unpack_fullrows_3_16                29.173%     2.63ms    379.59
arrow_unpack_fullrows_3_16                      27.444%     2.80ms    357.08
duckdb_unpack_fullrows_3_16                     5.7569%    13.35ms     74.91
----------------------------------------------------------------------------
velox_unpack_fullrows_4_16                                844.47us     1.18K
legacy_unpack_naive_fullrows_4_16               8.5395%     9.89ms    101.12
legacy_unpack_fast_fullrows_4_16                 24.28%     3.48ms    287.51
fastpforlib_unpack_fullrows_4_16                35.308%     2.39ms    418.11
arrow_unpack_fullrows_4_16                       32.53%     2.60ms    385.21
duckdb_unpack_fullrows_4_16                     5.8245%    14.50ms     68.97
----------------------------------------------------------------------------
velox_unpack_fullrows_5_16                                876.32us     1.14K
legacy_unpack_naive_fullrows_5_16               8.9603%     9.78ms    102.25
legacy_unpack_fast_fullrows_5_16                24.301%     3.61ms    277.30
fastpforlib_unpack_fullrows_5_16                28.944%     3.03ms    330.29
arrow_unpack_fullrows_5_16                       28.49%     3.08ms    325.11
duckdb_unpack_fullrows_5_16                     5.3094%    16.50ms     60.59
----------------------------------------------------------------------------
velox_unpack_fullrows_6_16                                986.12us     1.01K
legacy_unpack_naive_fullrows_6_16               8.4883%    11.62ms     86.08
legacy_unpack_fast_fullrows_6_16                26.564%     3.71ms    269.38
fastpforlib_unpack_fullrows_6_16                32.394%     3.04ms    328.50
arrow_unpack_fullrows_6_16                      31.138%     3.17ms    315.76
duckdb_unpack_fullrows_6_16                     4.8514%    20.33ms     49.20
----------------------------------------------------------------------------
velox_unpack_fullrows_7_16                                  1.21ms    828.23
legacy_unpack_naive_fullrows_7_16               11.843%    10.19ms     98.09
legacy_unpack_fast_fullrows_7_16                32.936%     3.67ms    272.79
fastpforlib_unpack_fullrows_7_16                 35.16%     3.43ms    291.20
arrow_unpack_fullrows_7_16                      36.047%     3.35ms    298.55
duckdb_unpack_fullrows_7_16                     6.0844%    19.84ms     50.39
----------------------------------------------------------------------------
velox_unpack_fullrows_8_16                                990.48us     1.01K
legacy_unpack_naive_fullrows_8_16               10.072%     9.83ms    101.69
legacy_unpack_fast_fullrows_8_16                30.115%     3.29ms    304.05
fastpforlib_unpack_fullrows_8_16                41.287%     2.40ms    416.84
arrow_unpack_fullrows_8_16                      34.355%     2.88ms    346.85
duckdb_unpack_fullrows_8_16                     4.7893%    20.68ms     48.35
----------------------------------------------------------------------------
velox_unpack_fullrows_9_16                                  1.20ms    831.97
legacy_unpack_naive_fullrows_9_16               11.855%    10.14ms     98.63
legacy_unpack_fast_fullrows_9_16                30.704%     3.91ms    255.45
fastpforlib_unpack_fullrows_9_16                35.125%     3.42ms    292.23
arrow_unpack_fullrows_9_16                      34.646%     3.47ms    288.24
duckdb_unpack_fullrows_9_16                     5.3386%    22.51ms     44.42
----------------------------------------------------------------------------
velox_unpack_fullrows_10_16                                 1.16ms    863.83
legacy_unpack_naive_fullrows_10_16              11.709%     9.89ms    101.15
legacy_unpack_fast_fullrows_10_16               29.118%     3.98ms    251.54
fastpforlib_unpack_fullrows_10_16                34.09%     3.40ms    294.48
arrow_unpack_fullrows_10_16                     33.373%     3.47ms    288.29
duckdb_unpack_fullrows_10_16                    4.9117%    23.57ms     42.43
----------------------------------------------------------------------------
velox_unpack_fullrows_11_16                                 1.40ms    714.78
legacy_unpack_naive_fullrows_11_16              14.052%     9.96ms    100.44
legacy_unpack_fast_fullrows_11_16                35.24%     3.97ms    251.89
fastpforlib_unpack_fullrows_11_16               38.379%     3.65ms    274.33
arrow_unpack_fullrows_11_16                     39.087%     3.58ms    279.39
duckdb_unpack_fullrows_11_16                    5.5651%    25.14ms     39.78
----------------------------------------------------------------------------
velox_unpack_fullrows_12_16                                 1.32ms    758.24
legacy_unpack_naive_fullrows_12_16              13.183%    10.00ms     99.96
legacy_unpack_fast_fullrows_12_16               33.508%     3.94ms    254.07
fastpforlib_unpack_fullrows_12_16               39.036%     3.38ms    295.98
arrow_unpack_fullrows_12_16                     36.912%     3.57ms    279.88
duckdb_unpack_fullrows_12_16                    4.9557%    26.61ms     37.58
----------------------------------------------------------------------------
velox_unpack_fullrows_13_16                                 1.61ms    620.98
legacy_unpack_naive_fullrows_13_16              15.984%    10.07ms     99.26
legacy_unpack_fast_fullrows_13_16               36.913%     4.36ms    229.23
fastpforlib_unpack_fullrows_13_16               33.991%     4.74ms    211.08
arrow_unpack_fullrows_13_16                     35.368%     4.55ms    219.63
duckdb_unpack_fullrows_13_16                    3.8358%    41.98ms     23.82
----------------------------------------------------------------------------
velox_unpack_fullrows_14_16                                 1.84ms    544.93
legacy_unpack_naive_fullrows_14_16              15.291%    12.00ms     83.32
legacy_unpack_fast_fullrows_14_16                40.69%     4.51ms    221.73
fastpforlib_unpack_fullrows_14_16               38.806%     4.73ms    211.46
arrow_unpack_fullrows_14_16                     48.812%     3.76ms    265.99
duckdb_unpack_fullrows_14_16                    5.3427%    34.35ms     29.11
----------------------------------------------------------------------------
velox_unpack_fullrows_15_16                                 1.64ms    610.46
legacy_unpack_naive_fullrows_15_16              16.487%     9.94ms    100.64
legacy_unpack_fast_fullrows_15_16               39.442%     4.15ms    240.78
fastpforlib_unpack_fullrows_15_16                 40.7%     4.02ms    248.46
arrow_unpack_fullrows_15_16                      33.76%     4.85ms    206.09
duckdb_unpack_fullrows_15_16                    4.3563%    37.60ms     26.59
----------------------------------------------------------------------------
velox_unpack_fullrows_16_16                                 1.33ms    750.01
legacy_unpack_naive_fullrows_16_16              13.689%     9.74ms    102.67
legacy_unpack_fast_fullrows_16_16               31.838%     4.19ms    238.79
fastpforlib_unpack_fullrows_16_16               59.941%     2.22ms    449.57
arrow_unpack_fullrows_16_16                     42.795%     3.12ms    320.97
duckdb_unpack_fullrows_16_16                    4.1505%    32.12ms     31.13
----------------------------------------------------------------------------
----------------------------------------------------------------------------
velox_unpack_fullrows_1_32                                  1.67ms    598.18
legacy_unpack_naive_fullrows_1_32                 17.4%     9.61ms    104.08
legacy_unpack_fast_fullrows_1_32                52.314%     3.20ms    312.93
lemirebmi_unpack_fullrows_1_32                  102.83%     1.63ms    615.08
fastpforlib_unpack_fullrows_1_32                64.787%     2.58ms    387.54
arrow_unpack_fullrows_1_32                      79.467%     2.10ms    475.36
duckdb_unpack_fullrows_1_32                      15.51%    10.78ms     92.78
----------------------------------------------------------------------------
velox_unpack_fullrows_2_32                                  1.62ms    618.83
legacy_unpack_naive_fullrows_2_32               16.948%     9.54ms    104.88
legacy_unpack_fast_fullrows_2_32                49.479%     3.27ms    306.19
lemirebmi_unpack_fullrows_2_32                  97.126%     1.66ms    601.05
fastpforlib_unpack_fullrows_2_32                62.528%     2.58ms    386.94
arrow_unpack_fullrows_2_32                       79.57%     2.03ms    492.40
duckdb_unpack_fullrows_2_32                     13.563%    11.91ms     83.93
----------------------------------------------------------------------------
velox_unpack_fullrows_3_32                                  1.64ms    608.80
legacy_unpack_naive_fullrows_3_32               17.116%     9.60ms    104.20
legacy_unpack_fast_fullrows_3_32                50.764%     3.24ms    309.06
lemirebmi_unpack_fullrows_3_32                  96.712%     1.70ms    588.79
fastpforlib_unpack_fullrows_3_32                56.986%     2.88ms    346.94
arrow_unpack_fullrows_3_32                      72.871%     2.25ms    443.64
duckdb_unpack_fullrows_3_32                     13.244%    12.40ms     80.63
----------------------------------------------------------------------------
velox_unpack_fullrows_4_32                                  1.59ms    630.64
legacy_unpack_naive_fullrows_4_32               17.072%     9.29ms    107.66
legacy_unpack_fast_fullrows_4_32                47.828%     3.32ms    301.62
lemirebmi_unpack_fullrows_4_32                   99.26%     1.60ms    625.97
fastpforlib_unpack_fullrows_4_32                63.302%     2.50ms    399.21
arrow_unpack_fullrows_4_32                       82.05%     1.93ms    517.44
duckdb_unpack_fullrows_4_32                     11.724%    13.52ms     73.94
----------------------------------------------------------------------------
velox_unpack_fullrows_5_32                                  1.61ms    622.75
legacy_unpack_naive_fullrows_5_32               17.482%     9.19ms    108.87
legacy_unpack_fast_fullrows_5_32                49.311%     3.26ms    307.08
lemirebmi_unpack_fullrows_5_32                  97.955%     1.64ms    610.01
fastpforlib_unpack_fullrows_5_32                57.233%     2.81ms    356.42
arrow_unpack_fullrows_5_32                      61.216%     2.62ms    381.22
duckdb_unpack_fullrows_5_32                     10.975%    14.63ms     68.35
----------------------------------------------------------------------------
velox_unpack_fullrows_6_32                                  1.70ms    587.36
legacy_unpack_naive_fullrows_6_32                18.31%     9.30ms    107.55
legacy_unpack_fast_fullrows_6_32                50.368%     3.38ms    295.84
lemirebmi_unpack_fullrows_6_32                  78.811%     2.16ms    462.90
fastpforlib_unpack_fullrows_6_32                60.457%     2.82ms    355.10
arrow_unpack_fullrows_6_32                      63.338%     2.69ms    372.02
duckdb_unpack_fullrows_6_32                     10.558%    16.13ms     62.01
----------------------------------------------------------------------------
velox_unpack_fullrows_7_32                                  1.79ms    559.53
legacy_unpack_naive_fullrows_7_32               19.351%     9.24ms    108.27
legacy_unpack_fast_fullrows_7_32                51.346%     3.48ms    287.30
lemirebmi_unpack_fullrows_7_32                  80.374%     2.22ms    449.72
fastpforlib_unpack_fullrows_7_32                 55.92%     3.20ms    312.89
arrow_unpack_fullrows_7_32                      63.181%     2.83ms    353.51
duckdb_unpack_fullrows_7_32                     10.625%    16.82ms     59.45
----------------------------------------------------------------------------
velox_unpack_fullrows_8_32                                  1.87ms    535.47
legacy_unpack_naive_fullrows_8_32               20.321%     9.19ms    108.81
legacy_unpack_fast_fullrows_8_32                58.855%     3.17ms    315.15
lemirebmi_unpack_fullrows_8_32                  83.575%     2.23ms    447.52
fastpforlib_unpack_fullrows_8_32                75.699%     2.47ms    405.35
arrow_unpack_fullrows_8_32                       79.56%     2.35ms    426.02
duckdb_unpack_fullrows_8_32                     10.249%    18.22ms     54.88
----------------------------------------------------------------------------
velox_unpack_fullrows_9_32                                  1.83ms    547.37
legacy_unpack_naive_fullrows_9_32               19.696%     9.28ms    107.81
legacy_unpack_fast_fullrows_9_32                49.922%     3.66ms    273.26
lemirebmi_unpack_fullrows_9_32                  77.424%     2.36ms    423.80
fastpforlib_unpack_fullrows_9_32                52.788%     3.46ms    288.95
arrow_unpack_fullrows_9_32                      57.631%     3.17ms    315.46
duckdb_unpack_fullrows_9_32                     6.8026%    26.86ms     37.24
----------------------------------------------------------------------------
velox_unpack_fullrows_10_32                                 1.98ms    505.99
legacy_unpack_naive_fullrows_10_32              21.702%     9.11ms    109.81
legacy_unpack_fast_fullrows_10_32               51.233%     3.86ms    259.24
lemirebmi_unpack_fullrows_10_32                 81.761%     2.42ms    413.70
fastpforlib_unpack_fullrows_10_32               57.099%     3.46ms    288.92
arrow_unpack_fullrows_10_32                     61.998%     3.19ms    313.71
duckdb_unpack_fullrows_10_32                    5.8895%    33.56ms     29.80
----------------------------------------------------------------------------
velox_unpack_fullrows_11_32                                 2.14ms    467.11
legacy_unpack_naive_fullrows_11_32              23.151%     9.25ms    108.14
legacy_unpack_fast_fullrows_11_32               54.483%     3.93ms    254.50
lemirebmi_unpack_fullrows_11_32                 87.838%     2.44ms    410.30
fastpforlib_unpack_fullrows_11_32               57.859%     3.70ms    270.27
arrow_unpack_fullrows_11_32                     64.048%     3.34ms    299.18
duckdb_unpack_fullrows_11_32                    5.4283%    39.44ms     25.36
----------------------------------------------------------------------------
velox_unpack_fullrows_13_32                                 2.19ms    455.93
legacy_unpack_naive_fullrows_13_32              23.623%     9.28ms    107.70
legacy_unpack_fast_fullrows_13_32               56.653%     3.87ms    258.30
lemirebmi_unpack_fullrows_13_32                 82.813%     2.65ms    377.57
fastpforlib_unpack_fullrows_13_32               59.415%     3.69ms    270.89
arrow_unpack_fullrows_13_32                     62.998%     3.48ms    287.23
duckdb_unpack_fullrows_13_32                    4.1405%    52.97ms     18.88
----------------------------------------------------------------------------
velox_unpack_fullrows_15_32                                 2.36ms    424.53
legacy_unpack_naive_fullrows_15_32              25.049%     9.40ms    106.34
legacy_unpack_fast_fullrows_15_32               59.063%     3.99ms    250.74
lemirebmi_unpack_fullrows_15_32                 73.126%     3.22ms    310.44
fastpforlib_unpack_fullrows_15_32               69.958%     3.37ms    296.99
arrow_unpack_fullrows_15_32                      64.61%     3.65ms    274.29
duckdb_unpack_fullrows_15_32                    3.3573%    70.16ms     14.25
----------------------------------------------------------------------------
velox_unpack_fullrows_17_32                                 2.51ms    398.07
legacy_unpack_naive_fullrows_17_32               26.83%     9.36ms    106.80
legacy_unpack_fast_fullrows_17_32               62.174%     4.04ms    247.50
lemirebmi_unpack_fullrows_17_32                 77.442%     3.24ms    308.28
fastpforlib_unpack_fullrows_17_32               65.746%     3.82ms    261.72
arrow_unpack_fullrows_17_32                     65.935%     3.81ms    262.47
duckdb_unpack_fullrows_17_32                    3.3879%    74.15ms     13.49
----------------------------------------------------------------------------
velox_unpack_fullrows_19_32                                 2.66ms    376.33
legacy_unpack_naive_fullrows_19_32              28.036%     9.48ms    105.51
legacy_unpack_fast_fullrows_19_32               65.007%     4.09ms    244.64
lemirebmi_unpack_fullrows_19_32                 74.725%     3.56ms    281.21
fastpforlib_unpack_fullrows_19_32               74.516%     3.57ms    280.43
arrow_unpack_fullrows_19_32                     66.863%     3.97ms    251.63
duckdb_unpack_fullrows_19_32                    3.5497%    74.86ms     13.36
----------------------------------------------------------------------------
velox_unpack_fullrows_21_32                                 2.78ms    359.64
legacy_unpack_naive_fullrows_21_32               29.23%     9.51ms    105.12
legacy_unpack_fast_fullrows_21_32               63.313%     4.39ms    227.70
lemirebmi_unpack_fullrows_21_32                 65.388%     4.25ms    235.16
fastpforlib_unpack_fullrows_21_32               70.753%     3.93ms    254.46
arrow_unpack_fullrows_21_32                     66.389%     4.19ms    238.76
duckdb_unpack_fullrows_21_32                    3.6014%    77.21ms     12.95
----------------------------------------------------------------------------
velox_unpack_fullrows_24_32                                 2.77ms    360.47
legacy_unpack_naive_fullrows_24_32              29.538%     9.39ms    106.48
legacy_unpack_fast_fullrows_24_32               66.924%     4.15ms    241.24
lemirebmi_unpack_fullrows_24_32                 66.484%     4.17ms    239.65
fastpforlib_unpack_fullrows_24_32               79.805%     3.48ms    287.67
arrow_unpack_fullrows_24_32                     67.181%     4.13ms    242.17
duckdb_unpack_fullrows_24_32                    3.4782%    79.76ms     12.54
----------------------------------------------------------------------------
velox_unpack_fullrows_28_32                                 2.91ms    343.95
legacy_unpack_naive_fullrows_28_32              28.431%    10.23ms     97.79
legacy_unpack_fast_fullrows_28_32               29.677%     9.80ms    102.07
lemirebmi_unpack_fullrows_28_32                 61.152%     4.75ms    210.33
fastpforlib_unpack_fullrows_28_32               68.672%     4.23ms    236.20
arrow_unpack_fullrows_28_32                     62.965%     4.62ms    216.57
duckdb_unpack_fullrows_28_32                    3.3935%    85.68ms     11.67
----------------------------------------------------------------------------
velox_unpack_fullrows_30_32                                 3.03ms    329.76
legacy_unpack_naive_fullrows_30_32              31.058%     9.76ms    102.42
legacy_unpack_fast_fullrows_30_32               29.464%    10.29ms     97.16
lemirebmi_unpack_fullrows_30_32                 62.334%     4.86ms    205.56
fastpforlib_unpack_fullrows_30_32                62.66%     4.84ms    206.63
arrow_unpack_fullrows_30_32                     57.507%     5.27ms    189.64
duckdb_unpack_fullrows_30_32                    3.2237%    94.07ms     10.63
----------------------------------------------------------------------------
velox_unpack_fullrows_32_32                                 3.25ms    307.57
legacy_unpack_naive_fullrows_32_32              34.602%     9.40ms    106.42
legacy_unpack_fast_fullrows_32_32               34.352%     9.46ms    105.66
lemirebmi_unpack_fullrows_32_32                 82.374%     3.95ms    253.36
fastpforlib_unpack_fullrows_32_32               125.33%     2.59ms    385.49
arrow_unpack_fullrows_32_32                     94.107%     3.45ms    289.44
duckdb_unpack_fullrows_32_32                    3.3778%    96.26ms     10.39
----------------------------------------------------------------------------
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_1_8                 72.211%     4.50ms    222.10
legacy_unpack_fast_oddrows_1_8                  183.39%     1.77ms    564.06
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_2_8                 71.969%     4.52ms    221.35
legacy_unpack_fast_oddrows_2_8                  197.83%     1.64ms    608.47
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_4_8                 73.124%     4.45ms    224.91
legacy_unpack_fast_oddrows_4_8                  188.96%     1.72ms    581.19
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_8_8                 72.718%     4.47ms    223.66
legacy_unpack_fast_oddrows_8_8                  189.31%     1.72ms    582.25
----------------------------------------------------------------------------
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_1_16                73.111%     4.45ms    224.87
legacy_unpack_fast_oddrows_1_16                 203.42%     1.60ms    625.67
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_2_16                72.713%     4.47ms    223.64
legacy_unpack_fast_oddrows_2_16                 192.62%     1.69ms    592.45
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_4_16                72.852%     4.46ms    224.07
legacy_unpack_fast_oddrows_4_16                 177.47%     1.83ms    545.83
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_8_16                72.527%     4.48ms    223.07
legacy_unpack_fast_oddrows_8_16                 183.07%     1.78ms    563.07
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_10_16               68.813%     4.72ms    211.65
legacy_unpack_fast_oddrows_10_16                166.27%     1.96ms    511.38
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_12_16               70.133%     4.64ms    215.71
legacy_unpack_fast_oddrows_12_16                171.99%     1.89ms    528.99
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_14_16               65.271%     4.98ms    200.75
legacy_unpack_fast_oddrows_14_16                149.19%     2.18ms    458.86
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_16_16                70.17%     4.63ms    215.82
legacy_unpack_fast_oddrows_16_16                143.44%     2.27ms    441.18
----------------------------------------------------------------------------
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_1_32                72.874%     4.46ms    224.14
legacy_unpack_fast_oddrows_1_32                 202.54%     1.61ms    622.93
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_2_32                72.676%     4.47ms    223.53
legacy_unpack_fast_oddrows_2_32                 201.83%     1.61ms    620.75
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_4_32                72.667%     4.47ms    223.50
legacy_unpack_fast_oddrows_4_32                 191.62%     1.70ms    589.37
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_7_32                72.032%     4.51ms    221.55
legacy_unpack_fast_oddrows_7_32                 171.75%     1.89ms    528.24
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_8_32                 72.08%     4.51ms    221.70
legacy_unpack_fast_oddrows_8_32                 174.44%     1.86ms    536.51
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_13_32               69.433%     4.68ms    213.55
legacy_unpack_fast_oddrows_13_32                150.84%     2.16ms    463.94
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_16_32               70.023%     4.64ms    215.37
legacy_unpack_fast_oddrows_16_32                 144.2%     2.25ms    443.51
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_22_32               53.599%     6.07ms    164.85
legacy_unpack_fast_oddrows_22_32                126.68%     2.57ms    389.62
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_24_32                66.98%     4.85ms    206.01
legacy_unpack_fast_oddrows_24_32                134.59%     2.42ms    413.97
----------------------------------------------------------------------------
legacy_unpack_naive_oddrows_31_32               54.604%     5.95ms    167.94
legacy_unpack_fast_oddrows_31_32                50.654%     6.42ms    155.80
----------------------------------------------------------------------------
*/
