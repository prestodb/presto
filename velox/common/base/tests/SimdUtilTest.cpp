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

#include "velox/common/base/SimdUtil.h"
#include <folly/Random.h>

#include <gtest/gtest.h>

using namespace facebook::velox;

namespace {

class SimdUtilTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    VLOG(1) << "Architecture: " << xsimd::default_arch::name();
  }

  void SetUp() override {
    rng_.seed(1);
  }

  void randomBits(std::vector<uint64_t>& bits, int32_t onesPer1000) {
    for (auto i = 0; i < bits.size() * 64; ++i) {
      if (folly::Random::rand32(rng_) % 1000 < onesPer1000) {
        bits::setBit(bits.data(), i);
      }
    }
  }

  int32_t simpleIndicesOfSetBits(
      const uint64_t* bits,
      int32_t begin,
      int32_t end,
      int32_t* indices) {
    auto orgIndices = indices;
    bits::forEachSetBit(
        bits, begin, end, [&](int32_t row) { *indices++ = row; });
    return indices - orgIndices;
  }

  void testIndices(int32_t onesPer1000) {
    constexpr int32_t kWords = 1000;
    std::vector<uint64_t> bits(kWords);
    std::vector<int32_t> reference(kWords * 64);
    std::vector<int32_t> test(kWords * 64);
    randomBits(bits, onesPer1000);
    int32_t begin = folly::Random::rand32(rng_) % 100;
    int32_t end = kWords * 64 - folly::Random::rand32(rng_) % 100;

    auto numReference =
        simpleIndicesOfSetBits(bits.data(), begin, end, reference.data());
    auto numTest = simd::indicesOfSetBits(bits.data(), begin, end, test.data());
    ASSERT_EQ(numReference, numTest);
    ASSERT_EQ(
        memcmp(
            reference.data(), test.data(), numReference * sizeof(reference[0])),
        0);
  }

  void testMemsetAndMemcpy(int32_t size) {
    int32_t words = bits::roundUp(size + 20, 8) / 8;
    std::vector<int64_t> source(words);
    for (auto& item : source) {
      item = folly::Random::rand64(rng_);
    }
    auto target = source;
    auto reference = source;
    int32_t offset = size % 11;
    memset(
        reinterpret_cast<char*>(reference.data()) + offset,
        source.back(),
        size);
    simd::memset(
        reinterpret_cast<char*>(target.data()) + offset, source.back(), size);
    EXPECT_EQ(reference, target);
    memcpy(
        reinterpret_cast<char*>(reference.data()) + offset,
        reinterpret_cast<char*>(source.data()) + offset,
        size);
    simd::memcpy(
        reinterpret_cast<char*>(target.data()) + offset,
        reinterpret_cast<char*>(source.data()) + offset,
        size);
    EXPECT_EQ(reference, target);
  }

  folly::Random::DefaultGenerator rng_;
};

TEST_F(SimdUtilTest, bitIndices) {
  testIndices(1);
  testIndices(10);
  testIndices(100);
  testIndices(250);
  testIndices(500);
  testIndices(999);
}

TEST_F(SimdUtilTest, gather32) {
  int32_t indices8[8] = {7, 6, 5, 4, 3, 2, 1, 0};
  int32_t indices6[8] = {7, 6, 5, 4, 3, 2, 1 << 31, 1 << 31};
  int32_t data[8] = {0, 11, 22, 33, 44, 55, 66, 77};
  constexpr int kBatchSize = xsimd::batch<int32_t>::size;
  const int32_t* indices = indices8 + (8 - kBatchSize);
  const int32_t* indicesMask = indices6 + (8 - kBatchSize);
  auto result = simd::gather(data, indices);
  for (auto i = 0; i < kBatchSize; ++i) {
    EXPECT_EQ(result.get(i), data[indices[i]]);
  }
  auto resultMask = simd::maskGather(
      xsimd::batch<int32_t>::broadcast(-1),
      simd::leadingMask<int32_t>(kBatchSize - 2),
      data,
      indicesMask);
  for (auto i = 0; i < kBatchSize - 2; ++i) {
    EXPECT_EQ(resultMask.get(i), data[indices[i]]);
  }
  EXPECT_EQ(resultMask.get(kBatchSize - 2), -1);
  EXPECT_EQ(resultMask.get(kBatchSize - 1), -1);
  auto bits = simd::toBitMask(result == resultMask);
  // Low (kBatchSize - 2) lanes are the same.
  EXPECT_EQ((1 << (kBatchSize - 2)) - 1, bits);
}

TEST_F(SimdUtilTest, gather64) {
  int32_t indices4[4] = {3, 2, 1, 0};
  int32_t indices3[4] = {3, 2, 1, 1 << 31};
  int64_t data[4] = {44, 55, 66, 77};
  constexpr int kBatchSize = xsimd::batch<int64_t>::size;
  const int32_t* indices = indices4 + (4 - kBatchSize);
  const int32_t* indicesMask = indices4 + (4 - kBatchSize);
  auto result = simd::gather(data, indices);
  for (auto i = 0; i < kBatchSize; ++i) {
    EXPECT_EQ(result.get(i), data[indices[i]]);
  }
  auto resultMask = simd::maskGather(
      xsimd::batch<int64_t>::broadcast(-1),
      simd::leadingMask<int64_t>(kBatchSize - 1),
      data,
      indicesMask);
  for (auto i = 0; i < kBatchSize - 1; ++i) {
    EXPECT_EQ(resultMask.get(i), data[indices[i]]);
  }
  EXPECT_EQ(resultMask.get(kBatchSize - 1), -1);
  auto bits = simd::toBitMask(result == resultMask);
  // Low kBatchSize - 1 lanes are the same.
  EXPECT_EQ((1 << (kBatchSize - 1)) - 1, bits);
}

TEST_F(SimdUtilTest, gather16) {
  int16_t data[32];
  int32_t indices[32];
  for (auto i = 0; i < 32; ++i) {
    indices[i] = 31 - i;
    data[i] = 15 + i;
  }
  xsimd::batch<int16_t> result[2];
  constexpr int kBatchSize = xsimd::batch<int16_t>::size;
  result[0] = simd::gather(data, indices, kBatchSize);
  result[1] = simd::gather(data, &indices[kBatchSize], kBatchSize - 1);
  auto resultPtr = reinterpret_cast<int16_t*>(result);
  for (auto i = 0; i < 2 * kBatchSize - 1; ++i) {
    EXPECT_EQ(data[indices[i]], resultPtr[i]) << i;
  }
  EXPECT_EQ(0, resultPtr[2 * kBatchSize - 1]);
}

TEST_F(SimdUtilTest, gatherBits) {
  // Even bits set, odd not set.
  uint64_t data[2] = {0x5555555555555555, 0x5555555555555555};
  int32_t indices[] = {11, 22, 33, 44, 55, 66, 77, 1 << 30};
  constexpr int N = xsimd::batch<int32_t>::size;
  auto vindex = xsimd::load_unaligned(indices + 8 - N);
  uint64_t bits = simd::gather8Bits(data, vindex, N - 1);
  for (auto i = 0; i < N - 1; ++i) {
    EXPECT_EQ(bits::isBitSet(&bits, i), bits::isBitSet(data, vindex.get(i)));
  }
  EXPECT_FALSE(bits::isBitSet(&bits, N - 1));
}

namespace {

// Find elements that satisfy a condition and pack them to the left.
template <typename T>
void testFilter(std::initializer_list<T> data) {
  auto batch = xsimd::load_unaligned(data.begin());
  auto result = simd::filter(batch, simd::toBitMask(batch > 15000));
  int32_t j = 0;
  for (auto i = 0; i < xsimd::batch<T>::size; ++i) {
    if (batch.get(i) > 15000) {
      EXPECT_EQ(result.get(j++), batch.get(i));
    }
  }
}

} // namespace

TEST_F(SimdUtilTest, filter16) {
  testFilter<int16_t>({
      12345,
      23456,
      111,
      32000,
      15200,
      1000,
      14000,
      17000,
      19000,
      -1000,
      11000,
      16321,
      23000,
      5000,
      22000,
      12,
  });
}

TEST_F(SimdUtilTest, filter32) {
  testFilter<int32_t>({12345, 23456, 111, 32000, 14123, 20000, 25000, 0});
}

TEST_F(SimdUtilTest, filter64) {
  testFilter<int64_t>({12345, 23456, 111, 32000});
}

TEST_F(SimdUtilTest, filterFloat) {
  testFilter<float>(
      {std::nanf("nan"), 23456, 111, 32000, 14123, 20000, 25000, 0});
}

TEST_F(SimdUtilTest, filterDouble) {
  testFilter<double>({std::nan("nan"), 23456, 111, 32000});
}

TEST_F(SimdUtilTest, misc) {
  // Widen to int64 from 4 uints
  uint32_t uints4[4] = {10000, 0, 0, 4000000000};
  int64_t longs4[4];
  xsimd::batch<int64_t>::load_unaligned(uints4).store_unaligned(longs4);
  for (auto i = 0; i < xsimd::batch<int64_t>::size; ++i) {
    EXPECT_EQ(uints4[i], longs4[i]);
  }

  // Widen to int64 from one half of 8 uints.
  uint32_t uints8[8] = {
      1, 2, 3, 4, 1000000000, 2000000000, 3000000000, 4000000000};
  auto vuints8 = xsimd::batch<int32_t>::load_unaligned(uints8);
  auto last4 = simd::getHalf<uint64_t, 1>(vuints8);
  if constexpr (last4.size == 4) {
    EXPECT_EQ(4000000000, last4.get(3));
    EXPECT_EQ(static_cast<int32_t>(4000000000), vuints8.get(7));
  } else {
    ASSERT_EQ(last4.size, 2);
    EXPECT_EQ(4, last4.get(1));
    EXPECT_EQ(4, vuints8.get(3));
  }

  // Masks
  for (auto i = 0; i < xsimd::batch<int32_t>::size; ++i) {
    auto compare = simd::toBitMask(
        simd::leadingMask<int32_t>(i) ==
        simd::fromBitMask<int32_t>((1 << i) - 1));
    EXPECT_EQ(simd::allSetBitMask<int32_t>(), compare);
    if (i < xsimd::batch<int64_t>::size) {
      compare = simd::toBitMask(
          simd::leadingMask<int64_t>(i) ==
          simd::fromBitMask<int64_t>((1 << i) - 1));
      EXPECT_EQ(simd::allSetBitMask<int64_t>(), compare);
    }
  }

  int32_t ints[] = {0, 1, 2, 4};
  EXPECT_TRUE(simd::isDense(&ints[0], 3));
  EXPECT_FALSE(simd::isDense(&ints[0], 4));
}

TEST_F(SimdUtilTest, memory) {
  for (auto size = 0; size < 150; ++size) {
    testMemsetAndMemcpy(size);
  }
}

TEST_F(SimdUtilTest, crc32) {
  uint32_t checksum = 0;
  checksum = simd::crc32U64(0, 123456789);
  EXPECT_EQ(checksum, 3531890030);
  checksum = simd::crc32U64(0, 987654321);
  EXPECT_EQ(checksum, 121285919);
}

TEST_F(SimdUtilTest, Batch64_assign) {
  auto b = simd::Batch64<int32_t>::from({0, 1});
  EXPECT_EQ(b.data[0], 0);
  EXPECT_EQ(b.data[1], 1);
  b.data[0] = -1;
  EXPECT_EQ(b.data[0], -1);
  EXPECT_EQ(b.data[1], 1);
}

TEST_F(SimdUtilTest, Batch64_arithmetics) {
  auto b = simd::Batch64<int32_t>::from({0, 1});
  auto bb = b + 42;
  EXPECT_EQ(bb.data[0], 42);
  EXPECT_EQ(bb.data[1], 43);
  bb = b - 1;
  EXPECT_EQ(bb.data[0], -1);
  EXPECT_EQ(bb.data[1], 0);
}

TEST_F(SimdUtilTest, Batch64_memory) {
  int32_t data[] = {0, 1};
  auto b = simd::Batch64<int32_t>::load_unaligned(data);
  EXPECT_EQ(b.data[0], 0);
  EXPECT_EQ(b.data[1], 1);
  (b + 1).store_unaligned(data);
  EXPECT_EQ(data[0], 1);
  EXPECT_EQ(data[1], 2);
}

} // namespace
