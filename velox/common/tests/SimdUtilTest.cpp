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

using V64 = simd::Vectors<int64_t>;
using V32 = simd::Vectors<int32_t>;
using V16 = simd::Vectors<int16_t>;
using VD = simd::Vectors<double>;
using VF = simd::Vectors<float>;

class SimdUtilTest : public testing::Test {
 protected:
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
  int32_t data8[8] = {0, 11, 22, 33, 44, 55, 66, 77};
  auto result8 = V32::gather32(&data8, V32::loadGather32Indices(&indices8[0]));
  int32_t result8Mem[8];
  V32::store(&result8Mem, result8);
  auto resultPtr = &result8Mem[0];
  for (auto i = 0; i < 8; ++i) {
    EXPECT_EQ(resultPtr[i], data8[indices8[i]]);
  }
  auto result6 = V32::maskGather32(
      V32::setAll(-1), V32::leadingMask(6), &data8, V32::load(&indices6[0]));
  for (auto i = 0; i < 6; ++i) {
    EXPECT_EQ(resultPtr[i], data8[indices8[i]]);
  }
  EXPECT_EQ(result6[6], -1);
  EXPECT_EQ(result6[7], -1);

  auto bits = V32::compareBitMask(V32::compareEq(result8, result6));
  // Low 6 lanes are the same.
  EXPECT_EQ(63, bits);
}

TEST_F(SimdUtilTest, gather64) {
  int32_t indices4[4] = {3, 2, 1, 0};
  int32_t indices3[4] = {3, 2, 1, 1 << 31};
  int64_t data4[4] = {44, 55, 66, 77};
  auto result4 = V64::gather32(&data4, V64::loadGather32Indices(&indices4[0]));
  int64_t result4Mem[4];
  V64::store(&result4Mem, result4);
  auto resultPtr = &result4Mem[0];
  for (auto i = 0; i < 4; ++i) {
    EXPECT_EQ(resultPtr[i], data4[indices4[i]]);
  }
  auto result3 = V64::maskGather32(
      V64::setAll(-1),
      V64::leadingMask(3),
      &data4,
      V64::loadGather32Indices(&indices3[0]));
  for (auto i = 0; i < 3; ++i) {
    EXPECT_EQ(resultPtr[i], data4[indices4[i]]);
  }
  EXPECT_EQ(result3[3], -1);
  auto bits = V64::compareBitMask(V64::compareEq(result4, result3));
  // Low 3 lanes are the same.
  EXPECT_EQ(7, bits);
}

TEST_F(SimdUtilTest, gather16) {
  int16_t data[32];
  int32_t indices[32];
  for (auto i = 0; i < 32; ++i) {
    indices[i] = 31 - i;
    data[i] = 15 + i;
  }
  __m256hi result[2];
  result[0] = simd::gather16x32(&data[0], &indices[0], 16);
  result[1] = simd::gather16x32(&data[0], &indices[16], 15);
  auto resultPtr = reinterpret_cast<int16_t*>(&result);
  for (auto i = 0; i < 31; ++i) {
    EXPECT_EQ(data[indices[i]], resultPtr[i]);
  }
  EXPECT_EQ(0, resultPtr[31]);
}

TEST_F(SimdUtilTest, gatherBits) {
  // Even bits set, odd not set.
  uint64_t data[2] = {0x5555555555555555, 0x5555555555555555};
  __m256si indices = {11, 22, 33, 44, 55, 66, 77, 1 << 30};
  uint64_t bits = simd::gather8Bits(data, indices, 7);
  auto intIndices = reinterpret_cast<int32_t*>(&indices);
  for (auto i = 0; i < 7; ++i) {
    EXPECT_EQ(bits::isBitSet(&bits, i), bits::isBitSet(data, intIndices[i]));
  }
  EXPECT_FALSE(bits::isBitSet(&bits, 7));
}

TEST_F(SimdUtilTest, permute32) {
  // Find elements that satisfy a condition and pack them to the left.
  __m256si data = {12345, 23456, 111, 32000, 14123, 20000, 25000};
  __m256si result;
  auto bits = V32::compareBitMask(V32::compareGt(data, V32::setAll(15000)));
  simd::storePermute(&result, data, V32::load(&simd::byteSetBits()[bits]));
  int32_t j = 0;
  for (auto i = 0; i < 8; ++i) {
    auto dataPtr = reinterpret_cast<int32_t*>(&data);
    auto resultPtr = reinterpret_cast<int32_t*>(&result);
    if (dataPtr[i] > 15000) {
      EXPECT_EQ(resultPtr[j], dataPtr[i]);
      ++j;
    }
  }
}

TEST_F(SimdUtilTest, permute64) {
  // Find elements that satisfy a condition and pack them to the left.
  __m256i data = {12345, 23456, 111, 32000};
  __m256i result;
  auto bits = V64::compareBitMask(V64::compareGt(data, V64::setAll(15000)));
  simd::storePermute(
      &result,
      reinterpret_cast<__m256si>(data),
      V32::load(&V64::permuteIndices()[bits]));
  int32_t j = 0;
  for (auto i = 0; i < 4; ++i) {
    auto dataPtr = reinterpret_cast<int64_t*>(&data);
    auto resultPtr = reinterpret_cast<int64_t*>(&result);
    if (dataPtr[i] > 15000) {
      EXPECT_EQ(resultPtr[j], dataPtr[i]);
      ++j;
    }
  }
}

TEST_F(SimdUtilTest, permute16) {
  // Find elements that satisfy a condition and pack them to the left.
  __m256hi data = {
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
  };
  auto bits = V16::compareBitMask(V16::compareGt(data, V16::setAll(15000)));
  uint8_t first8 = bits & 0xff;
  uint8_t second8 = bits >> 8;
  int16_t result[16];
  simd::storePermute16<0>(
      &result,
      reinterpret_cast<__m256i>(data),
      V32::load(&simd::byteSetBits()[first8]));
  simd::storePermute16<1>(
      &result[__builtin_popcount(first8)],
      reinterpret_cast<__m256i>(data),
      V32::load(&simd::byteSetBits()[second8]));
  int32_t j = 0;
  for (auto i = 0; i < 16; ++i) {
    auto dataPtr = reinterpret_cast<int16_t*>(&data);
    auto resultPtr = reinterpret_cast<int16_t*>(&result);
    if (dataPtr[i] > 15000) {
      EXPECT_EQ(resultPtr[j], dataPtr[i]);
      ++j;
    }
  }
}

TEST_F(SimdUtilTest, permuteFloat) {
  // Find elements that satisfy a condition and pack them to the left.
  __m256 data = {std::nanf("nan"), 23456, 111, 32000, 14123, 20000, 25000};
  __m256si result;
  auto bits = VF::compareBitMask(VF::compareGt(data, VF::setAll(15000)));
  simd::storePermute(
      &result,
      reinterpret_cast<__m256si>(data),
      V32::load(&simd::byteSetBits()[bits]));
  int32_t j = 0;
  for (auto i = 0; i < 8; ++i) {
    auto dataPtr = reinterpret_cast<float*>(&data);
    auto resultPtr = reinterpret_cast<float*>(&result);
    if (dataPtr[i] > 15000) {
      EXPECT_EQ(resultPtr[j], dataPtr[i]);
      ++j;
    }
  }
}

TEST_F(SimdUtilTest, permuteDouble) {
  // Find elements that satisfy a condition and pack them to the left.
  __m256d data = {std::nan("nan"), 23456, 111, 32000};
  __m256i result;
  auto bits = VD::compareBitMask(VD::compareGt(data, VD::setAll(15000)));
  simd::storePermute(
      &result,
      reinterpret_cast<__m256si>(data),
      V32::load(V64::permuteIndices()[bits]));
  int32_t j = 0;
  for (auto i = 0; i < 4; ++i) {
    auto dataPtr = reinterpret_cast<double*>(&data);
    auto resultPtr = reinterpret_cast<double*>(&result);
    if (dataPtr[i] > 15000) {
      EXPECT_EQ(resultPtr[j], dataPtr[i]);
      ++j;
    }
  }
}

TEST_F(SimdUtilTest, misc) {
  // Widen to int64 from 4 uints
  uint32_t uints4[4] = {10000, 0, 0, 4000000000};
  int64_t longs4[4];
  V64::store(&longs4, V64::from32u(*reinterpret_cast<__m128si_u*>(&uints4)));
  for (auto i = 0; i < 4; ++i) {
    EXPECT_EQ(uints4[i], longs4[i]);
  }

  // Widen to int64 from one half of 8 uints.
  uint32_t uints8[8] = {
      1, 2, 3, 4, 1000000000, 2000000000, 3000000000, 4000000000};
  auto last4 = V32::as4x64u<1>(V32::load(&uints8));
  EXPECT_EQ(4000000000, V64::extract<3>(last4));

  EXPECT_EQ(
      static_cast<int32_t>(4000000000), V32::extract<7>(V32::load(&uints8)));

  // Masks
  for (auto i = 0; i < V32::VSize; ++i) {
    auto compare = V32::compareBitMask(
        V32::compareEq(V32::leadingMask(i), V32::mask((1 << i) - 1)));
    EXPECT_EQ(V32::kAllTrue, compare);
    if (i < V64::VSize) {
      compare = V64::compareBitMask(
          V64::compareEq(V64::leadingMask(i), V64::mask((1 << i) - 1)));
      EXPECT_EQ(V64::kAllTrue, compare);
    }
  }

  EXPECT_EQ(32, simd::kPadding);

  int32_t ints[] = {0, 1, 2, 4};
  EXPECT_TRUE(simd::isDense(&ints[0], 3));
  EXPECT_FALSE(simd::isDense(&ints[0], 4));
}

TEST_F(SimdUtilTest, memory) {
  for (auto size = 0; size < 150; ++size) {
    testMemsetAndMemcpy(size);
  }
}
