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
#include "velox/common/base/Crc.h"

#include <unordered_set>

#include <boost/crc.hpp>
#include <folly/Random.h>
#include <folly/hash/Checksum.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

DECLARE_bool(bmi2); // NOLINT

namespace facebook {
namespace velox {
namespace bits {

class BitUtilTest : public testing::Test {
 protected:
  template <typename T>
  void testBitAccess() {
    T words[16 / sizeof(T)];
    memset(words, 0, sizeof(words));
    EXPECT_FALSE(isBitSet(words, 111));
    setBit(words, 111);
    EXPECT_TRUE(isBitSet(words, 111));
    clearBit(words, 111);
    EXPECT_FALSE(isBitSet(words, 111));
  }

  void testSetRange(std::vector<uint64_t>& words, int32_t range) {
    for (int32_t i = 0; i < words.size() * 64 - range; ++i) {
      ASSERT_EQ(
          isAllSet(&words[0], i, i + range, true),
          simpleIsAllSet(&words[0], i, i + range, true));
      ASSERT_EQ(
          isAllSet(&words[0], i, i + range, false),
          simpleIsAllSet(&words[0], i, i + range, false));
    }
  }

  bool simpleIsAllSet(uint64_t* bits, int32_t begin, int32_t end, bool value) {
    for (int32_t i = begin; i < end; ++i) {
      if (isBitSet(bits, i) != value) {
        return false;
      }
    }
    return true;
  }

  void testCopyBits(
      const std::vector<uint64_t>& source,
      int32_t bit,
      int32_t numBits) {
    std::vector<uint64_t> target(source.size());
    std::vector<uint64_t> temp(source.size());
    copyBits(source.data(), bit, temp.data(), 64 - bit, numBits);
    for (auto i = 0; i < numBits; ++i) {
      EXPECT_EQ(
          isBitSet(source.data(), i + bit),
          isBitSet(temp.data(), i + (64 - bit)));
    }
    copyBits(source.data(), 0, target.data(), 0, bit);
    copyBits(
        source.data(),
        bit + numBits,
        target.data(),
        bit + numBits,
        target.size() * 64 - bit - numBits);
    copyBits(temp.data(), 64 - bit, target.data(), bit, numBits);
    EXPECT_EQ(source, target);
  }
};

static int32_t
simpleCountBits(const uint64_t* bits, int32_t begin, int32_t end) {
  int32_t count = 0;
  for (int32_t i = begin; i < end; ++i) {
    count += isBitSet(bits, i);
  }
  return count;
}

TEST_F(BitUtilTest, countBits) {
  uint64_t data[] = {
      0xff00ff00ffff00ff,
      0x0f0f0f0f0f0f0f0f,
      0xfedcba9876543210,
      0xf0f0f0f0f0f0f0f0,
      0x0123456789abcdef};
  // We drop one high bit and one low bit on every round.
  int32_t totalBits = sizeof(data) * 8;
  for (int32_t i = 0; i < 10 + (totalBits / 2); ++i) {
    EXPECT_EQ(
        countBits(data, i, totalBits - i),
        simpleCountBits(data, i, totalBits - i));
  }
}

TEST_F(BitUtilTest, reverseBits) {
  const unsigned char BitReverseTable256[] = {
      0x00, 0x80, 0x40, 0xC0, 0x20, 0xA0, 0x60, 0xE0, 0x10, 0x90, 0x50, 0xD0,
      0x30, 0xB0, 0x70, 0xF0, 0x08, 0x88, 0x48, 0xC8, 0x28, 0xA8, 0x68, 0xE8,
      0x18, 0x98, 0x58, 0xD8, 0x38, 0xB8, 0x78, 0xF8, 0x04, 0x84, 0x44, 0xC4,
      0x24, 0xA4, 0x64, 0xE4, 0x14, 0x94, 0x54, 0xD4, 0x34, 0xB4, 0x74, 0xF4,
      0x0C, 0x8C, 0x4C, 0xCC, 0x2C, 0xAC, 0x6C, 0xEC, 0x1C, 0x9C, 0x5C, 0xDC,
      0x3C, 0xBC, 0x7C, 0xFC, 0x02, 0x82, 0x42, 0xC2, 0x22, 0xA2, 0x62, 0xE2,
      0x12, 0x92, 0x52, 0xD2, 0x32, 0xB2, 0x72, 0xF2, 0x0A, 0x8A, 0x4A, 0xCA,
      0x2A, 0xAA, 0x6A, 0xEA, 0x1A, 0x9A, 0x5A, 0xDA, 0x3A, 0xBA, 0x7A, 0xFA,
      0x06, 0x86, 0x46, 0xC6, 0x26, 0xA6, 0x66, 0xE6, 0x16, 0x96, 0x56, 0xD6,
      0x36, 0xB6, 0x76, 0xF6, 0x0E, 0x8E, 0x4E, 0xCE, 0x2E, 0xAE, 0x6E, 0xEE,
      0x1E, 0x9E, 0x5E, 0xDE, 0x3E, 0xBE, 0x7E, 0xFE, 0x01, 0x81, 0x41, 0xC1,
      0x21, 0xA1, 0x61, 0xE1, 0x11, 0x91, 0x51, 0xD1, 0x31, 0xB1, 0x71, 0xF1,
      0x09, 0x89, 0x49, 0xC9, 0x29, 0xA9, 0x69, 0xE9, 0x19, 0x99, 0x59, 0xD9,
      0x39, 0xB9, 0x79, 0xF9, 0x05, 0x85, 0x45, 0xC5, 0x25, 0xA5, 0x65, 0xE5,
      0x15, 0x95, 0x55, 0xD5, 0x35, 0xB5, 0x75, 0xF5, 0x0D, 0x8D, 0x4D, 0xCD,
      0x2D, 0xAD, 0x6D, 0xED, 0x1D, 0x9D, 0x5D, 0xDD, 0x3D, 0xBD, 0x7D, 0xFD,
      0x03, 0x83, 0x43, 0xC3, 0x23, 0xA3, 0x63, 0xE3, 0x13, 0x93, 0x53, 0xD3,
      0x33, 0xB3, 0x73, 0xF3, 0x0B, 0x8B, 0x4B, 0xCB, 0x2B, 0xAB, 0x6B, 0xEB,
      0x1B, 0x9B, 0x5B, 0xDB, 0x3B, 0xBB, 0x7B, 0xFB, 0x07, 0x87, 0x47, 0xC7,
      0x27, 0xA7, 0x67, 0xE7, 0x17, 0x97, 0x57, 0xD7, 0x37, 0xB7, 0x77, 0xF7,
      0x0F, 0x8F, 0x4F, 0xCF, 0x2F, 0xAF, 0x6F, 0xEF, 0x1F, 0x9F, 0x5F, 0xDF,
      0x3F, 0xBF, 0x7F, 0xFF};

  uint8_t bytes[10000];
  for (size_t i = 0; i < 10000; i++) {
    bytes[i] = rand();
  }

  uint8_t bytesCopy[10000];
  memcpy(bytesCopy, bytes, 10000);

  reverseBits(bytes, 10000);
  for (size_t i = 0; i < 10000; i++) {
    EXPECT_EQ(bytes[i], BitReverseTable256[bytesCopy[i]]);
  }

  reverseBits(bytes, 10000);
  for (size_t i = 0; i < 10000; i++) {
    EXPECT_EQ(bytes[i], bytesCopy[i]);
  }
}

TEST_F(BitUtilTest, isAllSet) {
  std::vector<uint64_t> data(100);
  fillBits(&data[0], 11, 222, true);
  fillBits(&data[0], 256, 384, true);
  // Fractional word.
  testSetRange(data, 11);
  // Parts of 2 words.
  testSetRange(data, 111);
  // Whole word and parts of surrounding words.
  testSetRange(data, 133);
  uint64_t* word = reinterpret_cast<uint64_t*>(&data[0]);
  EXPECT_EQ(findFirstBit(word, 0, 11), -1);
  EXPECT_EQ(findFirstBit(word, 0, 12), 11);
  EXPECT_EQ(findFirstBit(word, 0, 123), 11);
  EXPECT_EQ(findLastBit(word, 0, 12), 11);
  EXPECT_EQ(findLastBit(word, 0, 88), 87);
  EXPECT_EQ(findLastBit(word, 233, 255), -1);
  EXPECT_EQ(findLastBit(word, 221, 255), 221);
  EXPECT_EQ(findLastBit(word, 0, 255), 221);
  EXPECT_EQ(findLastBit(word, 381, 600), 383);
}

TEST_F(BitUtilTest, findLastUnsetBit) {
  uint64_t allOnes = static_cast<uint64_t>(-1);

  std::vector<uint64_t> vector(100, allOnes);
  auto data = vector.data();
  int32_t dataSizeInBits = 64 * vector.size();

  ASSERT_EQ(-1, findLastUnsetBit(data, 0 /*begin*/, dataSizeInBits /*end*/));

  clearBit(data, 500);
  clearBit(data, 300);

  ASSERT_EQ(500, findLastUnsetBit(data, 0 /*begin*/, dataSizeInBits /*end*/));

  ASSERT_EQ(300, findLastUnsetBit(data, 0 /*begin*/, 400));
  ASSERT_EQ(300, findLastUnsetBit(data, 100 /*begin*/, 400));

  ASSERT_EQ(-1, findLastUnsetBit(data, 350 /*begin*/, 450));
}

TEST_F(BitUtilTest, nbytes) {
  EXPECT_EQ(nbytes(0), 0);
  EXPECT_EQ(nbytes(1), 1);
  EXPECT_EQ(nbytes(23), 3);
  EXPECT_EQ(nbytes(24), 3);
  EXPECT_EQ(nbytes(25), 4);
}

TEST_F(BitUtilTest, nwords) {
  EXPECT_EQ(nwords(0), 0);
  EXPECT_EQ(nwords(24), 1);
  EXPECT_EQ(nwords(63), 1);
  EXPECT_EQ(nwords(64), 1);
  EXPECT_EQ(nwords(65), 2);
}

TEST_F(BitUtilTest, setBits) {
  testBitAccess<uint64_t>();
  testBitAccess<uint32_t>();
  testBitAccess<uint16_t>();
  testBitAccess<uint8_t>();
  uint64_t words[2] = {0, 0};
  uint8_t* bytes = reinterpret_cast<uint8_t*>(words);
  // Set with words, test with bytes.
  EXPECT_FALSE(isBitSet(words, 111));
  setBit(words, 111);
  EXPECT_TRUE(isBitSet(bytes, 111));
  clearBit(words, 111);
  EXPECT_FALSE(isBitSet(bytes, 111));
}

TEST_F(BitUtilTest, booleans) {
  std::vector<uint64_t> left(8);
  std::vector<uint64_t> right(8);
  // Test filling
  fillBits(&left[0], 11, 89, true);
  fillBits(&right[0], 11, 89, true);
  EXPECT_EQ(left, right);
  EXPECT_TRUE(isSubset(&left[0], &right[0], 10, 90));
  EXPECT_TRUE(isSubset(&right[0], &left[0], 10, 90));
  EXPECT_TRUE(hasIntersection(&left[0], &right[0], 10, 90));

  fillBits(&left[0], 91, 132, true);
  EXPECT_TRUE(isSubset(&left[0], &right[0], 10, 90));
  // False, we just added bits 91... to 'left'.
  EXPECT_FALSE(isSubset(&left[0], &right[0], 10, 122));
  EXPECT_FALSE(isAllSet(&left[0], 0, 14, false));
  EXPECT_TRUE(isAllSet(&left[0], 22, 83, true));
  EXPECT_FALSE(isAllSet(&left[0], 22, 92, true));

  fillBits(&left[0], 0, left.size() * 64, false);
  fillBits(&right[0], 0, left.size() * 64, false);
  fillBits(&left[0], 10, 222, true);
  fillBits(&right[0], 210, 300, true);
  EXPECT_TRUE(hasIntersection(&left[0], &right[0], 200, 240));
  EXPECT_FALSE(hasIntersection(&left[0], &right[0], 235, 240));
  auto result = left;
  orRange<false>(&result[0], &left[0], &right[0], 5, 400);
  EXPECT_TRUE(isAllSet(&result[0], 10, 300));
  EXPECT_FALSE(isBitSet(&result[0], 9) || isBitSet(&result[0], 300));
  result = left;
  andRange<false>(&result[0], &left[0], &right[0], 5, 400);
  EXPECT_TRUE(isAllSet(&result[0], 210, 222));
  EXPECT_FALSE(isBitSet(&result[0], 209) || isBitSet(&result[0], 222));
}

TEST_F(BitUtilTest, testBits) {
  uint64_t data[] = {0x0, 0x0, 0x0, 0x0, 0x0};
  auto totalBits = sizeof(data) * 8;

  // no bits are set
  auto neverCalled = [](int32_t idx) {
    EXPECT_TRUE(false) << "Didn't expect this call";
    return true;
  };
  EXPECT_TRUE(testSetBits(data, 0, totalBits, neverCalled));
  EXPECT_TRUE(testSetBits(data, 2, totalBits, neverCalled));
  EXPECT_TRUE(testSetBits(data, 2, totalBits - 3, neverCalled));

  int32_t expectedIndex = 0;
  auto calledOnEachBit = [&expectedIndex](int32_t idx) {
    EXPECT_EQ(expectedIndex, idx);
    expectedIndex++;
    return true;
  };

  EXPECT_TRUE(testUnsetBits(data, 0, totalBits, calledOnEachBit));
  EXPECT_EQ(totalBits, expectedIndex);

  expectedIndex = 2;
  EXPECT_TRUE(testUnsetBits(data, 2, totalBits, calledOnEachBit));
  EXPECT_EQ(totalBits, expectedIndex);

  expectedIndex = 2;
  EXPECT_TRUE(testUnsetBits(data, 2, totalBits - 3, calledOnEachBit));
  EXPECT_EQ(totalBits - 3, expectedIndex);

  // set even bits
  for (size_t i = 0; i < totalBits; i += 2) {
    setBit(data, i);
  }

  auto calledOnEveryOther = [&expectedIndex](int32_t idx) {
    EXPECT_EQ(expectedIndex, idx);
    expectedIndex += 2;
    return true;
  };

  expectedIndex = 0;
  EXPECT_TRUE(testSetBits(data, 0, totalBits, calledOnEveryOther));
  EXPECT_EQ(totalBits, expectedIndex);

  expectedIndex = 2;
  EXPECT_TRUE(testSetBits(data, 2, totalBits, calledOnEveryOther));
  EXPECT_EQ(totalBits, expectedIndex);

  expectedIndex = 2;
  EXPECT_TRUE(testSetBits(data, 2, totalBits - 3, calledOnEveryOther));
  EXPECT_EQ(totalBits - 2, expectedIndex);

  expectedIndex = 1;
  EXPECT_TRUE(testUnsetBits(data, 0, totalBits, calledOnEveryOther));
  EXPECT_EQ(totalBits + 1, expectedIndex);

  expectedIndex = 3;
  EXPECT_TRUE(testUnsetBits(data, 2, totalBits, calledOnEveryOther));
  EXPECT_EQ(totalBits + 1, expectedIndex);

  expectedIndex = 3;
  EXPECT_TRUE(testUnsetBits(data, 2, totalBits - 3, calledOnEveryOther));
  EXPECT_EQ(totalBits - 3, expectedIndex);

  // test early termination
  int32_t maxIndex = 0;
  auto terminatedEarly = [&expectedIndex, &maxIndex](int32_t idx) {
    EXPECT_EQ(expectedIndex, idx);
    expectedIndex += 2;
    return expectedIndex < maxIndex;
  };

  expectedIndex = 0;
  maxIndex = 10;
  EXPECT_FALSE(testSetBits(data, 0, totalBits, terminatedEarly));
  EXPECT_EQ(maxIndex, expectedIndex);

  // test early termination within partial first word
  expectedIndex = 2;
  maxIndex = 6;
  EXPECT_FALSE(testSetBits(data, 2, totalBits, terminatedEarly));
  EXPECT_EQ(maxIndex, expectedIndex);

  // test early termination within partial last word
  expectedIndex = 2;
  maxIndex = totalBits - 4;
  EXPECT_FALSE(testSetBits(data, 2, totalBits - 3, terminatedEarly));
  EXPECT_EQ(maxIndex, expectedIndex);
}

TEST_F(BitUtilTest, forEachBit) {
  uint64_t data[] = {0x0, 0x0, 0x0, 0x0, 0x0};
  auto totalBits = sizeof(data) * 8;

  // no bits are set
  auto neverCalled = [](int32_t idx) {
    EXPECT_TRUE(false) << "Didn't expect this call";
  };
  forEachSetBit(data, 0, totalBits, neverCalled);
  forEachSetBit(data, 2, totalBits, neverCalled);
  forEachSetBit(data, 2, totalBits - 3, neverCalled);

  int32_t expectedIndex = 0;
  auto calledOnEachBit = [&expectedIndex](int32_t idx) {
    EXPECT_EQ(expectedIndex, idx);
    expectedIndex++;
  };

  forEachUnsetBit(data, 0, totalBits, calledOnEachBit);
  EXPECT_EQ(totalBits, expectedIndex);

  expectedIndex = 2;
  forEachUnsetBit(data, 2, totalBits, calledOnEachBit);
  EXPECT_EQ(totalBits, expectedIndex);

  expectedIndex = 2;
  forEachUnsetBit(data, 2, totalBits - 3, calledOnEachBit);
  EXPECT_EQ(totalBits - 3, expectedIndex);

  // set even bits
  for (size_t i = 0; i < totalBits; i += 2) {
    setBit(data, i);
  }

  auto calledOnEveryOther = [&expectedIndex](int32_t idx) {
    EXPECT_EQ(expectedIndex, idx);
    expectedIndex += 2;
  };

  expectedIndex = 0;
  forEachSetBit(data, 0, totalBits, calledOnEveryOther);
  EXPECT_EQ(totalBits, expectedIndex);

  expectedIndex = 2;
  forEachSetBit(data, 2, totalBits, calledOnEveryOther);
  EXPECT_EQ(totalBits, expectedIndex);

  expectedIndex = 2;
  forEachSetBit(data, 2, totalBits - 3, calledOnEveryOther);
  EXPECT_EQ(totalBits - 2, expectedIndex);

  expectedIndex = 1;
  forEachUnsetBit(data, 0, totalBits, calledOnEveryOther);
  EXPECT_EQ(totalBits + 1, expectedIndex);

  expectedIndex = 3;
  forEachUnsetBit(data, 2, totalBits, calledOnEveryOther);
  EXPECT_EQ(totalBits + 1, expectedIndex);

  expectedIndex = 3;
  forEachUnsetBit(data, 2, totalBits - 3, calledOnEveryOther);
  EXPECT_EQ(totalBits - 3, expectedIndex);

  totalBits = 100;
  std::vector<uint64_t> bits(bits::nwords(totalBits));
  uint64_t* rawBits = bits.data();

  // Test all bits set.
  bits::fillBits(rawBits, 0, totalBits, true);
  int count = 0;
  auto countEach = [&](auto row) {
    ASSERT_EQ(row, count);
    count++;
  };

  bits::forEachBit(rawBits, 0, totalBits, true, countEach);
  ASSERT_EQ(totalBits, count);

  // Test all bits unset.
  bits::fillBits(rawBits, 0, totalBits, false);

  count = 0;
  bits::forEachBit(rawBits, 0, totalBits, false, countEach);
  ASSERT_EQ(totalBits, count);

  // Test all but one bit set.
  bits::fillBits(rawBits, 0, totalBits, true);
  bits::setBit(rawBits, 50, false);
  count = 0;
  auto incrementCount = [&](auto _) { count++; };
  bits::forEachBit(rawBits, 0, totalBits, true, incrementCount);
  ASSERT_EQ(totalBits - 1, count);

  // Test all but one bit unset.
  bits::fillBits(rawBits, 0, totalBits, false);
  bits::setBit(rawBits, 50, true);
  count = 0;
  bits::forEachBit(rawBits, 0, totalBits, false, incrementCount);
  ASSERT_EQ(totalBits - 1, count);
}

TEST_F(BitUtilTest, hash) {
  std::unordered_set<size_t> hashes;
  const char* text = "Forget the night, come live with us in forests of azure";
  for (int32_t i = 0; i < strlen(text); ++i) {
    hashes.insert(hashBytes(1, text, i));
  }
  EXPECT_EQ(hashes.size(), strlen(text));
}

TEST_F(BitUtilTest, nextPowerOfTwo) {
  EXPECT_EQ(nextPowerOfTwo(0), 0);
  EXPECT_EQ(nextPowerOfTwo(1), 1);
  EXPECT_EQ(nextPowerOfTwo(2), 2);
  EXPECT_EQ(nextPowerOfTwo(3), 4);
  EXPECT_EQ(nextPowerOfTwo(4), 4);
  EXPECT_EQ(nextPowerOfTwo(5), 8);
  EXPECT_EQ(nextPowerOfTwo(6), 8);
  EXPECT_EQ(nextPowerOfTwo(7), 8);
  EXPECT_EQ(nextPowerOfTwo(8), 8);
  EXPECT_EQ(nextPowerOfTwo(31), 32);
  EXPECT_EQ(nextPowerOfTwo(32), 32);
  EXPECT_EQ(nextPowerOfTwo(33), 64);
}

TEST_F(BitUtilTest, isPowerOfTwo) {
  EXPECT_TRUE(isPowerOfTwo(0));
  EXPECT_TRUE(isPowerOfTwo(1));
  EXPECT_TRUE(isPowerOfTwo(2));
  EXPECT_FALSE(isPowerOfTwo(3));
  EXPECT_TRUE(isPowerOfTwo(4));
  EXPECT_FALSE(isPowerOfTwo(7));
  EXPECT_TRUE(isPowerOfTwo(64));
  EXPECT_FALSE(isPowerOfTwo(65));
  EXPECT_FALSE(isPowerOfTwo(1000));
  EXPECT_TRUE(isPowerOfTwo(1024));
}

TEST_F(BitUtilTest, getAndClearLastSetBit) {
  uint16_t bits = 0;
  for (int32_t i = 0; i < 16; i++) {
    bits::setBit(&bits, i, i % 3 == 0);
  }

  EXPECT_EQ(getAndClearLastSetBit(bits), 0);
  EXPECT_EQ(getAndClearLastSetBit(bits), 3);
  EXPECT_EQ(getAndClearLastSetBit(bits), 6);
  EXPECT_EQ(getAndClearLastSetBit(bits), 9);
  EXPECT_EQ(getAndClearLastSetBit(bits), 12);
  EXPECT_EQ(getAndClearLastSetBit(bits), 15);
  EXPECT_EQ(bits, 0);

  for (int32_t i = 0; i < 16; i++) {
    bits::setBit(&bits, i, i % 5 == 2);
  }

  EXPECT_EQ(getAndClearLastSetBit(bits), 2);
  EXPECT_EQ(getAndClearLastSetBit(bits), 7);
  EXPECT_EQ(getAndClearLastSetBit(bits), 12);
  EXPECT_EQ(bits, 0);
}

TEST_F(BitUtilTest, negate) {
  char data[35];
  for (int32_t i = 0; i < 100; i++) {
    setBit(data, i, i % 2 == 0);
  }

  negate(data, 64);
  for (int32_t i = 0; i < 64; i++) {
    EXPECT_EQ(isBitSet(data, i), i % 2 != 0) << "at " << i;
  }
  for (int32_t i = 64; i < 100; i++) {
    EXPECT_EQ(isBitSet(data, i), i % 2 == 0) << "at " << i;
  }

  negate(data, 64);
  for (int32_t i = 0; i < 64; i++) {
    EXPECT_EQ(isBitSet(data, i), i % 2 == 0) << "at " << i;
  }

  negate(data, 72);
  for (int32_t i = 0; i < 72; i++) {
    EXPECT_EQ(isBitSet(data, i), i % 2 != 0) << "at " << i;
  }
  for (int32_t i = 72; i < 100; i++) {
    EXPECT_EQ(isBitSet(data, i), i % 2 == 0) << "at " << i;
  }

  negate(data, 72);
  for (int32_t i = 0; i < 72; i++) {
    EXPECT_EQ(isBitSet(data, i), i % 2 == 0) << "at " << i;
  }

  negate(data, 100);
  for (int32_t i = 0; i < 100; i++) {
    EXPECT_EQ(isBitSet(data, i), i % 2 != 0) << "at " << i;
  }

  negate(data, 100);
  for (int32_t i = 0; i < 100; i++) {
    EXPECT_EQ(isBitSet(data, i), i % 2 == 0) << "at " << i;
  }
}

TEST_F(BitUtilTest, bitfields) {
  uint64_t source = 0x123456789abcdef0;
  uint64_t mask20 = (1 << 20) - 1;
  uint64_t mask31 = (1UL << 31) - 1;
  // Do a load that fits in uint32_t
  EXPECT_EQ(
      detail::loadBits<uint32_t>(&source, 3, 20) & mask20,
      source >> 3 & mask20);

  // Do a load that accesses the next byte after first uint32_t
  EXPECT_EQ(
      detail::loadBits<uint32_t>(&source, 3, 31) & mask31,
      source >> 3 & mask31);

  uint64_t target = 0x123456789abcdef0;
  uint64_t original = target;
  uint32_t value = 0x76543210;
  detail::storeBits<uint32_t>(&target, 3, value, 20);
  // Check the bit field was written and the bits outside were left as
  // is.  Force load from the address of target, the type punned store
  // above may have been missed by alias tracking in optimized mode.
  auto targetValue = *reinterpret_cast<volatile uint64_t*>(&target);
  EXPECT_EQ((targetValue >> 3) & mask20, value & mask20);
  EXPECT_EQ(targetValue & ~(mask20 << 3), original & ~(mask20 << 3));

  // Repeat the same writing a bit field that overflows into the next byte.
  target = original;

  detail::storeBits<uint32_t>(&target, 3, value, 31);
  // Check the bit field was written and the bits outside were left as is.
  targetValue = *reinterpret_cast<volatile uint64_t*>(&target);
  EXPECT_EQ((targetValue >> 3) & mask31, value & mask31);
  EXPECT_EQ(targetValue & ~(mask31 << 3), original & ~(mask31 << 3));
}

TEST_F(BitUtilTest, copyBits) {
  std::vector<uint64_t> source(10);

  for (auto i = 0; i < source.size(); ++i) {
    source[i] = 0x1234567890abcdef >> i;
  }
  testCopyBits(source, 10, 21);
  testCopyBits(source, 55, 21);
  testCopyBits(source, 0, 128);
  testCopyBits(source, 1, 128);
  testCopyBits(source, 63, 128);
  testCopyBits(source, 45, 301);
  testCopyBits(source, 63, 407);
  // Write bits 61..638 in temp, check that there is no write past
  // 10th word in temp.
  testCopyBits(source, 3, 640 - 62);
}

TEST_F(BitUtilTest, scatterBits) {
  constexpr int32_t kSize = 100;
  constexpr int32_t kNumBits = kSize * 64 - 2;
  std::vector<uint64_t> mask(kSize);
  auto maskData = mask.data();

  // Ranges of tens of set and unset bits.
  fillBits(maskData, kNumBits - 130, kNumBits - 2, true);
  fillBits(maskData, kNumBits - 601, kNumBits - 403, true);

  folly::Random::DefaultGenerator rng;
  rng.seed(1);
  // Range of mostly set bits, 1/50 is not set.
  for (auto bit = kNumBits - 1000; bit > 400; --bit) {
    if (folly::Random::rand32(rng) % 50) {
      setBit(maskData, bit);
    }
  }
  // Alternating groups of 5 bits with 0-3 bis set.
  for (auto i = 0; i < 305; i += 5) {
    auto numSet = (i / 5) % 4;
    for (auto j = 0; j < numSet; ++j) {
      setBit(maskData, i + j, true);
    }
  }

  auto numInMask = bits::countBits(maskData, 0, kNumBits);
  std::vector<uint64_t> source(kSize);
  uint64_t seed = 0x123456789abcdef0LL;
  for (auto& item : source) {
    item = seed;
    seed *= 0x5cdf;
  }
  std::vector<char> test(kSize * 8);
  std::vector<char> reference(kSize * 8);
  auto sourceAsChar = reinterpret_cast<char*>(source.data());
  scatterBits(numInMask, kNumBits, sourceAsChar, maskData, test.data());
  // Generate the reference output with the non-BMI implementation.
  FLAGS_bmi2 = false; // NOLINT
  scatterBits(numInMask, kNumBits, sourceAsChar, maskData, reference.data());
  FLAGS_bmi2 = true; // NOLINT
  EXPECT_EQ(reference, test);
  // Repeat the same in place.
  scatterBits(numInMask, kNumBits, sourceAsChar, maskData, sourceAsChar);
  for (int32_t i = kNumBits - 1; i >= 0; --i) {
    EXPECT_EQ(
        bits::isBitSet(reference.data(), i), bits::isBitSet(sourceAsChar, i));
  }
}

TEST_F(BitUtilTest, hashMix) {
  EXPECT_NE(bits::hashMix(123, 321), bits::hashMix(321, 123));
  EXPECT_EQ(
      bits::commutativeHashMix(123, 321), bits::commutativeHashMix(321, 123));
}

TEST_F(BitUtilTest, crc) {
  const char* text =
      "We were sailing on the sloop John B., ny grandfather and me...";
  const char* text2 = "around old Nassau we would rowm";
  boost::crc_32_type crc32;
  crc32.process_bytes(text, sizeof(text));
  crc32.process_bytes(text2, sizeof(text2));
  auto boostCrc = crc32.checksum();
  bits::Crc32 crc;
  crc.process_bytes(text, sizeof(text));
  crc.process_bytes(text2, sizeof(text2));
  auto follyCrc = crc.checksum();

  EXPECT_EQ(boostCrc, follyCrc);
}

TEST_F(BitUtilTest, pad) {
  char bytes[100];
  memset(bytes, 1, sizeof(bytes));
  bits::padToAlignment(&bytes[11], 30, 7, 16);
  // We expect a 0 in bytes[11 +7] ... bytes[11 + 15].
  EXPECT_EQ(1, bytes[11 + 6]);
  for (auto i = 11 + 7; i < 11 + 16; ++i) {
    EXPECT_EQ(0, bytes[i]);
  }
  EXPECT_EQ(1, bytes[11 + 16]);

  // Test with end of data before next aligned address.
  memset(bytes, 1, sizeof(bytes));
  bits::padToAlignment(&bytes[11], 12, 7, 16);
  // We expect a 0 in bytes[11 +7] ... bytes[11 + 12].
  EXPECT_EQ(1, bytes[11 + 6]);
  for (auto i = 11 + 7; i < 11 + 12; ++i) {
    EXPECT_EQ(0, bytes[i]);
  }
  EXPECT_EQ(1, bytes[11 + 13]);
}

} // namespace bits
} // namespace velox
} // namespace facebook
