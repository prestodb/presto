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
#include "velox/common/memory/RawVector.h"
#include "velox/common/time/Timer.h"

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
    auto run = [&](int begin, int end) {
      auto numReference =
          simpleIndicesOfSetBits(bits.data(), begin, end, reference.data());
      auto numTest =
          simd::indicesOfSetBits(bits.data(), begin, end, test.data());
      ASSERT_EQ(numReference, numTest);
      ASSERT_EQ(
          memcmp(
              reference.data(),
              test.data(),
              numReference * sizeof(reference[0])),
          0);
    };
    int32_t begin = folly::Random::rand32(rng_) % 100;
    int32_t end = kWords * 64 - folly::Random::rand32(rng_) % 100;
    for (int offset = 0; offset < 64; ++offset) {
      run(begin + offset, end);
      run(begin, end - offset);
      run(begin + offset, end - offset);
    }
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

  template <class Integral1, class Integral2>
  Integral2 random(Integral1 low, Integral2 up) {
    std::uniform_int_distribution<> range(low, up);
    return range(rng_);
  }

  void randomString(std::string* toFill, unsigned int maxSize = 1000) {
    assert(toFill);
    toFill->resize(random(0, maxSize));
    for (int i = 0; i < toFill->size(); i++) {
      (*toFill)[i] = random('a', 'z');
    }
  }

  folly::Random::DefaultGenerator rng_;
};

TEST_F(SimdUtilTest, setAll) {
  auto bits = simd::setAll(true);
  auto words = reinterpret_cast<int64_t*>(&bits);
  for (int i = 0; i < xsimd::batch<int64_t>::size; ++i) {
    EXPECT_EQ(words[i], -1ll);
  }
}

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

  uint64_t source = 0x123456789abcdefLU;
  raw_vector<int32_t> bitIndices;
  uint64_t result = 0;
  for (auto i = 61; i >= 0; i -= 2) {
    bitIndices.push_back(i);
  }
  simd::gatherBits(
      &source,
      folly::Range<int32_t*>(bitIndices.data(), bitIndices.size()),
      &result);
  for (auto i = 0; i < bitIndices.size(); ++i) {
    EXPECT_EQ(
        bits::isBitSet(&source, bitIndices[i]), bits::isBitSet(&result, i));
  }
}

TEST_F(SimdUtilTest, transpose) {
  constexpr int32_t kMaxSize = 100;
  std::vector<int32_t> data32(kMaxSize);
  std::vector<int64_t> data64(kMaxSize);
  raw_vector<int32_t> indices(kMaxSize);
  constexpr int64_t kMagic = 0x4fe12LU;
  // indices are scattered over 0..kMaxSize - 1.
  for (auto i = 0; i < kMaxSize; ++i) {
    indices[i] = ((i * kMagic) & 0xffffff) % indices.size();
    data32[i] = i;
    data64[i] = static_cast<int64_t>(i) << 32;
  }
  for (auto size = 1; size < kMaxSize; ++size) {
    std::vector<int32_t> result32(kMaxSize + 1, -1);
    simd::transpose(
        data32.data(),
        folly::Range<const int32_t*>(indices.data(), size),
        result32.data());
    for (auto i = 0; i < size; ++i) {
      EXPECT_EQ(data32[indices[i]], result32[i]);
    }
    // See that there is no write past 'size'.
    EXPECT_EQ(-1, result32[size]);

    std::vector<int64_t> result64(kMaxSize + 1, -1);
    simd::transpose(
        data64.data(),
        folly::Range<const int32_t*>(indices.data(), size),
        result64.data());
    for (auto i = 0; i < size; ++i) {
      EXPECT_EQ(data64[indices[i]], result64[i]);
    }
    // See that there is no write past 'size'.
    EXPECT_EQ(-1, result64[size]);
  }
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

template <typename T>
void validateReinterpretBatch() {
  using U = typename std::make_unsigned<T>::type;
  T data[xsimd::batch<T>::size]{};
  static_assert(xsimd::batch<T>::size >= 2);
  data[0] = -1;
  data[1] = 123;
  auto origin = xsimd::batch<T>::load_unaligned(data);
  auto casted = simd::reinterpretBatch<U>(origin);
  auto origin2 = simd::reinterpretBatch<T>(origin);
  ASSERT_EQ(casted.get(0), std::numeric_limits<U>::max());
  ASSERT_EQ(origin2.get(0), -1);
  ASSERT_EQ(casted.get(1), 123);
  ASSERT_EQ(origin2.get(1), 123);
  for (int i = 2; i < origin.size; ++i) {
    ASSERT_EQ(casted.get(i), 0);
    ASSERT_EQ(origin.get(i), 0);
  }
}

TEST_F(SimdUtilTest, reinterpretBatch) {
  validateReinterpretBatch<int8_t>();
  validateReinterpretBatch<int16_t>();
  validateReinterpretBatch<int32_t>();
  validateReinterpretBatch<int64_t>();
}

TEST_F(SimdUtilTest, memEqualUnsafe) {
  constexpr int32_t kSize = 132;
  struct {
    char x[kSize];
    char y[kSize];
    char padding[sizeof(xsimd::batch<uint8_t>)];
  } data;
  memset(&data, 11, sizeof(data));
  EXPECT_TRUE(simd::memEqualUnsafe(data.x, data.y, kSize));
  EXPECT_TRUE(simd::memEqualUnsafe(data.x, data.y, 17));
  EXPECT_TRUE(simd::memEqualUnsafe(data.x, data.y, 32));
  EXPECT_TRUE(simd::memEqualUnsafe(data.x, data.y, 33));

  // Make data at 67 not equal.
  data.y[67] = 0;
  EXPECT_TRUE(simd::memEqualUnsafe(data.x, data.y, 67));
  EXPECT_FALSE(simd::memEqualUnsafe(data.x, data.y, 68));

  // Redo the test offset by 1 to test unaligned.
  EXPECT_TRUE(simd::memEqualUnsafe(&data.x[1], &data.y[1], 66));
  EXPECT_FALSE(simd::memEqualUnsafe(&data.x[1], &data.y[1], 67));
}

TEST_F(SimdUtilTest, memcpyTime) {
  constexpr int64_t kMaxMove = 128;
  constexpr int64_t kSize = (128 << 20) + kMaxMove;
  constexpr uint64_t kSizeMask = (128 << 20) - 1;
  constexpr int32_t kMoveMask = kMaxMove - 1;
  constexpr uint64_t kMagic1 = 0x5231871;
  constexpr uint64_t kMagic3 = 0xfae1;
  constexpr uint64_t kMagic2 = 0x817952491;
  std::vector<char> dataV(kSize);

  auto data = dataV.data();
  uint64_t simd = 0;
  uint64_t sys = 0;
  {
    MicrosecondTimer t(&simd);
    for (auto ctr = 0; ctr < 100; ++ctr) {
      for (auto i = 0; i < 10000; ++i) {
        char* from = data + ((i * kMagic1) & kSizeMask);
        char* to = data + ((i * kMagic2) & kSizeMask);
        int32_t size = (i * kMagic3) % kMoveMask;
        simd::memcpy(to, from, size);
      }
    }
  }
  {
    MicrosecondTimer t(&sys);
    for (auto ctr = 0; ctr < 100; ++ctr) {
      for (auto i = 0; i < 10000; ++i) {
        char* from = data + ((i * kMagic1) & kSizeMask);
        char* to = data + ((i * kMagic2) & kSizeMask);
        int32_t size = (i * kMagic3) % kMoveMask;
        ::memcpy(to, from, size);
      }
    }
  }
  LOG(INFO) << "simd=" << simd << " sys=" << sys;
}

/// Copy from std::boyer_moore_searcher proposal:
/// https://github.com/mclow/search-library/blob/master/basic_tests.cpp
/// Basic sanity checking. It makes sure that all the algorithms work.
TEST_F(SimdUtilTest, basicSimdStrStr) {
  auto checkOne = [](const std::string& text, const std::string& needle) {
    auto size = text.size();
    auto k = needle.size();
    ASSERT_EQ(
        simd::simdStrstr(text.data(), size, needle.data(), k),
        text.find(needle));
  };
  std::string haystack1("NOW AN FOWE\220ER ANNMAN THE ANPANMANEND");
  std::string needle1("ANPANMAN");
  std::string needle2("MAN THE");
  std::string needle3("WE\220ER");
  // At the beginning
  std::string needle4("NOW ");
  // At the end
  std::string needle5("NEND");
  // Nowhere
  std::string needle6("NOT FOUND");
  // Nowhere
  std::string needle7("NOT FO\340ND");

  std::string haystack2("ABC ABCDAB ABCDABCDABDE");
  std::string needle11("ABCDABD");

  std::string haystack3("abra abracad abracadabra");
  std::string needle12("abracadabra");

  std::string needle13("");
  std::string haystack4("");

  checkOne(haystack1, needle1);
  checkOne(haystack1, needle2);
  checkOne(haystack1, needle3);
  checkOne(haystack1, needle4);
  checkOne(haystack1, needle5);
  checkOne(haystack1, needle6);
  checkOne(haystack1, needle7);

  // Cant find long pattern in short corpus
  checkOne(needle1, haystack1);
  // Find something in itself
  checkOne(haystack1, haystack1);
  // Find something in itself
  checkOne(haystack2, haystack2);

  checkOne(haystack2, needle11);
  checkOne(haystack3, needle12);
  // Find the empty string
  checkOne(haystack1, needle13);
  // Can't find in an empty haystack
  checkOne(haystack4, needle1);

  // Comment copy from the origin code.
  // Mikhail Levin <svarneticist@gmail.com> found a problem, and this was the
  // test that triggered it.
  const std::string mikhailPattern =
      "GATACACCTACCTTCACCAGTTACTCTATGCACTAGGTGCGCCAGGCCCATGCACAAGGGCTTGAGTGGATGGGAAGGA"
      "TGTGCCCTAGTGATGGCAGCATAAGCTACGCAGAGAAGTTCCAGGGCAGAGTCACCATGACCAGGGACACATCCACGAG"
      "CACAGCCTACATGGAGCTGAGCAGCCTGAGATCTGAAGACACGGCCATGTATTACTGTGGGAGAGATGTCTGGAGTGGT"
      "TATTATTGCCCCGGTAATATTACTACTACTACTACTACATGGACGTCTGGGGCAAAGGGACCACG";
  const std::string mikhailCorpus = std::string(8, 'a') + mikhailPattern;

  checkOne(mikhailCorpus, mikhailPattern);
}

TEST_F(SimdUtilTest, variableNeedleSize) {
  std::string s1 = "aabbccddeeffgghhiijjkkllmmnnooppqqrrssttuuvvwwxxyyzz";
  std::string s2 = "aabbccddeeffgghhiijjkkllmmnnooppqqrrssttuuvvwwxxyyzz";
  std::string s3 = "01234567890123456789";
  auto test = [](char* text, size_t size, char* needle, size_t k) {
    if (simd::simdStrstr(text, size, needle, k) !=
        std::string_view(text, size).find(std::string_view(needle, k))) {
      LOG(ERROR) << "text: " << std::string(text, size)
                 << " needle :" << std::string(needle, k);
    }
    ASSERT_EQ(
        simd::simdStrstr(text, size, needle, k),
        std::string_view(text, size).find(std::string_view(needle, k)));
  };
  // Match cases (prefix/middle/suffix): substrings in s2 should be a substring
  // in s1. Choose different needle-size left from s2, testing prefix-match in
  // s1.
  for (int k = 0; k < s2.size(); k++) {
    test(s1.data(), s1.size(), s2.data(), k);
  }
  // Choose different needle-size left from s2, testing middle-match in s1.
  for (int i = 0; i < 20; i++) {
    for (int k = 0; k < 28; k++) {
      char* data = s2.data() + i;
      test(s1.data(), s1.size(), data, k);
    }
  }
  // Choose different needle-size right from s2, testing suffix-match in s1.
  for (int k = 0; k < s2.size(); k++) {
    char* data = s2.data() + s2.size() - k;
    test(s1.data(), s1.size(), data, k);
  }
  // Not match case : substring in s3 not in s1.
  for (auto k = 0; k < s3.size(); k++) {
    test(s1.data(), s1.size(), s3.data(), k);
  }

  // FirstBlock match
  for (auto k = 0; k < s3.size(); k++) {
    std::string somePrefix = "xxxxxx";
    std::string matchString = "a" + std::string(k, 'x');
    std::string someSuffix = "yyyyyyyy";
    std::string text = somePrefix + matchString + someSuffix;
    auto s = "a" + std::string(k, '9');
    test(text.data(), text.size(), s.data(), s.size());
  }
  // FirstBlock and LastBlock match
  for (auto k = 0; k < s3.size(); k++) {
    std::string somePrefix = "xxxxxx";
    std::string matchString = "a" + std::string(k, 'x') + "b";
    std::string someSuffix = "yyyyyyyy";
    std::string text = somePrefix + matchString + someSuffix;
    auto s = "a" + std::string(k, '9') + "b";
    test(text.data(), text.size(), s.data(), s.size());
  }
}

/// Copy from
/// https://github.com/facebook/folly/blob/ce5edfb9b08ead9e78cb46879e7b9499861f7cd2/folly/test/FBStringTest.cpp#L1277
/// clause11_21_4_7_2_a1
TEST_F(SimdUtilTest, randomStringStrStr) {
  std::string test;
  const int kTestLoop = 1;
  auto checkOne =
      [](const std::string& text, const std::string& needle, size_t pos) {
        auto size = text.length() - pos;
        auto textPtr = text.data() + pos;
        auto k = needle.size();
        ASSERT_EQ(
            simd::simdStrstr(textPtr, size, needle.data(), k),
            text.substr(pos).find(needle));
      };
  for (int i = 0; i < kTestLoop; i++) {
    // clause11_21_4_7_2_a1
    randomString(&test);
    auto from = random(0, test.size());
    auto length = random(0, test.size() - from);
    std::string str = test.substr(from, length);
    checkOne(test, str, random(0, test.size()));
  }
}

} // namespace
