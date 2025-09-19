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
#include "velox/common/hyperloglog/DenseHll.h"

#include <gtest/gtest-typed-test.h>
#include <gtest/gtest.h>
#include <vector>

#define XXH_INLINE_ALL
#include <xxhash.h>

#include "velox/common/encode/Base64.h"

using namespace facebook::velox;
using namespace facebook::velox::common::hll;
using namespace facebook::velox::encoding;

template <typename T>
uint64_t hashOne(T value) {
  return XXH64(&value, sizeof(value), 0);
}

template <typename TAllocator>
class DenseHllTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    if constexpr (std::is_same_v<TAllocator, HashStringAllocator>) {
      allocator_ = &hsa_;
    } else {
      allocator_ = pool_.get();
    }
  }

  DenseHll<TAllocator> roundTrip(DenseHll<TAllocator>& hll) {
    auto serialized = this->serialize(hll);
    return DenseHll(serialized.data(), allocator_);
  }

  std::string serialize(DenseHll<TAllocator>& denseHll) {
    auto size = denseHll.serializedSize();
    std::string serialized;
    serialized.resize(size);
    denseHll.serialize(serialized.data());
    return serialized;
  }

  template <typename T>
  void testMergeWith(const std::vector<T>& left, const std::vector<T>& right) {
    testMergeWith(left, right, false);
    testMergeWith(left, right, true);
  }

  template <typename T>
  void testMergeWith(
      const std::vector<T>& left,
      const std::vector<T>& right,
      bool serialized) {
    DenseHll hllLeft{11, allocator_};
    DenseHll hllRight{11, allocator_};
    DenseHll expected{11, allocator_};

    for (auto value : left) {
      auto hash = hashOne(value);
      hllLeft.insertHash(hash);
      expected.insertHash(hash);
    }

    for (auto value : right) {
      auto hash = hashOne(value);
      hllRight.insertHash(hash);
      expected.insertHash(hash);
    }

    if (serialized) {
      auto serializedRight = this->serialize(hllRight);
      hllLeft.mergeWith(serializedRight.data());
    } else {
      hllLeft.mergeWith(hllRight);
    }

    ASSERT_EQ(hllLeft.cardinality(), expected.cardinality());
    ASSERT_EQ(this->serialize(hllLeft), this->serialize(expected));

    auto hllLeftSerialized = this->serialize(hllLeft);
    ASSERT_EQ(
        DenseHlls::cardinality(hllLeftSerialized.data()),
        expected.cardinality());
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  HashStringAllocator hsa_{pool_.get()};
  TAllocator* allocator_;
};

using AllocatorTypes =
    ::testing::Types<HashStringAllocator, memory::MemoryPool>;

class NameGenerator {
 public:
  template <typename TAllocator>
  static std::string GetName(int) {
    if constexpr (std::is_same_v<TAllocator, HashStringAllocator>) {
      return "hsa";
    } else if constexpr (std::is_same_v<TAllocator, memory::MemoryPool>) {
      return "pool";
    } else {
      VELOX_UNREACHABLE(
          "Only HashStringAllocator and MemoryPool are supported allocator types.");
    }
  }
};

TYPED_TEST_SUITE(DenseHllTest, AllocatorTypes, NameGenerator);

TYPED_TEST(DenseHllTest, basic) {
  int8_t indexBitLength = 11;
  DenseHll denseHll{indexBitLength, this->allocator_};

  for (int i = 0; i < 1'000; i++) {
    auto value = i % 17;
    auto hash = hashOne(value);
    denseHll.insertHash(hash);
  }

  // We cannot get accurate estimate with very small number of index bits.
  auto expectedCardinality = 17;
  if (indexBitLength <= 5) {
    expectedCardinality = 13;
  } else if (indexBitLength == 6) {
    expectedCardinality = 15;
  } else if (indexBitLength == 7) {
    expectedCardinality = 16;
  }

  ASSERT_EQ(expectedCardinality, denseHll.cardinality());

  DenseHll deserialized = this->roundTrip(denseHll);
  ASSERT_EQ(expectedCardinality, deserialized.cardinality());

  auto serialized = this->serialize(denseHll);
  ASSERT_EQ(expectedCardinality, DenseHlls::cardinality(serialized.data()));
}

TYPED_TEST(DenseHllTest, highCardinality) {
  int8_t indexBitLength = 11;
  DenseHll denseHll{indexBitLength, this->allocator_};

  for (int i = 0; i < 10'000'000; i++) {
    auto hash = hashOne(i);
    denseHll.insertHash(hash);
  }

  ASSERT_NEAR(10'000'000, denseHll.cardinality(), 150'000);

  auto deserialized = this->roundTrip(denseHll);
  ASSERT_EQ(denseHll.cardinality(), deserialized.cardinality());

  auto serialized = this->serialize(denseHll);
  ASSERT_EQ(denseHll.cardinality(), DenseHlls::cardinality(serialized.data()));
}

namespace {
template <typename T>
std::vector<T> sequence(T start, T end) {
  std::vector<T> data;
  data.reserve(end - start);
  for (auto i = start; i < end; i++) {
    data.push_back(i);
  }
  return data;
}
} // namespace

TYPED_TEST(DenseHllTest, mergeWith) {
  // small, non-overlapping
  this->testMergeWith(sequence(0, 100), sequence(100, 200));
  this->testMergeWith(sequence(100, 200), sequence(0, 100));

  // small, overlapping
  this->testMergeWith(sequence(0, 100), sequence(50, 150));
  this->testMergeWith(sequence(50, 150), sequence(0, 100));

  // small, same
  this->testMergeWith(sequence(0, 100), sequence(0, 100));

  // large, non-overlapping
  this->testMergeWith(sequence(0, 20'000), sequence(20'000, 40'000));
  this->testMergeWith(sequence(20'000, 40'000), sequence(0, 20'000));

  // large, overlapping
  this->testMergeWith(sequence(0, 2'000'000), sequence(1'000'000, 3'000'000));
  this->testMergeWith(sequence(1'000'000, 3'000'000), sequence(0, 2'000'000));

  // large, same
  this->testMergeWith(sequence(0, 2'000'000), sequence(0, 2'000'000));
}

// Separate test class for testing various index bit lengths
template <typename TAllocator, int8_t IndexBitLength>
struct AllocatorWithIndexBits {
  using AllocatorType = TAllocator;
  static constexpr int8_t indexBitLength() {
    return IndexBitLength;
  }
};

template <typename TParam>
class DenseHllMergeTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    if constexpr (std::is_same_v<
                      typename TParam::AllocatorType,
                      HashStringAllocator>) {
      allocator_ = &hsa_;
    } else {
      allocator_ = pool_.get();
    }
  }

  std::string serialize(DenseHll<typename TParam::AllocatorType>& denseHll) {
    auto size = denseHll.serializedSize();
    std::string serialized;
    serialized.resize(size);
    denseHll.serialize(serialized.data());
    return serialized;
  }

  template <typename T>
  void testMergeWith(
      int8_t indexBitLength,
      const std::vector<T>& left,
      const std::vector<T>& right) {
    testMergeWith(indexBitLength, left, right, false);
    testMergeWith(indexBitLength, left, right, true);
  }

  template <typename T>
  void testMergeWith(
      int8_t indexBitLength,
      const std::vector<T>& left,
      const std::vector<T>& right,
      bool serialized) {
    DenseHll hllLeft{indexBitLength, allocator_};
    DenseHll hllRight{indexBitLength, allocator_};
    DenseHll expected{indexBitLength, allocator_};

    for (auto value : left) {
      auto hash = hashOne(value);
      hllLeft.insertHash(hash);
      expected.insertHash(hash);
    }

    for (auto value : right) {
      auto hash = hashOne(value);
      hllRight.insertHash(hash);
      expected.insertHash(hash);
    }

    if (serialized) {
      auto serializedRight = this->serialize(hllRight);
      hllLeft.mergeWith(serializedRight.data());
    } else {
      hllLeft.mergeWith(hllRight);
    }

    ASSERT_EQ(hllLeft.cardinality(), expected.cardinality());
    ASSERT_EQ(this->serialize(hllLeft), this->serialize(expected));

    auto hllLeftSerialized = this->serialize(hllLeft);
    ASSERT_EQ(
        DenseHlls::cardinality(hllLeftSerialized.data()),
        expected.cardinality());
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  HashStringAllocator hsa_{pool_.get()};
  typename TParam::AllocatorType* allocator_;
};

using DenseHllMergeTestParams = ::testing::Types<
    // HashStringAllocator with all index bit lengths
    AllocatorWithIndexBits<HashStringAllocator, 4>,
    AllocatorWithIndexBits<HashStringAllocator, 5>,
    AllocatorWithIndexBits<HashStringAllocator, 6>,
    AllocatorWithIndexBits<HashStringAllocator, 7>,
    AllocatorWithIndexBits<HashStringAllocator, 8>,
    AllocatorWithIndexBits<HashStringAllocator, 9>,
    AllocatorWithIndexBits<HashStringAllocator, 10>,
    AllocatorWithIndexBits<HashStringAllocator, 11>,
    AllocatorWithIndexBits<HashStringAllocator, 12>,
    AllocatorWithIndexBits<HashStringAllocator, 13>,
    AllocatorWithIndexBits<HashStringAllocator, 14>,
    AllocatorWithIndexBits<HashStringAllocator, 15>,
    AllocatorWithIndexBits<HashStringAllocator, 16>,
    // MemoryPool with all index bit lengths
    AllocatorWithIndexBits<memory::MemoryPool, 4>,
    AllocatorWithIndexBits<memory::MemoryPool, 5>,
    AllocatorWithIndexBits<memory::MemoryPool, 6>,
    AllocatorWithIndexBits<memory::MemoryPool, 7>,
    AllocatorWithIndexBits<memory::MemoryPool, 8>,
    AllocatorWithIndexBits<memory::MemoryPool, 9>,
    AllocatorWithIndexBits<memory::MemoryPool, 10>,
    AllocatorWithIndexBits<memory::MemoryPool, 11>,
    AllocatorWithIndexBits<memory::MemoryPool, 12>,
    AllocatorWithIndexBits<memory::MemoryPool, 13>,
    AllocatorWithIndexBits<memory::MemoryPool, 14>,
    AllocatorWithIndexBits<memory::MemoryPool, 15>,
    AllocatorWithIndexBits<memory::MemoryPool, 16>>;

class ComprehensiveNameGenerator {
 public:
  template <typename TParam>
  static std::string GetName(int) {
    std::string allocatorName;
    if constexpr (std::is_same_v<
                      typename TParam::AllocatorType,
                      HashStringAllocator>) {
      allocatorName = "hsa";
    } else if constexpr (std::is_same_v<
                             typename TParam::AllocatorType,
                             memory::MemoryPool>) {
      allocatorName = "pool";
    } else {
      VELOX_UNREACHABLE(
          "Only HashStringAllocator and MemoryPool are supported allocator types.");
    }
    return fmt::format("{}_{}", allocatorName, TParam::indexBitLength());
  }
};

TYPED_TEST_SUITE(
    DenseHllMergeTest,
    DenseHllMergeTestParams,
    ComprehensiveNameGenerator);

class DenseHllCanDeserializeTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

TEST_F(DenseHllCanDeserializeTest, canDeserialize) {
  // These are not valid HLL but all pass canDeserialize version only check.
  std::vector<folly::StringPiece> invalidStrings{
      "AxIRESUhEzNBFCQWYxEjIzI1ISURNidCMlViIjOSNyATBhYSIiJDUyMBIlcSMDUiEUEiESM1ITckQkQTMSMhMyQx",
      "Aw==",
      "AyAAABEQAgAlAgAQAQAlMgAhQQABAwAAERAAAAEQACA=",
      "AwDuUjGFaQ==",
      "AwkLD8BYTA9BXyg="};

  for (folly::StringPiece& invalidString : invalidStrings) {
    auto invalidHll = Base64::decode(invalidString);
    EXPECT_TRUE(DenseHlls::canDeserialize(invalidHll.c_str()));
    EXPECT_FALSE(
        DenseHlls::canDeserialize(invalidHll.c_str(), invalidHll.length()));
  }

  std::vector<folly::StringPiece> validStrings{
      "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",
  };

  for (folly::StringPiece& validString : validStrings) {
    auto validHll = Base64::decode(validString);
    EXPECT_TRUE(DenseHlls::canDeserialize(validHll.c_str()));
    EXPECT_TRUE(DenseHlls::canDeserialize(validHll.c_str(), validHll.length()));
  }
}

TYPED_TEST(DenseHllMergeTest, mergeWith) {
  int8_t indexBitLength = TypeParam::indexBitLength();

  // small, non-overlapping
  this->testMergeWith(indexBitLength, sequence(0, 100), sequence(100, 200));
  this->testMergeWith(indexBitLength, sequence(100, 200), sequence(0, 100));

  // small, overlapping
  this->testMergeWith(indexBitLength, sequence(0, 100), sequence(50, 150));
  this->testMergeWith(indexBitLength, sequence(50, 150), sequence(0, 100));

  // small, same
  this->testMergeWith(indexBitLength, sequence(0, 100), sequence(0, 100));

  // large, non-overlapping
  this->testMergeWith(
      indexBitLength, sequence(0, 20'000), sequence(20'000, 40'000));
  this->testMergeWith(
      indexBitLength, sequence(20'000, 40'000), sequence(0, 20'000));

  // large, overlapping
  this->testMergeWith(
      indexBitLength, sequence(0, 2'000'000), sequence(1'000'000, 3'000'000));
  this->testMergeWith(
      indexBitLength, sequence(1'000'000, 3'000'000), sequence(0, 2'000'000));

  // large, same
  this->testMergeWith(
      indexBitLength, sequence(0, 2'000'000), sequence(0, 2'000'000));
}
