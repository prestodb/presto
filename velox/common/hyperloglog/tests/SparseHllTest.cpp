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
#include "velox/common/hyperloglog/SparseHll.h"

#define XXH_INLINE_ALL
#include <xxhash.h>

#include <gtest/gtest-typed-test.h>
#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::common::hll;

template <typename T>
uint64_t hashOne(T value) {
  return XXH64(&value, sizeof(value), 0);
}

template <typename TAllocator>
class SparseHllTest : public ::testing::Test {
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
    SparseHll hllLeft{allocator_};
    SparseHll hllRight{allocator_};
    SparseHll expected{allocator_};

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

    hllLeft.verify();
    hllRight.verify();

    if (serialized) {
      auto serializedRight = serialize(11, hllRight);
      hllLeft.mergeWith(serializedRight.data());
    } else {
      hllLeft.mergeWith(hllRight);
    }

    hllLeft.verify();

    ASSERT_EQ(hllLeft.cardinality(), expected.cardinality());

    auto hllLeftSerialized = serialize(11, hllLeft);
    ASSERT_EQ(
        SparseHlls::cardinality(hllLeftSerialized.data()),
        expected.cardinality());
  }

  SparseHll<TAllocator> roundTrip(
      SparseHll<TAllocator>& hll,
      int8_t indexBitLength = 11) {
    auto serialized = serialize(indexBitLength, hll);
    return SparseHll(serialized.data(), allocator_);
  }

  std::string serialize(
      int8_t indexBitLength,
      const SparseHll<TAllocator>& sparseHll) {
    auto size = sparseHll.serializedSize();
    std::string serialized;
    serialized.resize(size);
    sparseHll.serialize(indexBitLength, serialized.data());
    return serialized;
  }

  std::string serialize(DenseHll<TAllocator>& denseHll) {
    auto size = denseHll.serializedSize();
    std::string serialized;
    serialized.resize(size);
    denseHll.serialize(serialized.data());
    return serialized;
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

TYPED_TEST_SUITE(SparseHllTest, AllocatorTypes, NameGenerator);

TYPED_TEST(SparseHllTest, basic) {
  SparseHll sparseHll{this->allocator_};
  for (int i = 0; i < 1'000; i++) {
    auto value = i % 17;
    auto hash = hashOne(value);
    sparseHll.insertHash(hash);
  }

  sparseHll.verify();
  ASSERT_EQ(17, sparseHll.cardinality());

  auto deserialized = this->roundTrip(sparseHll);
  deserialized.verify();
  ASSERT_EQ(17, deserialized.cardinality());

  auto serialized = this->serialize(11, sparseHll);
  ASSERT_EQ(17, SparseHlls::cardinality(serialized.data()));
}

TYPED_TEST(SparseHllTest, highCardinality) {
  SparseHll sparseHll{this->allocator_};
  for (int i = 0; i < 1'000; i++) {
    auto hash = hashOne(i);
    sparseHll.insertHash(hash);
  }

  sparseHll.verify();
  ASSERT_EQ(1'000, sparseHll.cardinality());

  auto deserialized = this->roundTrip(sparseHll);
  deserialized.verify();
  ASSERT_EQ(1'000, deserialized.cardinality());

  auto serialized = this->serialize(11, sparseHll);
  ASSERT_EQ(1'000, SparseHlls::cardinality(serialized.data()));
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

TYPED_TEST(SparseHllTest, mergeWith) {
  // with overlap
  this->testMergeWith(sequence(0, 100), sequence(50, 150));
  this->testMergeWith(sequence(50, 150), sequence(0, 100));

  // no overlap
  this->testMergeWith(sequence(0, 100), sequence(200, 300));
  this->testMergeWith(sequence(200, 300), sequence(0, 100));

  // idempotent
  this->testMergeWith(sequence(0, 100), sequence(0, 100));

  // empty sequence
  this->testMergeWith(sequence(0, 100), {});
  this->testMergeWith({}, sequence(100, 300));
}

TYPED_TEST(SparseHllTest, toDense) {
  int8_t indexBitLength = 11;

  SparseHll sparseHll{this->allocator_};
  DenseHll expectedHll{indexBitLength, this->allocator_};
  for (int i = 0; i < 1'000; i++) {
    auto hash = hashOne(i);
    sparseHll.insertHash(hash);
    expectedHll.insertHash(hash);
  }

  DenseHll denseHll{indexBitLength, this->allocator_};
  sparseHll.toDense(denseHll);
  ASSERT_EQ(denseHll.cardinality(), expectedHll.cardinality());
  ASSERT_EQ(this->serialize(denseHll), this->serialize(expectedHll));
}

TYPED_TEST(SparseHllTest, testNumberOfZeros) {
  int8_t indexBitLength = 11;
  for (int i = 0; i < 64 - indexBitLength; ++i) {
    auto hash = 1ull << i;
    SparseHll sparseHll(this->allocator_);
    sparseHll.insertHash(hash);
    DenseHll expectedHll(indexBitLength, this->allocator_);
    expectedHll.insertHash(hash);
    DenseHll denseHll(indexBitLength, this->allocator_);
    sparseHll.toDense(denseHll);
    ASSERT_EQ(this->serialize(denseHll), this->serialize(expectedHll));
  }
}

template <typename TAllocator, int8_t IndexBitLength>
struct AllocatorWithIndexBits {
  using AllocatorType = TAllocator;
  static constexpr int8_t indexBitLength() {
    return IndexBitLength;
  }
};

template <typename TParam>
class SparseHllToDenseTest : public ::testing::Test {
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

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  HashStringAllocator hsa_{pool_.get()};
  typename TParam::AllocatorType* allocator_;
};

using SparseHllToDenseTestParams = ::testing::Types<
    // HashStringAllocator with various index bit lengths
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
    // MemoryPool with various index bit lengths
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

class ToDenseNameGenerator {
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
    SparseHllToDenseTest,
    SparseHllToDenseTestParams,
    ToDenseNameGenerator);

TYPED_TEST(SparseHllToDenseTest, toDense) {
  int8_t indexBitLength = TypeParam::indexBitLength();

  SparseHll sparseHll{this->allocator_};
  DenseHll expectedHll{indexBitLength, this->allocator_};
  for (int i = 0; i < 1'000; i++) {
    auto hash = hashOne(i);
    sparseHll.insertHash(hash);
    expectedHll.insertHash(hash);
  }

  DenseHll denseHll{indexBitLength, this->allocator_};
  sparseHll.toDense(denseHll);
  ASSERT_EQ(denseHll.cardinality(), expectedHll.cardinality());
  ASSERT_EQ(this->serialize(denseHll), this->serialize(expectedHll));
}

TYPED_TEST(SparseHllToDenseTest, testNumberOfZeros) {
  auto indexBitLength = TypeParam::indexBitLength();
  for (int i = 0; i < 64 - indexBitLength; ++i) {
    auto hash = 1ull << i;
    SparseHll sparseHll(this->allocator_);
    sparseHll.insertHash(hash);
    DenseHll expectedHll(indexBitLength, this->allocator_);
    expectedHll.insertHash(hash);
    DenseHll denseHll(indexBitLength, this->allocator_);
    sparseHll.toDense(denseHll);
    ASSERT_EQ(this->serialize(denseHll), this->serialize(expectedHll));
  }
}
