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

#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::common::hll;

template <typename T>
uint64_t hashOne(T value) {
  return XXH64(&value, sizeof(value), 0);
}

class SparseHllTest : public ::testing::Test {
 protected:
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
    SparseHll hllLeft{&allocator_};
    SparseHll hllRight{&allocator_};
    SparseHll expected{&allocator_};

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
        SparseHll::cardinality(hllLeftSerialized.data()),
        expected.cardinality());
  }

  SparseHll roundTrip(SparseHll& hll) {
    auto serialized = serialize(11, hll);
    return SparseHll(serialized.data(), &allocator_);
  }

  std::string serialize(int8_t indexBitLength, const SparseHll& sparseHll) {
    auto size = sparseHll.serializedSize();
    std::string serialized;
    serialized.resize(size);
    sparseHll.serialize(indexBitLength, serialized.data());
    return serialized;
  }

  std::string serialize(DenseHll& denseHll) {
    auto size = denseHll.serializedSize();
    std::string serialized;
    serialized.resize(size);
    denseHll.serialize(serialized.data());
    return serialized;
  }

  HashStringAllocator allocator_{memory::MappedMemory::getInstance()};
};

TEST_F(SparseHllTest, basic) {
  SparseHll sparseHll{&allocator_};
  for (int i = 0; i < 1'000; i++) {
    auto value = i % 17;
    auto hash = hashOne(value);
    sparseHll.insertHash(hash);
  }

  sparseHll.verify();
  ASSERT_EQ(17, sparseHll.cardinality());

  auto deserialized = roundTrip(sparseHll);
  deserialized.verify();
  ASSERT_EQ(17, deserialized.cardinality());

  auto serialized = serialize(11, sparseHll);
  ASSERT_EQ(17, SparseHll::cardinality(serialized.data()));
}

TEST_F(SparseHllTest, highCardinality) {
  SparseHll sparseHll{&allocator_};
  for (int i = 0; i < 1'000; i++) {
    auto hash = hashOne(i);
    sparseHll.insertHash(hash);
  }

  sparseHll.verify();
  ASSERT_EQ(1'000, sparseHll.cardinality());

  auto deserialized = roundTrip(sparseHll);
  deserialized.verify();
  ASSERT_EQ(1'000, deserialized.cardinality());

  auto serialized = serialize(11, sparseHll);
  ASSERT_EQ(1'000, SparseHll::cardinality(serialized.data()));
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

TEST_F(SparseHllTest, mergeWith) {
  // with overlap
  testMergeWith(sequence(0, 100), sequence(50, 150));
  testMergeWith(sequence(50, 150), sequence(0, 100));

  // no overlap
  testMergeWith(sequence(0, 100), sequence(200, 300));
  testMergeWith(sequence(200, 300), sequence(0, 100));

  // idempotent
  testMergeWith(sequence(0, 100), sequence(0, 100));
}

class SparseHllToDenseTest : public ::testing::TestWithParam<int8_t> {
 protected:
  std::string serialize(DenseHll& denseHll) {
    auto size = denseHll.serializedSize();
    std::string serialized;
    serialized.resize(size);
    denseHll.serialize(serialized.data());
    return serialized;
  }

  HashStringAllocator allocator_{memory::MappedMemory::getInstance()};
};

TEST_P(SparseHllToDenseTest, toDense) {
  int8_t indexBitLength = GetParam();

  SparseHll sparseHll{&allocator_};
  DenseHll expectedHll{indexBitLength, &allocator_};
  for (int i = 0; i < 1'000; i++) {
    auto hash = hashOne(i);
    sparseHll.insertHash(hash);
    expectedHll.insertHash(hash);
  }

  DenseHll denseHll{indexBitLength, &allocator_};
  sparseHll.toDense(denseHll);
  ASSERT_EQ(denseHll.cardinality(), expectedHll.cardinality());
  ASSERT_EQ(serialize(denseHll), serialize(expectedHll));
}

INSTANTIATE_TEST_SUITE_P(
    SparseHllToDenseTest,
    SparseHllToDenseTest,
    ::testing::Values(4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16));
