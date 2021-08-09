/*
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
#include "velox/aggregates/hyperloglog/DenseHll.h"
#define XXH_INLINE_ALL
#include <gtest/gtest.h>
#include "velox/external/xxhash.h"

using namespace facebook::velox;
using namespace facebook::velox::aggregate::hll;

template <typename T>
uint64_t hashOne(T value) {
  return XXH64(&value, sizeof(value), 0);
}

class DenseHllTest : public ::testing::TestWithParam<int8_t> {
 protected:
  DenseHll roundTrip(DenseHll& hll) {
    auto size = hll.serializedSize();
    std::string serialized;
    serialized.resize(size);
    hll.serialize(serialized.data());

    return DenseHll(serialized.data(), &allocator_);
  }

  std::string serialize(DenseHll& denseHll) {
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
    DenseHll hllLeft{indexBitLength, &allocator_};
    DenseHll hllRight{indexBitLength, &allocator_};
    DenseHll expected{indexBitLength, &allocator_};

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
      auto serializedRight = serialize(hllRight);
      hllLeft.mergeWith(serializedRight.data());
    } else {
      hllLeft.mergeWith(hllRight);
    }

    ASSERT_EQ(hllLeft.cardinality(), expected.cardinality());
    ASSERT_EQ(serialize(hllLeft), serialize(expected));
  }

  exec::HashStringAllocator allocator_{memory::MappedMemory::getInstance()};
};

TEST_P(DenseHllTest, basic) {
  int8_t indexBitLength = GetParam();

  DenseHll denseHll{indexBitLength, &allocator_};
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

  DenseHll deserialized = roundTrip(denseHll);
  ASSERT_EQ(expectedCardinality, deserialized.cardinality());
}

TEST_P(DenseHllTest, highCardinality) {
  int8_t indexBitLength = GetParam();

  DenseHll denseHll{indexBitLength, &allocator_};
  for (int i = 0; i < 10'000'000; i++) {
    auto hash = hashOne(i);
    denseHll.insertHash(hash);
  }

  if (indexBitLength >= 11) {
    ASSERT_NEAR(10'000'000, denseHll.cardinality(), 150'000);
  }

  DenseHll deserialized = roundTrip(denseHll);
  ASSERT_EQ(denseHll.cardinality(), deserialized.cardinality());
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

TEST_P(DenseHllTest, mergeWith) {
  int8_t indexBitLength = GetParam();

  // small, non-overlapping
  testMergeWith(indexBitLength, sequence(0, 100), sequence(100, 200));
  testMergeWith(indexBitLength, sequence(100, 200), sequence(0, 100));

  // small, overlapping
  testMergeWith(indexBitLength, sequence(0, 100), sequence(50, 150));
  testMergeWith(indexBitLength, sequence(50, 150), sequence(0, 100));

  // small, same
  testMergeWith(indexBitLength, sequence(0, 100), sequence(0, 100));

  // large, non-overlapping
  testMergeWith(indexBitLength, sequence(0, 20'000), sequence(20'000, 40'000));
  testMergeWith(indexBitLength, sequence(20'000, 40'000), sequence(0, 20'000));

  // large, overlapping
  testMergeWith(
      indexBitLength, sequence(0, 2'000'000), sequence(1'000'000, 3'000'000));
  testMergeWith(
      indexBitLength, sequence(1'000'000, 3'000'000), sequence(0, 2'000'000));

  // large, same
  testMergeWith(indexBitLength, sequence(0, 2'000'000), sequence(0, 2'000'000));
}

INSTANTIATE_TEST_SUITE_P(
    DenseHllTest,
    DenseHllTest,
    ::testing::Values(4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16));
