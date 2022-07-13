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
#include "velox/functions/prestosql/hyperloglog/DenseHll.h"

#include <gtest/gtest.h>
#include <algorithm>
#include <map>
#include <vector>

#define XXH_INLINE_ALL
#include <xxhash.h>

#include "velox/common/encode/Base64.h"

using namespace facebook::velox;
using namespace facebook::velox::aggregate::hll;
using namespace facebook::velox::encoding;

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

    auto hllLeftSerialized = serialize(hllLeft);
    ASSERT_EQ(
        DenseHll::cardinality(hllLeftSerialized.data()),
        expected.cardinality());
  }

  HashStringAllocator allocator_{memory::MappedMemory::getInstance()};
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

  auto serialized = serialize(denseHll);
  ASSERT_EQ(expectedCardinality, DenseHll::cardinality(serialized.data()));
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

  auto serialized = serialize(denseHll);
  ASSERT_EQ(denseHll.cardinality(), DenseHll::cardinality(serialized.data()));
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

TEST_P(DenseHllTest, canDeserialize) {
  // These are not valid HLL but all pass canDeserialize version only check.
  std::vector<folly::StringPiece> invalidStrings{
      "AxIRESUhEzNBFCQWYxEjIzI1ISURNidCMlViIjOSNyATBhYSIiJDUyMBIlcSMDUiEUEiESM1ITckQkQTMSMhMyQx",
      "Aw==",
      "AyAAABEQAgAlAgAQAQAlMgAhQQABAwAAERAAAAEQACA=",
      "AwDuUjGFaQ==",
      "AwkLD8BYTA9BXyg="};

  for (folly::StringPiece& invalidString : invalidStrings) {
    auto invalidHll = Base64::decode(invalidString);
    EXPECT_TRUE(DenseHll::canDeserialize(invalidHll.c_str()));
    EXPECT_FALSE(
        DenseHll::canDeserialize(invalidHll.c_str(), invalidHll.length()));
  }

  std::vector<folly::StringPiece> validStrings{
      "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",
  };

  for (folly::StringPiece& validString : validStrings) {
    auto validHll = Base64::decode(validString);
    EXPECT_TRUE(DenseHll::canDeserialize(validHll.c_str()));
    EXPECT_TRUE(DenseHll::canDeserialize(validHll.c_str(), validHll.length()));
  }
}

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
