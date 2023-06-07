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

#include "velox/common/base/BloomFilter.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/AggregationTestBase.h"
#include "velox/functions/sparksql/aggregates/Register.h"

namespace facebook::velox::functions::aggregate::sparksql::test {
namespace {
class BloomFilterAggAggregateTest
    : public aggregate::test::AggregationTestBase {
 public:
  BloomFilterAggAggregateTest() {
    AggregationTestBase::SetUp();
    registerAggregateFunctions("");
    allowInputShuffle();
  }

  std::string getSerializedBloomFilter(int32_t capacity) {
    BloomFilter bloomFilter;
    bloomFilter.reset(capacity);
    for (auto i = 0; i < 9; ++i) {
      bloomFilter.insert(folly::hasher<int64_t>()(i));
    }
    std::string data;
    data.resize(bloomFilter.serializedSize());
    bloomFilter.serialize(data.data());
    return data;
  }
};
} // namespace

TEST_F(BloomFilterAggAggregateTest, basic) {
  auto vectors = {makeRowVector({makeFlatVector<int64_t>(
      100, [](vector_size_t row) { return row % 9; })})};
  auto bloomFilter = getSerializedBloomFilter(4);
  auto expected = {
      makeRowVector({makeConstant<StringView>(StringView(bloomFilter), 1)})};

  testAggregations(vectors, {}, {"bloom_filter_agg(c0, 5, 64)"}, expected);
}

TEST_F(BloomFilterAggAggregateTest, bloomFilterAggArgument) {
  auto vectors = {makeRowVector({makeFlatVector<int64_t>(
      100, [](vector_size_t row) { return row % 9; })})};

  auto bloomFilter1 = getSerializedBloomFilter(3);
  auto expected1 = {
      makeRowVector({makeConstant<StringView>(StringView(bloomFilter1), 1)})};
  testAggregations(vectors, {}, {"bloom_filter_agg(c0, 6)"}, expected1);
  // This capacity is kMaxNumBits / 16.
  auto bloomFilter2 = getSerializedBloomFilter(262144);
  auto expected2 = {
      makeRowVector({makeConstant<StringView>(StringView(bloomFilter2), 1)})};
  testAggregations(vectors, {}, {"bloom_filter_agg(c0)"}, expected2);
}

TEST_F(BloomFilterAggAggregateTest, emptyInput) {
  auto vectors = {makeRowVector({makeFlatVector<int64_t>({})})};
  auto expected = {makeRowVector(
      {makeNullableFlatVector<StringView>({std::nullopt}, VARBINARY())})};
  testAggregations(vectors, {}, {"bloom_filter_agg(c0, 5, 64)"}, expected);
}

TEST_F(BloomFilterAggAggregateTest, nullBloomFilter) {
  auto vectors = {makeRowVector({makeAllNullFlatVector<int64_t>(2)})};
  auto expectedFake = {makeRowVector(
      {makeNullableFlatVector<StringView>({std::nullopt}, VARBINARY())})};
  VELOX_ASSERT_THROW(
      testAggregations(
          vectors, {}, {"bloom_filter_agg(c0, 5, 64)"}, expectedFake),
      "First argument of bloom_filter_agg cannot be null");
}
} // namespace facebook::velox::functions::aggregate::sparksql::test
