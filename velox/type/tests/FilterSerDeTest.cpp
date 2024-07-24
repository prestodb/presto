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

#include <limits>
#include <memory>

#include <velox/type/Filter.h>
#include "velox/expression/ExprToSubfieldFilter.h"

#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::exec;

class FilterSerDeTest : public testing::Test {
 protected:
  FilterSerDeTest() {
    Filter::registerSerDe();
  }

  static void testSerde(const Filter& filter) {
    auto obj = filter.serialize();
    auto copy = ISerializable::deserialize<Filter>(obj);
    EXPECT_EQ(filter.toString(), copy->toString());
    ASSERT_TRUE(filter.testingEquals(*copy));
  }

  template <typename T>
  const std::vector<T> getValues(T lower, T upper) {
    size_t sz =
        3 + folly::Random::rand32() % 7; // values size must greater than 1
    std::vector<T> values;
    values.reserve(sz);
    for (size_t i = 0; i < sz; ++i) {
      auto num = lower + folly::Random::rand32() % (upper - lower);
      values.push_back(num);
    }
    return values;
  }
};

TEST_F(FilterSerDeTest, simpleFilters) {
  testSerde(AlwaysFalse());
  testSerde(AlwaysTrue());
  testSerde(IsNull());
  testSerde(IsNotNull());
  testSerde(BoolValue(true, false));
  testSerde(BoolValue(false, false));
}

TEST_F(FilterSerDeTest, bigintFilters) {
  uint64_t hi = 0XFFFFFFFFFFFF8A59;
  uint64_t lo = 0X99FC706655BFAC11;

  testSerde(BigintRange(lo, hi, true));
  testSerde(BigintRange(lo, hi, false));
  testSerde(NegatedBigintRange(lo, hi, true));
  testSerde(NegatedBigintRange(lo, hi, false));

  int128_t upper = HugeInt::build(hi, lo);
  hi = 0XABCDFFFFFFFF8A59;
  int128_t lower = HugeInt::build(hi, lo);
  testSerde(HugeintRange(lower, upper, true));
  testSerde(HugeintRange(lower, upper, false));
}

TEST_F(FilterSerDeTest, valuesFilters) {
  for (int r = 0; r < 7; ++r) {
    int64_t lower = 13;
    int64_t upper = 9527;
    std::vector<int64_t> values = getValues<int64_t>(lower, upper);
    size_t sz = values.size();
    std::vector<std::string> strValues;
    for (size_t i = 0; i < sz; ++i) {
      strValues.emplace_back(std::to_string(values[i]));
    }

    int128_t lowerHugeint = 0x0123456789ABCDEF;
    int128_t upperHugeint = 0xFEDCBA9876543210;
    std::vector<int128_t> valuesHugeint =
        getValues<int128_t>(lowerHugeint, upperHugeint);

    for (auto nullAllowed : {false, true}) {
      testSerde(BigintValuesUsingHashTable(lower, upper, values, nullAllowed));
      testSerde(BigintValuesUsingBitmask(lower, upper, values, nullAllowed));
      testSerde(
          NegatedBigintValuesUsingHashTable(lower, upper, values, nullAllowed));
      testSerde(
          NegatedBigintValuesUsingBitmask(lower, upper, values, nullAllowed));
      testSerde(BytesValues(strValues, nullAllowed));
      testSerde(NegatedBytesValues(strValues, nullAllowed));

      testSerde(HugeintValuesUsingHashTable(
          lowerHugeint, upperHugeint, valuesHugeint, nullAllowed));
    }
  }
}

TEST_F(FilterSerDeTest, rangeFilters) {
  FloatRange floatRange(1.0, true, true, 124.5, false, true, false);
  testSerde(floatRange);
  DoubleRange doubleRange(1.0, true, true, 124.5, false, true, false);
  testSerde(doubleRange);

  BytesRange bytesRange("ABCD", true, true, "FFFF", false, true, false);
  testSerde(bytesRange);
  NegatedBytesRange negatedBytesRange(
      "ABCD", true, true, "FFFF", false, true, false);
  testSerde(negatedBytesRange);
}

TEST_F(FilterSerDeTest, multiRangeFilter) {
  int64_t base = 13;
  int64_t lower = folly::Random::rand32() % base;
  int64_t upper = lower + folly::Random::rand32() % base + 1;
  std::vector<std::unique_ptr<BigintRange>> ranges;
  ranges.reserve(3);
  // bigint ranges must not overlap
  for (int i = 0; i < 3; ++i) {
    lower += base * i;
    upper += upper * i;
    ranges.emplace_back(
        std::make_unique<BigintRange>(BigintRange(lower, upper, true)));
  }
  BigintMultiRange bigintMultiRange(std::move(ranges), true);
  testSerde(bigintMultiRange);
}

TEST_F(FilterSerDeTest, multiFilter) {
  std::vector<std::unique_ptr<common::Filter>> filters;
  filters.emplace_back(std::make_unique<AlwaysTrue>());
  filters.emplace_back(std::make_unique<BoolValue>(false, true));
  filters.emplace_back(
      std::make_unique<BigintRange>(BigintRange(12, 798, true)));
  std::vector<int64_t> values{3, 7, 18};
  filters.emplace_back(
      std::make_unique<BigintValuesUsingHashTable>(1, 10, values, true));
  filters.emplace_back(std::make_unique<BytesRange>(
      "ABCD", true, true, "FFFF", false, true, false));

  MultiRange multiRange(std::move(filters), true);
  testSerde(multiRange);
}

TEST_F(FilterSerDeTest, timestampFilter) {
  Timestamp hi(100000, 2000);
  Timestamp lo(-123, 99999);

  testSerde(TimestampRange(lo, hi, true));
  testSerde(TimestampRange(lo, hi, false));
}
