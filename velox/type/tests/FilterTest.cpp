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

#include "velox/type/Filter.h"
#include "velox/type/tests/FilterBulder.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <numeric>

#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook ::velox::common;
using namespace facebook ::velox::common::test;

using V64 = simd::Vectors<int64_t>;
using V32 = simd::Vectors<int32_t>;
using V16 = simd::Vectors<int16_t>;

TEST(FilterTest, alwaysFalse) {
  AlwaysFalse alwaysFalse;
  EXPECT_FALSE(alwaysFalse.testInt64(1));
  EXPECT_FALSE(alwaysFalse.testNonNull());
  EXPECT_FALSE(alwaysFalse.testNull());

  EXPECT_EQ(
      "Filter(AlwaysFalse, deterministic, null not allowed)",
      alwaysFalse.toString());
}

TEST(FilterTest, alwaysTrue) {
  AlwaysTrue alwaysTrue;
  EXPECT_TRUE(alwaysTrue.testInt64(1));
  EXPECT_TRUE(alwaysTrue.testNonNull());
  EXPECT_TRUE(alwaysTrue.testNull());
  __m256i int64s = {};
  EXPECT_EQ(V64::kAllTrue, V64::compareResult(alwaysTrue.test4x64(int64s)));
  __m256si int32s = {};
  EXPECT_EQ(
      V32::kAllTrue,
      V32::compareResult(
          alwaysTrue.test8x32(reinterpret_cast<__m256i>(int32s))));
  __m256si int16s = {};
  EXPECT_EQ(
      V16::kAllTrue,
      V16::compareResult(
          alwaysTrue.test16x16(reinterpret_cast<__m256i>(int16s))));

  EXPECT_EQ(
      "Filter(AlwaysTrue, deterministic, null allowed)", alwaysTrue.toString());
}

TEST(FilterTest, isNotNull) {
  IsNotNull notNull;
  EXPECT_TRUE(notNull.testNonNull());
  EXPECT_TRUE(notNull.testInt64(10));

  EXPECT_FALSE(notNull.testNull());
}

TEST(FilterTest, isNull) {
  IsNull isNull;
  EXPECT_TRUE(isNull.testNull());

  EXPECT_FALSE(isNull.testNonNull());
  EXPECT_FALSE(isNull.testInt64(10));

  EXPECT_EQ("Filter(IsNull, deterministic, null allowed)", isNull.toString());
}

// Applies 'filter' to all T type lanes of '*vector' and compares the result to
// the ScalarTest applied to the same.
template <typename T, typename ScalarTest>
void checkSimd(
    Filter* filter,
    const typename simd::Vectors<T>::TV* vector,
    ScalarTest scalarTest) {
  using CompareType = typename simd::Vectors<T>::CompareType;
  auto data = *vector;
  typename simd::Vectors<T>::CompareType result;
  switch (sizeof(T)) {
    case 8:
      result = reinterpret_cast<CompareType>(
          filter->test4x64(reinterpret_cast<__m256i>(data)));
      break;
    case 4:
      result = reinterpret_cast<CompareType>(
          filter->test8x32(reinterpret_cast<__m256i>(data)));
      break;
    case 2:
      result = reinterpret_cast<CompareType>(
          filter->test16x16(reinterpret_cast<__m256i>(data)));
      break;
    default:
      FAIL() << "Bad type width " << sizeof(T);
  }
  auto bits =
      simd::Vectors<T>::compareBitMask(simd::Vectors<T>::compareResult(result));
  for (auto i = 0; i < simd::Vectors<T>::VSize; ++i) {
    T lane = reinterpret_cast<const T*>(vector)[i];
    EXPECT_EQ(scalarTest(lane), bits::isBitSet(&bits, i)) << "Lane " << i;
  }
}

TEST(FilterTest, bigIntRange) {
  // x = 1
  auto filter = equal(1, false);
  EXPECT_TRUE(filter->testInt64(1));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testInt64(0));
  EXPECT_FALSE(filter->testInt64(11));

  EXPECT_FALSE(filter->testInt64Range(100, 150, false));
  EXPECT_FALSE(filter->testInt64Range(100, 150, true));
  EXPECT_TRUE(filter->testInt64Range(1, 10, false));
  EXPECT_TRUE(filter->testInt64Range(-10, 10, false));

  {
    __m256i n4 = {2, 1, 1000, -1000};
    checkSimd<int64_t>(
        filter.get(), &n4, [&](int64_t x) { return filter->testInt64(x); });
    __m256si n8 = {2, 1, 1000, -1000, 1, 1, 0, 1111};
    checkSimd<int32_t>(
        filter.get(), &n8, [&](int64_t x) { return filter->testInt64(x); });
    __m256hi n16 = {
        2,
        1,
        1000,
        -1000,
        1,
        1,
        0,
        1111,
        2,
        1,
        1000,
        -1000,
        1,
        1,
        0,
        1111,
    };
    checkSimd<int16_t>(
        filter.get(), &n16, [&](int64_t x) { return filter->testInt64(x); });
  }

  // x between 1 and 10
  filter = between(1, 10, false);
  EXPECT_TRUE(filter->testInt64(1));
  EXPECT_TRUE(filter->testInt64(10));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testInt64(0));
  EXPECT_FALSE(filter->testInt64(11));

  {
    __m256i n4 = {2, 1, 1000, -1000};
    checkSimd<int64_t>(
        filter.get(), &n4, [&](int64_t x) { return filter->testInt64(x); });
    __m256si n8 = {2, 1, 1000, -1000, 1, 1, 0, 1111};
    checkSimd<int32_t>(
        filter.get(), &n8, [&](int64_t x) { return filter->testInt64(x); });
    __m256hi n16 = {
        2, 1, 1000, -1000, 1, 1, 0, 1111, 2, 1, 1000, -1000, 1, 1, 0, 1111};
    checkSimd<int16_t>(
        filter.get(), &n16, [&](int64_t x) { return filter->testInt64(x); });
  }

  EXPECT_FALSE(filter->testInt64Range(-150, -10, false));
  EXPECT_FALSE(filter->testInt64Range(-150, -10, true));
  EXPECT_TRUE(filter->testInt64Range(5, 20, false));
  EXPECT_TRUE(filter->testInt64Range(-10, 10, false));

  // x > 0 or null
  filter = greaterThan(0, true);
  EXPECT_TRUE(filter->testNull());
  EXPECT_FALSE(filter->testInt64(0));
  EXPECT_TRUE(filter->testInt64(10));
  {
    __m256i n4 = {2, 10000000000, 1000, -1000};
    checkSimd<int64_t>(
        filter.get(), &n4, [&](int64_t x) { return filter->testInt64(x); });
    __m256si n8 = {2, 1, 1000, -1000, 1, 1000000000, 0, -2000000000};
    checkSimd<int32_t>(
        filter.get(), &n8, [&](int64_t x) { return filter->testInt64(x); });
    __m256hi n16 = {
        2,
        1,
        32000,
        -1000,
        -32000,
        1,
        0,
        1111,
        2,
        1,
        1000,
        -1000,
        1,
        1,
        0,
        1111};
    checkSimd<int16_t>(
        filter.get(), &n16, [&](int64_t x) { return filter->testInt64(x); });
  }
  EXPECT_FALSE(filter->testInt64Range(-100, 0, false));
  EXPECT_TRUE(filter->testInt64Range(-100, -10, true));
  EXPECT_TRUE(filter->testInt64Range(-100, 10, false));
  EXPECT_TRUE(filter->testInt64Range(-100, 10, true));
}

TEST(FilterTest, bigintValuesUsingHashTable) {
  auto filter = createBigintValues({1, 10, 100, 10'000}, false);
  ASSERT_TRUE(dynamic_cast<BigintValuesUsingHashTable*>(filter.get()));

  EXPECT_TRUE(filter->testInt64(1));
  EXPECT_TRUE(filter->testInt64(10));
  EXPECT_TRUE(filter->testInt64(100));
  EXPECT_TRUE(filter->testInt64(10'000));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testInt64(-1));
  EXPECT_FALSE(filter->testInt64(2));
  EXPECT_FALSE(filter->testInt64(102));
  EXPECT_FALSE(filter->testInt64(INT64_MAX));

  EXPECT_TRUE(filter->testInt64Range(5, 50, false));
  EXPECT_FALSE(filter->testInt64Range(11, 11, false));
  EXPECT_FALSE(filter->testInt64Range(-10, -5, false));
  EXPECT_FALSE(filter->testInt64Range(10'234, 20'000, false));
}

TEST(FilterTest, bigintValuesUsingBitmask) {
  auto filter = createBigintValues({1, 10, 100, 1000}, false);
  ASSERT_TRUE(dynamic_cast<BigintValuesUsingBitmask*>(filter.get()));

  EXPECT_TRUE(filter->testInt64(1));
  EXPECT_TRUE(filter->testInt64(10));
  EXPECT_TRUE(filter->testInt64(100));
  EXPECT_TRUE(filter->testInt64(1000));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testInt64(-1));
  EXPECT_FALSE(filter->testInt64(2));
  EXPECT_FALSE(filter->testInt64(102));
  EXPECT_FALSE(filter->testInt64(INT64_MAX));

  EXPECT_TRUE(filter->testInt64Range(5, 50, false));
  EXPECT_FALSE(filter->testInt64Range(11, 11, false));
  EXPECT_FALSE(filter->testInt64Range(-10, -5, false));
  EXPECT_FALSE(filter->testInt64Range(1234, 2000, false));
}

TEST(FilterTest, bigintMultiRange) {
  // x between 1 and 10 or x between 100 and 120
  auto filter = bigintOr(between(1, 10), between(100, 120));
  ASSERT_TRUE(dynamic_cast<BigintMultiRange*>(filter.get()));

  EXPECT_TRUE(filter->testInt64(1));
  EXPECT_TRUE(filter->testInt64(5));
  EXPECT_TRUE(filter->testInt64(10));
  EXPECT_TRUE(filter->testInt64(100));
  EXPECT_TRUE(filter->testInt64(110));
  EXPECT_TRUE(filter->testInt64(120));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testInt64(0));
  EXPECT_FALSE(filter->testInt64(50));
  EXPECT_FALSE(filter->testInt64(150));

  EXPECT_TRUE(filter->testInt64Range(5, 15, false));
  EXPECT_TRUE(filter->testInt64Range(5, 15, true));
  EXPECT_TRUE(filter->testInt64Range(105, 115, false));
  EXPECT_TRUE(filter->testInt64Range(105, 115, true));
  EXPECT_FALSE(filter->testInt64Range(15, 45, false));
  EXPECT_FALSE(filter->testInt64Range(15, 45, true));
}

TEST(FilterTest, boolValue) {
  auto boolValueTrue = boolEqual(true);
  EXPECT_TRUE(boolValueTrue->testBool(true));

  EXPECT_FALSE(boolValueTrue->testNull());
  EXPECT_FALSE(boolValueTrue->testBool(false));

  EXPECT_TRUE(boolValueTrue->testInt64Range(0, 1, false));
  EXPECT_TRUE(boolValueTrue->testInt64Range(1, 1, false));
  EXPECT_FALSE(boolValueTrue->testInt64Range(0, 0, false));

  auto boolValueFalse = boolEqual(false);
  EXPECT_TRUE(boolValueFalse->testBool(false));

  EXPECT_FALSE(boolValueFalse->testNull());
  EXPECT_FALSE(boolValueFalse->testBool(true));

  EXPECT_TRUE(boolValueFalse->testInt64Range(0, 1, false));
  EXPECT_FALSE(boolValueFalse->testInt64Range(1, 1, false));
  EXPECT_TRUE(boolValueFalse->testInt64Range(0, 0, false));
}

TEST(FilterTest, doubleRange) {
  auto filter = betweenDouble(1.2, 1.2);
  EXPECT_TRUE(filter->testDouble(1.2));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testDouble(1.3));
  {
    __m256d n4 = {1.0, std::nan("nan"), 1.3, 1e200};
    checkSimd<double>(
        filter.get(), &n4, [&](double x) { return filter->testDouble(x); });
  }

  filter = lessThanOrEqualDouble(1.2);
  EXPECT_TRUE(filter->testDouble(1.2));
  EXPECT_TRUE(filter->testDouble(1.1));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testDouble(1.3));

  {
    __m256d n4 = {-1e100, std::nan("nan"), 1.3, 1e200};
    checkSimd<double>(
        filter.get(), &n4, [&](double x) { return filter->testDouble(x); });
  }

  filter = greaterThanDouble(1.2);
  EXPECT_TRUE(filter->testDouble(1.3));
  EXPECT_TRUE(filter->testDouble(5.6));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testDouble(1.2));
  EXPECT_FALSE(filter->testDouble(-19.267));
  {
    __m256d n4 = {-1e100, std::nan("nan"), 1.3, 1e200};
    checkSimd<double>(
        filter.get(), &n4, [&](double x) { return filter->testDouble(x); });
  }

  filter = betweenDouble(1.2, 3.4);
  EXPECT_TRUE(filter->testDouble(1.2));
  EXPECT_TRUE(filter->testDouble(1.5));
  EXPECT_TRUE(filter->testDouble(3.4));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testDouble(-0.3));
  EXPECT_FALSE(filter->testDouble(55.6));
  EXPECT_FALSE(filter->testDouble(NAN));

  {
    __m256d n4 = {3.4, 1.3, 1.1, 1e200};
    checkSimd<double>(
        filter.get(), &n4, [&](double x) { return filter->testDouble(x); });
  }

  EXPECT_THROW(betweenDouble(NAN, NAN), VeloxRuntimeError)
      << "able to create a DoubleRange with NaN";
}

TEST(FilterTest, floatRange) {
  auto filter = betweenFloat(1.2, 1.2);
  EXPECT_TRUE(filter->testFloat(1.2f));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testFloat(1.1f));
  {
    __m256 n8 = {1.0, std::nanf("nan"), 1.3, 1e20, -1e20, 0, 0, 0};
    checkSimd<float>(
        filter.get(), &n8, [&](float x) { return filter->testFloat(x); });
  }

  filter = lessThanFloat(1.2);
  EXPECT_TRUE(filter->testFloat(1.1f));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testFloat(1.2f));
  EXPECT_FALSE(filter->testFloat(15.632f));
  {
    __m256 n8 = {1.0, std::nanf("nan"), 1.3, 1e20, -1e20, 0, 1.1, 1.2};
    checkSimd<float>(
        filter.get(), &n8, [&](float x) { return filter->testFloat(x); });
  }

  filter = betweenFloat(1.2, 3.4);
  EXPECT_TRUE(filter->testFloat(1.2f));
  EXPECT_TRUE(filter->testFloat(2.3f));
  EXPECT_TRUE(filter->testFloat(3.4f));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testFloat(1.1f));
  EXPECT_FALSE(filter->testFloat(15.632f));
  EXPECT_FALSE(filter->testFloat(std::nanf("NAN")));
  {
    __m256 n8 = {1.0, std::nanf("nan"), 3.4, 3.1, -1e20, 0, 1.1, 1.2};
    checkSimd<float>(
        filter.get(), &n8, [&](float x) { return filter->testFloat(x); });
  }

  EXPECT_THROW(
      betweenFloat(std::nanf("NAN"), std::nanf("NAN")), VeloxRuntimeError)
      << "able to create a FloatRange with NaN";
}

TEST(FilterTest, bytesRange) {
  {
    auto filter = equal("abc");
    EXPECT_TRUE(filter->testBytes("abc", 3));
    EXPECT_FALSE(filter->testBytes("acb", 3));
    EXPECT_TRUE(filter->testLength(3));
    __m256si length8 = {0, 1, 3, 0, 4, 10, 11, 12};
    // The bit for lane 2 should be set.
    EXPECT_EQ(
        4,
        V32::compareBitMask(V32::compareResult(filter->test8xLength(length8))));

    EXPECT_FALSE(filter->testNull());
    EXPECT_FALSE(filter->testBytes("apple", 5));
    EXPECT_FALSE(filter->testLength(4));

    EXPECT_TRUE(filter->testBytesRange("abc", "abc", false));
    EXPECT_TRUE(filter->testBytesRange("a", "z", false));
    EXPECT_TRUE(filter->testBytesRange("aaaaa", "bbbb", false));
    EXPECT_FALSE(filter->testBytesRange("apple", "banana", false));
    EXPECT_FALSE(filter->testBytesRange("orange", "plum", false));

    EXPECT_FALSE(filter->testBytesRange(std::nullopt, "a", false));
    EXPECT_TRUE(filter->testBytesRange(std::nullopt, "abc", false));
    EXPECT_TRUE(filter->testBytesRange(std::nullopt, "banana", false));

    EXPECT_FALSE(filter->testBytesRange("banana", std::nullopt, false));
    EXPECT_TRUE(filter->testBytesRange("abc", std::nullopt, false));
    EXPECT_TRUE(filter->testBytesRange("a", std::nullopt, false));
  }

  char const* theBestOfTimes =
      "It was the best of times, it was the worst of times, it was the age of wisdom, it was the age of foolishness, it was the epoch of belief, it was the epoch of incredulity,...";
  auto filter = lessThanOrEqual(theBestOfTimes);
  EXPECT_TRUE(filter->testBytes(theBestOfTimes, std::strlen(theBestOfTimes)));
  EXPECT_TRUE(filter->testBytes(theBestOfTimes, 5));
  EXPECT_TRUE(filter->testBytes(theBestOfTimes, 50));
  EXPECT_TRUE(filter->testBytes(theBestOfTimes, 100));
  // testLength is true of all lengths for a range filter.
  EXPECT_TRUE(filter->testLength(1));
  EXPECT_TRUE(filter->testLength(1000));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testBytes("Zzz", 3));
  EXPECT_FALSE(filter->testBytes("It was the best of times, zzz", 30));

  EXPECT_TRUE(filter->testBytesRange("Apple", "banana", false));
  EXPECT_FALSE(filter->testBytesRange("Pear", "Plum", false));
  EXPECT_FALSE(filter->testBytesRange("apple", "banana", false));

  filter = greaterThanOrEqual("abc");
  EXPECT_TRUE(filter->testBytes("abc", 3));
  EXPECT_TRUE(filter->testBytes("ad", 2));
  EXPECT_TRUE(filter->testBytes("apple", 5));
  EXPECT_TRUE(filter->testBytes("banana", 6));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testBytes("ab", 2));
  EXPECT_FALSE(filter->testBytes("_abc", 4));

  filter = between("apple", "banana");
  EXPECT_TRUE(filter->testBytes("apple", 5));
  EXPECT_TRUE(filter->testBytes("banana", 6));
  EXPECT_TRUE(filter->testBytes("avocado", 7));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testBytes("camel", 5));
  EXPECT_FALSE(filter->testBytes("_abc", 4));

  filter = std::make_unique<BytesRange>(
      "apple", false, true, "banana", false, false, false);
  EXPECT_TRUE(filter->testBytes("banana", 6));
  EXPECT_TRUE(filter->testBytes("avocado", 7));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testBytes("apple", 5));
  EXPECT_FALSE(filter->testBytes("camel", 5));
  EXPECT_FALSE(filter->testBytes("_abc", 4));

  filter = std::make_unique<BytesRange>(
      "apple", false, true, "banana", false, true, false);
  EXPECT_TRUE(filter->testBytes("avocado", 7));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testBytes("apple", 5));
  EXPECT_FALSE(filter->testBytes("banana", 6));
  EXPECT_FALSE(filter->testBytes("camel", 5));
  EXPECT_FALSE(filter->testBytes("_abc", 4));
}

TEST(FilterTest, bytesValues) {
  // The filter has values of size on either side of 8 bytes.
  std::vector<std::string> values({"Igne", "natura", "renovitur", "integra."});
  auto filter = in(values);
  EXPECT_TRUE(filter->testBytes("Igne", 4));
  EXPECT_TRUE(filter->testBytes("natura", 6));
  EXPECT_TRUE(filter->testBytes("natural", 6));
  EXPECT_TRUE(filter->testBytes("renovitur", 9));
  EXPECT_TRUE(filter->testBytes("integra.", 8));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testBytes("natura", 5));
  EXPECT_FALSE(filter->testBytes("apple", 5));

  EXPECT_TRUE(filter->testLength(4));
  EXPECT_TRUE(filter->testLength(6));
  EXPECT_TRUE(filter->testLength(8));
  EXPECT_TRUE(filter->testLength(9));

  EXPECT_FALSE(filter->testLength(1));
  EXPECT_FALSE(filter->testLength(5));
  EXPECT_FALSE(filter->testLength(125));

  EXPECT_TRUE(filter->testBytesRange("natura", "naturel", false));
  EXPECT_TRUE(filter->testBytesRange("igloo", "ocean", false));
  EXPECT_FALSE(filter->testBytesRange("igloo", "igloo", false));
  EXPECT_FALSE(filter->testBytesRange("Apple", "Banana", false));
  EXPECT_FALSE(filter->testBytesRange("sun", "water", false));

  EXPECT_TRUE(filter->testBytesRange("natura", std::nullopt, false));
  EXPECT_TRUE(filter->testBytesRange("igloo", std::nullopt, false));
  EXPECT_FALSE(filter->testBytesRange("sun", std::nullopt, false));

  EXPECT_TRUE(filter->testBytesRange(std::nullopt, "Igne", false));
  EXPECT_TRUE(filter->testBytesRange(std::nullopt, "ocean", false));
  EXPECT_FALSE(filter->testBytesRange(std::nullopt, "Banana", false));
}

TEST(FilterTest, multiRange) {
  auto filter = orFilter(between("abc", "abc"), greaterThanOrEqual("dragon"));

  EXPECT_TRUE(filter->testBytes("abc", 3));
  EXPECT_TRUE(filter->testBytes("dragon", 6));
  EXPECT_TRUE(filter->testBytes("dragonfly", 9));
  EXPECT_TRUE(filter->testBytes("drought", 7));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testBytes("apple", 5));

  EXPECT_TRUE(filter->testBytesRange("abc", "abc", false));
  EXPECT_TRUE(filter->testBytesRange("apple", "pear", false));
  EXPECT_FALSE(filter->testBytesRange("apple", "banana", false));
  EXPECT_FALSE(filter->testBytesRange("aaa", "aa123", false));

  filter = orFilter(lessThanDouble(1.2), greaterThanDouble(1.2));

  EXPECT_TRUE(filter->testDouble(1.1));
  EXPECT_TRUE(filter->testDouble(1.3));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testDouble(std::nan("nan")));
  EXPECT_FALSE(filter->testDouble(1.2));

  filter = orFilter(lessThanFloat(1.2), greaterThanFloat(1.2));

  EXPECT_FALSE(filter->testNull());
  EXPECT_TRUE(filter->testFloat(1.1f));
  EXPECT_FALSE(filter->testFloat(1.2f));
  EXPECT_TRUE(filter->testFloat(1.3f));
  EXPECT_FALSE(filter->testFloat(std::nanf("nan")));
}

TEST(FilterTest, multiRangeWithNaNs) {
  // x <> 1.2 with nanAllowed true
  auto filter =
      orFilter(lessThanFloat(1.2), greaterThanFloat(1.2), false, true);
  EXPECT_TRUE(filter->testFloat(std::nanf("nan")));
  EXPECT_FALSE(filter->testFloat(1.2f));
  EXPECT_TRUE(filter->testFloat(1.1f));

  filter = orFilter(lessThanDouble(1.2), greaterThanDouble(1.2), false, true);
  EXPECT_TRUE(filter->testDouble(std::nan("nan")));
  EXPECT_FALSE(filter->testDouble(1.2));
  EXPECT_TRUE(filter->testDouble(1.1));

  // x <> 1.2 with nanAllowed false
  filter = orFilter(lessThanFloat(1.2), greaterThanFloat(1.2));
  EXPECT_FALSE(filter->testFloat(std::nanf("nan")));
  EXPECT_TRUE(filter->testFloat(1.0f));

  filter = orFilter(lessThanDouble(1.2), greaterThanDouble(1.2));
  EXPECT_FALSE(filter->testDouble(std::nan("nan")));
  EXPECT_TRUE(filter->testDouble(1.4));

  // x NOT IN (1.2, 1.3) with nanAllowed true
  filter = orFilter(lessThanFloat(1.2), greaterThanFloat(1.3), false, true);
  EXPECT_TRUE(filter->testFloat(std::nanf("nan")));
  EXPECT_FALSE(filter->testFloat(1.2f));
  EXPECT_FALSE(filter->testFloat(1.3f));
  EXPECT_TRUE(filter->testFloat(1.4f));
  EXPECT_TRUE(filter->testFloat(1.1f));

  filter = orFilter(lessThanDouble(1.2), greaterThanDouble(1.3), false, true);
  EXPECT_TRUE(filter->testDouble(std::nan("nan")));
  EXPECT_FALSE(filter->testDouble(1.2));
  EXPECT_FALSE(filter->testDouble(1.3));
  EXPECT_TRUE(filter->testDouble(1.4));
  EXPECT_TRUE(filter->testDouble(1.1));

  // x NOT IN (1.2) with nanAllowed false
  filter = orFilter(lessThanFloat(1.2), greaterThanFloat(1.2));
  EXPECT_FALSE(filter->testFloat(std::nanf("nan")));
  EXPECT_FALSE(filter->testFloat(1.2f));
  EXPECT_TRUE(filter->testFloat(1.3f));

  filter = orFilter(lessThanDouble(1.2), greaterThanDouble(1.2));
  EXPECT_FALSE(filter->testDouble(std::nan("nan")));
  EXPECT_FALSE(filter->testDouble(1.2));
  EXPECT_TRUE(filter->testDouble(1.3));
}

TEST(FilterTest, createBigintValues) {
  // Small number of values from a very large range.
  {
    std::vector<int64_t> values = {
        std::numeric_limits<int64_t>::max() - 1'000,
        std::numeric_limits<int64_t>::min() + 1'000,
        0,
        123};
    auto filter = createBigintValues(values, true);
    ASSERT_TRUE(dynamic_cast<BigintValuesUsingHashTable*>(filter.get()))
        << filter->toString();
    for (auto v : values) {
      ASSERT_TRUE(filter->testInt64(v));
    }
    ASSERT_FALSE(filter->testInt64(-5));
    ASSERT_FALSE(filter->testInt64(12345));
    ASSERT_TRUE(filter->testNull());
  }

  // Small number of values from a small range.
  {
    std::vector<int64_t> values = {0, 123, -7, 56};
    auto filter = createBigintValues(values, true);
    ASSERT_TRUE(dynamic_cast<BigintValuesUsingBitmask*>(filter.get()))
        << filter->toString();
    for (auto v : values) {
      ASSERT_TRUE(filter->testInt64(v));
    }
    ASSERT_FALSE(filter->testInt64(-5));
    ASSERT_FALSE(filter->testInt64(12345));
    ASSERT_TRUE(filter->testNull());
  }

  // Dense sequence of values without gaps.
  {
    std::vector<int64_t> values(100);
    std::iota(values.begin(), values.end(), 5);
    auto filter = createBigintValues(values, false);
    ASSERT_TRUE(dynamic_cast<BigintRange*>(filter.get())) << filter->toString();
    for (int i = 5; i < 105; i++) {
      ASSERT_TRUE(filter->testInt64(i));
    }
    ASSERT_FALSE(filter->testInt64(4));
    ASSERT_FALSE(filter->testInt64(106));
    ASSERT_FALSE(filter->testNull());
  }

  // Single value.
  {
    std::vector<int64_t> values = {37};
    auto filter = createBigintValues(values, false);
    ASSERT_TRUE(dynamic_cast<BigintRange*>(filter.get())) << filter->toString();
    for (int i = -100; i <= 100; i++) {
      if (i != 37) {
        ASSERT_FALSE(filter->testInt64(i));
      }
    }
    ASSERT_TRUE(filter->testInt64(37));
    ASSERT_FALSE(filter->testNull());
  }
}

namespace {

void addUntypedFilters(std::vector<std::unique_ptr<Filter>>& filters) {
  filters.push_back(std::make_unique<AlwaysFalse>());
  filters.push_back(std::make_unique<AlwaysTrue>());
  filters.push_back(std::make_unique<IsNull>());
  filters.push_back(std::make_unique<IsNotNull>());
}

void testMergeWithUntyped(Filter* left, Filter* right) {
  auto merged = left->mergeWith(right);

  // Null.
  ASSERT_EQ(merged->testNull(), left->testNull() && right->testNull());

  // Not null.
  ASSERT_EQ(merged->testNonNull(), left->testNonNull() && right->testNonNull());

  // Integer value.
  ASSERT_EQ(
      merged->testInt64(123), left->testInt64(123) && right->testInt64(123));
}

void testMergeWithBool(Filter* left, Filter* right) {
  auto merged = left->mergeWith(right);
  ASSERT_EQ(merged->testNull(), left->testNull() && right->testNull());
  ASSERT_EQ(
      merged->testBool(true), left->testBool(true) && right->testBool(true));
  ASSERT_EQ(
      merged->testBool(false), left->testBool(false) && right->testBool(false));
}

void testMergeWithBigint(Filter* left, Filter* right) {
  auto merged = left->mergeWith(right);

  ASSERT_EQ(merged->testNull(), left->testNull() && right->testNull())
      << "left: " << left->toString() << ", right: " << right->toString()
      << ", merged: " << merged->toString();

  for (int64_t i = -1'000; i <= 1'000; i++) {
    ASSERT_EQ(merged->testInt64(i), left->testInt64(i) && right->testInt64(i))
        << "at " << i << ", left: " << left->toString()
        << ", right: " << right->toString()
        << ", merged: " << merged->toString();
  }
}

void testMergeWithDouble(Filter* left, Filter* right) {
  auto merged = left->mergeWith(right);
  ASSERT_EQ(merged->testNull(), left->testNull() && right->testNull());
  for (int64_t i = -10; i <= 10; i++) {
    double d = i * 0.1;
    ASSERT_EQ(
        merged->testDouble(d), left->testDouble(d) && right->testDouble(d));
  }
}

void testMergeWithFloat(Filter* left, Filter* right) {
  auto merged = left->mergeWith(right);
  ASSERT_EQ(merged->testNull(), left->testNull() && right->testNull());
  for (int64_t i = -10; i <= 10; i++) {
    float f = i * 0.1;
    ASSERT_EQ(merged->testFloat(f), left->testFloat(f) && right->testFloat(f));
  }
}
} // namespace

TEST(FilterTest, mergeWithUntyped) {
  std::vector<std::unique_ptr<Filter>> filters;
  addUntypedFilters(filters);

  for (const auto& left : filters) {
    for (const auto& right : filters) {
      testMergeWithUntyped(left.get(), right.get());
    }
  }
}

TEST(FilterTest, mergeWithBool) {
  std::vector<std::unique_ptr<Filter>> filters;
  addUntypedFilters(filters);
  filters.push_back(boolEqual(true, false));
  filters.push_back(boolEqual(true, true));
  filters.push_back(boolEqual(false, false));
  filters.push_back(boolEqual(false, true));

  for (const auto& left : filters) {
    for (const auto& right : filters) {
      testMergeWithBool(left.get(), right.get());
    }
  }
}

TEST(FilterTest, mergeWithBigint) {
  std::vector<std::unique_ptr<Filter>> filters;
  addUntypedFilters(filters);
  // Equality.
  filters.push_back(equal(123));
  filters.push_back(equal(123, true));

  // Between.
  filters.push_back(between(-7, 13));
  filters.push_back(between(-7, 13, true));
  filters.push_back(between(150, 500));
  filters.push_back(between(150, 500, true));

  // IN-list.
  filters.push_back(in({1, 2, 3, 67, 134}));
  filters.push_back(in({1, 2, 3, 67, 134}, true));
  filters.push_back(in({-7, -6, -5, -4, -3, -2}));
  filters.push_back(in({-7, -6, -5, -4, -3, -2}, true));
  filters.push_back(in({1, 2, 3, 67, 10'134}));
  filters.push_back(in({1, 2, 3, 67, 10'134}, true));

  for (const auto& left : filters) {
    for (const auto& right : filters) {
      testMergeWithBigint(left.get(), right.get());
    }
  }
}

TEST(FilterTest, mergeWithDouble) {
  std::vector<std::unique_ptr<Filter>> filters;
  addUntypedFilters(filters);

  // Less than.
  filters.push_back(lessThanDouble(1.2));
  filters.push_back(lessThanDouble(1.2, true));
  filters.push_back(lessThanOrEqualDouble(1.2));
  filters.push_back(lessThanOrEqualDouble(1.2, true));

  // Greater than.
  filters.push_back(greaterThanDouble(1.2));
  filters.push_back(greaterThanDouble(1.2, true));
  filters.push_back(greaterThanOrEqualDouble(1.2));
  filters.push_back(greaterThanOrEqualDouble(1.2, true));

  // Between.
  filters.push_back(betweenDouble(-1.2, 3.4));
  filters.push_back(betweenDouble(-1.2, 3.4, true));

  for (const auto& left : filters) {
    for (const auto& right : filters) {
      testMergeWithDouble(left.get(), right.get());
    }
  }
}

TEST(FilterTest, mergeWithFloat) {
  std::vector<std::unique_ptr<Filter>> filters;
  addUntypedFilters(filters);

  // Less than.
  filters.push_back(lessThanFloat(1.2));
  filters.push_back(lessThanFloat(1.2, true));
  filters.push_back(lessThanOrEqualFloat(1.2));
  filters.push_back(lessThanOrEqualFloat(1.2, true));

  // Greater than.
  filters.push_back(greaterThanFloat(1.2));
  filters.push_back(greaterThanFloat(1.2, true));
  filters.push_back(greaterThanOrEqualFloat(1.2));
  filters.push_back(greaterThanOrEqualFloat(1.2, true));

  // Between.
  filters.push_back(betweenFloat(-1.2, 3.4));
  filters.push_back(betweenFloat(-1.2, 3.4, true));

  for (const auto& left : filters) {
    for (const auto& right : filters) {
      testMergeWithFloat(left.get(), right.get());
    }
  }
}

TEST(FilterTest, mergeWithBigintMultiRange) {
  std::vector<std::unique_ptr<Filter>> filters;
  addUntypedFilters(filters);

  filters.push_back(bigintOr(equal(12), between(25, 47)));
  filters.push_back(bigintOr(equal(12), between(25, 47), true));

  filters.push_back(bigintOr(lessThan(12), greaterThan(47)));
  filters.push_back(bigintOr(lessThan(12), greaterThan(47), true));

  filters.push_back(bigintOr(lessThanOrEqual(12), greaterThan(47)));
  filters.push_back(bigintOr(lessThanOrEqual(12), greaterThan(47), true));

  filters.push_back(bigintOr(lessThanOrEqual(12), greaterThanOrEqual(47)));
  filters.push_back(
      bigintOr(lessThanOrEqual(12), greaterThanOrEqual(47), true));

  filters.push_back(bigintOr(lessThan(-3), equal(12), between(25, 47)));
  filters.push_back(bigintOr(lessThan(-3), equal(12), between(25, 47), true));

  // IN-list using bitmask.
  filters.push_back(in({1, 2, 3, 56}));
  filters.push_back(in({1, 2, 3, 56}, true));

  // IN-list using hash table.
  filters.push_back(in({1, 2, 3, 67, 10'134}));
  filters.push_back(in({1, 2, 3, 67, 10'134}, true));

  for (const auto& left : filters) {
    for (const auto& right : filters) {
      testMergeWithBigint(left.get(), right.get());
    }
  }
}
