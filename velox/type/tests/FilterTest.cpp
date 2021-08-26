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

#include <cstdint>
#include <limits>
#include <memory>

#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook ::velox::common;

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
  auto filter = std::make_unique<BigintRange>(1, 1, false);
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
  filter = std::make_unique<BigintRange>(1, 10, false);
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
  filter = std::make_unique<BigintRange>(
      1, std::numeric_limits<int64_t>::max(), true);
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
  std::vector<int64_t> values = {1, 10, 100, 1000};
  auto filter = std::make_unique<BigintValuesUsingHashTable>(
      1, 1000, std::move(values), false);
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

TEST(FilterTest, bigintValuesUsingBitmask) {
  std::vector<int64_t> values = {1, 10, 100, 1000};
  auto filter =
      std::make_unique<BigintValuesUsingBitmask>(1, 1000, values, false);

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
  std::vector<std::unique_ptr<BigintRange>> filters;
  filters.emplace_back(std::make_unique<BigintRange>(1, 10, false));
  filters.emplace_back(std::make_unique<BigintRange>(100, 120, false));
  auto filter = BigintMultiRange(std::move(filters), false);

  EXPECT_TRUE(filter.testInt64(1));
  EXPECT_TRUE(filter.testInt64(5));
  EXPECT_TRUE(filter.testInt64(10));
  EXPECT_TRUE(filter.testInt64(100));
  EXPECT_TRUE(filter.testInt64(110));
  EXPECT_TRUE(filter.testInt64(120));

  EXPECT_FALSE(filter.testNull());
  EXPECT_FALSE(filter.testInt64(0));
  EXPECT_FALSE(filter.testInt64(50));
  EXPECT_FALSE(filter.testInt64(150));

  EXPECT_TRUE(filter.testInt64Range(5, 15, false));
  EXPECT_TRUE(filter.testInt64Range(5, 15, true));
  EXPECT_TRUE(filter.testInt64Range(105, 115, false));
  EXPECT_TRUE(filter.testInt64Range(105, 115, true));
  EXPECT_FALSE(filter.testInt64Range(15, 45, false));
  EXPECT_FALSE(filter.testInt64Range(15, 45, true));
}

TEST(FilterTest, boolValue) {
  auto boolValueTrue = BoolValue(true, false);
  EXPECT_TRUE(boolValueTrue.testBool(true));

  EXPECT_FALSE(boolValueTrue.testNull());
  EXPECT_FALSE(boolValueTrue.testBool(false));

  EXPECT_TRUE(boolValueTrue.testInt64Range(0, 1, false));
  EXPECT_TRUE(boolValueTrue.testInt64Range(1, 1, false));
  EXPECT_FALSE(boolValueTrue.testInt64Range(0, 0, false));

  auto boolValueFalse = BoolValue(false, false);
  EXPECT_TRUE(boolValueFalse.testBool(false));

  EXPECT_FALSE(boolValueFalse.testNull());
  EXPECT_FALSE(boolValueFalse.testBool(true));

  EXPECT_TRUE(boolValueFalse.testInt64Range(0, 1, false));
  EXPECT_FALSE(boolValueFalse.testInt64Range(1, 1, false));
  EXPECT_TRUE(boolValueFalse.testInt64Range(0, 0, false));
}

TEST(FilterTest, doubleRange) {
  auto filter = std::make_unique<DoubleRange>(
      1.2, false, false, 1.2, false, false, false);
  EXPECT_TRUE(filter->testDouble(1.2));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testDouble(1.3));
  {
    __m256d n4 = {1.0, std::nan("nan"), 1.3, 1e200};
    checkSimd<double>(
        filter.get(), &n4, [&](double x) { return filter->testDouble(x); });
  }

  filter = std::make_unique<DoubleRange>(
      std::numeric_limits<double>::lowest(),
      true,
      true,
      1.2,
      false,
      false,
      false);
  EXPECT_TRUE(filter->testDouble(1.2));
  EXPECT_TRUE(filter->testDouble(1.1));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testDouble(1.3));

  {
    __m256d n4 = {-1e100, std::nan("nan"), 1.3, 1e200};
    checkSimd<double>(
        filter.get(), &n4, [&](double x) { return filter->testDouble(x); });
  }

  filter = std::make_unique<DoubleRange>(
      1.2, false, true, std::numeric_limits<double>::max(), true, true, false);
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

  filter = std::make_unique<DoubleRange>(
      1.2, false, false, 3.4, false, false, false);
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

  try {
    filter = std::make_unique<DoubleRange>(
        NAN, false, false, NAN, false, false, false);
    EXPECT_TRUE(false) << "able to create a DoubleRange with NaN";
  } catch (const std::exception& e) {
    // expected
  }
}

TEST(FilterTest, floatRange) {
  auto filter = std::make_unique<FloatRange>(
      1.2f, false, false, 1.2f, false, false, false);
  EXPECT_TRUE(filter->testFloat(1.2f));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testFloat(1.1f));
  {
    __m256 n8 = {1.0, std::nanf("nan"), 1.3, 1e20, -1e20, 0, 0, 0};
    checkSimd<float>(
        filter.get(), &n8, [&](float x) { return filter->testFloat(x); });
  }

  filter = std::make_unique<FloatRange>(
      std::numeric_limits<float>::lowest(),
      true,
      true,
      1.2f,
      false,
      true,
      false);
  EXPECT_TRUE(filter->testFloat(1.1f));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testFloat(1.2f));
  EXPECT_FALSE(filter->testFloat(15.632f));
  {
    __m256 n8 = {1.0, std::nanf("nan"), 1.3, 1e20, -1e20, 0, 1.1, 1.2};
    checkSimd<float>(
        filter.get(), &n8, [&](float x) { return filter->testFloat(x); });
  }

  filter = std::make_unique<FloatRange>(
      1.2f, false, false, 3.4f, false, false, false);
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

  try {
    std::make_unique<FloatRange>(
        std::nanf("NAN"), false, false, std::nanf("NAN"), false, false, false);
    EXPECT_TRUE(false) << "able to create a FloatRange with NaN";
  } catch (const std::exception& e) {
    // expected
  }
}

TEST(FilterTest, bytesRange) {
  auto filter = std::make_shared<BytesRange>(
      "abc", false, false, "abc", false, false, false);
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

  char const* theBestOfTimes =
      "It was the best of times, it was the worst of times, it was the age of wisdom, it was the age of foolishness, it was the epoch of belief, it was the epoch of incredulity,...";
  filter = std::make_shared<BytesRange>(
      "", true, true, theBestOfTimes, false, false, false);
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

  filter =
      std::make_shared<BytesRange>("abc", false, false, "", true, true, false);
  EXPECT_TRUE(filter->testBytes("abc", 3));
  EXPECT_TRUE(filter->testBytes("ad", 2));
  EXPECT_TRUE(filter->testBytes("apple", 5));
  EXPECT_TRUE(filter->testBytes("banana", 6));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testBytes("ab", 2));
  EXPECT_FALSE(filter->testBytes("_abc", 4));

  filter = std::make_shared<BytesRange>(
      "apple", false, false, "banana", false, false, false);
  EXPECT_TRUE(filter->testBytes("apple", 5));
  EXPECT_TRUE(filter->testBytes("banana", 6));
  EXPECT_TRUE(filter->testBytes("avocado", 7));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testBytes("camel", 5));
  EXPECT_FALSE(filter->testBytes("_abc", 4));

  filter = std::make_shared<BytesRange>(
      "apple", false, true, "banana", false, false, false);
  EXPECT_TRUE(filter->testBytes("banana", 6));
  EXPECT_TRUE(filter->testBytes("avocado", 7));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testBytes("apple", 5));
  EXPECT_FALSE(filter->testBytes("camel", 5));
  EXPECT_FALSE(filter->testBytes("_abc", 4));

  filter = std::make_shared<BytesRange>(
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
  std::vector<std::string> vec({"Igne", "natura", "renovitur", "integra."});
  auto filter = std::make_unique<BytesValues>(std::move(vec), false);
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
  std::vector<std::unique_ptr<Filter>> filters;
  filters.push_back(std::make_unique<BytesRange>(
      "abc", false, false, "abc", false, false, false));
  filters.push_back(std::make_unique<BytesRange>(
      "dragon", false, false, "", true, true, false));
  auto filter = std::make_unique<MultiRange>(std::move(filters), false, false);

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

  filters.clear();
  filters.push_back(std::make_unique<DoubleRange>(
      std::numeric_limits<double>::lowest(),
      true,
      true,
      1.2,
      false,
      true,
      false));
  filters.push_back(std::make_unique<DoubleRange>(
      1.2, false, true, std::numeric_limits<double>::max(), true, true, false));
  filter = std::make_unique<MultiRange>(std::move(filters), false, false);

  EXPECT_TRUE(filter->testDouble(1.1));
  EXPECT_TRUE(filter->testDouble(1.3));

  EXPECT_FALSE(filter->testNull());
  EXPECT_FALSE(filter->testDouble(std::nan("nan")));
  EXPECT_FALSE(filter->testDouble(1.2));

  filters.clear();
  filters.push_back(std::make_unique<FloatRange>(
      std::numeric_limits<float>::lowest(),
      true,
      true,
      1.2f,
      false,
      true,
      false));
  filters.push_back(std::make_unique<FloatRange>(
      1.2f,
      false,
      true,
      std::numeric_limits<float>::lowest(),
      true,
      true,
      false));
  filter = std::make_unique<MultiRange>(std::move(filters), false, false);

  EXPECT_FALSE(filter->testNull());
  EXPECT_TRUE(filter->testFloat(1.1f));
  EXPECT_FALSE(filter->testFloat(1.2f));
  EXPECT_TRUE(filter->testFloat(1.3f));
  EXPECT_FALSE(filter->testFloat(std::nanf("nan")));
}

TEST(FilterTest, multiRangeWithNaNs) {
  // x <> 1.2 with nanAllowed true
  std::vector<std::unique_ptr<Filter>> filters;
  filters.push_back(std::make_unique<FloatRange>(
      std::numeric_limits<float>::lowest(),
      true,
      true,
      1.2f,
      false,
      true,
      false));
  filters.push_back(std::make_unique<FloatRange>(
      1.2f, false, true, std::numeric_limits<float>::max(), true, true, false));
  auto filter = std::make_unique<MultiRange>(std::move(filters), false, true);
  EXPECT_TRUE(filter->testFloat(std::nanf("nan")));
  EXPECT_FALSE(filter->testFloat(1.2f));
  EXPECT_TRUE(filter->testFloat(1.1f));

  filters.clear();
  filters.push_back(std::make_unique<DoubleRange>(
      std::numeric_limits<double>::lowest(),
      true,
      true,
      1.2,
      false,
      true,
      false));
  filters.push_back(std::make_unique<DoubleRange>(
      1.2, false, true, std::numeric_limits<double>::max(), true, true, false));
  filter = std::make_unique<MultiRange>(std::move(filters), false, true);
  EXPECT_TRUE(filter->testDouble(std::nan("nan")));
  EXPECT_FALSE(filter->testDouble(1.2));
  EXPECT_TRUE(filter->testDouble(1.1));

  // x <> 1.2 with nanAllowed false
  filters.clear();
  filters.push_back(std::make_unique<FloatRange>(
      std::numeric_limits<float>::lowest(),
      true,
      true,
      1.2f,
      false,
      true,
      false));
  filters.push_back(std::make_unique<FloatRange>(
      1.2f, false, true, std::numeric_limits<float>::max(), true, true, false));

  filter = std::make_unique<MultiRange>(std::move(filters), false, false);
  EXPECT_FALSE(filter->testFloat(std::nanf("nan")));
  EXPECT_TRUE(filter->testFloat(1.0f));

  filters.clear();
  filters.push_back(std::make_unique<DoubleRange>(
      std::numeric_limits<double>::lowest(),
      true,
      true,
      1.2,
      false,
      true,
      false));
  filters.push_back(std::make_unique<DoubleRange>(
      1.2, false, true, std::numeric_limits<double>::max(), true, true, false));

  filter = std::make_unique<MultiRange>(std::move(filters), false, false);
  EXPECT_FALSE(filter->testDouble(std::nan("nan")));
  EXPECT_TRUE(filter->testDouble(1.4));

  // x NOT IN (1.2, 1.3) with nanAllowed true
  auto floatRangePtr3 =
      std::make_unique<FloatRange>(1.2f, false, true, 1.3f, false, true, false);
  auto floatRangePtr4 = std::make_unique<FloatRange>(
      1.3f, false, true, std::numeric_limits<float>::max(), true, true, false);
  filters.clear();
  filters.push_back(std::make_unique<FloatRange>(
      std::numeric_limits<float>::lowest(),
      true,
      true,
      1.2f,
      false,
      true,
      false));
  filters.push_back(std::make_unique<FloatRange>(
      1.2f, false, true, 1.3f, false, true, false));
  filters.push_back(std::make_unique<FloatRange>(
      1.3f, false, true, std::numeric_limits<float>::max(), true, true, false));
  filter = std::make_unique<MultiRange>(std::move(filters), false, true);
  EXPECT_TRUE(filter->testFloat(std::nanf("nan")));
  EXPECT_FALSE(filter->testFloat(1.2f));
  EXPECT_FALSE(filter->testFloat(1.3f));
  EXPECT_TRUE(filter->testFloat(1.4f));
  EXPECT_TRUE(filter->testFloat(1.1f));

  filters.clear();
  filters.push_back(std::make_unique<DoubleRange>(
      std::numeric_limits<double>::lowest(),
      true,
      true,
      1.2,
      false,
      true,
      false));
  filters.push_back(std::make_unique<FloatRange>(
      1.2f, false, true, 1.3f, false, true, false));
  filters.push_back(std::make_unique<FloatRange>(
      1.3f, false, true, std::numeric_limits<float>::max(), true, true, false));
  filter = std::make_unique<MultiRange>(std::move(filters), false, true);
  EXPECT_TRUE(filter->testDouble(std::nan("nan")));
  EXPECT_FALSE(filter->testDouble(1.2));
  EXPECT_FALSE(filter->testDouble(1.3));
  EXPECT_TRUE(filter->testDouble(1.4));
  EXPECT_TRUE(filter->testDouble(1.1));

  // x NOT IN (1.2) with nanAllowed false
  auto floatRangePtr5 = std::make_unique<FloatRange>(
      1.2f, false, true, std::numeric_limits<float>::max(), true, true, false);
  filters.clear();
  filters.push_back(std::make_unique<FloatRange>(
      std::numeric_limits<float>::lowest(),
      true,
      true,
      1.2f,
      false,
      true,
      false));
  filters.push_back(std::move(floatRangePtr5));
  filter = std::make_unique<MultiRange>(std::move(filters), false, false);
  EXPECT_FALSE(filter->testFloat(std::nanf("nan")));
  EXPECT_FALSE(filter->testFloat(1.2f));
  EXPECT_TRUE(filter->testFloat(1.3f));

  filters.clear();
  filters.push_back(std::make_unique<DoubleRange>(
      std::numeric_limits<double>::lowest(),
      true,
      true,
      1.2,
      false,
      true,
      false));
  filters.push_back(std::make_unique<DoubleRange>(
      1.2, false, true, std::numeric_limits<double>::max(), true, true, false));
  filter = std::make_unique<MultiRange>(std::move(filters), false, false);
  EXPECT_FALSE(filter->testDouble(std::nan("nan")));
  EXPECT_FALSE(filter->testDouble(1.2));
  EXPECT_TRUE(filter->testDouble(1.3));
}

TEST(FilterTest, createBigintValues) {
  std::vector<int64_t> values = {
      std::numeric_limits<int64_t>::max() - 1'000,
      std::numeric_limits<int64_t>::min() + 1'000,
      0,
      123};
  auto filter = createBigintValues(values, true);
  for (auto v : values) {
    ASSERT_TRUE(filter->testInt64(v));
  }
  ASSERT_FALSE(filter->testInt64(-5));
  ASSERT_FALSE(filter->testInt64(12345));
  ASSERT_TRUE(filter->testNull());
}
