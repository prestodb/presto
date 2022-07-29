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
#include "velox/type/Variant.h"
#include <gtest/gtest.h>
#include <velox/type/Type.h>
#include <numeric>

using namespace facebook::velox;

TEST(VariantTest, arrayInferType) {
  EXPECT_EQ(*ARRAY(UNKNOWN()), *variant(TypeKind::ARRAY).inferType());
  EXPECT_EQ(*ARRAY(UNKNOWN()), *variant::array({}).inferType());
  EXPECT_EQ(
      *ARRAY(BIGINT()),
      *variant::array({variant(TypeKind::BIGINT)}).inferType());
  EXPECT_EQ(
      *ARRAY(VARCHAR()),
      *variant::array({variant(TypeKind::VARCHAR)}).inferType());
  EXPECT_EQ(
      *ARRAY(ARRAY(DOUBLE())),
      *variant::array({variant::array({variant(TypeKind::DOUBLE)})})
           .inferType());
}

TEST(VariantTest, mapInferType) {
  EXPECT_EQ(*variant::map({{1LL, 1LL}}).inferType(), *MAP(BIGINT(), BIGINT()));
  EXPECT_EQ(*variant::map({}).inferType(), *MAP(UNKNOWN(), UNKNOWN()));

  const variant nullBigint = variant::null(TypeKind::BIGINT);
  const variant nullReal = variant::null(TypeKind::REAL);
  EXPECT_EQ(
      *variant::map({{nullBigint, nullReal}, {1LL, 1.0f}}).inferType(),
      *MAP(BIGINT(), REAL()));
  EXPECT_EQ(
      *variant::map({{nullBigint, 1.0f}, {1LL, nullReal}}).inferType(),
      *MAP(BIGINT(), REAL()));
  EXPECT_EQ(
      *variant::map({{nullBigint, 1.0f}}).inferType(), *MAP(UNKNOWN(), REAL()));
}

struct Foo {};

struct Bar {};

TEST(VariantTest, opaque) {
  auto foo = std::make_shared<Foo>();
  auto foo2 = std::make_shared<Foo>();
  auto bar = std::make_shared<Bar>();
  {
    variant v = variant::opaque(foo);
    EXPECT_TRUE(v.hasValue());
    EXPECT_EQ(TypeKind::OPAQUE, v.kind());
    EXPECT_EQ(foo, v.opaque<Foo>());
    EXPECT_THROW(v.opaque<Bar>(), std::exception);
    EXPECT_EQ(*v.inferType(), *OPAQUE<Foo>());
  }
  {
    EXPECT_EQ(1, foo.use_count());
    variant v = variant::opaque(foo);
    EXPECT_EQ(2, foo.use_count());
    variant vv = v;
    EXPECT_EQ(3, foo.use_count());
    { variant tmp = std::move(vv); }
    EXPECT_EQ(2, foo.use_count());
    v = 0;
    EXPECT_EQ(1, foo.use_count());
  }
  {
    variant v1 = variant::opaque(foo);
    variant vv1 = variant::opaque(foo);
    variant v2 = variant::opaque(foo2);
    variant v3 = variant::opaque(bar);
    variant vint = 123;
    EXPECT_EQ(v1, vv1);
    EXPECT_NE(v1, v2);
    EXPECT_NE(v1, v3);
    EXPECT_NE(v1, vint);
  }
}

TEST(VariantTest, shortDecimal) {
  auto shortDecimalType = DECIMAL(10, 3);
  variant v = variant::shortDecimal(1234, shortDecimalType);
  EXPECT_TRUE(v.hasValue());
  EXPECT_EQ(TypeKind::SHORT_DECIMAL, v.kind());
  EXPECT_EQ(1234, v.value<TypeKind::SHORT_DECIMAL>().value().unscaledValue());
  EXPECT_EQ(10, v.value<TypeKind::SHORT_DECIMAL>().precision);
  EXPECT_EQ(3, v.value<TypeKind::SHORT_DECIMAL>().scale);
  EXPECT_EQ(*v.inferType(), *shortDecimalType);
  EXPECT_EQ(v.toJson(), "1.234");
  // 1.2345
  variant u1 = variant::shortDecimal(12345, DECIMAL(10, 4));
  // 1.2345 > 1.234
  EXPECT_LT(
      v.value<TypeKind::SHORT_DECIMAL>(), u1.value<TypeKind::SHORT_DECIMAL>());
  // 0.1234
  variant u2 = variant::shortDecimal(1234, DECIMAL(10, 4));
  // 0.1234 < 1.234
  EXPECT_LT(
      u2.value<TypeKind::SHORT_DECIMAL>(), v.value<TypeKind::SHORT_DECIMAL>());
}

TEST(VariantTest, shortDecimalNull) {
  variant null = variant::shortDecimal(std::nullopt, DECIMAL(10, 5));
  EXPECT_TRUE(null.isNull());
  EXPECT_EQ(null.toJson(), "null");
  EXPECT_EQ(*null.inferType(), *DECIMAL(10, 5));
  EXPECT_THROW(variant::null(TypeKind::SHORT_DECIMAL), VeloxException);
}

TEST(VariantTest, longDecimal) {
  auto longDecimalType = DECIMAL(20, 3);
  variant v = variant::longDecimal(12345, longDecimalType);
  EXPECT_TRUE(v.hasValue());
  EXPECT_EQ(TypeKind::LONG_DECIMAL, v.kind());
  EXPECT_EQ(12345, v.value<TypeKind::LONG_DECIMAL>().value().unscaledValue());
  EXPECT_EQ(20, v.value<TypeKind::LONG_DECIMAL>().precision);
  EXPECT_EQ(3, v.value<TypeKind::LONG_DECIMAL>().scale);
  EXPECT_EQ(*v.inferType(), *longDecimalType);
  EXPECT_EQ(v.toJson(), "12.345");
  // 1.2345
  variant u1 = variant::longDecimal(12345, DECIMAL(20, 4));
  // 1.2345 < 12.345
  EXPECT_LT(
      u1.value<TypeKind::LONG_DECIMAL>(), v.value<TypeKind::LONG_DECIMAL>());
  // 12.3456
  variant u2 = variant::longDecimal(123456, DECIMAL(20, 4));
  // 12.3456 > 12.345
  EXPECT_LT(
      v.value<TypeKind::LONG_DECIMAL>(), u2.value<TypeKind::LONG_DECIMAL>());
}

TEST(VariantTest, longDecimalNull) {
  variant null = variant::longDecimal(std::nullopt, DECIMAL(20, 5));
  EXPECT_TRUE(null.isNull());
  EXPECT_EQ(null.toJson(), "null");
  EXPECT_EQ(*null.inferType(), *DECIMAL(20, 5));
  EXPECT_THROW(variant::null(TypeKind::LONG_DECIMAL), VeloxException);
}

/// Test variant::equalsWithEpsilon by summing up large 64-bit integers (> 15
/// digits long) into double in different order to get slightly different
/// results due to loss of precision.
TEST(VariantTest, equalsWithEpsilonDouble) {
  std::vector<int64_t> data = {
      -6524373357247204968,
      -1459602477200235160,
      -5427507077629018454,
      -6362318851342815124,
      -6567761115475435067,
      9193194088128540374,
      -7862838580565801772,
      -7650459730033994045,
      327870505158904254,
  };

  double sum1 = std::accumulate(data.begin(), data.end(), 0.0);

  double sumEven = 0;
  double sumOdd = 0;
  for (auto i = 0; i < data.size(); i++) {
    if (i % 2 == 0) {
      sumEven += data[i];
    } else {
      sumOdd += data[i];
    }
  }

  double sum2 = sumOdd + sumEven;

  ASSERT_NE(sum1, sum2);
  ASSERT_DOUBLE_EQ(sum1, sum2);
  ASSERT_TRUE(variant(sum1).equalsWithEpsilon(variant(sum2)));

  // Add up all numbers but one. Make sure the result is not equal to sum1.
  double sum3 = 0;
  for (auto i = 0; i < data.size(); i++) {
    if (i != 5) {
      sum3 += data[i];
    }
  }

  ASSERT_NE(sum1, sum3);
  ASSERT_FALSE(variant(sum1).equalsWithEpsilon(variant(sum3)));
}

/// Similar to equalsWithEpsilonDouble, test variant::equalsWithEpsilon by
/// summing up large 32-bit integers into float in different order to get
/// slightly different results due to loss of precision.
TEST(VariantTest, equalsWithEpsilonFloat) {
  std::vector<int32_t> data{
      -795755684,
      581869302,
      -404620562,
      -708632711,
      545404204,
      -133711905,
      -372047867,
      949333985,
      -1579004998,
      1323567403,
  };

  float sum1 = std::accumulate(data.begin(), data.end(), 0.0f);

  float sumEven = 0;
  float sumOdd = 0;
  for (auto i = 0; i < data.size(); i++) {
    if (i % 2 == 0) {
      sumEven += data[i];
    } else {
      sumOdd += data[i];
    }
  }

  float sum2 = sumOdd + sumEven;

  ASSERT_NE(sum1, sum2);
  ASSERT_FLOAT_EQ(sum1, sum2);
  ASSERT_TRUE(variant(sum1).equalsWithEpsilon(variant(sum2)));

  // Add up all numbers but one. Make sure the result is not equal to sum1.
  float sum3 = 0;
  for (auto i = 0; i < data.size(); i++) {
    if (i != 5) {
      sum3 += data[i];
    }
  }

  ASSERT_NE(sum1, sum3);
  ASSERT_FALSE(variant(sum1).equalsWithEpsilon(variant(sum3)));
}
