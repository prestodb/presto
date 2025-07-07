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
#include <numeric>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/tests/utils/CustomTypesForTesting.h"

using namespace facebook::velox;

namespace {
void testSerDe(const Variant& value) {
  auto serialized = value.serialize();
  auto copy = Variant::create(serialized);

  ASSERT_EQ(value, copy);
}
} // namespace

TEST(VariantTest, arrayInferType) {
  EXPECT_EQ(*ARRAY(UNKNOWN()), *Variant(TypeKind::ARRAY).inferType());
  EXPECT_EQ(*ARRAY(UNKNOWN()), *Variant::array({}).inferType());
  EXPECT_EQ(
      *ARRAY(BIGINT()),
      *Variant::array({Variant(TypeKind::BIGINT)}).inferType());
  EXPECT_EQ(
      *ARRAY(VARCHAR()),
      *Variant::array({Variant(TypeKind::VARCHAR)}).inferType());
  EXPECT_EQ(
      *ARRAY(ARRAY(DOUBLE())),
      *Variant::array({Variant::array({Variant(TypeKind::DOUBLE)})})
           .inferType());
  VELOX_ASSERT_THROW(
      Variant::array({Variant(123456789), Variant("velox")}),
      "All array elements must be of the same kind");
}

TEST(VariantTest, mapInferType) {
  EXPECT_EQ(*Variant::map({{1LL, 1LL}}).inferType(), *MAP(BIGINT(), BIGINT()));
  EXPECT_EQ(*Variant::map({}).inferType(), *MAP(UNKNOWN(), UNKNOWN()));

  const Variant nullBigint = Variant::null(TypeKind::BIGINT);
  const Variant nullReal = Variant::null(TypeKind::REAL);
  EXPECT_EQ(
      *Variant::map({{nullBigint, nullReal}, {1LL, 1.0f}}).inferType(),
      *MAP(BIGINT(), REAL()));
  EXPECT_EQ(
      *Variant::map({{nullBigint, 1.0f}, {1LL, nullReal}}).inferType(),
      *MAP(BIGINT(), REAL()));
  EXPECT_EQ(
      *Variant::map({{nullBigint, 1.0f}}).inferType(), *MAP(UNKNOWN(), REAL()));
}

struct Foo {};

struct Bar {};

TEST(VariantTest, opaque) {
  auto foo = std::make_shared<Foo>();
  auto foo2 = std::make_shared<Foo>();
  auto bar = std::make_shared<Bar>();
  {
    Variant v = Variant::opaque(foo);
    EXPECT_TRUE(v.hasValue());
    EXPECT_EQ(TypeKind::OPAQUE, v.kind());
    EXPECT_EQ(foo, v.opaque<Foo>());
    EXPECT_THROW(v.opaque<Bar>(), std::exception);
    EXPECT_EQ(*v.inferType(), *OPAQUE<Foo>());
  }

  // Check that the expected shared ptrs are acquired.
  {
    EXPECT_EQ(1, foo.use_count());
    Variant v = Variant::opaque(foo);
    EXPECT_EQ(2, foo.use_count());
    Variant vv = v;
    EXPECT_EQ(3, foo.use_count());
    { Variant tmp = std::move(vv); }
    EXPECT_EQ(2, foo.use_count());
    v = 0;
    EXPECT_EQ(1, foo.use_count());
  }

  // Test opaque equality.
  {
    Variant v1 = Variant::opaque(foo);
    Variant vv1 = Variant::opaque(foo);
    Variant v2 = Variant::opaque(foo2);
    Variant v3 = Variant::opaque(bar);
    Variant vint = 123;
    EXPECT_EQ(v1, vv1);
    EXPECT_NE(v1, v2);
    EXPECT_NE(v1, v3);
    EXPECT_NE(v1, vint);
  }

  // Test hashes. The semantic of the hash follows the object it points to
  // (it hashes the pointer).
  {
    Variant v1 = Variant::opaque(foo);
    Variant vv1 = Variant::opaque(foo);

    Variant v2 = Variant::opaque(foo2);
    Variant v3 = Variant::opaque(bar);

    EXPECT_EQ(v1.hash(), vv1.hash());
    EXPECT_NE(v1.hash(), v2.hash());
    EXPECT_NE(vv1.hash(), v2.hash());

    EXPECT_NE(v1.hash(), v3.hash());
    EXPECT_NE(v2.hash(), v3.hash());
  }

  // Test opaque casting.
  {
    Variant fooOpaque = Variant::opaque(foo);
    Variant barOpaque = Variant::opaque(bar);
    Variant int1 = Variant((int64_t)123);

    auto castFoo1 = fooOpaque.tryOpaque<Foo>();
    auto castBar1 = fooOpaque.tryOpaque<Bar>();
    auto castBar2 = barOpaque.tryOpaque<Bar>();

    EXPECT_EQ(castFoo1, foo);
    EXPECT_EQ(castBar1, nullptr);
    EXPECT_EQ(castBar2, bar);
    EXPECT_THROW(int1.tryOpaque<Foo>(), std::invalid_argument);
  }
}

/// Test Variant::equalsWithEpsilon by summing up large 64-bit integers (> 15
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
  ASSERT_TRUE(Variant(sum1).equalsWithEpsilon(Variant(sum2)));

  // Add up all numbers but one. Make sure the result is not equal to sum1.
  double sum3 = 0;
  for (auto i = 0; i < data.size(); i++) {
    if (i != 5) {
      sum3 += data[i];
    }
  }

  ASSERT_NE(sum1, sum3);
  ASSERT_FALSE(Variant(sum1).equalsWithEpsilon(Variant(sum3)));
}

/// Similar to equalsWithEpsilonDouble, test Variant::equalsWithEpsilon by
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
  ASSERT_TRUE(Variant(sum1).equalsWithEpsilon(Variant(sum2)));

  // Add up all numbers but one. Make sure the result is not equal to sum1.
  float sum3 = 0;
  for (auto i = 0; i < data.size(); i++) {
    if (i != 5) {
      sum3 += data[i];
    }
  }

  ASSERT_NE(sum1, sum3);
  ASSERT_FALSE(Variant(sum1).equalsWithEpsilon(Variant(sum3)));
}

TEST(VariantTest, mapWithNaNKey) {
  // Verify that map variants treat all NaN keys as equivalent and comparable
  // (consider them the largest) with other values.
  static const double KNan = std::numeric_limits<double>::quiet_NaN();
  auto mapType = MAP(DOUBLE(), INTEGER());
  {
    // NaN added at the start of insertions.
    std::map<Variant, Variant> mapVariant;
    mapVariant.insert({Variant(KNan), Variant(1)});
    mapVariant.insert({Variant(1.2), Variant(2)});
    mapVariant.insert({Variant(12.4), Variant(3)});
    EXPECT_EQ(
        "[{\"key\":1.2,\"value\":2},{\"key\":12.4,\"value\":3},{\"key\":\"NaN\",\"value\":1}]",
        Variant::map(mapVariant).toJson(mapType));
  }
  {
    // NaN added in the middle of insertions.
    std::map<Variant, Variant> mapVariant;
    mapVariant.insert({Variant(1.2), Variant(2)});
    mapVariant.insert({Variant(KNan), Variant(1)});
    mapVariant.insert({Variant(12.4), Variant(3)});
    EXPECT_EQ(
        "[{\"key\":1.2,\"value\":2},{\"key\":12.4,\"value\":3},{\"key\":\"NaN\",\"value\":1}]",
        Variant::map(mapVariant).toJson(mapType));
  }
}

TEST(VariantTest, serialize) {
  // Null values.
  testSerDe(Variant(TypeKind::BOOLEAN));
  testSerDe(Variant(TypeKind::TINYINT));
  testSerDe(Variant(TypeKind::SMALLINT));
  testSerDe(Variant(TypeKind::INTEGER));
  testSerDe(Variant(TypeKind::BIGINT));
  testSerDe(Variant(TypeKind::REAL));
  testSerDe(Variant(TypeKind::DOUBLE));
  testSerDe(Variant(TypeKind::VARCHAR));
  testSerDe(Variant(TypeKind::VARBINARY));
  testSerDe(Variant(TypeKind::TIMESTAMP));
  testSerDe(Variant(TypeKind::ARRAY));
  testSerDe(Variant(TypeKind::MAP));
  testSerDe(Variant(TypeKind::ROW));
  testSerDe(Variant(TypeKind::UNKNOWN));

  // Non-null values.
  testSerDe(Variant(true));
  testSerDe(Variant((int8_t)12));
  testSerDe(Variant((int16_t)1234));
  testSerDe(Variant((int32_t)12345));
  testSerDe(Variant((int64_t)1234567));
  testSerDe(Variant((float)1.2));
  testSerDe(Variant((double)1.234));
  testSerDe(Variant("This is a test."));
  testSerDe(Variant::binary("This is a test."));
  testSerDe(Variant(Timestamp(1, 2)));
}

struct SerializableClass {
  const std::string name;
  const bool value;
  SerializableClass(std::string name, bool value)
      : name(std::move(name)), value(value) {}
};

class VariantSerializationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    static folly::once_flag once;
    folly::call_once(once, []() {
      OpaqueType::registerSerialization<SerializableClass>(
          "SerializableClass",
          [](const std::shared_ptr<SerializableClass>& obj) -> std::string {
            return folly::json::serialize(
                folly::dynamic::object("name", obj->name)("value", obj->value),
                getSerializationOptions());
          },
          [](const std::string& json) -> std::shared_ptr<SerializableClass> {
            folly::dynamic obj = folly::parseJson(json);
            return std::make_shared<SerializableClass>(
                obj["name"].asString(), obj["value"].asBool());
          });
    });
    var_ = Variant::opaque<SerializableClass>(
        std::make_shared<SerializableClass>("test_class", false));
  }

  Variant var_;
};

TEST_F(VariantSerializationTest, serializeOpaque) {
  auto serialized = var_.serialize();
  auto deserialized_variant = Variant::create(serialized);
  auto opaque = deserialized_variant.value<TypeKind::OPAQUE>().obj;
  auto original_class = std::static_pointer_cast<SerializableClass>(opaque);
  EXPECT_EQ(original_class->name, "test_class");
  EXPECT_EQ(original_class->value, false);
}

TEST_F(VariantSerializationTest, opaqueToString) {
  const auto type = var_.inferType();
  auto s = var_.toJson(type);
  EXPECT_EQ(
      s,
      "Opaque<type:OPAQUE<SerializableClass>,value:\"{\"name\":\"test_class\",\"value\":false}\">");
}

TEST(VariantFloatingToJsonTest, normalTest) {
  // Zero
  EXPECT_EQ(Variant::create<float>(0).toJson(REAL()), "0");
  EXPECT_EQ(Variant::create<double>(0).toJson(DOUBLE()), "0");

  // Infinite
  EXPECT_EQ(
      Variant::create<float>(std::numeric_limits<float>::infinity())
          .toJson(REAL()),
      "\"Infinity\"");
  EXPECT_EQ(
      Variant::create<double>(std::numeric_limits<double>::infinity())
          .toJson(DOUBLE()),
      "\"Infinity\"");

  // NaN
  EXPECT_EQ(Variant::create<float>(0.0 / 0.0).toJson(REAL()), "\"NaN\"");
  EXPECT_EQ(Variant::create<double>(0.0 / 0.0).toJson(DOUBLE()), "\"NaN\"");
}

TEST(VariantTest, opaqueSerializationNotRegistered) {
  struct opaqueSerializationTestStruct {};
  auto opaqueBeforeRegistration =
      Variant::opaque<opaqueSerializationTestStruct>(
          std::make_shared<opaqueSerializationTestStruct>());
  EXPECT_THROW(opaqueBeforeRegistration.serialize(), VeloxException);

  OpaqueType::registerSerialization<opaqueSerializationTestStruct>(
      "opaqueSerializationStruct");

  auto opaqueAfterRegistration = Variant::opaque<opaqueSerializationTestStruct>(
      std::make_shared<opaqueSerializationTestStruct>());
  EXPECT_THROW(opaqueAfterRegistration.serialize(), VeloxException);
}

TEST(VariantTest, toJsonRow) {
  auto rowType = ROW({{"c0", DECIMAL(20, 3)}});
  EXPECT_EQ(
      "[123456.789]",
      Variant::row({static_cast<int128_t>(123456789)}).toJson(rowType));

  rowType = ROW({{"c0", DECIMAL(10, 2)}});
  EXPECT_EQ("[12345.67]", Variant::row({1234567LL}).toJson(rowType));

  rowType = ROW({{"c0", DECIMAL(20, 1)}, {"c1", BOOLEAN()}, {"c3", VARCHAR()}});
  EXPECT_EQ(
      "[1234567890.1,true,\"test works fine\"]",
      Variant::row({static_cast<int128_t>(12345678901),
                    Variant((bool)true),
                    Variant((std::string) "test works fine")})
          .toJson(rowType));

  // Row Variant tests with wrong type passed to Variant::toJson()
  rowType = ROW({{"c0", DECIMAL(10, 3)}});
  VELOX_ASSERT_THROW(
      Variant::row({static_cast<int128_t>(123456789)}).toJson(rowType),
      "(HUGEINT vs. BIGINT) Wrong type in Variant::toJson");

  rowType = ROW({{"c0", DECIMAL(20, 3)}});
  VELOX_ASSERT_THROW(
      Variant::row({123456789LL}).toJson(rowType),
      "(BIGINT vs. HUGEINT) Wrong type in Variant::toJson");
  VELOX_ASSERT_THROW(
      Variant::row(
          {static_cast<int128_t>(123456789),
           Variant((
               std::
                   string) "test confirms Variant child count is greater than expected"),
           Variant((bool)false)})
          .toJson(rowType),
      "(3 vs. 1) Wrong number of fields in a struct in Variant::toJson");

  rowType =
      ROW({{"c0", DECIMAL(19, 4)}, {"c1", VARCHAR()}, {"c2", DECIMAL(10, 3)}});
  VELOX_ASSERT_THROW(
      Variant::row(
          {static_cast<int128_t>(12345678912),
           Variant((
               std::
                   string) "test confirms Variant child count is lesser than expected")})
          .toJson(rowType),
      "(2 vs. 3) Wrong number of fields in a struct in Variant::toJson");

  // Row Variant tests that contains NULL variants.
  EXPECT_EQ(
      "[null,null,null]",
      Variant::row({Variant::null(TypeKind::HUGEINT),
                    Variant::null(TypeKind::VARCHAR),
                    Variant::null(TypeKind::BIGINT)})
          .toJson(rowType));
}

TEST(VariantTest, toJsonArray) {
  auto arrayType = ARRAY(DECIMAL(9, 2));
  EXPECT_EQ(
      "[1234567.89,6345654.64,2345452.78]",
      Variant::array({123456789LL, 634565464LL, 234545278LL})
          .toJson(arrayType));

  arrayType = ARRAY(DECIMAL(20, 3));
  EXPECT_EQ(
      "[123456.789,634565.464,234545.278]",
      Variant::array({static_cast<int128_t>(123456789),
                      static_cast<int128_t>(634565464),
                      static_cast<int128_t>(234545278)})
          .toJson(arrayType));

  // Array is empty.
  EXPECT_EQ("[]", Variant::array({}).toJson(arrayType));

  // Array Variant tests that contains NULL variants.
  EXPECT_EQ(
      "[null,null,null]",
      Variant::array({Variant::null(TypeKind::HUGEINT),
                      Variant::null(TypeKind::HUGEINT),
                      Variant::null(TypeKind::HUGEINT)})
          .toJson(arrayType));
}

TEST(VariantTest, toJsonMap) {
  auto mapType = MAP(VARCHAR(), DECIMAL(6, 3));
  std::map<Variant, Variant> mapValue = {
      {(std::string) "key1", 235499LL}, {(std::string) "key2", 123456LL}};
  EXPECT_EQ(
      "[{\"key\":\"key1\",\"value\":235.499},{\"key\":\"key2\",\"value\":123.456}]",
      Variant::map(mapValue).toJson(mapType));

  mapType = MAP(VARCHAR(), DECIMAL(20, 3));
  mapValue = {
      {(std::string) "key1", static_cast<int128_t>(45464562323423)},
      {(std::string) "key2", static_cast<int128_t>(12334581232456)}};
  EXPECT_EQ(
      "[{\"key\":\"key1\",\"value\":45464562323.423},{\"key\":\"key2\",\"value\":12334581232.456}]",
      Variant::map(mapValue).toJson(mapType));

  // Map Variant tests that contains NULL variants.
  mapValue = {
      {Variant::null(TypeKind::VARCHAR), Variant::null(TypeKind::HUGEINT)}};
  EXPECT_EQ(
      "[{\"key\":null,\"value\":null}]",
      Variant::map(mapValue).toJson(mapType));
}

TEST(VariantTest, typeWithCustomComparison) {
  auto zero = Variant::typeWithCustomComparison<TypeKind::BIGINT>(
      0, test::BIGINT_TYPE_WITH_CUSTOM_COMPARISON());
  auto one = Variant::typeWithCustomComparison<TypeKind::BIGINT>(
      1, test::BIGINT_TYPE_WITH_CUSTOM_COMPARISON());
  auto zeroEquivalent = Variant::typeWithCustomComparison<TypeKind::BIGINT>(
      256, test::BIGINT_TYPE_WITH_CUSTOM_COMPARISON());
  auto oneEquivalent = Variant::typeWithCustomComparison<TypeKind::BIGINT>(
      257, test::BIGINT_TYPE_WITH_CUSTOM_COMPARISON());
  auto null = Variant::null(TypeKind::BIGINT);

  ASSERT_TRUE(zero.equals(zeroEquivalent));
  ASSERT_TRUE(zero.equalsWithEpsilon(zeroEquivalent));

  ASSERT_TRUE(one.equals(oneEquivalent));
  ASSERT_TRUE(one.equalsWithEpsilon(oneEquivalent));

  ASSERT_FALSE(zero.equals(one));
  ASSERT_FALSE(zero.equalsWithEpsilon(one));

  ASSERT_FALSE(one.equals(zeroEquivalent));
  ASSERT_FALSE(one.equalsWithEpsilon(zeroEquivalent));

  ASSERT_FALSE(zero.equals(null));
  ASSERT_FALSE(zero.equalsWithEpsilon(null));

  ASSERT_FALSE(null.equals(one));
  ASSERT_FALSE(null.equalsWithEpsilon(one));

  ASSERT_FALSE(zero < zeroEquivalent);
  ASSERT_FALSE(zero.lessThanWithEpsilon(zeroEquivalent));

  ASSERT_FALSE(one < oneEquivalent);
  ASSERT_FALSE(one.lessThanWithEpsilon(oneEquivalent));

  ASSERT_TRUE(zero < one);
  ASSERT_TRUE(zero.lessThanWithEpsilon(one));

  ASSERT_FALSE(one < zeroEquivalent);
  ASSERT_FALSE(one.lessThanWithEpsilon(zeroEquivalent));

  ASSERT_FALSE(zero < null);
  ASSERT_FALSE(zero.lessThanWithEpsilon(null));

  ASSERT_TRUE(null < one);
  ASSERT_TRUE(null.lessThanWithEpsilon(one));

  ASSERT_EQ(zero.hash(), zeroEquivalent.hash());
  ASSERT_EQ(one.hash(), oneEquivalent.hash());
  ASSERT_NE(zero.hash(), one.hash());
  ASSERT_NE(zero.hash(), null.hash());
}

TEST(VariantTest, hashMap) {
  auto a = Variant::map({{1, 10}, {2, 20}});
  auto b = Variant::map({{1, 20}, {2, 10}});
  auto c = Variant::map({{2, 20}, {1, 10}});
  auto d = Variant::map({{1, 10}, {2, 20}, {3, 30}});

  ASSERT_NE(a.hash(), b.hash());
  ASSERT_EQ(a.hash(), c.hash());
  ASSERT_NE(a.hash(), d.hash());
}
