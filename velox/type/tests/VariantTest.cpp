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

namespace facebook::velox {

namespace {
void testSerDe(const Variant& value) {
  auto serialized = value.serialize();
  auto copy = Variant::create(serialized);

  ASSERT_EQ(value, copy);
}

TEST(VariantTest, arrayInferType) {
  EXPECT_EQ(*ARRAY(UNKNOWN()), *Variant(TypeKind::ARRAY).inferType());
  EXPECT_EQ(*ARRAY(UNKNOWN()), *Variant::array({}).inferType());
  EXPECT_EQ(*ARRAY(UNKNOWN()), *Variant::null(TypeKind::ARRAY).inferType());
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
  EXPECT_EQ(
      *MAP(UNKNOWN(), UNKNOWN()), *Variant::null(TypeKind::MAP).inferType());

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

TEST(VariantTest, rowInferType) {
  EXPECT_EQ(
      *ROW({BIGINT(), VARCHAR()}),
      *Variant::row({Variant(1LL), Variant("velox")}).inferType());
  EXPECT_EQ(*ROW({}), *Variant::null(TypeKind::ROW).inferType());
}

TEST(VariantTest, arrayTypeCompatibility) {
  const auto empty = Variant::array({});

  EXPECT_TRUE(empty.isTypeCompatible(ARRAY(UNKNOWN())));
  EXPECT_TRUE(empty.isTypeCompatible(ARRAY(BIGINT())));

  EXPECT_FALSE(empty.isTypeCompatible(UNKNOWN()));
  EXPECT_FALSE(empty.isTypeCompatible(BIGINT()));
  EXPECT_FALSE(empty.isTypeCompatible(MAP(INTEGER(), REAL())));

  const auto null = Variant::null(TypeKind::ARRAY);

  EXPECT_TRUE(null.isTypeCompatible(ARRAY(UNKNOWN())));
  EXPECT_TRUE(null.isTypeCompatible(ARRAY(BIGINT())));

  EXPECT_FALSE(null.isTypeCompatible(UNKNOWN()));
  EXPECT_FALSE(null.isTypeCompatible(BIGINT()));
  EXPECT_FALSE(null.isTypeCompatible(MAP(INTEGER(), REAL())));

  const auto array = Variant::array({1, 2, 3});

  EXPECT_TRUE(array.isTypeCompatible(ARRAY(INTEGER())));

  EXPECT_FALSE(array.isTypeCompatible(INTEGER()));
  EXPECT_FALSE(array.isTypeCompatible(ARRAY(REAL())));
}

TEST(VariantTest, mapTypeCompatibility) {
  const auto empty = Variant::map({});

  EXPECT_TRUE(empty.isTypeCompatible(MAP(UNKNOWN(), UNKNOWN())));
  EXPECT_TRUE(empty.isTypeCompatible(MAP(BIGINT(), BIGINT())));
  EXPECT_TRUE(empty.isTypeCompatible(MAP(REAL(), UNKNOWN())));
  EXPECT_TRUE(empty.isTypeCompatible(MAP(INTEGER(), DOUBLE())));

  EXPECT_FALSE(empty.isTypeCompatible(UNKNOWN()));
  EXPECT_FALSE(empty.isTypeCompatible(BIGINT()));
  EXPECT_FALSE(empty.isTypeCompatible(ARRAY(INTEGER())));

  const auto null = Variant::null(TypeKind::MAP);

  EXPECT_TRUE(null.isTypeCompatible(MAP(UNKNOWN(), UNKNOWN())));
  EXPECT_TRUE(null.isTypeCompatible(MAP(BIGINT(), BIGINT())));
  EXPECT_TRUE(null.isTypeCompatible(MAP(REAL(), UNKNOWN())));
  EXPECT_TRUE(null.isTypeCompatible(MAP(INTEGER(), DOUBLE())));

  EXPECT_FALSE(null.isTypeCompatible(UNKNOWN()));
  EXPECT_FALSE(null.isTypeCompatible(BIGINT()));
  EXPECT_FALSE(null.isTypeCompatible(ARRAY(INTEGER())));

  const auto map = Variant::map({{1, 1.0f}, {2, 2.0f}});

  EXPECT_TRUE(map.isTypeCompatible(MAP(INTEGER(), REAL())));

  EXPECT_FALSE(map.isTypeCompatible(MAP(INTEGER(), DOUBLE())));
  EXPECT_FALSE(map.isTypeCompatible(UNKNOWN()));
  EXPECT_FALSE(map.isTypeCompatible(ARRAY(BIGINT())));
}

TEST(VariantTest, rowTypeCompatibility) {
  const auto empty = Variant::row({});

  EXPECT_TRUE(empty.isTypeCompatible(ROW({})));

  EXPECT_FALSE(empty.isTypeCompatible(BIGINT()));
  EXPECT_FALSE(empty.isTypeCompatible(ROW({INTEGER(), REAL()})));

  const auto null = Variant::null(TypeKind::ROW);

  EXPECT_TRUE(null.isTypeCompatible(ROW({INTEGER(), REAL()})));
  EXPECT_TRUE(null.isTypeCompatible(ROW({"a", "b"}, {INTEGER(), REAL()})));

  EXPECT_FALSE(null.isTypeCompatible(BIGINT()));

  const auto row = Variant::row({1, 2.0f});

  EXPECT_TRUE(row.isTypeCompatible(ROW({INTEGER(), REAL()})));
  EXPECT_TRUE(row.isTypeCompatible(ROW({"a", "b"}, {INTEGER(), REAL()})));

  EXPECT_FALSE(row.isTypeCompatible(ROW({INTEGER()})));
  EXPECT_FALSE(row.isTypeCompatible(ROW({INTEGER(), DOUBLE()})));
  EXPECT_FALSE(row.isTypeCompatible(BIGINT()));
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
        R"([{"key":1.2,"value":2},{"key":12.4,"value":3},{"key":"NaN","value":1}])",
        Variant::map(mapVariant).toJson(mapType));
  }

  {
    // NaN added in the middle of insertions.
    std::map<Variant, Variant> mapVariant;
    mapVariant.insert({Variant(1.2), Variant(2)});
    mapVariant.insert({Variant(KNan), Variant(1)});
    mapVariant.insert({Variant(12.4), Variant(3)});
    EXPECT_EQ(
        R"([{"key":1.2,"value":2},{"key":12.4,"value":3},{"key":"NaN","value":1}])",
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
  testSerDe(Variant(static_cast<int8_t>(12)));
  testSerDe(Variant(static_cast<int16_t>(1234)));
  testSerDe(Variant(static_cast<int32_t>(12345)));
  testSerDe(Variant(static_cast<int64_t>(1234567)));
  testSerDe(Variant(static_cast<float>(1.2f)));
  testSerDe(Variant(static_cast<double>(1.234)));
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

    value_ = Variant::opaque<SerializableClass>(
        std::make_shared<SerializableClass>("test_class", false));
  }

  Variant value_;
};

TEST_F(VariantSerializationTest, serializeOpaque) {
  auto serialized = value_.serialize();
  auto deserialized = Variant::create(serialized);
  auto opaque = deserialized.value<TypeKind::OPAQUE>().obj;
  auto original = std::static_pointer_cast<SerializableClass>(opaque);
  EXPECT_EQ(original->name, "test_class");
  EXPECT_EQ(original->value, false);
}

TEST_F(VariantSerializationTest, opaqueToJson) {
  const auto type = value_.inferType();

  const auto expected =
      R"(Opaque<type:OPAQUE<facebook::velox::(anonymous namespace)::SerializableClass>,value:"{"name":"test_class","value":false}">)";
  EXPECT_EQ(value_.toJson(type), expected);
  EXPECT_EQ(value_.toString(type), expected);
}

TEST(VariantFloatingToJsonTest, normalTest) {
  // Zero
  EXPECT_EQ(Variant::create<float>(0).toJson(REAL()), "0");
  EXPECT_EQ(Variant::create<double>(0).toJson(DOUBLE()), "0");

  // Infinite
  EXPECT_EQ(
      Variant::create<float>(std::numeric_limits<float>::infinity())
          .toJson(REAL()),
      R"("Infinity")");
  EXPECT_EQ(
      Variant::create<double>(std::numeric_limits<double>::infinity())
          .toJson(DOUBLE()),
      R"("Infinity")");

  // NaN
  EXPECT_EQ(Variant::create<float>(0.0 / 0.0).toJson(REAL()), R"("NaN")");
  EXPECT_EQ(Variant::create<double>(0.0 / 0.0).toJson(DOUBLE()), R"("NaN")");
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
      R"([1234567890.1,true,"test works fine"])",
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
           Variant(
               "test confirms Variant child count is greater than expected"),
           Variant(false)})
          .toJson(rowType),
      "(3 vs. 1) Wrong number of fields in a struct in Variant::toJson");

  rowType =
      ROW({{"c0", DECIMAL(19, 4)}, {"c1", VARCHAR()}, {"c2", DECIMAL(10, 3)}});
  VELOX_ASSERT_THROW(
      Variant::row(
          {static_cast<int128_t>(12345678912),
           Variant(
               "test confirms Variant child count is lesser than expected")})
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
      {"key1", 235499LL}, {"key2", 123456LL}};
  EXPECT_EQ(
      R"([{"key":"key1","value":235.499},{"key":"key2","value":123.456}])",
      Variant::map(mapValue).toJson(mapType));

  mapType = MAP(VARCHAR(), DECIMAL(20, 3));
  mapValue = {
      {"key1", static_cast<int128_t>(45464562323423)},
      {"key2", static_cast<int128_t>(12334581232456)}};
  EXPECT_EQ(
      R"([{"key":"key1","value":45464562323.423},{"key":"key2","value":12334581232.456}])",
      Variant::map(mapValue).toJson(mapType));

  // Map Variant tests that contains NULL variants.
  mapValue = {
      {Variant::null(TypeKind::VARCHAR), Variant::null(TypeKind::HUGEINT)}};
  EXPECT_EQ(
      R"([{"key":null,"value":null}])", Variant::map(mapValue).toJson(mapType));
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

TEST(VariantTest, toString) {
  EXPECT_EQ(Variant::array({1, 2, 3}).toString(ARRAY(INTEGER())), "[1,2,3]");
  EXPECT_EQ(
      Variant::map({{1, 2}, {3, 4}}).toString(MAP(INTEGER(), INTEGER())),
      R"([{"key":1,"value":2},{"key":3,"value":4}])");
  EXPECT_EQ(
      Variant::row({1, 2, 3}).toString(ROW({INTEGER(), INTEGER(), INTEGER()})),
      "[1,2,3]");
}

template <typename T>
void testPrimitiveGetter(T v) {
  auto value = Variant(v);
  EXPECT_FALSE(value.isNull());
  EXPECT_EQ(value.value<T>(), value);
}

TEST(VariantTest, primitiveGetters) {
  testPrimitiveGetter<bool>(true);
  testPrimitiveGetter<int32_t>(10);
  testPrimitiveGetter<int64_t>(10);
  testPrimitiveGetter<float>(1.2);
  testPrimitiveGetter<double>(1.2);
}

template <typename T>
void testArrayGetter(const std::vector<T>& inputs) {
  std::vector<Variant> variants;
  variants.reserve(inputs.size());
  for (const auto& v : inputs) {
    variants.emplace_back(v);
  }

  auto value = Variant::array(variants);

  EXPECT_FALSE(value.isNull());

  auto variantItems = value.array();
  EXPECT_EQ(variantItems.size(), inputs.size());
  for (auto i = 0; i < inputs.size(); ++i) {
    EXPECT_FALSE(variantItems.at(i).isNull());
    EXPECT_EQ(variantItems.at(i).template value<T>(), inputs.at(i));
  }

  auto primitiveItems = value.template array<T>();
  EXPECT_EQ(primitiveItems.size(), inputs.size());
  for (auto i = 0; i < inputs.size(); ++i) {
    EXPECT_EQ(primitiveItems.at(i), inputs.at(i));
  }
}

TEST(VariantTest, arrayGetter) {
  testArrayGetter<bool>({true, false, true});
  testArrayGetter<int32_t>({1, 2, 3});
  testArrayGetter<int64_t>({1, 2, 3});
  testArrayGetter<float>({1.2, 2.3, 3.4});
  testArrayGetter<double>({1.2, 2.3, 3.4});
}

template <typename K, typename V>
void testMapGetter(const std::map<K, V>& inputs) {
  std::map<Variant, Variant> variants;
  for (const auto& [k, v] : inputs) {
    variants.emplace(k, v);
  }

  auto value = Variant::map(variants);

  EXPECT_FALSE(value.isNull());

  auto variantItems = value.map();
  EXPECT_EQ(variantItems.size(), inputs.size());

  auto expectedIt = inputs.begin();
  for (auto it = variantItems.begin(); it != variantItems.end(); it++) {
    auto [k, v] = *it;

    EXPECT_FALSE(k.isNull());
    EXPECT_FALSE(v.isNull());
    EXPECT_EQ(k.template value<K>(), (*expectedIt).first);
    EXPECT_EQ(v.template value<V>(), (*expectedIt).second);
    expectedIt++;
  }

  auto primitiveItems = value.template map<K, V>();
  EXPECT_EQ(primitiveItems, inputs);
}

TEST(VariantTest, mapGetter) {
  testMapGetter<int32_t, float>({{1, 1.2}, {2, 2.3}, {3, 3.4}});
  testMapGetter<int8_t, double>({{1, 1.2}, {2, 2.3}, {3, 3.4}});
}

} // namespace
} // namespace facebook::velox
