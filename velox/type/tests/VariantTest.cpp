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

using namespace facebook::velox;

namespace {
void testSerDe(const variant& value) {
  auto serialized = value.serialize();
  auto copy = variant::create(serialized);

  ASSERT_EQ(value, copy);
}
} // namespace

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
  VELOX_ASSERT_THROW(
      variant::array({variant(123456789), variant("velox")}),
      "All array elements must be of the same kind");
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

TEST(VariantTest, mapWithNaNKey) {
  // Verify that map variants treat all NaN keys as equivalent and comparable
  // (consider them the largest) with other values.
  static const double KNan = std::numeric_limits<double>::quiet_NaN();
  auto mapType = MAP(DOUBLE(), INTEGER());
  {
    // NaN added at the start of insertions.
    std::map<variant, variant> mapVariant;
    mapVariant.insert({variant(KNan), variant(1)});
    mapVariant.insert({variant(1.2), variant(2)});
    mapVariant.insert({variant(12.4), variant(3)});
    EXPECT_EQ(
        "[{\"key\":1.2,\"value\":2},{\"key\":12.4,\"value\":3},{\"key\":\"NaN\",\"value\":1}]",
        variant::map(mapVariant).toJson(mapType));
  }
  {
    // NaN added in the middle of insertions.
    std::map<variant, variant> mapVariant;
    mapVariant.insert({variant(1.2), variant(2)});
    mapVariant.insert({variant(KNan), variant(1)});
    mapVariant.insert({variant(12.4), variant(3)});
    EXPECT_EQ(
        "[{\"key\":1.2,\"value\":2},{\"key\":12.4,\"value\":3},{\"key\":\"NaN\",\"value\":1}]",
        variant::map(mapVariant).toJson(mapType));
  }
}

TEST(VariantTest, serialize) {
  // Null values.
  testSerDe(variant(TypeKind::BOOLEAN));
  testSerDe(variant(TypeKind::TINYINT));
  testSerDe(variant(TypeKind::SMALLINT));
  testSerDe(variant(TypeKind::INTEGER));
  testSerDe(variant(TypeKind::BIGINT));
  testSerDe(variant(TypeKind::REAL));
  testSerDe(variant(TypeKind::DOUBLE));
  testSerDe(variant(TypeKind::VARCHAR));
  testSerDe(variant(TypeKind::VARBINARY));
  testSerDe(variant(TypeKind::TIMESTAMP));
  testSerDe(variant(TypeKind::ARRAY));
  testSerDe(variant(TypeKind::MAP));
  testSerDe(variant(TypeKind::ROW));
  testSerDe(variant(TypeKind::UNKNOWN));

  // Non-null values.
  testSerDe(variant(true));
  testSerDe(variant((int8_t)12));
  testSerDe(variant((int16_t)1234));
  testSerDe(variant((int32_t)12345));
  testSerDe(variant((int64_t)1234567));
  testSerDe(variant((float)1.2));
  testSerDe(variant((double)1.234));
  testSerDe(variant("This is a test."));
  testSerDe(variant::binary("This is a test."));
  testSerDe(variant(Timestamp(1, 2)));
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
    var_ = variant::opaque<SerializableClass>(
        std::make_shared<SerializableClass>("test_class", false));
  }

  variant var_;
};

TEST_F(VariantSerializationTest, serializeOpaque) {
  auto serialized = var_.serialize();
  auto deserialized_variant = variant::create(serialized);
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
  EXPECT_EQ(variant::create<float>(0).toJson(REAL()), "0");
  EXPECT_EQ(variant::create<double>(0).toJson(DOUBLE()), "0");

  // Infinite
  EXPECT_EQ(
      variant::create<float>(std::numeric_limits<float>::infinity())
          .toJson(REAL()),
      "\"Infinity\"");
  EXPECT_EQ(
      variant::create<double>(std::numeric_limits<double>::infinity())
          .toJson(DOUBLE()),
      "\"Infinity\"");

  // NaN
  EXPECT_EQ(variant::create<float>(0.0 / 0.0).toJson(REAL()), "\"NaN\"");
  EXPECT_EQ(variant::create<double>(0.0 / 0.0).toJson(DOUBLE()), "\"NaN\"");
}

TEST(VariantTest, opaqueSerializationNotRegistered) {
  struct opaqueSerializationTestStruct {};
  auto opaqueBeforeRegistration =
      variant::opaque<opaqueSerializationTestStruct>(
          std::make_shared<opaqueSerializationTestStruct>());
  EXPECT_THROW(opaqueBeforeRegistration.serialize(), VeloxException);

  OpaqueType::registerSerialization<opaqueSerializationTestStruct>(
      "opaqueSerializationStruct");

  auto opaqueAfterRegistration = variant::opaque<opaqueSerializationTestStruct>(
      std::make_shared<opaqueSerializationTestStruct>());
  EXPECT_THROW(opaqueAfterRegistration.serialize(), VeloxException);
}

TEST(VariantTest, toJsonRow) {
  auto rowType = ROW({{"c0", DECIMAL(20, 3)}});
  EXPECT_EQ(
      "[123456.789]",
      variant::row({static_cast<int128_t>(123456789)}).toJson(rowType));

  rowType = ROW({{"c0", DECIMAL(10, 2)}});
  EXPECT_EQ("[12345.67]", variant::row({1234567LL}).toJson(rowType));

  rowType = ROW({{"c0", DECIMAL(20, 1)}, {"c1", BOOLEAN()}, {"c3", VARCHAR()}});
  EXPECT_EQ(
      "[1234567890.1,true,\"test works fine\"]",
      variant::row({static_cast<int128_t>(12345678901),
                    variant((bool)true),
                    variant((std::string) "test works fine")})
          .toJson(rowType));

  // Row variant tests with wrong type passed to variant::toJson()
  rowType = ROW({{"c0", DECIMAL(10, 3)}});
  VELOX_ASSERT_THROW(
      variant::row({static_cast<int128_t>(123456789)}).toJson(rowType),
      "(HUGEINT vs. BIGINT) Wrong type in variant::toJson");

  rowType = ROW({{"c0", DECIMAL(20, 3)}});
  VELOX_ASSERT_THROW(
      variant::row({123456789LL}).toJson(rowType),
      "(BIGINT vs. HUGEINT) Wrong type in variant::toJson");
  VELOX_ASSERT_THROW(
      variant::row(
          {static_cast<int128_t>(123456789),
           variant((
               std::
                   string) "test confirms variant child count is greater than expected"),
           variant((bool)false)})
          .toJson(rowType),
      "(3 vs. 1) Wrong number of fields in a struct in variant::toJson");

  rowType =
      ROW({{"c0", DECIMAL(19, 4)}, {"c1", VARCHAR()}, {"c2", DECIMAL(10, 3)}});
  VELOX_ASSERT_THROW(
      variant::row(
          {static_cast<int128_t>(12345678912),
           variant((
               std::
                   string) "test confirms variant child count is lesser than expected")})
          .toJson(rowType),
      "(2 vs. 3) Wrong number of fields in a struct in variant::toJson");

  // Row variant tests that contains NULL variants.
  EXPECT_EQ(
      "[null,null,null]",
      variant::row({variant::null(TypeKind::HUGEINT),
                    variant::null(TypeKind::VARCHAR),
                    variant::null(TypeKind::BIGINT)})
          .toJson(rowType));
}

TEST(VariantTest, toJsonArray) {
  auto arrayType = ARRAY(DECIMAL(9, 2));
  EXPECT_EQ(
      "[1234567.89,6345654.64,2345452.78]",
      variant::array({123456789LL, 634565464LL, 234545278LL})
          .toJson(arrayType));

  arrayType = ARRAY(DECIMAL(20, 3));
  EXPECT_EQ(
      "[123456.789,634565.464,234545.278]",
      variant::array({static_cast<int128_t>(123456789),
                      static_cast<int128_t>(634565464),
                      static_cast<int128_t>(234545278)})
          .toJson(arrayType));

  // Array is empty.
  EXPECT_EQ("[]", variant::array({}).toJson(arrayType));

  // Array variant tests that contains NULL variants.
  EXPECT_EQ(
      "[null,null,null]",
      variant::array({variant::null(TypeKind::HUGEINT),
                      variant::null(TypeKind::HUGEINT),
                      variant::null(TypeKind::HUGEINT)})
          .toJson(arrayType));
}

TEST(VariantTest, toJsonMap) {
  auto mapType = MAP(VARCHAR(), DECIMAL(6, 3));
  std::map<variant, variant> mapValue = {
      {(std::string) "key1", 235499LL}, {(std::string) "key2", 123456LL}};
  EXPECT_EQ(
      "[{\"key\":\"key1\",\"value\":235.499},{\"key\":\"key2\",\"value\":123.456}]",
      variant::map(mapValue).toJson(mapType));

  mapType = MAP(VARCHAR(), DECIMAL(20, 3));
  mapValue = {
      {(std::string) "key1", static_cast<int128_t>(45464562323423)},
      {(std::string) "key2", static_cast<int128_t>(12334581232456)}};
  EXPECT_EQ(
      "[{\"key\":\"key1\",\"value\":45464562323.423},{\"key\":\"key2\",\"value\":12334581232.456}]",
      variant::map(mapValue).toJson(mapType));

  // Map variant tests that contains NULL variants.
  mapValue = {
      {variant::null(TypeKind::VARCHAR), variant::null(TypeKind::HUGEINT)}};
  EXPECT_EQ(
      "[{\"key\":null,\"value\":null}]",
      variant::map(mapValue).toJson(mapType));
}
