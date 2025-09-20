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
#include <gtest/gtest.h>

#include "velox/common/memory/Memory.h"
#include "velox/core/Expressions.h"
#include "velox/functions/prestosql/types/HyperLogLogType.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/functions/prestosql/types/TDigestType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/Variant.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::core::test {

namespace {
struct TestOpaqueStruct {
  int value;
  std::string name;

  TestOpaqueStruct(int v, std::string n) : value(v), name(std::move(n)) {}

  bool operator==(const TestOpaqueStruct& other) const {
    return value == other.value && name == other.name;
  }
};

} // namespace

class ConstantTypedExprTest : public ::testing::Test,
                              public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool();

    // Register serialization/deserialization functions needed for the tests
    Type::registerSerDe();
    ITypedExpr::registerSerDe();

    // Register OPAQUE type serialization for TestOpaqueStruct
    static folly::once_flag once;
    folly::call_once(once, []() {
      OpaqueType::registerSerialization<TestOpaqueStruct>(
          "TestOpaqueStruct",
          [](const std::shared_ptr<TestOpaqueStruct>& obj) -> std::string {
            return folly::json::serialize(
                folly::dynamic::object("value", obj->value)("name", obj->name),
                folly::json::serialization_opts{});
          },
          [](const std::string& json) -> std::shared_ptr<TestOpaqueStruct> {
            folly::dynamic obj = folly::parseJson(json);
            return std::make_shared<TestOpaqueStruct>(
                obj["value"].asInt(), obj["name"].asString());
          });
    });
  }

  // Helper functions
  template <typename T>
  std::shared_ptr<ConstantTypedExpr> createVariantExpr(
      const TypePtr& type,
      const T& value) {
    return std::make_shared<ConstantTypedExpr>(type, variant(value));
  }

  std::shared_ptr<ConstantTypedExpr> createNullVariantExpr(
      const TypePtr& type) {
    return std::make_shared<ConstantTypedExpr>(
        type, variant::null(type->kind()));
  }

  std::shared_ptr<ConstantTypedExpr> createVectorExpr(const VectorPtr& vector) {
    return std::make_shared<ConstantTypedExpr>(vector);
  }

  template <typename T>
  VectorPtr createConstantVector(const TypePtr& type, const T& value) {
    return BaseVector::createConstant(type, variant(value), 1, pool_.get());
  }

  VectorPtr createNullConstantVector(const TypePtr& type) {
    return BaseVector::createNullConstant(type, 1, pool_.get());
  }

  // Test Data
  struct TestValues {
    variant nullValue;
    std::vector<variant> nonNullValues;

    TestValues(TypeKind kind) : nullValue(variant::null(kind)) {}
  };

  TestValues getTestValues(TypeKind kind) {
    TestValues values(kind);

    switch (kind) {
      case TypeKind::BOOLEAN:
        values.nonNullValues = {variant(true), variant(false)};
        break;
      case TypeKind::TINYINT:
        values.nonNullValues = {
            variant(int8_t(0)), variant(int8_t(127)), variant(int8_t(-128))};
        break;
      case TypeKind::SMALLINT:
        values.nonNullValues = {
            variant(int16_t(0)),
            variant(int16_t(32767)),
            variant(int16_t(-32768))};
        break;
      case TypeKind::INTEGER:
        values.nonNullValues = {
            variant(int32_t(0)),
            variant(int32_t(2147483647)),
            variant(int32_t(-2147483648))};
        break;
      case TypeKind::BIGINT:
        values.nonNullValues = {
            variant(int64_t(0)),
            variant(int64_t(9223372036854775807LL)),
            variant(int64_t(-9223372036854775808ULL))};
        break;
      case TypeKind::REAL:
        values.nonNullValues = {variant(0.0f), variant(3.14f), variant(-1.5f)};
        break;
      case TypeKind::DOUBLE:
        values.nonNullValues = {
            variant(0.0), variant(3.14159), variant(-2.71828)};
        break;
      case TypeKind::VARCHAR:
        values.nonNullValues = {
            variant(""), variant("hello"), variant("test string")};
        break;
      case TypeKind::VARBINARY:
        values.nonNullValues = {
            variant::binary(""),
            variant::binary("binary data"),
            variant::binary("\x00\x01\x02")};
        break;
      case TypeKind::TIMESTAMP:
        values.nonNullValues = {
            variant(Timestamp(0, 0)),
            variant(Timestamp(1234567890, 123456789))};
        break;
      case TypeKind::HUGEINT:
        values.nonNullValues = {
            variant(int128_t(0)),
            variant(int128_t(123)),
            variant(int128_t(-456))};
        break;
      default:
        // For complex types, we'll handle them within individual tests.
        break;
    }
    return values;
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  const std::vector<TypeKind> scalarTypes_ = {
      TypeKind::BOOLEAN,
      TypeKind::TINYINT,
      TypeKind::SMALLINT,
      TypeKind::INTEGER,
      TypeKind::BIGINT,
      TypeKind::REAL,
      TypeKind::DOUBLE,
      TypeKind::VARCHAR,
      TypeKind::VARBINARY,
      TypeKind::TIMESTAMP,
      TypeKind::HUGEINT};
};

TEST_F(ConstantTypedExprTest, null) {
  auto makeNull = [](const TypePtr& type) {
    return std::make_shared<ConstantTypedExpr>(
        type, variant::null(type->kind()));
  };

  EXPECT_FALSE(*makeNull(BIGINT()) == *makeNull(DOUBLE()));
  EXPECT_FALSE(*makeNull(ARRAY(BIGINT())) == *makeNull(ARRAY(DOUBLE())));
  EXPECT_FALSE(
      *makeNull(MAP(BIGINT(), INTEGER())) ==
      *makeNull(MAP(BIGINT(), DOUBLE())));

  EXPECT_FALSE(*makeNull(JSON()) == *makeNull(VARCHAR()));
  EXPECT_FALSE(*makeNull(VARCHAR()) == *makeNull(JSON()));

  EXPECT_FALSE(*makeNull(ARRAY(JSON())) == *makeNull(ARRAY(VARCHAR())));
  EXPECT_FALSE(*makeNull(ARRAY(VARCHAR())) == *makeNull(ARRAY(JSON())));
  EXPECT_FALSE(
      *makeNull(MAP(BIGINT(), JSON())) == *makeNull(MAP(BIGINT(), VARCHAR())));
  EXPECT_FALSE(
      *makeNull(MAP(BIGINT(), VARCHAR())) == *makeNull(MAP(BIGINT(), JSON())));

  EXPECT_FALSE(*makeNull(HYPERLOGLOG()) == *makeNull(VARBINARY()));
  EXPECT_FALSE(*makeNull(VARBINARY()) == *makeNull(HYPERLOGLOG()));

  EXPECT_FALSE(*makeNull(TDIGEST(DOUBLE())) == *makeNull(VARBINARY()));
  EXPECT_FALSE(*makeNull(VARBINARY()) == *makeNull(TDIGEST(DOUBLE())));

  EXPECT_FALSE(*makeNull(TIMESTAMP_WITH_TIME_ZONE()) == *makeNull(BIGINT()));
  EXPECT_FALSE(*makeNull(BIGINT()) == *makeNull(TIMESTAMP_WITH_TIME_ZONE()));

  EXPECT_TRUE(*makeNull(DOUBLE()) == *makeNull(DOUBLE()));
  EXPECT_TRUE(*makeNull(ARRAY(DOUBLE())) == *makeNull(ARRAY(DOUBLE())));

  EXPECT_TRUE(*makeNull(JSON()) == *makeNull(JSON()));
  EXPECT_TRUE(
      *makeNull(MAP(VARCHAR(), JSON())) == *makeNull(MAP(VARCHAR(), JSON())));

  EXPECT_FALSE(*makeNull(JSON()) == *makeNull(VARCHAR()));
  EXPECT_FALSE(
      *makeNull(ROW({"a", "b"}, {INTEGER(), REAL()})) ==
      *makeNull(ROW({"x", "y"}, {INTEGER(), REAL()})));
}

TEST_F(ConstantTypedExprTest, hashScalarTypes) {
  // Tests the consistency of the hash value returned by the ConstantTypedExpr
  // between its construction using variant and Velox vectors.
  for (auto kind : scalarTypes_) {
    auto type = createScalarType(kind);
    auto testValues = getTestValues(kind);

    // null values
    auto nullVariantExpr = createNullVariantExpr(type);
    auto nullVectorExpr = createVectorExpr(createNullConstantVector(type));
    EXPECT_EQ(nullVariantExpr->hash(), nullVectorExpr->hash())
        << "Hash mismatch for null " << TypeKindName::toName(kind);

    // non-null values
    for (const auto& value : testValues.nonNullValues) {
      auto variantExpr = std::make_shared<ConstantTypedExpr>(type, value);
      auto vectorExpr = createVectorExpr(
          BaseVector::createConstant(type, value, 1, pool_.get()));
      EXPECT_EQ(variantExpr->hash(), vectorExpr->hash())
          << "Hash mismatch for non-null " << TypeKindName::toName(kind)
          << " with value " << value.toJson(type);
    }
  }
}

TEST_F(ConstantTypedExprTest, hashComplexTypes) {
  // ARRAY
  auto arrayType = ARRAY(INTEGER());

  // null values
  auto nullArrayVariantExpr = createNullVariantExpr(arrayType);
  auto nullArrayVectorExpr =
      createVectorExpr(createNullConstantVector(arrayType));
  EXPECT_EQ(nullArrayVariantExpr->hash(), nullArrayVectorExpr->hash())
      << "Hash mismatch for null ARRAY variant vs vector";

  // non-null values
  auto arrayVariant = Variant::array({1, 2, 3});
  auto arrayVariantExpr =
      std::make_shared<ConstantTypedExpr>(arrayType, arrayVariant);
  auto arrayVector = makeArrayVector<int32_t>({{1, 2, 3}});
  auto arrayVectorExpr = createVectorExpr(arrayVector);
  EXPECT_EQ(arrayVariantExpr->hash(), arrayVectorExpr->hash())
      << "Hash mismatch for non-null ARRAY variant vs vector";

  // MAP
  auto mapType = MAP(VARCHAR(), INTEGER());

  // null values
  auto nullMapVariantExpr = createNullVariantExpr(mapType);
  auto nullMapVectorExpr = createVectorExpr(createNullConstantVector(mapType));
  EXPECT_EQ(nullMapVariantExpr->hash(), nullMapVectorExpr->hash())
      << "Hash mismatch for null MAP variant vs vector";

  // non-null values
  std::map<Variant, Variant> mapData = {{"key1", 1}, {"key2", 2}};
  auto mapVariant = Variant::map(mapData);
  auto mapVariantExpr =
      std::make_shared<ConstantTypedExpr>(mapType, mapVariant);
  auto mapVector =
      makeMapVector<std::string, int32_t>({{{"key1", 1}, {"key2", 2}}});
  auto mapVectorExpr = createVectorExpr(mapVector);
  EXPECT_EQ(mapVariantExpr->hash(), mapVectorExpr->hash())
      << "Hash mismatch for non-null MAP variant vs vector";

  // ROW
  auto rowType = ROW({{"a", INTEGER()}, {"b", VARCHAR()}});

  // null values
  auto nullRowVariantExpr = createNullVariantExpr(rowType);
  auto nullRowVectorExpr = createVectorExpr(createNullConstantVector(rowType));
  EXPECT_EQ(nullRowVariantExpr->hash(), nullRowVectorExpr->hash())
      << "Hash mismatch for null ROW variant vs vector";

  // non-null values
  auto rowVariant = Variant::row({42, "hello"});
  auto rowVariantExpr =
      std::make_shared<ConstantTypedExpr>(rowType, rowVariant);
  auto rowVector = makeRowVector(
      {makeFlatVector<int32_t>({42}), makeFlatVector<std::string>({"hello"})});
  auto rowVectorExpr = createVectorExpr(rowVector);
  EXPECT_EQ(rowVariantExpr->hash(), rowVectorExpr->hash())
      << "Hash mismatch for non-null ROW variant vs vector";

  // OPAQUE
  auto testObj = std::make_shared<TestOpaqueStruct>(42, "test_data");
  auto opaqueType = OPAQUE<TestOpaqueStruct>();

  // null values
  auto nullOpaqueVariantExpr = createNullVariantExpr(opaqueType);
  auto nullOpaqueVectorExpr =
      createVectorExpr(createNullConstantVector(opaqueType));
  EXPECT_EQ(nullOpaqueVariantExpr->hash(), nullOpaqueVectorExpr->hash())
      << "Hash mismatch for null OPAQUE";

  // non-null values
  auto opaqueVariant = Variant::opaque(testObj);
  auto opaqueVariantExpr =
      std::make_shared<ConstantTypedExpr>(opaqueType, opaqueVariant);
  auto opaqueVectorExpr = createVectorExpr(
      BaseVector::createConstant(opaqueType, opaqueVariant, 1, pool_.get()));
  EXPECT_EQ(opaqueVariantExpr->hash(), opaqueVectorExpr->hash())
      << "Hash mismatch for non-null OPAQUE";
}

TEST_F(ConstantTypedExprTest, serdeScalarTypes) {
  // Test serialize/deserialize APIs for scalar types to ensure backward
  // compatibility.
  for (auto kind : scalarTypes_) {
    auto type = createScalarType(kind);
    auto testValues = getTestValues(kind);

    // null values
    auto nullVariantExpr = createNullVariantExpr(type);
    auto serialized = nullVariantExpr->serialize();
    auto deserialized = ConstantTypedExpr::create(serialized, pool_.get());
    EXPECT_TRUE(*nullVariantExpr == *deserialized)
        << "Serialize/deserialize mismatch for null variant "
        << TypeKindName::toName(kind);
    auto nullVectorExpr = createVectorExpr(createNullConstantVector(type));
    serialized = nullVectorExpr->serialize();
    deserialized = ConstantTypedExpr::create(serialized, pool_.get());
    EXPECT_TRUE(*nullVectorExpr == *deserialized)
        << "Serialize/deserialize mismatch for null vector "
        << TypeKindName::toName(kind);

    // non-null values
    for (const auto& value : testValues.nonNullValues) {
      auto variantExpr = std::make_shared<ConstantTypedExpr>(type, value);
      serialized = variantExpr->serialize();
      deserialized = ConstantTypedExpr::create(serialized, pool_.get());
      EXPECT_TRUE(*variantExpr == *deserialized)
          << "Serialize/deserialize mismatch for variant "
          << TypeKindName::toName(kind);

      auto vectorExpr = createVectorExpr(
          BaseVector::createConstant(type, value, 1, pool_.get()));
      serialized = vectorExpr->serialize();
      deserialized = ConstantTypedExpr::create(serialized, pool_.get());
      EXPECT_TRUE(*vectorExpr == *deserialized)
          << "Serialize/deserialize mismatch for vector "
          << TypeKindName::toName(kind) << " with value " << value.toJson(type);
    }
  }
}

TEST_F(ConstantTypedExprTest, serdeComplexTypes) {
  // ARRAY
  auto arrayType = ARRAY(INTEGER());

  // null values
  auto nullArrayVariantExpr = createNullVariantExpr(arrayType);
  auto serialized = nullArrayVariantExpr->serialize();
  auto deserialized = ConstantTypedExpr::create(serialized, nullptr);
  EXPECT_TRUE(*nullArrayVariantExpr == *deserialized)
      << "Serialize/deserialize mismatch for null ARRAY variant";
  auto nullArrayVectorExpr =
      createVectorExpr(createNullConstantVector(arrayType));
  serialized = nullArrayVectorExpr->serialize();
  deserialized = ConstantTypedExpr::create(serialized, pool_.get());
  EXPECT_TRUE(*nullArrayVectorExpr == *deserialized)
      << "Serialize/deserialize mismatch for null ARRAY vector";

  // non-null values
  auto arrayVariant = Variant::array({1, 2, 3});
  auto arrayVariantExpr =
      std::make_shared<ConstantTypedExpr>(arrayType, arrayVariant);
  serialized = arrayVariantExpr->serialize();
  deserialized = ConstantTypedExpr::create(serialized, nullptr);
  EXPECT_TRUE(*arrayVariantExpr == *deserialized)
      << "Serialize/deserialize mismatch for ARRAY variant with data";
  auto arrayVector = makeArrayVector<int32_t>({{1, 2, 3}});
  auto arrayVectorExpr = createVectorExpr(arrayVector);
  serialized = arrayVectorExpr->serialize();
  deserialized = ConstantTypedExpr::create(serialized, pool_.get());
  EXPECT_TRUE(*arrayVectorExpr == *deserialized)
      << "Serialize/deserialize mismatch for ARRAY vector with data";

  // MAP
  auto mapType = MAP(VARCHAR(), INTEGER());
  // null values
  auto nullMapVariantExpr = createNullVariantExpr(mapType);
  serialized = nullMapVariantExpr->serialize();
  deserialized = ConstantTypedExpr::create(serialized, nullptr);
  EXPECT_TRUE(*nullMapVariantExpr == *deserialized)
      << "Serialize/deserialize mismatch for null MAP variant";

  // non-null values
  std::map<Variant, Variant> mapData = {{"key1", 1}, {"key2", 2}};
  auto mapVariant = Variant::map(mapData);
  auto mapVariantExpr =
      std::make_shared<ConstantTypedExpr>(mapType, mapVariant);
  serialized = mapVariantExpr->serialize();
  deserialized = ConstantTypedExpr::create(serialized, nullptr);
  EXPECT_TRUE(*mapVariantExpr == *deserialized)
      << "Serialize/deserialize mismatch for MAP variant with data";

  // ROW
  auto rowType = ROW({{"a", INTEGER()}, {"b", VARCHAR()}});
  // null values
  auto nullRowVariantExpr = createNullVariantExpr(rowType);
  serialized = nullRowVariantExpr->serialize();
  deserialized = ConstantTypedExpr::create(serialized, nullptr);
  EXPECT_TRUE(*nullRowVariantExpr == *deserialized)
      << "Serialize/deserialize mismatch for null ROW variant";

  // non-null values
  auto rowVariant = Variant::row({42, "hello"});
  auto rowVariantExpr =
      std::make_shared<ConstantTypedExpr>(rowType, rowVariant);
  serialized = rowVariantExpr->serialize();
  deserialized = ConstantTypedExpr::create(serialized, nullptr);
  EXPECT_TRUE(*rowVariantExpr == *deserialized)
      << "Serialize/deserialize mismatch for ROW variant with data";

  // OPAQUE
  auto opaqueType = OPAQUE<TestOpaqueStruct>();

  // null values
  auto nullOpaqueVariantExpr = createNullVariantExpr(opaqueType);
  serialized = nullOpaqueVariantExpr->serialize();
  deserialized = ConstantTypedExpr::create(serialized, nullptr);
  EXPECT_TRUE(*nullOpaqueVariantExpr == *deserialized)
      << "Serialize/deserialize mismatch for null OPAQUE variant";
  auto nullOpaqueVectorExpr =
      createVectorExpr(createNullConstantVector(opaqueType));
  serialized = nullOpaqueVectorExpr->serialize();
  deserialized = ConstantTypedExpr::create(serialized, pool_.get());
  EXPECT_TRUE(*nullOpaqueVectorExpr == *deserialized)
      << "Serialize/deserialize mismatch for null OPAQUE vector";

  // non-null values
  auto testObj = std::make_shared<TestOpaqueStruct>(42, "test_data");
  auto opaqueVariant = Variant::opaque(testObj);
  auto opaqueVariantExpr =
      std::make_shared<ConstantTypedExpr>(opaqueType, opaqueVariant);
  serialized = opaqueVariantExpr->serialize();
  deserialized = ConstantTypedExpr::create(serialized, nullptr);
  auto actualObj = static_pointer_cast<const ConstantTypedExpr>(deserialized)
                       ->value()
                       .value<TypeKind::OPAQUE>()
                       .obj;
  EXPECT_EQ(*testObj, *static_pointer_cast<TestOpaqueStruct>(actualObj));
}

TEST_F(ConstantTypedExprTest, toStringScalarTypes) {
  for (auto kind : scalarTypes_) {
    auto type = createScalarType(kind);
    auto testValues = getTestValues(kind);

    // null values
    auto nullVariantExpr = createNullVariantExpr(type);
    auto nullVectorExpr = createVectorExpr(createNullConstantVector(type));
    EXPECT_EQ(nullVariantExpr->toString(), nullVectorExpr->toString())
        << "toString mismatch for null " << TypeKindName::toName(kind);

    // non-null values
    for (const auto& value : testValues.nonNullValues) {
      auto variantExpr = std::make_shared<ConstantTypedExpr>(type, value);
      auto vectorExpr = createVectorExpr(
          BaseVector::createConstant(type, value, 1, pool_.get()));
      EXPECT_EQ(variantExpr->toString(), vectorExpr->toString())
          << "toString mismatch for " << TypeKindName::toName(kind)
          << " with value " << value.toJson(type);
    }
  }
}

TEST_F(ConstantTypedExprTest, toStringComplexTypes) {
  // ARRAY
  auto arrayType = ARRAY(INTEGER());

  // null values
  auto nullArrayVariantExpr = createNullVariantExpr(arrayType);
  auto nullArrayVectorExpr =
      createVectorExpr(createNullConstantVector(arrayType));
  EXPECT_EQ(nullArrayVariantExpr->toString(), nullArrayVectorExpr->toString())
      << "toString mismatch for null ARRAY";

  // non-null values
  auto arrayVariant = Variant::array({1, 2, 3});
  auto arrayVariantExpr =
      std::make_shared<ConstantTypedExpr>(arrayType, arrayVariant);
  auto arrayVector = makeArrayVector<int32_t>({{1, 2, 3}});
  auto arrayVectorExpr = createVectorExpr(arrayVector);
  EXPECT_EQ(arrayVariantExpr->toString(), arrayVectorExpr->toString())
      << "toString mismatch for ARRAY variant vs vector";

  // MAP
  auto mapType = MAP(VARCHAR(), INTEGER());

  // null values
  auto nullMapVariantExpr = createNullVariantExpr(mapType);
  auto nullMapVectorExpr = createVectorExpr(createNullConstantVector(mapType));
  EXPECT_EQ(nullMapVariantExpr->toString(), nullMapVectorExpr->toString())
      << "toString mismatch for null MAP";

  // non-null values
  std::map<Variant, Variant> mapData = {{"key1", 1}, {"key2", 2}};
  auto mapVariant = Variant::map(mapData);
  auto mapVariantExpr =
      std::make_shared<ConstantTypedExpr>(mapType, mapVariant);
  auto mapVector =
      makeMapVector<std::string, int32_t>({{{"key1", 1}, {"key2", 2}}});
  auto mapVectorExpr = createVectorExpr(mapVector);
  EXPECT_EQ(mapVariantExpr->toString(), mapVectorExpr->toString())
      << "toString mismatch for MAP variant vs vector";

  // ROW
  auto rowType = ROW({{"a", INTEGER()}, {"b", VARCHAR()}});

  // null values
  auto nullRowVariantExpr = createNullVariantExpr(rowType);
  auto nullRowVectorExpr = createVectorExpr(createNullConstantVector(rowType));
  EXPECT_EQ(nullRowVariantExpr->toString(), nullRowVectorExpr->toString())
      << "toString mismatch for null ROW";

  // non-null values
  auto rowVariant = Variant::row({42, "hello"});
  auto rowVariantExpr =
      std::make_shared<ConstantTypedExpr>(rowType, rowVariant);
  auto rowVector = makeRowVector(
      {makeFlatVector<int32_t>({42}), makeFlatVector<std::string>({"hello"})});
  auto rowVectorExpr = createVectorExpr(rowVector);
  EXPECT_EQ(rowVariantExpr->toString(), rowVectorExpr->toString())
      << "toString mismatch for ROW variant vs vector";

  // OPAQUE
  auto opaqueType = OPAQUE<TestOpaqueStruct>();

  // null values
  auto nullOpaqueVariantExpr = createNullVariantExpr(opaqueType);
  auto nullOpaqueVectorExpr =
      createVectorExpr(createNullConstantVector(opaqueType));
  EXPECT_EQ(nullOpaqueVariantExpr->toString(), nullOpaqueVectorExpr->toString())
      << "toString mismatch for null OPAQUE";

  // non-null values
  auto testObj = std::make_shared<TestOpaqueStruct>(42, "test_data");
  auto opaqueVariant = Variant::opaque(testObj);
  auto opaqueVariantExpr =
      std::make_shared<ConstantTypedExpr>(opaqueType, opaqueVariant);
  auto opaqueVectorExpr = createVectorExpr(
      BaseVector::createConstant(opaqueType, opaqueVariant, 1, pool_.get()));
  EXPECT_EQ(opaqueVariantExpr->toString(), opaqueVectorExpr->toString())
      << "toString mismatch for OPAQUE variant vs vector";
}

} // namespace facebook::velox::core::test
