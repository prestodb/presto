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

#include "velox/expression/signature_parser/SignatureParser.h"

using namespace facebook::velox::exec;

namespace facebook::velox {
namespace {

std::string roundTrip(const std::string& typeSignature) {
  return parseTypeSignature(typeSignature).toString();
}

void testScalarType(const std::string& typeSignature) {
  auto signature = parseTypeSignature(typeSignature);
  ASSERT_EQ(signature.baseName(), typeSignature);
  ASSERT_EQ(signature.parameters().size(), 0);
}

class CustomType : public VarcharType {
 public:
  CustomType() = default;

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }
};

static const TypePtr& JSON() {
  static const TypePtr instance{std::make_shared<CustomType>()};
  return instance;
}

static const TypePtr& TIMESTAMP_WITH_TIME_ZONE() {
  static const TypePtr instance{std::make_shared<CustomType>()};
  return instance;
}

class TypeFactories : public CustomTypeFactories {
 public:
  TypeFactories(const TypePtr& type) : type_(type) {}

  TypePtr getType() const override {
    return type_;
  }

  exec::CastOperatorPtr getCastOperator() const override {
    return nullptr;
  }

 private:
  TypePtr type_;
};

class ParseTypeSignatureTest : public ::testing::Test {
 private:
  void SetUp() override {
    // Register custom types with and without spaces.
    registerCustomType("json", std::make_unique<const TypeFactories>(JSON()));
    registerCustomType(
        "timestamp with time zone",
        std::make_unique<const TypeFactories>(TIMESTAMP_WITH_TIME_ZONE()));
  }
};

TEST_F(ParseTypeSignatureTest, scalar) {
  testScalarType("tinyint");
  testScalarType("smallint");
  testScalarType("integer");
  testScalarType("bigint");
  testScalarType("real");
  testScalarType("double");
  testScalarType("timestamp");
}

TEST_F(ParseTypeSignatureTest, decimal) {
  {
    auto signature = parseTypeSignature("DECIMAL(a_precision, a_scale)");
    ASSERT_EQ(signature.baseName(), "DECIMAL");
    ASSERT_EQ(signature.parameters().size(), 2);

    auto param1 = signature.parameters()[0];
    ASSERT_EQ(param1.baseName(), "a_precision");
    ASSERT_EQ(param1.parameters().size(), 0);

    auto param2 = signature.parameters()[1];
    ASSERT_EQ(param2.baseName(), "a_scale");
    ASSERT_EQ(param2.parameters().size(), 0);
  }
  {
    auto signature = parseTypeSignature("decimal(38, a_scale)");
    ASSERT_EQ(signature.baseName(), "decimal");
    auto param1 = signature.parameters()[0];
    ASSERT_EQ(param1.baseName(), "38");
    ASSERT_EQ(param1.parameters().size(), 0);

    auto param2 = signature.parameters()[1];
    ASSERT_EQ(param2.baseName(), "a_scale");
    ASSERT_EQ(param2.parameters().size(), 0);
  }
}

TEST_F(ParseTypeSignatureTest, array) {
  {
    auto signature = parseTypeSignature("array(bigint)");
    ASSERT_EQ(signature.baseName(), "array");
    ASSERT_EQ(signature.parameters().size(), 1);

    auto param = signature.parameters()[0];
    ASSERT_EQ(param.baseName(), "bigint");
    ASSERT_EQ(param.parameters().size(), 0);
  }
  {
    auto signature = parseTypeSignature("array(timestamp with time zone)");
    ASSERT_EQ(signature.baseName(), "array");
    ASSERT_EQ(signature.parameters().size(), 1);

    auto param = signature.parameters()[0];
    ASSERT_EQ(param.baseName(), "timestamp with time zone");
    ASSERT_EQ(param.parameters().size(), 0);
  }
  {
    auto signature = parseTypeSignature(
        "array(row(timestamp timestamp with time zone, decimal(a_scale, 14)))");
    ASSERT_EQ(signature.baseName(), "array");
    ASSERT_EQ(signature.parameters().size(), 1);

    auto param = signature.parameters()[0];
    ASSERT_EQ(param.baseName(), "row");
    ASSERT_EQ(param.parameters().size(), 2);

    auto field1 = param.parameters()[0];
    ASSERT_EQ(field1.baseName(), "timestamp with time zone");
    ASSERT_EQ(field1.rowFieldName(), "timestamp");
    ASSERT_EQ(field1.parameters().size(), 0);

    auto field2 = param.parameters()[1];
    ASSERT_EQ(field2.baseName(), "decimal");
    ASSERT_EQ(field2.parameters().size(), 2);

    auto decimalarg1 = field2.parameters()[0];
    ASSERT_EQ(decimalarg1.baseName(), "a_scale");
    ASSERT_EQ(decimalarg1.parameters().size(), 0);

    auto decimalarg2 = field2.parameters()[1];
    ASSERT_EQ(decimalarg2.baseName(), "14");
    ASSERT_EQ(decimalarg2.parameters().size(), 0);
  }
}

TEST_F(ParseTypeSignatureTest, map) {
  auto signature = parseTypeSignature("map(interval day to second, double)");
  ASSERT_EQ(signature.baseName(), "map");
  ASSERT_EQ(signature.parameters().size(), 2);

  auto key = signature.parameters()[0];
  ASSERT_EQ(key.baseName(), "interval day to second");
  ASSERT_EQ(key.parameters().size(), 0);

  auto value = signature.parameters()[1];
  ASSERT_EQ(value.baseName(), "double");
  ASSERT_EQ(value.parameters().size(), 0);
}

TEST_F(ParseTypeSignatureTest, row) {
  {
    auto signature = parseTypeSignature("row(v)");
    ASSERT_EQ(signature.baseName(), "row");
    ASSERT_EQ(signature.parameters().size(), 1);
  }

  // test quoted row fields.
  {
    auto signature = parseTypeSignature("row(\"12col0\" v, l)");
    ASSERT_EQ(signature.baseName(), "row");
    ASSERT_EQ(signature.parameters().size(), 2);
    ASSERT_FALSE(signature.rowFieldName().has_value());
    auto field1 = signature.parameters()[0];
    ASSERT_EQ(field1.baseName(), "v");
    ASSERT_EQ(field1.rowFieldName(), "12col0");
    ASSERT_EQ(field1.parameters().size(), 0);

    auto field2 = signature.parameters()[1];
    ASSERT_EQ(field2.baseName(), "l");
    ASSERT_EQ(field2.parameters().size(), 0);
  }

  {
    auto signature = parseTypeSignature(
        "row(\"field 0\" array(json), \"field 1\" row(bla varchar))");
    ASSERT_EQ(signature.baseName(), "row");
    ASSERT_EQ(signature.parameters().size(), 2);
    ASSERT_FALSE(signature.rowFieldName().has_value());
    auto field0 = signature.parameters()[0];
    ASSERT_EQ(field0.baseName(), "array");
    ASSERT_EQ(field0.rowFieldName(), "field 0");
    ASSERT_EQ(field0.parameters().size(), 1);
    auto arrayElement = field0.parameters()[0];
    ASSERT_EQ(arrayElement.baseName(), "json");

    auto field1 = signature.parameters()[1];
    ASSERT_EQ(field1.baseName(), "row");
    ASSERT_EQ(field1.rowFieldName(), "field 1");
    ASSERT_EQ(field1.parameters().size(), 1);

    auto rowfield = field1.parameters()[0];
    ASSERT_EQ(rowfield.baseName(), "varchar");
    ASSERT_EQ(rowfield.rowFieldName(), "bla");
    ASSERT_EQ(rowfield.parameters().size(), 0);
  }
}

TEST_F(ParseTypeSignatureTest, roundTrip) {
  ASSERT_EQ(roundTrip("bigint"), "bigint");

  ASSERT_EQ(roundTrip("array(T)"), "array(T)");
  ASSERT_EQ(roundTrip("array(array(T))"), "array(array(T))");
  ASSERT_EQ(roundTrip("array(row(K,V))"), "array(row(K,V))");

  ASSERT_EQ(roundTrip("map(K,V)"), "map(K,V)");

  ASSERT_EQ(roundTrip("function(S,R)"), "function(S,R)");

  // Test a complex type as the second field in a row
  ASSERT_EQ(
      roundTrip("row(map(K,V),map(bigint,array(double)))"),
      "row(map(K,V),map(bigint,array(double)))");
}

TEST_F(ParseTypeSignatureTest, invalidSignatures) {
  EXPECT_THROW(parseTypeSignature("array(varchar"), VeloxRuntimeError);
  EXPECT_THROW(parseTypeSignature("array(array(T)"), VeloxRuntimeError);
}

} // namespace
} // namespace facebook::velox
