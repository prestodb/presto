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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/types/BigintEnumRegistration.h"
#include "velox/functions/prestosql/types/BigintEnumType.h"
#include "velox/functions/prestosql/types/parser/TypeParser.h"

namespace facebook::velox::functions::prestosql {
namespace {

class CustomType : public VarcharType {
 public:
  CustomType() = default;

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }
};

static const TypePtr& JSON() {
  static const TypePtr instance{new CustomType()};
  return instance;
}

static const TypePtr& TIMESTAMP_WITH_TIME_ZONE() {
  static const TypePtr instance{new CustomType()};
  return instance;
}

class TypeFactory : public CustomTypeFactory {
 public:
  TypeFactory(const TypePtr& type) : type_(type) {}

  TypePtr getType(
      const std::vector<TypeParameter>& /*parameters*/) const override {
    return type_;
  }

  exec::CastOperatorPtr getCastOperator() const override {
    return nullptr;
  }

  AbstractInputGeneratorPtr getInputGenerator(
      const InputGeneratorConfig& /*config*/) const override {
    return nullptr;
  }

 private:
  TypePtr type_;
};

class TypeParserTest : public ::testing::Test {
 private:
  void SetUp() override {
    // Register custom types with and without spaces.
    registerCustomType("json", std::make_unique<const TypeFactory>(JSON()));
    registerCustomType(
        "timestamp with time zone",
        std::make_unique<const TypeFactory>(TIMESTAMP_WITH_TIME_ZONE()));
  }
};

TEST_F(TypeParserTest, booleanType) {
  ASSERT_EQ(*parseType("boolean"), *BOOLEAN());
}

TEST_F(TypeParserTest, integerType) {
  ASSERT_EQ(*parseType("int"), *INTEGER());
  ASSERT_EQ(*parseType("integer"), *INTEGER());
}

TEST_F(TypeParserTest, varcharType) {
  ASSERT_EQ(*parseType("varchar"), *VARCHAR());
  ASSERT_EQ(*parseType("varchar(4)"), *VARCHAR());
}

TEST_F(TypeParserTest, charType) {
  VELOX_ASSERT_UNSUPPORTED_THROW(parseType("char"), "");
  VELOX_ASSERT_UNSUPPORTED_THROW(parseType("char(4)"), "");
}

TEST_F(TypeParserTest, varbinary) {
  ASSERT_EQ(*parseType("varbinary"), *VARBINARY());
}

TEST_F(TypeParserTest, arrayType) {
  ASSERT_EQ(*parseType("array(bigint)"), *ARRAY(BIGINT()));

  ASSERT_EQ(*parseType("array(int)"), *ARRAY(INTEGER()));
  ASSERT_EQ(*parseType("array(integer)"), *ARRAY(INTEGER()));

  ASSERT_EQ(*parseType("array(array(bigint))"), *ARRAY(ARRAY(BIGINT())));

  ASSERT_EQ(*parseType("array(array(int))"), *ARRAY(ARRAY(INTEGER())));

  ASSERT_EQ(
      *parseType("array(timestamp with time zone)"),
      *ARRAY(TIMESTAMP_WITH_TIME_ZONE()));

  ASSERT_EQ(*parseType("array(DECIMAL(10,5))"), *ARRAY(DECIMAL(10, 5)));
}

TEST_F(TypeParserTest, mapType) {
  ASSERT_EQ(*parseType("map(bigint,bigint)"), *MAP(BIGINT(), BIGINT()));

  ASSERT_EQ(
      *parseType("map(timestamp with time zone,bigint)"),
      *MAP(TIMESTAMP_WITH_TIME_ZONE(), BIGINT()));

  ASSERT_EQ(
      *parseType("map(timestamp with time zone, timestamp with time zone)"),
      *MAP(TIMESTAMP_WITH_TIME_ZONE(), TIMESTAMP_WITH_TIME_ZONE()));

  ASSERT_EQ(
      *parseType("map(json, timestamp with time zone)"),
      *MAP(JSON(), TIMESTAMP_WITH_TIME_ZONE()));

  ASSERT_EQ(
      *parseType("map(bigint,array(bigint))"), *MAP(BIGINT(), ARRAY(BIGINT())));

  ASSERT_EQ(
      *parseType("map(timestamp with time zone, varchar)"),
      *MAP(TIMESTAMP_WITH_TIME_ZONE(), VARCHAR()));

  ASSERT_EQ(
      *parseType("map(bigint,map(bigint,map(varchar,bigint)))"),
      *MAP(BIGINT(), MAP(BIGINT(), MAP(VARCHAR(), BIGINT()))));

  ASSERT_EQ(
      *parseType("maP(DECIMAL(10,5), DECIMAL(20, 4))"),
      *MAP(DECIMAL(10, 5), DECIMAL(20, 4)));

  // Complex types as map keys.
  ASSERT_EQ(
      *parseType("map(row(bigint),bigint)"), *MAP(ROW({BIGINT()}), BIGINT()));

  ASSERT_EQ(
      *parseType("map(array(double),bigint)"), *MAP(ARRAY(DOUBLE()), BIGINT()));

  ASSERT_EQ(
      *parseType("map(map(tinyint, varchar),bigint)"),
      *MAP(MAP(TINYINT(), VARCHAR()), BIGINT()));
}

TEST_F(TypeParserTest, invalidType) {
  VELOX_ASSERT_THROW(
      parseType("blah()"),
      "Failed to parse type [blah()]. "
      "syntax error, unexpected RPAREN");

  VELOX_ASSERT_THROW(parseType("array()"), "Failed to parse type [array()]");

  VELOX_ASSERT_THROW(parseType("map()"), "Failed to parse type [map()]");

  VELOX_ASSERT_THROW(parseType("x"), "Failed to parse type [x]");

  // Ensure this is not treated as a row type.
  VELOX_ASSERT_UNSUPPORTED_THROW(
      parseType("rowxxx(a)"), "Failed to parse type [a]. Type not registered.");
}

TEST_F(TypeParserTest, rowType) {
  // Unnamed fields.
  ASSERT_EQ(
      *parseType("row(bigint,varchar, real, timestamp with time zone)"),
      *ROW({BIGINT(), VARCHAR(), REAL(), TIMESTAMP_WITH_TIME_ZONE()}));

  ASSERT_EQ(
      *parseType("row(a bigint,b varchar,c real)"),
      *ROW({"a", "b", "c"}, {BIGINT(), VARCHAR(), REAL()}));

  ASSERT_EQ(
      *parseType("row(a timestamp with time zone,b json,c real)"),
      *ROW({"a", "b", "c"}, {TIMESTAMP_WITH_TIME_ZONE(), JSON(), REAL()}));

  ASSERT_EQ(
      *parseType("row(a bigint,b array(bigint),c row(a decimal(10,5)))"),
      *ROW(
          {"a", "b", "c"},
          {BIGINT(), ARRAY(BIGINT()), ROW({"a"}, {DECIMAL(10, 5)})}));

  // Quoted field name starting with number and scalar type.
  ASSERT_EQ(
      *parseType("row(\"12 tb\" bigint,b bigint,c bigint)"),
      *ROW({"12 tb", "b", "c"}, {BIGINT(), BIGINT(), BIGINT()}));

  ASSERT_EQ(
      *parseType("row(\"a\" bigint, \"b\" array(varchar), "
                 "\"c\" timestamp with time zone)"),
      *ROW(
          {"a", "b", "c"},
          {BIGINT(), ARRAY(VARCHAR()), TIMESTAMP_WITH_TIME_ZONE()}));

  ASSERT_EQ(
      *parseType("row(a varchar(10),b row(a bigint))"),
      *ROW({"a", "b"}, {VARCHAR(), ROW({"a"}, {BIGINT()})}));

  ASSERT_EQ(
      *parseType("array(row(col0 bigint,col1 double))"),
      *ARRAY(ROW({"col0", "col1"}, {BIGINT(), DOUBLE()})));

  ASSERT_EQ(
      *parseType("row(col0 array(row(col0 bigint,col1 double)))"),
      *ROW({"col0"}, {ARRAY(ROW({"col0", "col1"}, {BIGINT(), DOUBLE()}))}));

  ASSERT_EQ(*parseType("row(bigint,varchar)"), *ROW({BIGINT(), VARCHAR()}));

  ASSERT_EQ(
      *parseType("row(bigint,array(bigint),row(a bigint))"),
      *ROW({BIGINT(), ARRAY(BIGINT()), ROW({"a"}, {BIGINT()})}));

  ASSERT_EQ(
      *parseType("row(varchar(10),b row(bigint))"),
      *ROW({"", "b"}, {VARCHAR(), ROW({BIGINT()})}));

  ASSERT_EQ(
      *parseType("array(row(col0 bigint,double))"),
      *ARRAY(ROW({"col0", ""}, {BIGINT(), DOUBLE()})));

  ASSERT_EQ(
      *parseType("row(col0 array(row(bigint,double)))"),
      *ROW({"col0"}, {ARRAY(ROW({BIGINT(), DOUBLE()}))}));

  ASSERT_EQ(
      *parseType("row(double double precision)"), *ROW({"double"}, {DOUBLE()}));

  ASSERT_EQ(*parseType("row(double precision)"), *ROW({DOUBLE()}));

  ASSERT_EQ(
      *parseType("RoW(a bigint,b varchar)"),
      *ROW({"a", "b"}, {BIGINT(), VARCHAR()}));

  ASSERT_EQ(*parseType("row(array(Json))"), *ROW({ARRAY(JSON())}));

  VELOX_ASSERT_UNSUPPORTED_THROW(
      *parseType("row(col0 row(array(HyperLogLog)))"),
      "Failed to parse type [HyperLogLog]. Type not registered.");

  // Field type canonicalization.
  ASSERT_EQ(*parseType("row(col iNt)"), *ROW({"col"}, {INTEGER()}));

  // Can only have names within rows.
  VELOX_ASSERT_UNSUPPORTED_THROW(
      parseType("asd bigint"),
      "Failed to parse type [asd bigint]. Type not registered.");
}

TEST_F(TypeParserTest, typesWithSpaces) {
  // Type is not registered.
  VELOX_ASSERT_UNSUPPORTED_THROW(
      parseType("row(time time with time zone)"),
      "Failed to parse type [time with time zone]. Type not registered.");

  ASSERT_EQ(
      *parseType("timestamp with time zone"), *TIMESTAMP_WITH_TIME_ZONE());

  // Type is registered.
  ASSERT_EQ(
      *parseType("row(col0 timestamp with time zone)"),
      *ROW({"col0"}, {TIMESTAMP_WITH_TIME_ZONE()}));

  ASSERT_EQ(
      *parseType("row(double double precision)"), *ROW({"double"}, {DOUBLE()}));

  VELOX_ASSERT_THROW(
      parseType("row(time with time zone)"),
      "Failed to parse type [with time zone]");

  ASSERT_EQ(*parseType("row(double precision)"), *ROW({DOUBLE()}));

  ASSERT_EQ(
      *parseType("row(INTERval DAY TO SECOND)"), *ROW({INTERVAL_DAY_TIME()}));

  ASSERT_EQ(
      *parseType("row(INTERVAL YEAR TO month)"), *ROW({INTERVAL_YEAR_MONTH()}));

  // quoted field name with valid type with spaces.
  ASSERT_EQ(
      *parseType(
          "row(\"timestamp with time zone\" timestamp with time zone,\"double\" double)"),
      *ROW(
          {"timestamp with time zone", "double"},
          {TIMESTAMP_WITH_TIME_ZONE(), DOUBLE()}));

  // quoted filed name with special characters & spaces
  ASSERT_EQ(
      *parseType("row(\"Nested Some more weirdtt +- \\:\\: charact\" varchar)"),
      *ROW({"Nested Some more weirdtt +- \\:\\: charact"}, {VARCHAR()}));

  // quoted field name with invalid type with spaces.
  VELOX_ASSERT_UNSUPPORTED_THROW(
      parseType(
          "row(\"timestamp with time zone\" timestamp timestamp with time zone)"),
      "Failed to parse type [timestamp timestamp with time zone]. Type not registered.");
}

TEST_F(TypeParserTest, intervalYearToMonthType) {
  ASSERT_EQ(
      *parseType("row(interval interval year to month)"),
      *ROW({"interval"}, {INTERVAL_YEAR_MONTH()}));

  ASSERT_EQ(
      *parseType("row(interval year to month)"), *ROW({INTERVAL_YEAR_MONTH()}));
}

TEST_F(TypeParserTest, functionType) {
  ASSERT_EQ(
      *parseType("function(bigint,bigint,bigint)"),
      *FUNCTION({BIGINT(), BIGINT()}, BIGINT()));
  ASSERT_EQ(
      *parseType("function(bigint,array(varchar),varchar)"),
      *FUNCTION({BIGINT(), ARRAY(VARCHAR())}, VARCHAR()));
}

TEST_F(TypeParserTest, decimalType) {
  ASSERT_EQ(*parseType("decimal(10, 5)"), *DECIMAL(10, 5));
  ASSERT_EQ(*parseType("decimal(20,10)"), *DECIMAL(20, 10));

  VELOX_ASSERT_THROW(parseType("decimal"), "Failed to parse type [decimal]");
  VELOX_ASSERT_THROW(
      parseType("decimal()"), "Failed to parse type [decimal()]");
  VELOX_ASSERT_THROW(
      parseType("decimal(20)"), "Failed to parse type [decimal(20)]");
  VELOX_ASSERT_THROW(
      parseType("decimal(, 20)"), "Failed to parse type [decimal(, 20)]");
}

// Checks that type names can also be field names.
TEST_F(TypeParserTest, fieldNames) {
  ASSERT_EQ(
      *parseType("row(bigint bigint, map bigint, row bigint, array bigint, "
                 "decimal bigint, function bigint, struct bigint, "
                 "varchar map(bigint, tinyint), varbinary array(bigint))"),
      *ROW(
          {"bigint",
           "map",
           "row",
           "array",
           "decimal",
           "function",
           "struct",
           "varchar",
           "varbinary"},
          {BIGINT(),
           BIGINT(),
           BIGINT(),
           BIGINT(),
           BIGINT(),
           BIGINT(),
           BIGINT(),
           MAP(BIGINT(), TINYINT()),
           ARRAY(BIGINT())}));
}

TEST_F(TypeParserTest, enumBasic) {
  registerBigintEnumType();
  LongEnumParameter moodInfo("test.enum.mood", {{"CURIOUS", 2}, {"HAPPY", 0}});
  ASSERT_EQ(
      *parseType(
          "test.enum.mood:BigintEnum(test.enum.mood{\"CURIOUS\":2, \"HAPPY\":0})"),
      *BIGINT_ENUM(moodInfo));

  // Parse negative integers.
  LongEnumParameter moodWithNegativeValue(
      "test.enum.mood", {{"CURIOUS", -2}, {"HAPPY", 0}});
  ASSERT_EQ(
      *parseType(
          "test.enum.mood:BigintEnum(test.enum.mood{\"CURIOUS\":-2, \"HAPPY\":0})"),
      *BIGINT_ENUM(moodWithNegativeValue));

  // Enum name that is not in the form catalog.namespace.enum_name.
  LongEnumParameter otherEnumInfo(
      "someEnumType", {{"CURIOUS", 2}, {"HAPPY", 0}});
  auto otherEnumString =
      "someEnumType:BigintEnum(someEnumType{\"CURIOUS\": 2, \"HAPPY\": 0})";
  ASSERT_EQ(*parseType(otherEnumString), *BIGINT_ENUM(otherEnumInfo));

  // Array type with enum values.
  ASSERT_EQ(
      *parseType(
          "array(test.enum.mood:BigintEnum(test.enum.mood{\"CURIOUS\":2, \"HAPPY\":0}))"),
      *ARRAY(BIGINT_ENUM(moodInfo)));

  // Map type with enum values.
  ASSERT_EQ(
      *parseType(
          "map(test.enum.mood:BigintEnum(test.enum.mood{\"CURIOUS\":-2, \"HAPPY\":0}), bigint)"),
      *MAP(BIGINT_ENUM(moodWithNegativeValue), BIGINT()));
  ASSERT_EQ(
      *parseType(
          "map(bigint,test.enum.mood:BigintEnum(test.enum.mood{\"CURIOUS\":-2, \"HAPPY\":0}))"),
      *MAP(BIGINT(), BIGINT_ENUM(moodWithNegativeValue)));

  // Row type with enum values.
  ASSERT_EQ(
      *parseType(
          "row(test.enum.mood:BigintEnum(test.enum.mood{\"CURIOUS\":-2, \"HAPPY\":0}))"),
      *ROW({BIGINT_ENUM(moodWithNegativeValue)}));
  ASSERT_EQ(
      *parseType(
          "row(c0 test.enum.mood:BigintEnum(test.enum.mood{\"CURIOUS\":-2,\"HAPPY\":0}))"),
      *ROW({"c0"}, {BIGINT_ENUM(moodWithNegativeValue)}));
}

TEST_F(TypeParserTest, invalidEnums) {
  // Duplicate keys.
  VELOX_ASSERT_THROW(
      parseType(
          "test.enum.mood:BigintEnum(test.enum.mood{\"CURIOUS\":-2, \"CURIOUS\":0})"),
      "Failed to parse map: [[\"CURIOUS\",-2], [\"CURIOUS\",0]], duplicate key found: CURIOUS");

  // Duplicate values.
  VELOX_ASSERT_THROW(
      parseType(
          "test.enum.mood:BigintEnum(test.enum.mood{\"CURIOUS\":-2, \"HAPPY\":-2})"),
      "Failed to parse map: [[\"CURIOUS\",-2], [\"HAPPY\",-2]], duplicate value found: -2");

  // Invalid enum type SmallintEnum.
  VELOX_ASSERT_THROW(
      parseType(
          "test.enum.mood:SmallintEnum(test.enum.mood{\"CURIOUS\":-2, \"HAPPY\":0})"),
      "Failed to parse type [test.enum.mood:SmallintEnum(test.enum.mood{\"CURIOUS\":-2, \"HAPPY\":0})]. Invalid type SmallintEnum, expected BigintEnum or VarcharEnum");

  // Invalid format: missing ":"
  VELOX_ASSERT_THROW(
      parseType("testNoColon(test.enum.mood{“CURIOUS”:-2, “HAPPY”:0})"),
      "Failed to parse type [testNoColon(test.enum.mood{“CURIOUS”:-2, “HAPPY”:0})]. syntax error, unexpected LBRACE, expecting COLON");

  // Invalid format: missing "("
  VELOX_ASSERT_THROW(
      parseType(
          "test.enum.mood:BigintEnum(test.enum.moodCURIOUS”:-2, “HAPPY”:0})"),
      "Failed to parse type [test.enum.mood:BigintEnum(test.enum.moodCURIOUS”:-2, “HAPPY”:0})]. syntax error, unexpected COLON, expecting LBRACE");

  // Invalid enum type with non integral values.
  VELOX_ASSERT_THROW(
      parseType(
          "test.enum.mood:BigintEnum(test.enum.mood{\"CURIOUS\": \"varchar value\", \"HAPPY\": \"0\"})"),
      "Failed to parse type [test.enum.mood:BigintEnum(test.enum.mood{\"CURIOUS\": \"varchar value\", \"HAPPY\": \"0\"})]. syntax error, unexpected QUOTED_ID, expecting NUMBER or SIGNED_INT");
  VELOX_ASSERT_THROW(
      parseType(
          "test.enum.mood:BigintEnum(test.enum.mood{\"CURIOUS\": 1.1, \"HAPPY\": -1.0})"),
      "Failed to parse type [test.enum.mood:BigintEnum(test.enum.mood{\"CURIOUS\": 1.1, \"HAPPY\": -1.0})]. syntax error, unexpected PERIOD, expecting COMMA or RBRACE");
}

} // namespace
} // namespace facebook::velox::functions::prestosql
