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

#include <memory>
#include <stdexcept>

#include "gtest/gtest-message.h"
#include "gtest/gtest-test-part.h"
#include "gtest/gtest.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/type/fbhive/HiveTypeParser.h"

using facebook::velox::TypeKind;

namespace facebook::velox::dwio::type::fbhive {

template <TypeKind KIND>
void validate(const char* str) {
  HiveTypeParser parser;
  auto t = parser.parse(str);
  ASSERT_EQ(t->kind(), KIND);
}

TEST(FbHive, typeParserPrimitive) {
  HiveTypeParser parser;
  validate<TypeKind::BOOLEAN>("boolean");
  validate<TypeKind::TINYINT>("tinyint");
  validate<TypeKind::SMALLINT>("smallint");
  validate<TypeKind::INTEGER>("int");
  validate<TypeKind::BIGINT>("bigint");
  validate<TypeKind::REAL>("float");
  validate<TypeKind::DOUBLE>("double");

  validate<TypeKind::INTEGER>("   int  ");
}

TEST(FbHive, map) {
  HiveTypeParser parser;
  auto t = parser.parse("map<int, bigint>");
  ASSERT_EQ(t->kind(), TypeKind::MAP);
  ASSERT_EQ(t->size(), 2);
  ASSERT_EQ(t->childAt(0)->kind(), TypeKind::INTEGER);
  ASSERT_EQ(t->childAt(1)->kind(), TypeKind::BIGINT);
  ASSERT_EQ(t->toString(), "MAP<INTEGER,BIGINT>");
}

TEST(FbHive, shortDecimal) {
  HiveTypeParser parser;
  auto t = parser.parse("short_decimal(10, 5)");
  ASSERT_EQ(t->kind(), TypeKind::SHORT_DECIMAL);
  auto type = t->asShortDecimal();
  ASSERT_EQ(type.precision(), 10);
  ASSERT_EQ(type.scale(), 5);
  ASSERT_EQ(t->toString(), "SHORT_DECIMAL(10,5)");
}

TEST(FbHive, longDecimal) {
  HiveTypeParser parser;
  auto t = parser.parse("long_decimal(20, 5)");
  ASSERT_EQ(t->kind(), TypeKind::LONG_DECIMAL);
  auto type = t->asLongDecimal();
  ASSERT_EQ(type.precision(), 20);
  ASSERT_EQ(type.scale(), 5);
  ASSERT_EQ(t->toString(), "LONG_DECIMAL(20,5)");
}

TEST(FbHive, list) {
  HiveTypeParser parser;
  auto t = parser.parse("array<bigint>");
  ASSERT_EQ(t->toString(), "ARRAY<BIGINT>");
}

TEST(FbHive, structNames) {
  HiveTypeParser parser;
  auto t = parser.parse("struct< foo : bigint , int : int, zoo : float>");
  ASSERT_EQ(t->toString(), "ROW<foo:BIGINT,int:INTEGER,zoo:REAL>");

  t = parser.parse("struct<_a:int,b_2:float,c3:double,4d:string,5:int>");
  ASSERT_EQ(
      t->toString(),
      "ROW<_a:INTEGER,b_2:REAL,c3:DOUBLE,\"4d\":VARCHAR,\"5\":INTEGER>");
}

TEST(FbHive, unionDeprecation) {
  HiveTypeParser parser;
  VELOX_ASSERT_THROW(
      parser.parse("uniontype< bigint , int, float>"),
      "Unexpected token uniontype at < bigint , int, float>");
  VELOX_ASSERT_THROW(
      parser.parse("struct<a:uniontype<int,string>>"),
      "Unexpected token uniontype at <int,string>>");
  VELOX_ASSERT_THROW(
      parser.parse(
          "struct<a:map<int,array<struct<a:map<string,int>,b:array<int>,c:uniontype<int,float>>>>>"),
      "Unexpected token uniontype at <int,float>>>>>");
}

TEST(FbHive, nested2) {
  HiveTypeParser parser;
  auto t = parser.parse("array<map<bigint, float>>");
  ASSERT_EQ(t->toString(), "ARRAY<MAP<BIGINT,REAL>>");
}

TEST(FbHive, badParse) {
  HiveTypeParser parser;
  VELOX_ASSERT_THROW(
      parser.parse("   "), "Unexpected end of stream parsing type!!!");
  VELOX_ASSERT_THROW(
      parser.parse("uniontype< bigint , int, float"),
      "Unexpected token uniontype at < bigint , int, float");
  VELOX_ASSERT_THROW(parser.parse("badid"), "Unexpected token badid at ");
  VELOX_ASSERT_THROW(
      parser.parse("struct<int, bigint>"), "Unexpected token  bigint>");
  VELOX_ASSERT_THROW(parser.parse("list<>"), "Unexpected token list at <>");
  VELOX_ASSERT_THROW(
      parser.parse("map<>"), "wrong param count for map type def");
  VELOX_ASSERT_THROW(
      parser.parse("uniontype<>"), "Unexpected token uniontype at <>");
  VELOX_ASSERT_THROW(
      parser.parse("list<int, bigint>"),
      "Unexpected token list at <int, bigint>");
  VELOX_ASSERT_THROW(
      parser.parse("map<int>"), "wrong param count for map type def");
  VELOX_ASSERT_THROW(
      parser.parse("short_decimal<20, 10>"), "Unexpected token 20, 10>");
  VELOX_ASSERT_THROW(
      parser.parse("short_decimal(20, 10>"), "Unexpected token ");
  VELOX_ASSERT_THROW(
      parser.parse("short_decimal(a, 10)"),
      "Decimal precision must be a positive integer");
  VELOX_ASSERT_THROW(
      parser.parse("long_decimal(20, b)"),
      "Decimal scale must be a positive integer");
}

TEST(FbHive, caseInsensitive) {
  HiveTypeParser parser;
  auto t = parser.parse("STRUCT<a:INT,b:ARRAY<DOUBLE>,c:MAP<STRING,INT>>");
  ASSERT_EQ(
      t->toString(), "ROW<a:INTEGER,b:ARRAY<DOUBLE>,c:MAP<VARCHAR,INTEGER>>");
}

TEST(FbHive, parseTypeToString) {
  HiveTypeParser parser;
  auto t = parser.parse("struct<a:int,b:string,c:binary,d:float>");
  ASSERT_EQ(t->toString(), "ROW<a:INTEGER,b:VARCHAR,c:VARBINARY,d:REAL>");
  auto t2 = parser.parse(t->toString());
  ASSERT_EQ(*t, *t2);
}

TEST(FbHive, parseSpecialChar) {
  HiveTypeParser parser;
  auto t = parser.parse("struct<a$_#:int>");
  ASSERT_EQ(t->toString(), "ROW<\"a$_#\":INTEGER>");
}

} // namespace facebook::velox::dwio::type::fbhive
