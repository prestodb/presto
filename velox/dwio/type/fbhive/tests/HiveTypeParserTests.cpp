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
#include "velox/dwio/type/fbhive/HiveTypeParser.h"

using facebook::velox::TypeKind;

namespace facebook {
namespace velox {
namespace dwio {
namespace type {
namespace fbhive {

template <TypeKind KIND>
void validate(const char* str) {
  HiveTypeParser parser;
  auto t = parser.parse(str);
  ASSERT_EQ(t->kind(), KIND);
}

TEST(FbHive, TypeParserPrimitive) {
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

TEST(FbHive, Map) {
  HiveTypeParser parser;
  auto t = parser.parse("map<int, bigint>");
  ASSERT_EQ(t->kind(), TypeKind::MAP);
  ASSERT_EQ(t->size(), 2);
  ASSERT_EQ(t->childAt(0)->kind(), TypeKind::INTEGER);
  ASSERT_EQ(t->childAt(1)->kind(), TypeKind::BIGINT);
  ASSERT_EQ(t->toString(), "MAP<INTEGER,BIGINT>");
}

TEST(FbHive, List) {
  HiveTypeParser parser;
  auto t = parser.parse("array<bigint>");
  ASSERT_EQ(t->toString(), "ARRAY<BIGINT>");
}

TEST(FbHive, StructNames) {
  HiveTypeParser parser;
  auto t = parser.parse("struct< foo : bigint , int : int, zoo : float>");
  ASSERT_EQ(t->toString(), "ROW<foo:BIGINT,int:INTEGER,zoo:REAL>");

  t = parser.parse("struct<_a:int,b_2:float,c3:double,4d:string,5:int>");
  ASSERT_EQ(
      t->toString(),
      "ROW<_a:INTEGER,b_2:REAL,c3:DOUBLE,\"4d\":VARCHAR,\"5\":INTEGER>");
}

TEST(FbHive, UnionDeprecation) {
  HiveTypeParser parser;
  try {
    parser.parse("uniontype< bigint , int, float>");
  } catch (const std::invalid_argument& e) {
    EXPECT_STREQ(
        "Unexpected token uniontype at < bigint , int, float>", e.what());
  }
}

TEST(FbHive, Nested2) {
  HiveTypeParser parser;
  auto t = parser.parse("array<map<bigint, float>>");
  ASSERT_EQ(t->toString(), "ARRAY<MAP<BIGINT,REAL>>");
}

TEST(FbHive, BadParse) {
  HiveTypeParser parser;
  ASSERT_THROW(parser.parse("   "), std::invalid_argument);
  ASSERT_THROW(
      parser.parse("uniontype< bigint , int, float"), std::invalid_argument);
  ASSERT_THROW(parser.parse("badid"), std::invalid_argument);
  ASSERT_THROW(parser.parse("struct<int, bigint>"), std::invalid_argument);
  ASSERT_THROW(parser.parse("list<>"), std::invalid_argument);
  ASSERT_THROW(parser.parse("map<>"), std::invalid_argument);
  ASSERT_THROW(parser.parse("uniontype<>"), std::invalid_argument);
  ASSERT_THROW(parser.parse("list<int, bigint>"), std::invalid_argument);
  ASSERT_THROW(parser.parse("map<int>"), std::invalid_argument);
  ASSERT_THROW(parser.parse("map<int, bigint, float>"), std::invalid_argument);
}

TEST(FbHive, CaseInsensitive) {
  HiveTypeParser parser;
  auto t = parser.parse("STRUCT<a:INT,b:ARRAY<DOUBLE>,c:MAP<STRING,INT>>");
  ASSERT_EQ(
      t->toString(), "ROW<a:INTEGER,b:ARRAY<DOUBLE>,c:MAP<VARCHAR,INTEGER>>");
}

TEST(FbHive, ParseTypeToString) {
  HiveTypeParser parser;
  auto t = parser.parse("struct<a:int,b:string,c:binary,d:float>");
  ASSERT_EQ(t->toString(), "ROW<a:INTEGER,b:VARCHAR,c:VARBINARY,d:REAL>");
  auto t2 = parser.parse(t->toString());
  ASSERT_EQ(*t, *t2);
}

} // namespace fbhive
} // namespace type
} // namespace dwio
} // namespace velox
} // namespace facebook
