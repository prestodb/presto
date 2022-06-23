/*
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

#include "presto_cpp/main/types/ParseTypeSignature.h"
#include "presto_cpp/main/types/TypeSignatureTypeConverter.h"
#include "velox/type/Type.h"

using namespace facebook::presto;
using namespace facebook::velox;

template <typename... T>
TypePtr rowSignature(T const&... elements) {
  std::vector<NamedType> types = {elements...};
  return rowFromNamedTypes(types);
}

TypePtr signature(std::string typeName) {
  return typeFromString(typeName);
}

NamedType namedParameter(std::string name, TypePtr type) {
  return NamedType{name, type};
}

NamedType namedParameter(std::string name, bool _unused, TypePtr type) {
  return namedParameter(name, type);
}

NamedType unnamedParameter(TypePtr type) {
  return NamedType{"", type};
}

TypePtr varchar() {
  return typeFromString("VARCHAR");
}

TypePtr varchar(int size) {
  return typeFromString("VARCHAR");
}

TypePtr array(TypePtr type) {
  return arrayFromType(type);
}
TypePtr map(TypePtr key, TypePtr value) {
  return mapFromKeyValueType(key, value);
}

#define ASSERT_THROWS_CONTAINS_MESSAGE(statement, exception, message) \
  try {                                                               \
    statement FAIL();                                                 \
  } catch (const exception& err) {                                    \
    EXPECT_PRED_FORMAT2(testing::IsSubstring, message, err.what());   \
  }

#define assertRowSignature(input, expected) \
  ASSERT_EQ(parseTypeSignature(input)->toString(), expected->toString());
#define assertSignature(input, expected) \
  ASSERT_EQ(parseTypeSignature(input)->toString(), expected)
#define assertSignatureFail(input) \
  ASSERT_ANY_THROW(parseTypeSignature(input)->toString();)
#define assertEquals(input, expected) \
  ASSERT_EQ(input->toString(), expected->toString())

// Checks that exception error message contains the given message.
#define assertRowSignatureContainsThrows(input, expected, exception, message) \
  ASSERT_THROWS_CONTAINS_MESSAGE(parseTypeSignature(input)->toString();       \
                                 , exception, message)

class TestTypeSignature : public ::testing::Test {};

TEST_F(TestTypeSignature, sig01) {
  assertSignature("boolean", "BOOLEAN");
}

TEST_F(TestTypeSignature, sig02) {
  assertSignature("varchar", "VARCHAR");
}

TEST_F(TestTypeSignature, sig03) {
  assertEquals(parseTypeSignature("int"), parseTypeSignature("integer"));
}

TEST_F(TestTypeSignature, sig04) {
  assertSignature("array(bigint)", "ARRAY<BIGINT>");
}

TEST_F(TestTypeSignature, sig05) {
  assertEquals(
      parseTypeSignature("array(int)"), parseTypeSignature("array(integer)"));
}

TEST_F(TestTypeSignature, sig06) {
  assertSignature("array(array(bigint))", "ARRAY<ARRAY<BIGINT>>");
}

TEST_F(TestTypeSignature, sig07) {
  assertEquals(
      parseTypeSignature("array(array(int))"),
      parseTypeSignature("array(array(integer))"));
}

TEST_F(TestTypeSignature, sig08) {
  assertSignature("map(bigint,bigint)", "MAP<BIGINT,BIGINT>");
}

TEST_F(TestTypeSignature, sig09) {
  assertSignature("map(bigint,array(bigint))", "MAP<BIGINT,ARRAY<BIGINT>>");
}

TEST_F(TestTypeSignature, sig10) {
  assertSignature(
      "map(bigint,map(bigint,map(varchar,bigint)))",
      "MAP<BIGINT,MAP<BIGINT,MAP<VARCHAR,BIGINT>>>");
}

TEST_F(TestTypeSignature, sig11) {
  assertSignatureFail("blah()");
}

TEST_F(TestTypeSignature, sig12) {
  assertSignatureFail("array()");
}

TEST_F(TestTypeSignature, sig13) {
  assertSignatureFail("map()");
}

TEST_F(TestTypeSignature, sig14) {
  assertSignatureFail("x");
}

TEST_F(TestTypeSignature, sig16) {
  // ensure this is not treated as a row type
  assertSignatureFail("rowxxx(a)");
}

TEST_F(TestTypeSignature, TestRow01) {
  assertRowSignature(
      "row(a bigint,b bigint,c bigint)",
      rowSignature(
          namedParameter("a", false, signature("bigint")),
          namedParameter("b", false, signature("bigint")),
          namedParameter("c", false, signature("bigint"))));
}

TEST_F(TestTypeSignature, TestRow02) {
  assertRowSignature(
      "row(a bigint,b array(bigint),c row(a bigint))",
      rowSignature(
          namedParameter("a", false, signature("bigint")),
          namedParameter("b", false, array(signature("bigint"))),
          namedParameter(
              "c",
              false,
              rowSignature(namedParameter("a", false, signature("bigint"))))));
}

// row signature with named fields
TEST_F(TestTypeSignature, row03) {
  assertRowSignature(
      "row(a bigint,b varchar)",
      rowSignature(
          namedParameter("a", false, signature("bigint")),
          namedParameter("b", false, varchar())));
}

TEST_F(TestTypeSignature, row04) {
  // Wondering about this test of '_varchar' ??
  // assertRowSignature(
  //        "row(__a__ bigint,_b@_: _varchar)",
  //        rowSignature(namedParameter("__a__", false, signature("bigint")),
  //        namedParameter("_b@_:", false, signature("_varchar"))));
}

TEST_F(TestTypeSignature, row05) {
  assertRowSignature(
      "row(a bigint,b array(bigint),c row(a bigint))",
      rowSignature(
          namedParameter("a", false, signature("bigint")),
          namedParameter("b", false, array(signature("bigint"))),
          namedParameter(
              "c",
              false,
              rowSignature(namedParameter("a", false, signature("bigint"))))));
}

TEST_F(TestTypeSignature, row06) {
  assertRowSignature(
      "row(a varchar(10),b row(a bigint))",
      rowSignature(
          namedParameter("a", false, varchar(10)),
          namedParameter(
              "b",
              false,
              rowSignature(namedParameter("a", false, signature("bigint"))))));
}

TEST_F(TestTypeSignature, row07) {
  assertRowSignature(
      "array(row(col0 bigint,col1 double))",
      array(rowSignature(
          namedParameter("col0", false, signature("bigint")),
          namedParameter("col1", false, signature("double")))));
}

TEST_F(TestTypeSignature, row08) {
  assertRowSignature(
      "row(col0 array(row(col0 bigint,col1 double)))",
      rowSignature(namedParameter(
          "col0",
          false,
          array(rowSignature(
              namedParameter("col0", false, signature("bigint")),
              namedParameter("col1", false, signature("double")))))));
}

TEST_F(TestTypeSignature, row09) {
  // row with mixed fields
  assertRowSignature(
      "row(bigint,varchar)",
      rowSignature(
          unnamedParameter(signature("bigint")), unnamedParameter(varchar())));
}

TEST_F(TestTypeSignature, row10) {
  assertRowSignature(
      "row(bigint,array(bigint),row(a bigint))",
      rowSignature(
          unnamedParameter(signature("bigint")),
          unnamedParameter(array(signature("bigint"))),
          unnamedParameter(
              rowSignature(namedParameter("a", false, signature("bigint"))))));
}

TEST_F(TestTypeSignature, row11) {
  assertRowSignature(
      "row(varchar(10),b row(bigint))",
      rowSignature(
          unnamedParameter(varchar(10)),
          namedParameter(
              "b",
              false,
              rowSignature(unnamedParameter(signature("bigint"))))));
}

TEST_F(TestTypeSignature, row12) {
  assertRowSignature(
      "array(row(col0 bigint,double))",
      array(rowSignature(
          namedParameter("col0", false, signature("bigint")),
          unnamedParameter(signature("double")))));
}

TEST_F(TestTypeSignature, row13) {
  assertRowSignature(
      "row(col0 array(row(bigint,double)))",
      rowSignature(namedParameter(
          "col0",
          false,
          array(rowSignature(
              unnamedParameter(signature("bigint")),
              unnamedParameter(signature("double")))))));
}

TEST_F(TestTypeSignature, row14) {
  assertRowSignature(
      "row(double double precision)",
      rowSignature(
          namedParameter("double", false, signature("double precision"))));
}

TEST_F(TestTypeSignature, row15) {
  assertRowSignature(
      "row(double precision)",
      rowSignature(unnamedParameter(signature("double precision"))));
}

TEST_F(TestTypeSignature, row16) {
  // preserve base name case
  assertRowSignature(
      "RoW(a bigint,b varchar)",
      rowSignature(
          namedParameter("a", false, signature("bigint")),
          namedParameter("b", false, varchar())));
}

TEST_F(TestTypeSignature, row17) {
  // field type canonicalization
  assertEquals(
      parseTypeSignature("row(col iNt)"),
      parseTypeSignature("row(col integer)"));
}

// TEST_F(TestTypeSignature, row18) {
// assertEquals(parseTypeSignature("row(a Int(p1))"), parseTypeSignature("row(a
// integer(p1))"));

// signature with invalid type
// assertRowSignature(
//        "row(\"time\" with time zone)",
//        rowSignature(namedParameter("time", true, signature("with time
//        zone"))));
//}

// The TestSpacesXX tests all throw failures.  The parser succeeds by the
// resulting types are not supported by Koski.
//
TEST_F(TestTypeSignature, spaces01) {
  // named fields of types with spaces
  assertRowSignatureContainsThrows(
      "row(time time with time zone)",
      rowSignature(
          namedParameter("time", false, signature("time with time zone"))),
      VeloxUserError,
      "Specified element is not found : TIME WITH TIME ZONE");
}

TEST_F(TestTypeSignature, spaces04) {
  assertRowSignatureContainsThrows(
      "row(interval interval year to month)",
      rowSignature(namedParameter(
          "interval", false, signature("interval year to month"))),
      VeloxUserError,
      "Specified element is not found : INTERVAL YEAR TO MONTH");
}

TEST_F(TestTypeSignature, spaces05) {
  assertRowSignature(
      "row(double double precision)",
      rowSignature(
          namedParameter("double", false, signature("double precision"))));
}

TEST_F(TestTypeSignature, spaces06) {
  // unnamed fields of types with spaces
  assertRowSignatureContainsThrows(
      "row(time with time zone)",
      rowSignature(unnamedParameter(signature("time with time zone"))),
      VeloxUserError,
      "Specified element is not found : TIME WITH TIME ZONE");
}

TEST_F(TestTypeSignature, spaces09) {
  assertRowSignatureContainsThrows(
      "row(interval year to month)",
      rowSignature(unnamedParameter(signature("interval year to month"))),
      VeloxUserError,
      "Specified element is not found : INTERVAL YEAR TO MONTH");
}

TEST_F(TestTypeSignature, spaces10) {
  assertRowSignature(
      "row(double precision)",
      rowSignature(unnamedParameter(signature("double precision"))));
}

TEST_F(TestTypeSignature, spaces11) {
  assertRowSignatureContainsThrows(
      "row(array(time with time zone))",
      rowSignature(unnamedParameter(array(signature("time with time zone")))),
      VeloxUserError,
      "Specified element is not found : TIME WITH TIME ZONE");
}

TEST_F(TestTypeSignature, spaces13) {
  // quoted field names
  assertRowSignatureContainsThrows(
      "row(\"time with time zone\" time with time zone,\"double\" double)",
      rowSignature(
          namedParameter(
              "time with time zone", true, signature("time with time zone")),
          namedParameter("double", true, signature("double"))),
      VeloxUserError,
      "Specified element is not found : TIME WITH TIME ZONE");
}

TEST_F(TestTypeSignature, functionType) {
  ASSERT_THROWS_CONTAINS_MESSAGE(
      parseTypeSignature("function(boolean,varchar(5),boolean)");
      ,
      VeloxUserError,
      "Failed to parse type [function(boolean,varchar(5),boolean)]");
}

TEST_F(TestTypeSignature, decimalType) {
  assertSignature("decimal(10, 5)", "SHORT_DECIMAL(10,5)");
  assertSignature("decimal(20,10)", "LONG_DECIMAL(20,10)");
  ASSERT_THROWS_CONTAINS_MESSAGE(
      parseTypeSignature("decimal");
      , VeloxUserError, "Failed to parse type [decimal]");
  ASSERT_THROWS_CONTAINS_MESSAGE(
      parseTypeSignature("decimal()");
      , VeloxUserError, "Failed to parse type [decimal()]");
  ASSERT_THROWS_CONTAINS_MESSAGE(
      parseTypeSignature("decimal(20)");
      , VeloxUserError, "Failed to parse type [decimal(20)]");
  ASSERT_THROWS_CONTAINS_MESSAGE(
      parseTypeSignature("decimal(, 20)");
      , VeloxUserError, "Failed to parse type [decimal(, 20)]");
}
