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
#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::velox;

namespace facebook::presto {
namespace {

class TestTypeSignature : public ::testing::Test {};

TEST_F(TestTypeSignature, booleanType) {
  ASSERT_EQ(*parseTypeSignature("boolean"), *BOOLEAN());
}

TEST_F(TestTypeSignature, integerType) {
  ASSERT_EQ(*parseTypeSignature("int"), *INTEGER());
  ASSERT_EQ(*parseTypeSignature("integer"), *INTEGER());
}

TEST_F(TestTypeSignature, varcharType) {
  ASSERT_EQ(*parseTypeSignature("varchar"), *VARCHAR());
}

TEST_F(TestTypeSignature, varbinary) {
  ASSERT_EQ(*parseTypeSignature("varbinary"), *VARBINARY());
}

TEST_F(TestTypeSignature, arrayType) {
  ASSERT_EQ(*parseTypeSignature("array(bigint)"), *ARRAY(BIGINT()));

  ASSERT_EQ(*parseTypeSignature("array(int)"), *ARRAY(INTEGER()));
  ASSERT_EQ(*parseTypeSignature("array(integer)"), *ARRAY(INTEGER()));

  ASSERT_EQ(
      *parseTypeSignature("array(array(bigint))"), *ARRAY(ARRAY(BIGINT())));

  ASSERT_EQ(*parseTypeSignature("array(array(int))"), *ARRAY(ARRAY(INTEGER())));
}

TEST_F(TestTypeSignature, mapType) {
  ASSERT_EQ(
      *parseTypeSignature("map(bigint,bigint)"), *MAP(BIGINT(), BIGINT()));

  ASSERT_EQ(
      *parseTypeSignature("map(bigint,array(bigint))"),
      *MAP(BIGINT(), ARRAY(BIGINT())));

  ASSERT_EQ(
      *parseTypeSignature("map(bigint,map(bigint,map(varchar,bigint)))"),
      *MAP(BIGINT(), MAP(BIGINT(), MAP(VARCHAR(), BIGINT()))));
}

TEST_F(TestTypeSignature, invalidType) {
  VELOX_ASSERT_THROW(
      parseTypeSignature("blah()"), "Failed to parse type [blah()]");

  VELOX_ASSERT_THROW(
      parseTypeSignature("array()"), "Failed to parse type [array()]");

  VELOX_ASSERT_THROW(
      parseTypeSignature("map()"), "Failed to parse type [map()]");

  VELOX_ASSERT_THROW(parseTypeSignature("x"), "Failed to parse type [x]");

  // Ensure this is not treated as a row type.
  VELOX_ASSERT_THROW(
      parseTypeSignature("rowxxx(a)"), "Failed to parse type [rowxxx(a)]");
}

TEST_F(TestTypeSignature, rowType) {
  ASSERT_EQ(
      *parseTypeSignature("row(a bigint,b varchar,c real)"),
      *ROW({"a", "b", "c"}, {BIGINT(), VARCHAR(), REAL()}));

  ASSERT_EQ(
      *parseTypeSignature("row(a bigint,b array(bigint),c row(a bigint))"),
      *ROW(
          {"a", "b", "c"},
          {BIGINT(), ARRAY(BIGINT()), ROW({"a"}, {BIGINT()})}));

  ASSERT_EQ(
      *parseTypeSignature("row(\"12\" bigint,b bigint,c bigint)"),
      *ROW({"12", "b", "c"}, {BIGINT(), BIGINT(), BIGINT()}));

  ASSERT_EQ(
      *parseTypeSignature("row(a varchar(10),b row(a bigint))"),
      *ROW({"a", "b"}, {VARCHAR(), ROW({"a"}, {BIGINT()})}));

  ASSERT_EQ(
      *parseTypeSignature("array(row(col0 bigint,col1 double))"),
      *ARRAY(ROW({"col0", "col1"}, {BIGINT(), DOUBLE()})));

  ASSERT_EQ(
      *parseTypeSignature("row(col0 array(row(col0 bigint,col1 double)))"),
      *ROW({"col0"}, {ARRAY(ROW({"col0", "col1"}, {BIGINT(), DOUBLE()}))}));

  ASSERT_EQ(
      *parseTypeSignature("row(bigint,varchar)"), *ROW({BIGINT(), VARCHAR()}));

  ASSERT_EQ(
      *parseTypeSignature("row(bigint,array(bigint),row(a bigint))"),
      *ROW({BIGINT(), ARRAY(BIGINT()), ROW({"a"}, {BIGINT()})}));

  ASSERT_EQ(
      *parseTypeSignature("row(varchar(10),b row(bigint))"),
      *ROW({"", "b"}, {VARCHAR(), ROW({BIGINT()})}));

  ASSERT_EQ(
      *parseTypeSignature("array(row(col0 bigint,double))"),
      *ARRAY(ROW({"col0", ""}, {BIGINT(), DOUBLE()})));

  ASSERT_EQ(
      *parseTypeSignature("row(col0 array(row(bigint,double)))"),
      *ROW({"col0"}, {ARRAY(ROW({BIGINT(), DOUBLE()}))}));

  ASSERT_EQ(
      *parseTypeSignature("row(double double precision)"),
      *ROW({"double"}, {DOUBLE()}));

  ASSERT_EQ(*parseTypeSignature("row(double precision)"), *ROW({DOUBLE()}));

  ASSERT_EQ(
      *parseTypeSignature("RoW(a bigint,b varchar)"),
      *ROW({"a", "b"}, {BIGINT(), VARCHAR()}));

  // Field type canonicalization.
  ASSERT_EQ(*parseTypeSignature("row(col iNt)"), *ROW({"col"}, {INTEGER()}));
}

TEST_F(TestTypeSignature, typesWithSpaces) {
  VELOX_ASSERT_THROW(
      parseTypeSignature("row(time time with time zone)"),
      "Specified element is not found : TIME WITH TIME ZONE");

  ASSERT_EQ(
      *parseTypeSignature("row(double double precision)"),
      *ROW({"double"}, {DOUBLE()}));

  VELOX_ASSERT_THROW(
      parseTypeSignature("row(time with time zone)"),
      "Specified element is not found : TIME WITH TIME ZONE");

  ASSERT_EQ(*parseTypeSignature("row(double precision)"), *ROW({DOUBLE()}));

  VELOX_ASSERT_THROW(
      parseTypeSignature("row(array(time with time zone))"),
      "Specified element is not found : TIME WITH TIME ZONE");

  // quoted field names
  VELOX_ASSERT_THROW(
      parseTypeSignature(
          "row(\"time with time zone\" time with time zone,\"double\" double)"),
      "Specified element is not found : TIME WITH TIME ZONE");
}

TEST_F(TestTypeSignature, intervalYearToMonthType) {
  ASSERT_EQ(
      *parseTypeSignature("row(interval interval year to month)"),
      *ROW({"interval"}, {INTERVAL_YEAR_MONTH()}));

  ASSERT_EQ(
      *parseTypeSignature("row(interval year to month)"),
      *ROW({INTERVAL_YEAR_MONTH()}));
}

TEST_F(TestTypeSignature, functionType) {
  ASSERT_EQ(
      *parseTypeSignature("function(bigint,bigint,bigint)"),
      *FUNCTION({BIGINT(), BIGINT()}, BIGINT()));
  ASSERT_EQ(
      *parseTypeSignature("function(bigint,array(varchar),varchar)"),
      *FUNCTION({BIGINT(), ARRAY(VARCHAR())}, VARCHAR()));
}

TEST_F(TestTypeSignature, decimalType) {
  ASSERT_EQ(*parseTypeSignature("decimal(10, 5)"), *DECIMAL(10, 5));
  ASSERT_EQ(*parseTypeSignature("decimal(20,10)"), *DECIMAL(20, 10));

  VELOX_ASSERT_THROW(
      parseTypeSignature("decimal"), "Failed to parse type [decimal]");
  VELOX_ASSERT_THROW(
      parseTypeSignature("decimal()"), "Failed to parse type [decimal()]");
  VELOX_ASSERT_THROW(
      parseTypeSignature("decimal(20)"), "Failed to parse type [decimal(20)]");
  VELOX_ASSERT_THROW(
      parseTypeSignature("decimal(, 20)"),
      "Failed to parse type [decimal(, 20)]");
}

} // namespace
} // namespace facebook::presto
