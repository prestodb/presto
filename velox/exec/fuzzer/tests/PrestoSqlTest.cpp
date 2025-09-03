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
#include "velox/exec/fuzzer/PrestoSql.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/functions/prestosql/types/QDigestType.h"
#include "velox/functions/prestosql/types/TDigestType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

namespace facebook::velox::exec::test {
namespace {

TEST(PrestoSqlTest, toTypeSql) {
  EXPECT_EQ(toTypeSql(BOOLEAN()), "BOOLEAN");
  EXPECT_EQ(toTypeSql(TINYINT()), "TINYINT");
  EXPECT_EQ(toTypeSql(SMALLINT()), "SMALLINT");
  EXPECT_EQ(toTypeSql(INTEGER()), "INTEGER");
  EXPECT_EQ(toTypeSql(BIGINT()), "BIGINT");
  EXPECT_EQ(toTypeSql(REAL()), "REAL");
  EXPECT_EQ(toTypeSql(DOUBLE()), "DOUBLE");
  EXPECT_EQ(toTypeSql(VARCHAR()), "VARCHAR");
  EXPECT_EQ(toTypeSql(VARBINARY()), "VARBINARY");
  EXPECT_EQ(toTypeSql(TDIGEST(DOUBLE())), "TDIGEST(DOUBLE)");
  EXPECT_EQ(toTypeSql(TIMESTAMP()), "TIMESTAMP");
  EXPECT_EQ(toTypeSql(QDIGEST(DOUBLE())), "QDIGEST(DOUBLE)");
  EXPECT_EQ(toTypeSql(QDIGEST(BIGINT())), "QDIGEST(BIGINT)");
  EXPECT_EQ(toTypeSql(QDIGEST(REAL())), "QDIGEST(REAL)");
  EXPECT_EQ(toTypeSql(DATE()), "DATE");
  EXPECT_EQ(toTypeSql(TIMESTAMP_WITH_TIME_ZONE()), "TIMESTAMP WITH TIME ZONE");
  EXPECT_EQ(toTypeSql(ARRAY(BOOLEAN())), "ARRAY(BOOLEAN)");
  EXPECT_EQ(toTypeSql(MAP(BOOLEAN(), INTEGER())), "MAP(BOOLEAN, INTEGER)");
  EXPECT_EQ(
      toTypeSql(ROW({{"a", BOOLEAN()}, {"b", INTEGER()}})),
      "ROW(a BOOLEAN, b INTEGER)");
  EXPECT_EQ(
      toTypeSql(
          ROW({{"a_", BOOLEAN()}, {"b$", INTEGER()}, {"c d", INTEGER()}})),
      "ROW(a_ BOOLEAN, b$ INTEGER, c d INTEGER)");
  EXPECT_EQ(toTypeSql(JSON()), "JSON");
  EXPECT_EQ(toTypeSql(UNKNOWN()), "UNKNOWN");
  VELOX_ASSERT_THROW(
      toTypeSql(FUNCTION({INTEGER()}, INTEGER())),
      "Type is not supported: FUNCTION");
}

void toUnaryOperator(
    const std::string& operatorName,
    const std::string& expectedSql) {
  auto expression = std::make_shared<core::CallTypedExpr>(
      INTEGER(),
      operatorName,
      std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c0"));
  EXPECT_EQ(toCallSql(expression), expectedSql);
}

void toBinaryOperator(
    const std::string& operatorName,
    const std::string& expectedSql) {
  auto expression = std::make_shared<core::CallTypedExpr>(
      INTEGER(),
      operatorName,
      std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"),
      std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c1"));
  EXPECT_EQ(toCallSql(expression), expectedSql);
}

void toIsNullOrIsNotNull(
    const std::string& operatorName,
    const std::string& expectedSql) {
  auto expression = std::make_shared<core::CallTypedExpr>(
      BOOLEAN(),
      operatorName,
      std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"));
  EXPECT_EQ(toCallSql(expression), expectedSql);
}

TEST(PrestoSqlTest, toCallSql) {
  // Unary operators
  toUnaryOperator("negate", "(- c0)");
  toUnaryOperator("not", "(not c0)");
  VELOX_ASSERT_THROW(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          INTEGER(),
          "not",
          std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c0"),
          std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c1"))),
      "Expected one argument to a unary operator");

  // Binary operators
  toBinaryOperator("plus", "(c0 + c1)");
  toBinaryOperator("subtract", "(c0 - c1)");
  toBinaryOperator("minus", "(c0 - c1)");
  toBinaryOperator("multiply", "(c0 * c1)");
  toBinaryOperator("divide", "(c0 / c1)");
  toBinaryOperator("eq", "(c0 = c1)");
  toBinaryOperator("neq", "(c0 <> c1)");
  toBinaryOperator("lt", "(c0 < c1)");
  toBinaryOperator("gt", "(c0 > c1)");
  toBinaryOperator("lte", "(c0 <= c1)");
  toBinaryOperator("gte", "(c0 >= c1)");
  toBinaryOperator("distinct_from", "(c0 is distinct from c1)");
  VELOX_ASSERT_THROW(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          INTEGER(),
          "plus",
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"),
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c1"),
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c3"))),
      "Expected two arguments to a binary operator");

  // Functions IS NULL and NOT NULL
  toIsNullOrIsNotNull("is_null", "(c0 is null)");
  toIsNullOrIsNotNull("not_null", "(c0 is not null)");
  VELOX_ASSERT_THROW(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          "is_null",
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"),
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c1"))),
      "Expected one argument to function 'is_null' or 'not_null'");

  // Function IN
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          "in",
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "a"),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "b"))),
      "'a' in ('b')");
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          "in",
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "a"),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "b"),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "c"),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "d"))),
      "'a' in ('b', 'c', 'd')");
  VELOX_ASSERT_THROW(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          "in",
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "a"))),
      "Expected at least two arguments to function 'in'");

  // Function LIKE
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          "like",
          std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c0"),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "a"))),
      "(c0 like 'a')");
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          "like",
          std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c0"),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "a"),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "b"))),
      "(c0 like 'a' escape 'b')");
  VELOX_ASSERT_THROW(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          "like",
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "a"))),
      "Expected at least two arguments to function 'like'");
  VELOX_ASSERT_THROW(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          "like",
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "a"),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "b"),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "c"),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "d"))),
      "Expected at most three arguments to function 'like'");

  // Functions OR and AND
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          "or",
          std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), true),
          std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), false))),
      "(BOOLEAN 'true' or BOOLEAN 'false')");
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          "and",
          std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), true),
          std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), false))),
      "(BOOLEAN 'true' and BOOLEAN 'false')");
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          "or",
          std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), true),
          std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), false),
          std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), true),
          std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), false))),
      "(BOOLEAN 'true' or BOOLEAN 'false' or BOOLEAN 'true' or BOOLEAN 'false')");
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          "and",
          std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), true),
          std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), false),
          std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), true),
          std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), false))),
      "(BOOLEAN 'true' and BOOLEAN 'false' and BOOLEAN 'true' and BOOLEAN 'false')");
  VELOX_ASSERT_THROW(
      toCallSql(std::make_shared<core::CallTypedExpr>(BOOLEAN(), "or")),
      "Expected at least two arguments to function 'or' or 'and'");

  // Functions ARRAY_CONSTRUCTOR and ROW_CONSTRUCTOR
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          ARRAY(INTEGER()),
          "array_constructor",
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "a"),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "b"),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "c"))),
      "ARRAY['a', 'b', 'c']");
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          ARRAY(INTEGER()), "array_constructor")),
      "ARRAY[]");
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          "row_constructor",
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "a"),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "b"),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "c"),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "d"))),
      "row('a', 'b', 'c', 'd')");
  VELOX_ASSERT_THROW(
      toCallSql(
          std::make_shared<core::CallTypedExpr>(BOOLEAN(), "row_constructor")),
      "Expected at least one argument to function 'row_constructor'");

  // Function BETWEEN
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          "between",
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"),
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c1"),
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c2"))),
      "(c0 between c1 and c2)");
  // Edge case check for ambiguous parantheses processing, query will fail
  // without the parantheses wrapping the left-hand side.
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          "lt",
          std::make_shared<core::CallTypedExpr>(
              BOOLEAN(),
              "between",
              std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"),
              std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"),
              std::make_shared<core::ConstantTypedExpr>(
                  INTEGER(), variant::null(TypeKind::INTEGER))),
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"))),
      "((c0 between c0 and cast(null as INTEGER)) < c0)");
  VELOX_ASSERT_THROW(
      toCallSql(std::make_shared<core::CallTypedExpr>(BOOLEAN(), "between")),
      "Expected three arguments to function 'between'");

  // Function SUBSCRIPT, builds '[]' SQL
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          INTEGER(),
          "subscript",
          std::make_shared<core::FieldAccessTypedExpr>(
              ARRAY(INTEGER()), "array"),
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"))),
      "array[c0]");
  VELOX_ASSERT_THROW(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          INTEGER(),
          "subscript",
          std::make_shared<core::FieldAccessTypedExpr>(
              ARRAY(INTEGER()), "array"),
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"),
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c1"))),
      "Expected two arguments to function 'subscript'");

  // Function SWITCH, builds 'CASE WHEN ... THEN ... ELSE ... END' SQL
  // SWITCH cases with no ELSE.
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          INTEGER(),
          "switch",
          std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "c0"),
          std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c1"))),
      "case when c0 then c1 end");
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          INTEGER(),
          "switch",
          std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "c0"),
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c1"),
          std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "c2"),
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c3"))),
      "case when c0 then c1 when c2 then c3 end");
  // SWITCH case with ELSE.
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          INTEGER(),
          "switch",
          std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "c0"),
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c1"),
          std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "c2"),
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c3"),
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c4"))),
      "case when c0 then c1 when c2 then c3 else c4 end");
  VELOX_ASSERT_THROW(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          INTEGER(),
          "switch",
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"))),
      "Expected at least two arguments to function 'switch'");

  // Generic functions
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(
          INTEGER(),
          "array_top_n",
          std::make_shared<core::FieldAccessTypedExpr>(ARRAY(INTEGER()), "c0"),
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c1"))),
      "array_top_n(c0, c1)");
  EXPECT_EQ(
      toCallSql(std::make_shared<core::CallTypedExpr>(REAL(), "infinity")),
      "infinity()");
}

TEST(PrestoSqlTest, toConcatSql) {
  auto expression = core::ConcatTypedExpr(
      {"field0", "field1"},
      std::vector<core::TypedExprPtr>{
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "a"),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), "b")});

  EXPECT_EQ(
      toConcatSql(expression),
      "cast(row('a', 'b') as ROW(field0 VARCHAR, field1 VARCHAR))");
}

TEST(PrestoSqlTest, toCallInputsSql) {
  std::stringstream sql;
  auto expression = std::make_shared<core::FieldAccessTypedExpr>(
      VARCHAR(),
      std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c0"),
      "field0");

  toCallInputsSql({expression}, sql);
  EXPECT_EQ(sql.str(), "c0.field0");
}

TEST(PrestoSqlTest, toConstantSql) {
  EXPECT_EQ(
      toConstantSql(core::ConstantTypedExpr(INTERVAL_YEAR_MONTH(), 123)),
      "INTERVAL '123' YEAR TO MONTH");
  EXPECT_EQ(
      toConstantSql(core::ConstantTypedExpr(INTERVAL_DAY_TIME(), int64_t(123))),
      "INTERVAL '123' DAY TO SECOND");
}

} // namespace
} // namespace facebook::velox::exec::test
