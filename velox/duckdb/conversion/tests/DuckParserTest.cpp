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
#include "velox/duckdb/conversion/DuckParser.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/PlanNode.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/parse/Expressions.h"

using namespace facebook::velox;
using namespace facebook::velox::duckdb;

namespace {
std::shared_ptr<const core::IExpr> parseExpr(const std::string& exprString) {
  ParseOptions options;
  return parseExpr(exprString, options);
}
} // namespace

TEST(DuckParserTest, constants) {
  // Integers.
  EXPECT_EQ("0", parseExpr("0")->toString());
  EXPECT_EQ("100", parseExpr("100")->toString());
  EXPECT_EQ("-787", parseExpr("-787")->toString());
  EXPECT_EQ(
      "9223372036854775807", parseExpr("9223372036854775807")->toString());

  // Doubles.
  EXPECT_EQ("1.99", parseExpr("1.99")->toString());
  EXPECT_EQ("-303.1234", parseExpr("-303.1234")->toString());

  // Strings.
  EXPECT_EQ("\"\"", parseExpr("''")->toString());
  EXPECT_EQ("\"hello world\"", parseExpr("'hello world'")->toString());

  // Nulls
  EXPECT_EQ("null", parseExpr("NULL")->toString());
  EXPECT_EQ("null", parseExpr("NULL::double")->toString());

  // Booleans
  EXPECT_EQ("true", parseExpr("TRUE")->toString());
  EXPECT_EQ("false", parseExpr("FALSE")->toString());
}

TEST(DuckParserTest, arrays) {
  // Literal arrays with different types.
  EXPECT_EQ("[1,2,-33]", parseExpr("ARRAY[1, 2, -33]")->toString());
  EXPECT_EQ(
      "[1.99,-8.3,0.878]", parseExpr("ARRAY[1.99, -8.3, 0.878]")->toString());
  EXPECT_EQ(
      "[\"asd\",\"qwe\",\"ewqq\"]",
      parseExpr("ARRAY['asd', 'qwe', 'ewqq']")->toString());
  EXPECT_EQ("[null,1]", parseExpr("ARRAY[NULL, 1]")->toString());

  // Empty array.
  EXPECT_EQ("[]", parseExpr("ARRAY[]")->toString());

  // Expressions with variables and without.
  EXPECT_EQ(
      "array_constructor(plus(\"x\",\"y\"),"
      "minus(\"y\",10),"
      "multiply(5,\"z\"),"
      "1,"
      "multiply(7,6))",
      parseExpr("ARRAY[x + y, y - 10, 5 * z, 1, 7 * 6]")->toString());

  // Array of variables and one constant.
  EXPECT_EQ(
      "array_constructor(\"x\",\"y\",\"z\",1)",
      parseExpr("ARRAY[x, y, z, 1]")->toString());
}

TEST(DuckParserTest, variables) {
  // Variables.
  EXPECT_EQ("\"c1\"", parseExpr("c1")->toString());
  EXPECT_EQ("\"c1\"", parseExpr("\"c1\"")->toString());
  EXPECT_EQ("dot(\"tbl\",\"c2\")", parseExpr("tbl.c2")->toString());
}

TEST(DuckParserTest, functions) {
  EXPECT_EQ("avg(\"col1\")", parseExpr("avg(col1)")->toString());
  EXPECT_EQ(
      "func(1,3.4,\"str\")", parseExpr("func(1, 3.4, 'str')")->toString());

  // Nested calls.
  EXPECT_EQ(
      "f(100,g(h(3.6)),99)", parseExpr("f(100, g(h(3.6)), 99)")->toString());
}

namespace {
std::string toString(
    const std::vector<
        std::pair<std::shared_ptr<const core::IExpr>, core::SortOrder>>&
        orderBy) {
  std::stringstream out;
  if (!orderBy.empty()) {
    out << "ORDER BY ";
    for (auto i = 0; i < orderBy.size(); ++i) {
      if (i > 0) {
        out << ", ";
      }
      out << orderBy[i].first->toString() << " "
          << orderBy[i].second.toString();
    }
  }

  return out.str();
}

std::string parseAgg(const std::string& expression) {
  ParseOptions options;
  auto aggregateExpr = parseAggregateExpr(expression, options);
  std::stringstream out;
  out << aggregateExpr.expr->toString();

  if (aggregateExpr.distinct) {
    out << " DISTINCT";
  }

  if (!aggregateExpr.orderBy.empty()) {
    out << " " << toString(aggregateExpr.orderBy);
  }

  if (aggregateExpr.maskExpr != nullptr) {
    out << " FILTER " << aggregateExpr.maskExpr->toString();
  }

  return out.str();
}
} // namespace

TEST(DuckParserTest, aggregates) {
  EXPECT_EQ("array_agg(\"x\")", parseAgg("array_agg(x)"));
  EXPECT_EQ(
      "array_agg(\"x\") ORDER BY \"y\" ASC NULLS LAST",
      parseAgg("array_agg(x ORDER BY y)"));
  EXPECT_EQ(
      "array_agg(\"x\") ORDER BY \"y\" DESC NULLS LAST",
      parseAgg("array_agg(x ORDER BY y DESC)"));
  EXPECT_EQ(
      "array_agg(\"x\") ORDER BY \"y\" ASC NULLS FIRST",
      parseAgg("array_agg(x ORDER BY y NULLS FIRST)"));
  EXPECT_EQ(
      "array_agg(\"x\") ORDER BY \"y\" ASC NULLS LAST, \"z\" ASC NULLS LAST",
      parseAgg("array_agg(x ORDER BY y, z)"));
}

TEST(DuckParserTest, aggregatesWithMasks) {
  EXPECT_EQ(
      "array_agg(\"x\") FILTER \"m\"",
      parseAgg("array_agg(x) filter (where m)"));
}

TEST(DuckParserTest, distinctAggregates) {
  EXPECT_EQ("count(\"x\") DISTINCT", parseAgg("count(distinct x)"));
  EXPECT_EQ("count(\"x\",\"y\") DISTINCT", parseAgg("count(distinct x, y)"));
  EXPECT_EQ("sum(\"x\") DISTINCT", parseAgg("sum(distinct x)"));
}

TEST(DuckParserTest, subscript) {
  EXPECT_EQ("subscript(\"c\",0)", parseExpr("c[0]")->toString());
  EXPECT_EQ(
      "subscript(\"col\",plus(10,99))", parseExpr("col[10 + 99]")->toString());
  EXPECT_EQ("subscript(\"m1\",34)", parseExpr("m1[34]")->toString());
  EXPECT_EQ(
      "subscript(\"m2\",\"key str\")", parseExpr("m2['key str']")->toString());

  EXPECT_EQ(
      "subscript(func(),\"key str\")",
      parseExpr("func()['key str']")->toString());
}

TEST(DuckParserTest, coalesce) {
  EXPECT_EQ("coalesce(null,0)", parseExpr("coalesce(NULL, 0)")->toString());
  EXPECT_EQ(
      "coalesce(\"col1\",plus(0,3),\"col2\",1002)",
      parseExpr("coalesce(col1,  0+ 3, col2, 1002)")->toString());
}

TEST(DuckParserTest, in) {
  EXPECT_EQ("in(\"col1\",[1,2,3])", parseExpr("col1 in (1, 2, 3)")->toString());
  EXPECT_EQ(
      "in(\"col1\",[1,2,null,3])",
      parseExpr("col1 in (1, 2, null, 3)")->toString());
  EXPECT_EQ(
      "in(\"col1\",[\"a\",\"b\",\"c\"])",
      parseExpr("col1 in ('a', 'b', 'c')")->toString());
  EXPECT_EQ(
      "in(\"col1\",[\"a\",null,\"b\",\"c\"])",
      parseExpr("col1 in ('a', null, 'b', 'c')")->toString());
}

TEST(DuckParserTest, notin) {
  EXPECT_EQ(
      "not(in(\"col1\",[1,2,3]))",
      parseExpr("col1 not in (1, 2, 3)")->toString());

  EXPECT_EQ(
      "not(in(\"col1\",[1,2,3]))",
      parseExpr("not(col1 in (1, 2, 3))")->toString());

  EXPECT_EQ(
      "not(in(\"col1\",[1,2,null,3]))",
      parseExpr("col1 not in (1, 2, null, 3)")->toString());

  EXPECT_EQ(
      "not(in(\"col1\",[1,2,null,3]))",
      parseExpr("not(col1 in (1, 2, null, 3))")->toString());

  EXPECT_EQ(
      "not(in(\"col1\",[\"a\",\"b\",\"c\"]))",
      parseExpr("col1 not in ('a', 'b', 'c')")->toString());

  EXPECT_EQ(
      "not(in(\"col1\",[\"a\",\"b\",\"c\"]))",
      parseExpr("not(col1 in ('a', 'b', 'c'))")->toString());

  EXPECT_EQ(
      "not(in(\"col1\",[\"a\",null,\"b\",\"c\"]))",
      parseExpr("col1 not in ('a', null, 'b', 'c')")->toString());

  EXPECT_EQ(
      "not(in(\"col1\",[\"a\",null,\"b\",\"c\"]))",
      parseExpr("not(col1 in ('a', null, 'b', 'c'))")->toString());
}

TEST(DuckParserTest, expressions) {
  // Comparisons.
  EXPECT_EQ("eq(1,0)", parseExpr("1 = 0")->toString());
  EXPECT_EQ("neq(1,0)", parseExpr("1 != 0")->toString());
  EXPECT_EQ("neq(1,0)", parseExpr("1 <> 0")->toString());
  EXPECT_EQ("not(1)", parseExpr("!1")->toString());
  EXPECT_EQ("gt(1,0)", parseExpr("1 > 0")->toString());
  EXPECT_EQ("gte(1,0)", parseExpr("1 >= 0")->toString());
  EXPECT_EQ("lt(1,0)", parseExpr("1 < 0")->toString());
  EXPECT_EQ("lte(1,0)", parseExpr("1 <= 0")->toString());
  EXPECT_EQ(
      "distinct_from(1,0)", parseExpr("1 IS DISTINCT FROM 0")->toString());

  // Arithmetic operators.
  EXPECT_EQ("plus(1,0)", parseExpr("1 + 0")->toString());
  EXPECT_EQ("minus(1,0)", parseExpr("1 - 0")->toString());
  EXPECT_EQ("multiply(1,0)", parseExpr("1 * 0")->toString());
  EXPECT_EQ("divide(1,0)", parseExpr("1 / 0")->toString());
  EXPECT_EQ("mod(1,0)", parseExpr("1 % 0")->toString());

  // ANDs and ORs.
  EXPECT_EQ("and(1,0)", parseExpr("1 and 0")->toString());
  EXPECT_EQ("or(1,0)", parseExpr("1 or 0")->toString());

  EXPECT_EQ(
      "and(and(and(1,0),2),3)", parseExpr("1 and 0 and 2 and 3")->toString());
  EXPECT_EQ(
      "or(and(1,0),and(2,3))", parseExpr("1 and 0 or 2 and 3")->toString());

  // NOT.
  EXPECT_EQ("not(1)", parseExpr("not 1")->toString());

  // Mix-and-match.
  EXPECT_EQ(
      "gte(plus(mod(\"c0\",10),0),multiply(f(100),9))",
      parseExpr("c0 % 10 + 0 >= f(100) * 9")->toString());
}

TEST(DuckParserTest, between) {
  EXPECT_EQ("between(\"c0\",0,1)", parseExpr("c0 between 0 and 1")->toString());

  EXPECT_EQ(
      "and(between(\"c0\",0,1),gt(\"c0\",10))",
      parseExpr("c0 between 0 and 1 and c0 > 10")->toString());
}

TEST(DuckParserTest, interval) {
  auto parseInterval = [](const std::string& sql) {
    auto expr =
        std::dynamic_pointer_cast<const core::ConstantExpr>(parseExpr(sql));
    VELOX_CHECK_NOT_NULL(expr);

    auto value =
        INTERVAL_DAY_TIME()->valueToString(expr->value().value<int64_t>());
    if (expr->alias()) {
      return fmt::format("{} AS {}", value, expr->alias().value());
    }

    return value;
  };

  EXPECT_EQ("0 05:00:00.000", parseInterval("INTERVAL 5 HOURS"));
  EXPECT_EQ("0 00:36:00.000", parseInterval("INTERVAL 36 MINUTES"));
  EXPECT_EQ("0 00:00:07.000", parseInterval("INTERVAL 7 SECONDS"));
  EXPECT_EQ("0 00:00:00.123", parseInterval("INTERVAL 123 MILLISECONDS"));

  EXPECT_EQ("0 00:00:12.345", parseInterval("INTERVAL 12345 MILLISECONDS"));
  EXPECT_EQ("0 03:25:45.678", parseInterval("INTERVAL 12345678 MILLISECONDS"));
  EXPECT_EQ("1 03:48:20.100", parseInterval("INTERVAL 100100100 MILLISECONDS"));

  EXPECT_EQ(
      "0 00:00:00.011 AS x", parseInterval("INTERVAL 11 MILLISECONDS AS x"));
}

TEST(DuckParserTest, cast) {
  EXPECT_EQ(
      "cast(\"1\", BIGINT)", parseExpr("cast('1' as bigint)")->toString());
  EXPECT_EQ(
      "cast(0.99, INTEGER)", parseExpr("cast(0.99 as integer)")->toString());
  EXPECT_EQ(
      "cast(0.99, SMALLINT)", parseExpr("cast(0.99 as smallint)")->toString());
  EXPECT_EQ(
      "cast(0.99, TINYINT)", parseExpr("cast(0.99 as tinyint)")->toString());
  EXPECT_EQ("cast(1, DOUBLE)", parseExpr("cast(1 as double)")->toString());
  EXPECT_EQ(
      "cast(\"col1\", REAL)", parseExpr("cast(col1 as real)")->toString());
  EXPECT_EQ(
      "cast(\"col1\", REAL)", parseExpr("cast(col1 as float)")->toString());
  EXPECT_EQ(
      "cast(0.99, VARCHAR)", parseExpr("cast(0.99 as string)")->toString());
  EXPECT_EQ(
      "cast(0.99, VARCHAR)", parseExpr("cast(0.99 as varchar)")->toString());
  // Cast varchar to varbinary produces a varbinary value which is serialized
  // using base64 encoding.
  EXPECT_EQ("\"YWJj\"", parseExpr("cast('abc' as varbinary)")->toString());
  EXPECT_EQ(
      "cast(\"str_col\", TIMESTAMP)",
      parseExpr("cast(str_col as timestamp)")->toString());

  EXPECT_EQ(
      "cast(\"str_col\", DATE)",
      parseExpr("cast(str_col as date)")->toString());

  EXPECT_EQ(
      "cast(\"str_col\", INTERVAL DAY TO SECOND)",
      parseExpr("cast(str_col as interval day to second)")->toString());

  // Unsupported casts for now.
  EXPECT_THROW(parseExpr("cast('2020-01-01' as TIME)"), std::runtime_error);

  // Complex types.
  EXPECT_EQ(
      "cast(\"c0\", ARRAY<BIGINT>)", parseExpr("c0::bigint[]")->toString());
  EXPECT_EQ(
      "cast(\"c0\", ARRAY<BIGINT>)",
      parseExpr("cast(c0 as bigint[])")->toString());

  EXPECT_EQ(
      "cast(\"c0\", MAP<VARCHAR,BIGINT>)",
      parseExpr("c0::map(varchar, bigint)")->toString());
  EXPECT_EQ(
      "cast(\"c0\", MAP<VARCHAR,BIGINT>)",
      parseExpr("cast(c0 as map(varchar, bigint))")->toString());

  EXPECT_EQ(
      "cast(\"c0\", ROW<a:BIGINT,b:REAL,c:VARCHAR>)",
      parseExpr("c0::struct(a bigint, b real, c varchar)")->toString());
  EXPECT_EQ(
      "cast(\"c0\", ROW<a:BIGINT,b:REAL,c:VARCHAR>)",
      parseExpr("cast(c0 as struct(a bigint, b real, c varchar))")->toString());
}

TEST(DuckParserTest, castToJson) {
  registerJsonType();
  EXPECT_EQ("cast(\"c0\", JSON)", parseExpr("cast(c0 as json)")->toString());
  EXPECT_EQ("cast(\"c0\", JSON)", parseExpr("cast(c0 as JSON)")->toString());
}

TEST(DuckParserTest, castToTimestampWithTimeZone) {
  registerTimestampWithTimeZoneType();
  EXPECT_EQ(
      "cast(\"c0\", TIMESTAMP WITH TIME ZONE)",
      parseExpr("cast(c0 as timestamp with time zone)")->toString());
  EXPECT_EQ(
      "cast(\"c0\", TIMESTAMP WITH TIME ZONE)",
      parseExpr("cast(c0 as TIMESTAMP WITH TIME ZONE)")->toString());
}

TEST(DuckParserTest, ifCase) {
  EXPECT_EQ("if(99,1,0)", parseExpr("if(99, 1, 0)")->toString());
  EXPECT_EQ(
      "if(\"a\",plus(\"b\",\"c\"),g(\"d\"))",
      parseExpr("if(a, b + c, g(d))")->toString());

  // CASE statements.
  EXPECT_EQ(
      "if(1,null,0)",
      parseExpr("case when 1 then NULL else 0 end")->toString());
  EXPECT_EQ(
      "if(gt(\"a\",0),1,0)",
      parseExpr("case when a > 0 then 1 else 0 end")->toString());
}

TEST(DuckParserTest, switchCase) {
  EXPECT_EQ(
      "switch(gt(\"a\",0),1,lt(\"a\",0),-1,0)",
      parseExpr("case when a > 0 then 1 when a < 0 then -1 else 0 end")
          ->toString());

  // no "else" clause
  EXPECT_EQ(
      "switch(gt(\"a\",0),1,lt(\"a\",0),-1)",
      parseExpr("case when a > 0 then 1 when a < 0 then -1end")->toString());

  EXPECT_EQ(
      "switch(eq(\"a\",1),\"x\",eq(\"a\",5),\"y\",\"z\")",
      parseExpr("case a when 1 then 'x' when 5 then 'y' else 'z' end")
          ->toString());
}

TEST(DuckParserTest, invalid) {
  // Invalid.
  EXPECT_THROW(parseExpr(""), std::exception);
  EXPECT_THROW(parseExpr(","), std::exception);
  EXPECT_THROW(parseExpr("a +"), std::exception);
  EXPECT_THROW(parseExpr("f(1"), std::exception);

  // Wrong number of expressions.
  VELOX_ASSERT_THROW(parseExpr("1, 10"), "Expected exactly one expression");
  VELOX_ASSERT_THROW(
      parseExpr("a, 99.8, 'c'"), "Expected exactly one expression");
}

TEST(DuckParserTest, isNull) {
  EXPECT_EQ("is_null(\"a\")", parseExpr("a IS NULL")->toString());
}

TEST(DuckParserTest, isNotNull) {
  EXPECT_EQ("not(is_null(\"a\"))", parseExpr("a IS NOT NULL")->toString());
}

TEST(DuckParserTest, structExtract) {
  // struct_extract is not the desired parsed function, but it being handled
  // enables nested dot (e.g. (a).b.c or (a.b).c)
  EXPECT_EQ("dot(\"a\",\"b\")", parseExpr("(a).b")->toString());
  EXPECT_EQ("dot(\"a\",\"b\")", parseExpr("(a.b)")->toString());
  EXPECT_EQ("dot(dot(\"a\",\"b\"),\"c\")", parseExpr("(a).b.c")->toString());
  EXPECT_EQ("dot(dot(\"a\",\"b\"),\"c\")", parseExpr("(a.b).c")->toString());
}

TEST(DuckParserTest, alias) {
  EXPECT_EQ("plus(\"a\",\"b\") AS sum", parseExpr("a + b AS sum")->toString());
  EXPECT_EQ(
      "gt(\"a\",\"b\") AS result", parseExpr("a > b AS result")->toString());
  EXPECT_EQ("2 AS multiplier", parseExpr("2 AS multiplier")->toString());
  EXPECT_EQ(
      "cast(\"a\", DOUBLE) AS a_double",
      parseExpr("cast(a AS DOUBLE) AS a_double")->toString());
  EXPECT_EQ("\"a\" AS b", parseExpr("a AS b")->toString());
}

TEST(DuckParserTest, like) {
  EXPECT_EQ("like(\"name\",\"%b%\")", parseExpr("name LIKE '%b%'")->toString());
  EXPECT_EQ(
      "like(\"name\",\"%#_%\",\"#\")",
      parseExpr("name LIKE '%#_%' ESCAPE '#'")->toString());
}

TEST(DuckParserTest, notLike) {
  EXPECT_EQ(
      "not(like(\"name\",\"%b%\"))",
      parseExpr("name NOT LIKE '%b%'")->toString());
  EXPECT_EQ(
      "not(like(\"name\",\"%#_%\",\"#\"))",
      parseExpr("name NOT LIKE '%#_%' ESCAPE '#'")->toString());
}

TEST(DuckParserTest, count) {
  EXPECT_EQ("count()", parseExpr("count()")->toString());
  EXPECT_EQ("count(1)", parseExpr("count(1)")->toString());
  EXPECT_EQ("count()", parseExpr("count(*)")->toString());
  EXPECT_EQ("count(\"a\")", parseExpr("count(a)")->toString());
}

TEST(DuckParserTest, orderBy) {
  auto parse = [](const auto& expr) {
    auto orderBy = parseOrderByExpr(expr);
    return fmt::format(
        "{} {}", orderBy.first->toString(), orderBy.second.toString());
  };

  EXPECT_EQ("\"c1\" ASC NULLS LAST", parse("c1"));
  EXPECT_EQ("\"c1\" ASC NULLS LAST", parse("c1 ASC"));
  EXPECT_EQ("\"c1\" DESC NULLS LAST", parse("c1 DESC"));

  EXPECT_EQ("\"c1\" ASC NULLS FIRST", parse("c1 NULLS FIRST"));
  EXPECT_EQ("\"c1\" ASC NULLS LAST", parse("c1 NULLS LAST"));

  EXPECT_EQ("\"c1\" ASC NULLS FIRST", parse("c1 ASC NULLS FIRST"));
  EXPECT_EQ("\"c1\" ASC NULLS LAST", parse("c1 ASC NULLS LAST"));
  EXPECT_EQ("\"c1\" DESC NULLS FIRST", parse("c1 DESC NULLS FIRST"));
  EXPECT_EQ("\"c1\" DESC NULLS LAST", parse("c1 DESC NULLS LAST"));
}

namespace {
const std::string windowTypeString(WindowType w) {
  switch (w) {
    case WindowType::kRange:
      return "RANGE";
    case WindowType::kRows:
      return "ROWS";
  }
  VELOX_UNREACHABLE();
}

const std::string boundTypeString(BoundType b) {
  switch (b) {
    case BoundType::kUnboundedPreceding:
      return "UNBOUNDED PRECEDING";
    case BoundType::kUnboundedFollowing:
      return "UNBOUNDED FOLLOWING";
    case BoundType::kPreceding:
      return "PRECEDING";
    case BoundType::kFollowing:
      return "FOLLOWING";
    case BoundType::kCurrentRow:
      return "CURRENT ROW";
  }
  VELOX_UNREACHABLE();
}

const std::string parseWindow(const std::string& expr) {
  ParseOptions options;
  auto windowExpr = parseWindowExpr(expr, options);
  std::string concatPartitions = "";
  int i = 0;
  for (const auto& partition : windowExpr.partitionBy) {
    concatPartitions += partition->toString();
    if (i > 0) {
      concatPartitions += " , ";
    }
    i++;
  }
  auto partitionString = windowExpr.partitionBy.empty()
      ? ""
      : fmt::format("PARTITION BY {}", concatPartitions);

  auto orderByString = toString(windowExpr.orderBy);

  auto frameString = fmt::format(
      "{} BETWEEN {}{} AND{} {}",
      windowTypeString(windowExpr.frame.type),
      (windowExpr.frame.startValue
           ? windowExpr.frame.startValue->toString() + " "
           : ""),
      boundTypeString(windowExpr.frame.startType),
      (windowExpr.frame.endValue ? " " + windowExpr.frame.endValue->toString()
                                 : ""),
      boundTypeString(windowExpr.frame.endType));

  return fmt::format(
      "{} OVER ({} {} {})",
      windowExpr.functionCall->toString(),
      partitionString,
      orderByString,
      frameString);
}
} // namespace

TEST(DuckParserTest, window) {
  EXPECT_EQ(
      "row_number() AS c OVER (PARTITION BY \"a\" ORDER BY \"b\" ASC NULLS LAST"
      " RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
      parseWindow("row_number() over (partition by a order by b) as c"));
  EXPECT_EQ(
      "row_number() AS a OVER (  RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
      parseWindow("row_number() over () as a"));
  EXPECT_EQ(
      "row_number() AS a OVER ( ORDER BY \"b\" ASC NULLS LAST "
      "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
      parseWindow("row_number() over (order by b) as a"));
  EXPECT_EQ(
      "row_number() OVER (PARTITION BY \"a\"  ROWS BETWEEN "
      "UNBOUNDED PRECEDING AND CURRENT ROW)",
      parseWindow(
          "row_number() over (partition by a rows between unbounded preceding and current row)"));
  EXPECT_EQ(
      "row_number() OVER (PARTITION BY \"a\" ORDER BY \"b\" ASC NULLS LAST "
      "ROWS BETWEEN plus(\"a\",10) PRECEDING AND 10 FOLLOWING)",
      parseWindow("row_number() over (partition by a order by b "
                  "rows between a + 10 preceding and 10 following)"));
  EXPECT_EQ(
      "row_number() OVER (PARTITION BY \"a\" ORDER BY \"b\" DESC NULLS FIRST "
      "ROWS BETWEEN plus(\"a\",10) PRECEDING AND 10 FOLLOWING)",
      parseWindow(
          "row_number() over (partition by a order by b desc nulls first "
          "rows between a + 10 preceding and 10 following)"));

  EXPECT_EQ(
      "lead(\"x\",\"y\",\"z\") OVER (  "
      "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
      parseWindow("lead(x, y, z) over ()"));

  EXPECT_EQ(
      "lag(\"x\",3,\"z\") OVER (  "
      "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
      parseWindow("lag(x, 3, z) over ()"));

  EXPECT_EQ(
      "nth_value(\"x\",3) OVER (  "
      "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
      parseWindow("nth_value(x, 3) over ()"));
}

TEST(DuckParserTest, windowWithIntegerConstant) {
  ParseOptions options;
  options.parseIntegerAsBigint = false;
  auto windowExpr = parseWindowExpr("nth_value(x, 3) over ()", options);
  auto func =
      std::dynamic_pointer_cast<const core::CallExpr>(windowExpr.functionCall);
  ASSERT_TRUE(func != nullptr)
      << windowExpr.functionCall->toString() << " is not a call expr";
  EXPECT_EQ(func->getInputs().size(), 2);
  auto param = func->getInputs()[1];
  auto constant = std::dynamic_pointer_cast<const core::ConstantExpr>(param);
  ASSERT_TRUE(constant != nullptr) << param->toString() << " is not a constant";
  EXPECT_EQ(*constant->type(), *INTEGER());
}

TEST(DuckParserTest, invalidExpression) {
  VELOX_ASSERT_THROW(
      parseExpr("func(a b)"),
      "Cannot parse expression: func(a b). Parser Error: syntax error at or near \"b\"");
}

TEST(DuckParserTest, parseDecimalConstant) {
  ParseOptions options;
  options.parseDecimalAsDouble = false;
  auto expr = parseExpr("1.234", options);
  if (auto constant =
          std::dynamic_pointer_cast<const core::ConstantExpr>(expr)) {
    ASSERT_EQ(*constant->type(), *DECIMAL(4, 3));
  } else {
    FAIL() << expr->toString() << " is not a constant";
  }
}

TEST(DuckParserTest, parseInteger) {
  ParseOptions options;
  options.parseIntegerAsBigint = false;
  auto expr = parseExpr("1234", options);
  if (auto constant =
          std::dynamic_pointer_cast<const core::ConstantExpr>(expr)) {
    ASSERT_EQ(*constant->type(), *INTEGER());
  } else {
    FAIL() << expr->toString() << " is not a constant";
  }
}

TEST(DuckParserTest, parseWithPrefix) {
  ParseOptions options;
  options.functionPrefix = "prefix.";
  EXPECT_EQ(
      "prefix.in(\"col1\",[1,2,3])",
      parseExpr("col1 in (1, 2, 3)", options)->toString());
  EXPECT_EQ(
      "prefix.like(\"name\",\"%b%\")",
      parseExpr("name LIKE '%b%'", options)->toString());
  EXPECT_EQ(
      "prefix.not(prefix.like(\"name\",\"%b%\"))",
      parseExpr("name NOT LIKE '%b%'", options)->toString());

  // Arithmetic operators.
  EXPECT_EQ("prefix.plus(1,0)", parseExpr("1 + 0", options)->toString());
  EXPECT_EQ("prefix.minus(1,0)", parseExpr("1 - 0", options)->toString());
  EXPECT_EQ("prefix.multiply(1,0)", parseExpr("1 * 0", options)->toString());
  EXPECT_EQ("prefix.divide(1,0)", parseExpr("1 / 0", options)->toString());
  EXPECT_EQ("prefix.mod(1,0)", parseExpr("1 % 0", options)->toString());

  // Comparisons.
  EXPECT_EQ("prefix.eq(1,0)", parseExpr("1 = 0", options)->toString());
  EXPECT_EQ("prefix.neq(1,0)", parseExpr("1 != 0", options)->toString());
  EXPECT_EQ("prefix.neq(1,0)", parseExpr("1 <> 0", options)->toString());
  EXPECT_EQ("prefix.not(1)", parseExpr("!1", options)->toString());
  EXPECT_EQ("prefix.gt(1,0)", parseExpr("1 > 0", options)->toString());
  EXPECT_EQ("prefix.gte(1,0)", parseExpr("1 >= 0", options)->toString());
  EXPECT_EQ("prefix.lt(1,0)", parseExpr("1 < 0", options)->toString());
  EXPECT_EQ("prefix.lte(1,0)", parseExpr("1 <= 0", options)->toString());
  EXPECT_EQ(
      "prefix.distinct_from(1,0)",
      parseExpr("1 IS DISTINCT FROM 0", options)->toString());
  EXPECT_EQ(
      "prefix.between(\"c0\",0,1)",
      parseExpr("c0 between 0 and 1", options)->toString());

  EXPECT_EQ("prefix.not(1)", parseExpr("not 1", options)->toString());
  EXPECT_EQ("prefix.count()", parseExpr("count()", options)->toString());
  EXPECT_EQ(
      "prefix.array_constructor(1,2,3,4,5)",
      parseExpr("array_constructor(1, 2, 3, 4, 5)", options)->toString());
  EXPECT_EQ(
      "prefix.is_null(\"a\")", parseExpr("a IS NULL", options)->toString());
  EXPECT_EQ(
      "prefix.not(prefix.is_null(\"a\"))",
      parseExpr("a IS NOT NULL", options)->toString());
  EXPECT_EQ(
      "prefix.avg(\"col1\")", parseExpr("avg(col1)", options)->toString());
  EXPECT_EQ(
      "prefix.f(100,prefix.g(prefix.h(3.6)),99)",
      parseExpr("f(100, g(h(3.6)), 99)", options)->toString());
  EXPECT_EQ(
      "prefix.subscript(\"c\",0)", parseExpr("c[0]", options)->toString());

  // Functions which prefix should not be applied to.
  EXPECT_EQ("and(1,0)", parseExpr("1 and 0", options)->toString());
  EXPECT_EQ("or(1,0)", parseExpr("1 or 0", options)->toString());
  EXPECT_EQ("dot(\"a\",\"b\")", parseExpr("(a).b", options)->toString());
  EXPECT_EQ("if(99,1,0)", parseExpr("if(99, 1, 0)", options)->toString());
  EXPECT_EQ(
      "switch(prefix.gt(\"a\",0),1,prefix.lt(\"a\",0),-1,0)",
      parseExpr("case when a > 0 then 1 when a < 0 then -1 else 0 end", options)
          ->toString());
  EXPECT_EQ(
      "coalesce(null,0)", parseExpr("coalesce(NULL, 0)", options)->toString());
  EXPECT_EQ(
      "cast(\"1\", BIGINT)",
      parseExpr("cast('1' as bigint)", options)->toString());
  EXPECT_EQ(
      "try(prefix.plus(\"c0\",\"c1\"))",
      parseExpr("try(c0 + c1)", options)->toString());
  // Alias.
  EXPECT_EQ(
      "prefix.plus(\"a\",\"b\") AS sum",
      parseExpr("a + b AS sum", options)->toString());
}

TEST(DuckParserTest, lambda) {
  // There is a bug in DuckDB in parsing lambda expressions that use
  // comparisons. This doesn't work: filter(a, x -> x = 10). This does:
  // filter(a, x -> (x = 10))
  EXPECT_EQ(
      "filter(\"a\",x -> eq(\"x\",10))",
      parseExpr("filter(a, x -> (x = 10))")->toString());

  EXPECT_EQ(
      "filter(\"a\",x -> plus(\"x\",1))",
      parseExpr("filter(a, x -> x + 1)")->toString());

  EXPECT_EQ(
      "transform_keys(\"m\",(k, v) -> plus(\"k\",1))",
      parseExpr("transform_keys(m, (k, v) -> k + 1)")->toString());

  // With capture.
  EXPECT_EQ(
      "filter(\"a\",x -> eq(\"x\",\"b\"))",
      parseExpr("filter(a, x -> (x = b))")->toString());

  EXPECT_EQ(
      "transform_keys(\"m\",(k, v) -> plus(\"k\",multiply(\"v\",\"b\")))",
      parseExpr("transform_keys(m, (k, v) -> k + v * b)")->toString());

  // Conditional lambdas.
  EXPECT_EQ(
      "filter(\"a\",if(gt(\"b\",0),x -> eq(\"x\",10),x -> eq(\"x\",20)))",
      parseExpr("filter(a, if (b > 0, x -> (x = 10), x -> (x = 20)))")
          ->toString());
}
