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

#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

using facebook::velox::exec::test::PlanBuilder;

namespace facebook::velox::core {
namespace {

class PlanNodeToSummaryStringTest : public testing::Test,
                                    public velox::test::VectorTestBase {
 public:
  PlanNodeToSummaryStringTest() {
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(PlanNodeToSummaryStringTest, basic) {
  auto rowType = ROW(
      {"a", "b", "c"}, {INTEGER(), ARRAY(BIGINT()), MAP(TINYINT(), BIGINT())});
  auto plan =
      PlanBuilder()
          .tableScan(rowType)
          .filter("a > 10 AND cardinality(b) > 20 AND c[1::tinyint] + 11 > 30")
          .project({
              "a + 1",
              "b[1::integer]",
              "b[2::integer]",
              "b[3::integer]",
              "b[4::integer]",
              "c[1::tinyint] + 11",
              "c[2::tinyint] + 12",
          })
          .planNode();
  ASSERT_EQ(
      "-- Project[2]: 7 fields: p0 BIGINT, p1 BIGINT, p2 BIGINT, p3 BIGINT, p4 BIGINT, ...\n"
      "      expressions: call: 9, cast: 7, constant: 9, field: 7\n"
      "      functions: plus: 3, subscript: 6\n"
      "      constants: BIGINT: 9\n"
      "      projections: 7 out of 7\n"
      "      dereferences: 0 out of 7\n"
      "  -- Filter[1]: 3 fields: a INTEGER, b ARRAY, c MAP\n"
      "        expressions: call: 8, cast: 2, constant: 5, field: 3\n"
      "        functions: and: 2, cardinality: 1, gt: 3, plus: 1, subscript: 1\n"
      "        constants: BIGINT: 5\n"
      "        filter: and(and(gt(cast ROW[\"a\"] as BIGINT,10),gt(cardinal...\n"
      "    -- TableScan[0]: 3 fields: a INTEGER, b ARRAY, c MAP\n"
      "          table: hive_table\n",
      plan->toSummaryString());

  ASSERT_EQ(
      "-- Project[2]: 7 fields: p0 BIGINT, p1 BIGINT, p2 BIGINT, ...\n"
      "      expressions: call: 9, cast: 7, constant: 9, field: 7\n"
      "      functions: plus: 3, subscript: 6\n"
      "      constants: BIGINT: 9\n"
      "      projections: 7 out of 7\n"
      "         p0: plus(cast ROW[\"a\"] as BIGINT,1)\n"
      "         p1: subscript(ROW[\"b\"],cast 1 as INTEGER)\n"
      "         ... 5 more\n"
      "      dereferences: 0 out of 7\n"
      "  -- Filter[1]: 3 fields: a INTEGER, b ARRAY(BIGINT), c MAP(TINYINT, BIGINT)\n"
      "        expressions: call: 8, cast: 2, constant: 5, field: 3\n"
      "        functions: and: 2, cardinality: 1, gt: 3, plus: 1, subscript: 1\n"
      "        constants: BIGINT: 5\n"
      "        filter: and(and(gt(cast ROW[\"a\"] as BIGINT,10),gt(cardinal...\n"
      "    -- TableScan[0]: 3 fields: a INTEGER, b ARRAY(BIGINT), c MAP(TINYINT, BIGINT)\n"
      "          table: hive_table\n",
      plan->toSummaryString({
          .project = {.maxProjections = 2},
          .maxOutputFields = 3,
          .maxChildTypes = 2,
      }));
}

TEST_F(PlanNodeToSummaryStringTest, expressions) {
  auto rowType =
      ROW({"a", "b", "c", "d"},
          {
              INTEGER(),
              ARRAY(BIGINT()),
              MAP(TINYINT(), BIGINT()),
              ROW({"x", "y", "z"}, {BIGINT(), BIGINT(), VARCHAR()}),
          });

  auto plan = PlanBuilder()
                  .tableScan(rowType)
                  .project({
                      "a",
                      "transform(b, x -> x + 1)",
                      "c",
                      "d.x * 10",
                      "d.y",
                      "length(d.z) * strpos(d.z, 'foo')",
                      "12.345",
                  })
                  .planNode();

  ASSERT_EQ(
      "-- Project[1]: 7 fields: a INTEGER, p1 ARRAY, c MAP, p3 BIGINT, y BIGINT, ...\n"
      "      expressions: call: 6, constant: 4, dereference: 4, field: 8, lambda: 1\n"
      "      functions: length: 1, multiply: 2, plus: 1, strpos: 1, transform: 1\n"
      "      constants: BIGINT: 2, DOUBLE: 1, VARCHAR: 1\n"
      "      projections: 4 out of 7\n"
      "      dereferences: 1 out of 7\n"
      "  -- TableScan[0]: 4 fields: a INTEGER, b ARRAY, c MAP, d ROW(3)\n"
      "        table: hive_table\n",
      plan->toSummaryString());

  ASSERT_EQ(
      "-- Project[1]: 7 fields: a INTEGER, p1 ARRAY, c MAP, p3 BIGINT, y BIGINT, ...\n"
      "      expressions: call: 6, constant: 4, dereference: 4, field: 8, lambda: 1\n"
      "      functions: length: 1, multiply: 2, plus: 1, strpos: 1, transform: 1\n"
      "      constants: BIGINT: 2, DOUBLE: 1, VARCHAR: 1\n"
      "      projections: 4 out of 7\n"
      "         p1: transform(ROW[\"b\"],lambda ROW<x:BIGINT> -> plus(RO...\n"
      "         p3: multiply(ROW[\"d\"][x],10)\n"
      "         p5: multiply(length(ROW[\"d\"][z]),strpos(ROW[\"d\"][z],\"f...\n"
      "         ... 1 more\n"
      "      dereferences: 1 out of 7\n"
      "         y: ROW[\"d\"][y]\n"
      "  -- TableScan[0]: 4 fields: a INTEGER, b ARRAY, c MAP, d ROW(3)\n"
      "        table: hive_table\n",
      plan->toSummaryString(
          {.project = {.maxProjections = 3, .maxDereferences = 2}}));
}

} // namespace
} // namespace facebook::velox::core
