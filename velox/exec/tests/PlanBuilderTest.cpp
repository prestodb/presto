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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/WindowFunction.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/VectorTestBase.h"

namespace facebook::velox::exec::test {

class PlanBuilderTest : public testing::Test,
                        public velox::test::VectorTestBase {
 public:
  PlanBuilderTest() {
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();
  }
};

TEST_F(PlanBuilderTest, duplicateSubfield) {
  VELOX_ASSERT_THROW(
      PlanBuilder()
          .tableScan(
              ROW({"a", "b"}, {BIGINT(), BIGINT()}),
              {"a < 5", "b = 7", "a > 0"},
              "a + b < 100")
          .planNode(),
      "Duplicate subfield: a");
}

TEST_F(PlanBuilderTest, invalidScalarFunctionCall) {
  VELOX_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b"}, {BIGINT(), BIGINT()}))
          .project({"to_unixtime(a)"})
          .planNode(),
      "Scalar function signature is not supported: to_unixtime(BIGINT).");

  VELOX_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b"}, {BIGINT(), BIGINT()}))
          .project({"to_unitime(a)"})
          .planNode(),
      "Scalar function doesn't exist: to_unitime.");
}

TEST_F(PlanBuilderTest, invalidAggregateFunctionCall) {
  VELOX_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b"}, {VARCHAR(), BIGINT()}))
          .partialAggregation({}, {"sum(a)"})
          .planNode(),
      "Aggregate function signature is not supported: sum(VARCHAR).");

  VELOX_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b"}, {VARCHAR(), BIGINT()}))
          .partialAggregation({}, {"maxx(a)"})
          .planNode(),
      "Aggregate function doesn't exist: maxx.");
}

namespace {

void registerWindowFunction() {
  std::vector<exec::FunctionSignaturePtr> signatures{
      exec::FunctionSignatureBuilder()
          .argumentType("BIGINT")
          .returnType("BIGINT")
          .build(),
  };
  exec::registerWindowFunction("window1", std::move(signatures), nullptr);
}
} // namespace

TEST_F(PlanBuilderTest, windowFunctionCall) {
  VELOX_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window({"window1(c) over (partition by a order by b) as d"})
          .planNode(),
      "Registry of window functions is empty.");

  registerWindowFunction();

  VELOX_CHECK_NOT_NULL(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window({"window1(c) over (partition by a order by b) as d"})
          .planNode());

  VELOX_CHECK_NOT_NULL(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window({"window1(c) over (partition by a) as d"})
          .planNode());

  VELOX_CHECK_NOT_NULL(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window({"window1(c) over ()"})
          .planNode());

  VELOX_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b"}, {VARCHAR(), BIGINT()}))
          .window({"window1(a) over (partition by a order by b) as d"})
          .planNode(),
      "Window function signature is not supported: window1(VARCHAR).");

  VELOX_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b"}, {VARCHAR(), BIGINT()}))
          .window({"window2(a) over (partition by a order by b) as d"})
          .planNode(),
      "Window function doesn't exist: window2.");
}

TEST_F(PlanBuilderTest, windowFrame) {
  registerWindowFunction();

  // TODO: Change these tests to validate the results of the parsing when
  // WindowNode::toString() is implemented.
  VELOX_CHECK_NOT_NULL(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window(
              {"window1(c) over (partition by a order by b rows between b preceding and current row) as d1",
               "window1(c) over (partition by a order by b range between b preceding and current row) as d2",
               "window1(c) over (partition by a order by b rows between unbounded preceding and current row) as d3",
               "window1(c) over (partition by a order by b range between unbounded preceding and current row) as d4",
               "window1(c) over (partition by a order by b rows between current row and b following) as d5",
               "window1(c) over (partition by a order by b range between current row and b following) as d6",
               "window1(c) over (partition by a order by b rows between current row and unbounded following) as d7",
               "window1(c) over (partition by a order by b range between current row and unbounded following) as d8",
               "window1(c) over (partition by a order by b rows between unbounded preceding and unbounded following) as d9",
               "window1(c) over (partition by a order by b rows between unbounded preceding and unbounded following) as d10"})
          .planNode());

  VELOX_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window(
              {"window1(c) over (partition by a order by b rows between b preceding and current row) as d1",
               "window1(c) over (partition by a order by b range between b preceding and current row) as d2",
               "window1(c) over (partition by b order by a rows between b preceding and current row) as d3"})
          .planNode(),
      "do not match PARTITION BY clauses.");

  VELOX_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window(
              {"window1(c) over (partition by a order by b rows between b preceding and current row) as d1",
               "window1(c) over (partition by a order by c rows between b preceding and current row) as d2"})
          .planNode(),
      "do not match ORDER BY clauses.");

  VELOX_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window(
              {"window1(c) over (partition by a order by b rows between b preceding and current row) as d1",
               "window1(c) over (partition by a order by b desc rows between b preceding and current row) as d2"})
          .planNode(),
      "do not match ORDER BY clauses.");
}
} // namespace facebook::velox::exec::test
