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
#include "velox/core/Expressions.h"
#include "velox/exec/WindowFunction.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/IExpr.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::exec::test {

class PlanBuilderTest : public testing::Test,
                        public velox::test::VectorTestBase {
 public:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  PlanBuilderTest() {
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();
  }
};

TEST_F(PlanBuilderTest, invalidSourceNode) {
  VELOX_ASSERT_THROW(
      PlanBuilder().project({"c0 > 5"}).planNode(),
      "Project cannot be the source node");
  VELOX_ASSERT_THROW(
      PlanBuilder().filter({"c0 > 5"}).planNode(),
      "Filter cannot be the source node");
}

TEST_F(PlanBuilderTest, duplicateSubfield) {
  VELOX_ASSERT_THROW(
      PlanBuilder(pool_.get())
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
  exec::registerWindowFunction(
      "window1",
      std::move(signatures),
      exec::WindowFunction::Metadata::defaultMetadata(),
      nullptr);
}
} // namespace

TEST_F(PlanBuilderTest, windowFunctionCall) {
  VELOX_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window({"window1(c) over (partition by a order by b) as d"})
          .planNode(),
      "Window function doesn't exist: window1.");

  registerWindowFunction();

  VELOX_CHECK_EQ(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window({"window1(c) over (partition by a order by b) as d"})
          .planNode()
          ->toString(true, false),
      "-- Window[1][partition by [a] order by [b ASC NULLS LAST] "
      "d := window1(ROW[\"c\"]) RANGE between UNBOUNDED PRECEDING and CURRENT ROW] "
      "-> a:VARCHAR, b:BIGINT, c:BIGINT, d:BIGINT\n");

  VELOX_CHECK_EQ(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window({"window1(c) over (partition by a) as d"})
          .planNode()
          ->toString(true, false),
      "-- Window[1][partition by [a] "
      "d := window1(ROW[\"c\"]) RANGE between UNBOUNDED PRECEDING and CURRENT ROW] "
      "-> a:VARCHAR, b:BIGINT, c:BIGINT, d:BIGINT\n");

  VELOX_CHECK_EQ(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window({"window1(c) over ()"})
          .planNode()
          ->toString(true, false),
      "-- Window[1][w0 := window1(ROW[\"c\"]) RANGE between UNBOUNDED PRECEDING and CURRENT ROW] "
      "-> a:VARCHAR, b:BIGINT, c:BIGINT, w0:BIGINT\n");

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

  // Validating that function invocations with different frames but the same
  // partitioning and order can be executed in the same node.
  VELOX_CHECK_EQ(
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
               "window1(c) over (partition by a order by b range between unbounded preceding and unbounded following) as d10"})
          .planNode()
          ->toString(true, false),
      "-- Window[1][partition by [a] order by [b ASC NULLS LAST] "
      "d1 := window1(ROW[\"c\"]) ROWS between b PRECEDING and CURRENT ROW, "
      "d2 := window1(ROW[\"c\"]) RANGE between b PRECEDING and CURRENT ROW, "
      "d3 := window1(ROW[\"c\"]) ROWS between UNBOUNDED PRECEDING and CURRENT ROW, "
      "d4 := window1(ROW[\"c\"]) RANGE between UNBOUNDED PRECEDING and CURRENT ROW, "
      "d5 := window1(ROW[\"c\"]) ROWS between CURRENT ROW and b FOLLOWING, "
      "d6 := window1(ROW[\"c\"]) RANGE between CURRENT ROW and b FOLLOWING, "
      "d7 := window1(ROW[\"c\"]) ROWS between CURRENT ROW and UNBOUNDED FOLLOWING, "
      "d8 := window1(ROW[\"c\"]) RANGE between CURRENT ROW and UNBOUNDED FOLLOWING, "
      "d9 := window1(ROW[\"c\"]) RANGE between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING, "
      "d10 := window1(ROW[\"c\"]) RANGE between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING] "
      "-> a:VARCHAR, b:BIGINT, c:BIGINT, d1:BIGINT, d2:BIGINT, d3:BIGINT, d4:BIGINT, "
      "d5:BIGINT, d6:BIGINT, d7:BIGINT, d8:BIGINT, d9:BIGINT, d10:BIGINT\n");

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

  VELOX_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW(
              {"a", "b", "c", "d"}, {VARCHAR(), BIGINT(), BIGINT(), BIGINT()}))
          .window({
              "window1(c) over (partition by a order by b, c range between d preceding and current row) as d1",
          })
          .planNode(),
      "Window frame of type RANGE PRECEDING or FOLLOWING requires single sorting key in ORDER BY");

  VELOX_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW(
              {"a", "b", "c", "d"}, {VARCHAR(), BIGINT(), BIGINT(), BIGINT()}))
          .window({
              "window1(c) over (partition by a, c range between d preceding and current row) as d1",
          })
          .planNode(),
      "Window frame of type RANGE PRECEDING or FOLLOWING requires single sorting key in ORDER BY");
}

TEST_F(PlanBuilderTest, missingOutputType) {
  VELOX_ASSERT_THROW(
      PlanBuilder().startTableScan().endTableScan(),
      "outputType must be specified");
}

TEST_F(PlanBuilderTest, projectExpressions) {
  // Non-typed Expressions.
  // Simple field access.
  auto data = ROW({"c0"}, {BIGINT()});
  VELOX_CHECK_EQ(
      PlanBuilder()
          .tableScan("tmp", data)
          .projectExpressions(
              {std::make_shared<core::FieldAccessExpr>("c0", std::nullopt)})
          .planNode()
          ->toString(true, false),
      "-- Project[1][expressions: (c0:BIGINT, ROW[\"c0\"])] -> c0:BIGINT\n");
  // Dereference test using field access query.
  data = ROW({"c0"}, {ROW({"field0"}, {BIGINT()})});
  VELOX_CHECK_EQ(
      PlanBuilder()
          .tableScan("tmp", data)
          .projectExpressions({std::make_shared<core::FieldAccessExpr>(
              "field0",
              std::nullopt,
              std::vector<core::ExprPtr>{
                  std::make_shared<core::FieldAccessExpr>(
                      "c0", std::nullopt)})})
          .planNode()
          ->toString(true, false),
      "-- Project[1][expressions: (field0:BIGINT, ROW[\"c0\"][field0])] -> field0:BIGINT\n");

  // Test Typed Expressions
  VELOX_CHECK_EQ(
      PlanBuilder()
          .tableScan("tmp", ROW({"c0"}, {ROW({VARCHAR()})}))
          .projectExpressions(
              {std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c0")})
          .planNode()
          ->toString(true, false),
      "-- Project[1][expressions: (p0:VARCHAR, \"c0\")] -> p0:VARCHAR\n");
  VELOX_CHECK_EQ(
      PlanBuilder()
          .tableScan("tmp", ROW({"c0"}, {ROW({VARCHAR()})}))
          .projectExpressions({std::make_shared<core::FieldAccessTypedExpr>(
              VARCHAR(),
              std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c0"),
              "field0")})
          .planNode()
          ->toString(true, false),
      "-- Project[1][expressions: (p0:VARCHAR, \"c0\"[\"field0\"])] -> p0:VARCHAR\n");
}

TEST_F(PlanBuilderTest, commitStrategyParameter) {
  auto data = makeRowVector({makeFlatVector<int64_t>(10, folly::identity)});
  auto directory = "/some/test/directory";

  // Lambda to create a plan with given commitStrategy and verify it
  auto testCommitStrategy = [&](connector::CommitStrategy commitStrategy) {
    // Create a plan with commitStrategy
    auto planBuilder = PlanBuilder().values({data}).tableWrite(
        directory,
        {},
        0,
        {},
        {},
        dwio::common::FileFormat::DWRF,
        {},
        PlanBuilder::kHiveDefaultConnectorId,
        {},
        nullptr,
        "",
        common::CompressionKind_NONE,
        nullptr,
        false);

    core::PlanNodePtr plan;
    // Conditionally set commitStrategy if it's not kNoCommit
    if (commitStrategy != connector::CommitStrategy::kNoCommit) {
      plan = PlanBuilder::TableWriterBuilder(planBuilder)
                 .commitStrategy(commitStrategy)
                 .endTableWriter()
                 .planNode();
    } else {
      plan = std::move(planBuilder.planNode());
    }

    // Verify the plan node has the correct commit strategy
    auto tableWriteNode =
        std::dynamic_pointer_cast<const core::TableWriteNode>(plan);
    ASSERT_NE(tableWriteNode, nullptr);
    ASSERT_EQ(tableWriteNode->commitStrategy(), commitStrategy);
  };

  // Test with explicit task commit strategy
  testCommitStrategy(connector::CommitStrategy::kTaskCommit);

  // Test with no explicit commit strategy (should default to kNoCommit)
  testCommitStrategy(connector::CommitStrategy::kNoCommit);
}

} // namespace facebook::velox::exec::test
