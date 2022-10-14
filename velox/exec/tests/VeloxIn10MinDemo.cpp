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
#include <folly/init/Init.h>
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/core/Expressions.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/tpch/gen/TpchGen.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec::test;

class VeloxIn10MinDemo : public VectorTestBase {
 public:
  const std::string kTpchConnectorId = "test-tpch";

  VeloxIn10MinDemo() {
    // Register Presto scalar functions.
    functions::prestosql::registerAllScalarFunctions();

    // Register Presto aggregate functions.
    aggregate::prestosql::registerAllAggregateFunctions();

    // Register type resolver with DuckDB SQL parser.
    parse::registerTypeResolver();

    // Register TPC-H connector.
    auto tpchConnector =
        connector::getConnectorFactory(
            connector::tpch::TpchConnectorFactory::kTpchConnectorName)
            ->newConnector(kTpchConnectorId, nullptr);
    connector::registerConnector(tpchConnector);
  }

  ~VeloxIn10MinDemo() {
    connector::unregisterConnector(kTpchConnectorId);
  }

  /// Parse SQL expression into a typed expression tree using DuckDB SQL parser.
  core::TypedExprPtr parseExpression(
      const std::string& text,
      const RowTypePtr& rowType) {
    parse::ParseOptions options;
    auto untyped = parse::parseExpr(text, options);
    return core::Expressions::inferTypes(untyped, rowType, execCtx_->pool());
  }

  /// Compile typed expression tree into an executable ExprSet.
  std::unique_ptr<exec::ExprSet> compileExpression(
      const std::string& expr,
      const RowTypePtr& rowType) {
    std::vector<core::TypedExprPtr> expressions = {
        parseExpression(expr, rowType)};
    return std::make_unique<exec::ExprSet>(
        std::move(expressions), execCtx_.get());
  }

  /// Evaluate an expression on one batch of data.
  VectorPtr evaluate(exec::ExprSet& exprSet, const RowVectorPtr& input) {
    exec::EvalCtx context(execCtx_.get(), &exprSet, input.get());

    SelectivityVector rows(input->size());
    std::vector<VectorPtr> result(1);
    exprSet.eval(rows, context, result);
    return result[0];
  }

  /// Make TPC-H split to add to TableScan node.
  exec::Split makeTpchSplit() const {
    return exec::Split(std::make_shared<connector::tpch::TpchConnectorSplit>(
        kTpchConnectorId));
  }

  /// Run the demo.
  void run();

  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::createForTest()};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
};

void VeloxIn10MinDemo::run() {
  // Let’s create two vectors of 64-bit integers and one vector of strings.
  auto a = makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6});
  auto b = makeFlatVector<int64_t>({0, 5, 10, 15, 20, 25, 30});
  auto dow = makeFlatVector<std::string>(
      {"monday",
       "tuesday",
       "wednesday",
       "thursday",
       "friday",
       "saturday",
       "sunday"});

  auto data = makeRowVector({"a", "b", "dow"}, {a, b, dow});

  std::cout << std::endl
            << "> vectors a, b, dow: " << data->toString() << std::endl;
  std::cout << data->toString(0, data->size()) << std::endl;

  // Expressions.

  // Now, let’s compute a sum of 'a' and 'b' by evaluating 'a + b' expression.

  // First we need to parse the expression into a fully typed expression tree.
  // Then, we need to compile it into an executable ExprSet.
  auto exprSet = compileExpression("a + b", asRowType(data->type()));

  // Let's print out the ExprSet:
  std::cout << std::endl << "> 'a + b' expression:" << std::endl;
  std::cout << exprSet->toString(false /*compact*/) << std::endl;

  // Now we are ready to evaluate the expression on a batch of data.
  auto c = evaluate(*exprSet, data);

  auto abc = makeRowVector({"a", "b", "c"}, {a, b, c});

  std::cout << std::endl << "> a, b, a + b: " << abc->toString() << std::endl;
  std::cout << abc->toString(0, c->size()) << std::endl;

  // Let's try a slightly more complex expression: `3 * a + sqrt(b)`.
  exprSet = compileExpression("2 * a + b % 3", asRowType(data->type()));

  std::cout << std::endl << "> '2 * a + b % 3' expression:" << std::endl;
  std::cout << exprSet->toString(false /*compact*/) << std::endl;

  auto d = evaluate(*exprSet, data);

  auto abd = makeRowVector({"a", "b", "d"}, {a, b, d});

  std::cout << std::endl
            << "> a, b, 2 * a + b % 3: " << abd->toString() << std::endl;
  std::cout << abd->toString(0, d->size()) << std::endl;

  // Let's transform 'dow' column into a 3-letter prefix with first letter
  // capitalized, e.g. Mon, Tue, etc.
  exprSet = compileExpression(
      "concat(upper(substr(dow, 1, 1)), substr(dow, 2, 2))",
      asRowType(data->type()));

  std::cout << std::endl
            << "> '3-letter prefix with first letter capitalized' expression:"
            << std::endl;
  std::cout << exprSet->toString(false /*compact*/) << std::endl;

  auto shortDow = evaluate(*exprSet, data);
  std::cout << std::endl
            << "> short days of week: " << shortDow->toString() << std::endl;
  std::cout << shortDow->toString(0, shortDow->size()) << std::endl;

  // Queries.

  // Let's compute sum and average of 'a' and 'b' by creating
  // and executing a query plan with an aggregation node.

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation(
                      {},
                      {"sum(a) AS sum_a",
                       "avg(a) AS avg_a",
                       "sum(b) AS sum_b",
                       "avg(b) AS avg_b"})
                  .planNode();

  auto sumAvg = AssertQueryBuilder(plan).copyResults(pool());

  std::cout << std::endl
            << "> sum and average for a and b: " << sumAvg->toString()
            << std::endl;
  std::cout << sumAvg->toString(0, sumAvg->size()) << std::endl;

  // Now, let's sort by 'a' descending.

  plan = PlanBuilder().values({data}).orderBy({"a DESC"}, false).planNode();

  auto sorted = AssertQueryBuilder(plan).copyResults(pool());

  std::cout << std::endl
            << "> data sorted on 'a' in descending order: "
            << sorted->toString() << std::endl;
  std::cout << sorted->toString(0, sorted->size()) << std::endl;

  // And take top 3 rows.

  plan = PlanBuilder().values({data}).topN({"a DESC"}, 3, false).planNode();

  auto top3 = AssertQueryBuilder(plan).copyResults(pool());

  std::cout << std::endl
            << "> top 3 rows as sorted on 'a' in descending order: "
            << top3->toString() << std::endl;
  std::cout << top3->toString(0, top3->size()) << std::endl;

  // We can also filter rows that have even values of 'a'.
  plan = PlanBuilder().values({data}).filter("a % 2 == 0").planNode();

  auto evenA = AssertQueryBuilder(plan).copyResults(pool());

  std::cout << std::endl
            << "> rows with even values of 'a': " << evenA->toString()
            << std::endl;
  std::cout << evenA->toString(0, evenA->size()) << std::endl;

  // Now, let's read some data from the TPC-H connector which generates TPC-H
  // data on the fly. We are going to read columns n_nationkey and n_name from
  // nation table and print first 10 rows.

  plan = PlanBuilder()
             .tableScan(
                 tpch::Table::TBL_NATION,
                 {"n_nationkey", "n_name"},
                 1 /*scaleFactor*/)
             .planNode();

  auto nations =
      AssertQueryBuilder(plan).split(makeTpchSplit()).copyResults(pool());

  std::cout << std::endl
            << "> first 10 rows from TPC-H nation table: "
            << nations->toString() << std::endl;
  std::cout << nations->toString(0, 10) << std::endl;

  // Let's join TPC-H nation and region tables to count number of nations in
  // each region and sort results by region name. We need to use one TableScan
  // node for nations table and another for region table. We also need to
  // provide splits for each TableScan node. We are going to use two
  // PlanBuilders: one for the probe side of the join and another one for the
  // build side. We are going to use PlanNodeIdGenerator to ensure that all plan
  // nodes in the final plan have unique IDs.

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId nationScanId;
  core::PlanNodeId regionScanId;
  plan = PlanBuilder(planNodeIdGenerator)
             .tableScan(
                 tpch::Table::TBL_NATION, {"n_regionkey"}, 1 /*scaleFactor*/)
             .capturePlanNodeId(nationScanId)
             .hashJoin(
                 {"n_regionkey"},
                 {"r_regionkey"},
                 PlanBuilder(planNodeIdGenerator)
                     .tableScan(
                         tpch::Table::TBL_REGION,
                         {"r_regionkey", "r_name"},
                         1 /*scaleFactor*/)
                     .capturePlanNodeId(regionScanId)
                     .planNode(),
                 "", // extra filter
                 {"r_name"})
             .singleAggregation({"r_name"}, {"count(1) as nation_cnt"})
             .orderBy({"r_name"}, false)
             .planNode();

  auto nationCnt = AssertQueryBuilder(plan)
                       .split(nationScanId, makeTpchSplit())
                       .split(regionScanId, makeTpchSplit())
                       .copyResults(pool());

  std::cout << std::endl
            << "> number of nations per region in TPC-H: "
            << nationCnt->toString() << std::endl;
  std::cout << nationCnt->toString(0, 10) << std::endl;
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv, false);

  VeloxIn10MinDemo demo;
  demo.run();
}
