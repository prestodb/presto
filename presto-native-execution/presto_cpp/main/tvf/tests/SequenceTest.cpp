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

#include "presto_cpp/main/tvf/core/TableFunctionProcessorNode.h"
#include "presto_cpp/main/tvf/exec/TableFunctionOperator.h"
#include "presto_cpp/main/tvf/exec/TableFunctionSplit.h"
#include "presto_cpp/main/tvf/exec/TableFunctionTranslator.h"
#include "presto_cpp/main/tvf/functions/TableFunctionsRegistration.h"
#include "presto_cpp/main/tvf/tests/PlanBuilder.h"

#include "velox/core/PlanNode.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::presto::tvf;

namespace facebook::velox::exec::test {

class SequenceTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    exec::test::OperatorTestBase::SetUp();
  }

  SequenceTest() {
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    registerAllTableFunctions("");
    parse::registerTypeResolver();

    Type::registerSerDe();
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();

    // This code is added in PrestoToVeloxQueryPlan.
    auto& registry = DeserializationWithContextRegistryForSharedPtr();
    registry.Register(
        "TableFunctionProcessorNode",
        presto::tvf::TableFunctionProcessorNode::create);

    velox::exec::Operator::registerOperator(
        std::make_unique<TableFunctionTranslator>());
  }

  std::unordered_map<std::string, std::shared_ptr<Argument>>
  sequenceArgs(int64_t start, int64_t stop, int64_t step) {
    std::unordered_map<std::string, std::shared_ptr<Argument>> args;
    args.insert(
        {"START",
         std::make_shared<ScalarArgument>(
             BIGINT(), makeConstant(start, 1, BIGINT()))});
    args.insert(
        {"STOP",
         std::make_shared<ScalarArgument>(
             BIGINT(), makeConstant(stop, 1, BIGINT()))});
    args.insert(
        {"STEP",
         std::make_shared<ScalarArgument>(
             BIGINT(), makeConstant(step, 1, BIGINT()))});

    return args;
  }

  std::vector<velox::exec::Split> splitsForTvf(const core::PlanNodePtr& node) {
    auto sequenceTvfNode =
        dynamic_pointer_cast<const TableFunctionProcessorNode>(node);
    auto sequenceSplits =
        TableFunction::getSplits("sequence", sequenceTvfNode->handle());
    std::vector<velox::exec::Split> tvfSplits;
    for (auto sequenceSplit : sequenceSplits) {
      auto tableFunctionSplit =
          std::make_shared<TableFunctionSplit>(sequenceSplit);
      tvfSplits.push_back(velox::exec::Split(tableFunctionSplit));
    }

    return tvfSplits;
  }
};

TEST_F(SequenceTest, basic) {
  auto plan = exec::test::PlanBuilder()
                  .addNode(addTvfNode("sequence", sequenceArgs(10, 30, 2)))
                  .planNode();

  auto expected = makeRowVector(
      {makeFlatVector<int64_t>({10, 12, 14, 16, 18, 20, 22, 24, 26, 28})});

  auto sequenceTvfNode =
      dynamic_pointer_cast<const TableFunctionProcessorNode>(plan);
  auto sequenceSplits =
      TableFunction::getSplits("sequence", sequenceTvfNode->handle());
  std::vector<velox::exec::Split> tvfSplits;
  for (auto sequenceSplit : sequenceSplits) {
    auto tableFunctionSplit =
        std::make_shared<TableFunctionSplit>(sequenceSplit);
    tvfSplits.push_back(velox::exec::Split(tableFunctionSplit));
  }

  AssertQueryBuilder(plan).splits(splitsForTvf(plan)).assertResults(expected);
}

TEST_F(SequenceTest, join) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  core::PlanNodeId sourceId1;
  auto source1 = exec::test::PlanBuilder(planNodeIdGenerator)
                     .addNode(addTvfNode("sequence", sequenceArgs(10, 30, 2)))
                     .capturePlanNodeId(sourceId1)
                     .planNode();

  core::PlanNodeId sourceId2;
  core::PlanNodePtr source2;
  auto plan =
      exec::test::PlanBuilder(planNodeIdGenerator)
          .addNode(addTvfNode("sequence", sequenceArgs(20, 30, 2)))
          .capturePlanNodeId(sourceId2)
          .capturePlanNode(source2)
          .project({"sequential_number AS left_sequence"})
          .nestedLoopJoin(
              source1, "sequential_number = left_sequence", {"left_sequence"})
          .planNode();

  auto expected =
      makeRowVector({makeFlatVector<int64_t>({20, 22, 24, 26, 28})});

  AssertQueryBuilder(plan)
      .splits(sourceId1, splitsForTvf(source1))
      .splits(sourceId2, splitsForTvf(source2))
      .assertResults(expected);
}

} // namespace facebook::velox::exec::test
