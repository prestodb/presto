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

#include "presto_cpp/main/tvf/exec/TableFunctionOperator.h"
#include "presto_cpp/main/tvf/exec/TableFunctionSplit.h"
#include "presto_cpp/main/tvf/functions/TableFunctionsRegistration.h"
#include "presto_cpp/main/tvf/tests/PlanBuilder.h"

#include "velox/core/PlanNode.h"
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
        "TableFunctionNode", presto::tvf::TableFunctionNode::create);

    velox::exec::Operator::registerOperator(
        std::make_unique<TableFunctionTranslator>());
  }
};

TEST_F(SequenceTest, basic) {
  std::unordered_map<std::string, std::shared_ptr<Argument>> args;
  args.insert({"START", std::make_shared<ScalarArgument>(BIGINT(), makeConstant(static_cast<int64_t>(10), 1, BIGINT()))});
  args.insert({"STOP", std::make_shared<ScalarArgument>(BIGINT(), makeConstant(static_cast<int64_t>(30), 1, BIGINT()))});
  args.insert({"STEP", std::make_shared<ScalarArgument>(BIGINT(), makeConstant(static_cast<int64_t>(2), 1, BIGINT()))});
  auto plan = exec::test::PlanBuilder()
                  .addNode(addTvfNode("sequence", args))
                  .planNode();

  auto expected = makeRowVector({
      makeFlatVector<int64_t>({10, 20, 30})
  });

  auto sequenceTvfNode = dynamic_pointer_cast<const TableFunctionNode>(plan);
  auto sequenceSplits = TableFunction::getSplits("sequence", sequenceTvfNode->handle());
  std::vector<const velox::exec::Split> splits;
  for (auto sequenceSplit : sequenceSplits) {
    auto tableFunctionSplit = std::make_shared<TableFunctionSplit>(sequenceSplit);
    splits.push_back(velox::exec::Split(tableFunctionSplit));
  }
  assertQuery(plan, splits, expected);
}
} // namespace facebook::velox::exec::test
