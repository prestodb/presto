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
#include "presto_cpp/main/tvf/exec/TableFunctionTranslator.h"
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
class ExcludeColumnsTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    exec::test::OperatorTestBase::SetUp();
  }

  ExcludeColumnsTest() {
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
};

TEST_F(ExcludeColumnsTest, basic) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<int32_t>({10, 20, 30}),
      makeConstant(true, 3),
  });
  auto type = asRowType(data->type());

  std::unordered_map<std::string, std::shared_ptr<Argument>> args;
  std::vector<std::string> excludeColumnNames = {"c0"};
  auto excludeColumnsDesc = std::make_shared<Descriptor>(excludeColumnNames);
  args.insert({"COLUMNS", excludeColumnsDesc});
  auto inputDesc = std::make_shared<TableArgument>(type);
  args.insert({"INPUT", inputDesc});

  auto plan = exec::test::PlanBuilder()
                  .values({data})
                  .addNode(addTvfNode("exclude_columns", args))
                  .planNode();
  auto expected = makeRowVector({
      makeFlatVector<int32_t>({10, 20, 30}),
      makeConstant(true, 3),
  });
  assertQuery(plan, expected);
}
} // namespace facebook::velox::exec::test
