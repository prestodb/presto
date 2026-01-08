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

#include "presto_cpp/main/tvf/exec/TableFunctionTranslator.h"
#include "presto_cpp/main/tvf/functions/TestingTableFunctions.h"
#include "presto_cpp/main/tvf/tests/PlanBuilder.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::presto::tvf;

namespace facebook::velox::exec::test {
class TableFunctionInvocationTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
  };

 public:
  TableFunctionInvocationTest() {
    registerSimpleTableFunction("simple_table_function");
    registerIdentityFunction("identity_table_function");
    registerRepeatFunction("repeat_table_function");
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
  };

 protected:
  std::unordered_map<std::string, std::shared_ptr<Argument>>
  simpleTableFunctionArgs(const std::string& column) {
    std::unordered_map<std::string, std::shared_ptr<Argument>> args;
    args.emplace(
        "COLUMN",
        std::make_shared<ScalarArgument>(
            VARCHAR(), makeConstant(StringView(column), 1, VARCHAR())));
    return args;
  }
};

TEST_F(TableFunctionInvocationTest, DISABLED_simple) {
  auto plan = PlanBuilder()
                  .addNode(addTvfNode(
                      "simple_table_function", simpleTableFunctionArgs("col")))
                  .planNode();

  auto expected = makeRowVector({});
  AssertQueryBuilder(plan).assertResults(expected);
}

TEST_F(TableFunctionInvocationTest, identity) {
  auto data = makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 3}),
       makeFlatVector<int32_t>({10, 20, 30})});
  auto type = asRowType(data->type());
  std::unordered_map<std::string, std::shared_ptr<Argument>> args;
  auto input = std::make_shared<TableArgument>(type);
  args.emplace("INPUT", input);

  auto plan = PlanBuilder()
                  .values({data})
                  .addNode(addTvfNode("identity_table_function", args))
                  .planNode();

  assertQuery(plan, data);
}

TEST_F(TableFunctionInvocationTest, repeat) {
  auto data = makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 3}),
       makeFlatVector<int32_t>({10, 20, 30})});
  auto type = asRowType(data->type());
  std::unordered_map<std::string, std::shared_ptr<Argument>> args;
  auto input = std::make_shared<TableArgument>(type);
  args.insert({"INPUT", input});
  std::shared_ptr<ScalarArgument> count = std::make_shared<ScalarArgument>(
      BIGINT(), makeConstant(static_cast<int64_t>(2), 1, BIGINT()));
  args.insert({"COUNT", count});

  auto plan = PlanBuilder()
                  .values({data})
                  .addNode(addTvfNode("repeat_table_function", args))
                  .planNode();

  auto expected = makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 3, 1, 2, 3}),
       makeFlatVector<int32_t>({10, 20, 30, 10, 20, 30})});

  assertQuery(plan, expected);
}

TEST_F(TableFunctionInvocationTest, repeatPartitionOrder) {
  auto data = makeRowVector(
      {makeFlatVector<int64_t>({1, 1, 2, 3}),
       makeFlatVector<int32_t>({10, 20, 20, 30})});
  auto type = asRowType(data->type());
  std::unordered_map<std::string, std::shared_ptr<Argument>> args;
  auto input = std::make_shared<TableArgument>(type);
  args.insert({"INPUT", input});
  std::shared_ptr<ScalarArgument> count = std::make_shared<ScalarArgument>(
      BIGINT(), makeConstant(static_cast<int64_t>(2), 1, BIGINT()));
  args.insert({"COUNT", count});

  std::vector<core::FieldAccessTypedExprPtr> partitions = {
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c0")};
  std::vector<core::FieldAccessTypedExprPtr> sorts = {
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c1")};
  std::vector<core::SortOrder> sortOrders = {{true, false}};

  auto plan =
      PlanBuilder()
          .values({data})
          .addNode(addTvfNode(
              "repeat_table_function", args, partitions, sorts, sortOrders))
          .planNode();

  auto expected = makeRowVector(
      {makeFlatVector<int64_t>({1, 1, 2, 3, 1, 1, 2, 3}),
       makeFlatVector<int32_t>({10, 20, 20, 30, 10, 20, 20, 30})});

  assertQuery(plan, expected);
}

} // namespace facebook::velox::exec::test
