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

#include "presto_cpp/main/tvf/functions/TableFunctionsRegistration.h"
#include "presto_cpp/main/tvf/tests/PlanBuilder.h"

#include "velox/core/PlanNode.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::presto::tvf;

namespace facebook::velox::exec::test {
class PlanNodeSerdeTest : public testing::Test,
                          public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  PlanNodeSerdeTest() {
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

    data_ = {makeRowVector({
        makeFlatVector<int64_t>({1, 2, 3}),
        makeFlatVector<int32_t>({10, 20, 30}),
        makeConstant(true, 3),
    })};
    type_ = asRowType(data_[0]->type());
  }

  void testSerde(const core::PlanNodePtr& plan) {
    auto serialized = plan->serialize();

    auto copy =
        velox::ISerializable::deserialize<core::PlanNode>(serialized, pool());

    LOG(INFO) << "\nplan->toString" << plan->toString(true, true) << "\n";
    LOG(INFO) << "\ncopy->toString" << copy->toString(true, true) << "\n";
    ASSERT_EQ(plan->toString(true, true), copy->toString(true, true));
  }

  /*static std::vector<std::string> reverseColumns(const RowTypePtr& rowType) {
    auto names = rowType->names();
    std::reverse(names.begin(), names.end());
    return names;
  }*/

  std::vector<RowVectorPtr> data_;
  RowTypePtr type_;
};

TEST_F(PlanNodeSerdeTest, excludeColumns) {
  std::unordered_map<std::string, std::shared_ptr<Argument>> args;
  std::vector<std::string> excludeColumnNames = {"c0"};
  auto excludeColumnsDesc = std::make_shared<Descriptor>(excludeColumnNames);
  args.insert({"COLUMNS", excludeColumnsDesc});
  auto inputDesc = std::make_shared<TableArgument>(type_);
  args.insert({"INPUT", inputDesc});
  auto plan = exec::test::PlanBuilder()
                  .values(data_, true)
                  .addNode(addTvfNode("exclude_columns", args))
                  .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, sequence) {
  std::unordered_map<std::string, std::shared_ptr<Argument>> args;
  args.insert(
      {"START",
       std::make_shared<ScalarArgument>(
           BIGINT(), makeConstant(static_cast<int64_t>(1), 1, BIGINT()))});
  args.insert(
      {"STOP",
       std::make_shared<ScalarArgument>(
           BIGINT(), makeConstant(static_cast<int64_t>(10), 1, BIGINT()))});
  args.insert(
      {"STEP",
       std::make_shared<ScalarArgument>(
           BIGINT(), makeConstant(static_cast<int64_t>(1), 1, BIGINT()))});
  auto plan = exec::test::PlanBuilder()
                  .addNode(addTvfNode("sequence", args))
                  .planNode();
  testSerde(plan);
}
} // namespace facebook::velox::exec::test
