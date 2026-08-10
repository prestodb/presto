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

#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"
#include "velox/expression/rpc/AsyncRPCFunctionRegistry.h"

using namespace facebook::presto;
using namespace facebook::velox;
namespace exec_rpc = facebook::velox::exec::rpc;

namespace {

// Create a minimal ValuesNode as source for RPCNode.
std::shared_ptr<protocol::ValuesNode> makeValuesNode() {
  auto node = std::make_shared<protocol::ValuesNode>();
  node->_type = "com.facebook.presto.sql.planner.plan.ValuesNode";
  node->id = "0";

  // Add a single output variable (the "comment" field from the query).
  protocol::VariableReferenceExpression commentVar;
  commentVar.name = "comment";
  commentVar.type = "varchar";
  node->outputVariables.push_back(commentVar);

  return node;
}

// Create a protocol RPCNode with a single source.
std::shared_ptr<protocol::RPCNode> makeRPCNode(
    std::shared_ptr<protocol::PlanNode> source) {
  auto node = std::make_shared<protocol::RPCNode>();
  node->_type = "com.facebook.presto.sql.planner.plan.RPCNode";
  node->id = "8";
  node->source = source;

  // Function name for the RPC call
  node->functionName = "fb_llm_inference";

  // Add arguments: just the variable reference (comment column).
  // We only use a VariableReferenceExpression to avoid needing properly
  // serialized Presto blocks for constant expressions in the test.
  auto arg1 = std::make_shared<protocol::VariableReferenceExpression>();
  arg1->_type = "variable";
  arg1->name = "comment";
  arg1->type = "varchar";
  node->arguments.push_back(arg1);

  // Argument column names match the arguments.
  node->argumentColumns = {"comment"};

  // Output variable is the RPC result.
  node->outputVariable.name = "__rpc_result";
  node->outputVariable.type = "varchar";

  // Default streaming mode.
  node->streamingMode = protocol::RPCNodeStreamingMode::PER_ROW;
  node->dispatchBatchSize = 0;

  return node;
}

} // namespace

class RPCPlanConverterTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::deprecatedAddDefaultLeafMemoryPool();
    queryCtx_ = core::QueryCtx::create();
  }

  void TearDown() override {
    exec_rpc::AsyncRPCFunctionRegistry::testingClear();
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<core::QueryCtx> queryCtx_;
};

// Test that converting an RPCNode fails if fb_llm_inference is not
// registered. This simulates the production bug where the function library
// was not linked into the binary.
TEST_F(RPCPlanConverterTest, rpcNodeWithoutRegisteredFunction) {
  // Clear any registered functions to simulate missing linkage.
  exec_rpc::AsyncRPCFunctionRegistry::testingClear();

  // Build the protocol plan: Values -> RPCNode
  auto valuesNode = makeValuesNode();
  auto rpcNode = makeRPCNode(valuesNode);

  // Create converter and convert.
  VeloxInteractiveQueryPlanConverter converter(queryCtx_.get(), pool_.get());

  // Plan conversion succeeds even without registration — validation is
  // deferred to RPCOperator::initialize() which calls
  // AsyncRPCFunctionRegistry::create() and fails if not registered.
  auto plan = converter.toVeloxQueryPlan(
      std::dynamic_pointer_cast<protocol::PlanNode>(rpcNode),
      nullptr,
      "20260124_042527_00001_gp3te.1.0.0.0");
  ASSERT_NE(plan, nullptr);

  auto* rpcPlanNode = dynamic_cast<const core::RPCNode*>(plan.get());
  ASSERT_NE(rpcPlanNode, nullptr);
  EXPECT_EQ(rpcPlanNode->functionName(), "fb_llm_inference");
}

// Test that converting an RPCNode succeeds when fb_llm_inference
// is properly registered. With name-based resolution, the plan converter
// only checks isRegistered() — actual function instantiation is deferred
// to RPCOperator::initialize().
TEST_F(RPCPlanConverterTest, rpcNodeWithRegisteredFunction) {
  // Clear and re-register the function.
  exec_rpc::AsyncRPCFunctionRegistry::testingClear();

  // Register a mock function for testing.
  exec_rpc::AsyncRPCFunctionRegistry::registerFunction(
      "fb_llm_inference", []() {
        // Return nullptr — the factory is not called during plan conversion
        // (only during operator initialization).
        return nullptr;
      });

  // Build the protocol plan: Values -> RPCNode
  auto valuesNode = makeValuesNode();
  auto rpcNode = makeRPCNode(valuesNode);

  // Create converter and attempt to convert.
  VeloxInteractiveQueryPlanConverter converter(queryCtx_.get(), pool_.get());

  // Plan conversion should succeed — the converter only checks
  // isRegistered(), not create(). The resulting core::RPCNode stores
  // the function name for deferred resolution by the operator.
  auto veloxPlan = converter.toVeloxQueryPlan(
      std::dynamic_pointer_cast<protocol::PlanNode>(rpcNode),
      nullptr,
      "20260124_042527_00001_gp3te.1.0.0.0");

  ASSERT_NE(veloxPlan, nullptr);
  auto veloxRpcNode = std::dynamic_pointer_cast<const core::RPCNode>(veloxPlan);
  ASSERT_NE(veloxRpcNode, nullptr);
  EXPECT_EQ(veloxRpcNode->functionName(), "fb_llm_inference");
  EXPECT_EQ(veloxRpcNode->outputColumn(), "__rpc_result");

  // Verify the call argument: a single column reference to "comment", typed
  // VARCHAR. A variable reference is not a constant, so it becomes a
  // FieldAccessTypedExpr column argument rather than a ConstantTypedExpr.
  ASSERT_EQ(veloxRpcNode->call()->inputs().size(), 1);
  auto* field = dynamic_cast<const core::FieldAccessTypedExpr*>(
      veloxRpcNode->call()->inputs()[0].get());
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->name(), "comment");
  EXPECT_EQ(field->type()->kind(), TypeKind::VARCHAR);
}

// A column argument's FieldAccess must carry the SOURCE column's actual type,
// not the argument expression's declared type. In production the Java planner
// hoists the argument expression (e.g. a CAST) into the source column, so the
// two normally agree; this test forces them to differ (source column "num" is
// BIGINT while the argument expression is declared VARCHAR) to pin that the
// converter uses the authoritative source-schema type. RPCOperator reads that
// column by name at runtime, so a FieldAccess typed from the argument
// expression would misdeclare the vector actually read.
TEST_F(RPCPlanConverterTest, columnArgUsesSourceColumnType) {
  exec_rpc::AsyncRPCFunctionRegistry::testingClear();
  exec_rpc::AsyncRPCFunctionRegistry::registerFunction(
      "fb_llm_inference", []() { return nullptr; });

  // Source produces column "num" of type BIGINT.
  auto valuesNode = std::make_shared<protocol::ValuesNode>();
  valuesNode->_type = "com.facebook.presto.sql.planner.plan.ValuesNode";
  valuesNode->id = "0";
  protocol::VariableReferenceExpression numVar;
  numVar.name = "num";
  numVar.type = "bigint";
  valuesNode->outputVariables.push_back(numVar);

  // The RPC argument references "num" but with a DIFFERENT declared type
  // (varchar), as a hoisted CAST(num AS varchar) would present it.
  // argumentColumns names the source column the operator actually reads.
  auto rpcNode = std::make_shared<protocol::RPCNode>();
  rpcNode->_type = "com.facebook.presto.sql.planner.plan.RPCNode";
  rpcNode->id = "8";
  rpcNode->source = valuesNode;
  rpcNode->functionName = "fb_llm_inference";
  auto arg = std::make_shared<protocol::VariableReferenceExpression>();
  arg->_type = "variable";
  arg->name = "num";
  arg->type = "varchar"; // differs from the source column's BIGINT
  rpcNode->arguments.push_back(arg);
  rpcNode->argumentColumns = {"num"};
  rpcNode->outputVariable.name = "__rpc_result";
  rpcNode->outputVariable.type = "varchar";
  rpcNode->streamingMode = protocol::RPCNodeStreamingMode::PER_ROW;
  rpcNode->dispatchBatchSize = 0;

  VeloxInteractiveQueryPlanConverter converter(queryCtx_.get(), pool_.get());
  auto veloxPlan = converter.toVeloxQueryPlan(
      std::dynamic_pointer_cast<protocol::PlanNode>(rpcNode),
      nullptr,
      "20260124_042527_00001_gp3te.1.0.0.0");
  auto veloxRpcNode = std::dynamic_pointer_cast<const core::RPCNode>(veloxPlan);
  ASSERT_NE(veloxRpcNode, nullptr);

  ASSERT_EQ(veloxRpcNode->call()->inputs().size(), 1);
  auto* field = dynamic_cast<const core::FieldAccessTypedExpr*>(
      veloxRpcNode->call()->inputs()[0].get());
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->name(), "num");
  // Authoritative source-column type (BIGINT), NOT the argument's declared
  // VARCHAR: this is the column RPCOperator reads by name at runtime.
  EXPECT_EQ(field->type()->kind(), TypeKind::BIGINT);
}

// A column argument that names a column absent from the source schema must fail
// fast with a clear message. In production the Java planner hoists argument
// expressions into source columns so this cannot happen, but a malformed plan
// should surface an actionable error rather than a generic lookup failure.
TEST_F(RPCPlanConverterTest, columnArgNotInSourceThrows) {
  exec_rpc::AsyncRPCFunctionRegistry::testingClear();
  exec_rpc::AsyncRPCFunctionRegistry::registerFunction(
      "fb_llm_inference", []() { return nullptr; });

  // Source produces column "comment"; the RPC column argument names a column
  // that does not exist in the source schema.
  auto valuesNode = makeValuesNode();
  auto rpcNode = makeRPCNode(valuesNode);
  rpcNode->argumentColumns = {"missing_col"};

  VeloxInteractiveQueryPlanConverter converter(queryCtx_.get(), pool_.get());
  try {
    converter.toVeloxQueryPlan(
        std::dynamic_pointer_cast<protocol::PlanNode>(rpcNode),
        nullptr,
        "20260124_042527_00001_gp3te.1.0.0.0");
    FAIL() << "expected conversion to throw for a missing source column";
  } catch (const VeloxException& e) {
    EXPECT_NE(
        std::string(e.what()).find("not found in source schema"),
        std::string::npos);
  }
}
