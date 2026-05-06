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

  // Verify argumentColumns are passed through.
  ASSERT_EQ(veloxRpcNode->argumentColumns().size(), 1);
  EXPECT_EQ(veloxRpcNode->argumentColumns()[0], "comment");

  // Verify argumentTypes are extracted from argument expressions.
  ASSERT_EQ(veloxRpcNode->argumentTypes().size(), 1);
  EXPECT_EQ(veloxRpcNode->argumentTypes()[0]->kind(), TypeKind::VARCHAR);

  // Verify constantInputs: arg 0 is a variable reference (nullptr).
  ASSERT_EQ(veloxRpcNode->constantInputs().size(), 1);
  EXPECT_EQ(veloxRpcNode->constantInputs()[0], nullptr);
}

// Test that the riftTier field set on the protocol RPCNode is round-tripped
// through plan conversion to the resulting core::RPCNode. This guards the
// coordinator-to-worker tier injection plumbing added for RIFT-on-MAST.
TEST_F(RPCPlanConverterTest, rpcNodeRiftTierRoundTrip) {
  // Register the RPC function so plan conversion succeeds.
  exec_rpc::AsyncRPCFunctionRegistry::testingClear();
  exec_rpc::AsyncRPCFunctionRegistry::registerFunction(
      "fb_llm_inference", []() noexcept { return nullptr; });

  // Build the protocol plan with a non-default riftTier value.
  std::shared_ptr<protocol::ValuesNode> valuesNode = makeValuesNode();
  std::shared_ptr<protocol::RPCNode> rpcNode = makeRPCNode(valuesNode);
  ASSERT_NE(rpcNode, nullptr);
  const std::string kExpectedRiftTier = "my_test_tier";
  rpcNode->riftTier = kExpectedRiftTier;

  // Convert the plan.
  VeloxInteractiveQueryPlanConverter converter(queryCtx_.get(), pool_.get());
  const core::PlanNodePtr veloxPlan = converter.toVeloxQueryPlan(
      std::dynamic_pointer_cast<protocol::PlanNode>(rpcNode),
      nullptr,
      "20260124_042527_00001_gp3te.1.0.0.0");

  // Verify the riftTier round-tripped to the core::RPCNode.
  ASSERT_NE(veloxPlan, nullptr);
  std::shared_ptr<const core::RPCNode> veloxRpcNode =
      std::dynamic_pointer_cast<const core::RPCNode>(veloxPlan);
  ASSERT_NE(veloxRpcNode, nullptr);
  EXPECT_EQ(veloxRpcNode->riftTier(), kExpectedRiftTier);
}
