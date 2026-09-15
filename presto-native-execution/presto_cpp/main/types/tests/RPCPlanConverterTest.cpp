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
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/rpc/RPCTypes.h"
#include "velox/expression/RegisterSpecialForm.h"
#include "velox/expression/rpc/AsyncRPCFunctionRegistry.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

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

std::shared_ptr<protocol::ConstantExpression> makeDirectConstantOptions() {
  const json serialized = json::parse(R"(
      {
        "@type": "constant",
        "type": "varchar",
        "valueBlock": "DgAAAFZBUklBQkxFX1dJRFRIAQAAAE8AAAAATwAAAHsiaW5mZXJlbmNlX2JhY2tlbmQiOiJpcG5leHQiLCJzdHJlYW1pbmdfbW9kZSI6ImJhdGNoIiwiZGlzcGF0Y2hfYmF0Y2hfc2l6ZSI6N30="
      }
  )");
  return serialized;
}

std::shared_ptr<protocol::RowExpression> makeCastConstantOptions() {
  const json serialized = json::parse(R"(
      {
        "@type": "call",
        "arguments": [
          {
            "@type": "call",
            "arguments": [
              {
                "@type": "constant",
                "type": "varchar",
                "valueBlock": "DgAAAFZBUklBQkxFX1dJRFRIAQAAAE8AAAAATwAAAHsiaW5mZXJlbmNlX2JhY2tlbmQiOiJpcG5leHQiLCJzdHJlYW1pbmdfbW9kZSI6ImJhdGNoIiwiZGlzcGF0Y2hfYmF0Y2hfc2l6ZSI6N30="
              }
            ],
            "displayName": "CAST",
            "functionHandle": {
              "@type": "$static",
              "signature": {
                "argumentTypes": ["varchar"],
                "kind": "SCALAR",
                "longVariableConstraints": [],
                "name": "presto.default.$operator$cast",
                "returnType": "varchar",
                "typeVariableConstraints": [],
                "variableArity": false
              },
              "builtInFunctionKind": "ENGINE"
            },
            "returnType": "varchar"
          }
        ],
        "displayName": "CAST",
        "functionHandle": {
          "@type": "$static",
          "signature": {
            "argumentTypes": ["varchar"],
            "kind": "SCALAR",
            "longVariableConstraints": [],
            "name": "presto.default.$operator$cast",
            "returnType": "varchar",
            "typeVariableConstraints": [],
            "variableArity": false
          },
          "builtInFunctionKind": "ENGINE"
        },
        "returnType": "varchar"
      }
  )");
  return serialized;
}

std::shared_ptr<protocol::CallExpression> makeNestedRpcArgument() {
  const json serialized = json::parse(R"(
      {
        "@type": "call",
        "arguments": [
          {
            "@type": "constant",
            "type": "varchar",
            "valueBlock": "DgAAAFZBUklBQkxFX1dJRFRIAQAAAE8AAAAATwAAAHsiaW5mZXJlbmNlX2JhY2tlbmQiOiJpcG5leHQiLCJzdHJlYW1pbmdfbW9kZSI6ImJhdGNoIiwiZGlzcGF0Y2hfYmF0Y2hfc2l6ZSI6N30="
          }
        ],
        "displayName": "nested_rpc_test",
        "functionHandle": {
          "@type": "$static",
          "signature": {
            "argumentTypes": ["varchar"],
            "kind": "SCALAR",
            "longVariableConstraints": [],
            "name": "presto.default.nested_rpc_test",
            "returnType": "varchar",
            "typeVariableConstraints": [],
            "variableArity": false
          },
          "builtInFunctionKind": "ENGINE"
        },
        "returnType": "varchar"
      }
  )");
  return serialized;
}

std::shared_ptr<protocol::CallExpression> makeCastInnerRpcResult() {
  const json serialized = json::parse(
      R"(
      {
        "@type": "call",
        "arguments": [
          {
            "@type": "variable",
            "name": "__inner_rpc_result",
            "type": "varchar"
          }
        ],
        "displayName": "CAST",
        "functionHandle": {
          "@type": "$static",
          "signature": {
            "argumentTypes": ["varchar"],
            "kind": "SCALAR",
            "longVariableConstraints": [],
            "name": "presto.default.$operator$cast",
            "returnType": "varchar",
            "typeVariableConstraints": [],
            "variableArity": false
          },
          "builtInFunctionKind": "ENGINE"
        },
        "returnType": "varchar"
      }
  )");
  return serialized;
}

std::shared_ptr<protocol::CallExpression> makeBigintToVarcharCast() {
  const json serialized = json::parse(R"(
      {
        "@type": "call",
        "arguments": [
          {
            "@type": "constant",
            "type": "bigint",
            "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAAoAAAAAAAAA"
          }
        ],
        "displayName": "CAST",
        "functionHandle": {
          "@type": "$static",
          "signature": {
            "argumentTypes": ["bigint"],
            "kind": "SCALAR",
            "longVariableConstraints": [],
            "name": "presto.default.$operator$cast",
            "returnType": "varchar",
            "typeVariableConstraints": [],
            "variableArity": false
          },
          "builtInFunctionKind": "ENGINE"
        },
        "returnType": "varchar"
      }
  )");
  return serialized;
}

std::shared_ptr<protocol::CallExpression> makeNestedStringOptions() {
  const json serialized = json::parse(R"(
      {
        "@type": "call",
        "arguments": [
          {
            "@type": "call",
            "arguments": [
              {"@type": "variable", "name": "expr_14", "type": "varchar"},
              {"@type": "variable", "name": "expr_14", "type": "varchar"}
            ],
            "displayName": "concat",
            "functionHandle": {
              "@type": "$static",
              "signature": {
                "argumentTypes": ["varchar", "varchar"],
                "kind": "SCALAR",
                "longVariableConstraints": [],
                "name": "presto.default.concat",
                "returnType": "varchar",
                "typeVariableConstraints": [],
                "variableArity": true
              },
              "builtInFunctionKind": "ENGINE"
            },
            "returnType": "varchar"
          },
          {
            "@type": "call",
            "arguments": [
              {"@type": "variable", "name": "expr_14", "type": "varchar"},
              {"@type": "variable", "name": "expr_14", "type": "varchar"}
            ],
            "displayName": "concat",
            "functionHandle": {
              "@type": "$static",
              "signature": {
                "argumentTypes": ["varchar", "varchar"],
                "kind": "SCALAR",
                "longVariableConstraints": [],
                "name": "presto.default.concat",
                "returnType": "varchar",
                "typeVariableConstraints": [],
                "variableArity": true
              },
              "builtInFunctionKind": "ENGINE"
            },
            "returnType": "varchar"
          },
          {"@type": "variable", "name": "expr_14", "type": "varchar"}
        ],
        "displayName": "replace",
        "functionHandle": {
          "@type": "$static",
          "signature": {
            "argumentTypes": ["varchar", "varchar", "varchar"],
            "kind": "SCALAR",
            "longVariableConstraints": [],
            "name": "presto.default.replace",
            "returnType": "varchar",
            "typeVariableConstraints": [],
            "variableArity": false
          },
          "builtInFunctionKind": "ENGINE"
        },
        "returnType": "varchar"
      }
  )");
  return serialized;
}

std::shared_ptr<protocol::ProjectNode> makeProjectedSource(
    std::shared_ptr<protocol::PlanNode> source,
    std::string outputName,
    std::shared_ptr<protocol::RowExpression> expression) {
  auto projectNode = std::make_shared<protocol::ProjectNode>();
  projectNode->_type = "com.facebook.presto.sql.planner.plan.ProjectNode";
  projectNode->id = "project";
  projectNode->source = std::move(source);

  protocol::VariableReferenceExpression output;
  output.name = std::move(outputName);
  output.type = "varchar";
  projectNode->assignments.assignments.emplace(
      std::move(output), std::move(expression));
  return projectNode;
}

std::shared_ptr<protocol::ProjectNode> makeProjectedOptionsSource() {
  auto valuesNode = makeValuesNode();
  VELOX_CHECK_NOT_NULL(valuesNode);
  auto projectNode =
      makeProjectedSource(valuesNode, "__rpc_arg", makeCastConstantOptions());
  VELOX_CHECK_NOT_NULL(projectNode);
  const auto& comment = valuesNode->outputVariables.front();
  projectNode->assignments.assignments.emplace(
      comment,
      std::make_shared<protocol::VariableReferenceExpression>(comment));
  return projectNode;
}

std::shared_ptr<protocol::ExchangeNode> makeGatheredOptionsSource() {
  auto source = makeProjectedOptionsSource();
  VELOX_CHECK_NOT_NULL(source);
  auto exchange = std::make_shared<protocol::ExchangeNode>();
  exchange->_type = "com.facebook.presto.sql.planner.plan.ExchangeNode";
  exchange->id = "gather";
  exchange->type = protocol::ExchangeNodeType::GATHER;
  exchange->scope = protocol::ExchangeNodeScope::LOCAL;

  protocol::VariableReferenceExpression comment;
  comment.name = "comment";
  comment.type = "varchar";
  protocol::VariableReferenceExpression options;
  options.name = "__rpc_arg";
  options.type = "varchar";
  exchange->partitioningScheme.outputLayout = {comment, options};
  exchange->sources = {std::move(source)};
  exchange->inputs = {{std::move(comment), std::move(options)}};
  return exchange;
}

std::shared_ptr<protocol::ProjectNode> makeOptionsAfterRpcSource() {
  auto valuesNode = makeValuesNode();
  VELOX_CHECK_NOT_NULL(valuesNode);
  auto optionsProject =
      makeProjectedSource(valuesNode, "expr_14", makeDirectConstantOptions());
  VELOX_CHECK_NOT_NULL(optionsProject);
  const auto& commentVariable = valuesNode->outputVariables.front();
  optionsProject->assignments.assignments.emplace(
      commentVariable,
      std::make_shared<protocol::VariableReferenceExpression>(commentVariable));

  auto localExchange = std::make_shared<protocol::ExchangeNode>();
  localExchange->_type = "com.facebook.presto.sql.planner.plan.ExchangeNode";
  localExchange->id = "gather_before_rpc";
  localExchange->type = protocol::ExchangeNodeType::GATHER;
  localExchange->scope = protocol::ExchangeNodeScope::LOCAL;
  protocol::VariableReferenceExpression optionsVariable;
  optionsVariable.name = "expr_14";
  optionsVariable.type = "varchar";
  localExchange->partitioningScheme.outputLayout = {
      commentVariable, optionsVariable};
  localExchange->sources = {std::move(optionsProject)};
  localExchange->inputs = {{commentVariable, optionsVariable}};

  auto innerRpcNode = makeRPCNode(std::move(localExchange));
  VELOX_CHECK_NOT_NULL(innerRpcNode);
  innerRpcNode->id = "inner_rpc";
  innerRpcNode->outputVariable.name = "__inner_rpc_result";

  auto computedOptions = makeProjectedSource(
      std::move(innerRpcNode), "__rpc_options", makeNestedStringOptions());
  VELOX_CHECK_NOT_NULL(computedOptions);
  computedOptions->id = "project_after_rpc";
  computedOptions->assignments.assignments.emplace(
      commentVariable,
      std::make_shared<protocol::VariableReferenceExpression>(commentVariable));
  return computedOptions;
}

} // namespace

class RPCPlanConverterTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    exec::registerFunctionCallToSpecialForms();
    functions::prestosql::registerAllScalarFunctions("presto.default.");
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

TEST_F(RPCPlanConverterTest, foldsInputIndependentRpcOptions) {
  auto valuesNode = makeValuesNode();
  VELOX_CHECK_NOT_NULL(valuesNode);
  auto rpcNode = makeRPCNode(valuesNode);
  VELOX_CHECK_NOT_NULL(rpcNode);
  for (int32_t i = 0; i < 2; ++i) {
    rpcNode->arguments.push_back(rpcNode->arguments.front());
    rpcNode->argumentColumns.push_back("comment");
  }
  rpcNode->arguments.push_back(makeDirectConstantOptions());
  rpcNode->argumentColumns.push_back("comment");

  VeloxInteractiveQueryPlanConverter converter(queryCtx_.get(), pool_.get());
  const auto veloxPlan = converter.toVeloxQueryPlan(
      std::dynamic_pointer_cast<protocol::PlanNode>(rpcNode),
      nullptr,
      "20260124_042527_00001_gp3te.1.0.0.0");
  VELOX_CHECK_NOT_NULL(veloxPlan);
  const auto* veloxRpcNode =
      dynamic_cast<const core::RPCNode*>(veloxPlan.get());
  ASSERT_NE(veloxRpcNode, nullptr);

  const auto& call = veloxRpcNode->call();
  VELOX_CHECK_NOT_NULL(call);
  ASSERT_EQ(call->inputs().size(), 4);
  const auto* options =
      dynamic_cast<const core::ConstantTypedExpr*>(call->inputs()[3].get());
  ASSERT_NE(options, nullptr);
  const auto value = options->toConstantVector(pool_.get());
  VELOX_CHECK_NOT_NULL(value);
  const auto* strings = value->as<SimpleVector<StringView>>();
  ASSERT_NE(strings, nullptr);
  EXPECT_EQ(
      strings->valueAt(0).str(),
      R"({"inference_backend":"ipnext","streaming_mode":"batch","dispatch_batch_size":7})");
  EXPECT_EQ(veloxRpcNode->streamingMode(), exec_rpc::RPCStreamingMode::kBatch);
  EXPECT_EQ(veloxRpcNode->dispatchBatchSize(), 7);
}

TEST_F(RPCPlanConverterTest, readsConstantRpcOptionsFromSourceProjection) {
  const std::vector<std::shared_ptr<protocol::PlanNode>> sources{
      makeProjectedOptionsSource(),
      makeGatheredOptionsSource(),
  };

  for (const auto& source : sources) {
    auto rpcNode = makeRPCNode(source);
    VELOX_CHECK_NOT_NULL(rpcNode);
    for (int32_t i = 0; i < 2; ++i) {
      rpcNode->arguments.push_back(rpcNode->arguments.front());
      rpcNode->argumentColumns.push_back("comment");
    }
    auto options = std::make_shared<protocol::VariableReferenceExpression>();
    options->_type = "variable";
    options->name = "__rpc_arg";
    options->type = "varchar";
    rpcNode->arguments.push_back(std::move(options));
    rpcNode->argumentColumns.push_back("__rpc_arg");

    VeloxInteractiveQueryPlanConverter converter(queryCtx_.get(), pool_.get());
    const auto veloxPlan = converter.toVeloxQueryPlan(
        std::dynamic_pointer_cast<protocol::PlanNode>(rpcNode),
        nullptr,
        "20260124_042527_00001_gp3te.1.0.0.0");
    VELOX_CHECK_NOT_NULL(veloxPlan);
    const auto* veloxRpcNode =
        dynamic_cast<const core::RPCNode*>(veloxPlan.get());
    ASSERT_NE(veloxRpcNode, nullptr);

    const auto& call = veloxRpcNode->call();
    VELOX_CHECK_NOT_NULL(call);
    const auto* constantOptions = dynamic_cast<const core::ConstantTypedExpr*>(
        call->inputs().at(3).get());
    ASSERT_NE(constantOptions, nullptr);
    EXPECT_EQ(
        veloxRpcNode->streamingMode(), exec_rpc::RPCStreamingMode::kBatch);
    EXPECT_EQ(veloxRpcNode->dispatchBatchSize(), 7);
  }
}

TEST_F(RPCPlanConverterTest, foldsOptionsAcrossPriorRpcNode) {
  auto rpcNode = makeRPCNode(makeOptionsAfterRpcSource());
  VELOX_CHECK_NOT_NULL(rpcNode);
  for (int32_t i = 0; i < 2; ++i) {
    rpcNode->arguments.push_back(rpcNode->arguments.front());
    rpcNode->argumentColumns.push_back("comment");
  }
  auto optionsVariable =
      std::make_shared<protocol::VariableReferenceExpression>();
  optionsVariable->_type = "variable";
  optionsVariable->name = "__rpc_options";
  optionsVariable->type = "varchar";
  rpcNode->arguments.push_back(std::move(optionsVariable));
  rpcNode->argumentColumns.push_back("__rpc_options");
  rpcNode->streamingMode = protocol::RPCNodeStreamingMode::BATCH;

  VeloxInteractiveQueryPlanConverter converter(queryCtx_.get(), pool_.get());
  const auto veloxPlan = converter.toVeloxQueryPlan(
      std::dynamic_pointer_cast<protocol::PlanNode>(rpcNode),
      nullptr,
      "20260124_042527_00001_gp3te.1.0.0.0");
  VELOX_CHECK_NOT_NULL(veloxPlan);
  const auto* veloxRpcNode =
      dynamic_cast<const core::RPCNode*>(veloxPlan.get());
  ASSERT_NE(veloxRpcNode, nullptr);

  const auto& call = veloxRpcNode->call();
  VELOX_CHECK_NOT_NULL(call);
  const auto* constantOptions =
      dynamic_cast<const core::ConstantTypedExpr*>(call->inputs().at(3).get());
  ASSERT_NE(constantOptions, nullptr);
  const auto value = constantOptions->toConstantVector(pool_.get());
  VELOX_CHECK_NOT_NULL(value);
  const auto* strings = value->as<SimpleVector<StringView>>();
  ASSERT_NE(strings, nullptr);
  const std::string expectedOptions =
      R"({"inference_backend":"ipnext","streaming_mode":"batch","dispatch_batch_size":7})";
  EXPECT_EQ(strings->valueAt(0).str(), expectedOptions);
}

TEST_F(RPCPlanConverterTest, doesNotFoldUnallowlistedProjectedCall) {
  auto rpcNode = makeRPCNode(makeProjectedSource(
      makeValuesNode(), "__rpc_arg", makeNestedRpcArgument()));
  VELOX_CHECK_NOT_NULL(rpcNode);
  rpcNode->arguments = {makeDirectConstantOptions()};
  rpcNode->argumentColumns = {"__rpc_arg"};

  VeloxInteractiveQueryPlanConverter converter(queryCtx_.get(), pool_.get());
  const auto veloxPlan = converter.toVeloxQueryPlan(
      std::dynamic_pointer_cast<protocol::PlanNode>(rpcNode),
      nullptr,
      "20260124_042527_00001_gp3te.1.0.0.0");
  VELOX_CHECK_NOT_NULL(veloxPlan);
  const auto* veloxRpcNode =
      dynamic_cast<const core::RPCNode*>(veloxPlan.get());
  ASSERT_NE(veloxRpcNode, nullptr);

  const auto& call = veloxRpcNode->call();
  VELOX_CHECK_NOT_NULL(call);
  const auto* field = dynamic_cast<const core::FieldAccessTypedExpr*>(
      call->inputs().front().get());
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->name(), "__rpc_arg");
}

TEST_F(RPCPlanConverterTest, preservesProjectedCastSemantics) {
  auto rpcNode = makeRPCNode(makeProjectedSource(
      makeValuesNode(), "__rpc_arg", makeBigintToVarcharCast()));
  VELOX_CHECK_NOT_NULL(rpcNode);
  auto argument = std::make_shared<protocol::VariableReferenceExpression>();
  argument->_type = "variable";
  argument->name = "__rpc_arg";
  argument->type = "varchar";
  rpcNode->arguments = {std::move(argument)};
  rpcNode->argumentColumns = {"__rpc_arg"};

  VeloxInteractiveQueryPlanConverter converter(queryCtx_.get(), pool_.get());
  const auto veloxPlan = converter.toVeloxQueryPlan(
      std::dynamic_pointer_cast<protocol::PlanNode>(rpcNode),
      nullptr,
      "20260124_042527_00001_gp3te.1.0.0.0");
  VELOX_CHECK_NOT_NULL(veloxPlan);
  const auto* veloxRpcNode =
      dynamic_cast<const core::RPCNode*>(veloxPlan.get());
  ASSERT_NE(veloxRpcNode, nullptr);

  const auto& call = veloxRpcNode->call();
  VELOX_CHECK_NOT_NULL(call);
  const auto* constant = dynamic_cast<const core::ConstantTypedExpr*>(
      call->inputs().front().get());
  ASSERT_NE(constant, nullptr);
  EXPECT_EQ(constant->type(), VARCHAR());
  const auto value = constant->toConstantVector(pool_.get());
  VELOX_CHECK_NOT_NULL(value);
  const auto* strings = value->as<SimpleVector<StringView>>();
  ASSERT_NE(strings, nullptr);
  EXPECT_EQ(strings->valueAt(0).str(), "10");
}

TEST_F(RPCPlanConverterTest, doesNotEvaluateNestedRpcArgument) {
  auto innerRpcNode = makeRPCNode(makeValuesNode());
  VELOX_CHECK_NOT_NULL(innerRpcNode);
  innerRpcNode->functionName = "nested_rpc_test";
  innerRpcNode->outputVariable.name = "__inner_rpc_result";

  auto projectedSource = makeProjectedSource(
      innerRpcNode, "__projected_inner_rpc_result", makeCastInnerRpcResult());
  auto outerRpcNode = makeRPCNode(std::move(projectedSource));
  VELOX_CHECK_NOT_NULL(outerRpcNode);
  outerRpcNode->functionName = "nested_rpc_test";
  outerRpcNode->arguments = {makeDirectConstantOptions()};
  outerRpcNode->argumentColumns = {"__projected_inner_rpc_result"};
  outerRpcNode->outputVariable.name = "__outer_rpc_result";

  VeloxInteractiveQueryPlanConverter converter(queryCtx_.get(), pool_.get());
  const auto veloxPlan = converter.toVeloxQueryPlan(
      std::dynamic_pointer_cast<protocol::PlanNode>(outerRpcNode),
      nullptr,
      "20260124_042527_00001_gp3te.1.0.0.0");
  VELOX_CHECK_NOT_NULL(veloxPlan);
  const auto* veloxRpcNode =
      dynamic_cast<const core::RPCNode*>(veloxPlan.get());
  ASSERT_NE(veloxRpcNode, nullptr);
  const auto& call = veloxRpcNode->call();
  VELOX_CHECK_NOT_NULL(call);
  const auto* field = dynamic_cast<const core::FieldAccessTypedExpr*>(
      call->inputs().front().get());
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->name(), "__projected_inner_rpc_result");
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
