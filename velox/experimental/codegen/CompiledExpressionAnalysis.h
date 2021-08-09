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
#pragma once
// This file defines the tranasformation pass that analyses pre-generated
// codegen-ast trees and generates the code for each of the supported
// expressions, in addition to some other analysis about the
// expressions.

#include <functional>
#include "velox/core/PlanNode.h"
#include "velox/experimental/codegen/CodegenASTAnalysis.h"
#include "velox/experimental/codegen/ast/AST.h"
#include "velox/experimental/codegen/ast/ASTAnalysis.h"
#include "velox/experimental/codegen/ast/ASTNode.h"
#include "velox/experimental/codegen/transform/PlanNodeTransform.h"

namespace facebook::velox::codegen {

using velox::core::ITypedExpr;
using velox::transform::PlanNodeAnalysis;

struct CompiledExpressionAnalysisResult {
  /// TOOD: Merge
  /// std::map<std::string,std::pair<size_t,facebook::velox::TypePtr>> into
  /// GeneratedExpressionStruct
  std::map<std::shared_ptr<const ITypedExpr>, GeneratedExpressionStruct>
      generatedCode_;
  std::map<core::PlanNodeId, bool> isDefaultNull_;
  std::map<core::PlanNodeId, bool> isDefaultNullStrict_;
};

namespace {

/// TODO: create a generic base visitor class for PlanNode
class GenerateCompiledExpressionVisitor {
 public:
  GenerateCompiledExpressionVisitor(
      const std::map<std::shared_ptr<const ITypedExpr>, CodegenASTNode>&
          astNodes,
      CompiledExpressionAnalysisResult& result,
      const UDFManager& udfManager,
      bool useSymbolsForArithmetic,
      DefaultScopedTimer::EventSequence& eventSequence,
      bool enableDefaultNullOpt = true,
      bool enableFilterDefaultNull = true)
      : enableDefaultNullOpt_(enableDefaultNullOpt),
        enableFilterDefaultNull_(enableFilterDefaultNull),
        astNodes_(astNodes),
        result_(result),
        generator_(udfManager, useSymbolsForArithmetic, false),
        eventSequence_(eventSequence) {}

  void visit(const core::PlanNode& planNode) {
    if (auto projectNode = dynamic_cast<const core::ProjectNode*>(&planNode)) {
      visitProjection(*projectNode);
    } else if (
        auto filterNode = dynamic_cast<const core::FilterNode*>(&planNode)) {
      visitFilter(*filterNode);
    } else if (
        auto valueNode = dynamic_cast<const core::ValuesNode*>(&planNode)) {
    } else if (
        dynamic_cast<const core::TableWriteNode*>(&planNode) ||
        dynamic_cast<const core::TableScanNode*>(&planNode)) {
      // Those plan nodes do not have expressions to transform
    } else {
      throw std::logic_error("Unknown node type:\n" + planNode.toString());
    }
  }

 private:
  bool enableDefaultNullOpt_ = true;

  // enable the extended default null analysis for filter
  bool enableFilterDefaultNull_ = true;

  const std::map<std::shared_ptr<const ITypedExpr>, CodegenASTNode>& astNodes_;
  CompiledExpressionAnalysisResult& result_;
  ExprCodeGenerator generator_;
  DefaultScopedTimer::EventSequence eventSequence_;

  const ASTNodePtr getCodegenAST(
      const std::shared_ptr<const core::ITypedExpr>& expr) {
    auto astIt = astNodes_.find(expr);
    if (astIt == astNodes_.end()) {
      return nullptr;
    }
    return astIt->second;
  }

  void generateCompiledExpression(
      const std::shared_ptr<const core::ITypedExpr>& expr,
      bool markAllInputsNotNullable) {
    auto ast = getCodegenAST(expr);
    if (!ast) {
      return;
    }

    auto outputExpression = std::make_shared<MakeRowExpression>(
        ast->typePtr(), std::vector<ASTNodePtr>{ast});

    if (markAllInputsNotNullable) {
      ast->markAllInputsNotNullable();
    }

    ast->propagateNullability();

    auto codeStruct = generator_.codegenExpression(*outputExpression.get());
    result_.generatedCode_.emplace(expr, codeStruct);
  }

  void visitProjection(const core::ProjectNode& projection) {
    VELOX_CHECK(!projection.sources().empty());
    VELOX_CHECK(projection.sources()[0] != nullptr);

    // check if the generated fused expression is default null.
    // for now its a property of the whole operator since we fuse all
    // expressions into one expression.
    bool isDefaultNull = false;
    bool isDefaultNullStrict = false;

    // currently when projection is after a filter, then only use default null
    // for filter
    if (enableDefaultNullOpt_ &&
        std::dynamic_pointer_cast<const core::FilterNode>(
            projection.sources()[0]) == nullptr) {
      isDefaultNull = true;
      isDefaultNullStrict = true;
      std::set<std::string> inputAttrs;
      auto& cxt = generator_.getContext();
      auto firstExpr = true;
      for (const auto& expr : projection.projections()) {
        auto ast = getCodegenAST(expr);
        if (!ast) {
          continue;
        }
        cxt.columnNameToIndexMap().clear();
        cxt.setComputeInputTypeDuringCodegen(true);
        ast->generateCode(cxt, "outputTuple");

        std::set<std::string> curAttrs;
        for (auto& [name, _] : cxt.columnNameToIndexMap()) {
          curAttrs.insert(name);
        }

        if (firstExpr) {
          // first column, define the attribute set
          inputAttrs = curAttrs;
          firstExpr = false;
        } else {
          // check if it's the same set of attributes
          if (inputAttrs != curAttrs) {
            isDefaultNull = isDefaultNullStrict = false;
            break;
          }
        }

        isDefaultNull = isDefaultNull && analysis::isDefaultNull(*ast.get());
        isDefaultNullStrict =
            isDefaultNullStrict && analysis::isDefaultNullStrict(*ast.get());
      }

      result_.isDefaultNull_[projection.id()] = isDefaultNull;
      result_.isDefaultNullStrict_[projection.id()] = isDefaultNullStrict;
    }
    for (const auto& expr : projection.projections()) {
      // when the compiled expression is default null we generate code
      // assuming all inputs are not nullable and the vector function only
      // executes not nullable inputs
      generateCompiledExpression(expr, isDefaultNullStrict || isDefaultNull);
    }
  }

  void visitFilter(const core::FilterNode& filterNode) {
    VELOX_CHECK(!filterNode.sources().empty());
    VELOX_CHECK(filterNode.sources()[0] != nullptr);
    auto ast = getCodegenAST(filterNode.filter());
    if (!ast) {
      return;
    }
    bool isDefaultNull = false;
    if (enableDefaultNullOpt_) {
      isDefaultNull = result_.isDefaultNull_[filterNode.id()] =
          enableFilterDefaultNull_ ? analysis::isFilterDefaultNull(*ast.get())
                                   : analysis::isDefaultNull(*ast.get());
      // filter doesn't care default null strict since it doesn't use
      // VectorWriter
    }
    generateCompiledExpression(filterNode.filter(), isDefaultNull);
  }
};
} // namespace

/// This analysis builds generates GeneratedExpressionStruct for each
/// expressions in the planNode.
class CompiledExpressionAnalysis : PlanNodeAnalysis {
 public:
  CompiledExpressionAnalysis(
      const UDFManager& udfManager,
      bool useSymbolsForArithmetic,
      DefaultScopedTimer::EventSequence& eventSequence,
      bool enableDefaultNullOpt = true,
      bool enableFilterDefaultNull = true)
      : udfManager_(udfManager),
        useSymbolsForArithmetic_(useSymbolsForArithmetic),
        eventSequence_(eventSequence),
        enableDefaultNullOpt_(enableDefaultNullOpt),
        enableFilterDefaultNull_(enableFilterDefaultNull) {}

  ~CompiledExpressionAnalysis() override {}

  void run(const core::PlanNode& plan) override {
    DefaultScopedTimer timer("CompiledExpressionAnalysis", eventSequence_);
    CodegenASTAnalysis astAnalysis(
        udfManager_, useSymbolsForArithmetic_, eventSequence_);
    astAnalysis.run(plan);
    GenerateCompiledExpressionVisitor visitor(
        astAnalysis.nodes(),
        result_,
        udfManager_,
        useSymbolsForArithmetic_,
        eventSequence_,
        enableDefaultNullOpt_,
        enableFilterDefaultNull_);
    dfsApply(
        plan, [&visitor](const auto& planNode) { visitor.visit(planNode); });
  }

  const auto& results() const {
    return result_;
  }

 private:
  CompiledExpressionAnalysisResult result_;
  const UDFManager& udfManager_;
  bool useSymbolsForArithmetic_;
  DefaultScopedTimer::EventSequence& eventSequence_;
  bool enableDefaultNullOpt_, enableFilterDefaultNull_;
};
} // namespace facebook::velox::codegen
