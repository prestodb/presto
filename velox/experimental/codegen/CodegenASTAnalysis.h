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

#include "velox/core/PlanNode.h"
#include "velox/experimental/codegen/code_generator/ExprCodeGenerator.h"
#include "velox/experimental/codegen/library_loader/NativeLibraryLoader.h"
#include "velox/experimental/codegen/transform/PlanNodeTransform.h"
#include "velox/experimental/codegen/transform/utils/adapters.h"
#include "velox/experimental/codegen/transform/utils/utils.h"
#include "velox/experimental/codegen/utils/timer/NestedScopedTimer.h"

namespace facebook::velox::codegen {

using velox::codegen::ExprCodeGenerator;
using velox::core::ITypedExpr;
using velox::transform::PlanNodeAnalysis;
using velox::transform::utils::dfsApply;

namespace {
class generateAstVisitor {
 public:
  generateAstVisitor(
      std::map<std::shared_ptr<const ITypedExpr>, CodegenASTNode>& astNodes_,
      const UDFManager& udfManager,
      bool useSymbolsForArithmetic,
      DefaultScopedTimer::EventSequence& eventSequence)
      : astNodes_(astNodes_),
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
  std::optional<CodegenASTNode> generateAst(
      const std::shared_ptr<const core::ITypedExpr>& expr,
      const RowType& inputType) {
    auto outputType =
        std::dynamic_pointer_cast<const velox::RowType>(expr->type());

    try {
      // convert velox expression tree to codegen AST
      auto codegenExprTree =
          generator_.convertVeloxExpressionToCodegenAST(expr, inputType);

      // validate the generated tree
      codegenExprTree->validate();

      return codegenExprTree;

    } catch (const std::runtime_error& error) {
      LOG(INFO) << fmt::format("Expression failed {}", error.what());
      return {};
    };
  }

  void visitProjection(const core::ProjectNode& projection) {
    const auto& inputType = *projection.sources()[0]->outputType().get();
    for (const auto& expr : projection.projections()) {
      auto ast = generateAst(expr, inputType);
      if (ast.has_value()) {
        astNodes_[expr] = ast.value();
      }
    }
  }

  void visitFilter(const core::FilterNode& filterNode) {
    const auto& inputType = *filterNode.sources()[0]->outputType().get();
    auto ast = generateAst(filterNode.filter(), inputType);
    if (ast.has_value()) {
      astNodes_[filterNode.filter()] = ast.value();
    }
  }

  std::map<std::shared_ptr<const ITypedExpr>, CodegenASTNode>& astNodes_;
  ExprCodeGenerator generator_;
  [[maybe_unused]] DefaultScopedTimer::EventSequence& eventSequence_;
};

} // namespace

/// AST generation Analysis
/// Scan a given plan node and generate a map ITypedExpr -> CodegenASTNode
// for every encountered expression
class CodegenASTAnalysis : PlanNodeAnalysis {
 public:
  CodegenASTAnalysis(
      const UDFManager& udfManager,
      bool useSymbolsForArithmetic,
      DefaultScopedTimer::EventSequence& eventSequence)
      : astNodes_(),
        visitor_(astNodes_, udfManager, useSymbolsForArithmetic, eventSequence),
        eventSequence_(eventSequence) {}

  ~CodegenASTAnalysis() override {}

  void run(const core::PlanNode& rootNode) override {
    DefaultScopedTimer timer("CodegenASTAnalysis", eventSequence_);
    dfsApply(rootNode, [this](const auto& planNode) {
      this->visitor_.visit(planNode);
    });
  }
  const std::map<std::shared_ptr<const ITypedExpr>, CodegenASTNode>& nodes()
      const {
    return astNodes_;
  }

 private:
  std::map<std::shared_ptr<const ITypedExpr>, CodegenASTNode> astNodes_;
  generateAstVisitor visitor_;
  [[maybe_unused]] DefaultScopedTimer::EventSequence& eventSequence_;
};
} // namespace facebook::velox::codegen
