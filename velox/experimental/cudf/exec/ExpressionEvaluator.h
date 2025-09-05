/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include "velox/expression/Expr.h"
#include "velox/type/Filter.h"
#include "velox/type/Subfield.h"
#include "velox/type/Type.h"

#include <cudf/ast/expressions.hpp>
#include <cudf/column/column.hpp>

#include <memory>
#include <string>
#include <variant>
#include <vector>

namespace facebook::velox::cudf_velox {

// Forward declaration to allow usage in PrecomputeInstruction
struct CudfExpressionNode;

// Pre-compute instructions for the expression,
// for ops that are not supported by cudf::ast
struct PrecomputeInstruction {
  int dependent_column_index;
  std::string ins_name;
  int new_column_index;
  std::vector<int> nested_dependent_column_indices;
  // Optional: compiled cudf expression node for recursive/non-AST eval
  std::shared_ptr<CudfExpressionNode> cudf_node;

  // Constructor to initialize the struct with values
  PrecomputeInstruction(
      int depIndex,
      const std::string& name,
      int newIndex,
      const std::shared_ptr<CudfExpressionNode>& node = nullptr)
      : dependent_column_index(depIndex),
        ins_name(name),
        new_column_index(newIndex),
        cudf_node(node) {}

  // TODO (dm): This two ctor situation is crazy.
  PrecomputeInstruction(
      int depIndex,
      const std::string& name,
      int newIndex,
      const std::vector<int>& nestedIndices,
      const std::shared_ptr<CudfExpressionNode>& node = nullptr)
      : dependent_column_index(depIndex),
        ins_name(name),
        new_column_index(newIndex),
        nested_dependent_column_indices(nestedIndices),
        cudf_node(node) {}
};

// Holds either a non-owning cudf::column_view (zero-copy) or an owning
// cudf::column (materialised result).
using ColumnOrView =
    std::variant<cudf::column_view, std::unique_ptr<cudf::column>>;

// Helper to always obtain a column_view.
inline cudf::column_view asView(ColumnOrView& holder) {
  return std::visit(
      [](auto& h) -> cudf::column_view {
        using T = std::decay_t<decltype(h)>;
        if constexpr (std::is_same_v<T, cudf::column_view>) {
          return h;
        } else {
          return h->view();
        }
      },
      holder);
}

class CudfFunction {
 public:
  virtual ~CudfFunction() = default;
  virtual ColumnOrView eval(
      std::vector<ColumnOrView>& inputColumns,
      rmm::cuda_stream_view stream,
      rmm::device_async_resource_ref mr) const = 0;
};

using CudfFunctionFactory = std::function<std::shared_ptr<CudfFunction>(
    const std::string& name,
    const std::shared_ptr<velox::exec::Expr>& expr)>;

bool registerCudfFunction(
    const std::string& name,
    CudfFunctionFactory factory,
    bool overwrite = true);

bool registerBuiltinFunctions(const std::string& prefix);

struct CudfExpressionNode {
  std::shared_ptr<velox::exec::Expr> expr;
  std::shared_ptr<CudfFunction> function;
  std::vector<std::shared_ptr<CudfExpressionNode>> subexpressions;

  static std::shared_ptr<CudfExpressionNode> create(
      const std::shared_ptr<velox::exec::Expr>& expr);

  // TODO (dm): A storage for keeping results in case this is a multiply
  // referenced subexpression (to do CSE)

  ColumnOrView eval(
      std::vector<std::unique_ptr<cudf::column>>& inputTableColumns,
      const RowTypePtr& inputRowSchema,
      rmm::cuda_stream_view stream,
      rmm::device_async_resource_ref mr);
};

cudf::ast::expression const& createAstTree(
    const std::shared_ptr<velox::exec::Expr>& expr,
    cudf::ast::tree& tree,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    const RowTypePtr& inputRowSchema,
    std::vector<PrecomputeInstruction>& precomputeInstructions);

cudf::ast::expression const& createAstTree(
    const std::shared_ptr<velox::exec::Expr>& expr,
    cudf::ast::tree& tree,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    const RowTypePtr& leftRowSchema,
    const RowTypePtr& rightRowSchema,
    std::vector<PrecomputeInstruction>& leftPrecomputeInstructions,
    std::vector<PrecomputeInstruction>& rightPrecomputeInstructions);

void addPrecomputedColumns(
    std::vector<std::unique_ptr<cudf::column>>& inputTableColumns,
    const std::vector<PrecomputeInstruction>& precomputeInstructions,
    const std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    rmm::cuda_stream_view stream);

// Convert subfield filters to cudf AST
cudf::ast::expression const& createAstFromSubfieldFilter(
    const common::Subfield& subfield,
    const common::Filter& filter,
    cudf::ast::tree& tree,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    const RowTypePtr& inputRowSchema);

// Build a single AST expression representing logical AND of all filters in
// 'subfieldFilters'. The resulting expression reference is owned by the passed
// 'tree'.
cudf::ast::expression const& createAstFromSubfieldFilters(
    const common::SubfieldFilters& subfieldFilters,
    cudf::ast::tree& tree,
    std::vector<std::unique_ptr<cudf::scalar>>& scalars,
    const RowTypePtr& inputRowSchema);

// Evaluates the expression tree
class ExpressionEvaluator {
 public:
  ExpressionEvaluator() = default;
  // Converts velox expressions to cudf::ast::tree, scalars and
  // precompute instructions and stores them
  ExpressionEvaluator(
      const std::vector<std::shared_ptr<velox::exec::Expr>>& exprs,
      const RowTypePtr& inputRowSchema);

  // Evaluates the expression tree for the given input columns
  std::vector<std::unique_ptr<cudf::column>> compute(
      std::vector<std::unique_ptr<cudf::column>>& inputTableColumns,
      rmm::cuda_stream_view stream,
      rmm::device_async_resource_ref mr);

  void close();

  static bool canBeEvaluated(
      const std::vector<std::shared_ptr<velox::exec::Expr>>& exprs);

 private:
  std::vector<cudf::ast::tree> exprAst_;
  std::vector<std::unique_ptr<cudf::scalar>> scalars_;
  // instruction on dependent column to get new column index on non-ast
  // supported operations in expressions
  // <dependent_column_index, "instruction", new_column_index>
  std::vector<PrecomputeInstruction> precomputeInstructions_;
  RowTypePtr inputRowSchema_;
};

} // namespace facebook::velox::cudf_velox
