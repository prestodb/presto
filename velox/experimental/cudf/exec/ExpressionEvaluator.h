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

#include "velox/core/Expressions.h"
#include "velox/expression/Expr.h"
#include "velox/type/Type.h"

#include <cudf/ast/expressions.hpp>

#include <memory>
#include <string>
#include <vector>

namespace facebook::velox::cudf_velox {

// Pre-compute instructions for the expression,
// for ops that are not supported by cudf::ast
struct PrecomputeInstruction {
  int dependent_column_index;
  std::string ins_name;
  int new_column_index;

  // Constructor to initialize the struct with values
  PrecomputeInstruction(int depIndex, const std::string& name, int newIndex)
      : dependent_column_index(depIndex),
        ins_name(name),
        new_column_index(newIndex) {}
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
};

} // namespace facebook::velox::cudf_velox
