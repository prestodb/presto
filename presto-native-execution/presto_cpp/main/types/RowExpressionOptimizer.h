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

#include <proxygen/lib/http/HTTPMessage.h>
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "presto_cpp/main/types/RowExpressionConverter.h"
#include "velox/expression/Expr.h"

using namespace facebook::velox;

namespace facebook::presto::expression {

enum OptimizerLevel {
  OPTIMIZED,
  EVALUATED,
};

/// Helper class to optimize Presto protocol RowExpressions sent along the REST
/// endpoint 'v1/expressions' to the sidecar.
class RowExpressionOptimizer {
 public:
  explicit RowExpressionOptimizer(memory::MemoryPool* pool)
      : pool_(pool),
        veloxExprConverter_(pool, &typeParser_),
        rowExpressionConverter_(RowExpressionConverter(pool)) {}

  /// Optimizes all expressions from the input json array. If the expression
  /// optimization fails for any of the input expressions, the second value in
  /// the returned pair is set to false and the returned json contains the
  /// exception. Otherwise, the returned json is an array of optimized Presto
  /// protocol RowExpressions.
  std::pair<json, bool> optimize(
      const proxygen::HTTPHeaders& httpHeaders,
      const json::array_t& input);

 private:
  /// Comparator for core::TypedExprPtr; used to deduplicate arguments to
  /// COALESCE special form expression.
  struct TypedExprComparator {
    bool operator()(const core::TypedExprPtr& a, const core::TypedExprPtr& b)
        const {
      return a->hash() < b->hash();
    }
  };

  /// Constant folds all possible subtrees of the input expression and returns a
  /// constant folded TypedExpr.
  core::TypedExprPtr constantFold(const core::TypedExprPtr& expr);

  /// Evaluates Presto protocol SpecialFormExpression `AND`, 'OR' when the
  /// inputs can be constant folded to `true`, `false`, or `NULL`.
  template <bool isAnd>
  core::TypedExprPtr optimizeConjunctExpression(
      const core::CallTypedExprPtr& expr);

  /// Evaluates Presto protocol SpecialFormExpression `IF` when the condition
  /// can be constant folded to `true` or `false`.
  core::TypedExprPtr optimizeIfExpression(const core::CallTypedExprPtr& expr);

  /// Adds input expression to the COALESCE expression's arguments. Returns
  /// input if it is a non-NULL constant, returns nullptr otherwise.
  core::TypedExprPtr addCoalesceArgument(
      const core::TypedExprPtr& input,
      std::set<core::TypedExprPtr, TypedExprComparator>& optimizedTypedExprs,
      std::vector<core::TypedExprPtr>& optimizedRowExpressions);

  /// Optimizes Presto protocol SpecialFormExpression `COALESCE` by removing
  /// `NULL`s from the argument list. Returns the first non-NULL constant
  /// argument to COALESCE. Otherwise NULLs and duplicate arguments are skipped
  /// from optimizedArguments and nullptr is returned.
  core::TypedExprPtr optimizeCoalesceExpression(
      const core::CallTypedExprPtr& expr,
      std::set<core::TypedExprPtr, TypedExprComparator>& inputTypedExprSet,
      std::vector<core::TypedExprPtr>& deduplicatedInputs);

  /// Optimizes expression using rules borrowed from Presto function
  /// visitSpecialForm() in RowExpressionInterpreter.java.
  core::TypedExprPtr optimizeExpression(const core::TypedExprPtr& expr);

  /// Optimizes and constant folds each expression from input json array and
  /// returns an array of expressions that are optimized and constant folded.
  /// For special form expressions, certain logical optimizations are performed
  /// in function `optimizeSpecialForm`. All possible subtrees of the expression
  /// are then constant folded in function
  /// `optimizeExpression`. The optimized expression is also evaluated if the
  /// optimization level in the header of http request made to 'v1/expressions'
  /// is 'EVALUATED'. `optimizeExpression` uses `RowExpressionConverter` to
  /// convert velox expression(s) to their corresponding Presto protocol
  /// RowExpression(s).
  json::array_t optimizeExpressions(const json::array_t& input);

  memory::MemoryPool* pool_;
  std::shared_ptr<core::QueryCtx> queryCtx_;
  TypeParser typeParser_;
  VeloxExprConverter veloxExprConverter_;
  RowExpressionConverter rowExpressionConverter_;
};
} // namespace facebook::presto::expression
