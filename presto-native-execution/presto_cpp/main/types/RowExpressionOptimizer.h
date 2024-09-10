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

using namespace facebook::velox;

namespace facebook::presto::expression {

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
  /// Converts Presto protocol RowExpression into a velox expression with
  /// constant folding enabled during velox expression compilation.
  exec::ExprPtr compileExpression(const RowExpressionPtr& inputRowExpr);

  /// Evaluates Presto protocol SpecialFormExpression `AND` when the inputs can
  /// be constant folded to `true`, `false`, or `NULL`.
  RowExpressionPtr optimizeAndSpecialForm(
      const SpecialFormExpressionPtr& specialFormExpr);

  /// Evaluates Presto protocol SpecialFormExpression `IF` when the condition
  /// can be constant folded to `true` or `false`.
  RowExpressionPtr optimizeIfSpecialForm(
      const SpecialFormExpressionPtr& specialFormExpr);

  /// Evaluates Presto protocol SpecialFormExpression `IS_NULL` when the input
  /// can be constant folded.
  RowExpressionPtr optimizeIsNullSpecialForm(
      const SpecialFormExpressionPtr& specialFormExpr);

  /// Evaluates Presto protocol SpecialFormExpression `OR` when the inputs can
  /// be constant folded to `true`, `false`, or `NULL`.
  RowExpressionPtr optimizeOrSpecialForm(
      const SpecialFormExpressionPtr& specialFormExpr);

  /// Optimizes Presto protocol SpecialFormExpression `COALESCE` by removing
  /// `NULL`s from the argument list.
  RowExpressionPtr optimizeCoalesceSpecialForm(
      const SpecialFormExpressionPtr& specialFormExpr);

  /// Optimizes Presto protocol SpecialFormExpressions using optimization rules
  /// borrowed from Presto function visitSpecialForm() in
  /// RowExpressionInterpreter.java.
  RowExpressionPtr optimizeSpecialForm(
      const SpecialFormExpressionPtr& specialFormExpr);

  /// Evaluates non-deterministic expressions with constant inputs.
  json evaluateNonDeterministicConstantExpr(exec::ExprSet& exprSet);

  /// Optimizes and constant folds each expression from input json array and
  /// returns an array of expressions that are optimized and constant folded.
  /// Each expression in the input array is optimized with helper functions
  /// `optimizeSpecialForm` (applicable only for special form expressions) and
  /// `optimizeExpression`. The optimized expression is also evaluated if the
  /// optimization level in the header of http request made to 'v1/expressions'
  /// is 'EVALUATED'. `optimizeExpression` uses `RowExpressionConverter` to
  /// convert velox expression(s) to their corresponding Presto protocol
  /// RowExpression(s).
  json::array_t optimizeExpressions(
      const json::array_t& input,
      const std::string& optimizationLevel);

  memory::MemoryPool* pool_;
  std::unique_ptr<core::ExecCtx> execCtx_;
  TypeParser typeParser_;
  VeloxExprConverter veloxExprConverter_;
  RowExpressionConverter rowExpressionConverter_;
};
} // namespace facebook::presto::expression
