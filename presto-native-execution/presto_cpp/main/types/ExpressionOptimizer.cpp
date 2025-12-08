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

#include "presto_cpp/main/types/ExpressionOptimizer.h"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Exception.h"
#include "presto_cpp/main/http/HttpServer.h"
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "presto_cpp/main/types/TypeParser.h"
#include "presto_cpp/main/types/VeloxToPrestoExpr.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/core/Expressions.h"
#include "velox/expression/Expr.h"
#include "velox/expression/ExprOptimizer.h"

namespace facebook::presto::expression {

namespace {

static const velox::expression::MakeFailExpr kMakeFailExpr =
    [](const std::string& error,
       const velox::TypePtr& type) -> velox::core::TypedExprPtr {
  return std::make_shared<velox::core::CastTypedExpr>(
      type,
      std::vector<velox::core::TypedExprPtr>{
          std::make_shared<velox::core::CallTypedExpr>(
              velox::UNKNOWN(),
              std::vector<velox::core::TypedExprPtr>{
                  std::make_shared<velox::core::ConstantTypedExpr>(
                      velox::VARCHAR(), error)},
              fmt::format(
                  "{}fail",
                  SystemConfig::instance()->prestoDefaultNamespacePrefix()))},
      false);
};

// Tries to evaluate `expr`, irrespective of its determinism, to a constant
// value.
velox::VectorPtr tryEvaluateToConstant(
    const velox::core::TypedExprPtr& expr,
    velox::core::QueryCtx* queryCtx,
    velox::memory::MemoryPool* pool) {
  auto data =
      velox::BaseVector::create<velox::RowVector>(velox::ROW({}), 1, pool);
  velox::core::ExecCtx execCtx{pool, queryCtx};
  velox::exec::ExprSet exprSet({expr}, &execCtx);
  velox::exec::EvalCtx evalCtx(&execCtx, &exprSet, data.get());

  const velox::SelectivityVector singleRow(1);
  std::vector<velox::VectorPtr> results(1);
  exprSet.eval(singleRow, evalCtx, results);
  return results.at(0);
}

protocol::RowExpressionOptimizationResult optimizeExpression(
    const RowExpressionPtr& input,
    OptimizerLevel& optimizerLevel,
    const VeloxExprConverter& prestoToVeloxConverter,
    const expression::VeloxToPrestoExprConverter& veloxToPrestoConverter,
    velox::core::QueryCtx* queryCtx,
    velox::memory::MemoryPool* pool) {
  protocol::RowExpressionOptimizationResult result;
  const auto expr = prestoToVeloxConverter.toVeloxExpr(input);
  auto optimized =
      velox::expression::optimize(expr, queryCtx, pool, kMakeFailExpr);

  if (optimizerLevel == OptimizerLevel::kEvaluated) {
    try {
      const auto evalResult = tryEvaluateToConstant(optimized, queryCtx, pool);
      optimized = std::make_shared<velox::core::ConstantTypedExpr>(evalResult);
    } catch (const velox::VeloxException& e) {
      result.expressionFailureInfo =
          toNativeSidecarFailureInfo(translateToPrestoException(e));
      result.optimizedExpression = nullptr;
      return result;
    } catch (const std::exception& e) {
      result.expressionFailureInfo =
          toNativeSidecarFailureInfo(translateToPrestoException(e));
      result.optimizedExpression = nullptr;
      return result;
    }
  }

  result.optimizedExpression =
      veloxToPrestoConverter.getRowExpression(optimized, input);
  return result;
}

} // namespace

std::vector<protocol::RowExpressionOptimizationResult> optimizeExpressions(
    const std::vector<RowExpressionPtr>& input,
    const std::string& timezone,
    OptimizerLevel& optimizerLevel,
    velox::core::QueryCtx* queryCtx,
    velox::memory::MemoryPool* pool) {
  TypeParser typeParser;
  const VeloxExprConverter prestoToVeloxConverter(pool, &typeParser);
  const expression::VeloxToPrestoExprConverter veloxToPrestoConverter(pool);
  std::vector<protocol::RowExpressionOptimizationResult> result;
  for (const auto& expression : input) {
    result.push_back(optimizeExpression(
        expression,
        optimizerLevel,
        prestoToVeloxConverter,
        veloxToPrestoConverter,
        queryCtx,
        pool));
  }
  return result;
}

} // namespace facebook::presto::expression
