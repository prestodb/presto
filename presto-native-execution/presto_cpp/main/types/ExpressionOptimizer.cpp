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
constexpr char const* kOptimized = "OPTIMIZED";
constexpr char const* kEvaluated = "EVALUATED";
constexpr char const* kTimezoneHeader = "X-Presto-Time-Zone";
constexpr char const* kOptimizerLevelHeader =
    "X-Presto-Expression-Optimizer-Level";

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

protocol::RowExpressionOptimizationResult getRowExpressionOptimizationResult(
    const RowExpressionPtr& rowExpression,
    const std::string& optimizerLevel,
    const VeloxExprConverter& veloxExprConverter,
    const expression::VeloxToPrestoExprConverter& veloxToPrestoExprConverter,
    velox::core::QueryCtx* queryCtx,
    velox::memory::MemoryPool* pool) {
  protocol::RowExpressionOptimizationResult result;
  const auto expr = veloxExprConverter.toVeloxExpr(rowExpression);
  auto optimized =
      velox::expression::optimize(expr, queryCtx, pool, kMakeFailExpr);

  if (optimizerLevel == kEvaluated) {
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
      veloxToPrestoExprConverter.getRowExpression(optimized, rowExpression);
  return result;
}

} // namespace

void optimizeExpressions(
    const proxygen::HTTPHeaders& httpHeaders,
    const json::array_t& inputRowExpressions,
    proxygen::ResponseHandler* downstream,
    folly::Executor* driverExecutor,
    velox::memory::MemoryPool* pool) {
  const auto& optimizerLevel =
      httpHeaders.getSingleOrEmpty(kOptimizerLevelHeader);
  VELOX_USER_CHECK(
      (optimizerLevel == kOptimized) || (optimizerLevel == kEvaluated),
      "Optimizer level should be OPTIMIZED or EVALUATED, received {}.",
      optimizerLevel);
  const auto& timezone = httpHeaders.getSingleOrEmpty(kTimezoneHeader);
  std::unordered_map<std::string, std::string> config(
      {{velox::core::QueryConfig::kSessionTimezone, timezone},
       {velox::core::QueryConfig::kAdjustTimestampToTimezone, "true"}});
  auto queryConfig = velox::core::QueryConfig{std::move(config)};
  auto queryCtx =
      velox::core::QueryCtx::create(driverExecutor, std::move(queryConfig));

  TypeParser typeParser;
  const VeloxExprConverter veloxExprConverter(pool, &typeParser);
  const expression::VeloxToPrestoExprConverter veloxToPrestoExprConverter(pool);
  const auto numExpr = inputRowExpressions.size();
  json j;
  json::array_t result;
  for (auto idx = 0; idx < numExpr; idx++) {
    const RowExpressionPtr inputRowExpr = inputRowExpressions[idx];
    const auto rowExpressionOptimizationResult =
        getRowExpressionOptimizationResult(
            inputRowExpr,
            optimizerLevel,
            veloxExprConverter,
            veloxToPrestoExprConverter,
            queryCtx.get(),
            pool);
    protocol::to_json(j, rowExpressionOptimizationResult);
    result.push_back(j);
  }

  http::sendOkResponse(downstream, result);
}

} // namespace facebook::presto::expression
