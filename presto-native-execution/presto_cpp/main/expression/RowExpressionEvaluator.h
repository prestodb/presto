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

#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/main/http/HttpServer.h"
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/Expr.h"
#include "velox/serializers/PrestoSerializer.h"

namespace facebook::presto::expression {

using RowExpressionPtr = std::shared_ptr<protocol::RowExpression>;
using SpecialFormExpressionPtr =
    std::shared_ptr<protocol::SpecialFormExpression>;

class RowExpressionEvaluator {
 public:
  explicit RowExpressionEvaluator()
      : pool_(velox::memory::MemoryManager::getInstance()->addLeafPool(
            "RowExpressionEvaluator")),
        execCtx_{std::make_unique<velox::core::ExecCtx>(
            pool_.get(),
            queryCtx_.get())},
        veloxToPrestoOperatorMap_(veloxToPrestoOperatorMap()),
        exprConverter_(pool_.get(), &typeParser_) {}

  /// Evaluate expressions sent along /v1/expressions endpoint.
  void evaluate(
      const std::vector<std::unique_ptr<folly::IOBuf>>& body,
      proxygen::ResponseHandler* downstream);

 protected:
  std::string serializeValueBlock(const velox::VectorPtr& vector);

  std::shared_ptr<protocol::ConstantExpression> getConstantRowExpression(
      const std::shared_ptr<const velox::exec::ConstantExpr>& constantExpr);

  velox::exec::ExprPtr compileExpression(const RowExpressionPtr& inputRowExpr);

  json getSpecialFormRowConstructor(
      const velox::exec::ExprPtr& expr,
      const json& inputRowExpr);

  json getSpecialForm(
      const velox::exec::ExprPtr& expr,
      const json& inputRowExpr);

  RowExpressionPtr optimizeAndSpecialForm(
      const SpecialFormExpressionPtr& specialFormExpr);

  RowExpressionPtr optimizeIfSpecialForm(
      const SpecialFormExpressionPtr& specialFormExpr);

  RowExpressionPtr optimizeIsNullSpecialForm(
      const SpecialFormExpressionPtr& specialFormExpr);

  RowExpressionPtr optimizeOrSpecialForm(
      const SpecialFormExpressionPtr& specialFormExpr);

  RowExpressionPtr optimizeCoalesceSpecialForm(
      const SpecialFormExpressionPtr& specialFormExpr);

  RowExpressionPtr optimizeSpecialForm(
      const SpecialFormExpressionPtr& specialFormExpr);

  json veloxExprToCallExpr(const velox::exec::ExprPtr& expr, const json& input);

  json veloxExprToRowExpr(
      const velox::exec::ExprPtr& expr,
      const json& inputRowExpr);

  json::array_t evaluateExpression(json::array_t& input);

  const std::shared_ptr<velox::memory::MemoryPool> pool_;
  const std::shared_ptr<velox::core::QueryCtx> queryCtx_{
      facebook::velox::core::QueryCtx::create()};
  const std::unique_ptr<velox::core::ExecCtx> execCtx_;
  const std::unordered_map<std::string, std::string> veloxToPrestoOperatorMap_;
  const std::unique_ptr<velox::serializer::presto::PrestoVectorSerde> serde_ =
      std::make_unique<velox::serializer::presto::PrestoVectorSerde>();
  const velox::serializer::presto::PrestoOptions options_;
  TypeParser typeParser_;
  VeloxExprConverter exprConverter_;
};
} // namespace facebook::presto::expression
