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

using namespace facebook::velox;

namespace facebook::presto::expression {

using RowExpressionPtr = std::shared_ptr<protocol::RowExpression>;
using SpecialFormExpressionPtr =
    std::shared_ptr<protocol::SpecialFormExpression>;

// Helper class to convert Velox Expr of different types to the respective kind
// of Presto RowExpression.
class RowExpressionConverter {
 public:
  explicit RowExpressionConverter(
      const std::shared_ptr<memory::MemoryPool>& pool)
      : pool_(pool), veloxToPrestoOperatorMap_(veloxToPrestoOperatorMap()) {}

  std::shared_ptr<protocol::ConstantExpression> getConstantRowExpression(
      const std::shared_ptr<const exec::ConstantExpr>& constantExpr);

  std::shared_ptr<protocol::ConstantExpression> getCurrentUser(
      const std::string& currentUser);

  json veloxExprToRowExpression(
      const exec::ExprPtr& expr,
      const json& inputRowExpr);

 protected:
  std::string getValueBlock(const VectorPtr& vector);

  json getRowConstructorSpecialForm(
      const exec::ExprPtr& expr,
      const json& inputRowExpr);

  std::pair<json::array_t, bool> getSwitchSpecialFormArgs(
      const exec::ExprPtr& expr,
      const json& input);

  json getSpecialForm(const exec::ExprPtr& expr, const json& inputRowExpr);

  json toConstantRowExpression(const exec::ExprPtr& expr);

  json toCallRowExpression(const exec::ExprPtr& expr, const json& input);

  const std::shared_ptr<memory::MemoryPool> pool_;
  const std::unordered_map<std::string, std::string> veloxToPrestoOperatorMap_;
  const std::unique_ptr<serializer::presto::PrestoVectorSerde> serde_ =
      std::make_unique<serializer::presto::PrestoVectorSerde>();
};

class RowExpressionOptimizer {
 public:
  explicit RowExpressionOptimizer()
      : pool_(memory::MemoryManager::getInstance()->addLeafPool(
            "RowExpressionOptimizer")),
        veloxExprConverter_(pool_.get(), &typeParser_),
        rowExpressionConverter_(RowExpressionConverter(pool_)) {}

  /// Optimize expressions sent along the proxygen endpoint '/v1/expressions'.
  void optimize(
      proxygen::HTTPMessage* message,
      const std::vector<std::unique_ptr<folly::IOBuf>>& body,
      proxygen::ResponseHandler* downstream);

 protected:
  /// Converts protocol::RowExpression into a velox expression with constant
  /// folding enabled during velox expression compilation.
  exec::ExprPtr compileExpression(const RowExpressionPtr& inputRowExpr);

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

  /// Optimizes special form expressions. Optimization rules borrowed from
  /// Presto function visitSpecialForm() in RowExpressionInterpreter.java.
  RowExpressionPtr optimizeSpecialForm(
      const SpecialFormExpressionPtr& specialFormExpr);

  /// Optimizes and constant folds each expression from input json array and
  /// returns an array of expressions that are optimized and constant folded.
  /// Each expression in the input array is optimized with helper functions
  /// optimizeSpecialForm (applicable only for special form expressions) and
  /// optimizeExpression. The optimized expression is also evaluated if the
  /// optimization level in the header of http request made to 'v1/expressions'
  /// is 'EVALUATED'. optimizeExpression uses RowExpressionConverter to convert
  /// Velox expression(s) to their corresponding Presto RowExpression(s).
  json::array_t optimizeExpressions(
      const json::array_t& input,
      const std::string& optimizationLevel,
      const std::string& currentUser);

  const std::shared_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<core::ExecCtx> execCtx_;
  TypeParser typeParser_;
  VeloxExprConverter veloxExprConverter_;
  RowExpressionConverter rowExpressionConverter_;
};
} // namespace facebook::presto::expression
