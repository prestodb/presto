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

// Helper class to convert Velox expressions of type exec::Expr
// (https://github.com/facebookincubator/velox/blob/1bd480e4f9c3e48c90ab036c1dea980ff8dae264/velox/expression/Expr.h#L154)
// to their corresponding type of RowExpression
// (https://github.com/prestodb/presto/blob/master/presto-spi/src/main/java/com/facebook/presto/spi/relation/RowExpression.java)
// in Presto.
// 1. Constant velox expressions of type exec::ConstantExpr
// (https://github.com/facebookincubator/velox/blob/main/velox/expression/ConstantExpr.h)
// are converted to Presto ConstantExpression
// (https://github.com/prestodb/presto/blob/master/presto-spi/src/main/java/com/facebook/presto/spi/relation/ConstantExpression.java).
// However, if the velox constant expression is of ROW type, it is converted to
// the ROW_CONSTRUCTOR SpecialFormExpression in Presto.
// 2. Velox expressions representing variables are of type exec::FieldReference
// (https://github.com/facebookincubator/velox/blob/main/velox/expression/FieldReference.h)
// and they are converted to Presto VariableReferenceExpression
// (https://github.com/prestodb/presto/blob/master/presto-spi/src/main/java/com/facebook/presto/spi/relation/VariableReferenceExpression.java).
// 3. Special form expressions and expressions with a vector function in Velox
// can map either to a Presto SpecialFormExpression
// (https://github.com/prestodb/presto/blob/master/presto-spi/src/main/java/com/facebook/presto/spi/relation/SpecialFormExpression.java)
// or to a Presto CallExpression
// (https://github.com/prestodb/presto/blob/master/presto-spi/src/main/java/com/facebook/presto/spi/relation/CallExpression.java).
// Such velox expressions are converted to Presto SpecialFormExpression if
// expression name belongs to the list of Presto SpecialFormExpression names
// (https://github.com/prestodb/presto/blob/c3e18d88a5d6797c63f787775011e2566d9dac2c/presto-spi/src/main/java/com/facebook/presto/spi/relation/SpecialFormExpression.java#L136).
// Otherwise, the velox expression is converted to a Presto CallExpression.
class RowExpressionConverter {
 public:
  explicit RowExpressionConverter(memory::MemoryPool* pool)
      : pool_(pool), veloxToPrestoOperatorMap_(veloxToPrestoOperatorMap()) {}

  std::shared_ptr<protocol::ConstantExpression> getConstantRowExpression(
      const std::shared_ptr<const exec::ConstantExpr>& constantExpr);

  std::shared_ptr<protocol::ConstantExpression> getCurrentUser(
      const std::string& currentUser);

  json veloxToPrestoRowExpression(
      const exec::ExprPtr& expr,
      const json& inputRowExpr);

 protected:
  std::string getValueBlock(const VectorPtr& vector);

  std::pair<json::array_t, bool> getSwitchSpecialFormArgs(
      const exec::ExprPtr& expr,
      const json& input);

  json getSpecialForm(const exec::ExprPtr& expr, const json& inputRowExpr);

  json toConstantRowExpression(const exec::ExprPtr& expr);

  json toCallRowExpression(const exec::ExprPtr& expr, const json& input);

  memory::MemoryPool* pool_;
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
        rowExpressionConverter_(RowExpressionConverter(pool_.get())) {}

  /// Optimizes all expressions from the input json array. If the expression
  /// optimization fails for any of the input expressions, the second value in
  /// the returned pair will be false and the returned json would contain the
  /// exception. Otherwise, the returned json will be an array of optimized row
  /// expressions.
  std::pair<json, bool> optimize(
      proxygen::HTTPMessage* message,
      const json::array_t& input);

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
