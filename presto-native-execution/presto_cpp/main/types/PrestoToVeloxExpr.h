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

#include <stdexcept>
#include "presto_cpp/main/types/TypeParser.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/core/Expressions.h"

namespace facebook::presto {

static const std::unordered_map<std::string, std::string> kPrestoOperatorMap = {
    // Operator overrides: com.facebook.presto.common.function.OperatorType
    {"presto.default.$operator$add", "presto.default.plus"},
    {"presto.default.$operator$between", "presto.default.between"},
    {"presto.default.$operator$divide", "presto.default.divide"},
    {"presto.default.$operator$equal", "presto.default.eq"},
    {"presto.default.$operator$greater_than", "presto.default.gt"},
    {"presto.default.$operator$greater_than_or_equal", "presto.default.gte"},
    {"presto.default.$operator$is_distinct_from",
     "presto.default.distinct_from"},
    {"presto.default.$operator$less_than", "presto.default.lt"},
    {"presto.default.$operator$less_than_or_equal", "presto.default.lte"},
    {"presto.default.$operator$modulus", "presto.default.mod"},
    {"presto.default.$operator$multiply", "presto.default.multiply"},
    {"presto.default.$operator$negation", "presto.default.negate"},
    {"presto.default.$operator$not_equal", "presto.default.neq"},
    {"presto.default.$operator$subtract", "presto.default.minus"},
    {"presto.default.$operator$subscript", "presto.default.subscript"},
    // Special form function overrides.
    {"presto.default.in", "in"},
};

const std::unordered_map<std::string, std::string> veloxToPrestoOperatorMap();

class VeloxExprConverter {
 public:
  VeloxExprConverter(velox::memory::MemoryPool* pool, TypeParser* typeParser)
      : pool_(pool), typeParser_(typeParser) {}

  std::shared_ptr<const velox::core::ConstantTypedExpr> toVeloxExpr(
      std::shared_ptr<protocol::ConstantExpression> pexpr) const;

  velox::core::TypedExprPtr toVeloxExpr(
      std::shared_ptr<protocol::SpecialFormExpression> pexpr) const;

  velox::core::FieldAccessTypedExprPtr toVeloxExpr(
      std::shared_ptr<protocol::VariableReferenceExpression> pexpr) const;

  std::shared_ptr<const velox::core::LambdaTypedExpr> toVeloxExpr(
      std::shared_ptr<protocol::LambdaDefinitionExpression> pexpr) const;

  // TODO Remove when protocols are updated to use shared_ptr
  std::shared_ptr<const velox::core::FieldAccessTypedExpr> toVeloxExpr(
      const protocol::VariableReferenceExpression& pexpr) const;

  velox::core::TypedExprPtr toVeloxExpr(
      const protocol::CallExpression& pexpr) const;

  velox::core::TypedExprPtr toVeloxExpr(
      std::shared_ptr<protocol::RowExpression> pexpr) const;

  // Deserializes Presto Block of a scalar type into a variant.
  velox::variant getConstantValue(
      const velox::TypePtr& type,
      const protocol::Block& block) const;

 private:
  std::vector<velox::core::TypedExprPtr> toVeloxExpr(
      std::vector<std::shared_ptr<protocol::RowExpression>> pexpr) const;

  std::optional<velox::core::TypedExprPtr> tryConvertLike(
      const protocol::CallExpression& pexpr) const;

  std::optional<velox::core::TypedExprPtr> tryConvertDate(
      const protocol::CallExpression& pexpr) const;

  velox::memory::MemoryPool* const pool_;
  TypeParser* const typeParser_;
};

} // namespace facebook::presto
