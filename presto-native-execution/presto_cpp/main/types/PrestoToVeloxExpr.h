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
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/core/Expressions.h"

using namespace facebook::velox::core;

namespace facebook::presto {

class VeloxExprConverter {
 public:
  explicit VeloxExprConverter(velox::memory::MemoryPool* pool) : pool_(pool) {}

  std::shared_ptr<const ConstantTypedExpr> toVeloxExpr(
      std::shared_ptr<protocol::ConstantExpression> pexpr) const;

  std::shared_ptr<const ITypedExpr> toVeloxExpr(
      std::shared_ptr<protocol::SpecialFormExpression> pexpr) const;

  std::shared_ptr<const FieldAccessTypedExpr> toVeloxExpr(
      std::shared_ptr<protocol::VariableReferenceExpression> pexpr) const;

  std::shared_ptr<const LambdaTypedExpr> toVeloxExpr(
      std::shared_ptr<protocol::LambdaDefinitionExpression> pexpr) const;

  // TODO Remove when protocols are updated to use shared_ptr
  std::shared_ptr<const FieldAccessTypedExpr> toVeloxExpr(
      const protocol::VariableReferenceExpression& pexpr) const;

  std::shared_ptr<const ITypedExpr> toVeloxExpr(
      const protocol::CallExpression& pexpr) const;

  std::shared_ptr<const ITypedExpr> toVeloxExpr(
      std::shared_ptr<protocol::RowExpression> pexpr) const;

  // Deserializes Presto Block of a scalar type into a variant.
  velox::variant getConstantValue(
      const velox::TypePtr& type,
      const protocol::Block& block) const;

 private:
  std::vector<std::shared_ptr<const ITypedExpr>> toVeloxExpr(
      std::vector<std::shared_ptr<protocol::RowExpression>> pexpr) const;

  std::optional<std::shared_ptr<const ITypedExpr>> tryConvertLike(
      const protocol::CallExpression& pexpr) const;

  std::optional<std::shared_ptr<const ITypedExpr>> tryConvertDate(
      const protocol::CallExpression& pexpr) const;

  velox::memory::MemoryPool* pool_;
};

} // namespace facebook::presto
