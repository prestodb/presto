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
#include "velox/substrait/SubstraitParser.h"

namespace facebook::velox::substrait {

/// This class is used to convert Substrait representations to Velox
/// expressions.
class SubstraitVeloxExprConverter {
 public:
  /// subParser: A Substrait parser used to convert Substrait representations
  /// into recognizable representations. functionMap: A pre-constructed map
  /// storing the relations between the function id and the function name.
  explicit SubstraitVeloxExprConverter(
      memory::MemoryPool* pool,
      const std::unordered_map<uint64_t, std::string>& functionMap)
      : pool_(pool), functionMap_(functionMap) {}

  /// Convert Substrait Field into Velox Field Expression.
  std::shared_ptr<const core::FieldAccessTypedExpr> toVeloxExpr(
      const ::substrait::Expression::FieldReference& substraitField,
      const RowTypePtr& inputType);

  /// Convert Substrait ScalarFunction into Velox Expression.
  std::shared_ptr<const core::ITypedExpr> toVeloxExpr(
      const ::substrait::Expression::ScalarFunction& substraitFunc,
      const RowTypePtr& inputType);

  /// Convert Substrait CastExpression to Velox Expression.
  std::shared_ptr<const core::ITypedExpr> toVeloxExpr(
      const ::substrait::Expression::Cast& castExpr,
      const RowTypePtr& inputType);

  /// Convert Substrait Literal into Velox Expression.
  std::shared_ptr<const core::ConstantTypedExpr> toVeloxExpr(
      const ::substrait::Expression::Literal& substraitLit);

  /// Convert Substrait Expression into Velox Expression.
  std::shared_ptr<const core::ITypedExpr> toVeloxExpr(
      const ::substrait::Expression& substraitExpr,
      const RowTypePtr& inputType);

 private:
  /// Memory pool.
  memory::MemoryPool* pool_;

  /// The Substrait parser used to convert Substrait representations into
  /// recognizable representations.
  SubstraitParser substraitParser_;

  /// The map storing the relations between the function id and the function
  /// name.
  std::unordered_map<uint64_t, std::string> functionMap_;
};

} // namespace facebook::velox::substrait
