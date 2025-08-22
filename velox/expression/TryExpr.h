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

#include "velox/expression/ExprConstants.h"
#include "velox/expression/FunctionCallToSpecialForm.h"
#include "velox/expression/SpecialForm.h"

namespace facebook::velox::exec {

class TryExpr : public SpecialForm {
 public:
  /// Try expression adds nulls, hence, doesn't support flat-no-nulls fast path.
  TryExpr(TypePtr type, ExprPtr&& input)
      : SpecialForm(
            SpecialFormKind::kTry,
            std::move(type),
            {std::move(input)},
            expression::kTry,
            false /* supportsFlatNoNullsFastPath */,
            false /* trackCpuUsage */) {}

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  void evalSpecialFormSimplified(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  void nullOutErrors(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) const;

 private:
  // This is safe to call only after all metadata is computed for input
  // expressions.
  void computePropagatesNulls() override {
    propagatesNulls_ = inputs_[0]->propagatesNulls();
  }
};

class TryCallToSpecialForm : public FunctionCallToSpecialForm {
 public:
  TypePtr resolveType(const std::vector<TypePtr>& argTypes) override;

  ExprPtr constructSpecialForm(
      const TypePtr& type,
      std::vector<ExprPtr>&& compiledChildren,
      bool trackCpuUsage,
      const core::QueryConfig& config) override;
};

} // namespace facebook::velox::exec
