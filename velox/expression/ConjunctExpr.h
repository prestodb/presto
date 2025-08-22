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

#include "velox/common/base/SelectivityInfo.h"
#include "velox/expression/ExprConstants.h"
#include "velox/expression/FunctionCallToSpecialForm.h"
#include "velox/expression/SpecialForm.h"

namespace facebook::velox::exec {

class ConjunctExpr : public SpecialForm {
 public:
  ConjunctExpr(
      TypePtr type,
      std::vector<ExprPtr>&& inputs,
      bool isAnd,
      bool inputsSupportFlatNoNullsFastPath)
      : SpecialForm(
            isAnd ? SpecialFormKind::kAnd : SpecialFormKind::kOr,
            std::move(type),
            std::move(inputs),
            isAnd ? expression::kAnd : expression::kOr,
            inputsSupportFlatNoNullsFastPath,
            false /* trackCpuUsage */),
        isAnd_(isAnd) {
    selectivity_.resize(inputs_.size());
    inputOrder_.resize(inputs_.size());
    std::iota(inputOrder_.begin(), inputOrder_.end(), 0);

    std::vector<TypePtr> inputTypes;
    inputTypes.reserve(inputs_.size());
    std::transform(
        inputs_.begin(),
        inputs_.end(),
        std::back_inserter(inputTypes),
        [](const ExprPtr& expr) { return expr->type(); });

    // Apply type checking.
    resolveType(inputTypes);
  }

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  bool isConditional() const override {
    return true;
  }

  const SelectivityInfo& selectivityAt(int32_t index) {
    return selectivity_[inputOrder_[index]];
  }

  std::string toSql(
      std::vector<VectorPtr>* complexConstants = nullptr) const override;

  void clearCache() override {
    Expr::clearCache();
    tempValues_.reset();
    tempNulls_.reset();
  }

 private:
  static TypePtr resolveType(const std::vector<TypePtr>& argTypes);

  void computePropagatesNulls() override {
    propagatesNulls_ = false;
  }

  void maybeReorderInputs();

  void updateResult(
      BaseVector* inputResult,
      EvalCtx& context,
      FlatVector<bool>* result,
      SelectivityVector* activeRows);

  bool evaluatesArgumentsOnNonIncreasingSelection() const override {
    return isAnd_;
  }

  // true if conjunction (and), false if disjunction (or).
  const bool isAnd_;

  // temp space for nulls and values of inputs
  BufferPtr tempValues_;
  BufferPtr tempNulls_;
  bool reorderEnabledChecked_ = false;
  bool reorderEnabled_;
  std::vector<SelectivityInfo> selectivity_;
  std::vector<int32_t> inputOrder_;

  friend class ConjunctCallToSpecialForm;
};

class ConjunctCallToSpecialForm : public FunctionCallToSpecialForm {
 public:
  explicit ConjunctCallToSpecialForm(bool isAnd)
      : FunctionCallToSpecialForm(), isAnd_(isAnd) {}

  TypePtr resolveType(const std::vector<TypePtr>& argTypes) override;

  ExprPtr constructSpecialForm(
      const TypePtr& type,
      std::vector<ExprPtr>&& compiledChildren,
      bool trackCpuUsage,
      const core::QueryConfig& config) override;

 private:
  const bool isAnd_;
};

} // namespace facebook::velox::exec
