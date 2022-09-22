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
            std::move(type),
            std::move(inputs),
            isAnd ? "and" : "or",
            inputsSupportFlatNoNullsFastPath,
            false /* trackCpuUsage */),
        isAnd_(isAnd) {
    selectivity_.resize(inputs_.size());
    inputOrder_.resize(inputs_.size());
    std::iota(inputOrder_.begin(), inputOrder_.end(), 0);
  }

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  bool propagatesNulls() const override {
    return false;
  }

  bool isConditional() const override {
    return true;
  }

  const SelectivityInfo& selectivityAt(int32_t index) {
    return selectivity_[inputOrder_[index]];
  }

  std::string toSql() const override;

 private:
  void maybeReorderInputs();
  void updateResult(
      BaseVector* inputResult,
      EvalCtx& context,
      FlatVector<bool>* result,
      SelectivityVector* activeRows);

  // true if conjunction (and), false if disjunction (or).
  const bool isAnd_;

  // Errors encountered before processing the current input.
  FlatVectorPtr<StringView> errors_;
  // temp space for nulls and values of inputs
  BufferPtr tempValues_;
  BufferPtr tempNulls_;
  bool reorderEnabledChecked_ = false;
  bool reorderEnabled_;
  std::vector<SelectivityInfo> selectivity_;
  std::vector<int32_t> inputOrder_;
};
} // namespace facebook::velox::exec
