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

#include <numeric>

#include "velox/common/base/SelectivityInfo.h"
#include "velox/expression/Expr.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::exec {

const char* const kIf = "if";
const char* const kSwitch = "switch";
const char* const kAnd = "and";
const char* const kOr = "or";
const char* const kTry = "try";

class SpecialForm : public Expr {
 public:
  SpecialForm(
      TypePtr type,
      std::vector<ExprPtr>&& inputs,
      const std::string& name,
      bool trackCpuUsage)
      : Expr(std::move(type), std::move(inputs), name, trackCpuUsage) {}

  bool isSpecialForm() const override {
    return true;
  }
};

enum class BooleanMix { kAllTrue, kAllFalse, kAllNull, kMixNonNull, kMix };

BooleanMix getFlatBool(
    BaseVector* vector,
    const SelectivityVector& activeRows,
    EvalCtx& context,
    BufferPtr* tempValues,
    BufferPtr* tempNulls,
    bool mergeNullsToValues,
    const uint64_t** valuesOut,
    const uint64_t** nullsOut);

class ConstantExpr : public SpecialForm {
 public:
  ConstantExpr(TypePtr type, variant value)
      : SpecialForm(
            std::move(type),
            std::vector<ExprPtr>(),
            "literal",
            false /* trackCpuUsage */),
        value_(std::move(value)),
        needToSetIsAscii_{type->isVarchar()} {}

  explicit ConstantExpr(VectorPtr value)
      : SpecialForm(
            value->type(),
            std::vector<ExprPtr>(),
            "literal",
            false /* trackCpuUsage */),
        needToSetIsAscii_{value->type()->isVarchar()} {
    VELOX_CHECK_EQ(value->encoding(), VectorEncoding::Simple::CONSTANT);
    sharedSubexprValues_ = std::move(value);
  }

  // Do not clear sharedSubexprValues_.
  void reset() override {}

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  void evalSpecialFormSimplified(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  const VectorPtr& value() const {
    return sharedSubexprValues_;
  }

  std::string toString(bool recursive = true) const override;

 private:
  const variant value_;
  bool needToSetIsAscii_;
};

class FieldReference : public SpecialForm {
 public:
  FieldReference(
      TypePtr type,
      std::vector<ExprPtr>&& inputs,
      const std::string& field)
      : SpecialForm(
            std::move(type),
            std::move(inputs),
            field,
            false /* trackCpuUsage */),
        field_(field) {}

  const std::string& field() const {
    return field_;
  }

  int32_t index(EvalCtx& context) {
    if (index_ != -1) {
      return index_;
    }
    auto* rowType = dynamic_cast<const RowType*>(context.row()->type().get());
    VELOX_CHECK(rowType, "The context has no row");
    index_ = rowType->getChildIdx(field_);
    return index_;
  }

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  void evalSpecialFormSimplified(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

 protected:
  void computeMetadata() override {
    propagatesNulls_ = true;
    if (inputs_.empty()) {
      distinctFields_.resize(1);
      distinctFields_[0] = this;
    } else {
      Expr::computeMetadata();
    }
  }

 private:
  const std::string field_;
  int32_t index_ = -1;
};

/// CASE expression:
///
/// case
///  when condition then result
///  [when ...]
///  [else result]
/// end
///
/// Evaluates each boolean condition from left to right until one is true and
/// returns the matching result. If no conditions are true, the result from the
/// ELSE clause is returned if it exists, otherwise null is returned.
///
/// IF expression can be represented as a CASE expression with a single
/// condition.
class SwitchExpr : public SpecialForm {
 public:
  /// Inputs are concatenated conditions and results with an optional "else" at
  /// the end, e.g. {condition1, result1, condition2, result2,..else}
  SwitchExpr(TypePtr type, std::vector<ExprPtr>&& inputs)
      : SpecialForm(
            std::move(type),
            std::move(inputs),
            kSwitch,
            false /* trackCpuUsage */),
        numCases_{inputs_.size() / 2},
        hasElseClause_{inputs_.size() % 2 == 1} {
    VELOX_CHECK_GT(numCases_, 0);

    for (auto i = 0; i < numCases_; i++) {
      auto& condition = inputs_[i * 2];
      VELOX_CHECK_EQ(condition->type()->kind(), TypeKind::BOOLEAN);
    }
  }

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  bool propagatesNulls() const override;

  bool isConditional() const override {
    return true;
  }

 private:
  const size_t numCases_;
  const bool hasElseClause_;
  BufferPtr tempValues_;
};

class ConjunctExpr : public SpecialForm {
 public:
  ConjunctExpr(TypePtr type, std::vector<ExprPtr>&& inputs, bool isAnd)
      : SpecialForm(
            std::move(type),
            std::move(inputs),
            isAnd ? kAnd : kOr,
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

class LambdaExpr : public SpecialForm {
 public:
  LambdaExpr(
      TypePtr type,
      std::shared_ptr<const RowType>&& signature,
      std::vector<std::shared_ptr<FieldReference>>&& capture,
      std::shared_ptr<Expr>&& body,
      bool trackCpuUsage)
      : SpecialForm(
            std::move(type),
            std::vector<std::shared_ptr<Expr>>(),
            "lambda",
            trackCpuUsage),
        signature_(std::move(signature)),
        capture_(std::move(capture)),
        body_(std::move(body)) {
    for (auto& field : capture_) {
      distinctFields_.push_back(field.get());
    }
  }

  bool propagatesNulls() const override {
    // A null capture does not result in a null function.
    return false;
  }

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

 private:
  void makeTypeWithCapture(EvalCtx& context);

  std::shared_ptr<const RowType> signature_;
  std::vector<std::shared_ptr<FieldReference>> capture_;
  ExprPtr body_;
  // Filled on first use.
  std::shared_ptr<const RowType> typeWithCapture_;
  std::vector<ChannelIndex> captureChannels_;
};

class TryExpr : public SpecialForm {
 public:
  TryExpr(TypePtr type, ExprPtr&& input)
      : SpecialForm(
            std::move(type),
            {std::move(input)},
            kTry,
            false /* trackCpuUsage */) {}

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  void evalSpecialFormSimplified(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  bool propagatesNulls() const override {
    return inputs_[0]->propagatesNulls();
  }

 private:
  void nullOutErrors(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result);
};
} // namespace facebook::velox::exec
