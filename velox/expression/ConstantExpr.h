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

#include "velox/expression/SpecialForm.h"

namespace facebook::velox::exec {
class ConstantExpr : public SpecialForm {
 public:
  ConstantExpr(TypePtr type, variant value)
      : SpecialForm(
            std::move(type),
            std::vector<ExprPtr>(),
            "literal",
            !value.isNull() /* supportsFlatNoNullsFastPath */,
            false /* trackCpuUsage */),
        value_(std::move(value)),
        needToSetIsAscii_{type->isVarchar()} {}

  explicit ConstantExpr(VectorPtr value)
      : SpecialForm(
            value->type(),
            std::vector<ExprPtr>(),
            "literal",
            !value->isNullAt(0) /* supportsFlatNoNullsFastPath */,
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
} // namespace facebook::velox::exec
