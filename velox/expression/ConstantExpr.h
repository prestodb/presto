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
  explicit ConstantExpr(VectorPtr value)
      : SpecialForm(
            SpecialFormKind::kConstant,
            value->type(),
            std::vector<ExprPtr>(),
            "literal",
            !value->isNullAt(0) /* supportsFlatNoNullsFastPath */,
            false /* trackCpuUsage */),
        needToSetIsAscii_{value->type()->isVarchar()} {
    VELOX_CHECK_EQ(value->encoding(), VectorEncoding::Simple::CONSTANT);
    sharedConstantValue_ = std::move(value);
  }

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  void evalSpecialFormSimplified(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  const VectorPtr& value() const {
    return sharedConstantValue_;
  }

  VectorPtr& mutableValue() {
    return sharedConstantValue_;
  }

  void setDefaultNullRowsSkipped(bool defaultNullRowsSkipped) {
    stats_.defaultNullRowsSkipped = defaultNullRowsSkipped;
  }

  std::string toString(bool recursive = true) const override;

  std::string toSql(
      std::vector<VectorPtr>* complexConstants = nullptr) const override;

 private:
  VectorPtr sharedConstantValue_;
  bool needToSetIsAscii_;
};
} // namespace facebook::velox::exec
