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
#include "velox/expression/ConstantExpr.h"

namespace facebook::velox::exec {

void ConstantExpr::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  ExceptionContextSetter exceptionContext(
      {[](auto* expr) { return static_cast<Expr*>(expr)->toString(); }, this});

  if (!sharedSubexprValues_) {
    sharedSubexprValues_ =
        BaseVector::createConstant(value_, 1, context.execCtx()->pool());
  }

  if (needToSetIsAscii_) {
    auto* vector =
        sharedSubexprValues_->asUnchecked<SimpleVector<StringView>>();
    LocalSelectivityVector singleRow(context);
    bool isAscii = vector->computeAndSetIsAscii(*singleRow.get(1, true));
    vector->setAllIsAscii(isAscii);
    needToSetIsAscii_ = false;
  }

  if (sharedSubexprValues_.unique()) {
    sharedSubexprValues_->resize(rows.end());
    context.moveOrCopyResult(sharedSubexprValues_, rows, result);
  } else {
    context.moveOrCopyResult(
        BaseVector::wrapInConstant(rows.end(), 0, sharedSubexprValues_),
        rows,
        result);
  }
}

void ConstantExpr::evalSpecialFormSimplified(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  ExceptionContextSetter exceptionContext(
      {[](auto* expr) { return static_cast<Expr*>(expr)->toString(); }, this});

  // Simplified path should never ask us to write to a vector that was already
  // pre-allocated.
  VELOX_CHECK(result == nullptr);

  if (sharedSubexprValues_ == nullptr) {
    result = BaseVector::createConstant(value_, rows.end(), context.pool());
  } else {
    result = BaseVector::wrapInConstant(rows.end(), 0, sharedSubexprValues_);
  }
}

std::string ConstantExpr::toString(bool /*recursive*/) const {
  if (sharedSubexprValues_ == nullptr) {
    return fmt::format("{}:{}", value_.toJson(), type()->toString());
  } else {
    return fmt::format(
        "{}:{}", sharedSubexprValues_->toString(0), type()->toString());
  }
}
} // namespace facebook::velox::exec
