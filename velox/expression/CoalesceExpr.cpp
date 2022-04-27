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
#include "velox/expression/CoalesceExpr.h"
#include "velox/expression/VarSetter.h"

namespace facebook::velox::exec {

CoalesceExpr::CoalesceExpr(TypePtr type, std::vector<ExprPtr>&& inputs)
    : SpecialForm(std::move(type), std::move(inputs), kCoalesce) {
  for (auto i = 1; i < inputs_.size(); i++) {
    VELOX_USER_CHECK_EQ(
        inputs_[0]->type()->kind(),
        inputs_[i]->type()->kind(),
        "Inputs to coalesce must have the same type");
  }
}

void CoalesceExpr::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx* context,
    VectorPtr* result) {
  // Make sure to include current expression in the error message in case of an
  // exception.
  ExceptionContextSetter exceptionContext(
      {[](auto* expr) { return static_cast<Expr*>(expr)->toString(); }, this});

  // Null positions to populate.
  exec::LocalSelectivityVector activeRowsHolder(context, rows.end());
  auto activeRows = activeRowsHolder.get();
  *activeRows = rows;

  // Fix finalSelection at "rows" unless already fixed.
  VarSetter finalSelection(
      context->mutableFinalSelection(), &rows, context->isFinalSelection());
  VarSetter isFinalSelection(context->mutableIsFinalSelection(), false);

  for (int i = 0; i < inputs_.size(); i++) {
    inputs_[i]->eval(*activeRows, context, result);

    const uint64_t* rawNulls = (*result)->flatRawNulls(*activeRows);
    if (!rawNulls) {
      // No nulls left.
      return;
    }

    activeRows->deselectNonNulls(rawNulls, 0, activeRows->end());
    if (!activeRows->hasSelections()) {
      // No nulls left.
      return;
    }
  }
}
} // namespace facebook::velox::exec
