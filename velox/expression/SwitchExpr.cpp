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
#include "velox/expression/SwitchExpr.h"
#include "velox/expression/BooleanMix.h"
#include "velox/expression/VarSetter.h"

namespace facebook::velox::exec {

void SwitchExpr::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  LocalSelectivityVector remainingRows(context, rows);

  LocalSelectivityVector thenRows(context);

  VectorPtr condition;
  const uint64_t* values;

  // SWITCH: fix finalSelection at "rows" unless already fixed
  VarSetter finalSelection(
      context.mutableFinalSelection(), &rows, context.isFinalSelection());
  VarSetter isFinalSelection(context.mutableIsFinalSelection(), false);

  for (auto i = 0; i < numCases_; i++) {
    if (!remainingRows.get()->hasSelections()) {
      break;
    }

    // evaluate the case condition
    inputs_[2 * i]->eval(*remainingRows.get(), context, condition);

    auto booleanMix = getFlatBool(
        condition.get(),
        *remainingRows.get(),
        context,
        &tempValues_,
        nullptr,
        true,
        &values,
        nullptr);
    switch (booleanMix) {
      case BooleanMix::kAllTrue:
        inputs_[2 * i + 1]->eval(*remainingRows.get(), context, result);
        return;
      case BooleanMix::kAllNull:
      case BooleanMix::kAllFalse:
        continue;
      default: {
        bits::andBits(
            thenRows.get(rows.end(), false)->asMutableRange().bits(),
            remainingRows.get()->asRange().bits(),
            values,
            0,
            rows.end());
        thenRows.get()->updateBounds();

        if (thenRows.get()->hasSelections()) {
          if (result) {
            BaseVector::ensureWritable(
                *thenRows.get(), result->type(), context.pool(), &result);
          }

          inputs_[2 * i + 1]->eval(*thenRows.get(), context, result);
          remainingRows.get()->deselect(*thenRows.get());
        }
      }
    }
  }

  // Evaluate the "else" clause.
  if (remainingRows.get()->hasSelections()) {
    if (result) {
      BaseVector::ensureWritable(
          *remainingRows.get(), result->type(), context.pool(), &result);
    }

    if (hasElseClause_) {
      inputs_.back()->eval(*remainingRows.get(), context, result);

    } else {
      // fill in nulls for remainingRows
      remainingRows.get()->applyToSelected(
          [&](auto row) { result->setNull(row, true); });
    }
  }
}

bool SwitchExpr::propagatesNulls() const {
  // The "switch" expression propagates nulls when all of the following
  // conditions are met:
  // - All "then" clauses and optional "else" clause propagate nulls.
  // - All "then" clauses and optional "else" clause use the same inputs.
  // - All "condition" clauses use a subset of "then"/"else" inputs.

  for (auto i = 0; i < numCases_; i += 2) {
    if (!inputs_[i + 1]->propagatesNulls()) {
      return false;
    }
  }

  const auto& firstThenFields = inputs_[1]->distinctFields();
  for (auto i = 0; i < numCases_; ++i) {
    const auto& condition = inputs_[i * 2];
    const auto& thenClause = inputs_[i * 2 + 1];
    if (!Expr::isSameFields(firstThenFields, thenClause->distinctFields())) {
      return false;
    }

    if (!Expr::isSubsetOfFields(condition->distinctFields(), firstThenFields)) {
      return false;
    }
  }

  if (hasElseClause_) {
    const auto& elseClause = inputs_.back();
    if (!elseClause->propagatesNulls()) {
      return false;
    }
    if (!Expr::isSameFields(firstThenFields, elseClause->distinctFields())) {
      return false;
    }
  }

  return true;
}
} // namespace facebook::velox::exec
