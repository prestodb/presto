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
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/ScopedVarSetter.h"

namespace facebook::velox::exec {

namespace {
bool hasElseClause(const std::vector<ExprPtr>& inputs) {
  return inputs.size() % 2 == 1;
}
} // namespace

SwitchExpr::SwitchExpr(
    TypePtr type,
    const std::vector<ExprPtr>& inputs,
    bool inputsSupportFlatNoNullsFastPath)
    : SpecialForm(
          std::move(type),
          inputs,
          "switch",
          hasElseClause(inputs) && inputsSupportFlatNoNullsFastPath,
          false /* trackCpuUsage */),
      numCases_{inputs_.size() / 2},
      hasElseClause_{hasElseClause(inputs_)} {
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

void SwitchExpr::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  LocalSelectivityVector remainingRows(context, rows);

  LocalSelectivityVector thenRows(context);

  VectorPtr condition;
  const uint64_t* values;

  // SWITCH: fix finalSelection at "rows" unless already fixed
  ScopedFinalSelectionSetter scopedFinalSelectionSetter(context, &rows);

  for (auto i = 0; i < numCases_; i++) {
    if (!remainingRows.get()->hasSelections()) {
      break;
    }
    // evaluate the case condition
    inputs_[2 * i]->eval(*remainingRows.get(), context, condition);

    const auto booleanMix = getFlatBool(
        condition.get(),
        *remainingRows.get(),
        context,
        &tempValues_,
        nullptr,
        true,
        &values,
        nullptr);
    context.releaseVector(condition);
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
          inputs_[2 * i + 1]->eval(*thenRows.get(), context, result);
          remainingRows.get()->deselect(*thenRows.get());
        }
      }
    }
  }

  // Evaluate the "else" clause.
  if (remainingRows.get()->hasSelections()) {
    if (hasElseClause_) {
      inputs_.back()->eval(*remainingRows.get(), context, result);
    } else {
      context.ensureWritable(*remainingRows.get(), type(), result);

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

// static
TypePtr SwitchExpr::resolveType(const std::vector<TypePtr>& argTypes) {
  VELOX_CHECK_GT(
      argTypes.size(),
      1,
      "Switch statements expect at least 2 arguments, received {}",
      argTypes.size());
  // Type structure is [cond1Type, then1Type, cond2Type, then2Type, ...
  // elseType*]

  // Make sure all 'condition' expressions hae type BOOLEAN and all 'then' and
  // an optional 'else' clause have the same type.
  int numCases = argTypes.size() / 2;

  auto& expressionType = argTypes[1];

  for (auto i = 0; i < numCases; i++) {
    auto& conditionType = argTypes[i * 2];
    auto& thenType = argTypes[i * 2 + 1];

    VELOX_CHECK_EQ(
        conditionType->kind(),
        TypeKind::BOOLEAN,
        "Condition of  SWITCH statement is not bool");

    VELOX_CHECK(
        thenType->equivalent(*expressionType),
        "All then clauses of a SWITCH statement must have the same type. "
        "Expected {}, but got {}.",
        expressionType->toString(),
        thenType->toString());
  }

  bool hasElse = argTypes.size() % 2 == 1;

  if (hasElse) {
    auto& elseClauseType = argTypes.back();

    VELOX_CHECK(
        elseClauseType->equivalent(*expressionType),
        "Else clause of a SWITCH statement must have the same type as 'then' clauses. "
        "Expected {}, but got {}.",
        expressionType->toString(),
        elseClauseType->toString());
  }

  return expressionType;
}

TypePtr SwitchCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& argTypes) {
  return SwitchExpr::resolveType(argTypes);
}

ExprPtr SwitchCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool /* trackCpuUsage */) {
  bool inputsSupportFlatNoNullsFastPath =
      Expr::allSupportFlatNoNullsFastPath(compiledChildren);
  return std::make_shared<SwitchExpr>(
      type, std::move(compiledChildren), inputsSupportFlatNoNullsFastPath);
}

TypePtr IfCallToSpecialForm::resolveType(const std::vector<TypePtr>& argTypes) {
  VELOX_CHECK(
      argTypes.size() == 3,
      "An IF statement must have 3 clauses, the if clause, the then clause, and the else clause. Expected 3, but got {}.",
      argTypes.size());

  return SwitchCallToSpecialForm::resolveType(argTypes);
}
} // namespace facebook::velox::exec
