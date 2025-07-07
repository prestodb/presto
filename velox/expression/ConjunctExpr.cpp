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
#include "velox/expression/ConjunctExpr.h"
#include "velox/expression/BooleanMix.h"
#include "velox/expression/FieldReference.h"
#include "velox/expression/ScopedVarSetter.h"

namespace facebook::velox::exec {

namespace {

uint64_t* rowsWithError(
    const SelectivityVector& rows,
    const SelectivityVector& activeRows,
    EvalCtx& context,
    EvalErrorsPtr& previousErrors,
    LocalSelectivityVector& errorRowsHolder) {
  const auto* errorsPtr = context.errorsPtr();
  if (!errorsPtr || !*errorsPtr) {
    // No new errors. Put the old errors back.
    context.swapErrors(previousErrors);
    return nullptr;
  }

  const auto& errors = *errorsPtr;
  uint64_t* errorMask = nullptr;
  SelectivityVector* errorRows = errorRowsHolder.get();
  if (!errorRows) {
    errorRows = errorRowsHolder.get(rows.end(), false);
  }
  errorMask = errorRows->asMutableRange().bits();
  std::fill(errorMask, errorMask + bits::nwords(rows.end()), 0);
  // A 1 in activeRows with a not null in errors null flags makes a 1
  // in errorMask.
  bits::andBits(
      errorMask,
      activeRows.asRange().bits(),
      errors->errorFlags(),
      rows.begin(),
      std::min(errors->size(), rows.end()));
  if (previousErrors) {
    // Add the new errors to the previous ones and free the new errors.
    bits::forEachSetBit(
        errors->errorFlags(), rows.begin(), errors->size(), [&](int32_t row) {
          context.addError(row, errors, previousErrors);
        });
    context.swapErrors(previousErrors);
    previousErrors = nullptr;
  }
  return errorMask;
}

void finalizeErrors(
    const SelectivityVector& rows,
    const SelectivityVector& activeRows,
    bool throwOnError,
    EvalCtx& context) {
  auto errors = context.errors();
  if (!errors) {
    return;
  }
  // Pre-existing errors outside of initial active rows are preserved. Errors in
  // the initial active rows but not in the final active rows are cleared.
  auto size =
      std::min(std::min(rows.size(), activeRows.size()), errors->size());
  for (auto i = 0; i < size; ++i) {
    if (!errors->hasErrorAt(i)) {
      continue;
    }
    if (rows.isValid(i) && !activeRows.isValid(i)) {
      errors->clearError(i);
    }
    if (throwOnError) {
      errors->throwIfErrorAt(i);
    }
  }
}
} // namespace

void ConjunctExpr::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  // TODO Revisit error handling
  bool throwOnError = *context.mutableThrowOnError();
  ScopedVarSetter saveError(context.mutableThrowOnError(), false);
  context.ensureWritable(rows, type(), result);
  auto flatResult = result->asFlatVector<bool>();
  // clear nulls from the result for the active rows.
  if (flatResult->mayHaveNulls()) {
    auto& nulls = flatResult->mutableNulls(rows.end());
    rows.clearNulls(nulls);
  }
  // Initialize result to all true for AND and all false for OR.
  auto values = flatResult->mutableValues()->asMutable<uint64_t>();
  if (isAnd_) {
    bits::orBits(values, rows.asRange().bits(), rows.begin(), rows.end());
  } else {
    bits::andWithNegatedBits(
        values, rows.asRange().bits(), rows.begin(), rows.end());
  }

  // OR: fix finalSelection at "rows" unless already fixed
  ScopedFinalSelectionSetter scopedFinalSelectionSetter(
      context, &rows, !isAnd_);

  bool handleErrors = false;
  LocalSelectivityVector errorRows(context);
  LocalSelectivityVector activeRowsHolder(context, rows);
  auto activeRows = activeRowsHolder.get();
  VELOX_DCHECK(activeRows != nullptr);
  int32_t numActive = activeRows->countSelected();
  for (int32_t i = 0; i < inputs_.size(); ++i) {
    VectorPtr inputResult;
    VectorRecycler inputResultRecycler(inputResult, context.vectorPool());
    EvalErrorsPtr errors;
    if (handleErrors) {
      context.swapErrors(errors);
    }

    SelectivityTimer timer(selectivity_[inputOrder_[i]], numActive);
    if (evaluatesArgumentsOnNonIncreasingSelection()) {
      // Exclude loading rows that we know for sure will have a false result.
      for (auto* field : inputs_[inputOrder_[i]]->distinctFields()) {
        if (multiplyReferencedFields_.count(field) > 0) {
          context.ensureFieldLoaded(field->index(context), *activeRows);
        }
      }
    }
    inputs_[inputOrder_[i]]->eval(*activeRows, context, inputResult);
    if (context.errors()) {
      handleErrors = true;
    }
    uint64_t* extraActive = nullptr;
    if (handleErrors) {
      // Add rows with new errors to activeRows and merge these with
      // previous errors.
      extraActive =
          rowsWithError(rows, *activeRows, context, errors, errorRows);
    }
    updateResult(inputResult.get(), context, flatResult, activeRows);
    if (extraActive) {
      uint64_t* activeBits = activeRows->asMutableRange().bits();
      bits::orBits(activeBits, extraActive, rows.begin(), rows.end());
      activeRows->updateBounds();
    }
    numActive = activeRows->countSelected();
    selectivity_[inputOrder_[i]].addOutput(numActive);

    if (!numActive) {
      break;
    }
  }
  // Clear errors for 'rows' that are not in 'activeRows'.
  finalizeErrors(rows, *activeRows, throwOnError, context);
  if (!reorderEnabledChecked_) {
    reorderEnabled_ = context.execCtx()
                          ->queryCtx()
                          ->queryConfig()
                          .adaptiveFilterReorderingEnabled();
    reorderEnabledChecked_ = true;
  }
  if (reorderEnabled_) {
    maybeReorderInputs();
  }
}

void ConjunctExpr::maybeReorderInputs() {
  bool reorder = false;
  for (auto i = 1; i < inputs_.size(); ++i) {
    if (selectivity_[inputOrder_[i - 1]].timeToDropValue() >
        selectivity_[inputOrder_[i]].timeToDropValue()) {
      reorder = true;
      break;
    }
  }
  if (reorder) {
    std::sort(
        inputOrder_.begin(),
        inputOrder_.end(),
        [this](size_t left, size_t right) {
          return selectivity_[left].timeToDropValue() <
              selectivity_[right].timeToDropValue();
        });
  }
}

namespace {
// helper functions for conjuncts operating on values, nulls and active rows a
// word at a time.
inline void setFalseForOne(uint64_t active, uint64_t source, uint64_t& target) {
  target &= ~active | ~source;
}

inline void setTrueForOne(uint64_t active, uint64_t source, uint64_t& target) {
  target |= active & source;
}

inline void
setPresentForOne(uint64_t active, uint64_t source, uint64_t& target) {
  target |= active & source;
}

inline void
setNonPresentForOne(uint64_t active, uint64_t source, uint64_t& target) {
  target &= ~active | ~source;
}

inline void updateAnd(
    uint64_t& resultValue,
    uint64_t& resultPresent,
    uint64_t& active,
    uint64_t testValue,
    uint64_t testPresent) {
  auto testFalse = ~testValue & testPresent;
  setFalseForOne(active, testFalse, resultValue);
  setPresentForOne(active, testFalse, resultPresent);
  auto resultTrue = resultValue & resultPresent;
  setNonPresentForOne(
      active, resultPresent & resultTrue & ~testPresent, resultPresent);
  active &= ~testFalse;
}

inline void updateOr(
    uint64_t& resultValue,
    uint64_t& resultPresent,
    uint64_t& active,
    uint64_t testValue,
    uint64_t testPresent) {
  auto testTrue = testValue & testPresent;
  setTrueForOne(active, testTrue, resultValue);
  setPresentForOne(active, testTrue, resultPresent);
  auto resultFalse = ~resultValue & resultPresent;
  setNonPresentForOne(
      active, resultPresent & resultFalse & ~testPresent, resultPresent);
  active &= ~testTrue;
}

} // namespace

void ConjunctExpr::updateResult(
    BaseVector* inputResult,
    EvalCtx& context,
    FlatVector<bool>* result,
    SelectivityVector* activeRows) {
  // Set result and clear active rows for the positions that are decided.
  const uint64_t* values = nullptr;
  const uint64_t* nulls = nullptr;
  switch (getFlatBool(
      inputResult,
      *activeRows,
      context,
      &tempValues_,
      &tempNulls_,
      false,
      &values,
      &nulls)) {
    case BooleanMix::kAllNull:
      result->addNulls(*activeRows);
      return;
    case BooleanMix::kAllFalse:
      if (isAnd_) {
        // Clear the nulls and values for all active rows.
        if (result->mayHaveNulls()) {
          activeRows->clearNulls(result->mutableRawNulls());
        }
        bits::andWithNegatedBits(
            result->mutableRawValues<uint64_t>(),
            activeRows->asRange().bits(),
            activeRows->begin(),
            activeRows->end());
        activeRows->clearAll();
      }
      return;
    case BooleanMix::kAllTrue:
      if (!isAnd_) {
        if (result->mayHaveNulls()) {
          bits::orBits(
              result->mutableRawNulls(),
              activeRows->asRange().bits(),
              activeRows->begin(),
              activeRows->end());
        }
        bits::orBits(
            result->mutableRawValues<uint64_t>(),
            activeRows->asRange().bits(),
            activeRows->begin(),
            activeRows->end());

        activeRows->clearAll();
      }
      return;
    default: {
      uint64_t* resultValues = result->mutableRawValues<uint64_t>();
      uint64_t* resultNulls = nullptr;
      if (nulls || result->mayHaveNulls()) {
        resultNulls = result->mutableRawNulls();
      }
      auto* activeBits = activeRows->asMutableRange().bits();
      if (isAnd_) {
        bits::forEachWord(
            activeRows->begin(),
            activeRows->end(),
            [&](int32_t index, uint64_t mask) {
              uint64_t nullWord =
                  resultNulls ? resultNulls[index] : bits::kNotNull64;
              uint64_t activeWord = activeBits[index] & mask;
              updateAnd(
                  resultValues[index],
                  nullWord,
                  activeWord,
                  values[index],
                  nulls ? nulls[index] : bits::kNotNull64);
              if (resultNulls) {
                resultNulls[index] = nullWord;
              }
              activeBits[index] &= ~mask | activeWord;
            },
            [&](int32_t index) {
              uint64_t nullWord =
                  resultNulls ? resultNulls[index] : bits::kNotNull64;
              updateAnd(
                  resultValues[index],
                  nullWord,
                  activeBits[index],
                  values[index],
                  nulls ? nulls[index] : bits::kNotNull64);
              if (resultNulls) {
                resultNulls[index] = nullWord;
              }
            });
      } else {
        bits::forEachWord(
            activeRows->begin(),
            activeRows->end(),
            [&](int32_t index, uint64_t mask) {
              uint64_t nullWord =
                  resultNulls ? resultNulls[index] : bits::kNotNull64;
              uint64_t activeWord = activeBits[index] & mask;
              updateOr(
                  resultValues[index],
                  nullWord,
                  activeWord,
                  values[index],
                  nulls ? nulls[index] : bits::kNotNull64);
              if (resultNulls) {
                resultNulls[index] = nullWord;
              }
              activeBits[index] &= ~mask | activeWord;
            },
            [&](int32_t index) {
              uint64_t nullWord =
                  resultNulls ? resultNulls[index] : bits::kNotNull64;
              updateOr(
                  resultValues[index],
                  nullWord,
                  activeBits[index],
                  values[index],
                  nulls ? nulls[index] : bits::kNotNull64);
              if (resultNulls) {
                resultNulls[index] = nullWord;
              }
            });
      }
      activeRows->updateBounds();
    }
  }
}

std::string ConjunctExpr::toSql(
    std::vector<VectorPtr>* complexConstants) const {
  std::stringstream out;
  out << "(" << inputs_[0]->toSql(complexConstants) << ")";
  for (auto i = 1; i < inputs_.size(); ++i) {
    out << " " << name_ << " " << "(" << inputs_[i]->toSql(complexConstants)
        << ")";
  }
  return out.str();
}

// static
TypePtr ConjunctExpr::resolveType(const std::vector<TypePtr>& argTypes) {
  VELOX_CHECK_GT(
      argTypes.size(),
      0,
      "Conjunct expressions expect at least one argument, received: {}",
      argTypes.size());

  for (const auto& argType : argTypes) {
    VELOX_CHECK(
        argType->kind() == TypeKind::BOOLEAN ||
            argType->kind() == TypeKind::UNKNOWN,
        "Conjunct expressions expect BOOLEAN or UNKNOWN arguments, received: {}",
        argType->toString());
  }

  return BOOLEAN();
}

TypePtr ConjunctCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& argTypes) {
  return ConjunctExpr::resolveType(argTypes);
}

ExprPtr ConjunctCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool /* trackCpuUsage */,
    const core::QueryConfig& /*config*/) {
  bool inputsSupportFlatNoNullsFastPath =
      Expr::allSupportFlatNoNullsFastPath(compiledChildren);

  return std::make_shared<ConjunctExpr>(
      type,
      std::move(compiledChildren),
      isAnd_,
      inputsSupportFlatNoNullsFastPath);
}
} // namespace facebook::velox::exec
