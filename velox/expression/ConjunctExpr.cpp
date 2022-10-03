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
#include "velox/expression/ScopedVarSetter.h"

namespace facebook::velox::exec {

namespace {

uint64_t* rowsWithError(
    const SelectivityVector& rows,
    const SelectivityVector& activeRows,
    EvalCtx& context,
    EvalCtx::ErrorVectorPtr& previousErrors,
    LocalSelectivityVector& errorRowsHolder) {
  auto errors = context.errors();
  if (!errors) {
    // No new errors. Put the old errors back.
    context.swapErrors(previousErrors);
    return nullptr;
  }
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
      errors->rawNulls(),
      rows.begin(),
      std::min(errors->size(), rows.end()));
  if (previousErrors) {
    // Add the new errors to the previous ones and free the new errors.
    bits::forEachSetBit(
        errors->rawNulls(), rows.begin(), errors->size(), [&](int32_t row) {
          context.addError(
              row,
              *std::static_pointer_cast<std::exception_ptr>(
                  errors->valueAt(row)),
              previousErrors);
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
  // null flag of error |= initial active & ~final active.
  int32_t numWords = bits::nwords(errors->size());
  auto errorNulls = errors->mutableNulls(errors->size())->asMutable<uint64_t>();
  for (int32_t i = 0; i < numWords; ++i) {
    errorNulls[i] &= rows.asRange().bits()[i] & activeRows.asRange().bits()[i];
    if (throwOnError && errorNulls[i]) {
      int32_t errorIndex = i * 64 + __builtin_ctzll(errorNulls[i]);
      if (errorIndex < errors->size() && errorIndex < rows.end()) {
        auto exceptionPtr = std::static_pointer_cast<std::exception_ptr>(
            errors->valueAt(errorIndex));
        std::rethrow_exception(*exceptionPtr);
      }
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
    auto nulls = flatResult->mutableNulls(rows.end());
    rows.clearNulls(nulls);
  }
  // Initialize result to all true for AND and all false for OR.
  auto values = flatResult->mutableValues(rows.end())->asMutable<uint64_t>();
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
    EvalCtx::ErrorVectorPtr errors;
    if (handleErrors) {
      context.swapErrors(errors);
    }

    SelectivityTimer timer(selectivity_[inputOrder_[i]], numActive);
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
                          ->config()
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
      result->addNulls(nullptr, *activeRows);
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
      bits::forEachSetBit(
          activeRows->asRange().bits(),
          activeRows->begin(),
          activeRows->end(),
          [&](int32_t row) {
            if (nulls && bits::isBitNull(nulls, row)) {
              result->setNull(row, true);
            } else {
              bool isTrue = bits::isBitSet(values, row);
              if (isAnd_ && !isTrue) {
                result->set(row, false);
                activeRows->setValid(row, false);
              } else if (!isAnd_ && isTrue) {
                result->set(row, true);
                activeRows->setValid(row, false);
              }
            }
          });
      activeRows->updateBounds();
    }
  }
}

std::string ConjunctExpr::toSql() const {
  std::stringstream out;
  out << "(" << inputs_[0]->toSql() << ")";
  for (auto i = 1; i < inputs_.size(); ++i) {
    out << " " << name_ << " "
        << "(" << inputs_[i]->toSql() << ")";
  }
  return out.str();
}
} // namespace facebook::velox::exec
