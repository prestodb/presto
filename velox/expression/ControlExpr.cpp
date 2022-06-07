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

#include "velox/expression/ControlExpr.h"
#include "velox/core/Expressions.h"
#include "velox/expression/VarSetter.h"
#include "velox/functions/lib/string/StringCore.h"

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

void FieldReference::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  ExceptionContextSetter exceptionContext(
      {[](auto* expr) { return static_cast<Expr*>(expr)->toString(); }, this});

  if (result) {
    BaseVector::ensureWritable(rows, type_, context.pool(), &result);
  }
  const RowVector* row;
  DecodedVector decoded;
  VectorPtr input;
  bool useDecode = false;
  if (inputs_.empty()) {
    row = context.row();
  } else {
    inputs_[0]->eval(rows, context, input);

    if (auto rowTry = input->as<RowVector>()) {
      // Make sure output is not copied
      if (rowTry->isCodegenOutput()) {
        auto rowType = dynamic_cast<const RowType*>(rowTry->type().get());
        index_ = rowType->getChildIdx(field_);
        result = std::move(rowTry->childAt(index_));
        VELOX_CHECK(result.unique());
        return;
      }
    }

    decoded.decode(*input.get(), rows);
    useDecode = !decoded.isIdentityMapping();
    const BaseVector* base = decoded.base();
    VELOX_CHECK(base->encoding() == VectorEncoding::Simple::ROW);
    row = base->as<const RowVector>();
  }
  if (index_ == -1) {
    auto rowType = dynamic_cast<const RowType*>(row->type().get());
    VELOX_CHECK(rowType);
    index_ = rowType->getChildIdx(field_);
  }
  // If we refer to a column of the context row, this may have been
  // peeled due to peeling off encoding, hence access it via
  // 'context'.  Check if the child is unique before taking the second
  // reference. Unique constant vectors can be resized in place, non-unique
  // must be copied to set the size.
  bool isUniqueChild = inputs_.empty() ? context.getField(index_).unique()
                                       : row->childAt(index_).unique();
  VectorPtr child =
      inputs_.empty() ? context.getField(index_) : row->childAt(index_);
  if (result.get()) {
    auto indices = useDecode ? decoded.indices() : nullptr;
    result->copy(child.get(), rows, indices);
  } else {
    if (child->encoding() == VectorEncoding::Simple::LAZY) {
      child = BaseVector::loadedVectorShared(child);
    }
    // The caller relies on vectors having a meaningful size. If we
    // have a constant that is not wrapped in anything we set its size
    // to correspond to rows.size(). This is in place for unique ones
    // and a copy otherwise.
    if (!useDecode && child->isConstantEncoding()) {
      if (isUniqueChild) {
        child->resize(rows.size());
      } else {
        child = BaseVector::wrapInConstant(rows.size(), 0, child);
      }
    }
    result = useDecode ? std::move(decoded.wrap(child, *input.get(), rows))
                       : std::move(child);
  }
}

void FieldReference::evalSpecialFormSimplified(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  ExceptionContextSetter exceptionContext(
      {[](auto* expr) { return static_cast<Expr*>(expr)->toString(); }, this});

  auto row = context.row();
  result = row->childAt(index(context));
  BaseVector::flattenVector(&result, rows.end());
}

void SwitchExpr::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  ExceptionContextSetter exceptionContext(
      {[](auto* expr) { return static_cast<Expr*>(expr)->toString(); }, this});

  LocalSelectivityVector remainingRows(context, rows);

  LocalSelectivityVector thenRows(context);

  VectorPtr condition;
  const uint64_t* values;

  // SWITCH: fix finalSelection at "rows" unless already fixed
  VarSetter finalSelection(
      context.mutableFinalSelection(), &rows, context.isFinalSelection());
  VarSetter isFinalSelection(context.mutableIsFinalSelection(), false);

  const bool isString = type()->kind() == TypeKind::VARCHAR;

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
  ExceptionContextSetter exceptionContext(
      {[](auto* expr) { return static_cast<Expr*>(expr)->toString(); }, this});

  // TODO Revisit error handling
  bool throwOnError = *context.mutableThrowOnError();
  VarSetter saveError(context.mutableThrowOnError(), false);
  BaseVector::ensureWritable(rows, type(), context.pool(), &result);
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
  VarSetter finalSelectionOr(
      context.mutableFinalSelection(),
      &rows,
      !isAnd_ && context.isFinalSelection());
  VarSetter isFinalSelectionOr(
      context.mutableIsFinalSelection(), false, !isAnd_);

  bool handleErrors = false;
  LocalSelectivityVector errorRows(context);
  LocalSelectivityVector activeRowsHolder(context, rows);
  auto activeRows = activeRowsHolder.get();
  assert(activeRows); // lint
  int32_t numActive = activeRows->countSelected();
  for (int32_t i = 0; i < inputs_.size(); ++i) {
    VectorPtr inputResult;
    EvalCtx::ErrorVectorPtr errors;
    if (handleErrors) {
      context.swapErrors(errors);
    }

    // AND: reduce finalSelection to activeRows unless it has been fixed by IF
    // or OR above.
    VarSetter finalSelectionAnd(
        context.mutableFinalSelection(),
        static_cast<const SelectivityVector*>(activeRows),
        isAnd_ && context.isFinalSelection());

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

namespace {
/// Checks if bits in specified positions are all set, all unset or mixed.
BooleanMix refineBooleanMixNonNull(
    const uint64_t* bits,
    const SelectivityVector& rows) {
  int32_t first = bits::findFirstBit(bits, rows.begin(), rows.end());
  if (first < 0) {
    return BooleanMix::kAllFalse;
  }
  if (first == rows.begin() && bits::isAllSet(bits, rows.begin(), rows.end())) {
    return BooleanMix::kAllTrue;
  }
  return BooleanMix::kMixNonNull;
}
} // namespace

BooleanMix getFlatBool(
    BaseVector* vector,
    const SelectivityVector& activeRows,
    EvalCtx& context,
    BufferPtr* tempValues,
    BufferPtr* tempNulls,
    bool mergeNullsToValues,
    const uint64_t** valuesOut,
    const uint64_t** nullsOut) {
  VELOX_CHECK_EQ(vector->typeKind(), TypeKind::BOOLEAN);
  const auto size = activeRows.end();
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT: {
      auto values = vector->asFlatVector<bool>()->rawValues<uint64_t>();
      if (!values) {
        return BooleanMix::kAllNull;
      }
      auto nulls = vector->rawNulls();
      if (nulls && mergeNullsToValues) {
        uint64_t* mergedValues;
        BaseVector::ensureBuffer<bool>(
            size, context.pool(), tempValues, &mergedValues);

        bits::andBits(
            mergedValues, values, nulls, activeRows.begin(), activeRows.end());

        bits::andBits(
            mergedValues,
            activeRows.asRange().bits(),
            activeRows.begin(),
            activeRows.end());

        *valuesOut = mergedValues;
        return refineBooleanMixNonNull(mergedValues, activeRows);
      }
      *valuesOut = values;
      if (!mergeNullsToValues) {
        *nullsOut = nulls;
      }
      return nulls ? BooleanMix::kMix
                   : refineBooleanMixNonNull(values, activeRows);
    }
    case VectorEncoding::Simple::CONSTANT: {
      if (vector->isNullAt(0)) {
        return BooleanMix::kAllNull;
      }
      return vector->as<ConstantVector<bool>>()->valueAt(0)
          ? BooleanMix::kAllTrue
          : BooleanMix::kAllFalse;
    }
    default: {
      uint64_t* nullsToSet = nullptr;
      uint64_t* valuesToSet = nullptr;
      if (vector->mayHaveNulls() && !mergeNullsToValues) {
        BaseVector::ensureBuffer<bool>(
            size, context.pool(), tempNulls, &nullsToSet);
        memset(nullsToSet, bits::kNotNullByte, bits::nbytes(size));
      }
      BaseVector::ensureBuffer<bool>(
          size, context.pool(), tempValues, &valuesToSet);
      memset(valuesToSet, 0, bits::nbytes(size));
      DecodedVector decoded(*vector, activeRows);
      auto values = decoded.data<uint64_t>();
      auto nulls = decoded.nulls();
      auto indices = decoded.indices();
      auto nullIndices = decoded.nullIndices();
      activeRows.applyToSelected([&](int32_t i) {
        auto index = indices[i];
        bool isNull =
            nulls && bits::isBitNull(nulls, nullIndices ? nullIndices[i] : i);
        if (mergeNullsToValues && nulls) {
          if (!isNull && bits::isBitSet(values, index)) {
            bits::setBit(valuesToSet, i);
          }
        } else if (!isNull && bits::isBitSet(values, index)) {
          bits::setBit(valuesToSet, i);
        }
        if (nullsToSet && isNull) {
          bits::setNull(nullsToSet, i);
        }
      });
      if (!mergeNullsToValues) {
        *nullsOut = nullsToSet;
      }
      *valuesOut = valuesToSet;
      return nullsToSet ? BooleanMix::kMix
                        : refineBooleanMixNonNull(valuesToSet, activeRows);
    }
  }
}

namespace {

// Represents an interpreted lambda expression. 'signature' describes
// the parameters passed by the caller. 'capture' is a row with a
// leading nullptr for each element in 'signature' followed by the
// vectors for the captures from the lambda's definition scope.
class ExprCallable : public Callable {
 public:
  ExprCallable(
      std::shared_ptr<const RowType> signature,
      std::shared_ptr<RowVector> capture,
      std::shared_ptr<Expr> body)
      : signature_(std::move(signature)),
        capture_(std::move(capture)),
        body_(std::move(body)) {}

  bool hasCapture() const override {
    return capture_->childrenSize() > signature_->size();
  }

  void apply(
      const SelectivityVector& rows,
      BufferPtr wrapCapture,
      EvalCtx* context,
      const std::vector<VectorPtr>& args,
      VectorPtr* result) override {
    std::vector<VectorPtr> allVectors = args;
    for (auto index = args.size(); index < capture_->childrenSize(); ++index) {
      auto values = capture_->childAt(index);
      if (wrapCapture) {
        values = BaseVector::wrapInDictionary(
            BufferPtr(nullptr), wrapCapture, rows.end(), values);
      }
      allVectors.push_back(values);
    }
    auto row = std::make_shared<RowVector>(
        context->pool(),
        capture_->type(),
        BufferPtr(nullptr),
        rows.end(),
        std::move(allVectors));
    EvalCtx lambdaCtx(context->execCtx(), context->exprSet(), row.get());
    if (!context->isFinalSelection()) {
      *lambdaCtx.mutableIsFinalSelection() = false;
      *lambdaCtx.mutableFinalSelection() = context->finalSelection();
    }
    body_->eval(rows, lambdaCtx, *result);
  }

 private:
  std::shared_ptr<const RowType> signature_;
  RowVectorPtr capture_;
  std::shared_ptr<Expr> body_;
};

} // namespace

void LambdaExpr::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  ExceptionContextSetter exceptionContext(
      {[](auto* expr) { return static_cast<Expr*>(expr)->toString(); }, this});

  if (!typeWithCapture_) {
    makeTypeWithCapture(context);
  }
  std::vector<VectorPtr> values(typeWithCapture_->size());
  for (auto i = 0; i < captureChannels_.size(); ++i) {
    assert(!values.empty());
    values[signature_->size() + i] = context.getField(captureChannels_[i]);
  }
  auto capture = std::make_shared<RowVector>(
      context.pool(),
      typeWithCapture_,
      BufferPtr(nullptr),
      rows.end(),
      values,
      0);
  auto callable = std::make_shared<ExprCallable>(signature_, capture, body_);
  std::shared_ptr<FunctionVector> functions;
  if (!result) {
    functions = std::make_shared<FunctionVector>(context.pool(), type_);
    result = functions;
  } else {
    VELOX_CHECK(result->encoding() == VectorEncoding::Simple::FUNCTION);
    functions = std::static_pointer_cast<FunctionVector>(result);
  }
  functions->addFunction(callable, rows);
}

void LambdaExpr::makeTypeWithCapture(EvalCtx& context) {
  // On first use, compose the type of parameters + capture and set
  // the indices of captures in the context row.
  if (capture_.empty()) {
    typeWithCapture_ = signature_;
  } else {
    auto& contextType = context.row()->type()->as<TypeKind::ROW>();
    auto parameterNames = signature_->names();
    auto parameterTypes = signature_->children();
    for (auto& reference : capture_) {
      auto& name = reference->field();
      auto channel = contextType.getChildIdx(name);
      captureChannels_.push_back(channel);
      parameterNames.push_back(name);
      parameterTypes.push_back(contextType.childAt(channel));
    }
    typeWithCapture_ =
        ROW(std::move(parameterNames), std::move(parameterTypes));
  }
}

// Apply onError methods of registered listeners on every row that encounters
// errors. The error vector must exist.
void applyListenersOnError(
    const SelectivityVector& rows,
    const EvalCtx& context) {
  auto errors = context.errors();
  VELOX_CHECK_NOT_NULL(errors);

  exec::LocalSelectivityVector errorRows(context.execCtx(), errors->size());
  errorRows->clearAll();
  rows.applyToSelected([&](auto row) {
    if (row < errors->size() && !errors->isNullAt(row)) {
      errorRows->setValid(row, true);
    }
  });
  errorRows->updateBounds();

  if (!errorRows->hasSelections()) {
    return;
  }

  exprSetListeners().withRLock([&](auto& listeners) {
    if (!listeners.empty()) {
      for (auto& listener : listeners) {
        listener->onError(*errorRows, *errors);
      }
    }
  });
}

void TryExpr::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  ExceptionContextSetter exceptionContext(
      {[](auto* expr) { return static_cast<Expr*>(expr)->toString(); }, this});

  VarSetter throwOnError(context.mutableThrowOnError(), false);
  // It's possible with nested TRY expressions that some rows already threw
  // exceptions in earlier expressions that haven't been handled yet. To avoid
  // incorrectly handling them here, store those errors and temporarily reset
  // the errors in context to nullptr, so we only handle errors coming from
  // expressions that are children of this TRY expression.
  // This also prevents this TRY expression from leaking exceptions to the
  // parent TRY expression, so the parent won't incorrectly null out rows that
  // threw exceptions which this expression already handled.
  VarSetter<EvalCtx::ErrorVectorPtr> errorsSetter(context.errorsPtr(), nullptr);
  inputs_[0]->eval(rows, context, result);

  nullOutErrors(rows, context, result);
}

void TryExpr::evalSpecialFormSimplified(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  VarSetter throwOnError(context.mutableThrowOnError(), false);
  // It's possible with nested TRY expressions that some rows already threw
  // exceptions in earlier expressions that haven't been handled yet. To avoid
  // incorrectly handling them here, store those errors and temporarily reset
  // the errors in context to nullptr, so we only handle errors coming from
  // expressions that are children of this TRY expression.
  // This also prevents this TRY expression from leaking exceptions to the
  // parent TRY expression, so the parent won't incorrectly null out rows that
  // threw exceptions which this expression already handled.
  VarSetter<EvalCtx::ErrorVectorPtr> errorsSetter(context.errorsPtr(), nullptr);
  inputs_[0]->evalSimplified(rows, context, result);

  nullOutErrors(rows, context, result);
}

void TryExpr::nullOutErrors(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  auto errors = context.errors();
  if (errors) {
    applyListenersOnError(rows, context);

    if (result->encoding() == VectorEncoding::Simple::CONSTANT) {
      // Since it's constant, if any row is NULL they're all NULL, so check row
      // 0 arbitrarily.
      if (result->isNullAt(0)) {
        // The result is already a NULL constant, so this is a no-op.
        return;
      }

      // If the result is constant, the input should have been constant as
      // well, so we should have consistently gotten exceptions.
      // If this is not the case, an easy way to handle it would be to wrap
      // the ConstantVector in a DictionaryVector and NULL out the rows that
      // saw errors.
      VELOX_DCHECK(
          rows.testSelected([&](auto row) { return !errors->isNullAt(row); }));
      // Set the result to be a NULL constant.
      result = BaseVector::createNullConstant(
          result->type(), result->size(), context.pool());
    } else {
      rows.applyToSelected([&](auto row) {
        if (row < errors->size() && !errors->isNullAt(row)) {
          result->setNull(row, true);
        }
      });
    }
  }
}

} // namespace facebook::velox::exec
