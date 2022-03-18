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

#include "velox/expression/EvalCtx.h"
#include "velox/common/base/RawVector.h"
#include "velox/expression/ControlExpr.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::exec {

ContextSaver::~ContextSaver() {
  if (context) {
    context->restore(this);
  }
}

EvalCtx::EvalCtx(core::ExecCtx* execCtx, ExprSet* exprSet, const RowVector* row)
    : execCtx_(execCtx), exprSet_(exprSet), row_(row) {}

void EvalCtx::setWrapped(
    Expr* expr,
    VectorPtr source,
    const SelectivityVector& rows,
    VectorPtr* result) {
  if (*result) {
    BaseVector::ensureWritable(rows, expr->type(), pool(), result);
    if (wrapEncoding_ == VectorEncoding::Simple::DICTIONARY) {
      if (!wrapNulls_) {
        (*result)->copy(source.get(), rows, wrap_->as<vector_size_t>());
      } else {
        auto nonNullRows = rows;
        nonNullRows.deselectNulls(
            wrapNulls_->as<uint64_t>(), rows.begin(), rows.end());
        if (nonNullRows.hasSelections()) {
          (*result)->copy(
              source.get(), nonNullRows, wrap_->as<vector_size_t>());
        }
        (*result)->addNulls(wrapNulls_->as<uint64_t>(), rows);
      }
      return;
    }
    if (wrapEncoding_ == VectorEncoding::Simple::CONSTANT) {
      rows.applyToSelected([&](auto row) {
        (*result)->copy(source.get(), row, rows.begin(), 1);
      });

      return;
    }
    VELOX_NYI();
  }
  if (wrapEncoding_ == VectorEncoding::Simple::DICTIONARY) {
    // If returning a dictionary for a conditional that will be merged with
    // other branches of a conditional, set the undefined positions of the
    // DictionaryVector to null.
    BufferPtr nulls;
    if (!isFinalSelection_) {
      // If this is not the final selection, i.e. we are inside an if, start
      // with all nulls.
      nulls = AlignedBuffer::allocate<bool>(rows.size(), pool(), bits::kNull);
      // Set the active rows to non-null.
      bits::orBits(
          nulls->asMutable<uint64_t>(),
          rows.asRange().bits(),
          rows.begin(),
          rows.end());
      if (wrapNulls_) {
        // Add the nulls from the wrapping.
        bits::andBits(
            nulls->asMutable<uint64_t>(),
            wrapNulls_->as<uint64_t>(),
            rows.begin(),
            rows.end());
      }
    } else {
      nulls = wrapNulls_;
    }
    if (!source) {
      // If all rows are null, make a flat vector of the right type with
      // the nulls.
      VELOX_CHECK(nulls);
      *result = BaseVector::create(expr->type(), rows.size(), pool());
      (*result)->addNulls(nulls->as<uint64_t>(), rows);
      return;
    }
    *result = BaseVector::wrapInDictionary(
        std::move(nulls), wrap_, rows.end(), std::move(source));
    return;
  }
  if (wrapEncoding_ == VectorEncoding::Simple::CONSTANT) {
    *result = BaseVector::wrapInConstant(
        rows.size(), constantWrapIndex_, std::move(source));
    return;
  }
  VELOX_CHECK(false, "Bad expression wrap encoding {}", wrapEncoding_);
}

void EvalCtx::saveAndReset(ContextSaver* saver, const SelectivityVector& rows) {
  if (saver->context) {
    return;
  }
  saver->context = this;
  saver->rows = &rows;
  saver->finalSelection = finalSelection_;
  saver->peeled = std::move(peeledFields_);
  saver->wrap = std::move(wrap_);
  saver->wrapNulls = std::move(wrapNulls_);
  saver->wrapEncoding = wrapEncoding_;
  wrapEncoding_ = VectorEncoding::Simple::FLAT;
  saver->nullsPruned = nullsPruned_;
  nullsPruned_ = false;
  if (errors_) {
    saver->errors = std::move(errors_);
  }
}

void EvalCtx::addError(
    vector_size_t index,
    const std::exception_ptr& exceptionPtr,
    ErrorVectorPtr* errorsPtr) const {
  auto errors = errorsPtr->get();
  auto oldSize = errors ? errors->size() : 0;
  if (!errors) {
    auto size = index + 1;
    *errorsPtr = std::make_shared<ErrorVector>(
        pool(),
        AlignedBuffer::allocate<bool>(size, pool(), true) /*nulls*/,
        size /*length*/,
        AlignedBuffer::allocate<ErrorVector::value_type>(
            size, pool(), ErrorVector::value_type()),
        std::vector<BufferPtr>(0),
        cdvi::EMPTY_METADATA,
        1 /*distinctValueCount*/,
        size /*nullCount*/,
        false /*isSorted*/,
        size /*representedBytes*/);
    errors = errorsPtr->get();
  } else if (errors->size() <= index) {
    errors->resize(index + 1);
  }
  // Set all new positions to null, including the one to be set.
  for (int32_t i = oldSize; i <= index; ++i) {
    errors->setNull(i, true);
  }
  if (errors->isNullAt(index)) {
    errors->setNull(index, false);
    errors->set(index, std::make_shared<std::exception_ptr>(exceptionPtr));
  }
}

void EvalCtx::restore(ContextSaver* saver) {
  peeledFields_ = std::move(saver->peeled);
  nullsPruned_ = saver->nullsPruned;
  if (errors_) {
    int32_t errorSize = errors_->size();
    // A constant wrap has no indices.
    auto indices = wrap_ ? wrap_->as<vector_size_t>() : nullptr;
    auto wrapNulls = wrapNulls_ ? wrapNulls_->as<uint64_t>() : nullptr;
    SelectivityIterator iter(*saver->rows);
    vector_size_t row;
    while (iter.next(row)) {
      // A known null in the outer row masks an error.
      if (wrapNulls && bits::isBitNull(wrapNulls, row)) {
        continue;
      }
      vector_size_t innerRow = indices ? indices[row] : constantWrapIndex_;
      if (innerRow < errorSize && !errors_->isNullAt(innerRow)) {
        addError(
            row,
            *std::static_pointer_cast<std::exception_ptr>(
                errors_->valueAt(innerRow)),
            &saver->errors);
      }
    }
  }
  errors_ = std::move(saver->errors);
  wrap_ = std::move(saver->wrap);
  wrapNulls_ = std::move(saver->wrapNulls);
  wrapEncoding_ = saver->wrapEncoding;
  finalSelection_ = saver->finalSelection;
}

void EvalCtx::setError(
    vector_size_t index,
    const std::exception_ptr& exceptionPtr) {
  if (throwOnError_) {
    std::rethrow_exception(exceptionPtr);
  }
  addError(index, exceptionPtr, &errors_);
}

void EvalCtx::setErrors(
    const SelectivityVector& rows,
    const std::exception_ptr& exceptionPtr) {
  rows.applyToSelected([&](auto row) { setError(row, exceptionPtr); });
}

const VectorPtr& EvalCtx::getField(int32_t index) const {
  const VectorPtr* field;
  if (!peeledFields_.empty()) {
    field = &peeledFields_[index];
  } else {
    field = &row_->childAt(index);
  }
  if ((*field)->isLazy() && (*field)->asUnchecked<LazyVector>()->isLoaded()) {
    auto lazy = (*field)->asUnchecked<LazyVector>();
    return lazy->loadedVectorShared();
  }
  return *field;
}

BaseVector* EvalCtx::getRawField(int32_t index) const {
  BaseVector* field;
  if (!peeledFields_.empty()) {
    field = peeledFields_[index].get();
  } else {
    field = row_->childAt(index).get();
  }
  if (field->isLazy() && field->asUnchecked<LazyVector>()->isLoaded()) {
    return field->loadedVector();
  }
  return field;
}

void EvalCtx::ensureFieldLoaded(int32_t index, const SelectivityVector& rows) {
  auto field = getField(index);
  if (isLazyNotLoaded(*field)) {
    const auto& rowsToLoad = isFinalSelection_ ? rows : *finalSelection_;

    LocalDecodedVector holder(this);
    auto decoded = holder.get();
    LocalSelectivityVector baseRowsHolder(this, 0);
    auto baseRows = baseRowsHolder.get();
    auto rawField = field.get();
    LazyVector::ensureLoadedRows(field, rowsToLoad, *decoded, *baseRows);
    if (rawField != field.get()) {
      const_cast<RowVector*>(row_)->childAt(index) = field;
    }
  } else {
    // This is needed in any case because wrappers must be initialized also if
    // they contain a loaded lazyVector.
    field->loadedVector();
  }
}

} // namespace facebook::velox::exec
