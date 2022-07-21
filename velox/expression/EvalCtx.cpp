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
#include "velox/expression/Expr.h"

namespace facebook::velox::exec {

ContextSaver::~ContextSaver() {
  if (context) {
    context->restore(*this);
  }
}

EvalCtx::EvalCtx(core::ExecCtx* execCtx, ExprSet* exprSet, const RowVector* row)
    : execCtx_(execCtx), exprSet_(exprSet), row_(row) {
  // TODO Change the API to replace raw pointers with non-const references.
  // Sanity check inputs to prevent crashes.
  VELOX_CHECK_NOT_NULL(execCtx);
  VELOX_CHECK_NOT_NULL(exprSet);
  VELOX_CHECK_NOT_NULL(row);

  inputFlatNoNulls_ = true;
  for (const auto& child : row->children()) {
    VELOX_CHECK_NOT_NULL(child);
    if ((!child->isFlatEncoding() && !child->isConstantEncoding()) ||
        child->mayHaveNulls()) {
      inputFlatNoNulls_ = false;
    }
  }
}

EvalCtx::EvalCtx(core::ExecCtx* execCtx)
    : execCtx_(execCtx), exprSet_(nullptr), row_(nullptr) {
  VELOX_CHECK_NOT_NULL(execCtx);
}

void EvalCtx::setWrapped(
    Expr* FOLLY_NONNULL expr,
    VectorPtr source,
    const SelectivityVector& rows,
    VectorPtr& result) {
  VectorPtr localResult;
  if (wrapEncoding_ == VectorEncoding::Simple::DICTIONARY) {
    if (!source) {
      // If all rows are null, make a constant null vector of the right type.
      localResult =
          BaseVector::createNullConstant(expr->type(), rows.size(), pool());
    } else {
      BufferPtr nulls;
      if (!rows.isAllSelected()) {
        // The new base vector may be shorter than the original base vector
        // (e.g. if positions at the end of the original vector were not
        // selected for evaluation). In this case some original indices
        // corresponding to non-selected rows may point past the end of the base
        // vector. Disable these by setting corresponding positions to null.
        nulls = AlignedBuffer::allocate<bool>(rows.size(), pool(), bits::kNull);
        // Set the active rows to non-null.
        rows.clearNulls(nulls);
        if (wrapNulls_) {
          // Add the nulls from the wrapping.
          bits::andBits(
              nulls->asMutable<uint64_t>(),
              wrapNulls_->as<uint64_t>(),
              rows.begin(),
              rows.end());
        }
        // Reset nulls buffer if all positions happen to be non-null.
        if (bits::isAllSet(
                nulls->as<uint64_t>(), 0, rows.end(), bits::kNotNull)) {
          nulls.reset();
        }
      } else {
        nulls = wrapNulls_;
      }
      localResult = BaseVector::wrapInDictionary(
          std::move(nulls), wrap_, rows.end(), std::move(source));
    }
  } else if (wrapEncoding_ == VectorEncoding::Simple::CONSTANT) {
    localResult = BaseVector::wrapInConstant(
        rows.size(), constantWrapIndex_, std::move(source));
  } else {
    VELOX_FAIL("Bad expression wrap encoding {}", wrapEncoding_);
  }

  moveOrCopyResult(localResult, rows, result);
}

void EvalCtx::saveAndReset(ContextSaver& saver, const SelectivityVector& rows) {
  if (saver.context) {
    return;
  }
  saver.context = this;
  saver.rows = &rows;
  saver.finalSelection = finalSelection_;
  saver.peeled = std::move(peeledFields_);
  saver.wrap = std::move(wrap_);
  saver.wrapNulls = std::move(wrapNulls_);
  saver.wrapEncoding = wrapEncoding_;
  wrapEncoding_ = VectorEncoding::Simple::FLAT;
  saver.nullsPruned = nullsPruned_;
  nullsPruned_ = false;
  if (errors_) {
    saver.errors = std::move(errors_);
  }
}

void EvalCtx::addError(
    vector_size_t index,
    const std::exception_ptr& exceptionPtr,
    ErrorVectorPtr& errorsPtr) const {
  auto errors = errorsPtr.get();
  auto oldSize = errors ? errors->size() : 0;
  if (!errors) {
    auto size = index + 1;
    errorsPtr = std::make_shared<ErrorVector>(
        pool(),
        AlignedBuffer::allocate<bool>(size, pool(), true) /*nulls*/,
        size /*length*/,
        AlignedBuffer::allocate<ErrorVector::value_type>(
            size, pool(), ErrorVector::value_type()),
        std::vector<BufferPtr>(0),
        SimpleVectorStats<std::shared_ptr<void>>{},
        1 /*distinctValueCount*/,
        size /*nullCount*/,
        false /*isSorted*/,
        size /*representedBytes*/);
    errors = errorsPtr.get();
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

void EvalCtx::restore(ContextSaver& saver) {
  peeledFields_ = std::move(saver.peeled);
  nullsPruned_ = saver.nullsPruned;
  if (errors_) {
    int32_t errorSize = errors_->size();
    // A constant wrap has no indices.
    auto indices = wrap_ ? wrap_->as<vector_size_t>() : nullptr;
    auto wrapNulls = wrapNulls_ ? wrapNulls_->as<uint64_t>() : nullptr;
    SelectivityIterator iter(*saver.rows);
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
            saver.errors);
      }
    }
  }
  errors_ = std::move(saver.errors);
  wrap_ = std::move(saver.wrap);
  wrapNulls_ = std::move(saver.wrapNulls);
  wrapEncoding_ = saver.wrapEncoding;
  finalSelection_ = saver.finalSelection;
}

void EvalCtx::setError(
    vector_size_t index,
    const std::exception_ptr& exceptionPtr) {
  if (throwOnError_) {
    std::rethrow_exception(exceptionPtr);
  }
  addError(index, exceptionPtr, errors_);
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

void EvalCtx::ensureFieldLoaded(int32_t index, const SelectivityVector& rows) {
  auto field = getField(index);
  if (isLazyNotLoaded(*field)) {
    const auto& rowsToLoad = isFinalSelection_ ? rows : *finalSelection_;

    LocalDecodedVector holder(*this);
    auto decoded = holder.get();
    LocalSelectivityVector baseRowsHolder(*this, 0);
    auto baseRows = baseRowsHolder.get();
    auto rawField = field.get();
    LazyVector::ensureLoadedRows(field, rowsToLoad, *decoded, *baseRows);
    if (rawField != field.get()) {
      if (peeledFields_.empty()) {
        const_cast<RowVector*>(row_)->childAt(index) = field;
      } else {
        peeledFields_[index] = field;
      }
    }
  } else {
    // This is needed in any case because wrappers must be initialized also if
    // they contain a loaded lazyVector.
    field->loadedVector();
  }
}

} // namespace facebook::velox::exec
