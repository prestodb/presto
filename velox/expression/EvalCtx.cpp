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
#include <exception>
#include "velox/common/base/RawVector.h"
#include "velox/expression/Expr.h"
#include "velox/expression/PeeledEncoding.h"

namespace facebook::velox::exec {

ScopedContextSaver::~ScopedContextSaver() {
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

void EvalCtx::saveAndReset(
    ScopedContextSaver& saver,
    const SelectivityVector& rows) {
  if (saver.context) {
    return;
  }
  saver.context = this;
  saver.rows = &rows;
  saver.finalSelection = finalSelection_;
  saver.peeled = std::move(peeledFields_);
  saver.peeledEncoding = std::move(peeledEncoding_);
  peeledEncoding_.reset();
  saver.nullsPruned = nullsPruned_;
  nullsPruned_ = false;
  if (errors_) {
    saver.errors = std::move(errors_);
  }
}

void EvalCtx::ensureErrorsVectorSize(ErrorVectorPtr& vector, vector_size_t size)
    const {
  auto oldSize = vector ? vector->size() : 0;
  if (!vector) {
    vector = std::make_shared<ErrorVector>(
        pool(),
        OpaqueType::create<void>(),
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
  } else if (vector->size() < size) {
    vector->resize(size, false);
  }
  // Set all new positions to null, including the one to be set.
  for (auto i = oldSize; i < size; ++i) {
    vector->setNull(i, true);
  }
}

void EvalCtx::addError(
    vector_size_t index,
    const std::exception_ptr& exceptionPtr,
    ErrorVectorPtr& errorsPtr) const {
  ensureErrorsVectorSize(errorsPtr, index + 1);
  if (errorsPtr->isNullAt(index)) {
    errorsPtr->setNull(index, false);
    errorsPtr->set(index, std::make_shared<std::exception_ptr>(exceptionPtr));
  }
}

void EvalCtx::addErrors(
    const SelectivityVector& rows,
    const ErrorVectorPtr& fromErrors,
    ErrorVectorPtr& toErrors) const {
  if (!fromErrors) {
    return;
  }

  ensureErrorsVectorSize(toErrors, fromErrors->size());
  rows.testSelected([&](auto row) {
    if (!fromErrors->isIndexInRange(row)) {
      return false;
    }
    if (!fromErrors->isNullAt(row) && toErrors->isNullAt(row)) {
      toErrors->set(
          row,
          std::static_pointer_cast<std::exception_ptr>(
              fromErrors->valueAt(row)));
    }
    return true;
  });
}

void EvalCtx::restore(ScopedContextSaver& saver) {
  peeledFields_ = std::move(saver.peeled);
  nullsPruned_ = saver.nullsPruned;
  if (errors_) {
    int32_t errorSize = errors_->size();
    peeledEncoding_->applyToNonNullInnerRows(
        *saver.rows, [&](auto outerRow, auto innerRow) {
          if (innerRow < errorSize && !errors_->isNullAt(innerRow)) {
            addError(
                outerRow,
                *std::static_pointer_cast<std::exception_ptr>(
                    errors_->valueAt(innerRow)),
                saver.errors);
          }
        });
  }
  errors_ = std::move(saver.errors);
  peeledEncoding_ = std::move(saver.peeledEncoding);
  finalSelection_ = saver.finalSelection;
}

namespace {
/// If exceptionPtr represents an std::exception, convert it to VeloxUserError
/// to add useful context for debugging.
std::exception_ptr toVeloxException(const std::exception_ptr& exceptionPtr) {
  try {
    std::rethrow_exception(exceptionPtr);
  } catch (const VeloxException& e) {
    return exceptionPtr;
  } catch (const std::exception& e) {
    return std::make_exception_ptr(
        VeloxUserError(std::current_exception(), e.what(), false));
  }
}

auto throwError(const std::exception_ptr& exceptionPtr) {
  std::rethrow_exception(toVeloxException(exceptionPtr));
}
} // namespace

void EvalCtx::setError(
    vector_size_t index,
    const std::exception_ptr& exceptionPtr) {
  if (throwOnError_) {
    throwError(exceptionPtr);
  }

  addError(index, toVeloxException(exceptionPtr), errors_);
}

void EvalCtx::setErrors(
    const SelectivityVector& rows,
    const std::exception_ptr& exceptionPtr) {
  if (throwOnError_) {
    throwError(exceptionPtr);
  }

  auto veloxException = toVeloxException(exceptionPtr);
  rows.applyToSelected(
      [&](auto row) { addError(row, veloxException, errors_); });
}

void EvalCtx::addElementErrorsToTopLevel(
    const SelectivityVector& elementRows,
    const BufferPtr& elementToTopLevelRows,
    ErrorVectorPtr& topLevelErrors) {
  if (!errors_) {
    return;
  }

  const auto* rawElementToTopLevelRows =
      elementToTopLevelRows->as<vector_size_t>();
  elementRows.applyToSelected([&](auto row) {
    if (errors_->isIndexInRange(row) && !errors_->isNullAt(row)) {
      addError(
          rawElementToTopLevelRows[row],
          *std::static_pointer_cast<std::exception_ptr>(errors_->valueAt(row)),
          topLevelErrors);
    }
  });
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

VectorPtr EvalCtx::ensureFieldLoaded(
    int32_t index,
    const SelectivityVector& rows) {
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

  return field;
}

ScopedFinalSelectionSetter::ScopedFinalSelectionSetter(
    EvalCtx& evalCtx,
    const SelectivityVector* finalSelection,
    bool checkCondition,
    bool override)
    : evalCtx_(evalCtx),
      oldFinalSelection_(*evalCtx.mutableFinalSelection()),
      oldIsFinalSelection_(*evalCtx.mutableIsFinalSelection()) {
  if ((evalCtx.isFinalSelection() && checkCondition) || override) {
    *evalCtx.mutableFinalSelection() = finalSelection;
    *evalCtx.mutableIsFinalSelection() = false;
  }
}

ScopedFinalSelectionSetter::~ScopedFinalSelectionSetter() {
  *evalCtx_.mutableFinalSelection() = oldFinalSelection_;
  *evalCtx_.mutableIsFinalSelection() = oldIsFinalSelection_;
}

VectorEncoding::Simple EvalCtx::wrapEncoding() const {
  return !peeledEncoding_ ? VectorEncoding::Simple::FLAT
                          : peeledEncoding_->getWrapEncoding();
}
} // namespace facebook::velox::exec
