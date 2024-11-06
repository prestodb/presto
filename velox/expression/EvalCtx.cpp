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
#include "velox/common/testutil/TestValue.h"
#include "velox/core/QueryConfig.h"
#include "velox/expression/Expr.h"
#include "velox/expression/PeeledEncoding.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {

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
  inputFlatNoNulls_ = false;
}

void EvalCtx::saveAndReset(ContextSaver& saver, const SelectivityVector& rows) {
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

void EvalCtx::ensureErrorsVectorSize(EvalErrorsPtr& vector, vector_size_t size)
    const {
  if (!vector) {
    vector = std::make_shared<EvalErrors>(pool(), size);
  } else {
    vector->ensureCapacity(size);
  }
}

void EvalCtx::addError(vector_size_t index, EvalErrorsPtr& errorsPtr) const {
  ensureErrorsVectorSize(errorsPtr, index + 1);
  errorsPtr->setError(index);
}

void EvalCtx::addError(
    vector_size_t index,
    const std::exception_ptr& exceptionPtr,
    EvalErrorsPtr& errorsPtr) const {
  ensureErrorsVectorSize(errorsPtr, index + 1);
  errorsPtr->setError(index, exceptionPtr);
}

void EvalCtx::addErrors(
    const SelectivityVector& rows,
    const EvalErrorsPtr& fromErrors,
    EvalErrorsPtr& toErrors) const {
  if (!fromErrors) {
    return;
  }

  ensureErrorsVectorSize(toErrors, fromErrors->size());
  toErrors->copyErrors(rows, *fromErrors);
}

void EvalCtx::addError(
    vector_size_t row,
    const EvalErrorsPtr& fromErrors,
    EvalErrorsPtr& toErrors) const {
  if (fromErrors != nullptr) {
    copyError(*fromErrors, row, toErrors, row);
  }
}

void EvalCtx::copyError(
    const EvalErrors& from,
    vector_size_t fromIndex,
    EvalErrorsPtr& to,
    vector_size_t toIndex) const {
  if (from.hasErrorAt(fromIndex)) {
    ensureErrorsVectorSize(to, toIndex + 1);
    to->copyError(from, fromIndex, toIndex);
  }
}

void EvalCtx::deselectErrors(SelectivityVector& rows) const {
  if (!errors_) {
    return;
  }
  // A non-null in errors resets the row. AND with the errors null mask.
  rows.deselectNonNulls(
      errors_->errorFlags(),
      rows.begin(),
      std::min(errors_->size(), rows.end()));
}

void EvalCtx::restore(ContextSaver& saver) {
  TestValue::adjust("facebook::velox::exec::EvalCtx::restore", this);

  peeledFields_ = std::move(saver.peeled);
  nullsPruned_ = saver.nullsPruned;
  if (errors_) {
    peeledEncoding_->applyToNonNullInnerRows(
        *saver.rows, [&](auto outerRow, auto innerRow) {
          copyError(*errors_, innerRow, saver.errors, outerRow);
        });
  }
  errors_ = std::move(saver.errors);
  peeledEncoding_ = std::move(saver.peeledEncoding);
  finalSelection_ = saver.finalSelection;
}

namespace {
auto throwError(const std::exception_ptr& exceptionPtr) {
  std::rethrow_exception(toVeloxException(exceptionPtr));
}

std::exception_ptr toVeloxUserError(const std::string& message) {
  return std::make_exception_ptr(VeloxUserError(
      __FILE__,
      __LINE__,
      __FUNCTION__,
      "",
      message,
      error_source::kErrorSourceUser,
      error_code::kInvalidArgument,
      false /*retriable*/));
}

} // namespace

void EvalCtx::setStatus(vector_size_t index, const Status& status) {
  VELOX_CHECK(!status.ok(), "Status must be an error");

  if (status.isUserError()) {
    if (throwOnError_) {
      VELOX_USER_FAIL(status.message());
    }
    if (captureErrorDetails_) {
      addError(index, toVeloxUserError(status.message()), errors_);
    } else {
      addError(index, errors_);
    }
  } else {
    VELOX_FAIL(status.message());
  }
}

void EvalCtx::setError(
    vector_size_t index,
    const std::exception_ptr& exceptionPtr) {
  if (throwOnError_) {
    throwError(exceptionPtr);
  }

  if (captureErrorDetails_) {
    addError(index, toVeloxException(exceptionPtr), errors_);
  } else {
    addError(index, errors_);
  }
}

void EvalCtx::setVeloxExceptionError(
    vector_size_t index,
    const std::exception_ptr& exceptionPtr) {
  if (throwOnError_) {
    std::rethrow_exception(exceptionPtr);
  }

  if (captureErrorDetails_) {
    addError(index, exceptionPtr, errors_);
  } else {
    addError(index, errors_);
  }
}

void EvalCtx::setErrors(
    const SelectivityVector& rows,
    const std::exception_ptr& exceptionPtr) {
  if (throwOnError_) {
    throwError(exceptionPtr);
  }

  if (captureErrorDetails_) {
    auto veloxException = toVeloxException(exceptionPtr);
    rows.applyToSelected(
        [&](auto row) { addError(row, veloxException, errors_); });
  } else {
    rows.applyToSelected([&](auto row) { addError(row, errors_); });
  }
}

void EvalCtx::addElementErrorsToTopLevel(
    const SelectivityVector& elementRows,
    const BufferPtr& elementToTopLevelRows,
    EvalErrorsPtr& topLevelErrors) {
  if (!errors_) {
    return;
  }

  const auto* rawElementToTopLevelRows =
      elementToTopLevelRows->as<vector_size_t>();
  elementRows.applyToSelected([&](auto row) {
    copyError(*errors_, row, topLevelErrors, rawElementToTopLevelRows[row]);
  });
}

void EvalCtx::convertElementErrorsToTopLevelNulls(
    const SelectivityVector& elementRows,
    const BufferPtr& elementToTopLevelRows,
    VectorPtr& result) {
  if (!errors_) {
    return;
  }

  auto rawNulls = result->mutableRawNulls();

  const auto* rawElementToTopLevelRows =
      elementToTopLevelRows->as<vector_size_t>();
  elementRows.applyToSelected([&](auto row) {
    if (errors_->hasErrorAt(row)) {
      bits::setNull(rawNulls, rawElementToTopLevelRows[row], true);
    }
  });
}

void EvalCtx::moveAppendErrors(EvalErrorsPtr& other) {
  if (!errors_) {
    return;
  }

  if (!other) {
    std::swap(errors_, other);
    return;
  }

  ensureErrorsVectorSize(other, errors_->size());
  other->copyErrors(*errors_);

  errors_.reset();
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

// Utility function used to extend the size of a complex Vector by wrapping it
// in a dictionary with identity mapping for existing rows.
// Note: If targetSize < the current size of the vector, then the result will
// have the same size as the input vector.
VectorPtr extendSizeByWrappingInDictionary(
    VectorPtr& vector,
    vector_size_t targetSize,
    EvalCtx& context) {
  auto currentSize = vector->size();
  targetSize = std::max(targetSize, currentSize);
  VELOX_DCHECK(
      !vector->type()->isPrimitiveType(), "Only used for complex types.");
  BufferPtr indices = allocateIndices(targetSize, context.pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  // Only fill in indices for existing rows in the vector.
  std::iota(rawIndices, rawIndices + currentSize, 0);
  // A nulls buffer is required otherwise wrapInDictionary() can return a
  // constant. Moreover, nulls will eventually be added, so it's not wasteful.
  auto nulls = allocateNulls(targetSize, context.pool());
  return BaseVector::wrapInDictionary(
      std::move(nulls), std::move(indices), targetSize, std::move(vector));
}

// Utility function used to resize a primitive type vector while ensuring that
// the result has all unique and writable buffers.
void resizePrimitiveTypeVectors(
    VectorPtr& vector,
    vector_size_t targetSize,
    EvalCtx& context) {
  VELOX_DCHECK(
      vector->type()->isPrimitiveType(), "Only used for primitive types.");
  auto currentSize = vector->size();
  LocalSelectivityVector extraRows(context, targetSize);
  extraRows->setValidRange(0, currentSize, false);
  extraRows->setValidRange(currentSize, targetSize, true);
  extraRows->updateBounds();
  BaseVector::ensureWritable(
      *extraRows, vector->type(), context.pool(), vector);
}

// static
void EvalCtx::addNulls(
    const SelectivityVector& rows,
    const uint64_t* rawNulls,
    EvalCtx& context,
    const TypePtr& type,
    VectorPtr& result) {
  // If there's no `result` yet, return a NULL ContantVector.
  if (!result) {
    result = BaseVector::createNullConstant(type, rows.end(), context.pool());
    return;
  }

  // If result is already a NULL ConstantVector, resize the vector if necessary,
  // or do nothing otherwise.
  if (result->isConstantEncoding() && result->isNullAt(0)) {
    if (result->size() < rows.end()) {
      if (result.use_count() == 1) {
        result->resize(rows.end());
      } else {
        result =
            BaseVector::createNullConstant(type, rows.end(), context.pool());
      }
    }
    return;
  }

  auto currentSize = result->size();
  auto targetSize = rows.end();
  if (result.use_count() != 1 || !result->isNullsWritable()) {
    if (result->type()->isPrimitiveType()) {
      if (currentSize < targetSize) {
        resizePrimitiveTypeVectors(result, targetSize, context);
      } else {
        BaseVector::ensureWritable(
            SelectivityVector::empty(), type, context.pool(), result);
      }
    } else {
      result = extendSizeByWrappingInDictionary(result, targetSize, context);
    }
  } else if (currentSize < targetSize) {
    VELOX_DCHECK(
        !result->isConstantEncoding(),
        "Should have been handled in code-path for !isNullsWritable()");
    if (VectorEncoding::isDictionary(result->encoding())) {
      // We can just resize the dictionary layer in-place. It also ensures
      // indices buffer is unique after resize.
      result->resize(targetSize);
    } else {
      if (result->type()->isPrimitiveType()) {
        // A flat vector can still have a shared values_ buffer so we ensure all
        // its buffers are unique while resizing.
        resizePrimitiveTypeVectors(result, targetSize, context);
      } else {
        result = extendSizeByWrappingInDictionary(result, targetSize, context);
      }
    }
  }

  result->addNulls(rawNulls, rows);
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
                          : peeledEncoding_->wrapEncoding();
}
} // namespace facebook::velox::exec
