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
    : execCtx_(execCtx),
      exprSet_(exprSet),
      row_(row),
      cacheEnabled_(execCtx->exprEvalCacheEnabled()),
      maxSharedSubexprResultsCached_(
          execCtx->queryCtx()
              ? execCtx->queryCtx()
                    ->queryConfig()
                    .maxSharedSubexprResultsCached()
              : core::QueryConfig({}).maxSharedSubexprResultsCached()) {
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
    : execCtx_(execCtx),
      exprSet_(nullptr),
      row_(nullptr),
      cacheEnabled_(execCtx->exprEvalCacheEnabled()),
      maxSharedSubexprResultsCached_(
          execCtx->queryCtx()
              ? execCtx->queryCtx()
                    ->queryConfig()
                    .maxSharedSubexprResultsCached()
              : core::QueryConfig({}).maxSharedSubexprResultsCached()) {
  VELOX_CHECK_NOT_NULL(execCtx);
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

void EvalCtx::ensureErrorsVectorSize(ErrorVectorPtr& vector, vector_size_t size)
    const {
  auto oldSize = vector ? vector->size() : 0;
  if (!vector) {
    vector = std::make_shared<ErrorVector>(
        pool(),
        OpaqueType::create<void>(),
        AlignedBuffer::allocate<bool>(size, pool(), false) /*nulls*/,
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
    // Set all new positions to null, including the one to be set.
    for (auto i = oldSize; i < size; ++i) {
      vector->setNull(i, true);
    }
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

void EvalCtx::restore(ContextSaver& saver) {
  TestValue::adjust("facebook::velox::exec::EvalCtx::restore", this);

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

// This should be used onlly when exceptionPtr is guranteed to be a
// VeloxException.
void EvalCtx::setVeloxExceptionError(
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
    if (errors_->isIndexInRange(row) && !errors_->isNullAt(row)) {
      bits::setNull(rawNulls, rawElementToTopLevelRows[row], true);
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
      if (result.unique()) {
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
  if (!result.unique() || !result->isNullsWritable()) {
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
