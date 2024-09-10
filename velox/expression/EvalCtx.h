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

#pragma once

#include <exception>
#include <functional>
#include <memory>

#include "velox/common/base/Portability.h"
#include "velox/core/QueryCtx.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

class Expr;
class ExprSet;
class LocalDecodedVector;
class LocalSelectivityVector;
struct ContextSaver;
class PeeledEncoding;

/// Tracks per-row errors that occurred during expression evaluation.
/// Used when EvalCtx::throwOnError() is false.
class EvalErrors {
 public:
  EvalErrors(memory::MemoryPool* pool, vector_size_t size)
      : pool_{pool}, size_{size} {
    VELOX_CHECK_NOT_NULL(pool);
    VELOX_CHECK_GT(size, 0);
    errorFlags_ = AlignedBuffer::allocate<bool>(size, pool, false);
    rawErrorFlags_ = errorFlags_->asMutable<uint64_t>();
  }

  vector_size_t size() const {
    return size_;
  }

  /// Similar to std::vector::reserve. Allocates internal buffers to fit at
  /// least 'size' rows. No-op if 'size()' is already at or exceeding requested.
  void ensureCapacity(vector_size_t size) {
    if (size_ >= size) {
      return;
    }

    AlignedBuffer::reallocate<bool>(&errorFlags_, size, false);
    rawErrorFlags_ = errorFlags_->asMutable<uint64_t>();
    if (exceptions_ != nullptr) {
      AlignedBuffer::reallocate<TError>(&exceptions_, size, TError());
    }

    size_ = size;
  }

  /// Returns true if at least one row has an error.
  bool hasError() const {
    const auto firstErrorRow = bits::findFirstBit(rawErrorFlags_, 0, size_);
    return firstErrorRow >= 0;
  }

  /// Returns true if 'index' has an error.
  bool hasErrorAt(vector_size_t index) const {
    return index < size_ && bits::isBitSet(rawErrorFlags_, index);
  }

  /// Throws if 'index' has an error. The caller must ensure that error details
  /// are available.
  void throwIfErrorAt(vector_size_t index) const {
    auto error = errorAt(index);
    if (error.has_value()) {
      VELOX_CHECK_NOT_NULL(error.value());
      std::rethrow_exception(*error.value());
    }
  }

  /// Finds first row in 'rows' that has an error and throws that error. The
  /// caller must ensure that error details are available.
  void throwFirstError(const SelectivityVector& rows) const {
    rows.testSelected([&](vector_size_t row) {
      if (row < size_) {
        throwIfErrorAt(row);
        return true;
      }
      return false;
    });
  }

  /// Returns std::nullopt if 'index' doesn't have an error.
  /// Returns nullptr if 'index' has an error, but error details are not
  /// available. Returns std::exception_ptr if 'index' has an error and error
  /// details are available.
  std::optional<std::shared_ptr<std::exception_ptr>> errorAt(
      vector_size_t index) const {
    if (!hasErrorAt(index)) {
      return std::nullopt;
    }

    if (exceptions_ == nullptr) {
      return {nullptr};
    }

    return exceptions_->as<TError>()[index];
  }

  /// Bitmask with bits set for rows with errors. Only first 'size()' bits are
  /// valid.
  const uint64_t* errorFlags() const {
    return rawErrorFlags_;
  }

  /// Returns the number of rows with errors.
  vector_size_t countErrors() const {
    return bits::countBits(rawErrorFlags_, 0, size_);
  }

  /// Marks 'index' as having an error. Doesn't specify error details.
  void setError(vector_size_t index) {
    ensureCapacity(index + 1);
    bits::setBit(rawErrorFlags_, index);
  }

  /// Clears error at 'index'.
  void clearError(vector_size_t index) {
    if (index < size_) {
      bits::clearBit(rawErrorFlags_, index);
    }
  }

  /// Marks 'index' as having an error and sets the exception_ptr. No-op if
  /// 'index' is already marked as having an error.
  void setError(vector_size_t index, const std::exception_ptr& exceptionPtr) {
    ensureCapacity(index + 1);
    if (!bits::isBitSet(rawErrorFlags_, index)) {
      bits::setBit(rawErrorFlags_, index);

      if (exceptions_ == nullptr) {
        exceptions_ = AlignedBuffer::allocate<TError>(size_, pool_, TError());
      }
      exceptions_->asMutable<TError>()[index] =
          std::make_shared<std::exception_ptr>(exceptionPtr);
    }
  }

  /// Copies an error from 'from' at index 'fromIndex' to this at index
  /// 'toIndex'. No-op if 'from' at index 'fromIndex' doesn't have an error or
  /// this already has an error at 'toIndex'.
  void copyError(
      const EvalErrors& from,
      vector_size_t fromIndex,
      vector_size_t toIndex) {
    if (from.hasErrorAt(fromIndex)) {
      ensureCapacity(toIndex + 1);
      if (!bits::isBitSet(rawErrorFlags_, toIndex)) {
        bits::setBit(rawErrorFlags_, toIndex);

        if (from.exceptions_ != nullptr) {
          if (exceptions_ == nullptr) {
            exceptions_ =
                AlignedBuffer::allocate<TError>(size_, pool_, TError());
          }

          exceptions_->asMutable<TError>()[toIndex] =
              from.exceptions_->as<TError>()[fromIndex];
        }
      }
    }
  }

  /// Copies errors from 'from' at 'rows' to corresponding rows in this. Doesn't
  /// overwrite existing errors.
  void copyErrors(const SelectivityVector& rows, const EvalErrors& from) {
    const auto fromSize = from.size();
    ensureCapacity(std::min(fromSize, rows.end()));
    rows.testSelected([&](auto row) {
      if (row < fromSize) {
        copyError(from, row, row);
        return true;
      }
      return false;
    });
  }

  /// Copies all errors from 'from' to corresponding rows in this. Doesn't
  /// overwrite existing errors.
  void copyErrors(const EvalErrors& from) {
    ensureCapacity(from.size());
    bits::forEachSetBit(from.errorFlags(), 0, from.size(), [&](auto row) {
      copyError(from, row, row);
    });
  }

 private:
  using TError = std::shared_ptr<std::exception_ptr>;

  memory::MemoryPool* pool_;
  vector_size_t size_;
  BufferPtr errorFlags_;
  uint64_t* rawErrorFlags_;
  BufferPtr exceptions_{nullptr};
};

using EvalErrorsPtr = std::shared_ptr<EvalErrors>;

// Context for holding the base row vector, error state and various
// flags for Expr interpreter.
class EvalCtx {
 public:
  EvalCtx(core::ExecCtx* execCtx, ExprSet* exprSet, const RowVector* row);

  /// For testing only.
  explicit EvalCtx(core::ExecCtx* execCtx);

  const RowVector* row() const {
    return row_;
  }

  /// Returns true if all input vectors in 'row' are flat or constant and have
  /// no nulls.
  bool inputFlatNoNulls() const {
    return inputFlatNoNulls_;
  }

  memory::MemoryPool* pool() const {
    return execCtx_->pool();
  }

  // Returns the index-th column of the base row. If we have peeled off
  // wrappers like dictionaries, then this provides access only to the
  // peeled off fields.
  const VectorPtr& getField(int32_t index) const;

  VectorPtr ensureFieldLoaded(int32_t index, const SelectivityVector& rows);

  void setPeeled(int32_t index, const VectorPtr& vector) {
    if (peeledFields_.size() <= index) {
      peeledFields_.resize(index + 1);
    }
    peeledFields_[index] = vector;
  }

  const std::vector<VectorPtr>& peeledFields() {
    return peeledFields_;
  }

  /// Used by peelEncodings.
  void saveAndReset(ContextSaver& saver, const SelectivityVector& rows);

  void restore(ContextSaver& saver);

  // @param status Must indicate an error. Cannot be "ok".
  void setStatus(vector_size_t index, const Status& status);

  // If exceptionPtr is known to be a VeloxException use setVeloxExceptionError
  // instead.
  void setError(vector_size_t index, const std::exception_ptr& exceptionPtr);

  // Similar to setError but more performant. Should be used when the caller
  // knows for sure that exception_ptr is a VeloxException.
  void setVeloxExceptionError(
      vector_size_t index,
      const std::exception_ptr& exceptionPtr);

  void setErrors(
      const SelectivityVector& rows,
      const std::exception_ptr& exceptionPtr);

  template <typename Callable>
  void applyToSelectedNoThrow(const SelectivityVector& rows, Callable func) {
    applyToSelectedNoThrow(rows, func, [](auto /* row */) INLINE_LAMBDA {});
  }

  /// Invokes a function on each selected row. Records per-row exceptions by
  /// calling 'setError'. The function onErrorFunc is called before 'setError'
  /// when exceptions are thrown. The functions Callable and OnError must take a
  /// single "row" argument of type vector_size_t and return void.
  template <typename Callable, typename OnError>
  void applyToSelectedNoThrow(
      const SelectivityVector& rows,
      Callable func,
      OnError onErrorFunc) {
    rows.template applyToSelected([&](auto row) INLINE_LAMBDA {
      try {
        func(row);
      } catch (const VeloxException& e) {
        if (!e.isUserError()) {
          throw;
        }

        onErrorFunc(row);

        // Avoid double throwing.
        setVeloxExceptionError(row, std::current_exception());
      } catch (const std::exception&) {
        onErrorFunc(row);
        setError(row, std::current_exception());
      }
    });
  }

  // Sets the error at 'index' in '*errorPtr' if the value is
  // null. Creates and resizes '*errorPtr' as needed and initializes
  // new positions to null.
  void addError(
      vector_size_t index,
      const std::exception_ptr& exceptionPtr,
      EvalErrorsPtr& errorsPtr) const;

  /// Copy std::exception_ptr in fromErrors at rows to the corresponding rows in
  /// toErrors. If there are existing exceptions in toErrors, these exceptions
  /// are preserved and those at the corresponding rows in fromErrors are
  /// ignored.
  void addErrors(
      const SelectivityVector& rows,
      const EvalErrorsPtr& fromErrors,
      EvalErrorsPtr& toErrors) const;

  /// Like above, but for a single row.
  void addError(
      vector_size_t row,
      const EvalErrorsPtr& fromErrors,
      EvalErrorsPtr& toErrors) const;

  // Given a mapping from element rows to top-level rows, add element-level
  // errors in errors_ to topLevelErrors.
  void addElementErrorsToTopLevel(
      const SelectivityVector& elementRows,
      const BufferPtr& elementToTopLevelRows,
      EvalErrorsPtr& topLevelErrors);

  // Given a mapping from element rows to top-level rows, set errors in
  // the elements as nulls in the top level row.
  void convertElementErrorsToTopLevelNulls(
      const SelectivityVector& elementRows,
      const BufferPtr& elementToTopLevelRows,
      VectorPtr& result);

  void deselectErrors(SelectivityVector& rows) const;

  /// Returns the vector of errors or nullptr if no errors. This is
  /// intentionally a raw pointer to signify that the caller may not
  /// retain references to this.
  ///
  /// When 'captureErrorDetails' is false, only null flags are being set, the
  /// values are null std::shared_ptr and should not be used.
  EvalErrors* errors() const {
    return errors_.get();
  }

  EvalErrorsPtr* errorsPtr() {
    return &errors_;
  }

  /// Make sure the error vector is addressable up to index `size`-1. Initialize
  /// all
  /// new elements to null.
  void ensureErrorsVectorSize(vector_size_t size) {
    ensureErrorsVectorSize(errors_, size);
  }

  void swapErrors(EvalErrorsPtr& other) {
    std::swap(errors_, other);
  }

  /// Adds errors in 'this' to 'other'. Clears errors from 'this'.
  void moveAppendErrors(EvalErrorsPtr& other);

  /// Boolean indicating whether exceptions that occur during expression
  /// evaluation should be thrown directly or saved for later processing.
  bool throwOnError() const {
    return throwOnError_;
  }

  bool* mutableThrowOnError() {
    return &throwOnError_;
  }

  /// Boolean indicating whether to capture details when storing exceptions for
  /// later processing (throwOnError_ == true).
  ///
  /// Conjunct expressions (AND, OR) require capturing error details, while TRY
  /// and TRY_CAST expressions do not.
  bool captureErrorDetails() const {
    return captureErrorDetails_;
  }

  bool* mutableCaptureErrorDetails() {
    return &captureErrorDetails_;
  }

  bool nullsPruned() const {
    return nullsPruned_;
  }

  bool* mutableNullsPruned() {
    return &nullsPruned_;
  }

  // Returns true if the set of rows the expressions are evaluated on are
  // complete, e.g. we are currently not under an IF where expressions are
  // evaluated only on a subset of rows which either passed the condition
  // ("then" branch) or not ("else" branch).
  bool isFinalSelection() const {
    return isFinalSelection_;
  }

  // True if the operands will not be evaluated on rows outside of the
  // current SelectivityVector. For example, true for top level
  // projections or conjuncts of a top level AND. False for then and
  // else of an IF.
  bool* mutableIsFinalSelection() {
    return &isFinalSelection_;
  }

  const SelectivityVector* FOLLY_NULLABLE* FOLLY_NONNULL
  mutableFinalSelection() {
    return &finalSelection_;
  }

  const SelectivityVector* finalSelection() const {
    return finalSelection_;
  }

  core::ExecCtx* execCtx() const {
    return execCtx_;
  }

  ExprSet* exprSet() const {
    return exprSet_;
  }

  VectorEncoding::Simple wrapEncoding() const;

  void setPeeledEncoding(std::shared_ptr<PeeledEncoding>& peel) {
    peeledEncoding_ = std::move(peel);
  }

  bool resultShouldBePreserved(
      const VectorPtr& result,
      const SelectivityVector& rows) const {
    return result && !isFinalSelection() && *finalSelection() != rows;
  }

  // Copy "rows" of localResult into results if "result" is partially populated
  // and must be preserved. Copy localResult pointer into result otherwise.
  void moveOrCopyResult(
      const VectorPtr& localResult,
      const SelectivityVector& rows,
      VectorPtr& result) const {
#ifndef NDEBUG
    if (localResult != nullptr) {
      // Make sure local/temporary vectors have consistent state.
      localResult->validate();
    }
#endif
    if (resultShouldBePreserved(result, rows)) {
      BaseVector::ensureWritable(rows, result->type(), result->pool(), result);
      result->copy(localResult.get(), rows, nullptr);
    } else {
      result = localResult;
    }
  }

  /// Adds nulls from 'rawNulls' to positions of 'result' given by
  /// 'rows'. Ensures that '*result' is writable, of sufficient size
  /// and that it can take nulls. Makes a new '*result' when
  /// appropriate.
  static void addNulls(
      const SelectivityVector& rows,
      const uint64_t* rawNulls,
      EvalCtx& context,
      const TypePtr& type,
      VectorPtr& result);

  VectorPool* vectorPool() const {
    return execCtx_->vectorPool();
  }

  VectorPtr getVector(const TypePtr& type, vector_size_t size) {
    return execCtx_->getVector(type, size);
  }

  // Return true if the vector was moved to the pool.
  bool releaseVector(VectorPtr& vector) {
    if (!vector) {
      return false;
    }
    return execCtx_->releaseVector(vector);
  }

  size_t releaseVectors(std::vector<VectorPtr>& vectors) {
    return execCtx_->releaseVectors(vectors);
  }

  /// Makes 'result' writable for 'rows'. Allocates or reuses a vector from the
  /// pool of 'execCtx_' if needed.
  void ensureWritable(
      const SelectivityVector& rows,
      const TypePtr& type,
      VectorPtr& result) {
    BaseVector::ensureWritable(
        rows, type, execCtx_->pool(), result, execCtx_->vectorPool());
  }

  PeeledEncoding* getPeeledEncoding() {
    return peeledEncoding_.get();
  }

  /// Returns true if dictionary memoization optimization is enabled, which
  /// allows the reuse of results between consecutive input batches if they are
  /// dictionary encoded and have the same alphabet(undelying flat vector).
  bool dictionaryMemoizationEnabled() const {
    return execCtx_->optimizationParams().dictionaryMemoizationEnabled;
  }

  /// Returns the maximum number of distinct inputs to cache results for in a
  /// given shared subexpression.
  uint32_t maxSharedSubexprResultsCached() const {
    return execCtx_->optimizationParams().maxSharedSubexprResultsCached;
  }

  /// Returns true if peeling is enabled.
  bool peelingEnabled() const {
    return execCtx_->optimizationParams().peelingEnabled;
  }

  /// Returns true if shared subexpression reuse is enabled.
  bool sharedSubExpressionReuseEnabled() const {
    return execCtx_->optimizationParams().sharedSubExpressionReuseEnabled;
  }

  /// Returns true if loading lazy inputs are deferred till they need to be
  /// accessed.
  bool deferredLazyLoadingEnabled() const {
    return execCtx_->optimizationParams().deferredLazyLoadingEnabled;
  }

 private:
  void ensureErrorsVectorSize(EvalErrorsPtr& errors, vector_size_t size) const;

  // Updates 'errorPtr' to clear null at 'index' to indicate an error has
  // occured without specifying error details.
  void addError(vector_size_t index, EvalErrorsPtr& errorsPtr) const;

  // Copy error from 'from' at index 'fromIndex' to 'to' at index 'toIndex'.
  // No-op if 'from' doesn't have an error at 'fromIndex' or if 'to' already has
  // an error at 'toIndex'.
  void copyError(
      const EvalErrors& from,
      vector_size_t fromIndex,
      EvalErrorsPtr& to,
      vector_size_t toIndex) const;

  core::ExecCtx* const execCtx_;
  ExprSet* const exprSet_;
  const RowVector* row_;
  bool inputFlatNoNulls_;

  // Corresponds 1:1 to children of 'row_'. Set to an inner vector
  // after removing dictionary/sequence wrappers.
  std::vector<VectorPtr> peeledFields_;

  // Set if peeling was successful, that is, common encodings from inputs were
  // peeled off.
  std::shared_ptr<PeeledEncoding> peeledEncoding_;

  // True if nulls in the input vectors were pruned (removed from the current
  // selectivity vector). Only possible is all expressions have default null
  // behavior.
  bool nullsPruned_{false};
  bool throwOnError_{true};

  bool captureErrorDetails_{true};

  // True if the current set of rows will not grow, e.g. not under and IF or OR.
  bool isFinalSelection_{true};

  // If isFinalSelection_ is false, the set of rows for the upper-most IF or
  // OR. Used to determine the set of rows for loading lazy vectors.
  const SelectivityVector* finalSelection_;

  // Stores exceptions encountered during expression evaluation.
  // If 'captureErrorDetails()' is false, stores flags indicating which rows had
  // errors without storing actual exceptions.
  EvalErrorsPtr errors_;
};

/// Utility wrapper struct that is used to temporarily reset the value of the
/// EvalCtx. EvalCtx::saveAndReset() is used to achieve that. Use
/// withContextSaver to ensure the original context is restored on a scucessful
/// run or call EvalContext::restore to do it manually.
struct ContextSaver {
  // The context to restore. nullptr if nothing to restore.
  EvalCtx* context = nullptr;
  std::vector<VectorPtr> peeled;
  std::shared_ptr<PeeledEncoding> peeledEncoding;
  bool nullsPruned = false;
  // The selection of the context being saved.
  const SelectivityVector* rows;
  const SelectivityVector* finalSelection;
  EvalErrorsPtr errors;
};

/// Restores the context when the body executes successfully.
template <typename F>
void withContextSaver(F&& f) {
  ContextSaver saver;
  f(saver);
  if (saver.context) {
    saver.context->restore(saver);
  }
}

/// Produces a SelectivityVector with a single row selected using a pool of
/// SelectivityVectors managed by the EvalCtx::execCtx().
class LocalSingleRow {
 public:
  LocalSingleRow(EvalCtx& context, vector_size_t row)
      : context_(*context.execCtx()),
        vector_(context_.getSelectivityVector(row + 1)) {
    vector_->clearAll();
    vector_->setValid(row, true);
    vector_->updateBounds();
  }

  ~LocalSingleRow() {
    context_.releaseSelectivityVector(std::move(vector_));
  }

  SelectivityVector& operator*() {
    return *vector_;
  }

  const SelectivityVector& operator*() const {
    return *vector_;
  }

  SelectivityVector* operator->() {
    return vector_.get();
  }

  const SelectivityVector* operator->() const {
    return vector_.get();
  }

 private:
  core::ExecCtx& context_;
  std::unique_ptr<SelectivityVector> vector_;
};

class LocalSelectivityVector {
 public:
  // Grab an instance of a SelectivityVector from the pool and resize it to
  // specified size.
  LocalSelectivityVector(EvalCtx& context, vector_size_t size)
      : context_(*context.execCtx()),
        vector_(context_.getSelectivityVector(size)) {}

  explicit LocalSelectivityVector(EvalCtx* context, vector_size_t size)
      : LocalSelectivityVector(*context, size) {}

  explicit LocalSelectivityVector(core::ExecCtx& context)
      : context_(context), vector_(nullptr) {}
  explicit LocalSelectivityVector(core::ExecCtx* context)
      : context_(*context), vector_(nullptr) {}

  explicit LocalSelectivityVector(EvalCtx& context)
      : context_(*context.execCtx()), vector_(nullptr) {}

  explicit LocalSelectivityVector(EvalCtx* context)
      : LocalSelectivityVector(*context) {}

  LocalSelectivityVector(core::ExecCtx& context, vector_size_t size)
      : context_(context), vector_(context_.getSelectivityVector(size)) {}

  LocalSelectivityVector(core::ExecCtx* context, vector_size_t size)
      : LocalSelectivityVector(*context, size) {}

  // Grab an instance of a SelectivityVector from the pool and initialize it to
  // the specified value.
  LocalSelectivityVector(EvalCtx& context, const SelectivityVector& value)
      : context_(*context.execCtx()), vector_(context_.getSelectivityVector()) {
    *vector_ = value;
  }

  ~LocalSelectivityVector() {
    if (vector_) {
      context_.releaseSelectivityVector(std::move(vector_));
    }
  }

  void allocate(vector_size_t size) {
    if (vector_) {
      context_.releaseSelectivityVector(std::move(vector_));
    }
    vector_ = context_.getSelectivityVector(size);
  }

  explicit operator SelectivityVector&() {
    return *vector_;
  }

  SelectivityVector* get() {
    return vector_.get();
  }

  SelectivityVector* get(vector_size_t size) {
    if (!vector_) {
      vector_ = context_.getSelectivityVector(size);
    }
    return vector_.get();
  }

  // Returns a recycled SelectivityVector with 'size' bits set to 'value'.
  SelectivityVector* get(vector_size_t size, bool value) {
    if (!vector_) {
      vector_ = context_.getSelectivityVector();
    }
    vector_->resizeFill(size, value);
    return vector_.get();
  }

  // Returns a recycled SelectivityVector initialized from 'other'.
  SelectivityVector* get(const SelectivityVector& other) {
    if (!vector_) {
      vector_ = context_.getSelectivityVector();
    }
    *vector_ = other;
    return vector_.get();
  }

  SelectivityVector& operator*() {
    VELOX_DCHECK_NOT_NULL(vector_, "get(size) must be called.");
    return *vector_;
  }

  const SelectivityVector& operator*() const {
    VELOX_DCHECK_NOT_NULL(vector_, "get(size) must be called.");
    return *vector_;
  }

  SelectivityVector* operator->() {
    VELOX_DCHECK_NOT_NULL(vector_, "get(size) must be called.");
    return vector_.get();
  }

  const SelectivityVector* operator->() const {
    VELOX_DCHECK_NOT_NULL(vector_, "get(size) must be called.");
    return vector_.get();
  }

 private:
  core::ExecCtx& context_;
  std::unique_ptr<SelectivityVector> vector_ = nullptr;
};

class LocalDecodedVector {
 public:
  explicit LocalDecodedVector(core::ExecCtx& context) : context_(context) {}

  explicit LocalDecodedVector(EvalCtx& context)
      : context_(*context.execCtx()) {}

  explicit LocalDecodedVector(EvalCtx* context)
      : LocalDecodedVector(*context) {}

  LocalDecodedVector(
      const EvalCtx& context,
      const BaseVector& vector,
      const SelectivityVector& rows,
      bool loadLazy = true)
      : context_(*context.execCtx()) {
    get()->decode(vector, rows, loadLazy);
  }

  LocalDecodedVector(LocalDecodedVector&& other) noexcept
      : context_{other.context_}, vector_{std::move(other.vector_)} {}

  void operator=(LocalDecodedVector&& other) {
    context_ = other.context_;
    vector_ = std::move(other.vector_);
  }

  ~LocalDecodedVector() {
    if (vector_) {
      context_.get().releaseDecodedVector(std::move(vector_));
    }
  }

  DecodedVector* get() {
    if (!vector_) {
      vector_ = context_.get().getDecodedVector();
    }
    return vector_.get();
  }

  // Must either use the constructor that provides data or call get() first.
  DecodedVector& operator*() {
    VELOX_DCHECK_NOT_NULL(vector_, "get() must be called.");
    return *vector_;
  }

  const DecodedVector& operator*() const {
    VELOX_DCHECK_NOT_NULL(vector_, "get() must be called.");
    return *vector_;
  }

  DecodedVector* operator->() {
    VELOX_DCHECK_NOT_NULL(vector_, "get() must be called.");
    return vector_.get();
  }

  const DecodedVector* operator->() const {
    VELOX_DCHECK_NOT_NULL(vector_, "get() must be called.");
    return vector_.get();
  }

 private:
  std::reference_wrapper<core::ExecCtx> context_;
  std::unique_ptr<DecodedVector> vector_;
};

/// Utility class used to activate final selection (setting isFinalSelection to
/// false and finalSelection to the input 'finalSelection') temporarily till it
/// goes out of scope. It only sets final selection if it has not already been
/// set and 'checkCondition' is true. Additionally, 'override' can be set to
/// true to always set finalSelection even if its already set.
class ScopedFinalSelectionSetter {
 public:
  ScopedFinalSelectionSetter(
      EvalCtx& evalCtx,
      const SelectivityVector* finalSelection,
      bool checkCondition = true,
      bool override = false);
  ~ScopedFinalSelectionSetter();

 private:
  EvalCtx& evalCtx_;
  const SelectivityVector* oldFinalSelection_;
  bool oldIsFinalSelection_;
};

} // namespace facebook::velox::exec
