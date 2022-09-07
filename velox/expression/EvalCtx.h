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

#include <functional>

#include "velox/common/base/Portability.h"
#include "velox/core/QueryCtx.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

class Expr;
class ExprSet;
struct ContextSaver;

// Context for holding the base row vector, error state and various
// flags for Expr interpreter.
class EvalCtx {
 public:
  EvalCtx(
      core::ExecCtx* FOLLY_NONNULL execCtx,
      ExprSet* FOLLY_NULLABLE exprSet,
      const RowVector* FOLLY_NULLABLE row);

  /// For testing only.
  explicit EvalCtx(core::ExecCtx* FOLLY_NONNULL execCtx);

  const RowVector* FOLLY_NONNULL row() const {
    return row_;
  }

  /// Returns true if all input vectors in 'row' are flat or constant and have
  /// no nulls.
  bool inputFlatNoNulls() const {
    return inputFlatNoNulls_;
  }

  memory::MemoryPool* FOLLY_NONNULL pool() const {
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

  /// Used by peelEncodings.
  void saveAndReset(ContextSaver& saver, const SelectivityVector& rows);

  void restore(ContextSaver& saver);

  // Creates or updates *result according to 'source'. The
  // 'source' position corresponding to each position in 'rows' is
  // given by the wrap produced by the last peeling in
  // EvalEncoding. If '*result' existed, positions not in 'rows' are
  // not changed.
  void setWrapped(
      Expr* FOLLY_NONNULL expr,
      VectorPtr source,
      const SelectivityVector& rows,
      VectorPtr& result);

  void setError(vector_size_t index, const std::exception_ptr& exceptionPtr);

  void setErrors(
      const SelectivityVector& rows,
      const std::exception_ptr& exceptionPtr);

  /// Invokes a function on each selected row. Records per-row exceptions by
  /// calling 'setError'. The function must take a single "row" argument of type
  /// vector_size_t and return void.
  template <typename Callable>
  void applyToSelectedNoThrow(const SelectivityVector& rows, Callable func) {
    rows.template applyToSelected([&](auto row) INLINE_LAMBDA {
      try {
        func(row);
      } catch (const std::exception& e) {
        setError(row, std::current_exception());
      }
    });
  }

  // Error vector uses an opaque flat vector to store std::exception_ptr.
  // Since opaque types are stored as shared_ptr<void>, this ends up being a
  // double pointer in the form of std::shared_ptr<std::exception_ptr>. This is
  // fine since we only need to actually follow the pointer in failure cases.
  using ErrorVector = FlatVector<std::shared_ptr<void>>;
  using ErrorVectorPtr = std::shared_ptr<ErrorVector>;

  // Sets the error at 'index' in '*errorPtr' if the value is
  // null. Creates and resizes '*errorPtr' as needed and initializes
  // new positions to null.
  void addError(
      vector_size_t index,
      const std::exception_ptr& exceptionPtr,
      ErrorVectorPtr& errorsPtr) const;

  // Returns the vector of errors or nullptr if no errors. This is
  // intentionally a raw pointer to signify that the caller may not
  // retain references to this.
  ErrorVector* FOLLY_NULLABLE errors() const {
    return errors_.get();
  }

  ErrorVectorPtr* FOLLY_NONNULL errorsPtr() {
    return &errors_;
  }

  void swapErrors(ErrorVectorPtr& other) {
    std::swap(errors_, other);
  }

  bool throwOnError() const {
    return throwOnError_;
  }

  bool* FOLLY_NONNULL mutableThrowOnError() {
    return &throwOnError_;
  }

  bool nullsPruned() const {
    return nullsPruned_;
  }

  bool* FOLLY_NONNULL mutableNullsPruned() {
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
  bool* FOLLY_NONNULL mutableIsFinalSelection() {
    return &isFinalSelection_;
  }

  const SelectivityVector* FOLLY_NULLABLE* FOLLY_NONNULL
  mutableFinalSelection() {
    return &finalSelection_;
  }

  const SelectivityVector* FOLLY_NULLABLE finalSelection() const {
    return finalSelection_;
  }

  core::ExecCtx* FOLLY_NONNULL execCtx() const {
    return execCtx_;
  }

  ExprSet* FOLLY_NULLABLE exprSet() const {
    return exprSet_;
  }

  VectorEncoding::Simple wrapEncoding() const {
    return wrapEncoding_;
  }

  void setConstantWrap(vector_size_t wrapIndex) {
    wrapEncoding_ = VectorEncoding::Simple::CONSTANT;
    constantWrapIndex_ = wrapIndex;
  }

  void setDictionaryWrap(BufferPtr wrap, BufferPtr wrapNulls) {
    wrapEncoding_ = VectorEncoding::Simple::DICTIONARY;
    wrap_ = std::move(wrap);
    wrapNulls_ = std::move(wrapNulls);
  }

  // Copy "rows" of localResult into results if "result" is partially populated
  // and must be preserved. Copy localResult pointer into result otherwise.
  void moveOrCopyResult(
      const VectorPtr& localResult,
      const SelectivityVector& rows,
      VectorPtr& result) const {
    if (result && !isFinalSelection() && *finalSelection() != rows) {
      BaseVector::ensureWritable(rows, result->type(), result->pool(), result);
      result->copy(localResult.get(), rows, nullptr);
    } else {
      result = localResult;
    }
  }

  VectorPool& vectorPool() const {
    return execCtx_->vectorPool();
  }

  VectorPtr getVector(const TypePtr& type, vector_size_t size) {
    return execCtx_->getVector(type, size);
  }

  bool releaseVector(VectorPtr& vector) {
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
        rows, type, execCtx_->pool(), result, &execCtx_->vectorPool());
  }

 private:
  core::ExecCtx* const FOLLY_NONNULL execCtx_;
  ExprSet* FOLLY_NULLABLE const exprSet_;
  const RowVector* FOLLY_NULLABLE row_;
  bool inputFlatNoNulls_;

  // Corresponds 1:1 to children of 'row_'. Set to an inner vector
  // after removing dictionary/sequence wrappers.
  std::vector<VectorPtr> peeledFields_;
  BufferPtr wrap_;
  BufferPtr wrapNulls_;
  VectorEncoding::Simple wrapEncoding_ = VectorEncoding::Simple::FLAT;
  vector_size_t constantWrapIndex_;

  // True if nulls in the input vectors were pruned (removed from the current
  // selectivity vector). Only possible is all expressions have default null
  // behavior.
  bool nullsPruned_{false};
  bool throwOnError_{true};

  // True if the current set of rows will not grow, e.g. not under and IF or OR.
  bool isFinalSelection_{true};

  // If isFinalSelection_ is false, the set of rows for the upper-most IF or
  // OR. Used to determine the set of rows for loading lazy vectors.
  const SelectivityVector* FOLLY_NULLABLE finalSelection_;

  // Stores exception found during expression evaluation. Exceptions are stored
  // in a opaque flat vector, which will translate to a
  // std::shared_ptr<std::exception_ptr>.
  ErrorVectorPtr errors_;
};

struct ContextSaver {
  ~ContextSaver();
  // The context to restore. nullptr if nothing to restore.
  EvalCtx* FOLLY_NULLABLE context = nullptr;
  std::vector<VectorPtr> peeled;
  BufferPtr wrap;
  BufferPtr wrapNulls;
  VectorEncoding::Simple wrapEncoding;
  bool nullsPruned = false;
  // The selection of the context being saved.
  const SelectivityVector* FOLLY_NONNULL rows;
  const SelectivityVector* FOLLY_NULLABLE finalSelection;
  EvalCtx::ErrorVectorPtr errors;
};

class LocalSelectivityVector {
 public:
  // Grab an instance of a SelectivityVector from the pool and resize it to
  // specified size.
  LocalSelectivityVector(EvalCtx& context, vector_size_t size)
      : context_(*context.execCtx()),
        vector_(context_.getSelectivityVector(size)) {}

  explicit LocalSelectivityVector(
      EvalCtx* FOLLY_NONNULL context,
      vector_size_t size)
      : LocalSelectivityVector(*context, size) {}

  explicit LocalSelectivityVector(core::ExecCtx& context)
      : context_(context), vector_(nullptr) {}
  explicit LocalSelectivityVector(core::ExecCtx* FOLLY_NONNULL context)
      : context_(*context), vector_(nullptr) {}

  explicit LocalSelectivityVector(EvalCtx& context)
      : context_(*context.execCtx()), vector_(nullptr) {}

  explicit LocalSelectivityVector(EvalCtx* FOLLY_NONNULL context)
      : LocalSelectivityVector(*context) {}

  LocalSelectivityVector(core::ExecCtx& context, vector_size_t size)
      : context_(context), vector_(context_.getSelectivityVector(size)) {}

  LocalSelectivityVector(
      core::ExecCtx* FOLLY_NONNULL context,
      vector_size_t size)
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

  SelectivityVector* FOLLY_NULLABLE get() {
    return vector_.get();
  }

  SelectivityVector* FOLLY_NONNULL get(vector_size_t size) {
    if (!vector_) {
      vector_ = context_.getSelectivityVector(size);
    }
    return vector_.get();
  }

  // Returns a recycled SelectivityVector with 'size' bits set to 'value'.
  SelectivityVector* FOLLY_NONNULL get(vector_size_t size, bool value) {
    if (!vector_) {
      vector_ = context_.getSelectivityVector();
    }
    vector_->resizeFill(size, value);
    return vector_.get();
  }

  // Returns a recycled SelectivityVector initialized from 'other'.
  SelectivityVector* FOLLY_NONNULL get(const SelectivityVector& other) {
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

  SelectivityVector* FOLLY_NULLABLE operator->() {
    VELOX_DCHECK_NOT_NULL(vector_, "get(size) must be called.");
    return vector_.get();
  }

  const SelectivityVector* FOLLY_NONNULL operator->() const {
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

  explicit LocalDecodedVector(EvalCtx* FOLLY_NONNULL context)
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

  DecodedVector* FOLLY_NONNULL get() {
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

  DecodedVector* FOLLY_NONNULL operator->() {
    VELOX_DCHECK_NOT_NULL(vector_, "get() must be called.");
    return vector_.get();
  }

  const DecodedVector* FOLLY_NONNULL operator->() const {
    VELOX_DCHECK_NOT_NULL(vector_, "get() must be called.");
    return vector_.get();
  }

 private:
  std::reference_wrapper<core::ExecCtx> context_;
  std::unique_ptr<DecodedVector> vector_;
};

} // namespace facebook::velox::exec
