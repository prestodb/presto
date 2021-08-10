/*
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

#include "velox/core/QueryCtx.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

class Expr;
class EvalCtx;

struct ContextSaver {
  ~ContextSaver();
  // The context to restore. nullptr if nothing to restore.
  EvalCtx* context = nullptr;
  std::vector<VectorPtr> peeled;
  BufferPtr wrap;
  BufferPtr wrapNulls;
  VectorEncoding::Simple wrapEncoding;
  bool mayHaveNulls = false;
  // The selection of the context being saved.
  const SelectivityVector* rows;
  const SelectivityVector* finalSelection;
  FlatVectorPtr<StringView> errors;
};

class ExprSet;

// Context for holding the base row vector, error state and various
// flags for Expr interpreter.
class EvalCtx {
 public:
  EvalCtx(core::ExecCtx* execCtx, ExprSet* exprSet, const RowVector* row);

  const RowVector* row() const {
    return row_;
  }

  memory::MemoryPool* pool() const {
    return execCtx_->pool();
  }

  // Returns the index-th column of the base row. If we have peeled off
  // wrappers like dictionaries, then this provides access only to the
  // peeled off fields.
  VectorPtr getField(int32_t index) const;

  BaseVector* getRawField(int32_t index) const;

  void ensureFieldLoaded(int32_t index, const SelectivityVector& rows);

  void setPeeled(int32_t index, const VectorPtr& vector) {
    if (peeledFields_.size() <= index) {
      peeledFields_.resize(index + 1);
    }
    peeledFields_[index] = vector;
  }

  /// Used by peelEncodings.
  void saveAndReset(ContextSaver* saver, const SelectivityVector& rows);

  void restore(ContextSaver* saver);

  // Creates or updates *result according to 'source'. The
  // 'source' position corresponding to each position in 'rows' is
  // given by the wrap produced by the last peeling in
  // EvalEncoding. If '*result' existed, positions not in 'rows' are
  // not changed.
  void setWrapped(
      Expr* expr,
      VectorPtr source,
      const SelectivityVector& rows,
      VectorPtr* result);

  void setError(vector_size_t index, const std::exception_ptr& exceptionPtr);

  /// Invokes a function on each selected row. Records per-row exceptions by
  /// calling 'setError'. The function must take a single "row" argument of type
  /// vector_size_t and return void.
  template <typename Callable>
  void applyToSelectedNoThrow(const SelectivityVector& rows, Callable func) {
    rows.template applyToSelected([&](auto row) {
      try {
        func(row);
      } catch (const std::exception& e) {
        setError(row, std::current_exception());
      }
    });
  }

  // Sets the error at 'index' in '*errorPtr' if the value is
  // null. Creates and resizes '*errorPtr' as needed and initializes
  // new positions to null.
  void addError(
      vector_size_t index,
      StringView string,
      FlatVectorPtr<StringView>* errorsPtr) const;

  // Returns the vector of errors or nullptr if no errors. This is
  // intentionally a raw pointer to signify that the caller may not
  // retain references to this.
  FlatVector<StringView>* errors() const {
    return errors_.get();
  }

  void swapErrors(FlatVectorPtr<StringView>* other) {
    std::swap(errors_, *other);
  }

  bool* mutableThrowOnError() {
    return &throwOnError_;
  }

  bool mayHaveNulls() const {
    return mayHaveNulls_;
  }

  bool* mutableMayHaveNulls() {
    return &mayHaveNulls_;
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

  const SelectivityVector** mutableFinalSelection() {
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
      VectorPtr* result) const {
    if (*result && !isFinalSelection()) {
      BaseVector::ensureWritable(
          rows, (*result)->type(), (*result)->pool(), result);
      (*result)->copy(localResult.get(), rows, nullptr);
    } else {
      *result = localResult;
    }
  }

 private:
  core::ExecCtx* const execCtx_;
  ExprSet* const exprSet_;
  const RowVector* row_;

  // Corresponds 1:1 to children of 'row_'. Set to an inner vector
  // after removing dictionary/sequence wrappers.
  std::vector<VectorPtr> peeledFields_;
  BufferPtr wrap_;
  BufferPtr wrapNulls_;
  VectorEncoding::Simple wrapEncoding_ = VectorEncoding::Simple::FLAT;
  vector_size_t constantWrapIndex_;

  bool mayHaveNulls_{false};
  bool throwOnError_{true};

  // True if the current set of rows will not grow, e.g. not under and IF or OR.
  bool isFinalSelection_{true};

  // If isFinalSelection_ is false, the set of rows for the upper-most IF or
  // OR. Used to determine the set of rows for loading lazy vectors.
  const SelectivityVector* finalSelection_;

  FlatVectorPtr<StringView> errors_;
};

class LocalSelectivityVector {
 public:
  // Grab an instance of a SelectivityVector from the pool and resize it to
  // specified size.
  LocalSelectivityVector(EvalCtx* context, vector_size_t size)
      : context_(context->execCtx()),
        vector_(context_->getSelectivityVector(size)) {}

  explicit LocalSelectivityVector(EvalCtx* context)
      : context_(context->execCtx()), vector_(nullptr) {}

  LocalSelectivityVector(core::ExecCtx* context, vector_size_t size)
      : context_(context), vector_(context_->getSelectivityVector(size)) {}

  // Grab an instance of a SelectivityVector from the pool and initialize it to
  // the specified value.
  LocalSelectivityVector(EvalCtx* context, const SelectivityVector& value)
      : context_(context->execCtx()),
        vector_(context_->getSelectivityVector(value.size())) {
    *vector_ = value;
  }

  ~LocalSelectivityVector() {
    if (vector_) {
      context_->releaseSelectivityVector(std::move(vector_));
    }
  }

  void allocate(vector_size_t size) {
    if (vector_) {
      context_->releaseSelectivityVector(std::move(vector_));
    }
    vector_ = context_->getSelectivityVector(size);
  }

  explicit operator SelectivityVector&() {
    return *vector_;
  }

  SelectivityVector* get() {
    return vector_.get();
  }

  SelectivityVector* get(vector_size_t size) {
    if (!vector_) {
      vector_ = context_->getSelectivityVector(size);
    }
    return vector_.get();
  }

 private:
  core::ExecCtx* const context_;
  std::unique_ptr<SelectivityVector> vector_ = nullptr;
};

class LocalDecodedVector {
 public:
  explicit LocalDecodedVector(EvalCtx* context)
      : context_(context->execCtx()) {}

  LocalDecodedVector(
      EvalCtx* context,
      const BaseVector& vector,
      const SelectivityVector& rows,
      bool loadLazy = true)
      : context_(context->execCtx()) {
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
      context_->releaseDecodedVector(std::move(vector_));
    }
  }

  DecodedVector* get() {
    if (!vector_) {
      vector_ = context_->getDecodedVector();
    }
    return vector_.get();
  }

  // Must either use the constructor that provides data or call get() first.
  DecodedVector* operator*() const {
    return vector_.get();
  }

  DecodedVector* operator->() const {
    return vector_.get();
  }

 private:
  core::ExecCtx* context_;
  std::unique_ptr<DecodedVector> vector_;
};

} // namespace facebook::velox::exec
