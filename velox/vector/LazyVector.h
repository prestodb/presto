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

#include "velox/common/base/RuntimeMetrics.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SimpleVector.h"

namespace facebook::velox {

// A set of unique, increasing row numbers for use as qualifying
// set. This is logically interchangeable with
// SelectivityVector. Since column reading does not have frequent bit
// operations but always loops over often sparse positions, an array
// of positions is more convenient. folly::Range is also faster to
// pass and access than the indirections and smart pointers that may
// be involved in SelectivityVector.
using RowSet = folly::Range<const vector_size_t*>;

// Defines a per-value callback to apply to values when loading a
// LazyVector. This enables pushing down an arbitrary operation into
// e.g. table scan when loading columns of scalar data types. In this
// way one can bypass copying data into a vector before use.
class ValueHook {
 public:
  // Type and constants for identifying specific hooks. Loaders may
  // have hook-specialized template instantiations for some
  // operations. We do not make a complete enumeration of all hooks in
  // the base class.
  using Kind = int32_t;
  static constexpr Kind kGeneric = 0;
  static constexpr bool kSkipNulls = true;

  virtual ~ValueHook() = default;

  virtual bool acceptsNulls() const {
    return false;
  }

  virtual Kind kind() const {
    return kGeneric;
  }

  virtual void addNull(vector_size_t /*index*/) {}

  virtual void addValue(vector_size_t row, const void* value) = 0;

  // Fallback implementation of bulk path for addValues. Actual
  // hooks are expected o override tis.
  virtual void addValues(
      const vector_size_t* rows,
      const void* values,
      vector_size_t size,
      uint8_t valueWidth) {
    auto valuesAsChar = reinterpret_cast<const char*>(values);
    for (auto i = 0; i < size; ++i) {
      addValue(rows[i], valuesAsChar + valueWidth * i);
    }
  }
};

// Produces values for a LazyVector for a set of positions.
class VectorLoader {
 public:
  virtual ~VectorLoader() = default;

  // Produces the lazy values for 'rows' and if 'hook' is non-nullptr,
  // calls hook on each. If 'hook' is nullptr, sets '*result' to a
  // vector that contains the values for 'rows'. 'rows' must be a
  // subset of the rows that were intended to be loadable when the
  // loader was created. This may be called once in the lifetime of
  // 'this'.
  void load(RowSet rows, ValueHook* hook, VectorPtr* result);

  // Converts 'rows' into a RowSet and calls load(). Provided for
  // convenience in loading LazyVectors in expression evaluation.
  void load(const SelectivityVector& rows, ValueHook* hook, VectorPtr* result);

 protected:
  virtual void
  loadInternal(RowSet rows, ValueHook* hook, VectorPtr* result) = 0;

  virtual void loadInternal(
      const SelectivityVector& rows,
      ValueHook* hook,
      VectorPtr* result);
};

// Simple interface to implement logging runtime stats to Velox operators.
// Inherit a concrete class from this with operator pointer.
class BaseRuntimeStatWriter {
 public:
  virtual ~BaseRuntimeStatWriter() = default;

  virtual void addRuntimeStat(
      const std::string& /* name */,
      const RuntimeCounter& /* value */) {}
};

// Setting a concrete runtime stats writer on the thread will ensure that lazy
// vectors will add time spent on loading data using that writer.
void setRunTimeStatWriter(std::unique_ptr<BaseRuntimeStatWriter>&& ptr);

// Vector class which produces values on first use. This is used for
// loading columns on demand. This allows eliding load of
// columns which have all values filtered out by e.g. joins or which
// are never referenced due to conditionals in projection. If the call
// site known that only a subset of the positions in the vector will
// ever be accessed, loading can be limited to these positions. This
// also allows pushing down computation into loading a column, hence
// bypassing materialization into a vector.
// Unloaded LazyVectors should be referenced only by one top-level vector.
// Otherwise, it runs the risk of being loaded for different set of rows by each
// top-level vector.
class LazyVector : public BaseVector {
 public:
  LazyVector(
      velox::memory::MemoryPool* pool,
      TypePtr type,
      vector_size_t size,
      std::unique_ptr<VectorLoader>&& loader)
      : BaseVector(
            pool,
            std::move(type),
            VectorEncoding::Simple::LAZY,
            BufferPtr(nullptr),
            size),
        loader_(std::move(loader)) {}

  void reset(std::unique_ptr<VectorLoader>&& loader, vector_size_t size) {
    BaseVector::length_ = size;
    loader_ = std::move(loader);
    allLoaded_ = false;
  }

  inline bool isLoaded() const {
    return allLoaded_;
  }

  // Loads the positions in 'rows' into loadedVector_. If 'hook' is
  // non-nullptr, the hook is instead called on the values and
  // loadedVector is not updated. This method is const because call
  // sites often have a const VaseVector. Lazy construction is
  // logically not a mutation.
  void load(RowSet rows, ValueHook* hook) const {
    VELOX_CHECK(!allLoaded_, "A LazyVector can be loaded at most once");
    allLoaded_ = true;
    if (rows.empty()) {
      if (!vector_) {
        vector_ = BaseVector::create(type_, 0, pool_);
      }
      return;
    }
    if (!vector_ && type_->kind() == TypeKind::ROW) {
      vector_ = BaseVector::create(type_, rows.back() + 1, pool_);
    }
    loader_->load(rows, hook, &vector_);
  }

  std::optional<int32_t> compare(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex,
      CompareFlags flags) const override {
    return loadedVector()->compare(other, index, otherIndex, flags);
  }

  uint64_t hashValueAt(vector_size_t index) const override {
    return loadedVector()->hashValueAt(index);
  }

  std::unique_ptr<SimpleVector<uint64_t>> hashAll() const override {
    return loadedVector()->hashAll();
  }

  const BaseVector* loadedVector() const override {
    return loadedVectorShared().get();
  }

  BaseVector* loadedVector() override {
    return loadedVectorShared().get();
  }

  // Returns a shared_ptr to the vector holding the values. If vector is not
  // loaded, loads all the rows, otherwise returns the loaded vector which can
  // have partially loaded rows.
  const VectorPtr& loadedVectorShared() const {
    if (!allLoaded_) {
      if (!vector_) {
        vector_ = BaseVector::create(type_, 0, pool_);
      }
      SelectivityVector allRows(BaseVector::length_);
      loader_->load(allRows, nullptr, &vector_);
      VELOX_CHECK(vector_);
      if (vector_->encoding() == VectorEncoding::Simple::LAZY) {
        vector_ = vector_->asUnchecked<LazyVector>()->loadedVectorShared();
      } else {
        // If the load produced a wrapper, load the wrapped vector.
        vector_->loadedVector();
      }
      allLoaded_ = true;
      const_cast<LazyVector*>(this)->BaseVector::nulls_ = vector_->nulls_;
      if (BaseVector::nulls_) {
        const_cast<LazyVector*>(this)->BaseVector::rawNulls_ =
            BaseVector::nulls_->as<uint64_t>();
      }
    } else {
      VELOX_CHECK(vector_);
    }
    return vector_;
  }

  const BaseVector* wrappedVector() const override {
    return loadedVector()->wrappedVector();
  }

  vector_size_t wrappedIndex(vector_size_t index) const override {
    return loadedVector()->wrappedIndex(index);
  }

  BufferPtr wrapInfo() const override {
    return loadedVector()->wrapInfo();
  }

  bool isScalar() const override {
    return type()->isPrimitiveType() || type()->isOpaque();
  }

  bool mayHaveNulls() const override {
    return loadedVector()->mayHaveNulls();
  }

  bool mayHaveNullsRecursive() const override {
    return loadedVector()->mayHaveNullsRecursive();
  }

  bool isNullAt(vector_size_t index) const override {
    return loadedVector()->isNullAt(index);
  }

  uint64_t retainedSize() const override {
    return isLoaded() ? loadedVector()->retainedSize()
                      : BaseVector::retainedSize();
  }

  /// Returns zero if vector has not been loaded yet.
  uint64_t estimateFlatSize() const override {
    return isLoaded() ? loadedVector()->estimateFlatSize() : 0;
  }

  std::string toString(vector_size_t index) const override {
    return loadedVector()->toString(index);
  }

  VectorPtr slice(vector_size_t offset, vector_size_t length) const override;

  // Loads 'rows' of 'vector'. 'vector' may be an arbitrary wrapping
  // of a LazyVector. 'rows' are translated through the wrappers. If
  // there is no LazyVector inside 'vector', this has no
  // effect. 'vector' may be replaced by a a new vector with 'rows'
  // loaded and the rest as after default construction.
  static void ensureLoadedRows(
      VectorPtr& vector,
      const SelectivityVector& rows);

  // as ensureLoadedRows, above, but takes a scratch DecodedVector and
  // SelectivityVector as arguments to enable reuse.
  static void ensureLoadedRows(
      VectorPtr& vector,
      const SelectivityVector& rows,
      DecodedVector& decoded,
      SelectivityVector& baseRows);

 private:
  std::unique_ptr<VectorLoader> loader_;

  // True if all values are loaded.
  mutable bool allLoaded_ = false;
  // Vector to hold loaded values. This may be present before load for
  // reuse. If loading is with ValueHook, this will not be created.
  mutable VectorPtr vector_;
};

} // namespace facebook::velox
