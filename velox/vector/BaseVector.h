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

#include <algorithm>
#include <memory>
#include <string>
#include <utility>

#include <fmt/format.h>
#include <folly/Format.h>
#include <folly/Range.h>
#include <folly/container/F14Map.h>

#include "velox/buffer/Buffer.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/CompareFlags.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Nulls.h"
#include "velox/type/Type.h"
#include "velox/type/Variant.h"
#include "velox/vector/BuilderTypeUtils.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/TypeAliases.h"
#include "velox/vector/VectorEncoding.h"
#include "velox/vector/VectorUtil.h"

namespace facebook {
namespace velox {

template <typename T>
class SimpleVector;

template <typename T>
class FlatVector;

class VectorPool;
class BaseVector;
using VectorPtr = std::shared_ptr<BaseVector>;

// Set of options that validate() accepts.
struct VectorValidateOptions {
  // If set to true then an unloaded lazy vector is loaded and validate is
  // called on the loaded vector. NOTE: loading a vector is non-trivial and
  // can modify how it's handled by downstream code-paths.
  bool loadLazy = false;

  // Any optional checks you want to execute on the vector.
  std::function<void(const BaseVector&)> callback;
};

class DecodedVector;

/**
 * Base class for all columnar-based vectors of any type.
 */
class BaseVector {
 public:
  BaseVector(const BaseVector&) = delete;
  BaseVector& operator=(const BaseVector&) = delete;

  static constexpr uint64_t kNullHash = 1;

  BaseVector(
      velox::memory::MemoryPool* pool,
      TypePtr type,
      VectorEncoding::Simple encoding,
      BufferPtr nulls,
      size_t length,
      std::optional<vector_size_t> distinctValueCount = std::nullopt,
      std::optional<vector_size_t> nullCount = std::nullopt,
      std::optional<ByteCount> representedByteCount = std::nullopt,
      std::optional<ByteCount> storageByteCount = std::nullopt);

  virtual ~BaseVector() = default;

  VectorEncoding::Simple encoding() const {
    return encoding_;
  }

  inline bool isLazy() const {
    return encoding() == VectorEncoding::Simple::LAZY;
  }

  // Returns false if vector has no nulls. Return true if vector may have nulls.
  virtual bool mayHaveNulls() const {
    return rawNulls_;
  }

  // Returns false if this vector and all of its children have no nulls. Returns
  // true if this vector or any of its children may have nulls.
  virtual bool mayHaveNullsRecursive() const {
    return mayHaveNulls();
  }

  inline bool isIndexInRange(vector_size_t index) const {
    // This compiles better than index >= 0 && index < length_.
    return static_cast<uint32_t>(index) < length_;
  }

  template <typename T>
  T* as() {
    static_assert(std::is_base_of_v<BaseVector, T>);
    return dynamic_cast<T*>(this);
  }

  template <typename T>
  const T* as() const {
    static_assert(std::is_base_of_v<BaseVector, T>);
    return dynamic_cast<const T*>(this);
  }

  // Use when the type of 'this' is already known. dynamic_cast() is slow.
  template <typename T>
  T* asUnchecked() {
    static_assert(std::is_base_of_v<BaseVector, T>);
    DCHECK(dynamic_cast<const T*>(this) != nullptr);
    return static_cast<T*>(this);
  }

  template <typename T>
  const T* asUnchecked() const {
    static_assert(std::is_base_of_v<BaseVector, T>);
    DCHECK(dynamic_cast<const T*>(this) != nullptr);
    return static_cast<const T*>(this);
  }

  template <typename T>
  const FlatVector<T>* asFlatVector() const {
    return dynamic_cast<const FlatVector<T>*>(this);
  }

  template <typename T>
  FlatVector<T>* asFlatVector() {
    return dynamic_cast<FlatVector<T>*>(this);
  }

  velox::memory::MemoryPool* pool() const {
    return pool_;
  }

  virtual bool isNullAt(vector_size_t idx) const {
    VELOX_DCHECK_GE(idx, 0, "Index must not be negative");
    VELOX_DCHECK_LT(idx, length_, "Index is too large");
    return rawNulls_ ? bits::isBitNull(rawNulls_, idx) : false;
  }

  /// Returns true if value at specified index is null or contains null.
  /// Primitive type values can be null, but cannot contain nulls. Arrays, maps
  /// and structs can be null and can contains nulls. Non-null array may contain
  /// one or more elements that are null or contain nulls themselves. Non-null
  /// maps may contain one more entry with key or value that's null or contains
  /// null. Non-null struct may contain a field that's null or contains null.
  virtual bool containsNullAt(vector_size_t idx) const = 0;

  std::optional<vector_size_t> getNullCount() const {
    return nullCount_;
  }

  void setNullCount(vector_size_t newNullCount) {
    nullCount_ = newNullCount;
  }

  const TypePtr& type() const {
    return type_;
  }

  /// Changes vector type. The new type can have a different
  /// logical representation while maintaining the same physical type.
  /// Additionally, note that the caller must ensure that this vector is not
  /// shared, i.e. singly-referenced.
  virtual void setType(const TypePtr& type) {
    VELOX_CHECK_NOT_NULL(type);
    VELOX_CHECK(
        type_->kindEquals(type),
        "Cannot change vector type from {} to {}. The old and new types can be different logical types, but the underlying physical types must match.",
        type_,
        type);
    type_ = type;
  }

  TypeKind typeKind() const {
    return typeKind_;
  }

  /**
   * Returns a smart pointer to the null bitmap data for this
   * vector. May hold nullptr if there are no nulls. Not const because
   * some vectors may generate this on first access. For ConstantVector, this
   * method returns a BufferPtr of only size 1. For DictionaryVector, this
   * method returns a BufferPtr for only nulls in the top-level layer.
   */
  const BufferPtr& nulls() const {
    return nulls_;
  }

  // Returns a pointer to the raw null bitmap buffer of this vector. Notice that
  // users should not used this API to access nulls directly of a ConstantVector
  // or DictionaryVector. If the vector is a ConstantVector, rawNulls_ is only
  // of size 1. If the vector is a DictionaryVector, rawNulls_ points to a raw
  // buffer of only nulls in the top-level layer. Nulls of a ConstantVector or
  // DictionaryVector can be accessed through the isNullAt() API or
  // DecodedVector.
  const uint64_t* rawNulls() const {
    return rawNulls_;
  }

  // Ensures that nulls are writable (mutable and single referenced for
  // BaseVector::length_).
  uint64_t* mutableRawNulls() {
    ensureNulls();
    return const_cast<uint64_t*>(rawNulls_);
  }

  BufferPtr& mutableNulls(vector_size_t size) {
    ensureNullsCapacity(size);
    return nulls_;
  }

  std::optional<vector_size_t> getDistinctValueCount() const {
    return distinctValueCount_;
  }

  /**
   * @return the number of rows of data in this vector
   */
  vector_size_t size() const {
    return length_;
  }

  virtual void append(const BaseVector* other) {
    auto totalSize = BaseVector::length_ + other->size();
    auto previousSize = BaseVector::size();
    resize(totalSize);
    copy(other, previousSize, 0, other->size());
  }

  /**
   * @return the number of bytes this vector takes on disk when in a compressed
   * and serialized format
   */
  std::optional<ByteCount> storageBytes() const {
    return storageByteCount_;
  }

  /**
   * @return the number of bytes required to naively represent all of the data
   * in this vector - the raw data size if not in a compressed or otherwise
   * optimized format
   */
  std::optional<ByteCount> representedBytes() const {
    return representedByteCount_;
  }

  /**
   * @return the number of bytes required to hold this vector in memory
   */
  ByteCount inMemoryBytes() const {
    return inMemoryBytes_;
  }

  /**
   * @return true if this vector has the same value at the given index as the
   * other vector at the other vector's index (including if both are null),
   * false otherwise
   * @throws if the type_ of other doesn't match the type_ of this
   */
  virtual bool equalValueAt(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex) const {
    static constexpr CompareFlags kEqualValueAtFlags =
        CompareFlags::equality(CompareFlags::NullHandlingMode::kNullAsValue);

    // Will always have value because nullHandlingMode is NullAsValue.
    return compare(other, index, otherIndex, kEqualValueAtFlags).value() == 0;
  }

  /// Returns true if this vector has the same value at the given index as the
  /// other vector at the other vector's index (including if both are null when
  /// nullHandlingMode is NullAsValue), false otherwise. If nullHandlingMode is
  /// StopAtNull, returns std::nullopt if null encountered.
  virtual std::optional<bool> equalValueAt(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex,
      CompareFlags::NullHandlingMode nullHandlingMode) const;

  int32_t compare(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex) const {
    // Default compare flags always generate value.
    return compare(other, index, otherIndex, CompareFlags()).value();
  }

  /// When CompareFlags is ASCENDING, returns < 0 if 'this' at 'index' is less
  /// than 'other' at 'otherIndex', 0 if equal and > 0 otherwise.
  /// When CompareFlags is DESCENDING, returns < 0 if 'this' at 'index' is
  /// larger than 'other' at 'otherIndex', 0 if equal and < 0 otherwise. If
  /// flags.nullHandlingMode is not NullAsValue, the function may returns
  /// std::nullopt if null encountered.
  virtual std::optional<int32_t> compare(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex,
      CompareFlags flags) const = 0;

  /// Sort values at specified 'indices'. Used to sort map keys.
  virtual void sortIndices(
      std::vector<vector_size_t>& indices,
      CompareFlags flags) const {
    std::sort(
        indices.begin(),
        indices.end(),
        [&](vector_size_t left, vector_size_t right) {
          return compare(this, left, right, flags) < 0;
        });
  }

  /// Sort values at specified 'indices' after applying the 'mapping'. Used to
  /// sort map keys.
  virtual void sortIndices(
      std::vector<vector_size_t>& indices,
      const vector_size_t* mapping,
      CompareFlags flags) const {
    std::sort(
        indices.begin(),
        indices.end(),
        [&](vector_size_t left, vector_size_t right) {
          return compare(this, mapping[left], mapping[right], flags) < 0;
        });
  }

  /// Compares values in range [start, start + size) and returns an index of a
  /// duplicate value if found.
  std::optional<vector_size_t> findDuplicateValue(
      vector_size_t start,
      vector_size_t size,
      CompareFlags flags);

  /**
   * @return the hash of the value at the given index in this vector
   */
  virtual uint64_t hashValueAt(vector_size_t index) const = 0;

  /**
   * @return a new vector that contains the hashes for all entries
   */
  virtual std::unique_ptr<SimpleVector<uint64_t>> hashAll() const = 0;

  /// Returns true if this vector is encoded as flat (FlatVector).
  bool isFlatEncoding() const {
    return encoding_ == VectorEncoding::Simple::FLAT;
  }

  /// Returns true if this vector is encoded as constant (ConstantVector).
  bool isConstantEncoding() const {
    return encoding_ == VectorEncoding::Simple::CONSTANT;
  }

  // Returns true if this vector has a scalar type. If so, values are
  // accessed by valueAt after casting the vector to a type()
  // dependent instantiation of SimpleVector<T>.
  virtual bool isScalar() const {
    return false;
  }

  // Returns the scalar or complex vector wrapped inside any nesting of
  // dictionary, sequence or constant vectors.
  virtual const BaseVector* wrappedVector() const {
    return this;
  }

  static const VectorPtr& wrappedVectorShared(const VectorPtr& vector);

  // Returns the index to apply for 'index' in the vector returned by
  // wrappedVector(). Translates the index over any nesting of
  // dictionaries, sequences and constants.
  virtual vector_size_t wrappedIndex(vector_size_t index) const {
    return index;
  }

  /// Sets the null indicator at 'idx'. This API throws if the vector is a
  /// ConstantVector.
  FOLLY_ALWAYS_INLINE virtual void setNull(vector_size_t idx, bool isNull) {
    VELOX_DCHECK(idx >= 0 && idx < length_);
    if (!nulls_ && !isNull) {
      return;
    }
    ensureNulls();
    bits::setNull(nulls_->asMutable<uint64_t>(), idx, isNull);
  }

  struct CopyRange {
    vector_size_t sourceIndex;
    vector_size_t targetIndex;
    vector_size_t count;
  };

  /// Sets null flags for each row in 'ranges' to 'isNull'.
  static void setNulls(
      uint64_t* rawNulls,
      const folly::Range<const CopyRange*>& ranges,
      bool isNull);

  /// Copies null flags for each row in 'ranges' from 'sourceRawNulls' to
  /// 'targetRawNulls'.
  static void copyNulls(
      uint64_t* targetRawNulls,
      const uint64_t* sourceRawNulls,
      const folly::Range<const CopyRange*>& ranges);

  static int32_t
  countNulls(const BufferPtr& nulls, vector_size_t begin, vector_size_t end) {
    return nulls ? bits::countNulls(nulls->as<uint64_t>(), begin, end) : 0;
  }

  static int32_t countNulls(const BufferPtr& nulls, vector_size_t size) {
    return countNulls(nulls, 0, size);
  }

  // Returns whether or not the nulls buffer can be modified.
  // This does not guarantee the existence of the nulls buffer, if using this
  // within BaseVector you still may need to call ensureNulls.
  virtual bool isNullsWritable() const {
    return !nulls_ || (nulls_->isMutable());
  }

  // Sets null when 'nulls' has a null value for active rows in 'rows'.
  // Is a no-op 'nulls' is a nullptr or 'rows' has no selections. This API
  // throws if the vector is a ConstantVector.
  virtual void addNulls(
      const uint64_t* FOLLY_NULLABLE nulls,
      const SelectivityVector& rows);

  // Sets nulls for all active row in 'nullRows'. Is a no-op if nullRows has no
  // selections. This API throws if the vector is a ConstantVector.
  virtual void addNulls(const SelectivityVector& nullRows);

  // Clears nulls for all active rows in 'nonNullRows'
  virtual void clearNulls(const SelectivityVector& nonNullRows);

  // Clears nulls for all row indices in range [begin, end).
  virtual void clearNulls(vector_size_t begin, vector_size_t end);

  void clearAllNulls() {
    clearNulls(0, size());
  }

  void reuseNulls();

  // Sets the size to 'newSize' and ensures there is space for the
  // indicated number of nulls and top level values (eg. values for Flat,
  // indices for Dictionary, etc). Any immutable buffers that need to be resized
  // are copied. 'setNotNull' indicates if nulls in range [oldSize, newSize]
  // should be set to not null.
  // Note: caller must ensure that the vector is writable; for instance have
  // recursively single referenced buffers and vectors.
  virtual void resize(vector_size_t newSize, bool setNotNull = true);

  // Sets the rows of 'this' given by 'rows' to
  // 'source.valueAt(toSourceRow ? toSourceRow[row] : row)', where
  // 'row' iterates over 'rows'. All active 'row' in 'rows' must map to a valid
  // row in the 'source'.
  virtual void copy(
      const BaseVector* source,
      const SelectivityVector& rows,
      const vector_size_t* toSourceRow);

  // Utility for making a deep copy of a whole vector.
  static VectorPtr copy(const BaseVector& vector) {
    auto result =
        BaseVector::create(vector.type(), vector.size(), vector.pool());
    result->copy(&vector, 0, 0, vector.size());
    return result;
  }

  virtual void copy(
      const BaseVector* source,
      vector_size_t targetIndex,
      vector_size_t sourceIndex,
      vector_size_t count) {
    if (count == 0) {
      return;
    }
    CopyRange range{sourceIndex, targetIndex, count};
    copyRanges(source, folly::Range(&range, 1));
  }

  /// Converts SelectivityVector into a list of CopyRanges having sourceIndex ==
  /// targetIndex. Aims to produce as few ranges as possible. If all rows are
  /// selected, returns a single range.
  static std::vector<CopyRange> toCopyRanges(const SelectivityVector& rows);

  // Copy multiple ranges at once.  This is more efficient than calling `copy`
  // multiple times, especially for ARRAY, MAP, and VARCHAR.
  virtual void copyRanges(
      const BaseVector* /*source*/,
      const folly::Range<const CopyRange*>& /*ranges*/) {
    VELOX_UNSUPPORTED("Can only copy into flat or complex vectors");
  }

  // Construct a zero-copy slice of the vector with the indicated offset and
  // length.
  virtual VectorPtr slice(vector_size_t offset, vector_size_t length) const = 0;

  // Returns a vector of the type of 'source' where 'indices' contains
  // an index into 'source' for each element of 'source'. The
  // resulting vector has position i set to source[i]. This is
  // equivalent to wrapping 'source' in a dictionary with 'indices'
  // but this may reuse structure if said structure is uniquely owned
  // or if a copy is more efficient than dictionary wrapping.
  static VectorPtr transpose(BufferPtr indices, VectorPtr&& source);

  static VectorPtr createConstant(
      const TypePtr& type,
      variant value,
      vector_size_t size,
      velox::memory::MemoryPool* pool);

  static VectorPtr createNullConstant(
      const TypePtr& type,
      vector_size_t size,
      velox::memory::MemoryPool* pool);

  static VectorPtr wrapInDictionary(
      BufferPtr nulls,
      BufferPtr indices,
      vector_size_t size,
      VectorPtr vector);

  static VectorPtr
  wrapInSequence(BufferPtr lengths, vector_size_t size, VectorPtr vector);

  // Creates a ConstantVector of specified length and value coming from the
  // 'index' element of the 'vector'. Peels off any encodings of the 'vector'
  // before making a new ConstantVector. The result vector is either a
  // ConstantVector holding a scalar value or a ConstantVector wrapping flat or
  // lazy vector. The result cannot be a wrapping over another constant or
  // dictionary vector. If copyBase is true and the result vector wraps a
  // vector, the wrapped vector is newly constructed by copying the value from
  // the original, guaranteeing no Vectors are shared with 'vector'.
  static VectorPtr wrapInConstant(
      vector_size_t length,
      vector_size_t index,
      VectorPtr vector,
      bool copyBase = false);

  // Makes 'result' writable for 'rows'. A wrapper (e.g. dictionary, constant,
  // sequence) is flattened and a multiply referenced flat vector is copied.
  // The content of 'rows' is not copied, as these values are intended to be
  // overwritten.
  //
  // After invoking this function, the 'result' is guaranteed to be a flat
  // uniquely-referenced vector with all data-dependent flags reset.
  //
  // Use SelectivityVector::empty() to make the 'result' writable and preserve
  // all current values.
  //
  // If 'result' is a lazy vector, then caller needs to ensure it is unique in
  // order to re-use the loaded vector. Otherwise, a copy would be created.
  static void ensureWritable(
      const SelectivityVector& rows,
      const TypePtr& type,
      velox::memory::MemoryPool* pool,
      VectorPtr& result,
      VectorPool* vectorPool = nullptr);

  virtual void ensureWritable(const SelectivityVector& rows);

  // Returns true if the following conditions hold:
  //  * The vector is singly referenced.
  //  * The vector has a Flat-like encoding (Flat, Array, Map, Row).
  //  * Any child Buffers are mutable  and singly referenced.
  //  * All of these conditions hold for child Vectors recursively.
  // This function is templated rather than taking a std::shared_ptr<BaseVector>
  // because if we were to do that the compiler would allocate a new shared_ptr
  // when this function is called making it not unique.
  template <typename T>
  static bool isVectorWritable(const std::shared_ptr<T>& vector) {
    if (!vector.unique()) {
      return false;
    }

    return vector->isWritable();
  }

  virtual bool isWritable() const {
    return false;
  }

  /// If 'vector' consists of a single value and is longer than one,
  /// returns an equivalent constant vector, else nullptr.
  static VectorPtr constantify(
      const std::shared_ptr<BaseVector>& vector,
      DecodedVector* decoded = nullptr);

  // Flattens the input vector and all of its children.
  static void flattenVector(VectorPtr& vector);

  template <typename T>
  static inline uint64_t byteSize(vector_size_t count) {
    return sizeof(T) * count;
  }

  // If 'vector' is a wrapper, returns the underlying values vector. This is
  // virtual and defined here because we must be able to access this in type
  // agnostic code without a switch on all data types.
  virtual const VectorPtr& valueVector() const {
    VELOX_UNSUPPORTED("Vector is not a wrapper");
  }

  virtual BaseVector* loadedVector() {
    return this;
  }

  virtual const BaseVector* loadedVector() const {
    return this;
  }

  static const VectorPtr& loadedVectorShared(const VectorPtr& vector);

  virtual const BufferPtr& values() const {
    VELOX_UNSUPPORTED("Only flat vectors have a values buffer");
  }

  virtual const void* valuesAsVoid() const {
    VELOX_UNSUPPORTED("Only flat vectors have a values buffer");
  }

  // If 'this' is a wrapper, returns the wrap info, interpretation depends on
  // encoding.
  virtual BufferPtr wrapInfo() const {
    throw std::runtime_error("Vector is not a wrapper");
  }

  template <typename T = BaseVector>
  static std::shared_ptr<T> create(
      const TypePtr& type,
      vector_size_t size,
      velox::memory::MemoryPool* pool) {
    return std::static_pointer_cast<T>(createInternal(type, size, pool));
  }

  static VectorPtr getOrCreateEmpty(
      VectorPtr vector,
      const TypePtr& type,
      velox::memory::MemoryPool* pool) {
    return vector ? vector : create(type, 0, pool);
  }

  // Set 'nulls' to be the nulls buffer of this vector. This API should not be
  // used on ConstantVector.
  void setNulls(const BufferPtr& nulls);

  // Reset the nulls buffer of this vector to be empty. This API should not be
  // used on ConstantVector.
  void resetNulls() {
    setNulls(nullptr);
  }

  /// Ensures that 'indices' is singly-referenced and has space for 'newSize'
  /// elements. Sets elements between the 'currentSize' and 'newSize' to 0 if
  /// 'newSize' > 'currentSize'.
  ///
  /// If 'indices' is nullptr, read-only, not uniquely-referenced, or doesn't
  /// have capacity for 'newSize' elements allocates new buffer and copies data
  /// to it. Updates '*rawIndices' to point to the start of 'indices' buffer.
  static void resizeIndices(
      vector_size_t currentSize,
      vector_size_t newSize,
      velox::memory::MemoryPool* pool,
      BufferPtr& indices,
      const vector_size_t** rawIndices);

  // Makes sure '*buffer' has space for 'size' items of T and is writable. Sets
  // 'raw' to point to the writable contents of '*buffer'.
  template <typename T, typename RawT>
  static void ensureBuffer(
      vector_size_t size,
      velox::memory::MemoryPool* pool,
      BufferPtr* buffer,
      RawT** raw) {
    vector_size_t minBytes = byteSize<T>(size);
    if (*buffer && (*buffer)->capacity() >= minBytes && (*buffer)->unique()) {
      (*buffer)->setSize(minBytes);
      if (raw) {
        *raw = (*buffer)->asMutable<RawT>();
      }
      return;
    }
    if (*buffer) {
      AlignedBuffer::reallocate<T>(buffer, size);
    } else {
      *buffer = AlignedBuffer::allocate<T>(size, pool);
    }
    if (raw) {
      *raw = (*buffer)->asMutable<RawT>();
    }
    (*buffer)->setSize(minBytes);
  }

  // Returns the byte size of memory that is kept live through 'this'.
  virtual uint64_t retainedSize() const {
    return nulls_ ? nulls_->capacity() : 0;
  }

  /// Returns an estimate of the 'retainedSize' of a flat representation of the
  /// data stored in this vector. Returns zero if this is a lazy vector that
  /// hasn't been loaded yet.
  virtual uint64_t estimateFlatSize() const;

  /// To safely reuse a vector one needs to (1) ensure that the vector as well
  /// as all its buffers and child vectors are singly-referenced and mutable
  /// (for buffers); (2) clear append-only string buffers and child vectors
  /// (elements of arrays, keys and values of maps, fields of structs); (3)
  /// reset all data-dependent flags.
  ///
  /// This method takes a non-const reference to a 'vector' and updates it to
  /// possibly a new flat vector of the specified size that is safe to reuse.
  /// If input 'vector' is not singly-referenced or not flat, replaces 'vector'
  /// with a new vector of the same type and specified size. If some of the
  /// buffers cannot be reused, these buffers are reset. Child vectors are
  /// updated by calling this method recursively with size zero. Data-dependent
  /// flags are reset after this call.
  static void prepareForReuse(VectorPtr& vector, vector_size_t size);

  /// Resets non-reusable buffers and updates child vectors by calling
  /// BaseVector::prepareForReuse.
  /// Base implementation checks and resets nulls buffer if needed. Keeps the
  /// nulls buffer if singly-referenced, mutable and has at least one null bit
  /// set.
  virtual void prepareForReuse();

  /// Returns a brief summary of the vector. If 'recursive' is true, includes a
  /// summary of all the layers of encodings starting with the top layer.
  ///
  /// For example,
  ///     with recursive 'false':
  ///
  ///         [DICTIONARY INTEGER: 5 elements, no nulls]
  ///
  ///     with recursive 'true':
  ///
  ///         [DICTIONARY INTEGER: 5 elements, no nulls], [FLAT INTEGER: 10
  ///             elements, no nulls]
  std::string toString(bool recursive) const;

  /// Same as toString(false). Provided to allow for easy invocation from LLDB.
  std::string toString() const {
    return toString(false);
  }

  /// Returns string representation of the value in the specified row.
  virtual std::string toString(vector_size_t index) const;

  /// Returns a list of values in rows [from, to).
  ///
  /// Automatically adjusts 'from' and 'to' to a range of valid indices. Returns
  /// empty string if 'from' is greater than or equal to vector size or 'to' is
  /// less than or equal to zero. Returns values up to the end of the vector if
  /// 'to' is greater than vector size. Returns values from the start of the
  /// vector if 'from' is negative.
  ///
  /// The type of the 'delimiter' is a const char* and not an std::string to
  /// allow for invoking this method from LLDB.
  std::string toString(
      vector_size_t from,
      vector_size_t to,
      const char* delimiter,
      bool includeRowNumbers = true) const;

  /// Returns a list of values in rows [from, to). Values are separated by a new
  /// line and prefixed with a row number.
  ///
  /// This method is provided to allow to easy invocation from LLDB.
  std::string toString(vector_size_t from, vector_size_t to) const {
    return toString(from, to, "\n");
  }

  /// Marks the vector as containing or being a lazy vector and being wrapped.
  /// Should only be used if 'this' is lazy or has a nested lazy vector.
  /// Returns true if this is the first time it was wrapped, else returns false.
  bool markAsContainingLazyAndWrapped() {
    if (containsLazyAndIsWrapped_) {
      return false;
    }
    containsLazyAndIsWrapped_ = true;
    return true;
  }

  void clearContainingLazyAndWrapped() {
    containsLazyAndIsWrapped_ = false;
  }

  bool memoDisabled() const {
    return memoDisabled_;
  }

  void disableMemo() {
    memoDisabled_ = true;
  }

  /// Used to check internal state of a vector like sizes of the buffers,
  /// enclosed child vectors, values in indices. Currently, its only used in
  /// debug builds to check the result of expressions and some interim results.
  virtual void validate(const VectorValidateOptions& options = {}) const;

  FOLLY_ALWAYS_INLINE static std::optional<int32_t>
  compareNulls(bool thisNull, bool otherNull, CompareFlags flags) {
    DCHECK(thisNull || otherNull);
    switch (flags.nullHandlingMode) {
      case CompareFlags::NullHandlingMode::kNullAsIndeterminate:
        if (flags.equalsOnly) {
          return kIndeterminate;
        } else {
          VELOX_USER_FAIL("Ordering nulls is not supported");
        }
      case CompareFlags::NullHandlingMode::kNullAsValue:
        if (thisNull && otherNull) {
          return 0;
        }

        if (flags.nullsFirst) {
          return thisNull ? -1 : 1;
        }
        return thisNull ? 1 : -1;
    }

    VELOX_UNREACHABLE(
        "The function should be called only if one of the inputs is null");
  }

  // Reset data-dependent flags to the "unknown" status. This is needed whenever
  // a vector is mutated because the modification may invalidate these flags.
  // Currently, we call this function in BaseVector::ensureWritable() and
  // BaseVector::prepareForReuse() that are expected to be called before any
  // vector mutation.
  //
  // Per-vector flags are reset to default values. Per-row flags are reset only
  // at the selected rows. If rows is a nullptr, per-row flags are reset at all
  // rows.
  virtual void resetDataDependentFlags(const SelectivityVector* /*rows*/) {
    nullCount_ = std::nullopt;
    distinctValueCount_ = std::nullopt;
    representedByteCount_ = std::nullopt;
    storageByteCount_ = std::nullopt;
  }

 protected:
  /// Returns a brief summary of the vector. The default implementation includes
  /// encoding, type, number of rows and number of nulls.
  ///
  /// For example,
  ///     [FLAT INTEGER: 3 elements, no nulls]
  ///     [DICTIONARY INTEGER: 5 elements, 1 nulls]
  virtual std::string toSummaryString() const;

  /*
   * Allocates or reallocates nulls_ with at least the given size if nulls_
   * hasn't been allocated yet or has been allocated with a smaller capacity.
   * Ensures that nulls are writable (mutable and single referenced for
   * minimumSize).
   */
  void ensureNullsCapacity(vector_size_t minimumSize, bool setNotNull = false);

  void ensureNulls() {
    ensureNullsCapacity(length_, true);
  }

  // Slice a buffer with specific type.
  //
  // For boolean type and if the offset is not multiple of 8, return a shifted
  // copy; otherwise return a BufferView into the original buffer (with shared
  // ownership of original buffer).
  static BufferPtr sliceBuffer(
      const Type&,
      const BufferPtr&,
      vector_size_t offset,
      vector_size_t length,
      memory::MemoryPool*);

  BufferPtr sliceNulls(vector_size_t offset, vector_size_t length) const {
    return sliceBuffer(*BOOLEAN(), nulls_, offset, length, pool_);
  }

  TypePtr type_;
  const TypeKind typeKind_;
  const VectorEncoding::Simple encoding_;
  BufferPtr nulls_;
  // Caches raw pointer to 'nulls->as<uint64_t>().
  const uint64_t* rawNulls_ = nullptr;
  velox::memory::MemoryPool* pool_;
  tsan_atomic<vector_size_t> length_{0};

  /**
   * Holds the number of nulls in the vector. If the number of nulls
   * is not available, it is set to std::nullopt. Setting the value to
   * zero does have implications (SIMD operations need null count to be
   * zero) and is not the same as std::nullopt.
   */
  std::optional<vector_size_t> nullCount_;
  std::optional<vector_size_t> distinctValueCount_;
  std::optional<ByteCount> representedByteCount_;
  std::optional<ByteCount> storageByteCount_;
  ByteCount inMemoryBytes_ = 0;

 private:
  static VectorPtr createInternal(
      const TypePtr& type,
      vector_size_t size,
      velox::memory::MemoryPool* pool);

  friend class LazyVector;

  /// Is true if this vector is a lazy vector or contains one and is being
  /// wrapped. Keeping track of this helps to enforce the invariant that an
  /// unloaded lazy vector should not be wrapped by two separate top level
  /// vectors. This would ensure we avoid it being loaded for two separate set
  /// of rows.
  bool containsLazyAndIsWrapped_{false};

  // Whether we should use Expr::evalWithMemo to cache the result of evaluation
  // on dictionary values (this vector).  Set to false when the dictionary
  // values are not going to be reused (e.g. result of filtering), so that we
  // don't need to reallocate the result for every batch.
  bool memoDisabled_{false};
};

/// Loops over rows in 'ranges' and invokes 'func' for each row.
/// @param TFunc A void function taking two arguments: targetIndex and
/// sourceIndex.
template <typename TFunc>
void applyToEachRow(
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    const TFunc& func) {
  for (const auto& range : ranges) {
    for (auto i = 0; i < range.count; ++i) {
      func(range.targetIndex + i, range.sourceIndex + i);
    }
  }
}

/// Loops over 'ranges' and invokes 'func' for each range.
/// @param TFunc A void function taking 3 arguments: targetIndex, sourceIndex
/// and count.
template <typename TFunc>
void applyToEachRange(
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    const TFunc& func) {
  for (const auto& range : ranges) {
    func(range.targetIndex, range.sourceIndex, range.count);
  }
}

template <>
uint64_t BaseVector::byteSize<bool>(vector_size_t count);

template <>
inline uint64_t BaseVector::byteSize<UnknownValue>(vector_size_t) {
  return 0;
}

// Returns true if vector is a Lazy vector, possibly wrapped, that hasn't
// been loaded yet.
bool isLazyNotLoaded(const BaseVector& vector);

// Allocates a buffer to fit at least 'size' indices and initializes them to
// zero.
inline BufferPtr allocateIndices(vector_size_t size, memory::MemoryPool* pool) {
  return AlignedBuffer::allocate<vector_size_t>(size, pool, 0);
}

// Allocates a buffer to fit at least 'size' null bits and initializes them to
// the provided 'initValue' which has a default value of non-null.
inline BufferPtr allocateNulls(
    vector_size_t size,
    memory::MemoryPool* pool,
    bool initValue = bits::kNotNull) {
  return AlignedBuffer::allocate<bool>(size, pool, initValue);
}

// Returns a summary of the null bits in the specified buffer and prints out
// first 'maxBitsToPrint' bits. Automatically adjusts if 'maxBitsToPrint' is
// greater than total number of bits available.
// For example: 3 out of 8 rows are null: .nn.n...
std::string printNulls(
    const BufferPtr& nulls,
    vector_size_t maxBitsToPrint = 30);

// Returns a summary of the indices buffer and prints out first
// 'maxIndicesToPrint' indices. Automatically adjusts if 'maxIndicesToPrint' is
// greater than total number of indices available.
// For example: 5 unique indices out of 6: 34, 79, 11, 0, 0, 33.
std::string printIndices(
    const BufferPtr& indices,
    vector_size_t maxIndicesToPrint = 10);

} // namespace velox
} // namespace facebook

namespace folly {

// Allow VectorEncoding::Simple to be transparently used by folly::sformat.
// e.g: folly::sformat("type: {}", encodingType);
template <>
class FormatValue<facebook::velox::VectorEncoding::Simple> {
 public:
  explicit FormatValue(const facebook::velox::VectorEncoding::Simple& type)
      : type_(type) {}

  template <typename FormatCallback>
  void format(FormatArg& arg, FormatCallback& cb) const {
    return format_value::formatString(
        facebook::velox::VectorEncoding::mapSimpleToName(type_), arg, cb);
  }

 private:
  facebook::velox::VectorEncoding::Simple type_;
};

} // namespace folly

template <>
struct fmt::formatter<facebook::velox::VectorEncoding::Simple> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(
      const facebook::velox::VectorEncoding::Simple& x,
      FormatContext& ctx) const {
    return format_to(
        ctx.out(), "{}", facebook::velox::VectorEncoding::mapSimpleToName(x));
  }
};
