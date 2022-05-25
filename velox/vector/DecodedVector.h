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

#include "velox/common/base/Exceptions.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/SelectivityVector.h"

#include <vector>

namespace facebook::velox {

// Canonicalizes an arbitrary vector to its base array (or vector, if
// complex type) and a list of indices into this, so that the indices
// cover the positions in a SelectivityVector. Nulls are represented
// via the null bitmap of the leaf vector.
class DecodedVector {
 public:
  DecodedVector() = default;
  DecodedVector(const DecodedVector& other) = delete;
  DecodedVector& operator=(const DecodedVector& other) = delete;

  // DecodedVector is used in VectorExec and needs to be move-constructed.
  DecodedVector(DecodedVector&& other) = default;

  // Loads the underlying lazy vector if not already loaded unless loadLazy is
  // false.
  //
  // loadLazy = false is used in HashAggregation to implement push-down of
  // aggregation into table scan. In this case, DecodedVector is used to combine
  // possibly multiple levels of wrappings into just one and then load
  // LazyVector only for the necessary rows using ValueHook which adds values to
  // aggregation accumulators directly.
  //
  // Limitations: Decoding constant vector wrapping a lazy vector that has not
  // been loaded yet with loadLazy = false is currently not supported.
  DecodedVector(
      const BaseVector& vector,
      const SelectivityVector& rows,
      bool loadLazy = true) {
    decode(vector, rows, loadLazy);
  }

  void decode(
      const BaseVector& vector,
      const SelectivityVector& rows,
      bool loadLazy = true);

  void makeIndices(
      const BaseVector& vector,
      const SelectivityVector& rows,
      int32_t numLevels);

  template <typename T>
  const T* data() const {
    return reinterpret_cast<const T*>(data_);
  }

  const uint64_t* nulls() const {
    return nulls_;
  }

  const vector_size_t* indices() {
    if (!indices_) {
      fillInIndices();
    }
    return &indices_[0];
  }

  // Returns nullptr if nulls() has a bit per top-level row. Otherwise,
  // returns a set of indices that gives the bit position in
  // nulls() for each top level decoded row.
  const vector_size_t* nullIndices() {
    if (hasExtraNulls_) {
      return nullptr;
    }
    return indices_;
  }

  bool hasExtraNulls() const {
    return hasExtraNulls_;
  }

  vector_size_t index(vector_size_t idx) const {
    if (isIdentityMapping_) {
      return idx;
    }
    if (isConstantMapping_) {
      return constantIndex_;
    }
    VELOX_DCHECK(indices_);
    return indices_[idx];
  }

  vector_size_t nullIndex(vector_size_t idx) const {
    if (isIdentityMapping_ || hasExtraNulls_) {
      return idx;
    }
    if (isConstantMapping_) {
      return 0;
    }
    VELOX_DCHECK(indices_);
    return indices_[idx];
  }

  template <typename T>
  T valueAt(vector_size_t idx) const {
    return reinterpret_cast<const T*>(data_)[index(idx)];
  }

  bool mayHaveNulls() const {
    return mayHaveNulls_;
  }

  bool mayHaveNullsRecursive() const {
    return mayHaveNulls_ || baseVector_->mayHaveNullsRecursive();
  }

  bool isNullAt(vector_size_t idx) const {
    if (!nulls_) {
      return false;
    }
    return bits::isBitNull(nulls_, nullIndex(idx));
  }

  vector_size_t size() const {
    return size_;
  }

  const BaseVector* base() const {
    return baseVector_;
  }

  bool isIdentityMapping() const {
    return isIdentityMapping_;
  }

  bool isConstantMapping() const {
    return isConstantMapping_;
  }

  // Wraps a vector with the same wrapping as another. 'wrapper' must
  // have been previously decoded by 'this'. This is used when 'data'
  // is a component of the base vector of 'wrapper' and must be used
  // in the same context, thus with the same indirections.
  VectorPtr wrap(
      VectorPtr data,
      const BaseVector& wrapper,
      const SelectivityVector& rows);

  // Pre-allocated vector of 0, 1, 2,
  static const std::vector<vector_size_t>& consecutiveIndices();

  // Pre-allocated vector of all zeros.
  static const std::vector<vector_size_t>& zeroIndices();

  bool indicesNotCopied() const {
    return copiedIndices_.empty() || indices_ < copiedIndices_.data() ||
        indices_ >= &copiedIndices_.back();
  }

  bool nullsNotCopied() const {
    return copiedNulls_.empty() || nulls_ != copiedNulls_.data();
  }

 private:
  void setFlatNulls(const BaseVector& vector, const SelectivityVector& rows);

  template <TypeKind kind>
  void decodeBiased(const BaseVector& vector, const SelectivityVector& rows);

  void makeIndicesMutable();

  void combineWrappers(
      const BaseVector* vector,
      const SelectivityVector& rows,
      int numLevels = -1);

  void applyDictionaryWrapper(
      const BaseVector& dictionaryVector,
      const SelectivityVector& rows);

  void applySequenceWrapper(
      const BaseVector& sequenceVector,
      const SelectivityVector& rows);

  void copyNulls(vector_size_t size);

  void fillInIndices();

  void setBaseData(const BaseVector& vector, const SelectivityVector& rows);

  void setBaseDataForConstant(
      const BaseVector& vector,
      const SelectivityVector& rows);

  void setBaseDataForBias(
      const BaseVector& vector,
      const SelectivityVector& rows);

  void reset(vector_size_t size);

  // Last valid index into 'indices_' + 1.
  vector_size_t size_ = 0;

  // The indices into 'data_' or 'baseVector_' for the rows in
  // 'rows' given to decode(). Only positions that are in
  // 'selection' are guaranteed to have valid values.
  const vector_size_t* indices_ = nullptr;

  // The base array of 'vector' given to decode(), nullptr if vector is of
  // complex type.
  const void* data_ = nullptr;

  // Null bitmask. One bit for each T in 'data_'. nullptr f if there
  // are no nulls.
  const uint64_t* nulls_ = nullptr;

  // The base vector of 'vector' given to decode(). This is the data
  // after sequence, constant and dictionary vectors have been peeled
  // off.
  const BaseVector* baseVector_ = nullptr;

  // True if either the leaf vector has nulls or if nulls were added
  // by a dictionary wrapper.
  bool mayHaveNulls_ = false;

  // True if nulls added by a dictionary wrapper.
  bool hasExtraNulls_ = false;

  bool isIdentityMapping_ = false;

  bool isConstantMapping_ = false;

  bool loadLazy_ = false;

  // Index of an element of the baseVector_ that points to a constant value of
  // complex type. Applies only when isConstantMapping_ is true and baseVector_
  // is of complex type (array, map, row).
  vector_size_t constantIndex_{0};

  // Holds data that needs to be copied out from the base vector,
  // e.g. exploded BiasVector values.
  std::vector<uint64_t> tempSpace_;

  // Holds indices if an array of indices needs to be materialized,
  // e.g. when combining nested dictionaries.
  std::vector<vector_size_t> copiedIndices_;

  // Used as backing for 'nulls_' when null-ness is combined from
  // dictionary and base values.
  std::vector<uint64_t> copiedNulls_;

  // Used as 'nulls_' for a null constant vector.
  static uint64_t constantNullMask_;
};

template <>
inline bool DecodedVector::valueAt(vector_size_t idx) const {
  return bits::isBitSet(reinterpret_cast<const uint64_t*>(data_), index(idx));
}

} // namespace facebook::velox
