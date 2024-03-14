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

#include <folly/container/F14Set.h>
#include "velox/common/base/RawVector.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox {

struct VectorValueSetEntry {
  const BaseVector* vector;
  vector_size_t index;
};

struct VectorValueSetHasher {
  size_t operator()(const VectorValueSetEntry& entry) const {
    return entry.vector->hashValueAt(entry.index);
  }
};

struct VectorValueSetComparer {
  bool operator()(
      const VectorValueSetEntry& left,
      const VectorValueSetEntry& right) const {
    return left.vector->equalValueAt(right.vector, left.index, right.index);
  }
};

using VectorValueSet = folly::F14FastSet<
    VectorValueSetEntry,
    VectorValueSetHasher,
    VectorValueSetComparer>;

/// A map translating values in a vector to positions in an alphabet
/// vector. if the values are complex type or variable length, also
/// keeps track of the serialized size of the values. Usable for
/// ad-hoc reencoding for serialization.
class VectorMap {
 public:
  // Constructs 'this' to index the distinct elements in 'alphabet'. 'alphabet'
  // is not changed and not owned. For example, when concatenating two
  // dictionaries, the first initializes the map.
  explicit VectorMap(BaseVector& alphabet);

  // Constructs an empty map initializing alphabet to an empty vector of 'type'.
  // Alphabet is owned. 'reserve' is the expected count of distinct values.
  VectorMap(
      const TypePtr& type,
      memory::MemoryPool* pool,
      int32_t reserve = 32);

  /// Assigns a zero-based id to each distinct value in 'vector' at positions
  /// 'rows'. The ids are returned in 'ids'. If insertToAlphabet is true and the
  /// value is not previously in alphabet_, the value is added to it. Alphabet
  /// must be owned if insertToAlphabet is true.
  void addMultiple(
      BaseVector& vector,
      folly::Range<const vector_size_t*> rows,
      bool insertToAlphabet,
      vector_size_t* ids);

  // Gets/assigns s an id to single value. The meaning of parameters is as in
  // addMultiple().
  vector_size_t addOne(
      const BaseVector& vector,
      vector_size_t row,
      bool insertToAlphabet = true);

  /// Returns the number of distinct values in 'this'.
  vector_size_t size() const {
    return isString_ ? distinctStrings_.size() + (nullIndex_ != kNoNullIndex)
                     : distinctSet_.size();
  }

  /// Returns the approximate serialized binary length of the 'index'th value in
  /// 'alphabet_'. The type must be a variable length type.
  vector_size_t lengthAt(vector_size_t index) const {
    VELOX_DCHECK_EQ(fixedWidth_, kVariableWidth);
    return alphabetSizes_[index];
  }

  const VectorPtr& alphabetOwned() const {
    return alphabetOwned_;
  }

 private:
  static constexpr vector_size_t kNoNullIndex = -1;
  static constexpr int32_t kVariableWidth = -1;

  // Vector containing all the distinct values.
  BaseVector* alphabet_;

  // Optional owning pointer to 'alphabet_'.
  VectorPtr alphabetOwned_;

  // Map from value in 'alphabet_' to the index in 'alphabet_'.
  VectorValueSet distinctSet_;

  // Map from string value in 'alphabet_' to index in 'alphabet_'. Used only if
  // 'alphabet_' is a FlatVector<StringView>.
  folly::F14FastMap<StringView, int32_t> distinctStrings_;

  // Index of null value in 'alphabet_'.
  vector_size_t nullIndex_{kNoNullIndex};

  // Serialized size estimate for the corresponding element of 'alphabet'.
  raw_vector<vector_size_t> alphabetSizes_;

  // True if  using 'distinctStrings_'
  const bool isString_;

  const int32_t fixedWidth_;
};

} // namespace facebook::velox
