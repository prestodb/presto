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
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/functions/prestosql/aggregates/AddressableNonNullValueList.h"
#include "velox/functions/prestosql/aggregates/Strings.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

/// Maintains a set of unique values. Non-null values are stored in F14FastSet.
/// A separate flag tracks presence of the null value.
template <
    typename T,
    typename Hash = std::hash<T>,
    typename EqualTo = std::equal_to<T>>
struct SetAccumulator {
  bool hasNull{false};
  folly::F14FastSet<T, Hash, EqualTo, AlignedStlAllocator<T, 16>> uniqueValues;

  SetAccumulator(const TypePtr& /*type*/, HashStringAllocator* allocator)
      : uniqueValues{AlignedStlAllocator<T, 16>(allocator)} {}

  SetAccumulator(Hash hash, EqualTo equalTo, HashStringAllocator* allocator)
      : uniqueValues{0, hash, equalTo, AlignedStlAllocator<T, 16>(allocator)} {}

  /// Adds value if new. No-op if the value was added before.
  void addValue(
      const DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* /*allocator*/) {
    if (decoded.isNullAt(index)) {
      hasNull = true;
    } else {
      uniqueValues.insert(decoded.valueAt<T>(index));
    }
  }

  /// Adds new values from an array.
  void addValues(
      const ArrayVector& arrayVector,
      vector_size_t index,
      const DecodedVector& values,
      HashStringAllocator* allocator) {
    const auto size = arrayVector.sizeAt(index);
    const auto offset = arrayVector.offsetAt(index);

    for (auto i = 0; i < size; ++i) {
      addValue(values, offset + i, allocator);
    }
  }

  /// Returns number of unique values including null.
  size_t size() const {
    return uniqueValues.size() + (hasNull ? 1 : 0);
  }

  /// Copies the unique values and null into the specified vector starting at
  /// the specified offset.
  vector_size_t extractValues(FlatVector<T>& values, vector_size_t offset) {
    vector_size_t index = offset;
    for (auto value : uniqueValues) {
      values.set(index++, value);
    }

    if (hasNull) {
      values.setNull(index++, true);
    }

    return index - offset;
  }

  void free(HashStringAllocator& allocator) {}
};

/// Maintains a set of unique strings.
struct StringViewSetAccumulator {
  /// A set of unique StringViews pointing to storage managed by 'strings'.
  SetAccumulator<StringView> base;

  /// Stores unique non-null non-inline strings.
  Strings strings;

  StringViewSetAccumulator(const TypePtr& type, HashStringAllocator* allocator)
      : base{type, allocator} {}

  void addValue(
      const DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* allocator) {
    if (decoded.isNullAt(index)) {
      base.hasNull = true;
    } else {
      auto value = decoded.valueAt<StringView>(index);
      if (!value.isInline()) {
        if (base.uniqueValues.contains(value)) {
          return;
        }
        value = strings.append(value, *allocator);
      }
      base.uniqueValues.insert(value);
    }
  }

  void addValues(
      const ArrayVector& arrayVector,
      vector_size_t index,
      const DecodedVector& values,
      HashStringAllocator* allocator) {
    const auto size = arrayVector.sizeAt(index);
    const auto offset = arrayVector.offsetAt(index);

    for (auto i = 0; i < size; ++i) {
      addValue(values, offset + i, allocator);
    }
  }

  size_t size() const {
    return base.size();
  }

  vector_size_t extractValues(
      FlatVector<StringView>& values,
      vector_size_t offset) {
    return base.extractValues(values, offset);
  }

  void free(HashStringAllocator& allocator) {
    strings.free(allocator);
  }
};

/// Maintains a set of unique arrays, maps or structs.
struct ComplexTypeSetAccumulator {
  /// A set of pointers to values stored in AddressableNonNullValueList.
  SetAccumulator<
      HashStringAllocator::Position,
      AddressableNonNullValueList::Hash,
      AddressableNonNullValueList::EqualTo>
      base;

  /// Stores unique non-null values.
  AddressableNonNullValueList values;

  ComplexTypeSetAccumulator(const TypePtr& type, HashStringAllocator* allocator)
      : base{
            AddressableNonNullValueList::Hash{},
            AddressableNonNullValueList::EqualTo{type},
            allocator} {}

  void addValue(
      const DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* allocator) {
    if (decoded.isNullAt(index)) {
      base.hasNull = true;
    } else {
      auto position = values.append(decoded, index, allocator);

      if (!base.uniqueValues.insert(position).second) {
        values.removeLast(position);
      }
    }
  }

  void addValues(
      const ArrayVector& arrayVector,
      vector_size_t index,
      const DecodedVector& values,
      HashStringAllocator* allocator) {
    const auto size = arrayVector.sizeAt(index);
    const auto offset = arrayVector.offsetAt(index);

    for (auto i = 0; i < size; ++i) {
      addValue(values, offset + i, allocator);
    }
  }

  size_t size() const {
    return base.size();
  }

  vector_size_t extractValues(BaseVector& values, vector_size_t offset) {
    vector_size_t index = offset;
    for (const auto& position : base.uniqueValues) {
      AddressableNonNullValueList::read(position, values, index++);
    }

    if (base.hasNull) {
      values.setNull(index++, true);
    }

    return index - offset;
  }

  void free(HashStringAllocator& allocator) {
    values.free(allocator);
  }
};

template <typename T>
struct SetAccumulatorTypeTraits {
  using AccumulatorType = SetAccumulator<T>;
};

template <>
struct SetAccumulatorTypeTraits<StringView> {
  using AccumulatorType = StringViewSetAccumulator;
};

template <>
struct SetAccumulatorTypeTraits<ComplexType> {
  using AccumulatorType = ComplexTypeSetAccumulator;
};

} // namespace facebook::velox::aggregate::prestosql
