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

#include <folly/container/F14Map.h>
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/exec/AddressableNonNullValueList.h"
#include "velox/exec/Strings.h"
#include "velox/functions/lib/aggregates/ValueList.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

namespace detail {
/// Maintains a key-value map. Keys must be non-null.
template <
    typename T,
    typename Hash = std::hash<T>,
    typename EqualTo = std::equal_to<T>>
struct MapAccumulator {
  // Value is the index of the corresponding entry in 'values'.
  folly::F14FastMap<
      T,
      int32_t,
      Hash,
      EqualTo,
      AlignedStlAllocator<std::pair<const T, int32_t>, 16>>
      keys;
  ValueList values;

  MapAccumulator(const TypePtr& /*type*/, HashStringAllocator* allocator)
      : keys{AlignedStlAllocator<std::pair<const T, int32_t>, 16>(allocator)} {}

  MapAccumulator(Hash hash, EqualTo equalTo, HashStringAllocator* allocator)
      : keys{
            0,
            hash,
            equalTo,
            AlignedStlAllocator<std::pair<const T, int32_t>, 16>(allocator)} {}

  /// Adds key-value pair if entry with that key doesn't exist yet.
  void insert(
      const DecodedVector& decodedKeys,
      const DecodedVector& decodedValues,
      vector_size_t index,
      HashStringAllocator& allocator) {
    // Drop duplicate keys.
    auto cnt = keys.size();
    if (keys.insert({decodedKeys.valueAt<T>(index), cnt}).second) {
      values.appendValue(decodedValues, index, &allocator);
    }
  }

  /// Returns number of key-value pairs.
  size_t size() const {
    return keys.size();
  }

  void extract(
      const VectorPtr& mapKeys,
      const VectorPtr& mapValues,
      vector_size_t offset) {
    const auto mapSize = keys.size();

    // Align keys and values as the order of keys in 'keys' may not match the
    // order of values in 'values'.
    folly::F14FastMap<int32_t, int32_t> indices;

    auto flatKeys = mapKeys->asFlatVector<T>();

    vector_size_t index = offset;
    for (auto value : keys) {
      flatKeys->set(index, value.first);
      indices[value.second] = index - offset;
      ++index;
    }

    extractValues(mapValues, offset, mapSize, indices);
  }

  void extractValues(
      const VectorPtr& mapValues,
      vector_size_t offset,
      int32_t mapSize,
      const folly::F14FastMap<int32_t, int32_t>& indices) {
    ValueListReader valuesReader(values);
    for (auto index = 0; index < mapSize; ++index) {
      valuesReader.next(*mapValues, offset + indices.at(index));
    }
  }

  void free(HashStringAllocator& allocator) {
    std::destroy_at(&keys);
    values.free(&allocator);
  }
};

/// Maintains a map with string keys.
struct StringViewMapAccumulator {
  /// A set of unique StringViews pointing to storage managed by 'strings'.
  MapAccumulator<StringView> base;

  /// Stores unique non-null non-inline strings.
  Strings strings;

  StringViewMapAccumulator(const TypePtr& type, HashStringAllocator* allocator)
      : base{type, allocator} {}

  void insert(
      const DecodedVector& decodedKeys,
      const DecodedVector& decodedValues,
      vector_size_t index,
      HashStringAllocator& allocator) {
    auto key = decodedKeys.valueAt<StringView>(index);
    if (!key.isInline()) {
      if (base.keys.contains(key)) {
        return;
      }
      key = strings.append(key, allocator);
    }

    auto cnt = base.keys.size();
    if (base.keys.insert({key, cnt}).second) {
      base.values.appendValue(decodedValues, index, &allocator);
    }
  }

  size_t size() const {
    return base.size();
  }

  void extract(
      const VectorPtr& mapKeys,
      const VectorPtr& mapValues,
      vector_size_t offset) {
    base.extract(mapKeys, mapValues, offset);
  }

  void free(HashStringAllocator& allocator) {
    strings.free(allocator);
    base.free(allocator);
  }
};

/// Maintains a map with keys of type array, map or struct.
struct ComplexTypeMapAccumulator {
  /// A set of pointers to values stored in AddressableNonNullValueList.
  MapAccumulator<
      AddressableNonNullValueList::Entry,
      AddressableNonNullValueList::Hash,
      AddressableNonNullValueList::EqualTo>
      base;

  /// Stores unique non-null keys.
  AddressableNonNullValueList serializedKeys;

  ComplexTypeMapAccumulator(const TypePtr& type, HashStringAllocator* allocator)
      : base{
            AddressableNonNullValueList::Hash{},
            AddressableNonNullValueList::EqualTo{type},
            allocator} {}

  void insert(
      const DecodedVector& decodedKeys,
      const DecodedVector& decodedValues,
      vector_size_t index,
      HashStringAllocator& allocator) {
    auto entry = serializedKeys.append(decodedKeys, index, &allocator);

    auto cnt = base.keys.size();
    if (!base.keys.insert({entry, cnt}).second) {
      serializedKeys.removeLast(entry);
      return;
    }

    base.values.appendValue(decodedValues, index, &allocator);
  }

  size_t size() const {
    return base.size();
  }

  void extract(
      const VectorPtr& mapKeys,
      const VectorPtr& mapValues,
      vector_size_t offset) {
    const auto mapSize = base.keys.size();

    folly::F14FastMap<int32_t, int32_t> indices;
    indices.reserve(mapSize);

    vector_size_t index = offset;
    for (const auto& value : base.keys) {
      AddressableNonNullValueList::read(value.first, *mapKeys, index);
      indices[value.second] = index - offset;
      ++index;
    }

    base.extractValues(mapValues, offset, mapSize, indices);
  }

  void free(HashStringAllocator& allocator) {
    serializedKeys.free(allocator);
    base.free(allocator);
  }
};

template <typename T>
struct MapAccumulatorTypeTraits {
  using AccumulatorType = MapAccumulator<T>;
};

template <>
struct MapAccumulatorTypeTraits<float> {
  using AccumulatorType = MapAccumulator<
      float,
      util::floating_point::NaNAwareHash<float>,
      util::floating_point::NaNAwareEquals<float>>;
};

template <>
struct MapAccumulatorTypeTraits<double> {
  using AccumulatorType = MapAccumulator<
      double,
      util::floating_point::NaNAwareHash<double>,
      util::floating_point::NaNAwareEquals<double>>;
};

template <>
struct MapAccumulatorTypeTraits<StringView> {
  using AccumulatorType = StringViewMapAccumulator;
};

template <>
struct MapAccumulatorTypeTraits<ComplexType> {
  using AccumulatorType = ComplexTypeMapAccumulator;
};

} // namespace detail

/// A wrapper around MapAccumulator that overrides hash and equal_to functions
/// to use the custom comparisons provided by a custom type.
template <TypeKind Kind>
struct CustomComparisonMapAccumulator {
  using NativeType = typename TypeTraits<Kind>::NativeType;

  struct Hash {
    const TypePtr& type;

    size_t operator()(const NativeType& value) const {
      return static_cast<const CanProvideCustomComparisonType<Kind>*>(
                 type.get())
          ->hash(value);
    }
  };

  struct EqualTo {
    const TypePtr& type;

    bool operator()(const NativeType& left, const NativeType& right) const {
      return static_cast<const CanProvideCustomComparisonType<Kind>*>(
                 type.get())
                 ->compare(left, right) == 0;
    }
  };

  /// The underlying MapAccumulator to which all operations are delegated.
  detail::MapAccumulator<
      NativeType,
      CustomComparisonMapAccumulator::Hash,
      CustomComparisonMapAccumulator::EqualTo>
      base;

  CustomComparisonMapAccumulator(
      const TypePtr& type,
      HashStringAllocator* allocator)
      : base{
            CustomComparisonMapAccumulator::Hash{type},
            CustomComparisonMapAccumulator::EqualTo{type},
            allocator} {}

  /// Adds key-value pair if entry with that key doesn't exist yet.
  void insert(
      const DecodedVector& decodedKeys,
      const DecodedVector& decodedValues,
      vector_size_t index,
      HashStringAllocator& allocator) {
    return base.insert(decodedKeys, decodedValues, index, allocator);
  }

  /// Returns number of key-value pairs.
  size_t size() const {
    return base.size();
  }

  void extract(
      const VectorPtr& mapKeys,
      const VectorPtr& mapValues,
      vector_size_t offset) {
    base.extract(mapKeys, mapValues, offset);
  }

  void extractValues(
      const VectorPtr& mapValues,
      vector_size_t offset,
      int32_t mapSize,
      const folly::F14FastMap<int32_t, int32_t>& indices) {
    base.extractValues(mapValues, offset, mapSize, indices);
  }

  void free(HashStringAllocator& allocator) {
    base.free(allocator);
  }
};

template <typename T>
using MapAccumulator =
    typename detail::MapAccumulatorTypeTraits<T>::AccumulatorType;

} // namespace facebook::velox::aggregate::prestosql
