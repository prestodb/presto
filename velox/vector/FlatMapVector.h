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

#include <unordered_map>
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox {

/// FlatMapVector implements the flat map layout, which is an alternative way of
/// encoding logical maps.
///
/// Flat maps represent logical maps (the MAP() TypePtr), but instead of keeping
/// two inner vectors, one for keys and one for values (and buffers for
/// lengths/offsets), like MapVector, flat maps group values for different keys
/// in different vectors.
///
/// The entire purpose of this layout is to support efficient key projection on
/// large maps, since projecting a particular key from the map can be done in a
/// zero-copy manner by just returning the map values vector for a particular
/// key.
///
/// For example, take the following datasets containing maps for 4 records:
///
/// {
///   {{101, 1}, {102, 2}},
///   {{101, 2}, {102, 2}},
///   {{101, 3}},
///   {{101, null}, {103, 4}},
/// }
///
/// While a map vector would organize data in the following way:
///
/// keys:
///   {101, 102, 101, 102, 101, 101, 103}
/// values:
///   {1, 2, 2, 2, 3, null, 4}
/// lengths/offsets:
///   {2, 2, 1, 2}, {0, 2, 4, 5}
///
/// A flat map organizes data as follows:
///
/// distinct keys:
///   {101, 102, 103}
/// map values:
///   0: {1, 2, 3, null}
///   1: {2, 2, null, null}
///   2: {null, null, null, 4}
///
/// To differentiate a null map value from a non-existing key in the map, an
/// optional bitmask is kept for each map values (the "in-map" buffers):
///
/// in-map:
///   0: {} // empty buffer means all exist / all 1s.
///   1: {1, 1, 0, 0}
///   3: {0, 0, 0, 1}
///
/// To allow mapping a key to the correct index in the map values vector (its
/// "channel"), a hash map is maintained inside the object. To enable generic
/// key types, projecting a key can be done by using a vector of abitrary type
/// (see "getKeyChannel()" below), but fast-paths for common key types are
/// provided (INTEGER/BIGINT/VARCHAR).
///
class FlatMapVector : public BaseVector {
 public:
  FlatMapVector(const FlatMapVector&) = delete;
  FlatMapVector& operator=(const FlatMapVector&) = delete;

  // Move the shared_ptr instead.
  FlatMapVector(FlatMapVector&& other) = delete;
  FlatMapVector& operator=(FlatMapVector&&) = delete;

  // The size of the distinct keys vector must match the size of map values (the
  // in-map buffers may not be present).
  FlatMapVector(
      velox::memory::MemoryPool* pool,
      std::shared_ptr<const Type> type,
      BufferPtr nulls,
      size_t length,
      VectorPtr distinctKeys,
      std::vector<VectorPtr> mapValues,
      std::vector<BufferPtr> inMaps,
      std::optional<vector_size_t> nullCount = std::nullopt,
      bool sortedKeys = false)
      : BaseVector(
            pool,
            type,
            VectorEncoding::Simple::FLAT_MAP,
            nulls,
            length,
            std::nullopt,
            nullCount),
        mapValues_(std::move(mapValues)),
        inMaps_(std::move(inMaps)),
        sortedKeys_(sortedKeys) {
    VELOX_CHECK(type->isMap(), "FlatMapVector requires a MAP type.");
    distinctKeys_ = BaseVector::getOrCreateEmpty(
        std::move(distinctKeys), type->childAt(0), pool);
    setDistinctKeysImpl(distinctKeys_);

    VELOX_CHECK_EQ(
        numDistinctKeys(),
        mapValues_.size(),
        "Wrong number of map value vectors.");
    VELOX_CHECK_LE(
        inMaps_.size(), numDistinctKeys(), "Wrong number of in map buffers.");
  }

  virtual ~FlatMapVector() override {}

  /// Overwrites the existing distinct keys vector, resizing map values and
  /// clearing in-map buffers.
  void setDistinctKeys(VectorPtr distinctKeys, bool sortedKeys = false) {
    setDistinctKeysImpl(std::move(distinctKeys));
    mapValues_.resize(numDistinctKeys());
    inMaps_.clear();
    sortedKeys_ = sortedKeys;
  }

  TypePtr keyType() const {
    return type()->asMap().keyType();
  }

  TypePtr valueType() const {
    return type()->asMap().valueType();
  }

  /// Returns the distinct keys on this map. Note that this is different than
  /// simply returning (all) keys from a map, since these are deduplicated
  /// across all rows from the map.
  const VectorPtr& distinctKeys() const {
    return distinctKeys_;
  }

  vector_size_t numDistinctKeys() const {
    return distinctKeys_ != nullptr ? distinctKeys_->size() : 0;
  }

  bool hasSortedKeys() const {
    return sortedKeys_;
  }

  const std::vector<VectorPtr>& mapValues() const {
    return mapValues_;
  }

  const std::vector<BufferPtr>& inMaps() const {
    return inMaps_;
  }

  /// Returns the channel for a particular key present in `keysVector` at index
  /// `index`. This is a general version that works for every key type,
  /// including complex types. Below, fast-paths are provided for common key
  /// types.
  ///
  /// This channel can be used to fetch mapValues and inMap buffers. Returns
  /// std::nullopt if there is no key match.
  std::optional<column_index_t> getKeyChannel(
      const VectorPtr& keysVector,
      vector_size_t index) const;

  /// Fast-path for returning the channel for a particular primitive type key.
  /// The returned channel can then be used to fetch the map values vectors and
  /// in-map buffers.
  ///
  /// When using these functions, it is the caller's reponsibility to provide a
  /// parameter that is compatible with the distinct keys flat vector in the
  /// object (int64_t for BIGINT(), int32_t for INTEGER(), and so forth).
  ///
  /// The function throws in case the types are not compatible, and returns
  /// std::nullopt if the types are compatible but the key wasn't not found.
  ///
  /// For key types other than the ones below (such as complex type keys),
  /// callers should use the general version based on VectorPtr above.
  std::optional<column_index_t> getKeyChannel(int32_t scalarKey) const;
  std::optional<column_index_t> getKeyChannel(int64_t scalarKey) const;
  std::optional<column_index_t> getKeyChannel(StringView scalarKey) const;

  /// Returns a vector containing a projection for a specific primitive key.
  /// This is an efficient operation in flat maps (zero-copy).
  ///
  /// The key restrictions are the same as the ones listed above for
  /// `getKeyChannel()`.
  template <typename TKey>
  VectorPtr projectKey(TKey scalarKey) const {
    if (auto channel = getKeyChannel(scalarKey)) {
      return mapValues_[channel.value()];
    }
    return nullptr;
  }

  /// Returns the size for the map at position `index`. Size means the number of
  /// logical key value pairs in the map. Note that this is not a particularly
  /// efficient operation in flat maps as it requires accessing the inMap bitmap
  /// for each distinct key.
  vector_size_t sizeAt(vector_size_t index) const;

  bool isInMap(column_index_t keyChannel, vector_size_t index) const {
    auto* inMap = inMapsAt(keyChannel)->asMutable<uint64_t>();
    return inMap ? bits::isBitSet(inMap, index) : true;
  }

  /// Get the map values vector at a given a map key channel.
  const VectorPtr& mapValuesAt(column_index_t index) const {
    VELOX_CHECK_LT(
        index,
        mapValues_.size(),
        "Trying to access non-existing key channel in FlatMapVector.");
    return mapValues_[index];
  }

  /// Non-const version of the method above.
  VectorPtr& mapValuesAt(column_index_t index) {
    VELOX_CHECK_LT(
        index,
        mapValues_.size(),
        "Trying to access non-existing key channel in FlatMapVector.");
    return mapValues_[index];
  }

  /// Get the in map buffer for a given a map key channel. Throws if in maps
  /// does not exist for this channel.
  const BufferPtr& inMapsAt(column_index_t index) const {
    VELOX_CHECK_LT(
        index,
        inMaps_.size(),
        "Trying to access non-existing key channel in FlatMapVector.");
    return inMaps_[index];
  }

  /// Get the in map buffer reference for a given map key channel. If `resize`
  /// is true, may resize up to `numDistinctKeys()` to ensure that the inMaps
  /// vector has the appropriate length.
  BufferPtr& inMapsAt(column_index_t index, bool resize = false) {
    if (index < inMaps_.size()) {
      return inMaps_[index];
    } else if (!resize || index >= numDistinctKeys()) {
      VELOX_CHECK_LT(
          index,
          inMaps_.size(),
          "Trying to access non-existing key channel in FlatMapVector.");
    }
    inMaps_.resize(index + 1);
    return inMaps_[index];
  }

  using BaseVector::toString;
  std::string toString(vector_size_t index) const override;

  // Resize will recursively change the size of each map value vector (nullptr
  // vectors will remain nullptr), in-map, and top level null buffers.
  void resize(vector_size_t newSize, bool setNotNull = true) override;

  VectorPtr slice(vector_size_t offset, vector_size_t length) const override;

  VectorPtr testingCopyPreserveEncodings(
      velox::memory::MemoryPool* pool = nullptr) const override;

  /// Returns true if the map is null, or either one of the key or value of its
  /// entries are null.
  bool containsNullAt(vector_size_t index) const override;

  /// Returns indices into the map at 'index' such
  /// that keys[indices[i]] < keys[indices[i + 1]].
  std::vector<vector_size_t> sortedKeyIndices(vector_size_t index) const;

  // Flat map comparison logic follows the comparison logic from MapVector.
  std::optional<int32_t> compare(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex,
      CompareFlags flags) const override;

  /// Returns the hash of the value at the given index in this vector.
  uint64_t hashValueAt(vector_size_t index) const override;

  std::unique_ptr<SimpleVector<uint64_t>> hashAll() const override;

  /// Converts and copies `this` into a MapVector.
  ///
  /// It does so by wrapping the distinctKeys_ vector into a dictionary to
  /// increase its cardinality accordingly, and copying each map values vector
  /// into a single vector containing all flattened elements.
  ///
  /// This is an expensive operation that should be used mostly for
  /// testing/validation purposes, and not for performance critical paths.
  MapVectorPtr toMapVector() const;

 private:
  void setDistinctKeysImpl(VectorPtr distinctKeys) {
    VELOX_CHECK(distinctKeys != nullptr);
    VELOX_CHECK(
        *distinctKeys->type() == *keyType(),
        "Unexpected key type: {}",
        distinctKeys->type()->toString());

    distinctKeys_ = distinctKeys;
    keyToChannel_.clear();

    for (vector_size_t i = 0; i < numDistinctKeys(); i++) {
      keyToChannel_.insert({distinctKeys->hashValueAt(i), i});
    }
  }

  // Vector containing the distinct map keys.
  VectorPtr distinctKeys_;

  // Map values - one VectorPtr per distinct key value. Indices on this vector
  // are aligned with the indices on distinctKeys.
  std::vector<VectorPtr> mapValues_;

  // In-map buffers indicate whether a particular key logically exists in the
  // map. The key is dictated by the std::vector position (aligned with
  // distinctKeys_).
  //
  // Note that key not existing is different from a key existing but having a
  // null value.
  std::vector<BufferPtr> inMaps_;

  // Hash table that enables flat map keys to find the channel (the index on
  // mapValues_ and inMaps_ for that key).
  //
  // To avoid having to template this class and supporting arbitrarily nested
  // keys, the hash table key is the hash of the flat map key. This means that
  // hash collisions need to be manually handled by comparing the actual key
  // values, and hence a multimap is needed.
  std::unordered_multimap<uint64_t, column_index_t> keyToChannel_;

  // Whether the distinct keys vector stores sorted keys.
  bool sortedKeys_;
};

using FlatMapVectorPtr = std::shared_ptr<FlatMapVector>;

} // namespace facebook::velox
