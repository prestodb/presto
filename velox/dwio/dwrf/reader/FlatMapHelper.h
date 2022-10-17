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

#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/reader/StripeStream.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::dwrf::flatmap_helper {
namespace detail {

// Reset vector with the desired size/hasNulls properties
void reset(VectorPtr& vector, vector_size_t size, bool hasNulls);

// Reset vector smart pointer if any of the buffers is not single referenced.
template <typename... T>
void resetIfNotWritable(VectorPtr& vector, const T&... buffer) {
  if ((... | (buffer && buffer->refCount() > 1))) {
    vector.reset();
  }
}

// Initialize string vector.
void initializeStringVector(
    VectorPtr& vector,
    memory::MemoryPool& pool,
    vector_size_t size,
    bool hasNulls,
    std::vector<BufferPtr>&& stringBuffers);

} // namespace detail

// Initialize flat vector
template <typename T>
void initializeFlatVector(
    VectorPtr& vector,
    memory::MemoryPool& pool,
    vector_size_t size,
    bool hasNulls,
    std::vector<BufferPtr>&& stringBuffers = {}) {
  detail::reset(vector, size, hasNulls);
  if (vector) {
    auto& flatVector = dynamic_cast<FlatVector<T>&>(*vector);
    detail::resetIfNotWritable(vector, flatVector.nulls(), flatVector.values());
    if (vector) {
      flatVector.setStringBuffers(stringBuffers);
    }
  }

  if (!vector) {
    vector = std::make_shared<FlatVector<T>>(
        &pool,
        hasNulls ? AlignedBuffer::allocate<bool>(size, &pool) : nullptr,
        0 /*length*/,
        AlignedBuffer::allocate<T>(size, &pool),
        std::move(stringBuffers));
    vector->setNullCount(0);
  }
}

// Initialize map vector.
void initializeMapVector(
    VectorPtr& vector,
    const std::shared_ptr<const Type>& type,
    memory::MemoryPool& pool,
    const std::vector<const BaseVector*>& vectors,
    std::optional<vector_size_t> sizeOverride = std::nullopt);

// Initialize vector with a list of vectors. Make sure the initialized vector
// has the capacity to hold all data from them.
void initializeVector(
    VectorPtr& vector,
    const std::shared_ptr<const Type>& type,
    memory::MemoryPool& pool,
    const std::vector<const BaseVector*>& vectors);

// Copy one value from source vector to target.
void copyOne(
    const std::shared_ptr<const Type>& type,
    BaseVector& target,
    vector_size_t targetIndex,
    const BaseVector& source,
    vector_size_t sourceIndex);

// Copy values from source vector to target.
void copy(
    const std::shared_ptr<const Type>& type,
    BaseVector& target,
    vector_size_t targetIndex,
    const BaseVector& source,
    vector_size_t sourceIndex,
    vector_size_t count);

// Represent key value based on type.
template <typename T>
class KeyValue {
 private:
  T value_;
  size_t h_;

 public:
  explicit KeyValue(T value) : value_{value}, h_{std::hash<T>()(value)} {}

  const T& get() const {
    return value_;
  }
  std::size_t hash() const {
    return h_;
  }

  bool operator==(const KeyValue<T>& other) const {
    return value_ == other.value_;
  }
};

template <typename T>
struct KeyValueHash {
  std::size_t operator()(const KeyValue<T>& kv) const {
    return kv.hash();
  }
};

template <typename T>
KeyValue<T> extractKey(const proto::KeyInfo& info) {
  return KeyValue<T>(info.intkey());
}

template <>
inline KeyValue<StringView> extractKey<StringView>(const proto::KeyInfo& info) {
  return KeyValue<StringView>(StringView(info.byteskey()));
}

template <typename T>
KeyValue<T> parseKeyValue(std::string_view str) {
  return KeyValue<T>(folly::to<T>(str));
}

template <>
inline KeyValue<StringView> parseKeyValue<StringView>(std::string_view str) {
  return KeyValue<StringView>(StringView(str));
}

enum class KeyProjectionMode { ALLOW, REJECT };

template <typename T>
struct KeyProjection {
  KeyProjectionMode mode = KeyProjectionMode::ALLOW;
  KeyValue<T> value;
};

template <typename T>
KeyProjection<T> convertDynamic(const folly::dynamic& v) {
  constexpr char reject_prefix = '!';
  const auto str = v.asString();
  const std::string_view view(str);
  if (!view.empty() && view.front() == reject_prefix) {
    return {
        .mode = KeyProjectionMode::REJECT,
        .value = parseKeyValue<T>(view.substr(1)),
    };
  } else {
    return {
        .mode = KeyProjectionMode::ALLOW,
        .value = parseKeyValue<T>(view),
    };
  }
}

template <typename T>
class KeyPredicate {
 public:
  using Lookup = std::unordered_set<KeyValue<T>, KeyValueHash<T>>;

  KeyPredicate(KeyProjectionMode mode, Lookup keyLookup)
      : keyLookup_{std::move(keyLookup)},
        predicate_{
            mode == KeyProjectionMode::ALLOW ? &KeyPredicate::allowFilter
                                             : &KeyPredicate::rejectFilter} {}

  bool operator()(const KeyValue<T>& key) const {
    return predicate_(key, keyLookup_);
  }

 private:
  static bool allowFilter(const KeyValue<T>& key, const Lookup& lookup) {
    return lookup.size() == 0 || lookup.count(key) > 0;
  }

  static bool rejectFilter(const KeyValue<T>& key, const Lookup& lookup) {
    return lookup.size() == 0 || lookup.count(key) == 0;
  }

  Lookup keyLookup_;
  std::function<bool(const KeyValue<T>&, const Lookup&)> predicate_;
};

template <typename T>
KeyPredicate<T> prepareKeyPredicate(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    StripeStreams& stripe) {
  std::vector<KeyProjectionMode> modes;
  std::vector<KeyValue<T>> keys;

  auto& cs = stripe.getColumnSelector();
  const auto expr = cs.getNode(requestedType->id)->getNode().expression;
  if (!expr.empty()) {
    // JSON parse option?
    auto array = folly::parseJson(expr);
    for (auto v : array) {
      VELOX_CHECK(!v.isNull(), "map key filter should not be null");
      auto converted = convertDynamic<T>(v);
      modes.push_back(converted.mode);
      keys.push_back(std::move(converted.value));
    }
    VLOG(1) << "[Flat-Map] key filters count: " << array.size();
  }

  VELOX_CHECK_EQ(modes.size(), keys.size());
  // You cannot mix allow key and reject key.
  VELOX_CHECK(
      modes.empty() ||
      std::all_of(modes.begin(), modes.end(), [&modes](const auto& v) {
        return v == modes.front();
      }));

  auto mode = modes.empty() ? KeyProjectionMode::ALLOW : modes.front();

  return KeyPredicate<T>(
      mode, typename KeyPredicate<T>::Lookup(keys.begin(), keys.end()));
}

} // namespace facebook::velox::dwrf::flatmap_helper
