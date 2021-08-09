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

#include <cmath>
#include <type_traits>

#include <folly/FixedString.h>
#include <folly/String.h>
#include <folly/container/F14Map.h>
#include <folly/hash/Hash.h>
#include <glog/logging.h>

#include "velox/functions/lib/string/StringCore.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook {
namespace velox {

namespace exec {
class EvalCtx;
}

/**
 * Adds the given value to the metaData in a proper encoding.  Will overwrite
 * any existing value in the map.
 */
template <typename V>
void encodeMetaData(
    folly::F14FastMap<std::string, std::string>& metaData,
    const std::string& key,
    V value) {
  metaData.insert_or_assign(key, velox::to<std::string>(value));
}

template <>
inline void encodeMetaData(
    folly::F14FastMap<std::string, std::string>& metaData,
    const std::string& key,
    StringView value) {
  metaData.insert_or_assign(key, std::string(value.data(), value.size()));
}

template <>
inline void encodeMetaData(
    folly::F14FastMap<std::string, std::string>& /*metaData*/,
    const std::string& /*key*/,
    std::shared_ptr<void> /*value*/) {
  VELOX_NYI();
}

// This class abstracts over various Columnar Storage Formats such that Velox
// can select the most appropriate one on a per field / per block basis.
// The goal is to use the most appropriate type to optimize for:
//   - Lazy deserialization if desired.
//   - serialization / rehydration cost, ideally we use a smart view into the
//     data without fully rehydrating.
//   - serialized bytes
//   - cpu cost of filtering
//   - optimize aggregation of sequential values
template <typename T>
class SimpleVector : public BaseVector {
 public:
  constexpr static auto META_MIN = folly::makeFixedString("CTV1");
  constexpr static auto META_MAX = folly::makeFixedString("CTV2");

  SimpleVector(
      velox::memory::MemoryPool* pool,
      std::shared_ptr<const Type> type,
      BufferPtr nulls,
      size_t length,
      const folly::F14FastMap<std::string, std::string>& metaData,
      folly::Optional<vector_size_t> distinctValueCount,
      folly::Optional<vector_size_t> nullCount,
      folly::Optional<bool> isSorted,
      folly::Optional<ByteCount> representedByteCount,
      folly::Optional<ByteCount> storageByteCount = folly::none)
      : BaseVector(
            pool,
            std::move(type),
            std::move(nulls),
            length,
            distinctValueCount,
            nullCount,
            representedByteCount,
            storageByteCount),
        isSorted_(isSorted),
        elementSize_(sizeof(T)) {
    setMinMax(metaData);
  }

  // Constructs SimpleVector inferring the type from T.
  SimpleVector(
      velox::memory::MemoryPool* pool,
      BufferPtr nulls,
      size_t length,
      const folly::F14FastMap<std::string, std::string>& metaData,
      folly::Optional<vector_size_t> distinctValueCount,
      folly::Optional<vector_size_t> nullCount,
      folly::Optional<bool> isSorted,
      folly::Optional<ByteCount> representedByteCount,
      folly::Optional<ByteCount> storageByteCount = folly::none)
      : SimpleVector(
            pool,
            CppToType<T>::create(),
            std::move(nulls),
            length,
            metaData,
            distinctValueCount,
            nullCount,
            isSorted,
            representedByteCount,
            storageByteCount) {}

  virtual ~SimpleVector() override {}

  folly::F14FastMap<std::string, std::string> genMetaData() const {
    folly::F14FastMap<std::string, std::string> metaData;
    if (min_.hasValue()) {
      encodeMetaData(metaData, META_MIN, min_.value());
    }
    if (max_.hasValue()) {
      encodeMetaData(metaData, META_MAX, max_.value());
    }
    return metaData;
  }

  // Concrete Vector types need to implement this themselves.
  // This method does not do bounds checking. When the value is null the return
  // value is technically undefined (currently implemented as default of T)
  virtual const T valueAt(vector_size_t idx) const = 0;

  bool equalValueAt(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex) const override {
    other = other->loadedVector();
    DCHECK(dynamic_cast<const SimpleVector<T>*>(other) != nullptr)
        << "Attempting to compare vectors not of the same type";
    bool otherNull = other->isNullAt(otherIndex);
    if (isNullAt(index)) {
      return otherNull;
    }
    if (otherNull) {
      return false;
    }
    auto simpleVector = reinterpret_cast<const SimpleVector<T>*>(other);

    if constexpr (std::is_floating_point<T>::value) {
      T v1 = valueAt(index);
      T v2 = simpleVector->valueAt(otherIndex);
      return (v1 == v2) || (std::isnan(v1) && std::isnan(v2));
    } else {
      return valueAt(index) == simpleVector->valueAt(otherIndex);
    }
  }

  int32_t compare(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex,
      CompareFlags flags) const override {
    other = other->loadedVector();
    DCHECK(dynamic_cast<const SimpleVector<T>*>(other) != nullptr)
        << "Attempting to compare vectors not of the same type";
    bool otherNull = other->isNullAt(otherIndex);
    if (isNullAt(index)) {
      if (otherNull) {
        return 0;
      }
      return flags.nullsFirst ? -1 : 1;
    }
    if (otherNull) {
      return flags.nullsFirst ? 1 : -1;
    }
    auto simpleVector = reinterpret_cast<const SimpleVector<T>*>(other);
    auto thisValue = valueAt(index);
    auto otherValue = simpleVector->valueAt(otherIndex);
    return thisValue == otherValue ? 0 : thisValue < otherValue ? -1 : 1;
  }

  /**
   * @return the hash of the value at the given index in this vector
   */
  uint64_t hashValueAt(vector_size_t index) const override {
    return isNullAt(index) ? BaseVector::kNullHash
                           : folly::hasher<T>{}(valueAt(index));
  }

  folly::Optional<bool> isSorted() const {
    return isSorted_;
  }

  const folly::Optional<T>& getMin() const {
    return min_;
  }

  const folly::Optional<T>& getMax() const {
    return max_;
  }

  /// Enabled for string vectors, returns the string encoding mode
  template <typename U = T>
  typename std::enable_if<
      std::is_same<U, StringView>::value,
      folly::Optional<functions::stringCore::StringEncodingMode>>::type
  getStringEncoding() const {
    return encodingMode_;
  }

  /// Enabled for string vectors, sets the string encoding.
  template <typename U = T>
  typename std::enable_if<std::is_same<U, StringView>::value, void>::type
  setStringEncoding(functions::stringCore::StringEncodingMode mode) {
    encodingMode_ = mode;
  }

  /// Enabled for string vectors, sets the string encoding to match another
  /// vector encoding
  template <typename U = T>
  typename std::enable_if<std::is_same<U, StringView>::value, void>::type
  copyStringEncodingFrom(const BaseVector* vector) {
    encodingMode_ =
        vector->asUnchecked<SimpleVector<StringView>>()->getStringEncoding();
  }

  /// Invalidate string encoding
  template <typename U = T>
  typename std::enable_if<std::is_same<U, StringView>::value, void>::type
  invalidateStringEncoding() {
    encodingMode_ = folly::none;
  }

  void resize(vector_size_t size) override {
    VELOX_CHECK(false, "Can only resize flat vectors.");
  }

  virtual vector_size_t elementSize() {
    return elementSize_;
  }

  bool mayAddNulls() const override {
    return false;
  }

  std::string toString(vector_size_t index) const override {
    std::stringstream out;
    if (isNullAt(index)) {
      out << "null";
    } else {
      if constexpr (std::is_same<T, std::shared_ptr<void>>::value) {
        VELOX_NYI("Can't serialize opaque objects yet");
      } else {
        out << velox::to<std::string>(valueAt(index));
      }
    }
    return out.str();
  }

 protected:
  /**
   * @return the value of the specified key from the given map, if present
   */
  template <typename V>
  folly::Optional<V> getMetaDataValue(
      const folly::F14FastMap<std::string, std::string>& metaData,
      const std::string& key) {
    const auto& value = metaData.find(key);
    return value == metaData.end()
        ? folly::none
        : folly::Optional<V>(velox::to<V>(value->second));
  }

  // Throws if the elementSize_ does not match sizeof(T) or if T is ComplexType.
  // Use for debug mode safety checking of scalar element access.
  inline void checkElementSize() const {
    VELOX_DCHECK(
        (!std::is_same_v<T, ComplexType>),
        "Using a complex type vector as scalar");
    VELOX_DCHECK(
        elementSize_ == sizeof(T),
        "Vector created with element size {} and used with size {}",
        elementSize_,
        sizeof(T));
  }

  folly::Optional<T> min_;
  folly::Optional<T> max_;
  // Holds the data for StringView min/max.
  std::string minString_;
  std::string maxString_;

 private:
  void setMinMax(const folly::F14FastMap<std::string, std::string>& metaData) {
    min_ = getMetaDataValue<T>(metaData, META_MIN);
    max_ = getMetaDataValue<T>(metaData, META_MAX);
  }

 protected:
  folly::Optional<bool> isSorted_ = folly::none;

  // Allows checking that access is with the same width of T as
  // construction. For example, casting FlatVector<uint8_t> to
  // FlatVector<StringView> makes buffer overruns.
  const uint8_t elementSize_;

  // If T is velox::StringView, specifies the string encoding mode
  folly::Optional<functions::stringCore::StringEncodingMode> encodingMode_ =
      folly::none;
}; // namespace velox

template <>
void SimpleVector<StringView>::setMinMax(
    const folly::F14FastMap<std::string, std::string>& metaData);

template <>
inline void SimpleVector<ComplexType>::setMinMax(
    const folly::F14FastMap<std::string, std::string>& /*metaData*/) {}

template <>
inline void SimpleVector<std::shared_ptr<void>>::setMinMax(
    const folly::F14FastMap<std::string, std::string>& /*metaData*/) {}

template <>
inline bool SimpleVector<ComplexType>::equalValueAt(
    const BaseVector* other,
    vector_size_t index,
    vector_size_t otherIndex) const {
  auto otherNull = other->isNullAt(otherIndex);
  if (isNullAt(index)) {
    return otherNull;
  } else if (otherNull) {
    return false;
  }
  return wrappedVector()->equalValueAt(
      other->wrappedVector(),
      wrappedIndex(index),
      other->wrappedIndex(otherIndex));
}

template <>
inline int32_t SimpleVector<ComplexType>::compare(
    const BaseVector* other,
    vector_size_t index,
    vector_size_t otherIndex,
    CompareFlags flags) const {
  other = other->loadedVector();
  auto wrapped = wrappedVector();
  auto otherWrapped = other->wrappedVector();
  DCHECK(wrapped->encoding() == otherWrapped->encoding())
      << "Attempting to compare vectors not of the same type";
  auto thisWrappedIndex = wrappedIndex(index);
  auto otherWrappedIndex = other->wrappedIndex(otherIndex);
  bool otherNull = otherWrapped->isNullAt(otherWrappedIndex);
  if (wrapped->isNullAt(thisWrappedIndex)) {
    if (otherNull) {
      return 0;
    }
    return flags.nullsFirst ? -1 : 1;
  }
  if (otherNull) {
    return flags.nullsFirst ? 1 : -1;
  }
  return wrapped->compare(
      otherWrapped, thisWrappedIndex, otherWrappedIndex, flags);
}

template <>
inline uint64_t SimpleVector<ComplexType>::hashValueAt(
    vector_size_t index) const {
  if (isNullAt(index)) {
    return BaseVector::kNullHash;
  }
  return wrappedVector()->hashValueAt(wrappedIndex(index));
}

template <typename T>
using SimpleVectorPtr = std::shared_ptr<SimpleVector<T>>;

} // namespace velox
} // namespace facebook
