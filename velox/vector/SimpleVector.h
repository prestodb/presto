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
      std::optional<vector_size_t> distinctValueCount,
      std::optional<vector_size_t> nullCount,
      std::optional<bool> isSorted,
      std::optional<ByteCount> representedByteCount,
      std::optional<ByteCount> storageByteCount = std::nullopt)
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
      std::optional<vector_size_t> distinctValueCount,
      std::optional<vector_size_t> nullCount,
      std::optional<bool> isSorted,
      std::optional<ByteCount> representedByteCount,
      std::optional<ByteCount> storageByteCount = std::nullopt)
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
    auto result = comparePrimitiveAsc(thisValue, otherValue);
    return flags.ascending ? result : result * -1;
  }

  /**
   * @return the hash of the value at the given index in this vector
   */
  uint64_t hashValueAt(vector_size_t index) const override {
    return isNullAt(index) ? BaseVector::kNullHash
                           : folly::hasher<T>{}(valueAt(index));
  }

  std::optional<bool> isSorted() const {
    return isSorted_;
  }

  const std::optional<T>& getMin() const {
    return min_;
  }

  const std::optional<T>& getMax() const {
    return max_;
  }

  void resize(vector_size_t size, bool setNotNull = true) override {
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

  /// This function takes a SelectivityVector and a mapping from the index's in
  /// the SelectivityVector to corresponding indexes in this vector. Then we
  /// return:
  /// 1. True if all specified rows after the translation are known to be ASCII.
  /// 2. False if all specified rows after translation contain atleast one non
  ///    ASCII character.
  /// 3. std::nullopt if ASCII-ness is not known for even one of the translated
  /// rows. If rowMappings is null then we revert to indexes in the
  /// SelectivityVector.
  template <typename U = T>
  typename std::
      enable_if<std::is_same<U, StringView>::value, std::optional<bool>>::type
      isAscii(
          const SelectivityVector& rows,
          const vector_size_t* rowMappings = nullptr) const {
    VELOX_CHECK(rows.hasSelections())
    if (asciiSetRows_.hasSelections()) {
      if (rowMappings) {
        bool isSubset = rows.template testSelected(
            [&](auto row) { return asciiSetRows_.isValid(rowMappings[row]); });
        return isSubset ? std::optional(isAllAscii_) : std::nullopt;
      }
      if (rows.isSubset(asciiSetRows_)) {
        return isAllAscii_;
      }
    }
    return std::nullopt;
  }

  /// This function takes an index and returns:
  /// 1. True if the string at that index is ASCII
  /// 2. False if the string at that index is not ASCII
  /// 3. std::nullopt if we havent computed ASCII'ness at that index.
  template <typename U = T>
  typename std::
      enable_if<std::is_same<U, StringView>::value, std::optional<bool>>::type
      isAscii(vector_size_t index) const {
    VELOX_CHECK_GE(index, 0)
    if (asciiSetRows_.size() > index && asciiSetRows_.isValid(index)) {
      return isAllAscii_;
    }
    return std::nullopt;
  }

  /// Computes and saves is-ascii flag for a given set of rows if not already
  /// present. Returns computed value.
  template <typename U = T>
  typename std::enable_if<std::is_same<U, StringView>::value, bool>::type
  computeAndSetIsAscii(const SelectivityVector& rows) {
    if (rows.isSubset(asciiSetRows_)) {
      return isAllAscii_;
    }
    ensureIsAsciiCapacity(rows.end());
    bool isAllAscii = true;
    rows.template applyToSelected([&](auto row) {
      if (!isNullAt(row)) {
        auto string = valueAt(row);
        isAllAscii &=
            functions::stringCore::isAscii(string.data(), string.size());
      }
    });

    // Set isAllAscii flag, it will unset if we encounter any utf.
    if (!asciiSetRows_.hasSelections()) {
      isAllAscii_ = isAllAscii;
    } else {
      isAllAscii_ &= isAllAscii;
    }

    asciiSetRows_.select(rows);
    return isAllAscii_;
  }

  /// Clears asciiness state.
  template <typename U = T>
  typename std::enable_if<std::is_same<U, StringView>::value, void>::type
  invalidateIsAscii() {
    asciiSetRows_.clearAll();
    isAllAscii_ = false;
  }

  /// Explicitly set asciness.
  template <typename U = T>
  typename std::enable_if<std::is_same<U, StringView>::value, void>::type
  setIsAscii(bool ascii, const SelectivityVector& rows) {
    ensureIsAsciiCapacity(rows.end());
    if (asciiSetRows_.hasSelections() && !asciiSetRows_.isSubset(rows)) {
      isAllAscii_ &= ascii;
    } else {
      isAllAscii_ = ascii;
    }

    asciiSetRows_.select(rows);
  }

  template <typename U = T>
  typename std::enable_if<std::is_same<U, StringView>::value, void>::type
  setAllIsAscii(bool ascii) {
    ensureIsAsciiCapacity(length_);
    isAllAscii_ = ascii;
    asciiSetRows_.setAll();
  }

 protected:
  template <typename U = T>
  typename std::enable_if<std::is_same<U, StringView>::value, void>::type
  ensureIsAsciiCapacity(vector_size_t size) {
    if (asciiSetRows_.size() < size) {
      asciiSetRows_.resize(size, false);
    }
  }

  /**
   * @return the value of the specified key from the given map, if present
   */
  template <typename V>
  std::optional<V> getMetaDataValue(
      const folly::F14FastMap<std::string, std::string>& metaData,
      const std::string& key) {
    const auto& value = metaData.find(key);
    return value == metaData.end()
        ? std::nullopt
        : std::optional<V>(velox::to<V>(value->second));
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

  std::optional<T> min_;
  std::optional<T> max_;
  // Holds the data for StringView min/max.
  std::string minString_;
  std::string maxString_;

 private:
  void setMinMax(const folly::F14FastMap<std::string, std::string>& metaData) {
    min_ = getMetaDataValue<T>(metaData, META_MIN);
    max_ = getMetaDataValue<T>(metaData, META_MAX);
  }

 protected:
  int comparePrimitiveAsc(const T& left, const T& right) const {
    if constexpr (std::is_floating_point<T>::value) {
      bool isLeftNan = std::isnan(left);
      bool isRightNan = std::isnan(right);
      if (isLeftNan) {
        return isRightNan ? 0 : 1;
      }
      if (isRightNan) {
        return -1;
      }
    }
    return left < right ? -1 : left == right ? 0 : 1;
  }

  std::optional<bool> isSorted_ = std::nullopt;

  // Allows checking that access is with the same width of T as
  // construction. For example, casting FlatVector<uint8_t> to
  // FlatVector<StringView> makes buffer overruns.
  const uint8_t elementSize_;

  // True is all strings in asciiSetRows_ are ASCII.
  bool isAllAscii_{false};

  // If T is StringView, store set of rows
  // where we have computed asciiness. A set bit means the row was processed.
  SelectivityVector asciiSetRows_;
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

  auto otherWrappedIndex = other->wrappedIndex(otherIndex);
  auto thisWrappedIndex = wrappedIndex(index);
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
