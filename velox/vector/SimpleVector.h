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
#include <optional>
#include <type_traits>

#include <folly/FixedString.h>
#include <folly/String.h>
#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>
#include <folly/hash/Hash.h>
#include <glog/logging.h>

#include "velox/functions/lib/string/StringCore.h"
#include "velox/type/DecimalUtil.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook {
namespace velox {

namespace exec {
class EvalCtx;
}

template <typename T>
struct SimpleVectorStats {
  std::optional<T> min;
  std::optional<T> max;
};

struct AsciiInfo {
  /// Returns true if ascii was processed for all rows and false otherwise.
  bool isAllAscii() const {
    return isAllAscii_;
  }

  /// Sets isAllAscii boolean flag.
  void setIsAllAscii(bool f) {
    isAllAscii_ = f;
  }

  /// Returns locked for read bit vector with bits set for rows where ascii was
  /// processed.
  auto readLockedAsciiComputedRows() const {
    return asciiComputedRows_.rlock();
  }

  /// Returns locked for write bit vector with bits set for rows where ascii was
  /// processed.
  auto writeLockedAsciiComputedRows() {
    return asciiComputedRows_.wlock();
  }

  /// Returns upgradable locked bit vector with bits set for rows where ascii
  /// was processed.
  auto upgradableLockedAsciiComputedRows() {
    return asciiComputedRows_.ulock();
  }

 private:
  // isAllAscii_ and asciiComputedRows_ are thread-safe because input vectors
  // can be shared across threads, hence this make their mutation thread safe.
  // Those are the only two fields that are allowed to be mutated in the
  // expression eval inputs vectors.

  // True is all strings in asciiComputedRows_ are ASCII.
  std::atomic_bool isAllAscii_{false};

  // If T is StringView, store set of rows where we have computed asciiness.
  // A set bit means the row was processed.
  folly::Synchronized<SelectivityVector> asciiComputedRows_;
};

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
  SimpleVector(const SimpleVector&) = delete;
  SimpleVector& operator=(const SimpleVector&) = delete;

  SimpleVector(
      velox::memory::MemoryPool* pool,
      TypePtr type,
      VectorEncoding::Simple encoding,
      BufferPtr nulls,
      size_t length,
      const SimpleVectorStats<T>& stats,
      std::optional<vector_size_t> distinctValueCount,
      std::optional<vector_size_t> nullCount,
      std::optional<bool> isSorted,
      std::optional<ByteCount> representedByteCount,
      std::optional<ByteCount> storageByteCount = std::nullopt)
      : BaseVector(
            pool,
            std::move(type),
            encoding,
            std::move(nulls),
            length,
            distinctValueCount,
            nullCount,
            representedByteCount,
            storageByteCount),
        isSorted_(isSorted),
        elementSize_(sizeof(T)),
        stats_(stats) {}

  virtual ~SimpleVector() override {}

  SimpleVectorStats<T> getStats() const {
    return stats_;
  }

  // Concrete Vector types need to implement this themselves.
  // This method does not do bounds checking. When the value is null the return
  // value is technically undefined (currently implemented as default of T)
  virtual const T valueAt(vector_size_t idx) const = 0;

  std::optional<int32_t> compare(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex,
      CompareFlags flags) const override {
    other = other->loadedVector();
    DCHECK(dynamic_cast<const SimpleVector<T>*>(other) != nullptr)
        << "Attempting to compare vectors not of the same type";
    bool otherNull = other->isNullAt(otherIndex);
    bool thisNull = isNullAt(index);

    if (otherNull || thisNull) {
      return BaseVector::compareNulls(thisNull, otherNull, flags);
    }

    auto simpleVector = reinterpret_cast<const SimpleVector<T>*>(other);
    auto thisValue = valueAt(index);
    auto otherValue = simpleVector->valueAt(otherIndex);
    auto result = comparePrimitiveAsc(thisValue, otherValue);
    return {flags.ascending ? result : result * -1};
  }

  void validate(const VectorValidateOptions& options) const override {
    BaseVector::validate(options);
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
    return stats_.min;
  }

  const std::optional<T>& getMax() const {
    return stats_.max;
  }

  void resize(vector_size_t size, bool setNotNull = true) override {
    VELOX_CHECK(false, "Can only resize flat vectors.");
  }

  virtual vector_size_t elementSize() {
    return elementSize_;
  }

  using BaseVector::toString;

  std::string valueToString(T value) const {
    if constexpr (std::is_same_v<T, bool>) {
      return value ? "true" : "false";
    } else if constexpr (std::is_same_v<T, std::shared_ptr<void>>) {
      return "<opaque>";
    } else if constexpr (
        std::is_same_v<T, int64_t> || std::is_same_v<T, int128_t>) {
      if (type()->isDecimal()) {
        return DecimalUtil::toString(value, type());
      } else {
        return velox::to<std::string>(value);
      }
    } else {
      return velox::to<std::string>(value);
    }
  }

  std::string toString(vector_size_t index) const override {
    VELOX_CHECK_LT(index, length_, "Vector index should be less than length.");
    std::stringstream out;
    if (isNullAt(index)) {
      out << "null";
    } else {
      out << valueToString(valueAt(index));
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
  typename std::enable_if_t<std::is_same_v<U, StringView>, std::optional<bool>>
  isAscii(
      const SelectivityVector& rows,
      const vector_size_t* rowMappings = nullptr) const {
    VELOX_CHECK(rows.hasSelections())
    auto rlockedAsciiComputedRows{asciiInfo.readLockedAsciiComputedRows()};
    if (rlockedAsciiComputedRows->hasSelections()) {
      if (rowMappings) {
        bool isSubset = rows.template testSelected([&](auto row) {
          return rlockedAsciiComputedRows->isValid(rowMappings[row]);
        });
        return isSubset ? std::optional(asciiInfo.isAllAscii()) : std::nullopt;
      }
      if (rows.isSubset(*rlockedAsciiComputedRows)) {
        return asciiInfo.isAllAscii();
      }
    }
    return std::nullopt;
  }

  /// This function takes an index and returns:
  /// 1. True if the string at that index is ASCII
  /// 2. False if the string at that index is not ASCII
  /// 3. std::nullopt if we havent computed ASCII'ness at that index.
  template <typename U = T>
  typename std::enable_if_t<std::is_same_v<U, StringView>, std::optional<bool>>
  isAscii(vector_size_t index) const {
    VELOX_CHECK_GE(index, 0)
    auto rlockedAsciiComputedRows{asciiInfo.readLockedAsciiComputedRows()};
    if (index < rlockedAsciiComputedRows->size() &&
        rlockedAsciiComputedRows->isValid(index)) {
      return asciiInfo.isAllAscii();
    }
    return std::nullopt;
  }

  /// Computes and saves is-ascii flag for a given set of rows if not already
  /// present. Returns computed value.
  template <typename U = T>
  typename std::enable_if_t<std::is_same_v<U, StringView>, bool>
  computeAndSetIsAscii(const SelectivityVector& rows) {
    if (rows.isSubset(*asciiInfo.readLockedAsciiComputedRows())) {
      return asciiInfo.isAllAscii();
    }
    ensureIsAsciiCapacity();
    bool isAllAscii = true;
    rows.template applyToSelected([&](auto row) {
      if (!isNullAt(row)) {
        auto string = valueAt(row);
        isAllAscii &=
            functions::stringCore::isAscii(string.data(), string.size());
      }
    });

    // Set isAllAscii flag, it will unset if we encounter any utf.
    auto wlockedAsciiComputedRows = asciiInfo.writeLockedAsciiComputedRows();
    if (!wlockedAsciiComputedRows->hasSelections()) {
      asciiInfo.setIsAllAscii(isAllAscii);
    } else {
      asciiInfo.setIsAllAscii(asciiInfo.isAllAscii() & isAllAscii);
    }

    wlockedAsciiComputedRows->select(rows);
    return asciiInfo.isAllAscii();
  }

  /// Clears asciiness state.
  template <typename U = T>
  typename std::enable_if_t<std::is_same_v<U, StringView>, void>
  invalidateIsAscii() {
    asciiInfo.writeLockedAsciiComputedRows()->clearAll();
    asciiInfo.setIsAllAscii(false);
  }

  /// Explicitly set asciness.
  template <typename U = T>
  typename std::enable_if_t<std::is_same_v<U, StringView>, void> setIsAscii(
      bool ascii,
      const SelectivityVector& rows) {
    ensureIsAsciiCapacity();
    auto wlockedAsciiComputedRows = asciiInfo.writeLockedAsciiComputedRows();
    if (wlockedAsciiComputedRows->hasSelections() &&
        !wlockedAsciiComputedRows->isSubset(rows)) {
      asciiInfo.setIsAllAscii(asciiInfo.isAllAscii() & ascii);
    } else {
      asciiInfo.setIsAllAscii(ascii);
    }

    wlockedAsciiComputedRows->select(rows);
  }

  template <typename U = T>
  typename std::enable_if_t<std::is_same_v<U, StringView>, void> setAllIsAscii(
      bool ascii) {
    ensureIsAsciiCapacity();
    asciiInfo.setIsAllAscii(ascii);
    asciiInfo.writeLockedAsciiComputedRows()->setAll();
  }

  template <typename U = T>
  typename std::enable_if_t<std::is_same_v<U, StringView>, bool> getAllIsAscii()
      const {
    return asciiInfo.isAllAscii();
  }

  /// Provides const access to asciiInfo. Used for tests only.
  template <typename U = T>
  typename std::enable_if_t<std::is_same_v<U, StringView>, const AsciiInfo&>
  testGetAsciiInfo() const {
    return asciiInfo;
  }

  FOLLY_ALWAYS_INLINE static int comparePrimitiveAsc(
      const T& left,
      const T& right) {
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

 protected:
  template <typename U = T>
  typename std::enable_if_t<std::is_same_v<U, StringView>, void>
  ensureIsAsciiCapacity() {
    auto ulockedAsciiComputedRows{
        asciiInfo.upgradableLockedAsciiComputedRows()};
    if (ulockedAsciiComputedRows->size() < length_) {
      ulockedAsciiComputedRows.moveFromUpgradeToWrite()->resize(length_, false);
    }
  }

  /// Ensure asciiInfo is of the correct size. But only if it is not empty.
  template <typename U = T>
  typename std::enable_if_t<std::is_same_v<U, StringView>, void>
  resizeIsAsciiIfNotEmpty(vector_size_t size, bool newAscii) {
    auto ulockedAsciiComputedRows{
        asciiInfo.upgradableLockedAsciiComputedRows()};
    if (ulockedAsciiComputedRows->hasSelections()) {
      if (ulockedAsciiComputedRows->size() < size) {
        ulockedAsciiComputedRows.moveFromUpgradeToWrite()->resize(
            size, newAscii);
        asciiInfo.setIsAllAscii(asciiInfo.isAllAscii() & newAscii);
      }
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

  virtual void resetDataDependentFlags(const SelectivityVector* rows) override {
    BaseVector::resetDataDependentFlags(rows);
    isSorted_ = std::nullopt;
    stats_ = SimpleVectorStats<T>{};

    if constexpr (std::is_same_v<T, StringView>) {
      if (rows) {
        asciiInfo.writeLockedAsciiComputedRows()->deselect(*rows);
      } else {
        invalidateIsAscii();
      }
    }
  }

  std::optional<bool> isSorted_ = std::nullopt;

  // Allows checking that access is with the same width of T as
  // construction. For example, casting FlatVector<uint8_t> to
  // FlatVector<StringView> makes buffer overruns.
  const uint8_t elementSize_;

  std::conditional_t<std::is_same_v<T, StringView>, AsciiInfo, int> asciiInfo;
  SimpleVectorStats<T> stats_;
};

template <>
void SimpleVector<StringView>::validate(
    const VectorValidateOptions& options) const;

template <>
inline std::optional<int32_t> SimpleVector<ComplexType>::compare(
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
  bool isNull = isNullAt(index);
  if (isNull || otherNull) {
    return BaseVector::compareNulls(isNull, otherNull, flags);
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
