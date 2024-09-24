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
#include <folly/hash/Hash.h>

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/vector/BuilderTypeUtils.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook {
namespace velox {

// Here are some common intel intrsic operations. Please refer to
// https://software.intel.com/sites/landingpage/IntrinsicsGuide for examples.

// _mm256_set1_epi<64x|32|16|8>(x) => set a 256bit vector with all values of x
//    at the requested bit width
// _mm256_cmpeq_epi<64|32|16|8>(a, b) => compare a vector of 8, 16, 32 or 64bit
//    values in a vector with another vector. Result is a vector of all 0xFF..
//    if the slot is equal between the two vectors or 0x00... if the slot is not
//    equal between the two vectors
// _mm256_cmpgt_epi<64|32|16|8>(a, b) => compare a vector of 8, 16, 32 or 64bit
//    values in a vector with another vector. Result is a vector of all 0xFF..
//    if the slot in `a` is greater than the slot in `b` or 0x00... otherwise
// _mm256_loadu_si256(addr) => load 256 bits at addr into a single 256b
// _mm256_movemask_ps(mask) -> Set each bit of mask dst based on the
//    most significant bit of the corresponding packed single-precision (32-bit)
//    floating-point element in a.
// _mm256_testc_si256 => Compute bitwise AND of 2 256 bit vectors (see comment
// blocks below for examples)

// uses the simd utilities to smooth out access to variable width intrinsics

// cost factors for individual operations on different filter paths - these are
// experimentally derived from micro-bench perf testing.
const double SIMD_CMP_COST = 0.00000051;
const double SET_CMP_COST = 0.000023;

template <typename T>
const T* FlatVector<T>::rawValues() const {
  return rawValues_;
}

template <typename T>
T FlatVector<T>::valueAtFast(vector_size_t idx) const {
  VELOX_DCHECK_LT(idx, BaseVector::length_, "Index out of range");
  return rawValues_[idx];
}

template <typename T>
Range<T> FlatVector<T>::asRange() const {
  return Range<T>(rawValues(), 0, BaseVector::length_);
}

template <typename T>
xsimd::batch<T> FlatVector<T>::loadSIMDValueBufferAt(size_t byteOffset) const {
  auto mem = reinterpret_cast<uint8_t*>(rawValues_) + byteOffset;
  if constexpr (std::is_same_v<T, bool>) {
    return xsimd::batch<T>(xsimd::load_unaligned(mem));
  } else {
    return xsimd::load_unaligned(reinterpret_cast<T*>(mem));
  }
}

template <typename T>
std::unique_ptr<SimpleVector<uint64_t>> FlatVector<T>::hashAll() const {
  BufferPtr hashBuffer =
      AlignedBuffer::allocate<uint64_t>(BaseVector::length_, BaseVector::pool_);
  auto hashData = hashBuffer->asMutable<uint64_t>();

  if (rawValues_ != nullptr) { // non all-null case
    folly::hasher<T> hasher;
    for (size_t i = 0; i < BaseVector::length_; ++i) {
      hashData[i] = hasher(valueAtFast(i));
    }
  }

  // overwrite the null hash values
  if (BaseVector::rawNulls_ != nullptr) {
    for (size_t i = 0; i < BaseVector::length_; ++i) {
      if (bits::isBitNull(BaseVector::rawNulls_, i)) {
        hashData[i] = BaseVector::kNullHash;
      }
    }
  }
  return std::make_unique<FlatVector<uint64_t>>(
      BaseVector::pool_,
      BIGINT(),
      BufferPtr(nullptr),
      BaseVector::length_,
      std::move(hashBuffer),
      std::vector<BufferPtr>() /*stringBuffers*/,
      SimpleVectorStats<uint64_t>{}, /* stats */
      std::nullopt /*distinctValueCount*/,
      0 /*nullCount*/,
      false /*sorted*/,
      sizeof(uint64_t) * BaseVector::length_ /*representedBytes*/);
}

template <typename T>
bool FlatVector<T>::useSimdEquality(size_t numCmpVals) const {
  if constexpr (!std::is_integral_v<T>) {
    return false;
  } else {
    // Uses a cost estimate for a SIMD comparison of a single comparison
    // value vs. that of doing the fallback set lookup to determine
    // whether or not to pursue the SIMD path or the fallback path.
    auto fallbackCost = SET_CMP_COST * BaseVector::length_;
    auto simdCost = SIMD_CMP_COST * numCmpVals * BaseVector::length_ /
        xsimd::batch<T>::size;
    return simdCost <= fallbackCost;
  }
}

template <typename T>
void FlatVector<T>::copyValuesAndNulls(
    const BaseVector* source,
    const SelectivityVector& rows,
    const vector_size_t* toSourceRow) {
  if (source->typeKind() == TypeKind::UNKNOWN) {
    auto* rawNulls = BaseVector::mutableRawNulls();
    rows.setNulls(rawNulls);
    return;
  }

  source = source->loadedVector();
  VELOX_CHECK_EQ(BaseVector::typeKind(), source->typeKind());
  VELOX_CHECK_GE(BaseVector::length_, rows.end());
  if (!toSourceRow) {
    VELOX_CHECK_GE(source->size(), rows.end());
  }

  uint64_t* rawNulls = const_cast<uint64_t*>(BaseVector::rawNulls_);
  if (source->mayHaveNulls()) {
    rawNulls = BaseVector::mutableRawNulls();
  }

  // Allocate values buffer if not allocated yet. This may happen if vector
  // contains only null values.
  if (!values_) {
    mutableRawValues();
  }

  if (source->isFlatEncoding()) {
    auto* flatSource = source->asUnchecked<FlatVector<T>>();
    if (flatSource->values() == nullptr) {
      // All source values are null.
      rows.setNulls(rawNulls);
      return;
    }

    if constexpr (std::is_same_v<T, bool>) {
      auto* rawValues = reinterpret_cast<uint64_t*>(rawValues_);
      auto* sourceValues = flatSource->template rawValues<uint64_t>();
      if (toSourceRow) {
        rows.applyToSelected([&](auto row) {
          auto sourceRow = toSourceRow[row];
          VELOX_DCHECK_GT(source->size(), sourceRow);
          bits::setBit(rawValues, row, bits::isBitSet(sourceValues, sourceRow));
        });
      } else {
        const auto numBits = rows.countSelected();
        if (numBits == rows.end() - rows.begin()) {
          // Fast path for copying contiguous range of bits.
          bits::copyBits(
              sourceValues, rows.begin(), rawValues, rows.begin(), numBits);
        } else {
          rows.applyToSelected([&](auto row) {
            bits::setBit(rawValues, row, bits::isBitSet(sourceValues, row));
          });
        }
      }
    } else {
      auto* sourceValues = flatSource->rawValues();
      if (toSourceRow) {
        rows.applyToSelected([&](auto row) {
          auto sourceRow = toSourceRow[row];
          VELOX_DCHECK_GT(source->size(), sourceRow);
          rawValues_[row] = sourceValues[sourceRow];
        });
      } else {
        rows.applyToSelected(
            [&](auto row) { rawValues_[row] = sourceValues[row]; });
      }
    }

    if (rawNulls) {
      const uint64_t* sourceNulls = source->rawNulls();

      if (!sourceNulls) {
        rows.clearNulls(rawNulls);
      } else {
        if (toSourceRow) {
          rows.applyToSelected([&](auto row) {
            auto sourceRow = toSourceRow[row];
            VELOX_DCHECK_GT(source->size(), sourceRow);
            bits::setNull(
                rawNulls, row, bits::isBitNull(sourceNulls, sourceRow));
          });
        } else {
          rows.copyNulls(rawNulls, sourceNulls);
        }
      }
    }
  } else if (source->isConstantEncoding()) {
    if (source->isNullAt(0)) {
      BaseVector::addNulls(rows);
      return;
    }
    auto constant = source->asUnchecked<ConstantVector<T>>();
    T value = constant->valueAt(0);
    if constexpr (std::is_same_v<T, bool>) {
      auto range = rows.asRange();
      auto* rawValues = reinterpret_cast<uint64_t*>(rawValues_);
      if (value) {
        bits::orBits(rawValues, range.bits(), range.begin(), range.end());
      } else {
        bits::andWithNegatedBits(
            rawValues, range.bits(), range.begin(), range.end());
      }
    } else {
      rows.applyToSelected([&](int32_t row) { rawValues_[row] = value; });
    }

    rows.clearNulls(rawNulls);
  } else {
    DecodedVector decoded(*source);
    if (toSourceRow == nullptr) {
      rows.applyToSelected([&](auto row) {
        if (!decoded.isNullAt(row)) {
          if constexpr (std::is_same_v<T, bool>) {
            auto* rawValues = reinterpret_cast<uint64_t*>(rawValues_);
            bits::setBit(rawValues, row, decoded.valueAt<T>(row));
          } else {
            rawValues_[row] = decoded.valueAt<T>(row);
          }
        }
      });

      if (rawNulls != nullptr) {
        auto* sourceNulls = decoded.nulls();
        if (sourceNulls == nullptr) {
          rows.clearNulls(rawNulls);
        } else {
          rows.copyNulls(rawNulls, sourceNulls);
        }
      }

    } else {
      rows.applyToSelected([&](auto row) {
        const auto sourceRow = toSourceRow[row];
        VELOX_DCHECK_GT(source->size(), sourceRow);
        if (!decoded.isNullAt(sourceRow)) {
          if constexpr (std::is_same_v<T, bool>) {
            auto* rawValues = reinterpret_cast<uint64_t*>(rawValues_);
            bits::setBit(rawValues, row, decoded.valueAt<T>(sourceRow));
          } else {
            rawValues_[row] = decoded.valueAt<T>(sourceRow);
          }

          if (rawNulls) {
            bits::clearNull(rawNulls, row);
          }
        } else {
          bits::setNull(rawNulls, row);
        }
      });
    }
  }
}

template <typename T>
void FlatVector<T>::copyRanges(
    const BaseVector* source,
    const folly::Range<const BaseVector::CopyRange*>& ranges) {
  if (source->typeKind() == TypeKind::UNKNOWN) {
    BaseVector::setNulls(BaseVector::mutableRawNulls(), ranges, true);
    return;
  }

  source = source->loadedVector();
  VELOX_CHECK_EQ(BaseVector::typeKind(), source->typeKind());

  if constexpr (std::is_same_v<T, StringView>) {
    auto leaf =
        source->wrappedVector()->asUnchecked<SimpleVector<StringView>>();
    if (BaseVector::pool_ != leaf->pool()) {
      applyToEachRow(ranges, [&](auto targetIndex, auto sourceIndex) {
        if (source->isNullAt(sourceIndex)) {
          this->setNull(targetIndex, true);
        } else {
          this->set(
              targetIndex, leaf->valueAt(source->wrappedIndex(sourceIndex)));
        }
      });
      return;
    }

    // We copy referencing the storage of 'source'.
    acquireSharedStringBuffers(source);
  }

  const uint64_t* sourceRawNulls = source->rawNulls();
  uint64_t* rawNulls = const_cast<uint64_t*>(BaseVector::rawNulls_);
  if (source->mayHaveNulls()) {
    rawNulls = BaseVector::mutableRawNulls();
  }

  // Allocate values buffer if not allocated yet. This may happen if vector
  // contains only null values.
  if (!values_) {
    mutableRawValues();
  }

  if (source->isFlatEncoding()) {
    auto* flatSource = source->asUnchecked<FlatVector<T>>();
    if (flatSource->values() == nullptr) {
      // All source values are null.
      BaseVector::setNulls(BaseVector::mutableRawNulls(), ranges, true);
      return;
    }

    if constexpr (std::is_same_v<T, bool>) {
      auto rawValues = reinterpret_cast<uint64_t*>(rawValues_);
      auto* sourceValues = flatSource->template rawValues<uint64_t>();
      applyToEachRange(
          ranges, [&](auto targetIndex, auto sourceIndex, auto count) {
            bits::copyBits(
                sourceValues, sourceIndex, rawValues, targetIndex, count);
          });
    } else {
      const T* sourceValues = flatSource->rawValues();
      applyToEachRange(
          ranges, [&](auto targetIndex, auto sourceIndex, auto count) {
            if (Buffer::is_pod_like_v<T>) {
              memcpy(
                  &rawValues_[targetIndex],
                  &sourceValues[sourceIndex],
                  count * sizeof(T));
            } else {
              std::copy(
                  sourceValues + sourceIndex,
                  sourceValues + sourceIndex + count,
                  rawValues_ + targetIndex);
            }
          });
    }

    if (rawNulls) {
      if (sourceRawNulls) {
        BaseVector::copyNulls(rawNulls, sourceRawNulls, ranges);
      } else {
        BaseVector::setNulls(rawNulls, ranges, false);
      }
    }
  } else if (source->isConstantEncoding()) {
    if (source->isNullAt(0)) {
      BaseVector::setNulls(rawNulls, ranges, true);
      return;
    }
    auto constant = source->asUnchecked<ConstantVector<T>>();
    T value = constant->valueAt(0);
    if constexpr (std::is_same_v<T, bool>) {
      auto rawValues = reinterpret_cast<uint64_t*>(rawValues_);
      applyToEachRange(
          ranges, [&](auto targetIndex, auto /*sourceIndex*/, auto count) {
            bits::fillBits(rawValues, targetIndex, targetIndex + count, value);
          });
    } else {
      applyToEachRow(ranges, [&](auto targetIndex, auto /*sourceIndex*/) {
        rawValues_[targetIndex] = value;
      });
    }

    if (rawNulls) {
      BaseVector::setNulls(rawNulls, ranges, false);
    }
  } else {
    auto* sourceVector = source->asUnchecked<SimpleVector<T>>();
    uint64_t* rawBoolValues = nullptr;
    if constexpr (std::is_same_v<T, bool>) {
      rawBoolValues = reinterpret_cast<uint64_t*>(rawValues_);
    }
    applyToEachRow(ranges, [&](auto targetIndex, auto sourceIndex) {
      if (!source->isNullAt(sourceIndex)) {
        auto sourceValue = sourceVector->valueAt(sourceIndex);
        if constexpr (std::is_same_v<T, bool>) {
          bits::setBit(rawBoolValues, targetIndex, sourceValue);
        } else {
          rawValues_[targetIndex] = sourceValue;
        }
        if (rawNulls) {
          bits::clearNull(rawNulls, targetIndex);
        }
      } else {
        bits::setNull(rawNulls, targetIndex);
      }
    });
  }
}

template <typename T>
VectorPtr FlatVector<T>::slice(vector_size_t offset, vector_size_t length)
    const {
  return std::make_shared<FlatVector<T>>(
      this->pool_,
      this->type_,
      this->sliceNulls(offset, length),
      length,
      values_ ? Buffer::slice<T>(values_, offset, length, this->pool_)
              : values_,
      std::vector<BufferPtr>(stringBuffers_));
}

template <typename T>
void FlatVector<T>::resize(vector_size_t newSize, bool setNotNull) {
  const vector_size_t previousSize = BaseVector::length_;
  if (newSize == previousSize) {
    return;
  }
  BaseVector::resize(newSize, setNotNull);
  if (!values_) {
    return;
  }

  if constexpr (std::is_same_v<T, StringView>) {
    resizeValues(newSize, StringView());
    if (newSize < previousSize) {
      // If we downsize, just invalidate ascii, because we might have become
      // 'all ascii' from 'not all ascii'.
      SimpleVector<StringView>::invalidateIsAscii();
    } else {
      // Properly init stringView objects. This is useful when vectors are
      // re-used where the size changes but not the capacity.
      // TODO: remove this when resizeValues() checks against size() instead of
      // capacity() when deciding to init values.
      auto stringViews = reinterpret_cast<StringView*>(rawValues_);
      for (auto index = previousSize; index < newSize; ++index) {
        new (&stringViews[index]) StringView();
      }
      SimpleVector<StringView>::resizeIsAsciiIfNotEmpty(newSize, false);
    }
    if (newSize == 0) {
      keepAtMostOneStringBuffer();
    }
  } else {
    resizeValues(newSize, std::nullopt);
  }
}

template <typename T>
void FlatVector<T>::ensureWritable(const SelectivityVector& rows) {
  auto newSize = std::max<vector_size_t>(rows.end(), BaseVector::length_);
  if (values_ && !values_->isMutable()) {
    BufferPtr newValues;
    if constexpr (std::is_same_v<T, StringView>) {
      // Make sure to initialize StringView values so they can be safely
      // accessed.
      newValues = AlignedBuffer::allocate<T>(newSize, BaseVector::pool_, T());
    } else {
      newValues = AlignedBuffer::allocate<T>(newSize, BaseVector::pool_);
    }

    if constexpr (std::is_same_v<T, bool>) {
      auto rawNewValues = newValues->asMutable<uint64_t>();
      std::memcpy(
          rawNewValues,
          rawValues_,
          std::min(values_->size(), newValues->size()));
    } else {
      auto rawNewValues = newValues->asMutable<T>();
      SelectivityVector rowsToCopy(BaseVector::length_);
      rowsToCopy.deselect(rows);
      rowsToCopy.applyToSelected(
          [&](vector_size_t row) { rawNewValues[row] = rawValues_[row]; });
    }

    // Keep the string buffers even if multiply referenced. These are
    // append-only and are written to in FlatVector::set which calls
    // getBufferWithSpace which allocates a new buffer if existing buffers
    // are multiply-referenced.

    // TODO Optimization: check and remove string buffers not referenced by
    // rowsToCopy

    values_ = std::move(newValues);
    rawValues_ = values_->asMutable<T>();
  }

  BaseVector::ensureWritable(rows);
}

template <typename T>
void FlatVector<T>::prepareForReuse() {
  BaseVector::prepareForReuse();

  // Check values buffer. Keep the buffer if singly-referenced and mutable.
  // Reset otherwise.
  if (values_ && !values_->isMutable()) {
    values_ = nullptr;
    rawValues_ = nullptr;
  }
}

template <typename T>
void FlatVector<T>::resizeValues(
    vector_size_t newSize,
    const std::optional<T>& initialValue) {
  // TODO: change this to isMutable(). See
  // https://github.com/facebookincubator/velox/issues/6562.
  if (values_ && !values_->isView()) {
    const uint64_t newByteSize = BaseVector::byteSize<T>(newSize);
    if (values_->capacity() < newByteSize) {
      AlignedBuffer::reallocate<T>(&values_, newSize, initialValue);
    } else {
      values_->setSize(newByteSize);
    }
    rawValues_ = values_->asMutable<T>();
    return;
  }
  BufferPtr newValues =
      AlignedBuffer::allocate<T>(newSize, BaseVector::pool_, initialValue);

  if (values_) {
    if constexpr (Buffer::is_pod_like_v<T>) {
      auto dst = newValues->asMutable<T>();
      auto src = values_->as<T>();
      auto len = std::min(values_->size(), newValues->size());
      memcpy(dst, src, len);
    } else {
      const vector_size_t previousSize = BaseVector::length_;
      auto* rawOldValues = newValues->asMutable<T>();
      auto* rawNewValues = newValues->asMutable<T>();
      const auto len = std::min<vector_size_t>(newSize, previousSize);
      for (vector_size_t row = 0; row < len; ++row) {
        rawNewValues[row] = rawOldValues[row];
      }
    }
  }
  values_ = std::move(newValues);
  rawValues_ = values_->asMutable<T>();
}

template <>
inline void FlatVector<bool>::resizeValues(
    vector_size_t newSize,
    const std::optional<bool>& initialValue) {
  // TODO: change this to isMutable(). See
  // https://github.com/facebookincubator/velox/issues/6562.
  if (values_ && !values_->isView()) {
    const uint64_t newByteSize = BaseVector::byteSize<bool>(newSize);
    if (values_->size() < newByteSize) {
      AlignedBuffer::reallocate<bool>(&values_, newSize, initialValue);
    } else {
      values_->setSize(newByteSize);
    }
    // ensure that the newly added positions have the right initial value for
    // the case where changes in size don't result in change in the size of
    // the underlying buffer.
    if (initialValue.has_value() && length_ < newSize) {
      auto rawData = values_->asMutable<uint64_t>();
      bits::fillBits(rawData, length_, newSize, initialValue.value());
    }
    rawValues_ = values_->asMutable<bool>();
    return;
  }
  BufferPtr newValues =
      AlignedBuffer::allocate<bool>(newSize, BaseVector::pool_, initialValue);

  if (values_) {
    auto dst = newValues->asMutable<char>();
    auto src = values_->as<char>();
    auto len = std::min(values_->size(), newValues->size());
    memcpy(dst, src, len);
  }
  values_ = std::move(newValues);
  rawValues_ = values_->asMutable<bool>();
}
} // namespace velox
} // namespace facebook
