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

#include <velox/vector/BaseVector.h>
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/vector/BuilderTypeUtils.h"
#include "velox/vector/ConstantVector.h"
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
const T FlatVector<T>::valueAtFast(vector_size_t idx) const {
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
    if constexpr (std::is_same_v<T, StringView>) {
      folly::hasher<folly::StringPiece> stringHasher;
      for (size_t i = 0; i < BaseVector::length_; ++i) {
        auto view = valueAt(i);
        folly::StringPiece piece(view.data(), view.size());
        hashData[i] = stringHasher(piece);
      }
    } else {
      folly::hasher<T> hasher;
      for (size_t i = 0; i < BaseVector::length_; ++i) {
        hashData[i] = hasher(valueAtFast(i));
      }
    }
  }

  // overwrite the null hash values
  if (!BaseVector::nullCount_.has_value() ||
      BaseVector::nullCount_.value() > 0) {
    for (size_t i = 0; i < BaseVector::length_; ++i) {
      if (bits::isBitNull(BaseVector::rawNulls_, i)) {
        hashData[i] = BaseVector::kNullHash;
      }
    }
  }
  return std::make_unique<FlatVector<uint64_t>>(
      BaseVector::pool_,
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
  source = source->loadedVector();
  VELOX_CHECK(
      BaseVector::compatibleKind(BaseVector::typeKind(), source->typeKind()));
  VELOX_CHECK_GE(BaseVector::length_, rows.end());
  SelectivityIterator iter(rows);
  vector_size_t row;
  const uint64_t* sourceNulls = source->rawNulls();
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
    auto flat = source->asUnchecked<FlatVector<T>>();
    auto* sourceValues =
        source->typeKind() != TypeKind::UNKNOWN ? flat->rawValues() : nullptr;
    if (toSourceRow) {
      while (iter.next(row)) {
        auto sourceRow = toSourceRow[row];
        if (sourceValues) {
          rawValues_[row] = sourceValues[sourceRow];
        }
        if (rawNulls) {
          bits::setNull(
              rawNulls,
              row,
              sourceNulls && bits::isBitNull(sourceNulls, sourceRow));
        }
      }
    } else {
      VELOX_CHECK_GE(source->size(), rows.end());
      rows.applyToSelected([&](vector_size_t row) {
        if (sourceValues) {
          rawValues_[row] = sourceValues[row];
        }
        if (rawNulls) {
          bits::setNull(
              rawNulls, row, sourceNulls && bits::isBitNull(sourceNulls, row));
        }
      });
    }
  } else if (source->isConstantEncoding()) {
    if (source->isNullAt(0)) {
      BaseVector::addNulls(nullptr, rows);
      return;
    }
    auto constant = source->asUnchecked<ConstantVector<T>>();
    T value = constant->valueAt(0);
    rows.applyToSelected([&](int32_t row) { rawValues_[row] = value; });
    rows.clearNulls(rawNulls);
  } else {
    auto sourceVector = source->typeKind() != TypeKind::UNKNOWN
        ? source->asUnchecked<SimpleVector<T>>()
        : nullptr;
    while (iter.next(row)) {
      auto sourceRow = toSourceRow ? toSourceRow[row] : row;
      if (!source->isNullAt(sourceRow)) {
        if (sourceVector) {
          rawValues_[row] = sourceVector->valueAt(sourceRow);
        }
        if (rawNulls) {
          bits::clearNull(rawNulls, row);
        }
      } else {
        bits::setNull(rawNulls, row);
      }
    }
  }
}

template <typename T>
void FlatVector<T>::copyValuesAndNulls(
    const BaseVector* source,
    vector_size_t targetIndex,
    vector_size_t sourceIndex,
    vector_size_t count) {
  source = source->loadedVector();
  VELOX_CHECK(
      BaseVector::compatibleKind(BaseVector::typeKind(), source->typeKind()));
  VELOX_CHECK_GE(source->size(), sourceIndex + count);
  VELOX_CHECK_GE(BaseVector::length_, targetIndex + count);
  const uint64_t* sourceNulls = source->rawNulls();
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
    auto flat = source->asUnchecked<FlatVector<T>>();
    if (!flat->values() || flat->values()->size() == 0) {
      // The vector must have all-null values.
      VELOX_CHECK_EQ(
          BaseVector::countNulls(flat->nulls(), 0, flat->size()), flat->size());
    } else if (source->typeKind() != TypeKind::UNKNOWN) {
      if (Buffer::is_pod_like_v<T>) {
        memcpy(
            &rawValues_[targetIndex],
            &flat->rawValues()[sourceIndex],
            count * sizeof(T));
      } else {
        const T* srcValues = flat->rawValues();
        std::copy(
            srcValues + sourceIndex,
            srcValues + sourceIndex + count,
            rawValues_ + targetIndex);
      }
    }
    if (rawNulls) {
      if (sourceNulls) {
        bits::copyBits(sourceNulls, sourceIndex, rawNulls, targetIndex, count);
      } else {
        bits::fillBits(
            rawNulls, targetIndex, targetIndex + count, bits::kNotNull);
      }
    }
  } else if (source->isConstantEncoding()) {
    if (source->isNullAt(0)) {
      bits::fillBits(rawNulls, targetIndex, targetIndex + count, bits::kNull);
      return;
    }
    auto constant = source->asUnchecked<ConstantVector<T>>();
    T value = constant->valueAt(0);
    for (auto row = targetIndex; row < targetIndex + count; ++row) {
      rawValues_[row] = value;
    }
    if (rawNulls) {
      bits::fillBits(
          rawNulls, targetIndex, targetIndex + count, bits::kNotNull);
    }
  } else {
    auto sourceVector = source->asUnchecked<SimpleVector<T>>();
    for (int32_t i = 0; i < count; ++i) {
      if (!source->isNullAt(sourceIndex + i)) {
        rawValues_[targetIndex + i] = sourceVector->valueAt(sourceIndex + i);
        if (rawNulls) {
          bits::clearNull(rawNulls, targetIndex + i);
        }
      } else {
        bits::setNull(rawNulls, targetIndex + i);
      }
    }
  }
}

template <typename T>
void FlatVector<T>::resize(vector_size_t size, bool setNotNull) {
  auto previousSize = BaseVector::length_;
  BaseVector::resize(size, setNotNull);
  if (!values_) {
    return;
  }
  const uint64_t minBytes = BaseVector::byteSize<T>(size);
  if (values_->capacity() < minBytes) {
    AlignedBuffer::reallocate<T>(&values_, size);
    rawValues_ = values_->asMutable<T>();
  }
  values_->setSize(minBytes);

  if (std::is_same<T, StringView>::value) {
    if (size < previousSize) {
      auto vector = this->template asUnchecked<SimpleVector<StringView>>();
      vector->invalidateIsAscii();
    }
    if (size == 0) {
      stringBuffers_.clear();
    }
    if (size > previousSize) {
      auto stringViews = reinterpret_cast<StringView*>(rawValues_);
      for (auto index = previousSize; index < size; ++index) {
        new (&stringViews[index]) StringView();
      }
    }
  }
}

template <typename T>
void FlatVector<T>::ensureWritable(const SelectivityVector& rows) {
  auto newSize = std::max<vector_size_t>(rows.size(), BaseVector::length_);
  if (values_ && !values_->unique()) {
    BufferPtr newValues;
    if constexpr (std::is_same<T, StringView>::value) {
      // Make sure to initialize StringView values so they can be safely
      // accessed.
      newValues = AlignedBuffer::allocate<T>(newSize, BaseVector::pool_, T());
    } else {
      newValues = AlignedBuffer::allocate<T>(newSize, BaseVector::pool_);
    }

    auto rawNewValues = newValues->asMutable<T>();
    SelectivityVector rowsToCopy(BaseVector::length_);
    rowsToCopy.deselect(rows);
    rowsToCopy.applyToSelected(
        [&](vector_size_t row) { rawNewValues[row] = rawValues_[row]; });

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
  if (values_ && !(values_->unique() && values_->isMutable())) {
    values_ = nullptr;
    rawValues_ = nullptr;
  }
}
} // namespace velox
} // namespace facebook
