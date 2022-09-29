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
#include "velox/vector/DecodedVector.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/base/BitUtil.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/BiasVector.h"
#include "velox/vector/LazyVector.h"
#include "velox/vector/SequenceVector.h"

namespace facebook::velox {

uint64_t DecodedVector::constantNullMask_;

namespace {
std::vector<vector_size_t> makeConsecutiveIndices(size_t size) {
  std::vector<vector_size_t> consecutiveIndices(size);
  for (vector_size_t i = 0; i < consecutiveIndices.size(); ++i) {
    consecutiveIndices[i] = i;
  }
  return consecutiveIndices;
}
} // namespace

const std::vector<vector_size_t>& DecodedVector::consecutiveIndices() {
  static std::vector<vector_size_t> consecutiveIndices =
      makeConsecutiveIndices(10'000);
  return consecutiveIndices;
}

const std::vector<vector_size_t>& DecodedVector::zeroIndices() {
  static std::vector<vector_size_t> indices(10'000);
  return indices;
};

void DecodedVector::decode(
    const BaseVector& vector,
    const SelectivityVector& rows,
    bool loadLazy) {
  reset(rows.end());
  loadLazy_ = loadLazy;
  if (loadLazy_ && (isLazyNotLoaded(vector) || vector.isLazy())) {
    decode(*vector.loadedVector(), rows);
    return;
  }

  auto encoding = vector.encoding();
  switch (encoding) {
    case VectorEncoding::Simple::FLAT:
    case VectorEncoding::Simple::BIASED:
    case VectorEncoding::Simple::ROW:
    case VectorEncoding::Simple::ARRAY:
    case VectorEncoding::Simple::MAP:
    case VectorEncoding::Simple::LAZY:
      isIdentityMapping_ = true;
      setBaseData(vector, rows);
      return;
    case VectorEncoding::Simple::CONSTANT: {
      isConstantMapping_ = true;
      if (isLazyNotLoaded(vector)) {
        baseVector_ = vector.valueVector().get();
        constantIndex_ = vector.wrapInfo()->as<vector_size_t>()[0];
        mayHaveNulls_ = true;
      } else {
        setBaseData(vector, rows);
      }
      break;
    }
    case VectorEncoding::Simple::DICTIONARY:
    case VectorEncoding::Simple::SEQUENCE: {
      combineWrappers(&vector, rows);
      break;
    }
    default:
      VELOX_FAIL(
          "Unsupported vector encoding: {}",
          VectorEncoding::mapSimpleToName(encoding));
  }
}

void DecodedVector::makeIndices(
    const BaseVector& vector,
    const SelectivityVector& rows,
    int32_t numLevels) {
  VELOX_CHECK_LE(rows.end(), vector.size());
  reset(rows.end());
  combineWrappers(&vector, rows, numLevels);
}

void DecodedVector::reset(vector_size_t size) {
  size_ = size;
  indices_ = nullptr;
  data_ = nullptr;
  nulls_ = nullptr;
  allNulls_.reset();
  baseVector_ = nullptr;
  mayHaveNulls_ = false;
  hasExtraNulls_ = false;
  isConstantMapping_ = false;
  isIdentityMapping_ = false;
  constantIndex_ = 0;
}

void DecodedVector::copyNulls(vector_size_t size) {
  auto numWords = bits::nwords(size);
  copiedNulls_.resize(numWords > 0 ? numWords : 1);
  if (nulls_) {
    std::copy(nulls_, nulls_ + numWords, copiedNulls_.data());
  } else {
    std::fill(copiedNulls_.begin(), copiedNulls_.end(), bits::kNotNull64);
  }
  nulls_ = copiedNulls_.data();
}

void DecodedVector::combineWrappers(
    const BaseVector* vector,
    const SelectivityVector& rows,
    int numLevels) {
  auto topEncoding = vector->encoding();
  BaseVector* values = nullptr;
  if (topEncoding == VectorEncoding::Simple::DICTIONARY) {
    indices_ = vector->wrapInfo()->as<vector_size_t>();
    values = vector->valueVector().get();
    nulls_ = vector->rawNulls();
    if (nulls_) {
      hasExtraNulls_ = true;
      mayHaveNulls_ = true;
    }
  } else if (topEncoding == VectorEncoding::Simple::SEQUENCE) {
    copiedIndices_.resize(rows.end());
    indices_ = &copiedIndices_[0];
    auto sizes = vector->wrapInfo()->as<vector_size_t>();
    vector_size_t lastBegin = 0;
    vector_size_t lastEnd = sizes[0];
    vector_size_t lastIndex = 0;
    values = vector->valueVector().get();
    rows.applyToSelected([&](vector_size_t row) {
      copiedIndices_[row] =
          offsetOfIndex(sizes, row, &lastBegin, &lastEnd, &lastIndex);
    });
  } else {
    VELOX_FAIL(
        "Unsupported wrapper encoding: {}",
        VectorEncoding::mapSimpleToName(topEncoding));
  }
  int32_t levelCounter = 0;
  for (;;) {
    if (numLevels != -1 && ++levelCounter == numLevels) {
      baseVector_ = values;
      return;
    }

    auto encoding = values->encoding();
    if (loadLazy_ && encoding == VectorEncoding::Simple::LAZY) {
      values = values->loadedVector();
      encoding = values->encoding();
    }

    switch (encoding) {
      case VectorEncoding::Simple::LAZY:
      case VectorEncoding::Simple::CONSTANT:
      case VectorEncoding::Simple::FLAT:
      case VectorEncoding::Simple::BIASED:
      case VectorEncoding::Simple::ROW:
      case VectorEncoding::Simple::ARRAY:
      case VectorEncoding::Simple::MAP:
        setBaseData(*values, rows);
        return;
      case VectorEncoding::Simple::DICTIONARY: {
        applyDictionaryWrapper(*values, rows);
        values = values->valueVector().get();
        break;
      }
      case VectorEncoding::Simple::SEQUENCE: {
        applySequenceWrapper(*values, rows);
        values = values->valueVector().get();
        break;
      }
      default:
        VELOX_CHECK(false, "Unsupported vector encoding");
    }
  }
}

void DecodedVector::applyDictionaryWrapper(
    const BaseVector& dictionaryVector,
    const SelectivityVector& rows) {
  if (!rows.hasSelections()) {
    // No further processing is needed.
    return;
  }

  auto newIndices = dictionaryVector.wrapInfo()->as<vector_size_t>();
  auto newNulls = dictionaryVector.rawNulls();
  if (newNulls) {
    hasExtraNulls_ = true;
    mayHaveNulls_ = true;
    // if we have both nulls for parent and the wrapped vectors, and nulls
    // buffer is not copied, make a copy because we may need to
    // change it when iterating through wrapped vector
    if (!nulls_ || nullsNotCopied()) {
      copyNulls(rows.end());
    }
  }
  auto copiedNulls = copiedNulls_.data();
  auto currentIndices = indices_;
  if (indicesNotCopied()) {
    copiedIndices_.resize(size_);
    indices_ = copiedIndices_.data();
  }

  rows.applyToSelected([&](vector_size_t row) {
    if (!nulls_ || !bits::isBitNull(nulls_, row)) {
      auto wrappedIndex = currentIndices[row];
      if (newNulls && bits::isBitNull(newNulls, wrappedIndex)) {
        bits::setNull(copiedNulls, row);
      } else {
        copiedIndices_[row] = newIndices[wrappedIndex];
      }
    }
  });
}

void DecodedVector::applySequenceWrapper(
    const BaseVector& sequenceVector,
    const SelectivityVector& rows) {
  if (!rows.hasSelections()) {
    // No further processing is needed.
    return;
  }

  const auto* lengths = sequenceVector.wrapInfo()->as<vector_size_t>();
  auto newNulls = sequenceVector.rawNulls();
  if (newNulls) {
    hasExtraNulls_ = true;
    mayHaveNulls_ = true;
    if (!nulls_ || nullsNotCopied()) {
      copyNulls(rows.end());
    }
  }
  auto copiedNulls = copiedNulls_.data();
  auto currentIndices = indices_;
  if (indicesNotCopied()) {
    copiedIndices_.resize(size_);
    indices_ = copiedIndices_.data();
  }

  vector_size_t lastBegin = 0;
  vector_size_t lastEnd = lengths[0];
  vector_size_t lastIndex = 0;
  rows.applyToSelected([&, this](vector_size_t row) {
    if (!nulls_ || !bits::isBitNull(nulls_, row)) {
      auto wrappedIndex = currentIndices[row];
      if (newNulls && bits::isBitNull(newNulls, wrappedIndex)) {
        bits::setNull(copiedNulls, row);
      } else {
        copiedIndices_[row] = offsetOfIndex(
            lengths, wrappedIndex, &lastBegin, &lastEnd, &lastIndex);
      }
    }
  });
}

void DecodedVector::fillInIndices() {
  if (isConstantMapping_) {
    if (size_ > zeroIndices().size() || constantIndex_ != 0) {
      copiedIndices_.resize(size_);
      std::fill(copiedIndices_.begin(), copiedIndices_.end(), constantIndex_);
      indices_ = copiedIndices_.data();
    } else {
      indices_ = zeroIndices().data();
    }
    return;
  }
  if (isIdentityMapping_) {
    if (size_ > consecutiveIndices().size()) {
      copiedIndices_.resize(size_);
      std::iota(copiedIndices_.begin(), copiedIndices_.end(), 0);
      indices_ = &copiedIndices_[0];
    } else {
      indices_ = consecutiveIndices().data();
    }
    return;
  }
  VELOX_FAIL(
      "DecodedVector::indices_ must be set for non-constant non-consecutive mapping.");
}

void DecodedVector::makeIndicesMutable() {
  if (indicesNotCopied()) {
    copiedIndices_.resize(size_ > 0 ? size_ : 1);
    memcpy(
        &copiedIndices_[0],
        indices_,
        copiedIndices_.size() * sizeof(copiedIndices_[0]));
    indices_ = &copiedIndices_[0];
  }
}

template <TypeKind kind>
void DecodedVector::decodeBiased(
    const BaseVector& vector,
    const SelectivityVector& rows) {
  using T = typename TypeTraits<kind>::NativeType;
  auto biased = vector.as<const BiasVector<T>>();
  // The API contract is that 'data_' is interpretable as a flat array
  // of T. The container is a vector of int64_t. So the number of
  // elements needed is the rounded up size over the number of T's
  // that fit in int64_t.
  auto numInt64 =
      bits::roundUp(size_, sizeof(int64_t)) / (sizeof(int64_t) / sizeof(T));
  tempSpace_.resize(numInt64);
  T* data = reinterpret_cast<T*>(&tempSpace_[0]); // NOLINT
  rows.applyToSelected(
      [data, biased](vector_size_t row) { data[row] = biased->valueAt(row); });
  data_ = data;
}

void DecodedVector::setFlatNulls(
    const BaseVector& vector,
    const SelectivityVector& rows) {
  if (hasExtraNulls_) {
    if (nullsNotCopied()) {
      copyNulls(rows.end());
    }
    auto leafNulls = vector.rawNulls();
    auto copiedNulls = &copiedNulls_[0];
    rows.applyToSelected([&](vector_size_t row) {
      if (!bits::isBitNull(nulls_, row) &&
          (leafNulls && bits::isBitNull(leafNulls, indices_[row]))) {
        bits::setNull(copiedNulls, row);
      }
    });
    nulls_ = &copiedNulls_[0];
  } else {
    nulls_ = vector.rawNulls();
    mayHaveNulls_ = nulls_ != nullptr;
  }
}

void DecodedVector::setBaseData(
    const BaseVector& vector,
    const SelectivityVector& rows) {
  auto encoding = vector.encoding();
  baseVector_ = &vector;
  switch (encoding) {
    case VectorEncoding::Simple::LAZY:
      break;
    case VectorEncoding::Simple::FLAT: {
      // values() may be nullptr if 'vector' is all nulls.
      data_ = vector.values() ? vector.values()->as<void>() : nullptr;
      setFlatNulls(vector, rows);
      break;
    }
    case VectorEncoding::Simple::ROW:
    case VectorEncoding::Simple::ARRAY:
    case VectorEncoding::Simple::MAP: {
      setFlatNulls(vector, rows);
      break;
    }
    case VectorEncoding::Simple::CONSTANT: {
      setBaseDataForConstant(vector, rows);
      break;
    }
    case VectorEncoding::Simple::BIASED: {
      setBaseDataForBias(vector, rows);
      break;
    }
    default:
      VELOX_UNREACHABLE();
  }
}

void DecodedVector::setBaseDataForConstant(
    const BaseVector& vector,
    const SelectivityVector& rows) {
  if (!vector.isScalar()) {
    baseVector_ = vector.wrappedVector();
    constantIndex_ = vector.wrappedIndex(0);
  }
  if (!hasExtraNulls_ || vector.isNullAt(0)) {
    // A mapping over a constant is constant except if the
    // mapping adds nulls and the constant is not null.
    isConstantMapping_ = true;
    hasExtraNulls_ = false;
    indices_ = nullptr;
    nulls_ = vector.isNullAt(0) ? &constantNullMask_ : nullptr;
  } else {
    makeIndicesMutable();
    rows.applyToSelected(
        [this](vector_size_t row) { copiedIndices_[row] = constantIndex_; });
    setFlatNulls(vector, rows);
  }
  data_ = vector.valuesAsVoid();
  if (!nulls_) {
    nulls_ = vector.isNullAt(0) ? &constantNullMask_ : nullptr;
  }
  mayHaveNulls_ = hasExtraNulls_ || nulls_;
}

void DecodedVector::setBaseDataForBias(
    const BaseVector& vector,
    const SelectivityVector& rows) {
  setFlatNulls(vector, rows);
  switch (vector.typeKind()) {
    case TypeKind::SMALLINT:
      decodeBiased<TypeKind::SMALLINT>(vector, rows);
      break;
    case TypeKind::INTEGER:
      decodeBiased<TypeKind::INTEGER>(vector, rows);
      break;
    case TypeKind::BIGINT:
      decodeBiased<TypeKind::BIGINT>(vector, rows);
      break;
    default:
      VELOX_FAIL(
          "Unsupported type for BiasVector: {}", vector.type()->toString());
  }
}

namespace {
bool isWrapper(VectorEncoding::Simple encoding) {
  return encoding == VectorEncoding::Simple::SEQUENCE ||
      encoding == VectorEncoding::Simple::DICTIONARY;
}

/// Returns true if 'wrapper' is a dictionary vector wrapping non-dictionary
/// vector.
bool isOneLevelDictionary(const BaseVector& wrapper) {
  return wrapper.encoding() == VectorEncoding::Simple::DICTIONARY &&
      !isWrapper(wrapper.valueVector()->encoding());
}

/// Copies 'size' entries from 'indices' into a newly allocated buffer.
BufferPtr copyIndicesBuffer(
    const vector_size_t* indices,
    vector_size_t size,
    memory::MemoryPool* pool) {
  BufferPtr copy = AlignedBuffer::allocate<vector_size_t>(size, pool);
  memcpy(
      copy->asMutable<vector_size_t>(),
      indices,
      BaseVector::byteSize<vector_size_t>(size));
  return copy;
}

/// Copies 'size' bits from 'nulls' into a newly allocated buffer. Returns
/// nullptr if 'nulls' is null.
BufferPtr copyNullsBuffer(
    const uint64_t* nulls,
    vector_size_t size,
    memory::MemoryPool* pool) {
  if (!nulls) {
    return nullptr;
  }

  BufferPtr copy = AlignedBuffer::allocate<bool>(size, pool);
  memcpy(copy->asMutable<uint64_t>(), nulls, BaseVector::byteSize<bool>(size));
  return copy;
}
} // namespace

DecodedVector::DictionaryWrapping DecodedVector::dictionaryWrapping(
    const BaseVector& wrapper,
    const SelectivityVector& rows) const {
  VELOX_CHECK(!isIdentityMapping_);
  VELOX_CHECK(!isConstantMapping_);

  VELOX_CHECK_LE(rows.end(), size_);

  if (isOneLevelDictionary(wrapper)) {
    // Re-use indices and nulls buffers.
    return {wrapper.wrapInfo(), wrapper.nulls()};
  } else {
    // Make a copy of the indices and nulls buffers.
    BufferPtr indices = copyIndicesBuffer(indices_, rows.end(), wrapper.pool());
    // Only copy nulls if we have nulls coming from one of the wrappers, don't
    // do it if nulls are missing or from the base vector.
    BufferPtr nulls = hasExtraNulls_
        ? copyNullsBuffer(nulls_, rows.end(), wrapper.pool())
        : nullptr;
    return {std::move(indices), std::move(nulls)};
  }
}

VectorPtr DecodedVector::wrap(
    VectorPtr data,
    const BaseVector& wrapper,
    const SelectivityVector& rows) {
  if (data->isConstantEncoding()) {
    return data;
  }
  if (wrapper.isConstantEncoding()) {
    if (wrapper.isNullAt(0)) {
      return BaseVector::createNullConstant(
          data->type(), rows.end(), data->pool());
    }
    return BaseVector::wrapInConstant(
        rows.end(), wrapper.wrappedIndex(0), data);
  }

  auto wrapping = dictionaryWrapping(wrapper, rows);
  return BaseVector::wrapInDictionary(
      std::move(wrapping.nulls),
      std::move(wrapping.indices),
      rows.end(),
      std::move(data));
}

const uint64_t* DecodedVector::nulls() {
  if (allNulls_.has_value()) {
    return allNulls_.value();
  }

  if (hasExtraNulls_) {
    allNulls_ = nulls_;
  } else if (!nulls_ || size_ == 0) {
    allNulls_ = nullptr;
  } else {
    if (isIdentityMapping_) {
      allNulls_ = nulls_;
    } else if (isConstantMapping_) {
      copiedNulls_.resize(0);
      copiedNulls_.resize(bits::nwords(size_), bits::kNull64);
      allNulls_ = copiedNulls_.data();
    } else {
      // Copy base nulls.
      copiedNulls_.resize(bits::nwords(size_));
      auto* rawCopiedNulls = copiedNulls_.data();
      for (auto i = 0; i < size_; ++i) {
        bits::setNull(rawCopiedNulls, i, bits::isBitNull(nulls_, indices_[i]));
      }
      allNulls_ = copiedNulls_.data();
    }
  }

  return allNulls_.value();
}
} // namespace facebook::velox
