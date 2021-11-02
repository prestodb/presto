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

namespace facebook {
namespace velox {

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
  static std::vector<vector_size_t> indices(10000);
  return indices;
};

void DecodedVector::decode(
    const BaseVector& vector,
    const SelectivityVector& rows,
    bool loadLazy) {
  size_ = rows.end();
  indices_ = nullptr;
  data_ = nullptr;
  nulls_ = nullptr;
  baseVector_ = nullptr;
  mayHaveNulls_ = false;
  hasExtraNulls_ = false;
  isConstantMapping_ = false;
  isIdentityMapping_ = false;
  constantIndex_ = 0;
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
      VELOX_CHECK(false, "Unsupported vector encoding");
  }
}

void DecodedVector::makeIndices(
    const BaseVector& vector,
    const SelectivityVector& rows,
    int32_t numLevels) {
  VELOX_CHECK_LE(rows.end(), vector.size());
  size_ = rows.end();
  indices_ = nullptr;
  data_ = nullptr;
  nulls_ = nullptr;
  mayHaveNulls_ = false;
  hasExtraNulls_ = false;
  isConstantMapping_ = false;
  isIdentityMapping_ = false;
  combineWrappers(&vector, rows, numLevels);
}

void DecodedVector::copyNulls(vector_size_t size) {
  auto numWords = bits::nwords(size);
  copiedNulls_.resize(numWords);
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
    VELOX_CHECK(false, "Unsupported wrapper encoding");
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
        auto newIndices = values->wrapInfo()->as<vector_size_t>();
        auto newNulls = values->rawNulls();
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

        rows.applyToSelected([&, this](vector_size_t row) {
          if (!nulls_ || !bits::isBitNull(nulls_, row)) {
            auto wrappedIndex = currentIndices[row];
            if (newNulls && bits::isBitNull(newNulls, wrappedIndex)) {
              bits::setNull(copiedNulls, row);
            } else {
              copiedIndices_[row] = newIndices[wrappedIndex];
            }
          }
        });

        values = values->valueVector().get();
        break;
      }
      case VectorEncoding::Simple::SEQUENCE: {
        const auto* lengths = values->wrapInfo()->as<vector_size_t>();
        vector_size_t lastBegin = 0;
        vector_size_t lastEnd = lengths[0];
        vector_size_t lastIndex = 0;
        auto newNulls = values->rawNulls();
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

        values = values->valueVector().get();
        break;
      }
      default:
        VELOX_CHECK(false, "Unsupported vector encoding");
    }
  }
}

void DecodedVector::fillInIndices() {
  if (isConstantMapping_) {
    if (size_ > zeroIndices().size() || constantIndex_ != 0) {
      copiedIndices_.resize(size_);
      std::fill(copiedIndices_.begin(), copiedIndices_.end(), constantIndex_);
      indices_ = &copiedIndices_[0];
    } else {
      indices_ = zeroIndices().data();
    }
    return;
  }
  if (isIdentityMapping_) {
    if (size_ > consecutiveIndices().size()) {
      copiedIndices_.resize(size_);
      for (vector_size_t i = 0; i < size_; ++i) {
        copiedIndices_[i] = i;
      }
      indices_ = &copiedIndices_[0];
    } else {
      indices_ = consecutiveIndices().data();
    }
    return;
  }
  VELOX_CHECK(
      false, "indices_ not set when non-constant, non-consecutive mapping.");
}

void DecodedVector::makeIndicesMutable() {
  if (indicesNotCopied()) {
    copiedIndices_.resize(size_);
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
    rows.applyToSelected([&, this](vector_size_t row) {
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
      return;
    case VectorEncoding::Simple::FLAT:
      data_ = vector.values()->as<void>();
    case VectorEncoding::Simple::ROW:
    case VectorEncoding::Simple::ARRAY:
    case VectorEncoding::Simple::MAP:
      setFlatNulls(vector, rows);
      return;

    case VectorEncoding::Simple::CONSTANT: {
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
        rows.applyToSelected([this](vector_size_t row) {
          copiedIndices_[row] = constantIndex_;
        });
        setFlatNulls(vector, rows);
      }
      data_ = vector.valuesAsVoid();
      if (!nulls_) {
        nulls_ = vector.isNullAt(0) ? &constantNullMask_ : nullptr;
      }
      mayHaveNulls_ = hasExtraNulls_ || nulls_;
      return;
    }
    case VectorEncoding::Simple::BIASED: {
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
          VELOX_CHECK(false, "Bad type for BiasVector");
      }
      break;
    }
    default:
      VELOX_NYI();
  }
}

static inline bool isWrapper(VectorEncoding::Simple encoding) {
  return encoding == VectorEncoding::Simple::SEQUENCE ||
      encoding == VectorEncoding::Simple::DICTIONARY;
}

VectorPtr DecodedVector::wrap(
    VectorPtr data,
    const BaseVector& wrapper,
    const SelectivityVector& rows) {
  if (data->isConstantEncoding()) {
    return data;
  }
  if (wrapper.isConstantEncoding()) {
    return BaseVector::wrapInConstant(
        rows.end(), wrapper.wrappedIndex(0), data);
  }
  VELOX_CHECK(size_ >= rows.end());
  // If 'wrapper' is one level of dictionary we use the indices and nulls array
  // as is.
  auto wrapEncoding = wrapper.encoding();
  VELOX_CHECK(isWrapper(wrapEncoding));
  auto valueEncoding = wrapper.valueVector()->encoding();
  if (!isWrapper(valueEncoding) &&
      wrapEncoding == VectorEncoding::Simple::DICTIONARY) {
    return BaseVector::wrapInDictionary(
        wrapper.nulls(), wrapper.wrapInfo(), rows.end(), std::move(data));
  }
  // Make a dictionary wrapper by copying 'indices' and 'nulls'.
  BufferPtr indices =
      AlignedBuffer::allocate<vector_size_t>(rows.end(), data->pool());
  memcpy(
      indices->asMutable<vector_size_t>(),
      indices_,
      sizeof(vector_size_t) * rows.end());
  BufferPtr nulls(nullptr);
  if (nulls_ != nullptr) {
    nulls = AlignedBuffer::allocate<bool>(rows.end(), data->pool());
    memcpy( // NOLINT
        nulls->asMutable<uint64_t>(),
        nulls_,
        bits::nbytes(rows.end()));
  }
  return BaseVector::wrapInDictionary(
      std::move(nulls), std::move(indices), rows.end(), std::move(data));
}
} // namespace velox
} // namespace facebook
