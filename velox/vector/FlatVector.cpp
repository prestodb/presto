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

#include "velox/vector/FlatVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook {
namespace velox {

template <>
const bool* FlatVector<bool>::rawValues() const {
  VELOX_UNSUPPORTED("rawValues() for bool is not supported");
}

template <>
const bool FlatVector<bool>::valueAtFast(vector_size_t idx) const {
  return bits::isBitSet(reinterpret_cast<const uint64_t*>(rawValues_), idx);
}

template <>
Range<bool> FlatVector<bool>::asRange() const {
  return Range<bool>(rawValues<int64_t>(), 0, BaseVector::length_);
}

template <>
void FlatVector<bool>::set(vector_size_t idx, bool value) {
  VELOX_DCHECK(idx < BaseVector::length_);
  if (BaseVector::rawNulls_) {
    BaseVector::setNull(idx, false);
  }
  bits::setBit(reinterpret_cast<uint64_t*>(rawValues_), idx, value);
}

template <>
void FlatVector<bool>::copyValuesAndNulls(
    const BaseVector* source,
    const SelectivityVector& rows,
    const vector_size_t* toSourceRow) {
  source = source->loadedVector();
  VELOX_CHECK(
      BaseVector::compatibleKind(BaseVector::typeKind(), source->typeKind()));
  VELOX_CHECK(BaseVector::length_ >= rows.end());
  const uint64_t* sourceNulls = source->rawNulls();
  uint64_t* rawNulls = const_cast<uint64_t*>(BaseVector::rawNulls_);
  if (source->mayHaveNulls()) {
    rawNulls = BaseVector::mutableRawNulls();
  }
  uint64_t* rawValues = reinterpret_cast<uint64_t*>(rawValues_);
  if (source->isFlatEncoding()) {
    auto flat = source->asUnchecked<FlatVector<bool>>();
    auto* sourceValues = source->typeKind() != TypeKind::UNKNOWN
        ? flat->rawValues<uint64_t>()
        : nullptr;
    if (!sourceValues) {
      // All rows in source vector are null.
      rows.applyToSelected(
          [&](auto row) { bits::setNull(rawNulls, row, true); });
    } else {
      if (toSourceRow) {
        rows.applyToSelected([&](auto row) {
          int32_t sourceRow = toSourceRow[row];
          if (sourceValues) {
            bits::setBit(
                rawValues, row, bits::isBitSet(sourceValues, sourceRow));
          }
          if (rawNulls) {
            bits::setNull(
                rawNulls,
                row,
                sourceNulls && bits::isBitNull(sourceNulls, sourceRow));
          }
        });
      } else {
        rows.applyToSelected([&](auto row) {
          if (sourceValues) {
            bits::setBit(rawValues, row, bits::isBitSet(sourceValues, row));
          }
          if (rawNulls) {
            bits::setNull(
                rawNulls,
                row,
                sourceNulls && bits::isBitNull(sourceNulls, row));
          }
        });
      }
    }
  } else if (source->isConstantEncoding()) {
    auto constant = source->asUnchecked<ConstantVector<bool>>();
    if (constant->isNullAt(0)) {
      addNulls(nullptr, rows);
      return;
    }
    bool value = constant->valueAt(0);
    auto range = rows.asRange();
    if (value) {
      bits::orBits(rawValues, range.bits(), range.begin(), range.end());
    } else {
      bits::andWithNegatedBits(
          rawValues, range.bits(), range.begin(), range.end());
    }
    rows.clearNulls(rawNulls);
  } else {
    auto sourceVector = source->asUnchecked<SimpleVector<bool>>();
    rows.applyToSelected([&](auto row) {
      int32_t sourceRow = toSourceRow ? toSourceRow[row] : row;
      if (!source->isNullAt(sourceRow)) {
        bits::setBit(rawValues, row, sourceVector->valueAt(sourceRow));
        if (rawNulls) {
          bits::clearNull(rawNulls, row);
        }
      } else {
        bits::setNull(rawNulls, row);
      }
    });
  }
}

template <>
void FlatVector<bool>::copyValuesAndNulls(
    const BaseVector* source,
    vector_size_t targetIndex,
    vector_size_t sourceIndex,
    vector_size_t count) {
  source = source->loadedVector();
  VELOX_CHECK(
      BaseVector::compatibleKind(BaseVector::typeKind(), source->typeKind()));
  VELOX_CHECK(source->size() >= sourceIndex + count);
  VELOX_CHECK(BaseVector::length_ >= targetIndex + count);

  const uint64_t* sourceNulls = source->rawNulls();
  auto rawValues = reinterpret_cast<uint64_t*>(rawValues_);
  uint64_t* rawNulls = const_cast<uint64_t*>(BaseVector::rawNulls_);
  if (source->mayHaveNulls()) {
    rawNulls = BaseVector::mutableRawNulls();
  }
  if (source->isFlatEncoding()) {
    if (source->typeKind() != TypeKind::UNKNOWN) {
      auto* sourceValues =
          source->asUnchecked<FlatVector<bool>>()->rawValues<uint64_t>();
      bits::copyBits(sourceValues, sourceIndex, rawValues, targetIndex, count);
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
    auto constant = source->asUnchecked<ConstantVector<bool>>();
    if (constant->isNullAt(0)) {
      bits::fillBits(rawNulls, targetIndex, targetIndex + count, bits::kNull);
      return;
    }
    bool value = constant->valueAt(0);
    bits::fillBits(rawValues, targetIndex, targetIndex + count, value);
    if (rawNulls) {
      bits::fillBits(
          rawNulls, targetIndex, targetIndex + count, bits::kNotNull);
    }
  } else {
    auto sourceVector = source->typeKind() != TypeKind::UNKNOWN
        ? source->asUnchecked<SimpleVector<bool>>()
        : nullptr;
    for (int32_t i = 0; i < count; ++i) {
      if (!source->isNullAt(sourceIndex + i)) {
        if (sourceVector) {
          bits::setBit(
              rawValues,
              targetIndex + i,
              sourceVector->valueAt(sourceIndex + i));
        }
        if (rawNulls) {
          bits::clearNull(rawNulls, targetIndex + i);
        }
      } else {
        bits::setNull(rawNulls, targetIndex + i);
      }
    }
  }
}

template <>
Buffer* FlatVector<StringView>::getBufferWithSpace(vector_size_t size) {
  // Check if the last buffer is uniquely referenced and has enough space.
  Buffer* buffer =
      stringBuffers_.empty() ? nullptr : stringBuffers_.back().get();
  if (buffer && buffer->unique() &&
      buffer->size() + size <= buffer->capacity()) {
    return buffer;
  }

  // Allocate a new buffer.
  int32_t newSize = std::max(kInitialStringSize, size);
  BufferPtr newBuffer = AlignedBuffer::allocate<char>(newSize, pool());
  newBuffer->setSize(0);
  stringBuffers_.push_back(newBuffer);
  return stringBuffers_.back().get();
}

template <>
void FlatVector<StringView>::prepareForReuse() {
  BaseVector::prepareForReuse();

  // Check values buffer. Keep the buffer if singly-referenced and mutable.
  // Reset otherwise.
  if (values_ && !(values_->unique() && values_->isMutable())) {
    values_ = nullptr;
    rawValues_ = nullptr;
  }

  // Check string buffers. Keep at most one singly-referenced buffer if it is
  // not too large.
  if (!stringBuffers_.empty()) {
    auto& firstBuffer = stringBuffers_.front();
    if (firstBuffer->unique() && firstBuffer->isMutable() &&
        firstBuffer->capacity() <= kMaxStringSizeForReuse) {
      firstBuffer->setSize(0);
      stringBuffers_.resize(1);
    } else {
      stringBuffers_.clear();
    }
  }

  // Clear the StringViews to avoid referencing freed memory.
  if (rawValues_) {
    for (auto i = 0; i < BaseVector::length_; ++i) {
      rawValues_[i] = StringView();
    }
  }

  // Clear ASCII-ness.
  SimpleVector<StringView>::invalidateIsAscii();
}

template <>
void FlatVector<StringView>::set(vector_size_t idx, StringView value) {
  VELOX_DCHECK(idx < BaseVector::length_);
  if (BaseVector::rawNulls_) {
    BaseVector::setNull(idx, false);
  }
  if (value.isInline()) {
    rawValues_[idx] = value;
  } else {
    Buffer* buffer = getBufferWithSpace(value.size());
    char* ptr = buffer->asMutable<char>() + buffer->size();
    buffer->setSize(buffer->size() + value.size());
    memcpy(ptr, value.data(), value.size());
    rawValues_[idx] = StringView(ptr, value.size());
  }
}

/// For types that requires buffer allocation this should be called only if
/// value is inlined or if value is already allocated in a buffer within the
/// vector. Used by StringWriter to allow UDFs to write directly into the
/// buffers and avoid copying.
template <>
void FlatVector<StringView>::setNoCopy(
    const vector_size_t idx,
    const StringView& value) {
  VELOX_DCHECK(idx < BaseVector::length_);
  VELOX_DCHECK(values_->isMutable());
  rawValues_[idx] = value;
  if (BaseVector::nulls_) {
    BaseVector::setNull(idx, false);
  }
}

template <typename T>
void FlatVector<T>::acquireSharedStringBuffers(const BaseVector* source) {
  auto leaf = source->wrappedVector();
  if (leaf->typeKind() == TypeKind::UNKNOWN) {
    // If the source is all nulls, it can be of unknown type.
    return;
  }
  switch (leaf->encoding()) {
    case VectorEncoding::Simple::FLAT: {
      auto* flat = leaf->asUnchecked<FlatVector<StringView>>();
      for (auto& buffer : flat->stringBuffers_) {
        if (std::find(stringBuffers_.begin(), stringBuffers_.end(), buffer) ==
            stringBuffers_.end())
          stringBuffers_.push_back(buffer);
      }
      break;
    }
    case VectorEncoding::Simple::CONSTANT: {
      if (!leaf->isNullAt(0)) {
        auto* constant = leaf->asUnchecked<ConstantVector<StringView>>();
        auto buffer = constant->getStringBuffer();
        if (buffer &&
            std::find(stringBuffers_.begin(), stringBuffers_.end(), buffer) ==
                stringBuffers_.end())
          stringBuffers_.push_back(buffer);
      }
      break;
    }

    default:
      VELOX_CHECK(
          false,
          "Assigning a non-flat, non-constant vector to a string vector");
  }
}

template <>
void FlatVector<StringView>::copy(
    const BaseVector* source,
    const SelectivityVector& rows,
    const vector_size_t* toSourceRow) {
  if (!rows.hasSelections()) {
    return;
  }

  // Source can be of Unknown type, in that case it should have null values.
  if (source->typeKind() == TypeKind::UNKNOWN) {
    if (source->isConstantEncoding()) {
      DCHECK(source->isNullAt(0));
      rows.applyToSelected([&](vector_size_t row) { setNull(row, true); });
    } else {
      rows.applyToSelected([&](vector_size_t row) {
        auto sourceRow = toSourceRow ? toSourceRow[row] : row;
        DCHECK(source->isNullAt(sourceRow));
        setNull(row, true);
      });
    }
    return;
  }

  auto leaf = source->wrappedVector()->asUnchecked<SimpleVector<StringView>>();

  if (pool_ == leaf->pool()) {
    // We copy referencing the storage of 'source'.
    copyValuesAndNulls(source, rows, toSourceRow);
    acquireSharedStringBuffers(source);
  } else {
    rows.applyToSelected([&](vector_size_t row) {
      auto sourceRow = toSourceRow ? toSourceRow[row] : row;
      if (source->isNullAt(sourceRow)) {
        setNull(row, true);
      } else {
        set(row, leaf->valueAt(source->wrappedIndex(sourceRow)));
      }
    });
  }

  if (auto stringVector = source->as<SimpleVector<StringView>>()) {
    if (auto ascii = stringVector->isAscii(rows, toSourceRow)) {
      setIsAscii(ascii.value(), rows);
    } else {
      // ASCII-ness for the 'rows' is not known.
      ensureIsAsciiCapacity(rows.end());
      // If we arent All ascii, then invalidate
      // because the remaining selected rows might be ascii
      if (!isAllAscii_) {
        invalidateIsAscii();
      } else {
        asciiSetRows_.deselect(rows);
      }
    }
  }
}

template <>
void FlatVector<StringView>::copy(
    const BaseVector* source,
    vector_size_t targetIndex,
    vector_size_t sourceIndex,
    vector_size_t count) {
  auto leaf = source->wrappedVector()->asUnchecked<SimpleVector<StringView>>();
  if (pool_ == leaf->pool()) {
    // We copy referencing the storage of 'source'.
    copyValuesAndNulls(source, targetIndex, sourceIndex, count);
    acquireSharedStringBuffers(source);
  } else {
    for (auto i = 0; i < count; ++i) {
      if (source->isNullAt(sourceIndex + i)) {
        setNull(targetIndex + i, true);
      } else {
        set(targetIndex + i,
            leaf->valueAt(source->wrappedIndex(sourceIndex + i)));
      }
    }
  }
}
} // namespace velox
} // namespace facebook
