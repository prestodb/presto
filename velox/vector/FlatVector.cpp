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
#include "velox/vector/ComplexVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox {

template <>
const bool* FlatVector<bool>::rawValues() const {
  VELOX_UNSUPPORTED("rawValues() for bool is not supported");
}

template <>
bool FlatVector<bool>::valueAtFast(vector_size_t idx) const {
  return bits::isBitSet(reinterpret_cast<const uint64_t*>(rawValues_), idx);
}

template <>
Range<bool> FlatVector<bool>::asRange() const {
  return Range<bool>(rawValues<int64_t>(), 0, BaseVector::length_);
}

template <>
void FlatVector<bool>::set(vector_size_t idx, bool value) {
  VELOX_DCHECK_LT(idx, BaseVector::length_);
  ensureValues();
  VELOX_DCHECK(!values_->isView());
  if (BaseVector::rawNulls_) {
    BaseVector::setNull(idx, false);
  }
  bits::setBit(reinterpret_cast<uint64_t*>(rawValues_), idx, value);
}

template <>
Buffer* FlatVector<StringView>::getBufferWithSpace(
    size_t size,
    bool exactSize) {
  VELOX_DCHECK_GE(stringBuffers_.size(), stringBufferSet_.size());

  // Check if the last buffer is uniquely referenced and has enough space.
  Buffer* buffer =
      stringBuffers_.empty() ? nullptr : stringBuffers_.back().get();
  if (buffer && buffer->unique() &&
      buffer->size() + size <= buffer->capacity()) {
    return buffer;
  }

  // Allocate a new buffer.
  const size_t newSize = exactSize ? size : std::max(kInitialStringSize, size);
  BufferPtr newBuffer = AlignedBuffer::allocate<char>(newSize, pool());
  newBuffer->setSize(0);
  addStringBuffer(newBuffer);
  return newBuffer.get();
}

template <>
char* FlatVector<StringView>::getRawStringBufferWithSpace(
    size_t size,
    bool exactSize) {
  Buffer* buffer = getBufferWithSpace(size, exactSize);
  char* rawBuffer = buffer->asMutable<char>() + buffer->size();
  buffer->setSize(buffer->size() + size);
  return rawBuffer;
}

template <>
void FlatVector<StringView>::prepareForReuse() {
  BaseVector::prepareForReuse();

  // Check values buffer. Keep the buffer if singly-referenced and mutable.
  // Reset otherwise.
  if (values_ && !values_->isMutable()) {
    values_ = nullptr;
    rawValues_ = nullptr;
  }

  keepAtMostOneStringBuffer();

  // Clear the StringViews to avoid referencing freed memory.
  if (rawValues_) {
    for (auto i = 0; i < BaseVector::length_; ++i) {
      rawValues_[i] = StringView();
    }
  }
}

template <>
void FlatVector<StringView>::set(vector_size_t idx, StringView value) {
  VELOX_DCHECK_LT(idx, BaseVector::length_);
  ensureValues();
  VELOX_DCHECK(!values_->isView());
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
  VELOX_DCHECK_LT(idx, BaseVector::length_);
  ensureValues();
  VELOX_DCHECK(!values_->isView());
  if (BaseVector::nulls_) {
    BaseVector::setNull(idx, false);
  }
  rawValues_[idx] = value;
}

template <>
void FlatVector<StringView>::acquireSharedStringBuffers(
    const BaseVector* source) {
  if (!source) {
    return;
  }
  if (source->typeKind() != TypeKind::VARBINARY &&
      source->typeKind() != TypeKind::VARCHAR) {
    return;
  }
  source = source->wrappedVector();
  switch (source->encoding()) {
    case VectorEncoding::Simple::FLAT: {
      auto* flat = source->asUnchecked<FlatVector<StringView>>();
      for (auto& buffer : flat->stringBuffers_) {
        addStringBuffer(buffer);
      }
      break;
    }
    case VectorEncoding::Simple::CONSTANT: {
      if (!source->isNullAt(0)) {
        auto* constant = source->asUnchecked<ConstantVector<StringView>>();
        auto buffer = constant->getStringBuffer();
        if (buffer != nullptr) {
          addStringBuffer(buffer);
        }
      }
      break;
    }

    default:
      VELOX_UNREACHABLE(
          "unexpected encoding inside acquireSharedStringBuffers: {}",
          source->toString());
  }
}

template <>
void FlatVector<StringView>::acquireSharedStringBuffersRecursive(
    const BaseVector* source) {
  if (!source) {
    return;
  }
  source = source->wrappedVector();

  switch (source->encoding()) {
    case VectorEncoding::Simple::FLAT: {
      if (source->typeKind() != TypeKind::VARCHAR &&
          source->typeKind() != TypeKind::VARBINARY) {
        // Nothing to acquire.
        return;
      }
      auto* flat = source->asUnchecked<FlatVector<StringView>>();
      for (auto& buffer : flat->stringBuffers_) {
        addStringBuffer(buffer);
      }
      return;
    }

    case VectorEncoding::Simple::ARRAY: {
      acquireSharedStringBuffersRecursive(
          source->asUnchecked<ArrayVector>()->elements().get());
      return;
    }

    case VectorEncoding::Simple::MAP: {
      acquireSharedStringBuffersRecursive(
          source->asUnchecked<MapVector>()->mapKeys().get());
      acquireSharedStringBuffersRecursive(
          source->asUnchecked<MapVector>()->mapValues().get());
      return;
    }

    case VectorEncoding::Simple::ROW: {
      for (auto& child : source->asUnchecked<RowVector>()->children()) {
        acquireSharedStringBuffersRecursive(child.get());
      }
      return;
    }

    case VectorEncoding::Simple::CONSTANT: {
      // wrappedVector can be constant vector only if the underlying type is
      // primitive.
      if (source->typeKind() != TypeKind::VARCHAR &&
          source->typeKind() != TypeKind::VARBINARY) {
        // Nothing to acquire.
        return;
      }
      auto* constantVector = source->asUnchecked<ConstantVector<StringView>>();
      if (constantVector->isNullAt(0)) {
        return;
      }
      auto* constant = source->asUnchecked<ConstantVector<StringView>>();
      auto buffer = constant->getStringBuffer();
      if (buffer != nullptr) {
        addStringBuffer(buffer);
      }
      return;
    }

    case VectorEncoding::Simple::LAZY:
    case VectorEncoding::Simple::DICTIONARY:
    case VectorEncoding::Simple::SEQUENCE:
    case VectorEncoding::Simple::BIASED:
    case VectorEncoding::Simple::FUNCTION:
      VELOX_UNREACHABLE(
          "unexpected encoding inside acquireSharedStringBuffersRecursive: {}",
          source->toString());
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
    DecodedVector decoded(*source);
    uint64_t* rawNulls = const_cast<uint64_t*>(BaseVector::rawNulls_);
    if (decoded.mayHaveNulls()) {
      rawNulls = BaseVector::mutableRawNulls();
    }

    size_t totalBytes = 0;
    rows.applyToSelected([&](vector_size_t row) {
      const auto sourceRow = toSourceRow ? toSourceRow[row] : row;
      if (decoded.isNullAt(sourceRow)) {
        bits::setNull(rawNulls, row);
      } else {
        if (rawNulls) {
          bits::clearNull(rawNulls, row);
        }
        auto v = decoded.valueAt<StringView>(sourceRow);
        if (v.isInline()) {
          rawValues_[row] = v;
        } else {
          totalBytes += v.size();
        }
      }
    });

    if (totalBytes > 0) {
      auto* buffer = getRawStringBufferWithSpace(totalBytes);
      rows.applyToSelected([&](vector_size_t row) {
        const auto sourceRow = toSourceRow ? toSourceRow[row] : row;
        if (!decoded.isNullAt(sourceRow)) {
          auto v = decoded.valueAt<StringView>(sourceRow);
          if (!v.isInline()) {
            memcpy(buffer, v.data(), v.size());
            rawValues_[row] = StringView(buffer, v.size());
            buffer += v.size();
          }
        }
      });
    }
  }

  if (auto stringVector = source->as<SimpleVector<StringView>>()) {
    if (auto ascii = stringVector->isAscii(rows, toSourceRow)) {
      setIsAscii(ascii.value(), rows);
    } else {
      // ASCII-ness for the 'rows' is not known.
      ensureIsAsciiCapacity();
      // If we arent All ascii, then invalidate
      // because the remaining selected rows might be ascii
      if (!asciiInfo.isAllAscii()) {
        invalidateIsAscii();
      } else {
        asciiInfo.writeLockedAsciiComputedRows()->deselect(rows);
      }
    }
  }
}

// For strings, we also verify if they point to valid memory locations inside
// the string buffers.
template <>
void FlatVector<StringView>::validate(
    const VectorValidateOptions& options) const {
  SimpleVector<StringView>::validate(options);
  auto byteSize = BaseVector::byteSize<StringView>(BaseVector::size());
  if (byteSize == 0) {
    return;
  }
  VELOX_CHECK_NOT_NULL(values_);
  VELOX_CHECK_GE(values_->size(), byteSize);
  auto rawValues = values_->as<StringView>();

  for (auto i = 0; i < BaseVector::length_; ++i) {
    if (isNullAt(i)) {
      continue;
    }
    auto stringView = rawValues[i];
    if (!stringView.isInline()) {
      bool isValid = false;
      for (const auto& buffer : stringBuffers_) {
        auto start = buffer->as<char>();
        if (stringView.data() >= start &&
            stringView.data() < start + buffer->size()) {
          isValid = true;
          break;
        }
      }
      VELOX_CHECK(
          isValid,
          "String view at idx {} points outside of the string buffers",
          i);
    }
  }
}

} // namespace facebook::velox
