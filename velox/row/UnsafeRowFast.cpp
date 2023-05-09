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
#include "velox/row/UnsafeRowFast.h"

namespace facebook::velox::row {

namespace {
static const int32_t kFieldWidth = 8;

int32_t alignBits(int32_t numBits) {
  return bits::nwords(numBits) * 8;
}

int32_t alignBytes(int32_t numBytes) {
  return bits::roundUp(numBytes, 8);
}
} // namespace

// static
std::optional<int32_t> UnsafeRowFast::fixedRowSize(const RowTypePtr& rowType) {
  for (const auto& child : rowType->children()) {
    if (!child->isFixedWidth()) {
      return std::nullopt;
    }
  }

  const size_t numFields = rowType->size();
  const size_t nullLength = alignBits(numFields);

  return nullLength + numFields * kFieldWidth;
}

UnsafeRowFast::UnsafeRowFast(const RowVectorPtr& vector)
    : typeKind_{vector->typeKind()}, decoded_{*vector} {
  initialize(vector->type());
}

UnsafeRowFast::UnsafeRowFast(const VectorPtr& vector)
    : typeKind_{vector->typeKind()}, decoded_{*vector} {
  initialize(vector->type());
}

void UnsafeRowFast::initialize(const TypePtr& type) {
  auto base = decoded_.base();
  switch (typeKind_) {
    case TypeKind::ARRAY: {
      auto arrayBase = base->as<ArrayVector>();
      children_.push_back(UnsafeRowFast(arrayBase->elements()));
      childIsFixedWidth_.push_back(
          arrayBase->elements()->type()->isFixedWidth());
      break;
    }
    case TypeKind::MAP: {
      auto mapBase = base->as<MapVector>();
      children_.push_back(UnsafeRowFast(mapBase->mapKeys()));
      children_.push_back(UnsafeRowFast(mapBase->mapValues()));
      childIsFixedWidth_.push_back(mapBase->mapKeys()->type()->isFixedWidth());
      childIsFixedWidth_.push_back(
          mapBase->mapValues()->type()->isFixedWidth());
      break;
    }
    case TypeKind::ROW: {
      auto rowBase = base->as<RowVector>();
      for (const auto& child : rowBase->children()) {
        children_.push_back(UnsafeRowFast(child));
        childIsFixedWidth_.push_back(child->type()->isFixedWidth());
      }

      rowNullBytes_ = alignBits(type->size());
      break;
    }
    case TypeKind::BOOLEAN:
      valueBytes_ = 1;
      fixedWidthTypeKind_ = true;
      break;
    case TypeKind::TINYINT:
      FOLLY_FALLTHROUGH;
    case TypeKind::SMALLINT:
      FOLLY_FALLTHROUGH;
    case TypeKind::INTEGER:
      FOLLY_FALLTHROUGH;
    case TypeKind::BIGINT:
      FOLLY_FALLTHROUGH;
    case TypeKind::REAL:
      FOLLY_FALLTHROUGH;
    case TypeKind::DOUBLE:
      FOLLY_FALLTHROUGH;
    case TypeKind::DATE:
      valueBytes_ = type->cppSizeInBytes();
      fixedWidthTypeKind_ = true;
      supportsBulkCopy_ = decoded_.isIdentityMapping();
      break;
    case TypeKind::TIMESTAMP:
      valueBytes_ = sizeof(int64_t);
      fixedWidthTypeKind_ = true;
      break;
    case TypeKind::VARCHAR:
      FOLLY_FALLTHROUGH;
    case TypeKind::VARBINARY:
      // Nothing to do.
      break;
    default:
      VELOX_UNSUPPORTED("Unsupported type: {}", type->toString());
  }
}

int32_t UnsafeRowFast::rowSize(vector_size_t index) {
  return rowRowSize(index);
}

int32_t UnsafeRowFast::variableWidthRowSize(vector_size_t index) {
  switch (typeKind_) {
    case TypeKind::VARCHAR:
      FOLLY_FALLTHROUGH;
    case TypeKind::VARBINARY: {
      auto value = decoded_.valueAt<StringView>(index);
      return alignBytes(value.size());
    }
    case TypeKind::ARRAY:
      return arrayRowSize(index);
    case TypeKind::MAP:
      return mapRowSize(index);
    case TypeKind::ROW:
      return rowRowSize(index);
    default:
      VELOX_UNREACHABLE(
          "Unexpected type kind: {}", mapTypeKindToName(typeKind_));
  };
}

bool UnsafeRowFast::isNullAt(vector_size_t index) {
  return decoded_.isNullAt(index);
}

int32_t UnsafeRowFast::serialize(vector_size_t index, char* buffer) {
  return serializeRow(index, buffer);
}

void UnsafeRowFast::serializeFixedWidth(vector_size_t index, char* buffer) {
  VELOX_DCHECK(fixedWidthTypeKind_);
  switch (typeKind_) {
    case TypeKind::BOOLEAN:
      *reinterpret_cast<bool*>(buffer) = decoded_.valueAt<bool>(index);
      break;
    case TypeKind::TIMESTAMP:
      *reinterpret_cast<int64_t*>(buffer) =
          decoded_.valueAt<Timestamp>(index).toMicros();
      break;
    default:
      memcpy(
          buffer,
          decoded_.data<char>() + decoded_.index(index) * valueBytes_,
          valueBytes_);
  }
}

void UnsafeRowFast::serializeFixedWidth(
    vector_size_t offset,
    vector_size_t size,
    char* buffer) {
  VELOX_DCHECK(supportsBulkCopy_);
  memcpy(
      buffer,
      decoded_.data<char>() + decoded_.index(offset) * valueBytes_,
      valueBytes_ * size);
}

int32_t UnsafeRowFast::serializeVariableWidth(
    vector_size_t index,
    char* buffer) {
  switch (typeKind_) {
    case TypeKind::VARCHAR:
      FOLLY_FALLTHROUGH;
    case TypeKind::VARBINARY: {
      auto value = decoded_.valueAt<StringView>(index);
      memcpy(buffer, value.data(), value.size());
      return value.size();
    }
    case TypeKind::ARRAY:
      return serializeArray(index, buffer);
    case TypeKind::MAP:
      return serializeMap(index, buffer);
    case TypeKind::ROW:
      return serializeRow(index, buffer);
    default:
      VELOX_UNREACHABLE(
          "Unexpected type kind: {}", mapTypeKindToName(typeKind_));
  };
}

int32_t UnsafeRowFast::arrayRowSize(vector_size_t index) {
  auto baseIndex = decoded_.index(index);

  // array size | null bits | fixed-width data | variable-width data
  auto arrayBase = decoded_.base()->asUnchecked<ArrayVector>();
  auto offset = arrayBase->offsetAt(baseIndex);
  auto size = arrayBase->sizeAt(baseIndex);

  return arrayRowSize(children_[0], offset, size, childIsFixedWidth_[0]);
}

int32_t UnsafeRowFast::serializeArray(vector_size_t index, char* buffer) {
  auto baseIndex = decoded_.index(index);

  // array size | null bits | fixed-width data | variable-width data
  auto arrayBase = decoded_.base()->asUnchecked<ArrayVector>();
  auto offset = arrayBase->offsetAt(baseIndex);
  auto size = arrayBase->sizeAt(baseIndex);

  return serializeAsArray(
      children_[0], offset, size, childIsFixedWidth_[0], buffer);
}

int32_t UnsafeRowFast::mapRowSize(vector_size_t index) {
  auto baseIndex = decoded_.index(index);

  //  size of serialized keys array in bytes | <keys array> | <values array>

  auto mapBase = decoded_.base()->asUnchecked<MapVector>();
  auto offset = mapBase->offsetAt(baseIndex);
  auto size = mapBase->sizeAt(baseIndex);

  return kFieldWidth +
      arrayRowSize(children_[0], offset, size, childIsFixedWidth_[0]) +
      arrayRowSize(children_[1], offset, size, childIsFixedWidth_[1]);
}

int32_t UnsafeRowFast::serializeMap(vector_size_t index, char* buffer) {
  auto baseIndex = decoded_.index(index);

  //  size of serialized keys array in bytes | <keys array> | <values array>

  auto mapBase = decoded_.base()->asUnchecked<MapVector>();
  auto offset = mapBase->offsetAt(baseIndex);
  auto size = mapBase->sizeAt(baseIndex);

  int32_t serializedBytes = kFieldWidth;

  auto keysSerializedBytes = serializeAsArray(
      children_[0],
      offset,
      size,
      childIsFixedWidth_[0],
      buffer + serializedBytes);
  serializedBytes += keysSerializedBytes;

  auto valuesSerializedBytes = serializeAsArray(
      children_[1],
      offset,
      size,
      childIsFixedWidth_[1],
      buffer + serializedBytes);
  serializedBytes += valuesSerializedBytes;

  // Write the size of serialized keys.
  *reinterpret_cast<int64_t*>(buffer) = keysSerializedBytes;

  return serializedBytes;
}

int32_t UnsafeRowFast::arrayRowSize(
    UnsafeRowFast& elements,
    vector_size_t offset,
    vector_size_t size,
    bool fixedWidth) {
  int32_t nullBytes = alignBits(size);

  int32_t rowSize = kFieldWidth + nullBytes;
  if (fixedWidth) {
    return rowSize + size * elements.valueBytes();
  }

  rowSize += size * kFieldWidth;

  for (auto i = 0; i < size; ++i) {
    if (!elements.isNullAt(offset + i)) {
      rowSize += alignBytes(elements.variableWidthRowSize(offset + i));
    }
  }

  return rowSize;
}

int32_t UnsafeRowFast::serializeAsArray(
    UnsafeRowFast& elements,
    vector_size_t offset,
    vector_size_t size,
    bool fixedWidth,
    char* buffer) {
  // array size | null bits | fixed-width data | variable-width data

  // Write array size.
  *reinterpret_cast<int64_t*>(buffer) = size;

  int32_t nullBytes = alignBits(size);

  int32_t nullsOffset = sizeof(int64_t);
  int32_t fixedWidthOffset = nullsOffset + nullBytes;

  auto childSize = fixedWidth ? elements.valueBytes() : kFieldWidth;

  int64_t variableWidthOffset = fixedWidthOffset + size * childSize;

  if (elements.supportsBulkCopy_) {
    if (elements.decoded_.mayHaveNulls()) {
      for (auto i = 0; i < size; ++i) {
        if (elements.isNullAt(offset + i)) {
          bits::setBit(buffer + nullsOffset, i, true);
        }
      }
    }
    elements.serializeFixedWidth(offset, size, buffer + fixedWidthOffset);
    return variableWidthOffset;
  }

  for (auto i = 0; i < size; ++i) {
    if (elements.isNullAt(offset + i)) {
      bits::setBit(buffer + nullsOffset, i, true);
    } else {
      if (fixedWidth) {
        elements.serializeFixedWidth(
            offset + i, buffer + fixedWidthOffset + i * childSize);
      } else {
        auto serializedBytes = elements.serializeVariableWidth(
            offset + i, buffer + variableWidthOffset);

        // Write size and offset.
        uint64_t sizeAndOffset = variableWidthOffset << 32 | serializedBytes;
        reinterpret_cast<uint64_t*>(buffer + fixedWidthOffset)[i] =
            sizeAndOffset;

        variableWidthOffset += alignBytes(serializedBytes);
      }
    }
  }
  return variableWidthOffset;
}

int32_t UnsafeRowFast::rowRowSize(vector_size_t index) {
  auto childIndex = decoded_.index(index);

  const auto numFields = children_.size();
  int32_t size = rowNullBytes_ + numFields * kFieldWidth;

  for (auto i = 0; i < numFields; ++i) {
    if (!childIsFixedWidth_[i] && !children_[i].isNullAt(childIndex)) {
      size += alignBytes(children_[i].variableWidthRowSize(childIndex));
    }
  }

  return size;
}

int32_t UnsafeRowFast::serializeRow(vector_size_t index, char* buffer) {
  auto childIndex = decoded_.index(index);

  int64_t variableWidthOffset = rowNullBytes_ + kFieldWidth * children_.size();

  for (auto i = 0; i < children_.size(); ++i) {
    auto& child = children_[i];

    // Write null bit.
    if (child.isNullAt(childIndex)) {
      bits::setBit(buffer, i, true);
      continue;
    }

    // Write value.
    if (childIsFixedWidth_[i]) {
      child.serializeFixedWidth(
          childIndex, buffer + rowNullBytes_ + i * kFieldWidth);
    } else {
      auto size = child.serializeVariableWidth(
          childIndex, buffer + variableWidthOffset);
      // Write size and offset.
      uint64_t sizeAndOffset = variableWidthOffset << 32 | size;
      reinterpret_cast<uint64_t*>(buffer + rowNullBytes_)[i] = sizeAndOffset;

      variableWidthOffset += alignBytes(size);
    }
  }

  return variableWidthOffset;
}
} // namespace facebook::velox::row
