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
#include "velox/row/CompactRow.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::row {

CompactRow::CompactRow(const RowVectorPtr& vector)
    : typeKind_{vector->typeKind()}, decoded_{*vector} {
  initialize(vector->type());
}

CompactRow::CompactRow(const VectorPtr& vector)
    : typeKind_{vector->typeKind()}, decoded_{*vector} {
  initialize(vector->type());
}

void CompactRow::initialize(const TypePtr& type) {
  auto base = decoded_.base();
  switch (typeKind_) {
    case TypeKind::ARRAY: {
      auto arrayBase = base->as<ArrayVector>();
      children_.push_back(CompactRow(arrayBase->elements()));
      childIsFixedWidth_.push_back(
          arrayBase->elements()->type()->isFixedWidth());
      break;
    }
    case TypeKind::MAP: {
      auto mapBase = base->as<MapVector>();
      children_.push_back(CompactRow(mapBase->mapKeys()));
      children_.push_back(CompactRow(mapBase->mapValues()));
      childIsFixedWidth_.push_back(mapBase->mapKeys()->type()->isFixedWidth());
      childIsFixedWidth_.push_back(
          mapBase->mapValues()->type()->isFixedWidth());
      break;
    }
    case TypeKind::ROW: {
      auto rowBase = base->as<RowVector>();
      for (const auto& child : rowBase->children()) {
        children_.push_back(CompactRow(child));
        childIsFixedWidth_.push_back(child->type()->isFixedWidth());
      }

      rowNullBytes_ = bits::nbytes(type->size());
      break;
    }
    case TypeKind::BOOLEAN:
      valueBytes_ = 1;
      fixedWidthTypeKind_ = true;
      break;
    case TypeKind::TINYINT:
      [[fallthrough]];
    case TypeKind::SMALLINT:
      [[fallthrough]];
    case TypeKind::INTEGER:
      [[fallthrough]];
    case TypeKind::BIGINT:
      [[fallthrough]];
    case TypeKind::HUGEINT:
      [[fallthrough]];
    case TypeKind::REAL:
      [[fallthrough]];
    case TypeKind::DOUBLE:
      valueBytes_ = type->cppSizeInBytes();
      fixedWidthTypeKind_ = true;
      supportsBulkCopy_ = decoded_.isIdentityMapping();
      break;
    case TypeKind::TIMESTAMP:
      valueBytes_ = sizeof(int64_t);
      fixedWidthTypeKind_ = true;
      break;
    case TypeKind::VARCHAR:
      [[fallthrough]];
    case TypeKind::VARBINARY:
      // Nothing to do.
      break;
    case TypeKind::UNKNOWN:
      // UNKNOWN values are always nulls, hence, do not take up space.
      valueBytes_ = 0;
      fixedWidthTypeKind_ = true;
      supportsBulkCopy_ = true;
      break;
    default:
      VELOX_UNSUPPORTED("Unsupported type: {}", type->toString());
  }
}

namespace {
std::optional<int32_t> fixedValueSize(const TypePtr& type) {
  if (type->isTimestamp()) {
    return sizeof(int64_t);
  }
  if (type->isFixedWidth()) {
    return type->cppSizeInBytes();
  }
  return std::nullopt;
}
} // namespace

// static
std::optional<int32_t> CompactRow::fixedRowSize(const RowTypePtr& rowType) {
  const size_t numFields = rowType->size();
  const size_t nullLength = bits::nbytes(numFields);

  size_t size = nullLength;
  for (const auto& child : rowType->children()) {
    if (auto valueBytes = fixedValueSize(child)) {
      size += valueBytes.value();
    } else {
      return std::nullopt;
    }
  }

  return size;
}

int32_t CompactRow::rowSize(vector_size_t index) {
  return rowRowSize(index);
}

int32_t CompactRow::rowRowSize(vector_size_t index) {
  auto childIndex = decoded_.index(index);

  const auto numFields = children_.size();
  int32_t size = rowNullBytes_;

  for (auto i = 0; i < numFields; ++i) {
    if (childIsFixedWidth_[i]) {
      size += children_[i].valueBytes_;
    } else if (!children_[i].isNullAt(childIndex)) {
      size += children_[i].variableWidthRowSize(childIndex);
    }
  }

  return size;
}

int32_t CompactRow::serializeRow(vector_size_t index, char* buffer) {
  auto childIndex = decoded_.index(index);

  int64_t valuesOffset = rowNullBytes_;

  auto* nulls = reinterpret_cast<uint8_t*>(buffer);

  for (auto i = 0; i < children_.size(); ++i) {
    auto& child = children_[i];

    // Write null bit. Advance offset if 'fixed-width'.
    if (child.isNullAt(childIndex)) {
      bits::setBit(nulls, i, true);
      if (childIsFixedWidth_[i]) {
        valuesOffset += child.valueBytes_;
      }
      continue;
    }

    if (childIsFixedWidth_[i]) {
      // Write fixed-width value.
      if (child.valueBytes_ > 0) {
        child.serializeFixedWidth(childIndex, buffer + valuesOffset);
      }
      valuesOffset += child.valueBytes_;
    } else {
      // Write non-null variable-width value.
      auto size =
          child.serializeVariableWidth(childIndex, buffer + valuesOffset);
      valuesOffset += size;
    }
  }

  return valuesOffset;
}

bool CompactRow::isNullAt(vector_size_t index) {
  return decoded_.isNullAt(index);
}

int32_t CompactRow::variableWidthRowSize(vector_size_t index) {
  switch (typeKind_) {
    case TypeKind::VARCHAR:
      [[fallthrough]];
    case TypeKind::VARBINARY: {
      auto value = decoded_.valueAt<StringView>(index);
      return sizeof(int32_t) + value.size();
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

int32_t CompactRow::arrayRowSize(vector_size_t index) {
  auto baseIndex = decoded_.index(index);

  auto arrayBase = decoded_.base()->asUnchecked<ArrayVector>();
  auto offset = arrayBase->offsetAt(baseIndex);
  auto size = arrayBase->sizeAt(baseIndex);

  return arrayRowSize(children_[0], offset, size, childIsFixedWidth_[0]);
}

int32_t CompactRow::arrayRowSize(
    CompactRow& elements,
    vector_size_t offset,
    vector_size_t size,
    bool fixedWidth) {
  const int32_t nullBytes = bits::nbytes(size);

  // array size | null bits | elements

  // 4 bytes for number of elements, some bytes for null flags.
  int32_t rowSize = sizeof(int32_t) + nullBytes;
  if (fixedWidth) {
    return rowSize + size * elements.valueBytes();
  }

  if (size == 0) {
    return rowSize;
  }

  // If element type is a complex type, then add 4 bytes for overall serialized
  // size of the array + 4 bytes per element for offset of the serialized
  // element.
  // size | nulls | serialized size | serialized offset 1 | serialized offset 2
  // |...| element 1 | element 2 |...

  if (!(elements.typeKind_ == TypeKind::VARCHAR ||
        elements.typeKind_ == TypeKind::VARBINARY)) {
    // 4 bytes for the overall serialized size + 4 bytes for the offset of each
    // element.
    rowSize += sizeof(int32_t) + size * sizeof(int32_t);
  }

  for (auto i = 0; i < size; ++i) {
    if (!elements.isNullAt(offset + i)) {
      rowSize += elements.variableWidthRowSize(offset + i);
    }
  }

  return rowSize;
}

int32_t CompactRow::serializeArray(vector_size_t index, char* buffer) {
  auto baseIndex = decoded_.index(index);

  // For complex-type elements:
  // array size | null bits | serialized size | offset e1 | offset e2 |... | e1
  // | e2 |...
  //
  // 'serialized size' is the number of bytes starting after null bits and to
  // the end of the array. Offsets are specified relative to position right
  // after 'serialized size'.
  //
  // For fixed-width or string element type:
  // array size | null bite | e1 | e2 |...

  auto arrayBase = decoded_.base()->asUnchecked<ArrayVector>();
  auto offset = arrayBase->offsetAt(baseIndex);
  auto size = arrayBase->sizeAt(baseIndex);

  return serializeAsArray(
      children_[0], offset, size, childIsFixedWidth_[0], buffer);
}

namespace {

constexpr size_t kSizeBytes = sizeof(int32_t);

void writeInt32(char* buffer, int32_t n) {
  memcpy(buffer, &n, sizeof(int32_t));
}

int32_t readInt32(const char* buffer) {
  int32_t n;
  memcpy(&n, buffer, sizeof(int32_t));
  return n;
}
} // namespace

int32_t CompactRow::serializeAsArray(
    CompactRow& elements,
    vector_size_t offset,
    vector_size_t size,
    bool fixedWidth,
    char* buffer) {
  // For complex-type elements:
  // array size | null bits | serialized size | offset e1 | offset e2 |... | e1
  // | e2 |...
  //
  // For fixed-width and string element types:
  // array size | null bits | e1 | e2 |...

  // Write array size.
  writeInt32(buffer, size);

  // Write null flags.
  const int32_t nullBytes = bits::nbytes(size);
  const int32_t nullsOffset = kSizeBytes;

  int32_t elementsOffset = nullsOffset + nullBytes;

  auto* rawNulls = reinterpret_cast<uint8_t*>(buffer + nullsOffset);

  if (elements.supportsBulkCopy_) {
    if (elements.decoded_.mayHaveNulls()) {
      for (auto i = 0; i < size; ++i) {
        if (elements.isNullAt(offset + i)) {
          bits::setBit(rawNulls, i, true);
        }
      }
    }
    elements.serializeFixedWidth(offset, size, buffer + elementsOffset);
    return elementsOffset + size * elements.valueBytes_;
  }

  if (fixedWidth) {
    for (auto i = 0; i < size; ++i) {
      if (elements.isNullAt(offset + i)) {
        bits::setBit(rawNulls, i, true);
      } else {
        elements.serializeFixedWidth(offset + i, buffer + elementsOffset);
      }
      elementsOffset += elements.valueBytes_;
    }
  } else if (
      elements.typeKind_ == TypeKind::VARCHAR ||
      elements.typeKind_ == TypeKind::VARBINARY) {
    for (auto i = 0; i < size; ++i) {
      if (elements.isNullAt(offset + i)) {
        bits::setBit(rawNulls, i, true);
      } else {
        auto serializedBytes = elements.serializeVariableWidth(
            offset + i, buffer + elementsOffset);
        elementsOffset += serializedBytes;
      }
    }
  } else {
    if (size > 0) {
      // Leave room for serialized size and offsets.
      const size_t baseOffset = elementsOffset + kSizeBytes;
      elementsOffset += kSizeBytes + size * kSizeBytes;

      for (auto i = 0; i < size; ++i) {
        if (elements.isNullAt(offset + i)) {
          bits::setBit(rawNulls, i, true);
        } else {
          writeInt32(
              buffer + baseOffset + i * kSizeBytes,
              elementsOffset - baseOffset);

          auto serializedBytes = elements.serializeVariableWidth(
              offset + i, buffer + elementsOffset);

          elementsOffset += serializedBytes;
        }
      }

      writeInt32(buffer + baseOffset - kSizeBytes, elementsOffset - baseOffset);
    }
  }

  return elementsOffset;
}

int32_t CompactRow::mapRowSize(vector_size_t index) {
  auto baseIndex = decoded_.index(index);

  //  <keys array> | <values array>

  auto mapBase = decoded_.base()->asUnchecked<MapVector>();
  auto offset = mapBase->offsetAt(baseIndex);
  auto size = mapBase->sizeAt(baseIndex);

  return arrayRowSize(children_[0], offset, size, childIsFixedWidth_[0]) +
      arrayRowSize(children_[1], offset, size, childIsFixedWidth_[1]);
}

int32_t CompactRow::serializeMap(vector_size_t index, char* buffer) {
  auto baseIndex = decoded_.index(index);

  //  <keys array> | <values array>

  auto mapBase = decoded_.base()->asUnchecked<MapVector>();
  auto offset = mapBase->offsetAt(baseIndex);
  auto size = mapBase->sizeAt(baseIndex);

  auto keysSerializedBytes = serializeAsArray(
      children_[0], offset, size, childIsFixedWidth_[0], buffer);

  auto valuesSerializedBytes = serializeAsArray(
      children_[1],
      offset,
      size,
      childIsFixedWidth_[1],
      buffer + keysSerializedBytes);

  return keysSerializedBytes + valuesSerializedBytes;
}

int32_t CompactRow::serialize(vector_size_t index, char* buffer) {
  return serializeRow(index, buffer);
}

void CompactRow::serializeFixedWidth(vector_size_t index, char* buffer) {
  VELOX_DCHECK(fixedWidthTypeKind_);
  switch (typeKind_) {
    case TypeKind::BOOLEAN:
      *reinterpret_cast<bool*>(buffer) = decoded_.valueAt<bool>(index);
      break;
    case TypeKind::TIMESTAMP: {
      auto micros = decoded_.valueAt<Timestamp>(index).toMicros();
      memcpy(buffer, &micros, sizeof(int64_t));
      break;
    }
    default:
      memcpy(
          buffer,
          decoded_.data<char>() + decoded_.index(index) * valueBytes_,
          valueBytes_);
  }
}

void CompactRow::serializeFixedWidth(
    vector_size_t offset,
    vector_size_t size,
    char* buffer) {
  VELOX_DCHECK(supportsBulkCopy_);
  // decoded_.data<char>() can be null if all values are null.
  if (decoded_.data<char>()) {
    memcpy(
        buffer,
        decoded_.data<char>() + decoded_.index(offset) * valueBytes_,
        valueBytes_ * size);
  }
}

int32_t CompactRow::serializeVariableWidth(vector_size_t index, char* buffer) {
  switch (typeKind_) {
    case TypeKind::VARCHAR:
      [[fallthrough]];
    case TypeKind::VARBINARY: {
      auto value = decoded_.valueAt<StringView>(index);
      writeInt32(buffer, value.size());
      if (!value.empty()) {
        memcpy(buffer + kSizeBytes, value.data(), value.size());
      }
      return kSizeBytes + value.size();
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

namespace {

// Reads single fixed-width value from buffer into flatVector[index].
template <typename T>
void readFixedWidthValue(
    bool isNull,
    const char* buffer,
    FlatVector<T>* flatVector,
    vector_size_t index) {
  if (isNull) {
    flatVector->setNull(index, true);
  } else if constexpr (std::is_same_v<T, Timestamp>) {
    int64_t micros;
    memcpy(&micros, buffer, sizeof(int64_t));
    flatVector->set(index, Timestamp::fromMicros(micros));
  } else {
    T value;
    memcpy(&value, buffer, sizeof(T));
    flatVector->set(index, value);
  }
}

template <typename T>
int32_t valueSize() {
  if constexpr (std::is_same_v<T, Timestamp>) {
    return sizeof(int64_t);
  } else {
    return sizeof(T);
  }
}

// Deserializes one fixed-width value from each 'row' in 'data'.
// Each value starts at data[row].data() + offsets[row].
//
// @param nulls Null flags for the values.
// @param offsets Offsets in 'data' for the serialized values. Not used if value
// is null.
template <TypeKind Kind>
VectorPtr deserializeFixedWidth(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    const std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  using T = typename TypeTraits<Kind>::NativeType;

  const auto numRows = data.size();
  auto flatVector = BaseVector::create<FlatVector<T>>(type, numRows, pool);

  auto* rawNulls = nulls->as<uint64_t>();

  for (auto i = 0; i < numRows; ++i) {
    const bool isNull = bits::isBitNull(rawNulls, i);
    readFixedWidthValue<T>(
        isNull, data[i].data() + offsets[i], flatVector.get(), i);
  }

  return flatVector;
}

vector_size_t totalSize(const vector_size_t* rawSizes, size_t numRows) {
  vector_size_t total = 0;
  for (auto i = 0; i < numRows; ++i) {
    total += rawSizes[i];
  }
  return total;
}

const uint8_t* readNulls(const char* buffer) {
  return reinterpret_cast<const uint8_t*>(buffer);
}

// Deserializes multiple fixed-width values from each 'row' in 'data'.
// Each set of values starts at data[row].data() + offsets[row] and contains
// null flags followed by values. The number of values is provided in
// sizes[row].
// nulls | v1 | v2 | v3 |...
// Advances offsets past the last value.
template <TypeKind Kind>
VectorPtr deserializeFixedWidthArrays(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& sizes,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  using T = typename TypeTraits<Kind>::NativeType;

  const int32_t valueBytes = valueSize<T>();

  const auto numRows = data.size();
  auto* rawSizes = sizes->as<vector_size_t>();

  const auto total = totalSize(rawSizes, numRows);

  auto flatVector = BaseVector::create<FlatVector<T>>(type, total, pool);

  vector_size_t index = 0;
  for (auto i = 0; i < numRows; ++i) {
    const auto size = rawSizes[i];
    if (size > 0) {
      auto nullBytes = bits::nbytes(size);

      auto* rawElementNulls = readNulls(data[i].data() + offsets[i]);

      offsets[i] += nullBytes;

      for (auto j = 0; j < size; ++j) {
        readFixedWidthValue<T>(
            bits::isBitSet(rawElementNulls, j),
            data[i].data() + offsets[i],
            flatVector.get(),
            index);

        offsets[i] += valueBytes;
        ++index;
      }
    }
  }

  return flatVector;
}

int32_t readString(
    const char* buffer,
    FlatVector<StringView>* flatVector,
    vector_size_t index) {
  int32_t size = readInt32(buffer);
  StringView value(buffer + kSizeBytes, size);
  flatVector->set(index, value);
  return kSizeBytes + size;
}

VectorPtr deserializeUnknowns(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  return BaseVector::createNullConstant(UNKNOWN(), data.size(), pool);
}

// Deserializes one string from each 'row' in 'data'.
// Each strings starts at data[row].data() + offsets[row].
// string size | <string bytes>
// Advances the offsets past the strings.
VectorPtr deserializeStrings(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  auto flatVector =
      BaseVector::create<FlatVector<StringView>>(type, numRows, pool);

  auto* rawNulls = nulls->as<uint64_t>();

  for (auto i = 0; i < numRows; ++i) {
    if (bits::isBitNull(rawNulls, i)) {
      flatVector->setNull(i, true);
    } else {
      offsets[i] +=
          readString(data[i].data() + offsets[i], flatVector.get(), i);
    }
  }

  return flatVector;
}

VectorPtr deserializeUnknownArrays(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& sizes,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  auto* rawSizes = sizes->as<vector_size_t>();
  const auto total = totalSize(rawSizes, numRows);

  return BaseVector::createNullConstant(UNKNOWN(), total, pool);
}

// Deserializes multiple strings from each 'row' in 'data'.
// Each set of strings starts at data[row].data() + offsets[row] and contains
// null flags followed by the strings. The number of strings is provided in
// sizes[row].
// nulls | size-of-s1 | <s1> | size-of-s2 | <s2> |...
// Advances offsets past the last string.
VectorPtr deserializeStringArrays(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& sizes,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  auto* rawSizes = sizes->as<vector_size_t>();

  const auto total = totalSize(rawSizes, numRows);

  auto flatVector =
      BaseVector::create<FlatVector<StringView>>(type, total, pool);

  vector_size_t index = 0;
  for (auto i = 0; i < numRows; ++i) {
    const auto size = rawSizes[i];
    if (size > 0) {
      auto nullBytes = bits::nbytes(size);

      auto* rawElementNulls = readNulls(data[i].data() + offsets[i]);

      offsets[i] += nullBytes;

      for (auto j = 0; j < size; ++j) {
        if (bits::isBitSet(rawElementNulls, j)) {
          flatVector->setNull(index++, true);
        } else {
          offsets[i] +=
              readString(data[i].data() + offsets[i], flatVector.get(), index);
          ++index;
        }
      }
    }
  }

  return flatVector;
}

VectorPtr deserialize(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool);

// Deserializes multiple arrays from each 'row' in 'data'.
// Each set of arrays starts at data[row].data() + offsets[row] and contains
// null flags followed by the arrays. The number of arrays is provided in
// sizes[row].
// nulls | serializes size | offset-of-a1 | offset-of-a2 |...
// |size-of-a1 | nulls-of-a1-elements | <a1 elements> |...
//
// Advances offsets past the last array.
VectorPtr deserializeComplexArrays(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& sizes,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  auto* rawSizes = sizes->as<vector_size_t>();

  const auto total = totalSize(rawSizes, numRows);

  BufferPtr nulls = allocateNulls(total, pool);
  auto* rawNulls = nulls->asMutable<uint64_t>();

  std::vector<std::string_view> nestedData(total);
  std::vector<size_t> nestedOffsets(total, 0);

  vector_size_t nestedIndex = 0;
  for (auto i = 0; i < numRows; ++i) {
    const auto size = rawSizes[i];
    if (size > 0) {
      // Read nulls.
      auto* rawElementNulls = readNulls(data[i].data() + offsets[i]);
      offsets[i] += bits::nbytes(size);

      // Read serialized size.
      auto serializedSize = readInt32(data[i].data() + offsets[i]);
      offsets[i] += kSizeBytes;

      // Read offsets of individual elements.
      auto buffer = data[i].data() + offsets[i];
      for (auto j = 0; j < size; ++j) {
        if (bits::isBitSet(rawElementNulls, j)) {
          bits::setNull(rawNulls, nestedIndex);
        } else {
          int32_t nestedOffset = readInt32(buffer + j * kSizeBytes);
          nestedOffsets[nestedIndex] = offsets[i] + nestedOffset;
          nestedData[nestedIndex] = data[i];
        }
        ++nestedIndex;
      }

      offsets[i] += serializedSize;
    }
  }

  return deserialize(type, nestedData, nulls, nestedOffsets, pool);
}

// Deserializes one array from each 'row' in 'data'.
// Each array starts at data[row].data() + offsets[row].
// size | element nulls | serialized size (if complex type elements)
// | element offsets (if complex type elements) | e1 | e2 | e3 |...
//
// Advances the offsets past the arrays.
ArrayVectorPtr deserializeArrays(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();

  auto* rawNulls = nulls->as<uint64_t>();

  BufferPtr arrayOffsets = allocateOffsets(numRows, pool);
  auto* rawArrayOffsets = arrayOffsets->asMutable<vector_size_t>();

  BufferPtr arraySizes = allocateSizes(numRows, pool);
  auto* rawArraySizes = arraySizes->asMutable<vector_size_t>();

  vector_size_t arrayOffset = 0;

  for (auto i = 0; i < numRows; ++i) {
    if (!bits::isBitNull(rawNulls, i)) {
      // Read array size.
      int32_t size = readInt32(data[i].data() + offsets[i]);
      offsets[i] += kSizeBytes;

      rawArrayOffsets[i] = arrayOffset;
      rawArraySizes[i] = size;
      arrayOffset += size;
    }
  }

  VectorPtr elements;
  const auto& elementType = type->childAt(0);
  if (elementType->isUnKnown()) {
    elements =
        deserializeUnknownArrays(elementType, data, arraySizes, offsets, pool);
  } else if (elementType->isFixedWidth()) {
    elements = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        deserializeFixedWidthArrays,
        elementType->kind(),
        elementType,
        data,
        arraySizes,
        offsets,
        pool);
  } else {
    switch (elementType->kind()) {
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        elements = deserializeStringArrays(
            elementType, data, arraySizes, offsets, pool);
        break;
      case TypeKind::ARRAY:
      case TypeKind::MAP:
      case TypeKind::ROW:
        elements = deserializeComplexArrays(
            elementType, data, arraySizes, offsets, pool);
        break;
      default:
        VELOX_UNREACHABLE("{}", elementType->toString());
    }
  }

  return std::make_shared<ArrayVector>(
      pool, type, nulls, numRows, arrayOffsets, arraySizes, elements);
}

// Deserializes one map from each 'row' in 'data'.
// Each map starts at data[row].data() + offsets[row].
// array-of-keys | array-of-values
// Advances the offsets past the maps.
VectorPtr deserializeMaps(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  auto arrayOfKeysType = ARRAY(type->childAt(0));
  auto arrayOfValuesType = ARRAY(type->childAt(1));
  auto arrayOfKeys =
      deserializeArrays(arrayOfKeysType, data, nulls, offsets, pool);
  auto arrayOfValues =
      deserializeArrays(arrayOfValuesType, data, nulls, offsets, pool);

  return std::make_shared<MapVector>(
      pool,
      type,
      nulls,
      data.size(),
      arrayOfKeys->offsets(),
      arrayOfKeys->sizes(),
      arrayOfKeys->elements(),
      arrayOfValues->elements());
}

RowVectorPtr deserializeRows(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool);

// Switches on 'type' and calls type-specific deserialize method to deserialize
// one value from each 'row' in 'data' starting at the specified offset.
// Each value starts at data[row].data() + offsets[row].
// If 'type' is variable-width, advances 'offsets' past deserialized values.
// If 'type' is fixed-width, offsets remain unmodified.
VectorPtr deserialize(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto typeKind = type->kind();

  if (typeKind == TypeKind::UNKNOWN) {
    return deserializeUnknowns(type, data, nulls, offsets, pool);
  }

  if (type->isFixedWidth()) {
    return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        deserializeFixedWidth, typeKind, type, data, nulls, offsets, pool);
  }
  switch (typeKind) {
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return deserializeStrings(type, data, nulls, offsets, pool);
      break;
    case TypeKind::ARRAY:
      return deserializeArrays(type, data, nulls, offsets, pool);
      break;
    case TypeKind::MAP:
      return deserializeMaps(type, data, nulls, offsets, pool);
      break;
    case TypeKind::ROW:
      return deserializeRows(type, data, nulls, offsets, pool);
      break;
    default:
      VELOX_UNREACHABLE("{}", type->toString());
  }
}

// Deserializes one struct from each 'row' in 'data'.
// nulls | field1 | field2 |...
RowVectorPtr deserializeRows(
    const TypePtr& type,
    const std::vector<std::string_view>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  const size_t numFields = type->size();

  std::vector<VectorPtr> fields;

  auto* rawNulls = nulls != nullptr ? nulls->as<uint64_t>() : nullptr;

  std::vector<BufferPtr> fieldNulls;
  fieldNulls.reserve(numFields);
  for (auto i = 0; i < numFields; ++i) {
    fieldNulls.emplace_back(allocateNulls(numRows, pool));
    auto* rawFieldNulls = fieldNulls.back()->asMutable<uint8_t>();
    for (auto row = 0; row < numRows; ++row) {
      auto* serializedNulls = readNulls(data[row].data() + offsets[row]);
      const auto isNull =
          (rawNulls != nullptr && bits::isBitNull(rawNulls, row)) ||
          bits::isBitSet(serializedNulls, i);
      bits::setBit(rawFieldNulls, row, !isNull);
    }
  }

  const size_t nullLength = bits::nbytes(numFields);
  for (auto row = 0; row < numRows; ++row) {
    if (rawNulls != nullptr && bits::isBitNull(rawNulls, row)) {
      continue;
    }
    offsets[row] += nullLength;
  }

  for (auto i = 0; i < numFields; ++i) {
    const auto& child = type->childAt(i);
    auto field = deserialize(child, data, fieldNulls[i], offsets, pool);
    // If 'field' is fixed-width, advance offsets for rows where top-level
    // struct is not null.
    if (auto numBytes = fixedValueSize(child)) {
      for (auto row = 0; row < numRows; ++row) {
        const auto isTopLevelNull =
            rawNulls != nullptr && bits::isBitNull(rawNulls, row);
        if (!isTopLevelNull) {
          offsets[row] += numBytes.value();
        }
      }
    }
    fields.emplace_back(std::move(field));
  }

  return std::make_shared<RowVector>(
      pool, type, nulls, numRows, std::move(fields));
}

} // namespace

// static
RowVectorPtr CompactRow::deserialize(
    const std::vector<std::string_view>& data,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  std::vector<size_t> offsets(numRows, 0);

  return deserializeRows(rowType, data, nullptr, offsets, pool);
}

} // namespace facebook::velox::row
