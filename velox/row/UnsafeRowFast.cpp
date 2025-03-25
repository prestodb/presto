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
#include "velox/vector/FlatVector.h"

namespace facebook::velox::row {

namespace {
static const int32_t kFieldWidth = 8;
using TRowSize = uint32_t;

int32_t alignBits(int32_t numBits) {
  return bits::nwords(numBits) * 8;
}

int32_t alignBytes(int32_t numBytes) {
  return bits::roundUp(numBytes, 8);
}

bool isFixedWidth(const TypePtr& type) {
  return type->isFixedWidth() && !type->isLongDecimal();
}
} // namespace

// static
std::optional<int32_t> UnsafeRowFast::fixedRowSize(const RowTypePtr& rowType) {
  for (const auto& child : rowType->children()) {
    if (!isFixedWidth(child)) {
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
      childIsFixedWidth_.push_back(isFixedWidth(arrayBase->elements()->type()));
      break;
    }
    case TypeKind::MAP: {
      auto mapBase = base->as<MapVector>();
      children_.push_back(UnsafeRowFast(mapBase->mapKeys()));
      children_.push_back(UnsafeRowFast(mapBase->mapValues()));
      childIsFixedWidth_.push_back(isFixedWidth(mapBase->mapKeys()->type()));
      childIsFixedWidth_.push_back(isFixedWidth(mapBase->mapValues()->type()));
      break;
    }
    case TypeKind::ROW: {
      auto rowBase = base->as<RowVector>();
      for (const auto& child : rowBase->children()) {
        children_.push_back(UnsafeRowFast(child));
        childIsFixedWidth_.push_back(isFixedWidth(child->type()));
      }

      rowNullBytes_ = alignBits(type->size());
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
    case TypeKind::REAL:
      [[fallthrough]];
    case TypeKind::DOUBLE:
      [[fallthrough]];
    case TypeKind::UNKNOWN:
      valueBytes_ = type->cppSizeInBytes();
      fixedWidthTypeKind_ = true;
      supportsBulkCopy_ = decoded_.isIdentityMapping();
      break;
    case TypeKind::TIMESTAMP:
      valueBytes_ = sizeof(int64_t);
      fixedWidthTypeKind_ = true;
      break;
    case TypeKind::HUGEINT:
      [[fallthrough]];
    case TypeKind::VARCHAR:
      [[fallthrough]];
    case TypeKind::VARBINARY:
      // Nothing to do.
      break;
    default:
      VELOX_UNSUPPORTED("Unsupported type: {}", type->toString());
  }
}

void UnsafeRowFast::serializedRowSizes(
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes) const {
  if (const auto fixedRowSize =
          UnsafeRowFast::fixedRowSize(asRowType(decoded_.base()->type()))) {
    for (const auto row : rows) {
      *sizes[row] = fixedRowSize.value() + sizeof(TRowSize);
    }
    return;
  }

  for (const auto& row : rows) {
    *sizes[row] = rowSize(row) + sizeof(TRowSize);
  }
}

int32_t UnsafeRowFast::rowSize(vector_size_t index) const {
  return rowRowSize(index);
}

int32_t UnsafeRowFast::variableWidthRowSize(vector_size_t index) const {
  switch (typeKind_) {
    case TypeKind::VARCHAR:
      [[fallthrough]];
    case TypeKind::VARBINARY: {
      auto value = decoded_.valueAt<StringView>(index);
      return alignBytes(value.size());
    }
    case TypeKind::HUGEINT:
      return DecimalUtil::getByteArrayLength(decoded_.valueAt<int128_t>(index));
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

bool UnsafeRowFast::isNullAt(vector_size_t index) const {
  return decoded_.isNullAt(index);
}

int32_t UnsafeRowFast::serialize(vector_size_t index, char* buffer) const {
  return serializeRow(index, buffer);
}

void UnsafeRowFast::serializeFixedWidth(vector_size_t index, char* buffer)
    const {
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
    char* buffer) const {
  VELOX_DCHECK(supportsBulkCopy_);
  // decoded_.data<char>() can be null if all values are null.
  if (decoded_.data<char>()) {
    memcpy(
        buffer,
        decoded_.data<char>() + decoded_.index(offset) * valueBytes_,
        valueBytes_ * size);
  }
}

int32_t UnsafeRowFast::serializeVariableWidth(vector_size_t index, char* buffer)
    const {
  switch (typeKind_) {
    case TypeKind::VARCHAR:
      [[fallthrough]];
    case TypeKind::VARBINARY: {
      auto value = decoded_.valueAt<StringView>(index);
      memcpy(buffer, value.data(), value.size());
      return value.size();
    }
    case TypeKind::HUGEINT: {
      auto value = decoded_.valueAt<int128_t>(index);
      return DecimalUtil::toByteArray(value, buffer);
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

int32_t UnsafeRowFast::arrayRowSize(vector_size_t index) const {
  auto baseIndex = decoded_.index(index);

  // array size | null bits | fixed-width data | variable-width data
  auto arrayBase = decoded_.base()->asUnchecked<ArrayVector>();
  auto offset = arrayBase->offsetAt(baseIndex);
  auto size = arrayBase->sizeAt(baseIndex);

  return arrayRowSize(children_[0], offset, size, childIsFixedWidth_[0]);
}

int32_t UnsafeRowFast::serializeArray(vector_size_t index, char* buffer) const {
  auto baseIndex = decoded_.index(index);

  // array size | null bits | fixed-width data | variable-width data
  auto arrayBase = decoded_.base()->asUnchecked<ArrayVector>();
  auto offset = arrayBase->offsetAt(baseIndex);
  auto size = arrayBase->sizeAt(baseIndex);

  return serializeAsArray(
      children_[0], offset, size, childIsFixedWidth_[0], buffer);
}

int32_t UnsafeRowFast::mapRowSize(vector_size_t index) const {
  auto baseIndex = decoded_.index(index);

  //  size of serialized keys array in bytes | <keys array> | <values array>

  auto mapBase = decoded_.base()->asUnchecked<MapVector>();
  auto offset = mapBase->offsetAt(baseIndex);
  auto size = mapBase->sizeAt(baseIndex);

  return kFieldWidth +
      arrayRowSize(children_[0], offset, size, childIsFixedWidth_[0]) +
      arrayRowSize(children_[1], offset, size, childIsFixedWidth_[1]);
}

int32_t UnsafeRowFast::serializeMap(vector_size_t index, char* buffer) const {
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
    const UnsafeRowFast& elements,
    vector_size_t offset,
    vector_size_t size,
    bool fixedWidth) const {
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
    const UnsafeRowFast& elements,
    vector_size_t offset,
    vector_size_t size,
    bool fixedWidth,
    char* buffer) const {
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

int32_t UnsafeRowFast::rowRowSize(vector_size_t index) const {
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

int32_t UnsafeRowFast::serializeRow(vector_size_t index, char* buffer) const {
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

namespace {
// Reads single fixed-width value from buffer into rawValue[index].
template <typename T>
inline void
readFixedWidthValue(const char* buffer, T* rawValue, vector_size_t index) {
  if constexpr (std::is_same_v<T, Timestamp>) {
    int64_t micros;
    ::memcpy(&micros, buffer, sizeof(int64_t));
    rawValue[index] = Timestamp::fromMicros(micros);
  } else {
    rawValue[index] = reinterpret_cast<const T*>(buffer)[0];
  }
}

template <typename T>
inline int32_t valueSize() {
  if constexpr (std::is_same_v<T, Timestamp>) {
    return sizeof(int64_t);
  } else {
    return sizeof(T);
  }
}

// Deserializes one fixed-width value from each 'row' in 'data'.
// Each value starts at data[row] + offsets[row].
// Advances the offsets past the fixed field width.
// @param nulls Null flags for the values.
// @param offsets Offsets in 'data' for the serialized values.
template <TypeKind Kind>
VectorPtr deserializeFixedWidth(
    const TypePtr& type,
    const std::vector<char*>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  using T = typename TypeTraits<Kind>::NativeType;

  const auto numRows = data.size();
  auto flatVector = BaseVector::create<FlatVector<T>>(type, numRows, pool);
  auto rawValues = flatVector->mutableRawValues();
  auto* rawNulls = nulls->as<uint64_t>();

  for (auto i = 0; i < numRows; ++i) {
    const bool isNull = bits::isBitNull(rawNulls, i);
    if (isNull) {
      flatVector->setNull(i, true);
    } else {
      readFixedWidthValue<T>(data[i] + offsets[i], rawValues, i);
    }
    offsets[i] += kFieldWidth;
  }

  return flatVector;
}

template <>
VectorPtr deserializeFixedWidth<TypeKind::BOOLEAN>(
    const TypePtr& type,
    const std::vector<char*>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  auto flatVector = BaseVector::create<FlatVector<bool>>(type, numRows, pool);
  auto rawValues = flatVector->mutableRawValues<uint64_t>();
  auto* rawNulls = nulls->as<uint64_t>();

  for (auto i = 0; i < numRows; ++i) {
    const bool isNull = bits::isBitNull(rawNulls, i);
    if (isNull) {
      flatVector->setNull(i, true);
    } else {
      bool value = reinterpret_cast<const bool*>(data[i] + offsets[i])[0];
      bits::setBit(rawValues, i, value);
    }
    offsets[i] += kFieldWidth;
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

inline const uint8_t* readNulls(const char* buffer) {
  return reinterpret_cast<const uint8_t*>(buffer);
}

// Deserializes multiple fixed-width values from each 'row' in 'data'.
// Each set of values starts at data[row] + offsets[row] and contains
// null flags followed by values. The number of values is provided in
// sizes[row].
// The serialization format is: [null bits][values or offset&length][variable
// length portion] The fixed value in Row is stored as 8 bytes, but in array,
// stores as its cpp size, for example, 8 bytes in row and 4 bytes in array for
// int32_t.
// @param arrayStartOffsets data[row] + arrayStartOffsets[row] is this array
// start address. For array, it is always 0, but for map, key and value is
// stored as two arrays, offset value is always 8 (unsafe key array numBytes)
// for key array, and 8 + `key array numBytes` for value array
// @param arrayDataOffsets The offset to the start of the data in the array.
template <TypeKind Kind>
VectorPtr deserializeFixedWidthArrays(
    const TypePtr& type,
    const std::vector<char*>& data,
    const BufferPtr& sizes,
    const std::vector<int32_t>& arrayStartOffsets,
    std::vector<int32_t>& arrayDataOffsets,
    memory::MemoryPool* pool) {
  using T = typename TypeTraits<Kind>::NativeType;

  const int32_t valueBytes = valueSize<T>();

  const auto numRows = data.size();
  auto* rawSizes = sizes->as<vector_size_t>();

  const auto total = totalSize(rawSizes, numRows);

  auto flatVector = BaseVector::create<FlatVector<T>>(type, total, pool);
  auto rawValues = flatVector->mutableRawValues();
  vector_size_t index = 0;

  for (auto i = 0; i < numRows; ++i) {
    const auto size = rawSizes[i];
    if (size > 0) {
      auto nullBytes = alignBits(size);

      auto* rawElementNulls =
          readNulls(data[i] + arrayStartOffsets[i] + arrayDataOffsets[i]);

      arrayDataOffsets[i] += nullBytes;

      for (auto j = 0; j < size; ++j, index++) {
        if (bits::isBitSet(rawElementNulls, j)) {
          flatVector->setNull(index, true);
        } else {
          if constexpr (Kind == TypeKind::BOOLEAN) {
            bool value = reinterpret_cast<const bool*>(
                data[i] + arrayStartOffsets[i] + arrayDataOffsets[i])[0];
            flatVector->set(index, value);
          } else {
            readFixedWidthValue<T>(
                data[i] + arrayStartOffsets[i] + arrayDataOffsets[i],
                rawValues,
                index);
          }
        }

        arrayDataOffsets[i] += valueBytes;
      }
    }
  }

  return flatVector;
}

inline const int32_t* readInt32Ptr(const char* buffer) {
  return reinterpret_cast<const int32_t*>(buffer);
}

inline int32_t readInt32(const char* buffer) {
  return *reinterpret_cast<const int32_t*>(buffer);
}

inline int64_t readInt64(const char* buffer) {
  return *reinterpret_cast<const int64_t*>(buffer);
}

// Reads the offset to start and length from buffer, set flatVector[index].
// @param buffer Stores the string length and offset to start address.
// @param start Start address of the row.
inline void readString(
    const char* buffer,
    const char* start,
    FlatVector<StringView>* flatVector,
    vector_size_t index) {
  const auto* sizeAndOffset = readInt32Ptr(buffer);
  StringView value(start + sizeAndOffset[1], sizeAndOffset[0]);
  flatVector->set(index, value);
}

// Reads the offset to start and length from buffer, set rawValue[index].
void readLongDecimal(
    const char* buffer,
    const char* start,
    int128_t* rawValue,
    vector_size_t index) {
  const auto* sizeAndOffset = readInt32Ptr(buffer);
  auto length = sizeAndOffset[0];
  const uint8_t* bytesValue =
      reinterpret_cast<const uint8_t*>(start + sizeAndOffset[1]);
  int128_t bigEndianValue = static_cast<int8_t>(bytesValue[0]) >= 0 ? 0 : -1;
  memcpy(
      reinterpret_cast<char*>(&bigEndianValue) + sizeof(int128_t) - length,
      bytesValue,
      length);
  rawValue[index] = bits::builtin_bswap128(bigEndianValue);
}

VectorPtr deserializeUnknowns(
    const TypePtr& type,
    const std::vector<char*>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  for (auto i = 0; i < numRows; ++i) {
    offsets[i] += kFieldWidth;
  }
  return BaseVector::createNullConstant(UNKNOWN(), numRows, pool);
}

// Deserializes one string from each 'row' in 'data'.
// Each string length and offset stores at data[row] + offsets[row].
// The string starts at data[row] + wordOffset.
// String format is:
// <string size and offset> |...| <string bytes>
// Advances the offsets past the fixed field width.
VectorPtr deserializeStrings(
    const TypePtr& type,
    const std::vector<char*>& data,
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
      readString(data[i] + offsets[i], data[i], flatVector.get(), i);
    }
    offsets[i] += kFieldWidth;
  }

  return flatVector;
}

VectorPtr deserializeLongDecimal(
    const TypePtr& type,
    const std::vector<char*>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  auto flatVector =
      BaseVector::create<FlatVector<int128_t>>(type, numRows, pool);
  auto rawValues = flatVector->mutableRawValues();

  auto* rawNulls = nulls->as<uint64_t>();

  for (auto i = 0; i < numRows; ++i) {
    if (bits::isBitNull(rawNulls, i)) {
      flatVector->setNull(i, true);
    } else {
      readLongDecimal(data[i] + offsets[i], data[i], rawValues, i);
    }
    offsets[i] += kFieldWidth;
  }

  return flatVector;
}

VectorPtr deserializeUnknownArrays(
    const TypePtr& type,
    const std::vector<char*>& data,
    const BufferPtr& sizes,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  auto* rawSizes = sizes->as<vector_size_t>();
  const auto total = totalSize(rawSizes, numRows);

  return BaseVector::createNullConstant(UNKNOWN(), total, pool);
}

VectorPtr deserializeLongDecimalArrays(
    const TypePtr& type,
    const std::vector<char*>& data,
    const BufferPtr& sizes,
    const std::vector<int32_t>& arrayStartOffsets,
    std::vector<int32_t>& arrayDataOffsets,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  auto* rawSizes = sizes->as<vector_size_t>();

  const auto total = totalSize(rawSizes, numRows);

  auto flatVector = BaseVector::create<FlatVector<int128_t>>(type, total, pool);
  auto rawValues = flatVector->mutableRawValues();

  vector_size_t index = 0;
  for (auto i = 0; i < numRows; ++i) {
    const auto size = rawSizes[i];
    if (size > 0) {
      auto nullBytes = alignBits(size);

      auto* rawElementNulls =
          readNulls(data[i] + arrayStartOffsets[i] + arrayDataOffsets[i]);

      arrayDataOffsets[i] += nullBytes;

      for (auto j = 0; j < size; ++j, index++) {
        if (bits::isBitSet(rawElementNulls, j)) {
          flatVector->setNull(index, true);
        } else {
          readLongDecimal(
              data[i] + arrayStartOffsets[i] + arrayDataOffsets[i],
              data[i] + arrayStartOffsets[i],
              rawValues,
              index);
        }
        arrayDataOffsets[i] += kFieldWidth;
      }
    }
  }

  return flatVector;
}

// Deserializes multiple strings from each 'row' in 'data'.
// Each set of strings starts at data[row] + offsets and contains
// null flags followed by the strings. The number of strings is provided in
// sizes[row].
// nulls | size-and-offset-s1 | size-and-offset-s2 | <s1> | <s2> |...
// Advances arrayDataOffsets past the fixed field width.
VectorPtr deserializeStringArrays(
    const TypePtr& type,
    const std::vector<char*>& data,
    const BufferPtr& sizes,
    const std::vector<int32_t>& arrayStartOffsets,
    std::vector<int32_t>& arrayDataOffsets,
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
      auto nullBytes = alignBits(size);

      auto* rawElementNulls =
          readNulls(data[i] + arrayStartOffsets[i] + arrayDataOffsets[i]);

      arrayDataOffsets[i] += nullBytes;

      for (auto j = 0; j < size; ++j, index++) {
        if (bits::isBitSet(rawElementNulls, j)) {
          flatVector->setNull(index, true);
        } else {
          readString(
              data[i] + arrayStartOffsets[i] + arrayDataOffsets[i],
              data[i] + arrayStartOffsets[i],
              flatVector.get(),
              index);
        }
        arrayDataOffsets[i] += kFieldWidth;
      }
    }
  }

  return flatVector;
}

VectorPtr deserialize(
    const TypePtr& type,
    const std::vector<char*>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool);

// Deserializes multiple arrays from each 'row' in 'data'.
// Each set of arrays starts at data[row] + offsets and contains
// null flags followed by the arrays. The number of arrays is provided in
// sizes[row].
// nulls | size-offset-a1 | size-offset-a2 | <a1 elements> |...
VectorPtr deserializeComplexArrays(
    const TypePtr& type,
    const std::vector<char*>& data,
    const BufferPtr& sizes,
    const std::vector<int32_t>& arrayStartOffsets,
    std::vector<int32_t>& arrayDataOffsets,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  auto* rawSizes = sizes->as<vector_size_t>();

  const auto total = totalSize(rawSizes, numRows);

  BufferPtr nulls = allocateNulls(total, pool);
  auto* rawNulls = nulls->asMutable<uint64_t>();

  std::vector<char*> nestedData(total);
  std::vector<size_t> nestedOffsets(total, 0);

  vector_size_t nestedIndex = 0;
  for (auto i = 0; i < numRows; ++i) {
    const auto size = rawSizes[i];
    if (size > 0) {
      // Read nulls.
      const auto* rawElementNulls =
          readNulls(data[i] + arrayStartOffsets[i] + arrayDataOffsets[i]);
      const auto nullLength = alignBits(size);
      arrayDataOffsets[i] += nullLength;
      for (auto j = 0; j < size; ++j, ++nestedIndex) {
        if (bits::isBitSet(rawElementNulls, j)) {
          bits::setNull(rawNulls, nestedIndex);
        } else {
          // Read offset of individual elements.
          const auto offset = readInt32(
              data[i] + arrayStartOffsets[i] + arrayDataOffsets[i] +
              sizeof(int32_t));
          nestedData[nestedIndex] = data[i] + arrayStartOffsets[i] + offset;
        }
        arrayDataOffsets[i] += kFieldWidth;
      }
    }
  }

  return deserialize(type, nestedData, nulls, nestedOffsets, pool);
}

enum class DeserializeArrayType {
  ARRAY,
  MAP_KEY,
  MAP_VALUE,
};

// Deserializes one array from each 'row' in 'data'.
// Array format is:
// [numElements][null bits][values or offset&length][variable length portion]
template <DeserializeArrayType DeserializeType = DeserializeArrayType::ARRAY>
ArrayVectorPtr deserializeArrays(
    const TypePtr& type,
    const std::vector<char*>& data,
    const BufferPtr& nulls,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();

  auto* rawNulls = nulls->as<uint64_t>();

  BufferPtr arrayOffsets = allocateOffsets(numRows, pool);
  auto* rawArrayOffsets = arrayOffsets->asMutable<vector_size_t>();

  BufferPtr arraySizes = allocateSizes(numRows, pool);
  auto* rawArraySizes = arraySizes->asMutable<vector_size_t>();

  vector_size_t arrayOffset = 0;
  // The array starts from data[row] + arrayStartOffsets[row]
  std::vector<int32_t> arrayStartOffsets(numRows, 0);
  // The offset to the start address of array.
  std::vector<int32_t> arrayDataOffsets(numRows, 0);
  for (auto i = 0; i < numRows; ++i) {
    if (!bits::isBitNull(rawNulls, i)) {
      if constexpr (DeserializeType == DeserializeArrayType::MAP_KEY) {
        // Skip the key array size.
        arrayStartOffsets[i] += sizeof(int64_t);
      } else if constexpr (DeserializeType == DeserializeArrayType::MAP_VALUE) {
        const auto keyArraySize = readInt64(data[i]);
        arrayStartOffsets[i] += sizeof(int64_t) + keyArraySize;
      }
      vector_size_t numElements = readInt64(data[i] + arrayStartOffsets[i]);
      arrayDataOffsets[i] += sizeof(int64_t);

      rawArrayOffsets[i] = arrayOffset;
      rawArraySizes[i] = numElements;
      arrayOffset += numElements;
    }
  }

  VectorPtr elements;
  const auto& elementType = type->childAt(0);
  if (elementType->isUnKnown()) {
    elements = deserializeUnknownArrays(elementType, data, arraySizes, pool);
  } else if (isFixedWidth(elementType)) {
    elements = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        deserializeFixedWidthArrays,
        elementType->kind(),
        elementType,
        data,
        arraySizes,
        arrayStartOffsets,
        arrayDataOffsets,
        pool);
  } else {
    switch (elementType->kind()) {
      case TypeKind::HUGEINT:
        elements = deserializeLongDecimalArrays(
            elementType,
            data,
            arraySizes,
            arrayStartOffsets,
            arrayDataOffsets,
            pool);
        break;
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        elements = deserializeStringArrays(
            elementType,
            data,
            arraySizes,
            arrayStartOffsets,
            arrayDataOffsets,
            pool);
        break;
      case TypeKind::ARRAY:
      case TypeKind::MAP:
      case TypeKind::ROW:
        elements = deserializeComplexArrays(
            elementType,
            data,
            arraySizes,
            arrayStartOffsets,
            arrayDataOffsets,
            pool);
        break;
      default:
        VELOX_UNREACHABLE("{}", elementType->toString());
    }
  }

  return std::make_shared<ArrayVector>(
      pool, type, nulls, numRows, arrayOffsets, arraySizes, elements);
}

// Deserializes one map from each 'row' in 'data'.
// Map format is:
// key-array-bytes | array-of-keys | array-of-values
VectorPtr deserializeMaps(
    const TypePtr& type,
    const std::vector<char*>& data,
    const BufferPtr& nulls,
    memory::MemoryPool* pool) {
  auto arrayOfKeysType = ARRAY(type->childAt(0));
  auto arrayOfValuesType = ARRAY(type->childAt(1));
  auto arrayOfKeys = deserializeArrays<DeserializeArrayType::MAP_KEY>(
      arrayOfKeysType, data, nulls, pool);
  auto arrayOfValues = deserializeArrays<DeserializeArrayType::MAP_VALUE>(
      arrayOfValuesType, data, nulls, pool);

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
    const std::vector<char*>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool);

// Switches on 'type' and calls type-specific deserialize method to deserialize
// one value from each 'row' in 'data'.
VectorPtr deserialize(
    const TypePtr& type,
    const std::vector<char*>& data,
    const BufferPtr& nulls,
    std::vector<size_t>& offsets,
    memory::MemoryPool* pool) {
  const auto typeKind = type->kind();

  if (typeKind == TypeKind::UNKNOWN) {
    return deserializeUnknowns(type, data, nulls, offsets, pool);
  }

  if (isFixedWidth(type)) {
    return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        deserializeFixedWidth, typeKind, type, data, nulls, offsets, pool);
  }

  switch (typeKind) {
    case TypeKind::HUGEINT:
      return deserializeLongDecimal(type, data, nulls, offsets, pool);
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return deserializeStrings(type, data, nulls, offsets, pool);
      break;
    case TypeKind::ARRAY:
      return deserializeArrays(type, data, nulls, pool);
      break;
    case TypeKind::MAP:
      return deserializeMaps(type, data, nulls, pool);
      break;
    case TypeKind::ROW:
      return deserializeRows(type, data, nulls, offsets, pool);
      break;
    default:
      VELOX_UNREACHABLE("{}", type->toString());
  }
}

// Deserializes one struct from each 'row' in 'data'.
// Each tuple has three parts: [null-tracking bit set] [values] [variable length
// portion]
// If the type is complex data type, extract the nested data and deserialize it.
RowVectorPtr deserializeRows(
    const TypePtr& type,
    const std::vector<char*>& data,
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
      auto* serializedNulls = readNulls(data[row] + offsets[row]);
      const auto isNull =
          (rawNulls != nullptr && bits::isBitNull(rawNulls, row)) ||
          bits::isBitSet(serializedNulls, i);
      bits::setBit(rawFieldNulls, row, !isNull);
    }
  }

  const size_t nullLength = alignBits(numFields);
  for (auto row = 0; row < numRows; ++row) {
    if (rawNulls != nullptr && bits::isBitNull(rawNulls, row)) {
      continue;
    }
    offsets[row] += nullLength;
  }

  for (auto i = 0; i < numFields; ++i) {
    const auto& child = type->childAt(i);
    if (!child->isPrimitiveType()) {
      std::vector<char*> nestedData(numRows);
      std::vector<size_t> nestedOffsets(numRows, 0);
      for (auto row = 0; row < numRows; ++row) {
        const auto offset =
            readInt32(data[row] + offsets[row] + sizeof(int32_t));
        nestedData[row] = data[row] + offset;
        offsets[row] += kFieldWidth;
      }
      auto field =
          deserialize(child, nestedData, fieldNulls[i], nestedOffsets, pool);
      fields.emplace_back(std::move(field));
    } else {
      auto field = deserialize(child, data, fieldNulls[i], offsets, pool);
      fields.emplace_back(std::move(field));
    }
  }

  return std::make_shared<RowVector>(
      pool, type, nulls, numRows, std::move(fields));
}

} // namespace

// static
RowVectorPtr UnsafeRowFast::deserialize(
    const std::vector<char*>& data,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool) {
  const auto numRows = data.size();
  std::vector<size_t> offsets(numRows, 0);

  return deserializeRows(rowType, data, nullptr, offsets, pool);
}

} // namespace facebook::velox::row
