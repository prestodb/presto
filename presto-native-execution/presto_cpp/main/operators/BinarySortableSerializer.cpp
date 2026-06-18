/*
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
#include "presto_cpp/main/operators/BinarySortableSerializer.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/exec/Operator.h"

namespace facebook::presto::operators {
namespace {

static constexpr int64_t DOUBLE_EXP_BIT_MASK = 0x7FF0000000000000L;
static constexpr int64_t DOUBLE_SIGNIF_BIT_MASK = 0x000FFFFFFFFFFFFFL;

static constexpr int64_t FLOAT_EXP_BIT_MASK = 0x7F800000;
static constexpr int64_t FLOAT_SIGNIF_BIT_MASK = 0x007FFFFF;

static constexpr size_t kNullByteSize = 1;

// This is to implement the java's Double.doubleToLongBits(double value)
FOLLY_ALWAYS_INLINE int64_t doubleToLong(double value) {
  const int64_t* result = reinterpret_cast<const int64_t*>(&value);

  if (((*result & DOUBLE_EXP_BIT_MASK) == DOUBLE_EXP_BIT_MASK) &&
      (*result & DOUBLE_SIGNIF_BIT_MASK) != 0L) {
    return 0x7ff8000000000000L;
  }
  return *result;
}

// This is to implement the java's Float.floatToLongBits(float value)
FOLLY_ALWAYS_INLINE int32_t floatToInt(float value) {
  const int32_t* result = reinterpret_cast<const int32_t*>(&value);

  if (((*result & FLOAT_EXP_BIT_MASK) == FLOAT_EXP_BIT_MASK) &&
      (*result & FLOAT_SIGNIF_BIT_MASK) != 0L) {
    return 0x7fc00000;
  }
  return *result;
}

template <typename T>
FOLLY_ALWAYS_INLINE void writeByte(T* out, int8_t value, bool isDescending) {
  if (isDescending) {
    out->appendByte(0xff ^ value);
  } else {
    out->appendByte(value);
  }
}

template <typename T>
FOLLY_ALWAYS_INLINE void writeLong(T* out, int64_t value, bool isDescending) {
  writeByte(out, static_cast<int8_t>((value >> 56) ^ 0x80), isDescending);
  writeByte(out, static_cast<int8_t>(value >> 48), isDescending);
  writeByte(out, static_cast<int8_t>(value >> 40), isDescending);
  writeByte(out, static_cast<int8_t>(value >> 32), isDescending);
  writeByte(out, static_cast<int8_t>(value >> 24), isDescending);
  writeByte(out, static_cast<int8_t>(value >> 16), isDescending);
  writeByte(out, static_cast<int8_t>(value >> 8), isDescending);
  writeByte(out, static_cast<int8_t>(value), isDescending);
}

template <typename T>
FOLLY_ALWAYS_INLINE void
writeInteger(T* out, int32_t value, bool isDescending) {
  writeByte(out, static_cast<int8_t>((value >> 24) ^ 0x80), isDescending);
  writeByte(out, static_cast<int8_t>(value >> 16), isDescending);
  writeByte(out, static_cast<int8_t>(value >> 8), isDescending);
  writeByte(out, static_cast<int8_t>(value), isDescending);
}

template <typename T>
FOLLY_ALWAYS_INLINE void writeBytes(
    T* out,
    const char* data,
    size_t offset,
    size_t size,
    bool isDescending) {
  for (auto i = 0; i < size; ++i) {
    if (data[offset + i] == 0 || data[offset + i] == 1) {
      writeByte(out, 1, isDescending);
      writeByte(out, data[offset + i], isDescending);
    } else {
      writeByte(out, data[offset + i], isDescending);
    }
  }
  writeByte(out, 0, isDescending);
}

FOLLY_ALWAYS_INLINE size_t
getBytesSerializedSize(const char* data, size_t offset, size_t size) {
  size_t count = 0;
  for (auto i = 0; i < size; ++i) {
    if (data[offset + i] == 0 || data[offset + i] == 1) {
      count += 2;
    } else {
      ++count;
    }
  }
  // One additional byte for end marker.
  return count + 1;
}

template <typename T>
FOLLY_ALWAYS_INLINE void writeBool(T* out, bool value) {
  writeByte(out, value, /*isDescending=*/false);
}

template <typename T>
void serializeSwitch(
    const velox::BaseVector& source,
    velox::vector_size_t index,
    T* out,
    bool isNullLast,
    bool isDescending);

size_t serializedSizeSwitch(
    const velox::BaseVector& source,
    velox::vector_size_t index);

void serializedSizeSwitchBatch(
    const velox::BaseVector& source,
    const folly::Range<const velox::vector_size_t*>& rows,
    velox::vector_size_t** sizes,
    velox::Scratch& scratch);

template <typename T>
void serializeBigInt(
    const velox::BaseVector& vector,
    velox::vector_size_t index,
    T* out,
    bool isNullLast,
    bool isDescending) {
  if (vector.isNullAt(index)) {
    writeBool(out, isNullLast);
  } else {
    writeBool(out, !isNullLast);
    const auto value =
        vector
            .asUnchecked<velox::SimpleVector<
                velox::TypeTraits<velox::TypeKind::BIGINT>::NativeType>>()
            ->valueAt(index);

    writeLong(out, value, isDescending);
  }
}

template <typename T>
void serializeDouble(
    const velox::BaseVector& vector,
    velox::vector_size_t index,
    T* out,
    bool isNullLast,
    bool isDescending) {
  if (vector.isNullAt(index)) {
    writeBool(out, isNullLast);
  } else {
    writeBool(out, !isNullLast);
    const auto value =
        vector
            .asUnchecked<velox::SimpleVector<
                velox::TypeTraits<velox::TypeKind::DOUBLE>::NativeType>>()
            ->valueAt(index);
    int64_t longValue = doubleToLong(value);

    if ((longValue & (1L << 63)) != 0) {
      // negative number, flip all bits
      longValue = ~longValue;
    } else {
      // positive number, flip the first bit
      longValue = longValue ^ (1L << 63);
    }
    writeByte(out, static_cast<int8_t>(longValue >> 56), isDescending);
    writeByte(out, static_cast<int8_t>(longValue >> 48), isDescending);
    writeByte(out, static_cast<int8_t>(longValue >> 40), isDescending);
    writeByte(out, static_cast<int8_t>(longValue >> 32), isDescending);
    writeByte(out, static_cast<int8_t>(longValue >> 24), isDescending);
    writeByte(out, static_cast<int8_t>(longValue >> 16), isDescending);
    writeByte(out, static_cast<int8_t>(longValue >> 8), isDescending);
    writeByte(out, static_cast<int8_t>(longValue), isDescending);
  }
}

template <typename T>
void serializeReal(
    const velox::BaseVector& vector,
    velox::vector_size_t index,
    T* out,
    bool isNullLast,
    bool isDescending) {
  if (vector.isNullAt(index)) {
    writeBool(out, isNullLast);
  } else {
    writeBool(out, !isNullLast);
    const auto value =
        vector
            .asUnchecked<velox::SimpleVector<
                velox::TypeTraits<velox::TypeKind::REAL>::NativeType>>()
            ->valueAt(index);
    int32_t intValue = floatToInt(value);

    if ((intValue & (1L << 31)) != 0) {
      // negative number, flip all bits
      intValue = ~intValue;
    } else {
      // positive number, flip the first bit
      intValue = intValue ^ (1L << 31);
    }
    writeByte(out, static_cast<int8_t>(intValue >> 24), isDescending);
    writeByte(out, static_cast<int8_t>(intValue >> 16), isDescending);
    writeByte(out, static_cast<int8_t>(intValue >> 8), isDescending);
    writeByte(out, static_cast<int8_t>(intValue), isDescending);
  }
}

template <typename T>
void serializeTinyInt(
    const velox::BaseVector& vector,
    velox::vector_size_t index,
    T* out,
    bool isNullLast,
    bool isDescending) {
  if (vector.isNullAt(index)) {
    writeBool(out, isNullLast);
  } else {
    writeBool(out, !isNullLast);
    const int8_t value =
        vector
            .asUnchecked<velox::SimpleVector<
                velox::TypeTraits<velox::TypeKind::TINYINT>::NativeType>>()
            ->valueAt(index);
    writeByte(out, static_cast<int8_t>(value ^ 0x80), isDescending);
  }
}

template <typename T>
void serializeSmallInt(
    const velox::BaseVector& vector,
    velox::vector_size_t index,
    T* out,
    bool isNullLast,
    bool isDescending) {
  if (vector.isNullAt(index)) {
    writeBool(out, isNullLast);
  } else {
    writeBool(out, !isNullLast);
    const int16_t value =
        vector
            .asUnchecked<velox::SimpleVector<
                velox::TypeTraits<velox::TypeKind::SMALLINT>::NativeType>>()
            ->valueAt(index);
    writeByte(out, static_cast<int8_t>((value >> 8) ^ 0x80), isDescending);
    writeByte(out, static_cast<int8_t>(value), isDescending);
  }
}

template <typename T>
void serializeInteger(
    const velox::BaseVector& vector,
    velox::vector_size_t index,
    T* out,
    bool isNullLast,
    bool isDescending) {
  if (vector.isNullAt(index)) {
    writeBool(out, isNullLast);
  } else {
    writeBool(out, !isNullLast);
    const int32_t value =
        vector
            .asUnchecked<velox::SimpleVector<
                velox::TypeTraits<velox::TypeKind::INTEGER>::NativeType>>()
            ->valueAt(index);
    writeInteger(out, value, isDescending);
  }
}

template <typename T>
void serializeDate(
    const velox::BaseVector& vector,
    velox::vector_size_t index,
    T* out,
    bool isNullLast,
    bool isDescending) {
  if (vector.isNullAt(index)) {
    writeBool(out, isNullLast);
  } else {
    writeBool(out, !isNullLast);
    const int32_t value =
        vector.asUnchecked<velox::SimpleVector<int32_t>>()->valueAt(index);
    writeInteger(out, value, isDescending);
  }
}

template <typename T>
void serializeTimestamp(
    const velox::BaseVector& vector,
    velox::vector_size_t index,
    T* out,
    bool isNullLast,
    bool isDescending) {
  if (vector.isNullAt(index)) {
    writeBool(out, isNullLast);
  } else {
    writeBool(out, !isNullLast);
    const int64_t value =
        vector
            .asUnchecked<velox::SimpleVector<
                velox::TypeTraits<velox::TypeKind::TIMESTAMP>::NativeType>>()
            ->valueAt(index)
            .toNanos();

    writeLong(out, value, isDescending);
  }
}

template <typename T>
void serializeBoolean(
    const velox::BaseVector& vector,
    velox::vector_size_t index,
    T* out,
    bool isNullLast,
    bool isDescending) {
  if (vector.isNullAt(index)) {
    writeBool(out, isNullLast);
  } else {
    writeBool(out, !isNullLast);
    const auto value =
        vector
            .asUnchecked<velox::SimpleVector<
                velox::TypeTraits<velox::TypeKind::BOOLEAN>::NativeType>>()
            ->valueAt(index);
    writeByte(out, static_cast<int8_t>(value ? 2 : 1), isDescending);
  }
}

template <typename T>
void serializeVarchar(
    const velox::BaseVector& vector,
    velox::vector_size_t index,
    T* out,
    bool isNullLast,
    bool isDescending) {
  if (vector.isNullAt(index)) {
    writeBool(out, isNullLast);
  } else {
    writeBool(out, !isNullLast);
    const auto value =
        vector
            .asUnchecked<velox::SimpleVector<
                velox::TypeTraits<velox::TypeKind::VARCHAR>::NativeType>>()
            ->valueAt(index);
    writeBytes(out, value.data(), /*offset=*/0, value.size(), isDescending);
  }
}

template <typename T>
void serializeVarbinary(
    const velox::BaseVector& vector,
    velox::vector_size_t index,
    T* out,
    bool isNullLast,
    bool isDescending) {
  if (vector.isNullAt(index)) {
    writeBool(out, isNullLast);
  } else {
    writeBool(out, !isNullLast);
    const auto value =
        vector
            .asUnchecked<velox::SimpleVector<
                velox::TypeTraits<velox::TypeKind::VARBINARY>::NativeType>>()
            ->valueAt(index);

    writeBytes(out, value.data(), /*offset=*/0, value.size(), isDescending);
  }
}

template <typename T>
void serializeRow(
    const velox::BaseVector& vector,
    velox::vector_size_t index,
    T* out,
    bool isNullLast,
    bool isDescending) {
  if (vector.isNullAt(index)) {
    writeBool(out, isNullLast);
  } else {
    const velox::DecodedVector decoded(vector);
    const auto* rowBase = decoded.base()->as<velox::RowVector>();
    const auto decodedIndex = decoded.index(index);
    const auto& type = rowBase->type()->as<velox::TypeKind::ROW>();
    const auto childrenSize = type.size();
    const auto& children = rowBase->children();

    for (int32_t i = 0; i < childrenSize; ++i) {
      if (i >= children.size() || !children[i]) {
        writeBool(out, isNullLast);
      } else {
        writeBool(out, !isNullLast);
        serializeSwitch(
            *children[i], decodedIndex, out, isNullLast, isDescending);
      }
    }
  }
}

size_t getSerializedRowSize(
    const velox::BaseVector& vector,
    velox::vector_size_t index) {
  const velox::DecodedVector decoded(vector);
  const auto* rowBase = decoded.base()->as<velox::RowVector>();
  const auto decodedIndex = decoded.index(index);
  const auto& type = rowBase->type()->as<velox::TypeKind::ROW>();
  const auto childrenSize = type.size();
  const auto& children = rowBase->children();

  size_t serializedSize = childrenSize;
  for (int32_t i = 0; i < childrenSize; ++i) {
    if (i < children.size() && children[i]) {
      serializedSize += serializedSizeSwitch(*children[i], decodedIndex);
    }
  }
  return serializedSize;
}

void calculateSerializedSizeRowBatch(
    const velox::BaseVector& source,
    const folly::Range<const velox::vector_size_t*>& rows,
    velox::vector_size_t** sizes,
    velox::Scratch& scratch) {
  const auto numRows = rows.size();

  const velox::DecodedVector decoded(source);
  const auto* rowBase = decoded.base()->as<velox::RowVector>();
  const auto& rowType = rowBase->type()->as<velox::TypeKind::ROW>();
  const auto& children = rowBase->children();
  const auto childrenSize = rowType.size();

  // Collect decoded indices for all rows
  velox::ScratchPtr<velox::vector_size_t, 1> decodedIndicesHolder(scratch);
  auto decodedIndices = decodedIndicesHolder.get(numRows);

  for (auto i = 0; i < numRows; ++i) {
    *sizes[i] += childrenSize;
    decodedIndices[i] = decoded.index(rows[i]);
  }

  // Process each child column in batch
  for (int32_t childIdx = 0; childIdx < childrenSize; ++childIdx) {
    if (childIdx < children.size() && children[childIdx]) {
      // Process this child column for all rows at once
      serializedSizeSwitchBatch(
          *children[childIdx],
          folly::Range<const velox::vector_size_t*>(decodedIndices, numRows),
          sizes,
          scratch);
    }
  }
}

template <typename T>
void serializeArrayElements(
    const velox::BaseVector& elements,
    velox::vector_size_t offset,
    velox::vector_size_t size,
    T* out,
    bool isNullLast,
    bool isDescending) {
  for (auto i = 0; i < size; ++i) {
    writeByte(out, 1, isDescending);
    serializeSwitch(elements, i + offset, out, isNullLast, isDescending);
  }
}

template <typename T>
void serializeArray(
    const velox::BaseVector& vector,
    velox::vector_size_t index,
    T* out,
    bool isNullLast,
    bool isDescending) {
  if (vector.isNullAt(index)) {
    writeBool(out, isNullLast);
  } else {
    writeBool(out, !isNullLast);
    const velox::DecodedVector decoded(vector);
    const auto* arrayBase = decoded.base()->as<velox::ArrayVector>();
    const auto decodedIndex = decoded.index(index);

    serializeArrayElements(
        *arrayBase->elements(),
        arrayBase->offsetAt(decodedIndex),
        arrayBase->sizeAt(decodedIndex),
        out,
        isNullLast,
        isDescending);
    writeByte(out, 0, isDescending);
  }
}

size_t getSerializedArraySize(
    const velox::BaseVector& vector,
    velox::vector_size_t index) {
  const velox::DecodedVector decoded(vector);
  const auto* arrayBase = decoded.base()->as<velox::ArrayVector>();
  const auto decodedIndex = decoded.index(index);
  size_t serializedSize = 0;
  for (auto i = 0; i < arrayBase->sizeAt(decodedIndex); ++i) {
    serializedSize +=
        1 + // null byte (element)
        serializedSizeSwitch(
            *arrayBase->elements(), i + arrayBase->offsetAt(decodedIndex));
  }
  // One additional byte for end marker.
  return serializedSize + 1;
}

void calculateSerializedSizeArrayBatch(
    const velox::BaseVector& source,
    const folly::Range<const velox::vector_size_t*>& rows,
    velox::vector_size_t** sizes,
    velox::Scratch& scratch) {
  const auto numRows = rows.size();

  const velox::DecodedVector decoded(source);
  const auto* arrayBase = decoded.base()->as<velox::ArrayVector>();

  // Collect element ranges and corresponding size pointers
  velox::ScratchPtr<velox::vector_size_t, 1> elementRowsHolder(scratch);
  velox::ScratchPtr<velox::vector_size_t*, 1> elementSizesHolder(scratch);

  // Count total elements
  velox::vector_size_t totalElements = 0;
  for (auto i = 0; i < numRows; ++i) {
    const auto decodedIndex = decoded.index(rows[i]);
    totalElements += arrayBase->sizeAt(decodedIndex);
  }

  auto elementRows = elementRowsHolder.get(totalElements);
  auto elementSizes = elementSizesHolder.get(totalElements);

  size_t elementIndex = 0;

  for (auto i = 0; i < numRows; ++i) {
    const auto decodedIndex = decoded.index(rows[i]);
    const auto offset = arrayBase->offsetAt(decodedIndex);
    const auto size = arrayBase->sizeAt(decodedIndex);

    // Add element indices and size pointers for batch processing
    for (auto j = 0; j < size; ++j) {
      elementRows[elementIndex] = offset + j;
      elementSizes[elementIndex] = sizes[i];
      elementIndex++;
      // Add element null byte
      *sizes[i] += 1;
    }
    // Add one end marker byte per row
    *sizes[i] += 1;
  }

  // Process all array elements in batch if there are any
  if (totalElements > 0) {
    serializedSizeSwitchBatch(
        *arrayBase->elements(),
        folly::Range<const velox::vector_size_t*>(elementRows, totalElements),
        elementSizes,
        scratch);
  }
}

template <typename T>
void serializeSwitch(
    const velox::BaseVector& source,
    velox::vector_size_t index,
    T* out,
    bool isNullLast,
    bool isDescending) {
  if (source.type()->isDate()) {
    return serializeDate(source, index, out, isNullLast, isDescending);
  }

  switch (source.typeKind()) {
    case velox::TypeKind::BIGINT:
      return serializeBigInt(source, index, out, isNullLast, isDescending);
    case velox::TypeKind::BOOLEAN:
      return serializeBoolean(source, index, out, isNullLast, isDescending);
    case velox::TypeKind::DOUBLE:
      return serializeDouble(source, index, out, isNullLast, isDescending);
    case velox::TypeKind::REAL:
      return serializeReal(source, index, out, isNullLast, isDescending);
    case velox::TypeKind::TINYINT:
      return serializeTinyInt(source, index, out, isNullLast, isDescending);
    case velox::TypeKind::SMALLINT:
      return serializeSmallInt(source, index, out, isNullLast, isDescending);
    case velox::TypeKind::INTEGER:
      return serializeInteger(source, index, out, isNullLast, isDescending);
    case velox::TypeKind::VARCHAR:
      return serializeVarchar(source, index, out, isNullLast, isDescending);
    case velox::TypeKind::VARBINARY:
      return serializeVarbinary(source, index, out, isNullLast, isDescending);
    case velox::TypeKind::TIMESTAMP:
      return serializeTimestamp(source, index, out, isNullLast, isDescending);
    case velox::TypeKind::ROW:
      return serializeRow(source, index, out, isNullLast, isDescending);
    case velox::TypeKind::ARRAY:
      return serializeArray(source, index, out, isNullLast, isDescending);
    default:
      VELOX_NYI("Unsupported type: {}", source.typeKind());
  }
}

size_t serializedSizeSwitch(
    const velox::BaseVector& source,
    velox::vector_size_t index) {
  if (source.isNullAt(index)) {
    return kNullByteSize;
  }
  if (source.type()->isDate()) {
    return kNullByteSize + sizeof(int32_t);
  }

  switch (source.typeKind()) {
    case velox::TypeKind::BIGINT:
      [[fallthrough]];
    case velox::TypeKind::BOOLEAN:
      [[fallthrough]];
    case velox::TypeKind::DOUBLE:
      [[fallthrough]];
    case velox::TypeKind::REAL:
      [[fallthrough]];
    case velox::TypeKind::TINYINT:
      [[fallthrough]];
    case velox::TypeKind::SMALLINT:
      [[fallthrough]];
    case velox::TypeKind::INTEGER:
      return kNullByteSize + source.type()->cppSizeInBytes();
    case velox::TypeKind::TIMESTAMP:
      return kNullByteSize + sizeof(int64_t);
    case velox::TypeKind::VARCHAR: {
      const auto varchar =
          source
              .asUnchecked<velox::SimpleVector<
                  velox::TypeTraits<velox::TypeKind::VARCHAR>::NativeType>>()
              ->valueAt(index);
      return kNullByteSize +
          getBytesSerializedSize(varchar.data(), /*offset=*/0, varchar.size());
    }
    case velox::TypeKind::VARBINARY: {
      const auto varbinary =
          source
              .asUnchecked<velox::SimpleVector<
                  velox::TypeTraits<velox::TypeKind::VARBINARY>::NativeType>>()
              ->valueAt(index);
      return kNullByteSize +
          getBytesSerializedSize(
                 varbinary.data(), /*offset=*/0, varbinary.size());
    }
    case velox::TypeKind::ROW:
      return getSerializedRowSize(source, index);
    case velox::TypeKind::ARRAY:
      return kNullByteSize + getSerializedArraySize(source, index);
    default:
      VELOX_NYI("Unsupported type: {}", source.typeKind());
  }
}

void serializedSizeSwitchBatch(
    const velox::BaseVector& source,
    const folly::Range<const velox::vector_size_t*>& rows,
    velox::vector_size_t** sizes,
    velox::Scratch& scratch) {
  // null handling using SIMD with scratch memory
  velox::ScratchPtr<uint64_t, 1> nullsHolder(scratch);
  velox::ScratchPtr<velox::vector_size_t, 1> nonNullsHolder(scratch);
  velox::ScratchPtr<velox::vector_size_t*, 1> nonNullSizesHolder(scratch);

  folly::Range<const velox::vector_size_t*> nonNullRows = rows;
  auto* nonNullSizes = sizes;
  const auto numRows = rows.size();

  if (source.mayHaveNulls()) {
    auto nulls = nullsHolder.get(velox::bits::nwords(numRows));
    for (auto i = 0; i < numRows; ++i) {
      // Set null bits (0 for null, 1 for non-null)
      velox::bits::setBit(nulls, i);
      if (source.isNullAt(rows[i])) {
        velox::bits::clearBit(nulls, i);
        *sizes[i] += kNullByteSize;
      }
    }

    auto nonNulls = nonNullsHolder.get(numRows);
    const auto numNonNull =
        velox::simd::indicesOfSetBits(nulls, 0, numRows, nonNulls);
    if (numNonNull == 0) {
      return;
    }
    nonNullSizes = nonNullSizesHolder.get(numNonNull);
    for (int32_t i = 0; i < numNonNull; ++i) {
      nonNullSizes[i] = sizes[nonNulls[i]];
    }

    velox::simd::transpose(
        rows.data(),
        folly::Range<const velox::vector_size_t*>(nonNulls, numNonNull),
        nonNulls);
    nonNullRows =
        folly::Range<const velox::vector_size_t*>(nonNulls, numNonNull);
  }

  // Process data for non-null rows only (or all rows if no nulls)
  if (source.type()->isDate()) {
    // Process date columns
    for (auto i = 0; i < nonNullRows.size(); ++i) {
      *nonNullSizes[i] += (kNullByteSize + sizeof(int32_t));
    }
    return;
  }

  switch (source.typeKind()) {
    case velox::TypeKind::BIGINT:
    case velox::TypeKind::BOOLEAN:
    case velox::TypeKind::DOUBLE:
    case velox::TypeKind::REAL:
    case velox::TypeKind::TINYINT:
    case velox::TypeKind::SMALLINT:
    case velox::TypeKind::INTEGER: {
      // Fixed-width types
      const auto elementSize = source.type()->cppSizeInBytes();
      for (auto i = 0; i < nonNullRows.size(); ++i) {
        *nonNullSizes[i] += (kNullByteSize + elementSize);
      }
      break;
    }
    case velox::TypeKind::TIMESTAMP: {
      // Timestamp
      for (auto i = 0; i < nonNullRows.size(); ++i) {
        *nonNullSizes[i] += (kNullByteSize + sizeof(int64_t));
      }
      break;
    }
    case velox::TypeKind::VARCHAR:
    case velox::TypeKind::VARBINARY: {
      auto valueBytes =
          source.asUnchecked<velox::SimpleVector<velox::StringView>>();
      for (auto i = 0; i < nonNullRows.size(); ++i) {
        const auto value = valueBytes->valueAt(nonNullRows[i]);
        *nonNullSizes[i] += kNullByteSize +
            getBytesSerializedSize(value.data(), /*offset=*/0, value.size());
      }
      break;
    }
    case velox::TypeKind::ROW: {
      calculateSerializedSizeRowBatch(
          source, nonNullRows, nonNullSizes, scratch);
      break;
    }
    case velox::TypeKind::ARRAY: {
      for (auto i = 0; i < nonNullRows.size(); ++i) {
        *nonNullSizes[i] += kNullByteSize;
      }
      calculateSerializedSizeArrayBatch(
          source, nonNullRows, nonNullSizes, scratch);
      break;
    }
    default:
      VELOX_NYI("Unsupported type: {}", source.typeKind());
  }
}

std::vector<std::pair<int32_t, velox::column_index_t>> computeSortChannels(
    const std::vector<velox::core::FieldAccessTypedExprPtr>& sortFields,
    const velox::RowTypePtr& inputRowType) {
  std::vector<std::pair<int32_t, velox::column_index_t>> sortChannels;
  sortChannels.reserve(sortFields.size());
  for (int32_t i = 0; i < sortFields.size(); ++i) {
    sortChannels.emplace_back(
        i, velox::exec::exprToChannel(sortFields[i].get(), inputRowType));
  }
  return sortChannels;
}
} // namespace

BinarySortableSerializer::BinarySortableSerializer(
    const velox::RowVectorPtr& source,
    const std::vector<velox::core::SortOrder>& sortOrders,
    const std::vector<velox::core::FieldAccessTypedExprPtr>& sortFields)
    : input_(source),
      sortOrders_(sortOrders),
      sortChannels_(
          computeSortChannels(sortFields, velox::asRowType(source->type()))) {
  VELOX_CHECK_EQ(sortFields.size(), sortOrders_.size());
}

void BinarySortableSerializer::serialize(
    velox::vector_size_t rowId,
    velox::StringVectorBuffer* out) const {
  const velox::DecodedVector decoded(*input_, /*loadLazy=*/true);
  const auto* rowBase = decoded.base()->as<velox::RowVector>();
  const auto decodedIndex = decoded.index(rowId);
  const auto& children = rowBase->children();

  for (const auto& pair : sortChannels_) {
    const int32_t idx = pair.first;
    const auto channel = pair.second;
    const bool isNullLast = !sortOrders_[idx].isNullsFirst();
    const bool isDescending = !sortOrders_[idx].isAscending();
    if (channel >= children.size()) {
      VELOX_CHECK_EQ(
          channel,
          velox::kConstantChannel,
          "Channel must be field access or constant");
      writeBool(out, isNullLast);
    } else {
      VELOX_CHECK_NOT_NULL(children[channel]);
      writeBool(out, !isNullLast);
      serializeSwitch(
          *children[channel], decodedIndex, out, isNullLast, isDescending);
    }
  }
}

size_t BinarySortableSerializer::serializedSizeInBytes(
    velox::vector_size_t rowId) const {
  const velox::DecodedVector decoded(*input_, /*loadLazy=*/true);
  const auto* rowBase = decoded.base()->as<velox::RowVector>();
  const auto decodedIndex = decoded.index(rowId);
  const auto& children = rowBase->children();

  size_t serializedSize = sortChannels_.size();
  for (const auto& sortChannelEntry : sortChannels_) {
    const auto channel = sortChannelEntry.second;
    if (channel >= children.size()) {
      VELOX_CHECK_EQ(
          channel,
          velox::kConstantChannel,
          "Channel must be field access or constant");
    } else {
      VELOX_CHECK_NOT_NULL(children[channel]);
      serializedSize += serializedSizeSwitch(*children[channel], decodedIndex);
    }
  }
  return serializedSize;
}

void BinarySortableSerializer::serializedSizeInBytes(
    velox::vector_size_t offset,
    velox::vector_size_t size,
    velox::vector_size_t** sizes,
    velox::Scratch& scratch) const {
  if (size == 0) {
    return;
  }
  VELOX_CHECK_LE(offset + size, input_->size(), "Invalid offset or size");

  // Decode the input vector
  const velox::DecodedVector decoded(*input_, /*loadLazy=*/true);
  const auto* rowBase = decoded.base()->as<velox::RowVector>();
  const auto& children = rowBase->children();

  // Initialize base sizes with one byte per sort channel
  for (auto i = 0; i < size; ++i) {
    *sizes[i] += sortChannels_.size();
  }

  // Process each sort channel in columnar order
  for (const auto& sortChannelEntry : sortChannels_) {
    const auto channel = sortChannelEntry.second;
    if (channel >= children.size()) {
      VELOX_CHECK_EQ(
          channel,
          velox::kConstantChannel,
          "Channel must be field access or constant");
    } else {
      VELOX_CHECK_NOT_NULL(children[channel]);
      velox::ScratchPtr<velox::vector_size_t, 1> decodedRowsHolder(scratch);
      auto decodedRows = decodedRowsHolder.get(size);
      for (auto i = 0; i < size; ++i) {
        decodedRows[i] = decoded.index(offset + i);
      }

      serializedSizeSwitchBatch(
          *children[channel],
          folly::Range<const velox::vector_size_t*>(decodedRows, size),
          sizes,
          scratch);
    }
  }
}

} // namespace facebook::presto::operators
