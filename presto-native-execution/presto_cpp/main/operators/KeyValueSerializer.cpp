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

#include "presto_cpp/main/operators/KeyValueSerializer.h"

#include "velox/common/memory/ByteStream.h"

namespace facebook::presto::operators {
namespace {

static constexpr int64_t DOUBLE_EXP_BIT_MASK = 0x7FF0000000000000L;
static constexpr int64_t DOUBLE_SIGNIF_BIT_MASK = 0x000FFFFFFFFFFFFFL;

static constexpr int64_t FLOAT_EXP_BIT_MASK = 0x7F800000;
static constexpr int64_t FLOAT_SIGNIF_BIT_MASK = 0x007FFFFF;

// This is to implement the java's Double.doubleToLongBits(double value)
FOLLY_ALWAYS_INLINE int64_t doubleToLong(const double value) {
  const int64_t* result = reinterpret_cast<const int64_t*>(&value);

  if (((*result & DOUBLE_EXP_BIT_MASK) == DOUBLE_EXP_BIT_MASK) &&
      (*result & DOUBLE_SIGNIF_BIT_MASK) != 0L) {
    return 0x7ff8000000000000L;
  }
  return *result;
}

// This is to implement the java's Float.floatToLongBits(float value)
FOLLY_ALWAYS_INLINE int32_t floatToInt(const float value) {
  const int32_t* result = reinterpret_cast<const int32_t*>(&value);

  if (((*result & FLOAT_EXP_BIT_MASK) == FLOAT_EXP_BIT_MASK) &&
      (*result & FLOAT_SIGNIF_BIT_MASK) != 0L) {
    return 0x7fc00000;
  }
  return *result;
}

FOLLY_ALWAYS_INLINE void writeByte(
    velox::ByteOutputStream* out,
    const int8_t value,
    const bool isDescending) {
  if (isDescending) {
    out->appendOne<int32_t>(0xff ^ value);
  } else {
    out->appendOne<int32_t>(value);
  }
}

FOLLY_ALWAYS_INLINE void
writeByte(RawBuffer* out, const int8_t value, const bool isDescending) {
  if (isDescending) {
    out->append(0xff ^ value);
  } else {
    out->append(value);
  }
}

template <typename T>
FOLLY_ALWAYS_INLINE void
writeLong(T* out, const int64_t value, const bool isDescending) {
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
writeInteger(T* out, const int32_t value, const bool isDescending) {
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
    auto value =
        vector.loadedVector()
            ->asUnchecked<velox::SimpleVector<
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
    auto value =
        vector.loadedVector()
            ->asUnchecked<velox::SimpleVector<
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
    auto value =
        vector.loadedVector()
            ->asUnchecked<velox::SimpleVector<
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
    int8_t value =
        vector.loadedVector()
            ->asUnchecked<velox::SimpleVector<
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
    int16_t value =
        vector.loadedVector()
            ->asUnchecked<velox::SimpleVector<
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
    int32_t value =
        vector.loadedVector()
            ->asUnchecked<velox::SimpleVector<
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
    int32_t value = vector.loadedVector()
                        ->asUnchecked<velox::SimpleVector<int32_t>>()
                        ->valueAt(index);
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
    int64_t value =
        vector.loadedVector()
            ->asUnchecked<velox::SimpleVector<
                velox::TypeTraits<velox::TypeKind::TIMESTAMP>::NativeType>>()
            ->valueAt(index)
            .toMicros();

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
    auto value =
        vector.loadedVector()
            ->asUnchecked<velox::SimpleVector<
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
    auto value =
        vector.loadedVector()
            ->asUnchecked<velox::SimpleVector<
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
    auto value =
        vector.loadedVector()
            ->asUnchecked<velox::SimpleVector<
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
    auto row = vector.wrappedVector()->template asUnchecked<velox::RowVector>();
    auto wrappedIndex = vector.wrappedIndex(index);
    const auto& type = row->type()->as<velox::TypeKind::ROW>();
    auto childrenSize = type.size();
    const auto& children = row->children();

    for (int32_t i = 0; i < childrenSize; ++i) {
      if (i >= children.size() || !children[i]) {
        writeBool(out, isNullLast);
      } else {
        writeBool(out, !isNullLast);
        serializeSwitch(
            *children[i], wrappedIndex, out, isNullLast, isDescending);
      }
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
    auto array = vector.wrappedVector()->asUnchecked<velox::ArrayVector>();
    auto wrappedIndex = vector.wrappedIndex(index);
    serializeArrayElements(
        *array->elements(),
        array->offsetAt(wrappedIndex),
        array->sizeAt(wrappedIndex),
        out,
        isNullLast,
        isDescending);
    writeByte(out, 0, isDescending);
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
      VELOX_NYI(fmt::format("Unsupported type: {}", source.typeKind()));
  }
};
} // namespace

void KeyValueSerializer::serialize(
    velox::vector_size_t index,
    velox::ByteOutputStream* out) {
  serializeInternal<velox::ByteOutputStream>(index, out);
}

void KeyValueSerializer::serializeUnsafe(
    velox::vector_size_t index,
    RawBuffer* out) {
  serializeInternal<RawBuffer>(index, out);
}

template <typename T>
void KeyValueSerializer::serializeInternal(
    velox::vector_size_t index,
    T* out) {
  auto* row =
      rowVector_->wrappedVector()->template asUnchecked<velox::RowVector>();
  auto wrappedIndex = rowVector_->wrappedIndex(index);
  const auto& children = row->children();

  for (const auto& pair : fieldChannels_) {
    int32_t idx = pair.first;
    auto channel = pair.second;
    bool isNullLast = !sortOrders_[idx].isNullsFirst();
    bool isDescending = !sortOrders_[idx].isAscending();
    if (channel >= children.size() || !children[channel]) {
      writeBool(out, isNullLast);
    } else {
      writeBool(out, !isNullLast);
      serializeSwitch(
          *children[channel], wrappedIndex, out, isNullLast, isDescending);
    }
  }
}

} // namespace facebook::presto::operators
