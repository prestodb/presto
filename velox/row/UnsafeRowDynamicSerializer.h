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

#pragma once

#include "velox/common/base/Exceptions.h"
#include "velox/row/UnsafeRowSerializer.h"
#include "velox/type/Date.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::row {

struct UnsafeRowDynamicSerializer : UnsafeRowSerializer {
  class UnsupportedSerializationException : public std::exception {};

  /// Dynamic function for serializing an element at the given index.
  /// \tparam DataType
  /// \param row The UnsafeRow to write to.
  /// \param pos The position in the row to write to.
  /// \param type The Velox type.
  /// \param data The data to serialize.
  /// \param idx used when DataType is a Vector, defaults to 0 otherwise.
  template <typename DataType>
  static void serializeElementAt(
      UnsafeRow& row,
      const TypePtr& type,
      const DataType& data,
      size_t pos,
      size_t idx = 0) {
    // Instead of getting a string_view of serialized data and then writing to
    // the row, we want to find the location to write to and write directly
    // to the buffer. This allows us to write once and avoid an extra copy.
    char* location = row.getSerializationLocation(pos, type->isFixedWidth());
    auto serializedSize = serialize(type, data, location, idx);
    row.writeOffsetAndNullAt(pos, serializedSize, type->isFixedWidth());
  }

  /// Dynamic version of the serialization function.
  /// \tparam DataType
  /// \param type The Velox type.
  /// \param data The data to serialize, primitive data is a FlatVector.
  /// \param buffer to write to
  /// \param idx Used to indicate element index when DataType is derived from
  /// BaseVector. Defaults to 0.
  /// \return size of variable length data written, 0 if no variable length
  /// data is written or only fixed data length data is written, std::nullopt
  /// otherwise
  template <typename DataType>
  static std::optional<size_t> serialize(
      const TypePtr& type,
      const DataType& data,
      char* buffer,
      size_t idx = 0) {
    VELOX_CHECK_NOT_NULL(buffer);

    if (type->isTimestamp()) {
      // Follow Spark, serialize timestamp as micros.
      return serializeTimestampMicros(data, buffer, idx);
    }

    if (type->isFixedWidth()) {
      return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          serializeFixedLength, type->kind(), data, buffer, idx);
    }

    switch (type->kind()) {
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        return serializeStringView(data, buffer, idx);
      case TypeKind::ARRAY:
      case TypeKind::MAP:
      case TypeKind::ROW:
        return serializeComplexType(type, data, buffer, idx);
      default:
        throw UnsupportedSerializationException();
    }
  }

 private:
  /// Dynamic serialization function for a vector of data.
  /// param type
  /// \param buffer pointer to the start of the buffer, we need this to
  /// accurately calculate offsets
  /// \param nullSet pointer to where we write the nullability
  /// \param offset
  /// \param size
  /// \param vector
  /// \return size of variable length data written, 0 if no variable length data
  /// is written or only fixed data length data is written, std::nullopt
  /// otherwise
  inline static std::optional<size_t> serializeVector(
      const TypePtr& type,
      char* buffer,
      char* nullSet,
      int32_t offset,
      vector_size_t size,
      const VectorPtr& vector) {
    if (!type->isFixedWidth()) {
      auto [nullLength, fixedDataStart] = computeFixedDataStart(nullSet, size);
      // Create a temporary unsafe row as a container to recursively serialize.
      UnsafeRow row = UnsafeRow(buffer, nullSet, fixedDataStart, size);
      for (int i = 0; i < size; i++) {
        serializeElementAt(row, type, vector, i, i + offset);
      }
      // Return the serialized data only.
      return UnsafeRow::alignToFieldWidth(row.size() - row.metadataSize());
    }

    auto serializeFlatVectorStub = [&](auto flatVector) {
      // Check that the cast to FlatVector was successful.
      VELOX_CHECK_NOT_NULL(flatVector);
      auto serializedDataSize =
          serializeFlatVector(nullSet, offset, size, flatVector);
      return nullSet - buffer + serializedDataSize.value_or(0);
    };

    switch (type->kind()) {
      case TypeKind::BOOLEAN:
        return serializeFlatVectorStub(
            vector->asFlatVector<TypeTraits<TypeKind::BOOLEAN>::NativeType>());
      case TypeKind::TINYINT:
        return serializeFlatVectorStub(
            vector->asFlatVector<TypeTraits<TypeKind::TINYINT>::NativeType>());
      case TypeKind::SMALLINT:
        return serializeFlatVectorStub(
            vector->asFlatVector<TypeTraits<TypeKind::SMALLINT>::NativeType>());
      case TypeKind::INTEGER:
        return serializeFlatVectorStub(
            vector->asFlatVector<TypeTraits<TypeKind::INTEGER>::NativeType>());
      case TypeKind::BIGINT:
        return serializeFlatVectorStub(
            vector->asFlatVector<TypeTraits<TypeKind::BIGINT>::NativeType>());
      case TypeKind::REAL:
        return serializeFlatVectorStub(
            vector->asFlatVector<TypeTraits<TypeKind::REAL>::NativeType>());
      case TypeKind::DOUBLE:
        return serializeFlatVectorStub(
            vector->asFlatVector<TypeTraits<TypeKind::DOUBLE>::NativeType>());
      case TypeKind::VARCHAR:
        return serializeFlatVectorStub(
            vector->asFlatVector<TypeTraits<TypeKind::VARCHAR>::NativeType>());
      case TypeKind::VARBINARY:
        return serializeFlatVectorStub(
            vector
                ->asFlatVector<TypeTraits<TypeKind::VARBINARY>::NativeType>());
      case TypeKind::TIMESTAMP:
        return serializeFlatVectorStub(
            vector
                ->asFlatVector<TypeTraits<TypeKind::TIMESTAMP>::NativeType>());
      default:
        throw UnsupportedSerializationException();
    }
  }

/// Template definition for unsupported types in the dynamic path.
#define FUNC(TYPE)                                              \
  inline static std::optional<size_t> serializeComplexType(     \
      const TypePtr& type,                                      \
      const TYPE& /*data*/,                                     \
      char* /*buffer*/,                                         \
      size_t /*idx*/) {                                         \
    VELOX_CHECK(false, "Unsupported type " + type->toString()); \
  }
  FUNC(int8_t)
  FUNC(int16_t)
  FUNC(int32_t)
  FUNC(int64_t)
  FUNC(double)
  FUNC(float)
  FUNC(std::string)
  FUNC(StringView)
  FUNC(char*)
  FUNC(Timestamp)
  FUNC(Date)

#undef FUNC

  /// Dynamic complex type serialization function.
  /// \param type
  /// \param data
  /// \param buffer
  /// \param idx
  /// \return size of variable length data written, 0 if no variable length data
  /// is written or only fixed data length data is written, std::nullopt
  /// otherwise
  inline static std::optional<size_t> serializeComplexType(
      const TypePtr& type,
      const VectorPtr& data,
      char* buffer,
      size_t idx) {
    if (type->isArray()) {
      const auto* arrays = data->wrappedVector()->as<ArrayVector>();
      VELOX_CHECK(arrays, "Invalid array in unsaferow conversion from");
      return serializeComplexType(
          type, arrays, buffer, data->wrappedIndex(idx));
    } else if (type->isMap()) {
      const auto* maps = data->wrappedVector()->as<MapVector>();
      VELOX_CHECK(maps, "Invalid map in unsaferow conversion from");
      return serializeComplexType(type, maps, buffer, data->wrappedIndex(idx));
    } else if (type->isRow()) {
      const auto* rows = data->wrappedVector()->as<RowVector>();
      VELOX_CHECK(rows, "Invalid map in unsaferow conversion from");
      return serializeComplexType(type, rows, buffer, data->wrappedIndex(idx));
    }
    throw UnsupportedSerializationException();
  }

  /// Serializing an element in Velox array given its
  /// offset and size (the elements in that location) in an the array
  /// \return the size of written bytes
  inline static std::optional<size_t> serializeArray(
      const TypePtr& elementsType,
      char* buffer,
      int32_t offset,
      vector_size_t size,
      const VectorPtr& elementsVector) {
    // Write the number of elements.
    writeWord(buffer, size);
    char* nullSet = buffer + 1 * UnsafeRow::kFieldWidthBytes;

    auto serializedDataSize = serializeVector(
        elementsType, buffer, nullSet, offset, size, elementsVector);

    // Size is metadata (1 word) + data.
    return UnsafeRow::alignToFieldWidth(
        1 * UnsafeRow::kFieldWidthBytes + serializedDataSize.value_or(0));
  }

  /// Dynamic array serialization function.
  /// \param data
  /// \param buffer
  /// \param idx
  /// \return size of variable length data written, 0 if no variable length data
  /// is written or only fixed data length data is written, std::nullopt
  /// otherwise
  inline static std::optional<size_t> serializeComplexType(
      const TypePtr& type,
      const ArrayVector* data,
      char* buffer,
      size_t idx) {
    VELOX_CHECK(data->isIndexInRange(idx));

    if (data->isNullAt(idx)) {
      return std::nullopt;
    }

    int32_t offset = data->offsetAt(idx);
    vector_size_t size = data->sizeAt(idx);

    auto arrayTypePtr = std::dynamic_pointer_cast<const ArrayType>(type);

    return serializeArray(
        arrayTypePtr->elementType(), buffer, offset, size, data->elements());
  }

  /// Dynamic map serialization function.
  /// \param type
  /// \param data
  /// \param buffer
  /// \param idx
  /// \return size of variable length data written, 0 if no variable length data
  /// is written or only fixed data length data is written, std::nullopt
  /// otherwise
  inline static std::optional<size_t> serializeComplexType(
      const TypePtr& type,
      const MapVector* data,
      char* buffer,
      size_t idx) {
    VELOX_CHECK(data->isIndexInRange(idx));

    if (data->isNullAt(idx)) {
      return std::nullopt;
    }

    auto* mapTypePtr = static_cast<const MapType*>(type.get());
    // Based on Spark definition to a serialized map
    // we serialize keys and values arrays back to back
    // And only add the size (in bytes) of serialized keys
    int32_t offset = data->offsetAt(idx);
    vector_size_t size = data->sizeAt(idx);

    auto keysArrayByteSize = serializeArray(
        mapTypePtr->keyType(),
        buffer + UnsafeRow::kFieldWidthBytes,
        offset,
        size,
        data->mapKeys());

    writeWord(buffer, keysArrayByteSize.value_or(0));

    auto valuesArrayByteSize = serializeArray(
        mapTypePtr->valueType(),
        buffer + UnsafeRow::kFieldWidthBytes + keysArrayByteSize.value_or(0),
        offset,
        size,
        data->mapValues());
    return keysArrayByteSize.value_or(0) + valuesArrayByteSize.value_or(0) +
        UnsafeRow::kFieldWidthBytes;
  }

  /// Dynamic row serialization function. This implementation assumes the
  /// following:
  /// 1. The number and the order of the children vectors match those of the
  ///   the row fields exactly.
  /// 2. All children vectors have the same length.
  /// 3. All children vectors' elements have the same order, i.e. element i in
  ///   child vector is in row i.
  /// UnsafeRow treats a Row type as a variable length data field.
  /// // TODO: ComplexVector.h should contain asserts for these assumptions
  /// \param type
  /// \param data
  /// \param buffer
  /// \param idx the row number to serialize
  /// \return size of variable length data written, 0 if no variable length data
  /// is written or only fixed data length data is written, std::nullopt
  /// otherwise
  // TODO: This function is untested, please add a test case to
  //  UnsafeRowSerializerTest.cpp
  inline static std::optional<size_t> serializeComplexType(
      const TypePtr& type,
      const RowVector* data,
      char* buffer,
      size_t idx) {
    if (data == nullptr) {
      return std::nullopt;
    }

    const size_t numFields = data->childrenSize();

    // Create a temporary unsafe row to represent this current RowVector.
    char* nullSet = buffer;
    auto [nullLength, fixedDataStart] =
        computeFixedDataStart(nullSet, numFields);
    UnsafeRow row = UnsafeRow(buffer, nullSet, fixedDataStart, numFields);

    for (int fieldIdx = 0; fieldIdx < numFields; fieldIdx++) {
      serializeElementAt(
          row,
          type->childAt(fieldIdx),
          data->childAt(fieldIdx),
          fieldIdx,
          /*the row number=*/idx);
    }

    return row.size();
  }
};

} // namespace facebook::velox::row
