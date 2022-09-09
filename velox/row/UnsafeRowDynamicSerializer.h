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
  static std::optional<size_t> serialize(
      const TypePtr& type,
      const VectorPtr& data,
      char* buffer,
      size_t idx = 0) {
    VELOX_CHECK_NOT_NULL(buffer);

    switch (type->kind()) {
#define FIXED_WIDTH(kind)                                             \
  case TypeKind::kind:                                                \
    return UnsafeRowSerializer::serializeFixedLength<TypeKind::kind>( \
        data, buffer, idx);
      FIXED_WIDTH(BOOLEAN);
      FIXED_WIDTH(TINYINT);
      FIXED_WIDTH(SMALLINT);
      FIXED_WIDTH(INTEGER);
      FIXED_WIDTH(BIGINT);
      FIXED_WIDTH(REAL);
      FIXED_WIDTH(DOUBLE);
      FIXED_WIDTH(TIMESTAMP);
      FIXED_WIDTH(DATE);
#undef FIXED_WIDTH
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        return serializeStringView(data, buffer, idx);
      case TypeKind::ARRAY:
        return serializeArray(
            type,
            data->wrappedVector()->template asUnchecked<ArrayVector>(),
            buffer,
            data->wrappedIndex(idx));
      case TypeKind::MAP:
        return serializeMap(
            type,
            data->wrappedVector()->template asUnchecked<MapVector>(),
            buffer,
            data->wrappedIndex(idx));
      case TypeKind::ROW:
        return serializeRow(
            type,
            data->wrappedVector()->template asUnchecked<RowVector>(),
            buffer,
            data->wrappedIndex(idx));
      default:
        VELOX_UNSUPPORTED("Unsupported type: {}", type->toString());
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

    std::optional<size_t> serializedDataSize =
        VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            serializeSimpleVector,
            type->kind(),
            nullSet,
            offset,
            size,
            vector.get());
    return nullSet - buffer + serializedDataSize.value_or(0);
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
    char* nullSet = buffer + UnsafeRow::kFieldWidthBytes;

    auto serializedDataSize = serializeVector(
        elementsType, buffer, nullSet, offset, size, elementsVector);

    // Size is metadata (1 word size) + data.
    return UnsafeRow::alignToFieldWidth(
        UnsafeRow::kFieldWidthBytes + serializedDataSize.value_or(0));
  }

  /// Dynamic array serialization function.
  /// \param data
  /// \param buffer
  /// \param idx
  /// \return size of variable length data written, 0 if no variable length data
  /// is written or only fixed data length data is written, std::nullopt
  /// otherwise
  inline static std::optional<size_t> serializeArray(
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
  inline static std::optional<size_t> serializeMap(
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
  inline static std::optional<size_t> serializeRow(
      const TypePtr& type,
      const RowVector* data,
      char* buffer,
      size_t idx) {
    if (data == nullptr || data->isNullAt(idx)) {
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

  /*
   * Utility functions for return an unsafe row size
   * before serializing it
   */

  /// Gets the size of a group of primitives in a vector
  /// \param Kind The kind of the elements
  /// \param size The number of elements
  /// \param data The elements vector
  /// \return
  template <TypeKind Kind>
  inline static size_t getSizeSimpleVector(
      size_t size,
      const BaseVector* data) {
    using NativeType = typename TypeTraits<Kind>::NativeType;
    size_t dataSize = size * sizeof(NativeType);
    return UnsafeRow::alignToFieldWidth(dataSize + UnsafeRow::kFieldWidthBytes);
  }

  /// Extracts and returns size for StringView fields
  ///
  /// \param data the vector containing the element
  /// \param idx the index of the element in the vector
  /// \return
  inline static size_t getSizeStringView(const VectorPtr& data, size_t idx) {
    const auto& simple = static_cast<const SimpleVector<StringView>&>(*data);
    if (simple.isNullAt(idx)) {
      return 0;
    }
    auto stringSize = simple.valueAt(idx).size();
    return stringSize;
  }

  /// Gets the size of a range of elements in an array
  /// \param elementsType type of elements
  /// \param offset the starting offset of elements
  /// \param size the size of elements
  /// \param elementsVector element vector
  /// \return
  inline static size_t getSizeArray(
      const TypePtr& elementsType,
      int32_t offset,
      vector_size_t size,
      const VectorPtr& elementsVector) {
    // Adding null bitset and also the size of the array first before adding
    // the elements
    // Check specs at
    // https://github.com/apache/spark/blob/master/sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/UnsafeArrayData.java
    size_t result = 1 * UnsafeRow::kFieldWidthBytes;
    size_t nullLength = UnsafeRow::getNullLength(size);

    result +=
        nullLength + getSizeVector(elementsType, offset, size, elementsVector);
    return result;
  }

  /// Gets the size of an element in a vector
  /// \tparam DataType type of element
  /// \param type vector type
  /// \param data vecotr
  /// \param idx element index
  /// \return
  template <typename DataType>
  static size_t
  getSizeElementAt(const TypePtr& type, const DataType& data, size_t idx) {
    auto serializedSize = getSize(type, data, idx);
    // Every variable size value must be aligned to field width
    return UnsafeRow::alignToFieldWidth(serializedSize);
  }

  /// Gets the size of a range of elements in a vector
  /// \param type type of the elements
  /// \param offset start of the range
  /// \param size number of elements in the range
  /// \param vector the vector containing the range
  /// \return
  inline static size_t getSizeVector(
      const TypePtr& type,
      int32_t offset,
      vector_size_t size,
      const VectorPtr& vector) {
    if (!type->isFixedWidth()) {
      // Complex elements take fixed block entry plus their variable size
      size_t fieldsSize = size * UnsafeRow::kFieldWidthBytes;
      size_t elementsSize = 0;
      for (int i = 0; i < size; i++) {
        elementsSize += getSizeElementAt(type, vector, i + offset);
      }
      return fieldsSize + UnsafeRow::alignToFieldWidth(elementsSize);
    } else {
      size_t serializedDataSize = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          getSizeSimpleVector, type->kind(), size, vector.get());
      return serializedDataSize;
    }
  }

  /// Gets the size of an element in a vector
  /// \param type the of the element
  /// \param data the vector
  /// \param idx the index of the element in the vector
  /// \return
  inline static size_t
  getSizeArray(const TypePtr& type, const ArrayVector* data, size_t idx) {
    VELOX_CHECK(data->isIndexInRange(idx));

    if (data->isNullAt(idx)) {
      return 0;
    }

    int32_t offset = data->offsetAt(idx);
    vector_size_t size = data->sizeAt(idx);

    auto arrayTypePtr = std::dynamic_pointer_cast<const ArrayType>(type);

    return getSizeArray(
        arrayTypePtr->elementType(), offset, size, data->elements());
  }

  /// Gets a size of an elememnt in map column
  /// \param type type of the element
  /// \param data map vector
  /// \param idx the element index in the map vector
  /// \return
  inline static size_t
  getSizeMap(const TypePtr& type, const MapVector* data, size_t idx) {
    // Spec for Unsafe maps is defined here:
    // https://github.com/apache/spark/blob/master/sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/UnsafeMapData.java
    // It consists of two arrays, one for keys and one for values also at
    // the very beginning the map start the size of the first array in 8 bytes
    VELOX_CHECK(data->isIndexInRange(idx));
    if (data->isNullAt(idx)) {
      return 0;
    }
    auto* mapTypePtr = static_cast<const MapType*>(type.get());
    // Based on Spark definition to a serialized map
    // we serialize keys and values arrays back to back
    // And only add the size (in bytes) of serialized keys
    int32_t offset = data->offsetAt(idx);
    vector_size_t size = data->sizeAt(idx);

    auto keysArrayByteSize =
        getSizeArray(mapTypePtr->keyType(), offset, size, data->mapKeys());

    auto valuesArrayByteSize =
        getSizeArray(mapTypePtr->valueType(), offset, size, data->mapValues());
    return keysArrayByteSize + valuesArrayByteSize +
        UnsafeRow::kFieldWidthBytes;
  }

 public:
  /// Returns size of a given element in a vector identified by i///
  /// \param type type of the element
  /// \param data the vector
  /// \param idx index of the element
  /// \return
  static size_t
  getSize(const TypePtr& type, const VectorPtr& data, size_t idx = 0) {
    switch (type->kind()) {
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        return getSizeStringView(data, idx);
      case TypeKind::ARRAY:
        return getSizeArray(
            type,
            data->wrappedVector()->template asUnchecked<ArrayVector>(),
            data->wrappedIndex(idx));
      case TypeKind::MAP:
        return getSizeMap(
            type,
            data->wrappedVector()->template asUnchecked<MapVector>(),
            data->wrappedIndex(idx));
      case TypeKind::ROW:
        return getSizeRow(
            type,
            data->wrappedVector()->template asUnchecked<RowVector>(),
            data->wrappedIndex(idx));
      default:
        return UnsafeRow::kFieldWidthBytes;
    }
  }

  /// This function prepares the vector hierarchy for serialization or size
  /// calculation
  /// \param data the vector data
  inline static void preloadVector(const VectorPtr data) {
    switch (data->typeKind()) {
      case TypeKind::ROW: {
        auto rowVector =
            data->wrappedVector()->template asUnchecked<RowVector>();
        for (int fieldIdx = 0; fieldIdx < rowVector->childrenSize();
             fieldIdx++) {
          preloadVector(rowVector->childAt(fieldIdx));
        }
      } break;
      case TypeKind::ARRAY: {
        auto arrayVector =
            data->wrappedVector()->template asUnchecked<ArrayVector>();
        preloadVector(arrayVector->elements());
      } break;
      case TypeKind::MAP: {
        auto mapVector =
            data->wrappedVector()->template asUnchecked<MapVector>();
        preloadVector(mapVector->mapKeys());
        preloadVector(mapVector->mapValues());
      } break;
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        data->loadedVector();
        break;
      default: {
        // No action needed
      }
    }
  }

  /// Gets a size of a row in a row vector
  /// \param type type of the row
  /// \param data row vector
  /// \param idx in dex of the row in the row vector
  /// \return
  inline static size_t
  getSizeRow(const TypePtr& type, const RowVector* data, size_t idx) {
    if (data == nullptr || data->isNullAt(idx)) {
      return 0;
    }
    const size_t numFields = data->childrenSize();
    size_t nullLength = UnsafeRow::getNullLength(numFields);

    size_t elementsSize = 0;
    for (int fieldIdx = 0; fieldIdx < numFields; fieldIdx++) {
      auto elementType = type->childAt(fieldIdx);
      elementsSize += getSizeElementAt(
          elementType,
          data->childAt(fieldIdx),
          /*the row number=*/idx);
      if (!elementType->isFixedWidth()) {
        elementsSize += UnsafeRow::kFieldWidthBytes;
      }
    }
    return nullLength + elementsSize;
  }
};

} // namespace facebook::velox::row
