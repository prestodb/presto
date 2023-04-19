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
#include "velox/row/UnsafeRow.h"
#include "velox/row/UnsafeRowParser.h"
#include "velox/type/Date.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::row {

struct UnsafeRowSerializer {
  /// Templated function for serializing an element at the given index.
  /// \tparam SqlType The Velox sql type.
  /// \tparam DataType
  /// \param row The UnsafeRow to write to.
  /// \param data The data to serialize.
  /// \param pos The position in the row to write to.
  /// \param idx used when DataType is a Vector, defaults to 0 otherwise.
  template <typename SqlType, typename DataType>
  static void serializeElementAt(
      UnsafeRow& row,
      const DataType& data,
      size_t pos,
      size_t idx = 0) {
    // Instead of getting a string_view of serialized data and then writing to
    // the row, we want to find the location to write to and write directly
    // to the buffer. This allows us to write once and avoid an extra copy.
    char* location = row.getSerializationLocation(
        pos, UnsafeRowStaticUtilities::isFixedWidth<SqlType>());
    std::optional<size_t> serializedSize =
        serialize<SqlType>(data, location, idx);
    row.writeOffsetAndNullAt(
        pos, serializedSize, UnsafeRowStaticUtilities::isFixedWidth<SqlType>());
  }

  /// Templated function for serializing an element at the given index where the
  /// element type is a velox Vector.
  /// @tparam SqlType
  /// @tparam DataType
  /// @param row
  /// @param data
  /// @param pos
  /// @param idx the index of the vector element to serialize
  template <typename SqlType, typename DataType>
  static void serializeVectorElementAt(
      UnsafeRow& row,
      const DataType& data,
      size_t pos,
      size_t idx) {
    char* location = row.getSerializationLocation(
        pos, UnsafeRowStaticUtilities::isFixedWidth<SqlType>());
    std::optional<size_t> serializedSize =
        serializeComplexVectors<SqlType>(data, location, idx);
    row.writeOffsetAndNullAt(
        pos, serializedSize, UnsafeRowStaticUtilities::isFixedWidth<SqlType>());
  }

  /// Templated version of the serialization function, used when the sql type is
  /// known at compile time, e.g. during codegen.
  /// \tparam SqlType The Velox sql type.
  /// \tparam DataType
  /// \param data The data to serialize, primitive data is a FlatVector.
  /// \param buffer to write to
  /// \param idx Used to indicate element index when DataType is derived from
  /// BaseVector. Defaults to 0.
  /// \return size of variable length data written, 0 if no variable length data
  /// is written or only fixed data length data is written, std::nullopt
  /// otherwise
  template <typename SqlType, typename DataType>
  static std::optional<size_t>
  serialize(const DataType& data, char* buffer, size_t idx = 0) {
    VELOX_CHECK_NOT_NULL(buffer);
    if constexpr (UnsafeRowStaticUtilities::isFixedWidth<SqlType>()) {
      return UnsafeRowSerializer::serializeFixedLength<
          UnsafeRowStaticUtilities::simpleSqlTypeToTypeKind<SqlType>()>(
          data, buffer, idx);
    } else if constexpr (
        std::is_same_v<SqlType, VarcharType> ||
        std::is_same_v<SqlType, VarbinaryType>) {
      return serializeStringView(data, buffer, idx);
    } else {
      return ComplexTypeSerializer<SqlType>::serialize(data, buffer);
    }
  }

  template <typename SqlType, typename DataType>
  static std::optional<size_t>
  serialize(const std::optional<DataType>& data, char* buffer, size_t idx = 0) {
    VELOX_CHECK_NOT_NULL(buffer);
    if (!data.has_value()) {
      return std::nullopt;
    }
    const DataType& val = data.value();
    return serialize<SqlType, DataType>(val, buffer, idx);
  }

  /// Templated version of the serialization function where complex types are
  /// represented as velox vectors.
  /// \tparam SqlType The Velox sql type.
  /// \tparam DataType
  /// \param data The data to serialize, primitive data is a FlatVector.
  /// \param buffer to write to
  /// \param idx Used to indicate element index when DataType is derived from
  /// BaseVector. Defaults to 0.
  /// \return size of variable length data written, 0 if no variable length data
  /// is written or only fixed data length data is written, std::nullopt
  /// otherwise
  template <typename SqlType, typename DataType>
  static std::optional<size_t>
  serializeComplexVectors(const DataType& data, char* buffer, size_t idx = 0) {
    if constexpr (
        UnsafeRowStaticUtilities::isFixedWidth<SqlType>() ||
        std::is_same_v<SqlType, VarcharType> ||
        std::is_same_v<SqlType, VarbinaryType>) {
      return serialize<SqlType, DataType>(data, buffer, idx);
    } else {
      /// Although we support Row<T...> in the static serializer, we do not
      /// support RowVector type in the ComplexTypeVectorSerializer. Codegen
      /// should generate code that uses the Row<T...> static serializer
      /// interface for performance reasons.
      return ComplexTypeVectorSerializer<SqlType>::serialize(data, buffer, idx);
    }
  }

 protected:
  /// Get the location where fixed length data starts.
  /// \param nullSet
  /// \param size
  /// \return the nullSet length and pointer to the start of fixed length data.
  inline static std::tuple<size_t, char*> computeFixedDataStart(
      char* nullSet,
      size_t size) {
    size_t nullLength = UnsafeRow::getNullLength(size);
    char* fixedDataStart = nullSet + nullLength;

    return std::tuple(nullLength, fixedDataStart);
  }

  /// Serializes a fundamental data field to a string_view in UnsafeRow fixed
  /// length data format.
  /// \tparam Kind
  /// \tparam DataType
  /// \param data
  /// \param buffer Pre allocated buffer for the return value
  /// \param idx This argument is ignored because DataType is not derived from
  /// a BaseVector
  /// \return size of variable length data written, 0 if no variable length data
  /// is written or only fixed data length data is written, std::nullopt
  /// otherwise
  template <TypeKind Kind>
  inline static std::optional<size_t> serializeFixedLength(
      const typename TypeTraits<Kind>::NativeType& data,
      char* buffer,
      size_t idx = 0) {
    if constexpr (Kind == TypeKind::TIMESTAMP) {
      *reinterpret_cast<int64_t*>(buffer) = data.toMicros();
    } else {
      *reinterpret_cast<typename TypeTraits<Kind>::NativeType*>(buffer) = data;
    }
    return 0;
  }

  /// Serializes a VectorPtr that points to some fixed length data into the
  /// UnsafeRow fixed length data format
  /// \tparam Kind
  /// \param data
  /// \param buffer Pre allocated buffer for the return value
  /// \param idx Element index in the VectorPtr
  /// \return size of variable length data written, 0 if no variable length data
  /// is written or only fixed data length data is written, std::nullopt
  /// otherwise
  template <TypeKind Kind>
  inline static std::optional<size_t>
  serializeFixedLength(const VectorPtr& vector, char* buffer, size_t idx) {
    static_assert(TypeTraits<Kind>::isFixedWidth);
    // TODO: refactor to merge the decoding of a vector of fixed length,
    // Timestamp and StringView data.
    using NativeType = typename TypeTraits<Kind>::NativeType;
    DCHECK_EQ(vector->type()->kind(), Kind);
    const auto& simple =
        *vector->loadedVector()->asUnchecked<SimpleVector<NativeType>>();
    VELOX_CHECK(vector->isIndexInRange(idx));

    if (simple.isNullAt(idx)) {
      return std::nullopt;
    }
    serializeFixedLength<Kind>(simple.valueAt(idx), buffer);
    return 0;
  }

  /// Serializes a StringView to UnsafeRow string format plus the padding
  /// required.
  /// \param data
  /// \param buffer Pre allocated buffer for the return value
  /// \param idx This argument is ignored because data is not a Vector.
  /// \return size of variable length data written, 0 if no variable length data
  /// is written or only fixed data length data is written, std::nullopt
  /// otherwise
  inline static std::optional<size_t>
  serializeStringView(const StringView& data, char* buffer, size_t idx = 0) {
    const char* const begin = data.data();
    std::copy(begin, begin + data.size(), buffer);
    return data.size();
  }

  /// Serializes a StringView to UnsafeRow string format plus the padding
  /// required.
  /// \param data
  /// \param buffer Pre allocated buffer for the return value
  /// \param idx
  /// \return size of variable length data written, 0 if no variable length data
  /// is written or only fixed data length data is written, std::nullopt
  /// otherwise
  inline static std::optional<size_t>
  serializeStringView(const VectorPtr& data, char* buffer, size_t idx) {
    auto* rawData = data->loadedVector();

    VELOX_CHECK(data->isIndexInRange(idx));
    const auto& simple = static_cast<const SimpleVector<StringView>&>(*rawData);

    if (simple.isNullAt(idx)) {
      return std::nullopt;
    }
    return serializeStringView(simple.valueAt(idx), buffer, idx);
  }

  /// Template definition for unsupported types.
  template <typename T>
  inline static std::optional<size_t>
  serializeStringView(const T& data, char* buffer, size_t idx) {
    throw("Unsupported data type for serializeStringView()");
  }

  /// Write the data as a uint64_t at the given location.
  /// \param buffer
  /// \param data
  /// \return size of variable length data written, 0 if no variable length data
  /// is written or only fixed data length data is written, std::nullopt
  /// otherwise
  inline static std::optional<size_t> writeWord(char* buffer, uint64_t data) {
    reinterpret_cast<uint64_t*>(buffer)[0] = data;
    return 0;
  }

  /// Serializes a vector of data.
  /// \tparam T
  /// \param nullSet pointer to where we should write the nullability
  /// \param offset
  /// \param size
  /// \param data non-null vector
  /// \return size of variable length data written, 0 if no variable length data
  /// is written or only fixed data length data is written, std::nullopt
  /// otherwise
  template <TypeKind Kind>
  inline static std::optional<size_t> serializeSimpleVector(
      char* nullSet,
      int32_t offset,
      size_t size,
      const BaseVector* data) {
    using T = typename TypeTraits<Kind>::NativeType;
    const auto& simple = *data->loadedVector()->asUnchecked<SimpleVector<T>>();
    auto [nullLength, fixedDataStart] = computeFixedDataStart(nullSet, size);
    auto stride = serializedSizeInBytes(data->type());
    for (int i = 0; i < size; i++) {
      bool isNull = simple.isNullAt(i + offset);
      if (isNull) {
        bits::setBit(nullSet, i);
      } else {
        bits::clearBit(nullSet, i);
        serializeFixedLength<Kind>(
            simple.valueAt(i + offset), fixedDataStart + i * stride);
      }
    }
    return UnsafeRow::alignToFieldWidth(size * stride + nullLength);
  }

  /// Struct declaration for partial template specialization.
  /// \tparam T
  template <typename T>
  struct ComplexTypeSerializer {};

  /// Serializes a fundamental array.
  /// \tparam SqlType
  /// \tparam DataType must have the following interface:
  /// size_t size(): returns number of elements
  /// T .at(): returns the element at the given index
  /// The element type must be a C++ fundamental type.
  /// \param data
  /// \param numElements
  /// \param row
  /// \return the size of the UnsafeRow fundamental array
  template <typename SqlType, typename DataType>
  inline static size_t serializeFundamentalArray(
      const DataType& data,
      size_t& numElements,
      UnsafeRow& row) {
    constexpr TypeKind kind =
        UnsafeRowStaticUtilities::simpleSqlTypeToTypeKind<SqlType>();
    using NativeType = typename TypeTraits<kind>::NativeType;
    for (int i = 0; i < numElements; i++) {
      auto& element = data.at(i);
      if (!element.has_value()) {
        row.setNullAt(i);
      } else {
        row.setNotNullAt(i);
        reinterpret_cast<NativeType*>(row.fixedLengthDataLocation())[i] =
            element.value();
      }
    }
    return row.metadataSize() +
        UnsafeRow::alignToFieldWidth(sizeof(NativeType) * numElements);
  }

  /// Serializes an array as an UnsafeRow. All UnsafeRow complex types contain
  /// smaller UnsafeRow array objects that looks like an UnsafeRow.
  /// \tparam T
  /// \tparam DataType
  /// \param values
  /// \param buffer Start of the buffer, we need this to compute the relative
  /// offset
  /// \param elementStart Start of the array
  /// \return the size of the serialized array.
  template <typename T, typename DataType>
  inline static size_t
  serializeArrayAsRow(DataType& values, char* buffer, char* elementStart) {
    size_t numElements = values.size();
    auto [nullLength, fixedDataStart] =
        computeFixedDataStart(elementStart, values.size());
    UnsafeRow row =
        UnsafeRow(buffer, elementStart, fixedDataStart, values.size());

    if constexpr (UnsafeRowStaticUtilities::isFixedWidth<T>()) {
      return row.metadataSize() +
          serializeFundamentalArray<T>(values, numElements, row);
    } else {
      for (int i = 0; i < values.size(); i++) {
        serializeElementAt<T>(row, values.at(i), i);
      }
      return row.size();
    }
  }

  /// Template specialization for Array<T>.
  /// \tparam T
  template <typename T>
  struct ComplexTypeSerializer<Array<T>> {
    template <typename DataType>
    /// Serializes an UnsafeRow Array.
    /// \tparam DataType must have this interface:
    /// size_t size(): number of elements
    /// T .at(): returns the element at the given index
    /// \param data
    /// \param buffer
    /// \return size of variable length data written, 0 if no variable length
    /// is written or only fixed data length data is written, std::nullopt
    /// otherwise
    inline static std::optional<size_t> serialize(
        const DataType& data,
        char* buffer) {
      // Write the number of elements.
      size_t numElements = data.size();
      size_t metaDataSize = 1 * UnsafeRow::kFieldWidthBytes;
      writeWord(buffer, numElements);

      char* elementStart = buffer + metaDataSize;
      return serializeArrayAsRow<T>(data, buffer, elementStart);
    }
  };

  /// Template specialization for Map<K, V>.
  /// \tparam K
  /// \tparam V
  template <typename K, typename V>
  struct ComplexTypeSerializer<Map<K, V>> {
    /// Takes a multimap and serializes to an UnsafeRow Map.
    /// \tparam KeysType
    /// \tparam ValuesType
    /// \param data
    /// \param buffer
    /// \return size of variable length data written, 0 if no variable length
    /// data is written or only fixed data length data is written, std::nullopt
    /// otherwise
    template <typename KeysType, typename ValuesType>
    inline static std::optional<size_t> serialize(
        const std::multimap<KeysType, ValuesType>& data,
        char* buffer) {
      VELOX_NYI("Static serializer for map needs to be first fixed");
      // Allocate space for writing offset to values.
      char* valueOffsetLocation = buffer;
      size_t mapSize = 1 * UnsafeRow::kFieldWidthBytes;

      // Write the number of elements.
      size_t numElements = data.size();

      std::vector<KeysType> keysVec;
      std::vector<ValuesType> valsVec;
      keysVec.reserve(numElements), valsVec.reserve(numElements);

      for (auto& [key, value] : data) {
        keysVec.emplace_back(std::reference_wrapper(key));
        valsVec.emplace_back(std::reference_wrapper(value));
      }

      char* keysStart = buffer + mapSize;
      // UnsafeRow treats the keys and values as UnsafeRow arrays.
      size_t keySize =
          ComplexTypeSerializer<Array<K>>::serialize(keysVec, keysStart)
              .value_or(0);

      // Write offset to values.
      writeWord(valueOffsetLocation, keySize);
      mapSize += keySize;

      char* valsStart = buffer + mapSize;
      size_t valsSize =
          ComplexTypeSerializer<Array<V>>::serialize(valsVec, valsStart)
              .value_or(0);

      return mapSize + valsSize;
    }
  };

  /// Template specialization for Row<T...>.
  template <typename T, typename... RestT>
  struct ComplexTypeSerializer<Row<T, RestT...>> {
    /// Takes a any number of elements and serialize as a row.
    /// \tparam V... data types of the elements
    /// \param buffer
    /// \param idx needed in case the elements are Vector types
    /// \param elements...
    /// \return size of variable length data written, 0 if no variable length
    /// data is written or only fixed data length data is written
    // TODO: Row type serializer is not tested
    template <typename... V>
    inline static size_t
    serialize(char* buffer, size_t idx, const V&... elements) {
      static constexpr size_t numFields = sizeof...(RestT) + 1;

      // Create a temporary unsafe row to represent this current RowVector.
      char* nullSet = buffer;
      auto [nullLength, fixedDataStart] =
          computeFixedDataStart(nullSet, numFields);
      UnsafeRow row = UnsafeRow(buffer, nullSet, fixedDataStart, numFields);

      return serializeElements<0, V...>(row, idx, elements...);
    }

    /// Takes a any number of elements and serialize as a row.
    /// \tparam pos the row position to serialize to
    /// \tparam V data types of the current element
    /// \tparam RestV... data types of the rest of the elements
    /// \param row
    /// \param idx needed in case the elements are Vector types
    /// \param element the current element
    /// \param restElements rest of the elements
    /// \return size of variable length data written, 0 if no variable length
    /// data is written or only fixed data length data is written
    template <size_t pos, typename V, typename... RestV>
    inline static size_t serializeElements(
        UnsafeRow& row,
        size_t idx,
        const V& element,
        const RestV&... restElements) {
      static_assert(sizeof...(RestT) == sizeof...(RestV));

      if (sizeof...(RestV) == 0) {
        return serializeElementAt<T, V>(row, element, pos, idx);
      }
      return serializeElementAt<T, V>(row, element, pos, idx) +
          ComplexTypeSerializer<Row<RestT...>>::
              template serializeElement<pos + 1, RestV...>(
                  row, idx, restElements...);
    }
  };

  /// Templated serialization function for a vector of data.
  /// \tparam T vector element type
  /// \param buffer pointer to the start of the buffer, we need this to
  /// accuately calculate offsets
  /// \param nullSet pointer to where we write the nullability
  /// \param offset
  /// \param size
  /// \param vector
  /// \return size of variable length data written, 0 if no variable length data
  /// is written or only fixed data length data is written, std::nullopt
  /// otherwise
  template <typename T>
  inline static std::optional<size_t> serializeVector(
      char* buffer,
      char* nullSet,
      int32_t offset,
      vector_size_t size,
      const VectorPtr& vector) {
    if constexpr (UnsafeRowStaticUtilities::isFixedWidth<T>()) {
      constexpr TypeKind kind =
          UnsafeRowStaticUtilities::simpleSqlTypeToTypeKind<T>();
      using NativeType = typename TypeTraits<kind>::NativeType;

      auto* flatVector = vector->asFlatVector<NativeType>();
      if (!flatVector) {
        throw("Cannot cast to FlatVector\n");
      }
      auto serializedDataSize =
          serializeSimpleVector<kind>(nullSet, offset, size, vector.get());
      return UnsafeRow::alignToFieldWidth(serializedDataSize.value_or(0));
    } else {
      auto [nullLength, fixedDataStart] = computeFixedDataStart(nullSet, size);
      // Create a temporary unsafe row as a container to recursively serialize.
      UnsafeRow row = UnsafeRow(buffer, nullSet, fixedDataStart, size);
      for (int i = 0; i < size; i++) {
        serializeVectorElementAt<T>(row, vector, i, i + offset);
      }
      // Return the serialized data only.
      return UnsafeRow::alignToFieldWidth(row.size() - row.metadataSize());
    }
  }

  template <typename T>
  struct ComplexTypeVectorSerializer {
    /// Template definition for serializeMapVectorElements where the type is not
    /// a complex type.
    /// \tparam T
    inline static std::optional<size_t> serializeMapVectorElements(
        char* mapElementStart,
        int32_t offset,
        vector_size_t size,
        const VectorPtr& vector) {
      return serializeVector<T>(
          mapElementStart, mapElementStart, offset, size, vector);
    }
  };

  /// Template specialization for Arrays
  /// \tparam T
  template <typename T>
  struct ComplexTypeVectorSerializer<Array<T>> {
    /// Serializes elements in a map vector.
    /// \param mapElementStart Pointer to the start of this element.
    /// \param offset
    /// \param size
    /// \param vector
    /// \return size of variable length data written, 0 if no variable length
    /// data is written or only fixed data length data is written, std::nullopt
    /// otherwise
    inline static std::optional<size_t> serializeMapVectorElements(
        char* mapElementStart,
        int32_t offset,
        vector_size_t size,
        const VectorPtr& vector) {
      // Write the array metadata (i.e. number of elements).
      writeWord(mapElementStart, size);
      char* nullSet = mapElementStart + 1 * UnsafeRow::kFieldWidthBytes;

      auto serializedSize = serializeVector<Array<T>>(
          mapElementStart, nullSet, offset, size, vector);
      return UnsafeRow::alignToFieldWidth(1 + serializedSize.value_or(0));
    }

    /// Templated array serialization function. We do not need to specialize
    /// data type to ArrayVectorPtr since this templated code is generated at
    /// compile time. The serialization follows this
    /// format:
    ///  [number of elements : 1 word]
    ///  [nullset : (num elements + 63) / 64 words]
    ///  [fixed length data or offsets: (num elements) words]
    ///  [variable length data: variable]
    /// \param data
    /// \param buffer
    /// \param offset
    /// \param size
    /// \return size of variable length data written, 0 if no variable length
    /// data is written or only fixed data length data is written, std::nullopt
    /// otherwise
    inline static std::optional<size_t>
    serialize(const VectorPtr& data, char* buffer, size_t idx) {
      VELOX_CHECK(data->isIndexInRange(idx));
      if (data->isNullAt(idx)) {
        return std::nullopt;
      }

      auto arrayVectorPtr = std::dynamic_pointer_cast<ArrayVector>(data);
      if (!arrayVectorPtr) {
        throw("Failed to cast to ArrayVector, type mismatch.");
      }

      int32_t offset = arrayVectorPtr->offsetAt(idx);
      vector_size_t size = arrayVectorPtr->sizeAt(idx);

      // Write the number of elements.
      writeWord(buffer, size);
      char* nullSet = buffer + 1 * UnsafeRow::kFieldWidthBytes;

      auto serializedDataSize = serializeVector<T>(
          buffer, nullSet, offset, size, arrayVectorPtr->elements());

      // Size is metadata (1 word) + data.
      return UnsafeRow::alignToFieldWidth(
          1 * UnsafeRow::kFieldWidthBytes + serializedDataSize.value_or(0));
    }
  };

  /// Template specialization for maps.
  /// \tparam K
  /// \tparam V
  template <typename K, typename V>
  struct ComplexTypeVectorSerializer<Map<K, V>> {
    /// Serializes elements in a map vector.
    /// \param mapElementStart Pointer to the start of this element.
    /// \param offset
    /// \param size
    /// \param vector
    /// \return size of variable length data written, 0 if no variable length
    /// data is written or only fixed data length data is written, std::nullopt
    /// otherwise
    inline static std::optional<size_t> serializeMapVectorElements(
        char* mapElementStart,
        int32_t offset,
        vector_size_t size,
        const VectorPtr& vector) {
      // Write offset to values.
      char* offsetToValuesLocation = mapElementStart;

      // Write number of keys.
      char* numKeysLocation =
          offsetToValuesLocation + UnsafeRow::kFieldWidthBytes;
      writeWord(numKeysLocation, size);

      char* keysLocation = numKeysLocation + UnsafeRow::kFieldWidthBytes;
      auto keysSize = serializeVector<K, V>(
          mapElementStart, keysLocation, offset, size, vector);

      writeWord(offsetToValuesLocation, keysSize.value_or(0));

      char* valuesLocation = keysLocation + keysSize;
      auto valuesSize = serializeVector<V>(
          mapElementStart, valuesLocation, offset, size, vector);

      return 2 * UnsafeRow::kFieldWidthBytes + keysSize.value_or(0) +
          valuesSize.value_or(0);
    }

    /// Templated map serialization function. The serialization follows this
    /// format:
    ///  [offset to values: 1 word]
    ///  [number of keys : 1 word]
    ///  [keys nullset : (num elements + 63) / 64 words]
    ///  [keys fixed length data or offsets: (num elements) words]
    ///  [keys variable length data: variable]
    ///  [number of values : 1 word]
    ///  [values null set : (num elements + 63) / 64 words]
    ///  [values fixed length data or offsets: (num elements) words]
    ///  [values variable length data: variable]
    /// \param data
    /// \param buffer
    /// \param idx
    /// \return size of variable length data written, 0 if no variable length
    /// data is written or only fixed data length data is written, std::nullopt
    /// otherwise
    inline static std::optional<size_t>
    serialize(const VectorPtr& data, char* buffer, size_t idx) {
      VELOX_CHECK(data->isIndexInRange(idx));
      if (data->isNullAt(idx)) {
        return std::nullopt;
      }

      auto mapVectorPtr = std::dynamic_pointer_cast<MapVector>(data);
      if (!mapVectorPtr) {
        throw("Failed to cast to MapVector, type mismatch.");
      }

      int32_t offset = mapVectorPtr->offsetAt(idx);
      vector_size_t size = mapVectorPtr->sizeAt(idx);

      char* offsetToValuesLocation = buffer;
      char* numKeysLocation = buffer + UnsafeRow::kFieldWidthBytes;

      // Write the keys.
      char* keysLocation = buffer + 2 * UnsafeRow::kFieldWidthBytes;
      auto keysSize =
          ComplexTypeVectorSerializer<K>::serializeMapVectorElements(
              keysLocation, offset, size, mapVectorPtr->mapKeys());

      // Write the keys metadata.
      writeWord(numKeysLocation, size);
      writeWord(
          offsetToValuesLocation,
          1 * UnsafeRow::kFieldWidthBytes + keysSize.value_or(0));

      char* valuesLocation = keysLocation + keysSize.value_or(0);
      auto valuesSize =
          ComplexTypeVectorSerializer<V>::serializeMapVectorElements(
              valuesLocation, offset, size, mapVectorPtr->mapValues());

      // Total size = 2 words of metadata + keysSize + valuesSize.
      size_t totalSize = 2 * UnsafeRow::kFieldWidthBytes +
          keysSize.value_or(0) + valuesSize.value_or(0);
      return UnsafeRow::alignToFieldWidth(totalSize);
    }
  };

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
    auto serializedSize = serialize(data, location, idx);
    row.writeOffsetAndNullAt(pos, serializedSize, type->isFixedWidth());
  }

 public:
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
  static std::optional<size_t>
  serialize(const VectorPtr& data, char* buffer, size_t idx) {
    VELOX_CHECK_NOT_NULL(buffer);

    switch (data->typeKind()) {
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
            data->type(),
            data->wrappedVector()->template asUnchecked<ArrayVector>(),
            buffer,
            data->wrappedIndex(idx));
      case TypeKind::MAP:
        return serializeMap(
            data->type(),
            data->wrappedVector()->template asUnchecked<MapVector>(),
            buffer,
            data->wrappedIndex(idx));
      case TypeKind::ROW:
        return serializeRow(
            data->type(),
            data->wrappedVector()->template asUnchecked<RowVector>(),
            buffer,
            data->wrappedIndex(idx));
      default:
        VELOX_UNSUPPORTED("Unsupported type: {}", data->type()->toString());
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
    return serializedDataSize.value_or(0);
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
    size_t dataSize = size * serializedSizeInBytes(data->type());
    return UnsafeRow::alignToFieldWidth(dataSize);
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
      int32_t offset,
      vector_size_t size,
      const VectorPtr& elementsVector) {
    // Adding null bitset and also the size of the array first before adding
    // the elements
    // Check specs at
    // https://github.com/apache/spark/blob/master/sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/UnsafeArrayData.java
    size_t result = 1 * UnsafeRow::kFieldWidthBytes;
    size_t nullLength = UnsafeRow::getNullLength(size);

    result += nullLength + getSizeVector(offset, size, elementsVector);
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
  inline static size_t
  getSizeVector(int32_t offset, vector_size_t size, const VectorPtr& vector) {
    const auto& type = vector->type();
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
  inline static size_t getSizeArray(const ArrayVector* data, size_t idx) {
    VELOX_CHECK(data->isIndexInRange(idx));

    if (data->isNullAt(idx)) {
      return 0;
    }

    int32_t offset = data->offsetAt(idx);
    vector_size_t size = data->sizeAt(idx);

    return getSizeArray(offset, size, data->elements());
  }

  /// Gets a size of an elememnt in map column
  /// \param type type of the element
  /// \param data map vector
  /// \param idx the element index in the map vector
  /// \return
  inline static size_t getSizeMap(const MapVector* data, size_t idx) {
    // Spec for Unsafe maps is defined here:
    // https://github.com/apache/spark/blob/master/sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/UnsafeMapData.java
    // It consists of two arrays, one for keys and one for values also at
    // the very beginning the map start the size of the first array in 8 bytes
    VELOX_CHECK(data->isIndexInRange(idx));
    if (data->isNullAt(idx)) {
      return 0;
    }

    // Based on Spark definition to a serialized map
    // we serialize keys and values arrays back to back
    // And only add the size (in bytes) of serialized keys
    int32_t offset = data->offsetAt(idx);
    vector_size_t size = data->sizeAt(idx);

    auto keysArrayByteSize = getSizeArray(offset, size, data->mapKeys());
    auto valuesArrayByteSize = getSizeArray(offset, size, data->mapValues());
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
            data->wrappedVector()->template asUnchecked<ArrayVector>(),
            data->wrappedIndex(idx));
      case TypeKind::MAP:
        return getSizeMap(
            data->wrappedVector()->template asUnchecked<MapVector>(),
            data->wrappedIndex(idx));
      case TypeKind::ROW:
        return getSizeRow(
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
  inline static size_t getSizeRow(const RowVector* data, size_t idx) {
    if (data->isNullAt(idx)) {
      return 0;
    }
    const size_t numFields = data->childrenSize();
    size_t nullLength = UnsafeRow::getNullLength(numFields);

    size_t elementsSize = 0;
    for (int fieldIdx = 0; fieldIdx < numFields; fieldIdx++) {
      auto elementType = data->type()->childAt(fieldIdx);
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
