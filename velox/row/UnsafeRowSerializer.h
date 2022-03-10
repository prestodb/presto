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

#include "velox/row/UnsafeRow.h"
#include "velox/row/UnsafeRowParser.h"
#include "velox/vector/ComplexVector.h"

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
        std::is_same<SqlType, VarcharType>::value ||
        std::is_same<SqlType, VarbinaryType>::value) {
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
        std::is_same<SqlType, VarcharType>::value ||
        std::is_same<SqlType, VarbinaryType>::value) {
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
    using NativeType = typename TypeTraits<Kind>::NativeType;
    const auto& simple =
        *data->loadedVector()->asUnchecked<SimpleVector<NativeType>>();
    auto [nullLength, fixedDataStart] = computeFixedDataStart(nullSet, size);
    size_t dataSize = size * sizeof(NativeType);
    auto stride = Kind == TypeKind::TIMESTAMP ? 8 : sizeof(NativeType);
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
    return UnsafeRow::alignToFieldWidth(dataSize);
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
      return UnsafeRow::alignToFieldWidth(
          nullSet - buffer + serializedDataSize.value_or(0));
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
};

} // namespace facebook::velox::row
