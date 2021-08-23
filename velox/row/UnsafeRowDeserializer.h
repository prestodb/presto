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

#if ENABLE_CONCEPTS
#include <concepts> // @manual
#endif

#include <queue>
#include "velox/common/base/Exceptions.h"
#include "velox/row/UnsafeRow.h"
#include "velox/row/UnsafeRowParser.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::row {

/**
 * A virtual class that represents iterators that loop through UnsafeRow data.
 */
class UnsafeRowDataIterator {
 public:
  /**
   * UnsafeRowDataIterator constructor.
   * @param isNull whether the element is null
   * @param type the element type
   */
  UnsafeRowDataIterator(bool isNull, TypePtr type)
      : isNull_(isNull), type_(type) {}

  virtual ~UnsafeRowDataIterator() = 0;

  /**
   * @return true if element is null.
   */
  bool isNull() {
    return isNull_;
  }

  /**
   * @return the element type.
   */
  TypePtr type() {
    return type_;
  }

  /**
   * @return the element size.
   */
  virtual size_t size() = 0;

 private:
  /**
   * Whether the element is null.
   */
  bool isNull_ = false;

  /**
   * The element type.
   */
  TypePtr type_;
};

inline UnsafeRowDataIterator::~UnsafeRowDataIterator() {}

using DataIteratorPtr = std::shared_ptr<UnsafeRowDataIterator>;

DataIteratorPtr getIteratorPtr(
    std::optional<std::string_view> data,
    TypePtr type);

/**
 * Iterator that represents a primitive object. Primitive includes strings
 * and anything that can be represented as a c++ fundamental type with at most
 * 8 bytes.
 */
struct UnsafeRowPrimitiveIterator : UnsafeRowDataIterator {
 public:
  /**
   * UnsafeRowPrimitiveIterator constructor.
   * @param data The part of the UnsafeRow data buffer that contains the
   * primitive value
   * @param type The element type
   */
  UnsafeRowPrimitiveIterator(std::optional<std::string_view> data, TypePtr type)
      : UnsafeRowDataIterator(!data.has_value(), type), data_(data) {}

  /**
   * @return a string_view over the part of the UnsafeRow data buffer that
   * contains the primitive value.
   */
  std::optional<std::string_view> data() {
    return data_;
  }

  /**
   * @return A primitive value occupies 1 field, so return 0 if the element is
   * null, 1 otherwise.
   */
  size_t size() override {
    return isNull() ? 0 : 1;
  }

 private:
  /**
   * A string_view over the part of the UnsafeRow data buffer that contains the
   * primitive value.
   */
  std::optional<std::string_view> data_;
};

/**
 * Iterator to traverse through an UnsafeRow array representation.  This is used
 * for:
 * UnsafeRow array objects, UnsafeRow Map keys, and UnsafeRow Map values.
 */
struct UnsafeRowArrayIterator : UnsafeRowDataIterator {
 public:
  /**
   * Constructor for UnsafeRowArray.
   * @param data The unsafe row array representation data
   * @param isFixedLength whether the elements in the array is fixed length
   * @param fixedDataWidth the data width if the element is fixed length
   */
  UnsafeRowArrayIterator(
      std::optional<std::string_view> data,
      bool isFixedLength,
      size_t fixedDataWidth = 0,
      TypePtr type = nullptr)
      : UnsafeRowDataIterator(!data.has_value(), type),
        isFixedLength_(isFixedLength),
        fixedDataWidth_(fixedDataWidth) {
    if (data.has_value()) {
      /*
       * [number of elements : 1 word]
       * [nullset : (num elements + 63) / 64 words]
       * [fixed length data or offsets: (num elements) words]
       * [variable length data: variable]
       */
      numElements_ = reinterpret_cast<const uint64_t*>(data->data())[0];

      if (numElements_ == 0) {
        return;
      }

      const char* nullSet = data->data() + UnsafeRow::kFieldWidthBytes;
      auto nullLengthBytes = UnsafeRow::getNullLength(numElements_);
      nulls_ = std::string_view(nullSet, nullLengthBytes);

      // numElements : 1 word, nullSet : nullLengthBytes
      elementsBaseOffset_ = UnsafeRow::kFieldWidthBytes + nullLengthBytes;
      arrayElementStart_ = data->data() + elementsBaseOffset_;
    }
  }

  /**
   * @return the size of the array
   */
  size_t size() override {
    return isNull() ? 0 : numElements_;
  }

  /**
   * @return return whether there's a next element.
   */
  bool hasNext() {
    return idx_ < numElements_;
  }

  bool childIsFixedLength() {
    return isFixedLength_;
  }

  /**
   * Exception to indicate the element is out of bounds.
   */
  class IndexOutOfBounds : public std::exception {};

  /**
   * @return throws IndexOutOfBounds if there is no next element,
   * std::nullopt if the next element is null, or the next element.
   */
  std::optional<std::string_view> next() {
    if (idx_ >= numElements_) {
      throw IndexOutOfBounds();
    }

    auto elementsOriginal = arrayElementStart_;
    auto elementsBaseOffsetOriginal = elementsBaseOffset_;

    if (isFixedLength_) {
      // Fixed width data is represented as continuous T * data
      arrayElementStart_ += fixedDataWidth_;
    } else {
      // increment element pointer to the next variable length element
      arrayElementStart_ += UnsafeRow::kFieldWidthBytes;
      // increment base offset
      elementsBaseOffset_ += UnsafeRow::kFieldWidthBytes;
    }

    // if null
    if (bits::isBitSet(nulls_.data(), idx_++)) {
      return std::nullopt;
    }

    if (isFixedLength_) {
      return std::string_view(elementsOriginal, fixedDataWidth_);
    }

    auto [size, offset] = readDataPointer(elementsOriginal);
    return std::string_view(
        elementsOriginal - elementsBaseOffsetOriginal + offset, size);
  }

 private:
  /**
   * The number of elements in the array.
   */
  size_t numElements_;

  /**
   * The UnsfaeRow nulls set.
   */
  std::string_view nulls_;

  /**
   * The start of the elements in the array (i.e. the array representation
   * not including the UnsafeRow array metadata).
   */
  const char* arrayElementStart_;

  /**
   * The elements offset with respect to the beginning of the array data
   */
  size_t elementsBaseOffset_;

  /**
   * Whether the elements are fixed length.
   */
  bool isFixedLength_;

  /**
   * The width of the elements if it is fixed length
   */
  size_t fixedDataWidth_;

  /**
   * The current element index.
   */
  size_t idx_ = 0;

  /**
   * Reads an UnsafeRow data pointer.
   * @param data
   * @return the size and offset as a tuple
   */
  std::tuple<uint32_t, uint32_t> readDataPointer(const char* data) {
    const uint64_t* dataPointer = &reinterpret_cast<const uint64_t*>(data)[0];
    uint32_t size = reinterpret_cast<const uint32_t*>(dataPointer)[0];
    uint32_t offset = reinterpret_cast<const uint32_t*>(dataPointer)[1];
    return std::tuple(size, offset);
  }
};
using ArrayIteratorPtr = std::shared_ptr<UnsafeRowArrayIterator>;

/**
 * Iterator representation of an UnsafeRowMap object.
 */
struct UnsafeRowMapIterator : UnsafeRowDataIterator {
 public:
  /**
   * UnsafeRowMapIterator constructor. The elements in an UnsafeRow Map appears
   * as an UnsafeRow array.
   * @param data
   * @param type
   */
  explicit UnsafeRowMapIterator(
      std::optional<std::string_view> data,
      TypePtr type = nullptr)
      : UnsafeRowDataIterator(!data.has_value(), type) {
    /*
     * UnsafeRow Map representation:
     * [offset to values : 1 word]
     * [keys in the form of an UnsafeRowArray]
     * [values in the form of an UnsafeRowArray]
     */

    if (!data.has_value()) {
      return;
    }

    auto mapTypePtr = std::dynamic_pointer_cast<const MapType>(type);

    // offsetToValues is at least 8, even if the map is empty
    size_t offsetToValues = reinterpret_cast<const uint64_t*>(data->data())[0];

    auto keysStart = data->data() + UnsafeRow::kFieldWidthBytes;
    auto keysData = std::string_view(keysStart, offsetToValues);
    auto valuesStart = keysStart + offsetToValues;
    auto valuesData =
        std::string_view(valuesStart, data->size() - keysData.size());

    // UnsafeRow complex elements are in the form of UnsafeRowArrays, wrap the
    // elements with an ArrayIterator so we can interpret the data.
    keysIteratorWrapper_ = std::dynamic_pointer_cast<UnsafeRowArrayIterator>(
        getIteratorPtr(keysData, ARRAY(mapTypePtr->keyType())));
    valuesIteratorWrapper_ = std::dynamic_pointer_cast<UnsafeRowArrayIterator>(
        getIteratorPtr(valuesData, ARRAY(mapTypePtr->valueType())));

    // The number of keys and values must be the same
    assert(keysIteratorWrapper_->size() == valuesIteratorWrapper_->size());
    numElements_ = keysIteratorWrapper_->size();
  }

  /**
   * @return the keysIteratorWrapper.
   */
  ArrayIteratorPtr keysIteratorWrapper() {
    return keysIteratorWrapper_;
  }

  /**
   * @return the valuesIteratorWrapper.
   */
  ArrayIteratorPtr valuesIteratorWrapper() {
    return valuesIteratorWrapper_;
  }

  /**
   * @return the number of key-value pairs in the map.
   */
  size_t size() override {
    return numElements_;
  }

 private:
  /**
   * The number of key-value pairs in the map.
   */
  size_t numElements_;

  /**
   * The keys iterator wrapped as an UnsafeRowArray.
   */
  ArrayIteratorPtr keysIteratorWrapper_;

  /**
   * The values iterator wrapped as an UnsafeRowArray.
   */
  ArrayIteratorPtr valuesIteratorWrapper_;
};

/**
 * Constructs a DataIteratorPtr based on the specified type.
 * @param data a string_view over the data to be converted to a DataIteratorPtr.
 * @param type the element type
 * @return a DataIteratorPtr.
 */
inline DataIteratorPtr getIteratorPtr(
    std::optional<std::string_view> data,
    TypePtr type) {
  if (type->isPrimitiveType()) {
    return std::make_shared<UnsafeRowPrimitiveIterator>(data, type);
  }
  if (type->isArray()) {
    auto arrayTypePtr = std::dynamic_pointer_cast<const ArrayType>(type);
    auto childTypePtr = arrayTypePtr->elementType();
    size_t childWidth =
        childTypePtr->isFixedWidth() ? childTypePtr->cppSizeInBytes() : 0;

    return std::make_shared<UnsafeRowArrayIterator>(
        data, childTypePtr->isFixedWidth(), childWidth, type);
  }
  if (type->isMap()) {
    return std::make_shared<UnsafeRowMapIterator>(data, type);
  }

  return nullptr;
}

/**
 * UnsafeRowDeserializer for primitive types.
 */
struct UnsafeRowPrimitiveDeserializer {
  /**
   * @tparam T the native type to deserialize to
   * @param data
   * @return the native type value
   */
  template <typename T>
  static T deserializeFixedWidth(std::string_view data) {
    assert(std::is_fundamental_v<T>);
    return reinterpret_cast<const T*>(data.data())[0];
  }

  /**
   * @param data
   * @return the value in velox::StringView
   */
  static StringView deserializeStringView(std::string_view data) {
    return StringView(data.data(), data.size());
  }
};

/**
 * TempaltedDeserializer that deserializes to std:: objects.
 * @tparam SqlType
 * @tparam NativeType
 */
template <typename SqlType, typename NativeType>
struct UnsafeRowStaticDeserializer {
  /**
   * Deserializes primitive types.
   * @param data
   * @return The std::optional<NativeType> value
   */
  static std::optional<NativeType> deserialize(
      std::optional<std::string_view> data) {
    if constexpr (
        UnsafeRowStaticUtilities::simpleSqlTypeToTypeKind<SqlType>() ==
        TypeKind::INVALID) {
      VELOX_NYI("Invalid deserialize SqlType");
    } else {
      if (!data.has_value()) {
        return std::nullopt;
      } else if constexpr (UnsafeRowStaticUtilities::isFixedWidth<SqlType>()) {
        return UnsafeRowPrimitiveDeserializer::deserializeFixedWidth<
            NativeType>(data.value());
      } else if constexpr (TypeTraits<UnsafeRowStaticUtilities::
                                          simpleSqlTypeToTypeKind<SqlType>()>::
                               isPrimitive) {
        // NativeElement is std::string_view instead of velox::StringView, so
        // append the next std::string_view directly
        return data;
      } else {
        // Complex types will be captured via template specializations
        VELOX_NYI(
            "Unsupported deserialize SqlType: {}",
            UnsafeRowStaticUtilities::simpleSqlTypeToTypeKind<SqlType>())
      }
    }
  }
};

/**
 * Partial template expansion for Array types.
 * @tparam SqlElement
 * @tparam NativeElement
 */
template <typename SqlElement, typename NativeElement>
struct UnsafeRowStaticDeserializer<
    Array<SqlElement>,
    std::vector<NativeElement>> {
  /**
   * Serializes an Array into a vector.
   * @param data
   * @return std::optional<std::vector<std::optional<NativeElement>>>
   */
  static std::optional<std::vector<std::optional<NativeElement>>> deserialize(
      std::optional<std::string_view> data) {
    if (!data.has_value()) {
      return std::nullopt;
    }

    std::vector<std::optional<NativeElement>> retVal;

    auto arrayIterator = UnsafeRowArrayIterator(
        data.value(),
        UnsafeRowStaticUtilities::isFixedWidth<SqlElement>(),
        sizeof(NativeElement));

    while (arrayIterator.hasNext()) {
      auto element =
          UnsafeRowStaticDeserializer<SqlElement, NativeElement>::deserialize(
              arrayIterator.next());
      retVal.push_back(element);
    }
    return retVal;
  }
};

/**
 * Partial template expansion for Map types.
 * @tparam SqlKey
 * @tparam SqlVal
 * @tparam NativeKey
 * @tparam NativeVal
 */
template <
    typename SqlKey,
    typename SqlVal,
    typename NativeKey,
    typename NativeVal>
struct UnsafeRowStaticDeserializer<
    Map<SqlKey, SqlVal>,
    std::multimap<NativeKey, NativeVal>> {
  /**
   * Deserializes an UnsafeRow map into a std::multimap.  Note that UnsafeRow
   * does not guarantee unique keys, so we do not want to return a std::map.
   * From sql documentation, primary key cannot be null, so NativeKey does not
   * have to be nullable.
   * @param data
   * @return std::optional<
      std::multimap<NativeKey, std::optional<NativeVal>>>
   */
  static std::optional<std::multimap<NativeKey, std::optional<NativeVal>>>
  deserialize(std::optional<std::string_view> data) {
    if (!data.has_value()) {
      return std::nullopt;
    }

    // parser the UnsafeRow Map and get the string_view over the keys and vals
    size_t offsetToValues = reinterpret_cast<const uint64_t*>(data->data())[0];
    auto keysStart = data->data() + UnsafeRow::kFieldWidthBytes;
    auto keysData = std::string_view(keysStart, offsetToValues);
    auto valuesStart = keysStart + offsetToValues;
    auto valuesData =
        std::string_view(valuesStart, data->size() - keysData.size());

    // create UnsafeRowArrayIterators to iterate through the key-value pairs
    auto keyIterator = UnsafeRowArrayIterator(
        keysData,
        UnsafeRowStaticUtilities::isFixedWidth<SqlKey>(),
        sizeof(NativeKey));
    auto valIterator = UnsafeRowArrayIterator(
        valuesData,
        UnsafeRowStaticUtilities::isFixedWidth<SqlVal>(),
        sizeof(NativeVal));

    assert(keyIterator.size() == valIterator.size());

    std::multimap<NativeKey, std::optional<NativeVal>> retVal;
    while (keyIterator.hasNext() && valIterator.hasNext()) {
      std::optional<NativeKey> key =
          UnsafeRowStaticDeserializer<SqlKey, NativeKey>::deserialize(
              keyIterator.next());
      std::optional<NativeVal> val =
          UnsafeRowStaticDeserializer<SqlVal, NativeKey>::deserialize(
              valIterator.next());
      if (key.has_value()) {
        // Sql does not allow null primary keys
        retVal.insert(std::make_pair(key.value(), val));
      }
    }

    return retVal;
  }
};

/**
 * UnsafeRow dynamic deserializer using TypePtr, deserializes an UnsafeRow to
 * a Vector.
 */
struct UnsafeRowDynamicVectorDeserializer {
  /**
   * Runs modified BFS on the DataIteratorsPtr traversalQueue, returns a vector
   * of DataIteratorsPtr in its flattened representation. The flattened
   * representation can then be converted to the Vector representation.
   * @param traversalQueue
   * @param retVal the return vector value
   * @return the element's flattened DataIteratorsPtr representation
   */
  static std::vector<DataIteratorPtr> getFlattenedIteratorsBFS(
      std::deque<DataIteratorPtr>& traversalQueue,
      std::vector<DataIteratorPtr>& retVal) {
    while (!traversalQueue.empty()) {
      auto currIterator = traversalQueue.front();
      traversalQueue.pop_front();

      if (!currIterator->isNull()) {
        // If the current iterator is a primitive type, we've reached the root.
        // If the current iterator is a map type, delay flattening the elements
        // until we convert the map iterator to a map vector.

        if (currIterator->type()->isArray()) {
          auto arrayTypePtr =
              std::dynamic_pointer_cast<const ArrayType>(currIterator->type());
          auto arrayIteratorPtr =
              std::dynamic_pointer_cast<UnsafeRowArrayIterator>(currIterator);

          while (arrayIteratorPtr->hasNext()) {
            auto childIterator = getIteratorPtr(
                arrayIteratorPtr->next(), arrayTypePtr->elementType());
            // Primitive type means we've reached the root, don't add it to the
            // traversal queue.  For map types, we delay flattening the elements
            // until conversion to vectors.
            if (!arrayTypePtr->elementType()->isPrimitiveType() &&
                !arrayTypePtr->elementType()->isMap()) {
              traversalQueue.emplace_back(childIterator);
            }
            retVal.emplace_back(childIterator);
          }
        }
      }
    }
    return retVal;
  }

  /**
   * Construct a flattened representation of the UnsafeRow using
   * DataIteratorsPtr.
   * @param data
   * @param type
   * @return the element's flattened DataIteratorsPtr representation
   */
  static std::vector<DataIteratorPtr> flattenComplexData(
      std::optional<std::string_view> data,
      TypePtr type) {
    std::vector<DataIteratorPtr> retVal;

    // If the element is primitive, wrap the primitive type with an
    // ArrayIteratorPtr so we can loop through the list of values, return the
    // list of PrimitiveIterators directly
    if (type->isPrimitiveType()) {
      auto arrayIteratorWrapper =
          std::dynamic_pointer_cast<UnsafeRowArrayIterator>(
              getIteratorPtr(data, ARRAY(type)));
      while (arrayIteratorWrapper->hasNext()) {
        retVal.emplace_back(getIteratorPtr(arrayIteratorWrapper->next(), type));
      }
      return retVal;
    }

    // For complex types, run BFS to get the flattened representation.
    std::deque<DataIteratorPtr> traversalQueue;
    auto iterator = getIteratorPtr(data, type);
    traversalQueue.emplace_back(iterator);
    retVal.emplace_back(iterator);

    return getFlattenedIteratorsBFS(traversalQueue, retVal);
  }

  /**
   * Allocate and populate the metadata Vectors in ArrayVector or MapVector.
   * @param dataIterators iterator that points to the first dataIterator to
   * process.
   * @param pool
   * @param numIteratorsToProcess
   * @return the populated metadata vectors and the number of null elements.
   */
  inline static std::tuple<BufferPtr, BufferPtr, BufferPtr, size_t>
  populateMetadataVectors(
      std::vector<DataIteratorPtr>::iterator dataIterators,
      memory::MemoryPool* pool,
      size_t numIteratorsToProcess) {
    BufferPtr offsets =
        AlignedBuffer::allocate<int32_t>(numIteratorsToProcess, pool);
    BufferPtr lengths =
        AlignedBuffer::allocate<vector_size_t>(numIteratorsToProcess, pool);
    BufferPtr nulls = AlignedBuffer::allocate<char>(
        bits::nbytes(numIteratorsToProcess), pool);
    auto* offsetsPtr = offsets->asMutable<int32_t>();
    auto* lengthsPtr = lengths->asMutable<vector_size_t>();
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    for (int i = 0; i < numIteratorsToProcess; i++) {
      DataIteratorPtr currItr = dataIterators[i];
      bits::setBit(
          nullsPtr, i, bits::kNull ? currItr->isNull() : !currItr->isNull());
      nullCount += currItr->isNull();
      lengthsPtr[i] = currItr->isNull() ? 0 : currItr->size();
      offsetsPtr[i] = i == 0 ? 0 : offsetsPtr[i - 1] + lengthsPtr[i - 1];
    }

    return std::tuple(offsets, lengths, nulls, nullCount);
  }

  /**
   * Give some number of dataIterators, find the total number of elements
   * encapsulated by the dataIterators.
   * @param dataIterators iterator that points to the first dataIterator to
   * process.
   * @param numIteratorsToProcess
   * @return
   */
  inline static size_t getNumChildrenElements(
      std::vector<DataIteratorPtr>::iterator dataIterators,
      size_t numIteratorsToProcess) {
    size_t totalNumElements = 0;
    for (auto itr = dataIterators; itr != dataIterators + numIteratorsToProcess;
         ++itr) {
      DataIteratorPtr itrPtr = *itr;
      totalNumElements += itrPtr->size();
    }
    return totalNumElements;
  }

  /**
   * Converts a list of UnsafeRowMapIterators to Vectors.
   * @param dataIterators iterator that points to the first dataIterator to
   * process.
   * @param pool
   * @param numIteratorsToProcess The number of adjacent UnsafeRowMapIterators
   * @return a MapVectorPtr
   */
  static VectorPtr convertMapIteratorsToVectors(
      std::vector<DataIteratorPtr>::iterator dataIterators,
      memory::MemoryPool* pool,
      size_t numIteratorsToProcess) {
    TypePtr type = (*dataIterators)->type();
    assert(type->isMap());

    size_t numMaps = numIteratorsToProcess;

    auto [offsets, lengths, nulls, nullCount] =
        populateMetadataVectors(dataIterators, pool, numMaps);

    std::deque<DataIteratorPtr> keysTraversalQueue, valuesTraversalQueue;
    for (int i = 0; i < numMaps; i++) {
      // add the iterator wrappers to the traversal queue but not the final
      // flattened iterators
      auto mapIteratorPtr =
          std::dynamic_pointer_cast<UnsafeRowMapIterator>(dataIterators[i]);
      keysTraversalQueue.emplace_back(mapIteratorPtr->keysIteratorWrapper());
      valuesTraversalQueue.emplace_back(
          mapIteratorPtr->valuesIteratorWrapper());
    }
    std::vector<DataIteratorPtr> flattenedKeys, flattenedValues;
    getFlattenedIteratorsBFS(keysTraversalQueue, flattenedKeys);
    getFlattenedIteratorsBFS(valuesTraversalQueue, flattenedValues);

    size_t totalNumElements =
        getNumChildrenElements(dataIterators, numIteratorsToProcess);

    VectorPtr keys =
        convertToVectors(flattenedKeys.begin(), pool, totalNumElements);
    VectorPtr values =
        convertToVectors(flattenedValues.begin(), pool, totalNumElements);

    return std::make_shared<MapVector>(
        pool, type, nulls, numMaps, offsets, lengths, keys, values, nullCount);
  }

  /**
   * Converts a list of UnsafeRowArrayIterators to Vectors.
   * @param dataIterators iterator that points to the first dataIterator to
   * process.
   * @param pool
   * @param numIteratorsToProcess The number of adjacent UnsafeRowArrayIterators
   * @return an ArrayVectorPtr
   */
  static VectorPtr convertArrayIteratorsToVectors(
      std::vector<DataIteratorPtr>::iterator dataIterators,
      memory::MemoryPool* pool,
      size_t numIteratorsToProcess) {
    TypePtr type = (*dataIterators)->type();
    assert(type->isArray());

    size_t numArrays = numIteratorsToProcess;

    auto [offsets, lengths, nulls, nullCount] =
        populateMetadataVectors(dataIterators, pool, numArrays);

    // get the array elements
    size_t totalNumElements =
        getNumChildrenElements(dataIterators, numIteratorsToProcess);
    VectorPtr elements = convertToVectors(
        dataIterators + numIteratorsToProcess, pool, totalNumElements);

    return std::make_shared<ArrayVector>(
        pool, type, nulls, numArrays, offsets, lengths, elements, nullCount);
  }

  /**
   * Converts a list of UnsafeRowPrimitiveIterators to a FlatVector
   * @tparam T the element's NativeType
   * @param dataIterators iterator that points to the first dataIterator to
   * process.
   * @param type The element type
   * @param pool
   * @param numIteratorsToProcess The number of adjacent
   * UnsafeRowPrimitiveIterators
   * @return a FlatVector
   */
  template <typename T>
  static VectorPtr createFlatVector(
      std::vector<DataIteratorPtr>::iterator dataIterators,
      TypePtr type,
      memory::MemoryPool* pool,
      size_t numIteratorsToProcess) {
    auto vector = BaseVector::create(type, numIteratorsToProcess, pool);

    size_t nullCount = 0;
    for (size_t i = 0; i < numIteratorsToProcess; i++) {
      auto iterator = std::dynamic_pointer_cast<UnsafeRowPrimitiveIterator>(
          dataIterators[i]);

      if (iterator->isNull()) {
        vector->setNull(i, true);
      } else {
        vector->setNull(i, false);

        if constexpr (std::is_same_v<T, StringView>) {
          StringView val =
              UnsafeRowPrimitiveDeserializer::deserializeStringView(
                  iterator->data()->data());
          auto flatVector = vector->asFlatVector<StringView>();
          flatVector->set(i, val);
        } else {
          T val = UnsafeRowPrimitiveDeserializer::deserializeFixedWidth<T>(
              iterator->data()->data());
          auto flatVector = vector->asFlatVector<T>();
          flatVector->set(i, val);
        }
        nullCount++;
      }
    }
    vector->setNullCount(nullCount);
    return vector;
  }

  /**
   * Calls createFlatVector with the correct template argument.
   * @param dataIterators iterator that points to the first dataIterator to
   * process.
   * @param pool
   * @param numIteratorsToProcess
   * @return A FlatVector
   */
  static VectorPtr convertPrimitiveIteratorsToVectors(
      std::vector<DataIteratorPtr>::iterator dataIterators,
      memory::MemoryPool* pool,
      size_t numIteratorsToProcess) {
    TypePtr type = (*dataIterators)->type();
    assert(type->isPrimitiveType());

    if (type->isBoolean()) {
      return createFlatVector<TypeTraits<TypeKind::BOOLEAN>::NativeType>(
          dataIterators, type, pool, numIteratorsToProcess);
    } else if (type->isTinyint()) {
      return createFlatVector<TypeTraits<TypeKind::TINYINT>::NativeType>(
          dataIterators, type, pool, numIteratorsToProcess);
    } else if (type->isSmallint()) {
      return createFlatVector<TypeTraits<TypeKind::SMALLINT>::NativeType>(
          dataIterators, type, pool, numIteratorsToProcess);
    } else if (type->isInteger()) {
      return createFlatVector<TypeTraits<TypeKind::INTEGER>::NativeType>(
          dataIterators, type, pool, numIteratorsToProcess);
    } else if (type->isBigint()) {
      return createFlatVector<TypeTraits<TypeKind::BIGINT>::NativeType>(
          dataIterators, type, pool, numIteratorsToProcess);
    } else if (type->isReal()) {
      return createFlatVector<TypeTraits<TypeKind::REAL>::NativeType>(
          dataIterators, type, pool, numIteratorsToProcess);
    } else if (type->isDouble()) {
      return createFlatVector<TypeTraits<TypeKind::DOUBLE>::NativeType>(
          dataIterators, type, pool, numIteratorsToProcess);
    } else if (type->isVarchar() || type->isVarbinary()) {
      return createFlatVector<StringView>(
          dataIterators, type, pool, numIteratorsToProcess);
    } else if (type->isTimestamp()) {
      return createFlatVector<TypeTraits<TypeKind::TIMESTAMP>::NativeType>(
          dataIterators, type, pool, numIteratorsToProcess);
    } else {
      VELOX_NYI(
          "Unsupported complex type in convertPrimitiveIteratorsToVectors")
    }
  }

  /**
   * Calls the correct function to convert the iterators to vectors based on
   * element type.
   * @param dataIterators iterator that points to the first dataIterator to
   * process.
   * @param pool
   * @param numIteratorsToProcess
   * @return
   */
  static VectorPtr convertToVectors(
      std::vector<DataIteratorPtr>::iterator dataIterators,
      memory::MemoryPool* pool,
      size_t numIteratorsToProcess = 1) {
    TypePtr type = (*dataIterators)->type();

    if (type->isMap()) {
      return convertMapIteratorsToVectors(
          dataIterators, pool, numIteratorsToProcess);
    } else if (type->isArray()) {
      return convertArrayIteratorsToVectors(
          dataIterators, pool, numIteratorsToProcess);
    } else if (type->isPrimitiveType()) {
      return convertPrimitiveIteratorsToVectors(
          dataIterators, pool, numIteratorsToProcess);
    } else {
      VELOX_NYI("Unsupported data iterators type");
    }
  }

  /**
   * Deserializes a complex element type to its Vector representation.
   * @param data A string_view over a given element in the UnsafeRow.
   * @param type the element type.
   * @param pool the memory pool to allocate Vectors from
   * @return a VectorPtr
   */
  static VectorPtr deserializeComplex(
      std::optional<std::string_view> data,
      TypePtr type,
      memory::MemoryPool* pool) {
    // flatten data
    std::vector<DataIteratorPtr> iterators = flattenComplexData(data, type);
    // deserialize to vector
    return convertToVectors(iterators.begin(), pool);
  }
};

} // namespace facebook::velox::row
