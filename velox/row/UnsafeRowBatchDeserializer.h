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
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::row {

/**
 * A virtual class that represents iterators that loop through UnsafeRow data.
 */
class UnsafeRowDataBatchIterator {
 public:
  /**
   * UnsafeRowDataBatchIterator constructor.
   * @param data the Data which needs to be process
   * @param type the element type
   */
  UnsafeRowDataBatchIterator(
      const std::vector<std::optional<std::string_view>>& data,
      const TypePtr& type)
      : data_(data), numRows_(data.size()), type_(type) {}

  virtual ~UnsafeRowDataBatchIterator() = 0;

  /**
   * Reads an UnsafeRow data pointer.
   * @param data
   * @return the size and offset as a tuple
   */
  FOLLY_ALWAYS_INLINE std::tuple<uint32_t, uint32_t> readDataPointer(
      const char* data) {
    const uint64_t offsetAndSize = *reinterpret_cast<const uint64_t*>(data);
    return {
        /*size*/ static_cast<uint32_t>(offsetAndSize),
        /*offset*/ offsetAndSize >> 32};
  }

  /**
   * @param idx
   * @return true if element is null.
   */
  virtual bool isNull(size_t idx) {
    return !data_[idx].has_value();
  }

  /**
   * @param idx
   * @return A primitive value occupies 1 field, so return 0 if the element is
   * null, 1 otherwise.
   */
  virtual size_t size(size_t idx) {
    return isNull(idx) ? 0 : 1;
  }

  /**
   * @return  return total number of rows.
   */
  size_t numRows() {
    return numRows_;
  }

  /**
   * @return the element type.
   */
  const TypePtr& type() {
    return type_;
  }

  virtual std::string toString(size_t idx) {
    std::stringstream str;
    str << "Data iterator of type " << type()->toString() << " of size "
        << size(idx);
    return str.str();
  }

 protected:
  /**
   * The data need to be processed. The vector represent a whole column of
   * data over.
   */
  const std::vector<std::optional<std::string_view>>& data_;

  /**
   * The number of rows in the given data.
   */
  size_t numRows_ = 0;

 private:
  /**
   * The element type.
   */
  TypePtr type_;
};

inline UnsafeRowDataBatchIterator::~UnsafeRowDataBatchIterator() {}

using DataBatchIteratorPtr = std::shared_ptr<UnsafeRowDataBatchIterator>;

/**
 * Iterator that represents a primitive object. Primitive includes strings
 * and anything that can be represented as a c++ fundamental type with at most
 * 8 bytes.
 */
struct UnsafeRowPrimitiveBatchIterator : UnsafeRowDataBatchIterator {
 public:
  /**
   * UnsafeRowPrimitiveBatchIterator constructor.
   * @param data The part of the UnsafeRow data buffer that contains the
   * primitive value
   * @param type The element type
   */
  UnsafeRowPrimitiveBatchIterator(
      const std::vector<std::optional<std::string_view>>& data,
      const TypePtr& type)
      : UnsafeRowDataBatchIterator(data, type) {}

  /**
   * @return a string_view over the part of the UnsafeRow data buffer that
   * contains the primitive value.
   */
  std::optional<std::string_view> data(size_t idx = 0) {
    return data_[idx];
  }

  bool hasNext() {
    return currentRow_ < numRows_;
  }

  const std::optional<std::string_view>& next() {
    VELOX_CHECK(hasNext());
    return data_[currentRow_++];
  }

  std::string toString(size_t idx) override {
    std::stringstream str;
    str << "Data iterator of type " << type()->toString() << " of size "
        << size(idx) << " with data "
        << (data_[idx].has_value() ? data_[idx].value() : "NULL");
    return str.str();
  }

 private:
  size_t currentRow_ = 0;
};

/**
 * Iterator representation of an UnsafeRow Struct object.
    UnsafeRow Struct representation:
    [nullset : (num elements + 63) / 64
    words] [variable length data: variable]
 */
struct UnsafeRowStructBatchIterator : UnsafeRowDataBatchIterator {
 public:
  /**
   * UnsafeRowStructBatchIterator constructor.
   * @param data
   * @param type
   */
  explicit UnsafeRowStructBatchIterator(
      const std::vector<std::optional<std::string_view>>& data,
      const TypePtr& type)
      : UnsafeRowDataBatchIterator(data, type),
        childrenTypes_(
            std::dynamic_pointer_cast<const RowType>(type)->children()),
        numElements_(std::dynamic_pointer_cast<const RowType>(type)->size()) {
    columnData_.resize(numRows_);
  }

  /**
   * @return return whether there's a next element.
   */
  bool hasNext() const {
    return idx_ < numElements_;
  }

  std::string toString(size_t idx) override {
    std::stringstream str;
    str << "Data iterator of type " << type()->toString() << " of size "
        << size(idx) << " hasNext " << hasNext();
    return str.str();
  }

  /**
   * @return throws IndexOutOfBounds if there is no next element,
   * std::nullopt if the next element is null, or the next element with it's
   * type.
   */
  const std::vector<std::optional<std::string_view>>& nextColumnBatch() {
    const TypePtr& type = childrenTypes_[idx_];
    bool isFixedLength = type->isFixedWidth();
    std::size_t cppSizeInBytes =
        type->isFixedWidth() ? type->cppSizeInBytes() : 0;
    std::size_t fieldOffset = UnsafeRow::getNullLength(numElements_) +
        idx_ * UnsafeRow::kFieldWidthBytes;

    for (int32_t i = 0; i < numRows_; ++i) {
      const char* rawData = data_[i]->data();
      const char* fieldData = rawData + fieldOffset;

      // if null
      if (bits::isBitSet(rawData, idx_)) {
        columnData_[i] = std::nullopt;
        continue;
      }
      // Fixed length field
      if (cppSizeInBytes) {
        columnData_[i] = std::string_view(fieldData, cppSizeInBytes);
        continue;
      }

      auto [size, offset] = readDataPointer(fieldData);
      columnData_[i] = std::string_view(rawData + offset, size);
    }
    ++idx_;
    return columnData_;
  }

  /**
   * @return the number of elements in the idx-th row.
   */
  size_t size(size_t idx) override {
    return isNull(idx) ? 0 : numElements_;
  }

 private:
  /**
   * The current element index.
   */
  size_t idx_ = 0;

  /**
   * The types of children.
   */
  const std::vector<TypePtr>& childrenTypes_;

  /**
   * The number of elements in the struct.
   */
  size_t numElements_;

  /**
   * The current processing column data.
   */
  std::vector<std::optional<std::string_view>> columnData_;
};
using StructBatchIteratorPtr = std::shared_ptr<UnsafeRowStructBatchIterator>;

/**
 * Iterator to traverse through an UnsafeRow array representation.  This is
 used
 * for:
 * UnsafeRow array objects, UnsafeRow Map keys, and UnsafeRow Map values.
 *
 * [number of elements : 1 word]
 * [nullset : (num elements + 63) / 64 words]
 * [fixed length data or offsets: (num elements) words]
 * [variable length data: variable]
 */
struct UnsafeRowArrayBatchIterator : UnsafeRowDataBatchIterator {
 public:
  /**
   * Constructor for UnsafeRowArray.
   * @param data A batch of unsafe row array representation data.
   * @param isFixedLength whether the elements in the array is fixed length
   * @param fixedDataWidth the data width if the element is fixed length
   */
  UnsafeRowArrayBatchIterator(
      const std::vector<std::optional<std::string_view>>& data,
      const TypePtr& type,
      bool isFixedLength,
      size_t fixedDataWidth = 0)
      : UnsafeRowDataBatchIterator(data, type),
        isFixedLength_(isFixedLength),
        fixedDataWidth_(fixedDataWidth) {
    elementType_ = type->asArray().elementType();
    totalNumElements_ = 0L;

    for (int32_t i = 0; i < numRows_; ++i) {
      if (data_[i].has_value()) {
        totalNumElements_ +=
            reinterpret_cast<const uint64_t*>(data_[i]->data())[0];
      }
    }
    columnData_.resize(totalNumElements_);
  }

  int64_t totalNumElements() {
    return totalNumElements_;
  }

  const TypePtr getElementType() {
    return elementType_;
  }

  /**
   * @return the size of the array
   */
  size_t size(size_t idx) override {
    return isNull(idx)
        ? 0
        : reinterpret_cast<const uint64_t*>(data_[idx]->data())[0];
  }

  bool childIsFixedLength() {
    return isFixedLength_;
  }

  std::string toString(size_t idx) override {
    std::stringstream str;
    str << "Data iterator of type " << type()->toString() << " of size "
        << size(idx) << " childIsFixedLength " << childIsFixedLength();
    return str.str();
  }

  /**
   * @return std::nullopt if the next element is null, or the next element.
   */
  const std::vector<std::optional<std::string_view>>& nextColumnBatch() {
    size_t idx = 0;
    size_t fieldWidth =
        isFixedLength_ ? fixedDataWidth_ : UnsafeRow::kFieldWidthBytes;
    size_t offsetWidth = isFixedLength_ ? 0 : UnsafeRow::kFieldWidthBytes;
    for (int32_t i = 0; i < numRows_; ++i) {
      if (!data_[i].has_value()) {
        continue;
      }
      const char* rawData = data_[i]->data();
      const char* nullSet = rawData + UnsafeRow::kFieldWidthBytes;
      size_t numElement = reinterpret_cast<const uint64_t*>(rawData)[0];
      auto nullLengthBytes = UnsafeRow::getNullLength(numElement);
      const size_t elementOffsetBase =
          UnsafeRow::kFieldWidthBytes + nullLengthBytes;
      for (int32_t j = 0; j < numElement; ++j) {
        const size_t elementOffset = elementOffsetBase + j * offsetWidth;
        const char* fieldData = rawData + elementOffsetBase + j * fieldWidth;

        // if null
        if (bits::isBitSet(nullSet, j)) {
          columnData_[idx++] = std::nullopt;
          continue;
        }

        if (isFixedLength_) {
          columnData_[idx++] = std::string_view(fieldData, fixedDataWidth_);
          continue;
        }

        auto [size, offset] = readDataPointer(fieldData);
        columnData_[idx++] =
            std::string_view(fieldData - elementOffset + offset, size);
      }
    }
    return columnData_;
  }

 private:
  /**
   * Whether the elements are fixed length.
   */
  bool isFixedLength_;

  /**
   * The width of the elements if it is fixed length
   */
  size_t fixedDataWidth_;

  /**
   * the children elements of all array data for the whole column.
   */
  std::vector<std::optional<std::string_view>> columnData_;

  /**
   * The element type.
   */
  TypePtr elementType_;

  /**
   * The total number of elements in all the array the current iterator
   * represents.
   */
  int64_t totalNumElements_;
};
using ArrayBatchIteratorPtr = std::shared_ptr<UnsafeRowArrayBatchIterator>;

DataBatchIteratorPtr getBatchIteratorPtr(
    const std::vector<std::optional<std::string_view>>& data,
    const TypePtr& type);

/**
 * Iterator representation of an UnsafeRowMap object.
 */
struct UnsafeRowMapBatchIterator : UnsafeRowDataBatchIterator {
 public:
  /**
   * UnsafeRowMapIterator constructor. The elements in an UnsafeRow Map
   * appears as an UnsafeRow array.
   * @param data
   * @param type
   */
  explicit UnsafeRowMapBatchIterator(
      const std::vector<std::optional<std::string_view>>& data,
      const TypePtr& type)
      : UnsafeRowDataBatchIterator(data, type) {
    keyArray_.resize(numRows_);
    valueArray_.resize(numRows_);
    for (int32_t i = 0; i < numRows_; ++i) {
      /*
       * UnsafeRow Map representation:
       * [offset to values : 1 word]
       * [keys in the form of an UnsafeRowArray]
       * [values in the form of an UnsafeRowArray]
       */

      if (!data_[i].has_value()) {
        continue;
      }

      // offsetToValues is at least 8, even if the map is empty
      size_t offsetToValues =
          reinterpret_cast<const uint64_t*>(data_[i]->data())[0];

      auto keysStart = data_[i]->data() + UnsafeRow::kFieldWidthBytes;
      auto keysData = std::string_view(keysStart, offsetToValues);
      keyArray_[i] = keysData;
      auto valuesStart = keysStart + offsetToValues;
      valueArray_[i] =
          std::string_view(valuesStart, data_[i]->size() - keysData.size());
    }

    auto mapTypePtr = std::dynamic_pointer_cast<const MapType>(type);
    keysIteratorWrapper_ =
        std::dynamic_pointer_cast<UnsafeRowArrayBatchIterator>(
            getBatchIteratorPtr(keyArray_, ARRAY(mapTypePtr->keyType())));
    valuesIteratorWrapper_ =
        std::dynamic_pointer_cast<UnsafeRowArrayBatchIterator>(
            getBatchIteratorPtr(valueArray_, ARRAY(mapTypePtr->valueType())));

    totalNumElements_ = 0;
    numElements_.resize(numRows_);
    for (int32_t i = 0; i < numRows_; ++i) {
      numElements_[i] = keysIteratorWrapper_->size(i);
      totalNumElements_ += numElements_[i];
    }
  }

  /**
   * @return the keysIteratorWrapper.
   */
  ArrayBatchIteratorPtr keysIteratorWrapper() {
    return keysIteratorWrapper_;
  }

  /**
   * @return the valuesIteratorWrapper.
   */
  ArrayBatchIteratorPtr valuesIteratorWrapper() {
    return valuesIteratorWrapper_;
  }

  /**
   * @return the number of key-value pairs in the map.
   */
  size_t size(size_t idx) override {
    return numElements_[idx];
  }

  /**
   * @return the total number of elements (key-value pairs) in the map.
   */
  int64_t totalNumElements() {
    return totalNumElements_;
  }

 private:
  /**
   * The keys iterator wrapped as an UnsafeRowArray.
   */
  ArrayBatchIteratorPtr keysIteratorWrapper_;

  /**
   * The values iterator wrapped as an UnsafeRowArray.
   */
  ArrayBatchIteratorPtr valuesIteratorWrapper_;

  std::vector<std::optional<std::string_view>> keyArray_;

  std::vector<std::optional<std::string_view>> valueArray_;

  std::vector<size_t> numElements_;

  int64_t totalNumElements_;
};

inline DataBatchIteratorPtr getBatchIteratorPtr(
    const std::vector<std::optional<std::string_view>>& data,
    const TypePtr& type) {
  if (type->isPrimitiveType()) {
    return std::make_shared<UnsafeRowPrimitiveBatchIterator>(data, type);
  } else if (type->isRow()) {
    return std::make_shared<UnsafeRowStructBatchIterator>(data, type);
  } else if (type->isArray()) {
    auto childTypePtr = type->asArray().elementType();
    size_t childWidth =
        childTypePtr->isFixedWidth() ? childTypePtr->cppSizeInBytes() : 0;
    return std::make_shared<UnsafeRowArrayBatchIterator>(
        data, type, childTypePtr->isFixedWidth(), childWidth);
  } else if (type->isMap()) {
    return std::make_shared<UnsafeRowMapBatchIterator>(data, type);
  }

  VELOX_NYI("Unknow data type " + type->toString());
  return nullptr;
}

/**
 * UnsafeRowDeserializer for primitive types.
 */
struct UnsafeRowPrimitiveBatchDeserializer {
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
 * UnsafeRow dynamic deserializer using TypePtr, deserializes an UnsafeRow to
 * a Vector.
 */
struct UnsafeRowDynamicVectorBatchDeserializer {
  /**
   * Allocate and populate the metadata Vectors in ArrayVector or MapVector.
   * @param dataIterators iterator that points to whole column batch of data.
   * process.
   * @param pool
   * @param size
   * @return the populated metadata vectors and the number of null elements.
   */
  inline static std::tuple<BufferPtr, BufferPtr, BufferPtr, size_t>
  populateMetadataVectors(
      const DataBatchIteratorPtr& dataIterator,
      memory::MemoryPool* pool,
      const size_t size) {
    BufferPtr offsets = AlignedBuffer::allocate<int32_t>(size, pool);
    BufferPtr lengths = AlignedBuffer::allocate<vector_size_t>(size, pool);
    BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), pool);
    auto* offsetsPtr = offsets->asMutable<int32_t>();
    auto* lengthsPtr = lengths->asMutable<vector_size_t>();
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    for (int i = 0; i < size; i++) {
      auto isNull = dataIterator->isNull(i);
      bits::setBit(nullsPtr, i, isNull ? bits::kNull : !bits::kNull);
      nullCount += isNull;
      lengthsPtr[i] = isNull ? 0 : dataIterator->size(i);
      offsetsPtr[i] = i == 0 ? 0 : offsetsPtr[i - 1] + lengthsPtr[i - 1];
    }

    return std::tuple(offsets, lengths, nulls, nullCount);
  }

  /**
   * Converts a list of UnsafeRowMapBatchIterators to Vectors.
   * @param dataIterators iterator that points to whole column batch of data.
   * @param pool
   * @return a MapVectorPtr
   */
  static VectorPtr convertMapIteratorsToVectors(
      const DataBatchIteratorPtr& dataIterator,
      memory::MemoryPool* pool) {
    TypePtr type = dataIterator->type();
    assert(type->isMap());

    auto* iteratorPtr =
        static_cast<UnsafeRowMapBatchIterator*>(dataIterator.get());
    size_t numMaps = iteratorPtr->numRows();

    auto [offsets, lengths, nulls, nullCount] =
        populateMetadataVectors(dataIterator, pool, numMaps);

    const auto& keysIterator = iteratorPtr->keysIteratorWrapper();
    const auto& valuesIterator = iteratorPtr->valuesIteratorWrapper();

    return std::make_shared<MapVector>(
        pool,
        type,
        nulls,
        numMaps,
        offsets,
        lengths,
        deserializeComplex(
            keysIterator->nextColumnBatch(), type->asMap().keyType(), pool),
        deserializeComplex(
            valuesIterator->nextColumnBatch(), type->asMap().valueType(), pool),
        nullCount);
  }

  /**
   * Converts a list of UnsafeRowArrayBatchIterators to Vectors.
   * @param dataIterator iterator that points to whole column batch of data.
   * process.
   * @param pool
   * @return an ArrayVectorPtr
   */
  static VectorPtr convertArrayIteratorsToVectors(
      const DataBatchIteratorPtr& dataIterator,
      memory::MemoryPool* pool) {
    TypePtr type = dataIterator->type();
    assert(type->isArray());

    auto* iteratorPtr =
        static_cast<UnsafeRowArrayBatchIterator*>(dataIterator.get());

    size_t numArrays = iteratorPtr->numRows();

    auto [offsets, lengths, nulls, nullCount] =
        populateMetadataVectors(dataIterator, pool, numArrays);

    // get the array elements
    size_t totalNumElements = iteratorPtr->totalNumElements();
    const TypePtr& elementType = iteratorPtr->getElementType();

    if (totalNumElements == 0) {
      return std::make_shared<ArrayVector>(
          pool, type, nulls, numArrays, offsets, lengths, nullptr, nullCount);
    }

    return std::make_shared<ArrayVector>(
        pool,
        type,
        nulls,
        numArrays,
        offsets,
        lengths,
        deserializeComplex(iteratorPtr->nextColumnBatch(), elementType, pool),
        nullCount);
  }

  /**
   * Converts a list of UnsafeRowStructBatchIterators to Vectors.
   * @param dataIterator iterator that points to whole column batch of data.
   * @param pool
   * @return an RowVectorPtr
   */
  static VectorPtr convertStructIteratorsToVectors(
      const DataBatchIteratorPtr& dataIterator,
      memory::MemoryPool* pool) {
    const TypePtr& type = dataIterator->type();
    assert(type->isRow());
    auto* StructBatchIteratorPtr =
        static_cast<UnsafeRowStructBatchIterator*>(dataIterator.get());
    const auto& rowType = type->asRow();
    size_t numFields = rowType.size();
    size_t numStructs = StructBatchIteratorPtr->numRows();

    auto [offsets, lengths, nulls, nullCount] =
        populateMetadataVectors(dataIterator, pool, numStructs);

    std::vector<VectorPtr> columnVectors(numFields);
    for (size_t i = 0; i < numFields; ++i) {
      columnVectors[i] = deserializeComplex(
          StructBatchIteratorPtr->nextColumnBatch(), rowType.childAt(i), pool);
    }
    size_t size = columnVectors[0]->size();
    return std::make_shared<RowVector>(
        pool, type, nulls, size, std::move(columnVectors));
  }

  /**
   * Converts a list of UnsafeRowPrimitiveBatchIterators to a FlatVector
   * @tparam Kind the element's type kind.
   * @param dataIterator iterator that points to the dataIterator over the
   * whole column batch of data.
   * @param type The element type
   * @param pool
   * @return a FlatVector
   */
  template <TypeKind Kind>
  static VectorPtr createFlatVector(
      const DataBatchIteratorPtr& dataIterator,
      const TypePtr& type,
      memory::MemoryPool* pool) {
    auto iterator = std::dynamic_pointer_cast<UnsafeRowPrimitiveBatchIterator>(
        dataIterator);
    size_t size = iterator->numRows();
    auto vector = BaseVector::create(type, size, pool);
    using Trait = ScalarTraits<Kind>;
    using InMemoryType = typename Trait::InMemoryType;

    size_t nullCount = 0;
    auto* flatResult = vector->asFlatVector<InMemoryType>();

    for (int32_t i = 0; i < size; ++i) {
      if (iterator->isNull(i)) {
        vector->setNull(i, true);
        iterator->next();
        nullCount++;
      } else {
        vector->setNull(i, false);

        if constexpr (std::is_same_v<InMemoryType, StringView>) {
          StringView val =
              UnsafeRowPrimitiveBatchDeserializer::deserializeStringView(
                  iterator->next().value());
          Trait::set(flatResult, i, val);
        } else {
          typename Trait::SerializedType val =
              UnsafeRowPrimitiveBatchDeserializer::deserializeFixedWidth<
                  typename Trait::SerializedType>(iterator->next().value());
          Trait::set(flatResult, i, val);
        }
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
      const DataBatchIteratorPtr& dataIterator,
      memory::MemoryPool* pool) {
    const TypePtr& type = dataIterator->type();
    assert(type->isPrimitiveType());

    return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        createFlatVector, type->kind(), dataIterator, type, pool);
  }

  static VectorPtr convertToVectors(
      const DataBatchIteratorPtr& dataIterator,
      memory::MemoryPool* pool) {
    const TypePtr& type = dataIterator->type();

    if (type->isPrimitiveType()) {
      return convertPrimitiveIteratorsToVectors(dataIterator, pool);
    } else if (type->isRow()) {
      return convertStructIteratorsToVectors(dataIterator, pool);
    } else if (type->isArray()) {
      return convertArrayIteratorsToVectors(dataIterator, pool);
    } else if (type->isMap()) {
      return convertMapIteratorsToVectors(dataIterator, pool);
    } else {
      VELOX_NYI("Unsupported data iterators type");
    }
  }

  /**
   * Deserializes a complex element type to its Vector representation.
   * @param data A string_view over a given element in the UnsafeRow.
   * @param type the element type.
   * @param pool the memory pool to allocate Vectors from
   *data to a array.
   * @return a VectorPtr
   */
  static VectorPtr deserializeComplex(
      std::optional<std::string_view> data,
      TypePtr type,
      memory::MemoryPool* pool) {
    std::vector<std::optional<std::string_view>> vectors{data};
    return deserializeComplex(vectors, type, pool);
  }

  /**
   * Deserializes a complex element type to its Vector representation.
   * @param data A vector of string_view over a given element in the
   *UnsafeRow.
   * @param type the element type.
   * @param pool the memory pool to allocate Vectors from
   *data to a array.
   * @return a VectorPtr
   */
  static VectorPtr deserializeComplex(
      const std::vector<std::optional<std::string_view>>& data,
      const TypePtr& type,
      memory::MemoryPool* pool) {
    return convertToVectors(getBatchIteratorPtr(data, type), pool);
  }
};

} // namespace facebook::velox::row
