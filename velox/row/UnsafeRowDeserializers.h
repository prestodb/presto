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
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::row {

namespace {
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
    const uint64_t offsetAndSize = readInt64(data);
    return {
        /*size*/ static_cast<uint32_t>(offsetAndSize),
        /*offset*/ offsetAndSize >> 32};
  }

  /**
   * @param idx
   * @return true if element is null.
   */
  virtual bool isNull(size_t idx) const {
    return !data_[idx].has_value();
  }

  /**
   * @param idx
   * @return A primitive value occupies 1 field, so return 0 if the element is
   * null, 1 otherwise.
   */
  virtual size_t size(size_t idx) const {
    return isNull(idx) ? 0 : 1;
  }

  /**
   * @return  return total number of rows.
   */
  size_t numRows() const {
    return numRows_;
  }

  /**
   * @return the element type.
   */
  const TypePtr& type() const {
    return type_;
  }

  virtual std::string toString(size_t idx) const {
    std::stringstream str;
    str << "Data iterator of type " << type()->toString() << " of size "
        << size(idx);
    return str.str();
  }

 protected:
  static uint64_t readInt64(const char* data) {
    return *reinterpret_cast<const uint64_t*>(data);
  }

  static bool isFixedWidth(const TypePtr& type) {
    return type->isFixedWidth() && !type->isLongDecimal();
  }

  /**
   * The data need to be processed. The vector represent a whole column of
   * data over.
   */
  const std::vector<std::optional<std::string_view>>& data_;

  /**
   * The number of rows in the given data.
   */
  const size_t numRows_;

 private:
  /**
   * The element type.
   */
  const TypePtr type_;
};

inline UnsafeRowDataBatchIterator::~UnsafeRowDataBatchIterator() {}

using DataBatchIteratorPtr = std::shared_ptr<UnsafeRowDataBatchIterator>;

/**
 * Iterator that represents a primitive object. Primitive includes strings
 * and anything that can be represented as a c++ fundamental type with at most
 * 8 bytes.
 */
struct PrimitiveBatchIterator : UnsafeRowDataBatchIterator {
 public:
  /**
   * PrimitiveBatchIterator constructor.
   * @param data The part of the UnsafeRow data buffer that contains the
   * primitive value
   * @param type The element type
   */
  PrimitiveBatchIterator(
      const std::vector<std::optional<std::string_view>>& data,
      const TypePtr& type)
      : UnsafeRowDataBatchIterator(data, type) {}

  /**
   * @return a string_view over the part of the UnsafeRow data buffer that
   * contains the primitive value.
   */
  std::optional<std::string_view> data(size_t idx) const {
    return data_[idx];
  }

  bool hasNext() const {
    return currentRow_ < numRows_;
  }

  const std::optional<std::string_view>& next() {
    VELOX_CHECK(hasNext());
    return data_[currentRow_++];
  }

  std::string toString(size_t idx) const override {
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
struct StructBatchIterator : UnsafeRowDataBatchIterator {
 public:
  /**
   * StructBatchIterator constructor.
   * @param data
   * @param type
   */
  explicit StructBatchIterator(
      const std::vector<std::optional<std::string_view>>& data,
      const TypePtr& type)
      : UnsafeRowDataBatchIterator(data, type),
        childTypes_(asRowType(type)->children()),
        numElements_(childTypes_.size()) {
    columnData_.resize(numRows_);
  }

  /**
   * @return return whether there's a next element.
   */
  bool hasNext() const {
    return idx_ < numElements_;
  }

  std::string toString(size_t idx) const override {
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
    const TypePtr& type = childTypes_[idx_];
    std::size_t fixedSize =
        isFixedWidth(type) ? serializedSizeInBytes(type) : 0;
    std::size_t fieldOffset = UnsafeRow::getNullLength(numElements_) +
        idx_ * UnsafeRow::kFieldWidthBytes;

    for (int32_t i = 0; i < numRows_; ++i) {
      // if null
      if (!data_[i] || bits::isBitSet(data_[i].value().data(), idx_)) {
        columnData_[i] = std::nullopt;
        continue;
      }

      const char* rawData = data_[i]->data();
      const char* fieldData = rawData + fieldOffset;

      // Fixed length field
      if (fixedSize > 0) {
        columnData_[i] = std::string_view(fieldData, fixedSize);
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
  size_t size(size_t idx) const override {
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
  const std::vector<TypePtr>& childTypes_;

  /**
   * The number of elements in the struct.
   */
  const size_t numElements_;

  /**
   * The current processing column data.
   */
  std::vector<std::optional<std::string_view>> columnData_;
};

using StructBatchIteratorPtr = std::shared_ptr<StructBatchIterator>;

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
struct ArrayBatchIterator : UnsafeRowDataBatchIterator {
 public:
  /**
   * Constructor for UnsafeRowArray.
   * @param data A batch of unsafe row array representation data.
   */
  ArrayBatchIterator(
      const std::vector<std::optional<std::string_view>>& data,
      const TypePtr& type)
      : UnsafeRowDataBatchIterator(data, type),
        elementType_{type->childAt(0)},
        isFixedLength_(isFixedWidth(elementType_)),
        fixedDataWidth_(
            isFixedLength_ ? serializedSizeInBytes(elementType_) : 0) {
    totalNumElements_ = 0L;

    for (int32_t i = 0; i < numRows_; ++i) {
      if (data_[i].has_value()) {
        totalNumElements_ += readInt64(data_[i]->data());
      }
    }
    columnData_.resize(totalNumElements_);
  }

  int64_t totalNumElements() const {
    return totalNumElements_;
  }

  const TypePtr& getElementType() const {
    return elementType_;
  }

  /**
   * @return the size of the array
   */
  size_t size(size_t idx) const override {
    return isNull(idx) ? 0 : readInt64(data_[idx]->data());
  }

  std::string toString(size_t idx) const override {
    std::stringstream str;
    str << "Data iterator of type " << type()->toString() << " of size "
        << size(idx) << " childIsFixedLength " << isFixedLength_;
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
      size_t numElement = readInt64(rawData);
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
   * The element type.
   */
  const TypePtr elementType_;

  /**
   * Whether the elements are fixed length.
   */
  const bool isFixedLength_;

  /**
   * The width of the elements if it is fixed length
   */
  const size_t fixedDataWidth_;

  /**
   * the children elements of all array data for the whole column.
   */
  std::vector<std::optional<std::string_view>> columnData_;

  /**
   * The total number of elements in all the array the current iterator
   * represents.
   */
  int64_t totalNumElements_;
};

using ArrayBatchIteratorPtr = std::shared_ptr<ArrayBatchIterator>;

DataBatchIteratorPtr getBatchIteratorPtr(
    const std::vector<std::optional<std::string_view>>& data,
    const TypePtr& type);

/**
 * Iterator representation of an UnsafeRowMap object.
 */
struct MapBatchIterator : UnsafeRowDataBatchIterator {
 public:
  /**
   * UnsafeRowMapIterator constructor. The elements in an UnsafeRow Map
   * appears as an UnsafeRow array.
   * @param data
   * @param type
   */
  explicit MapBatchIterator(
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
      size_t offsetToValues = readInt64(data_[i]->data());

      auto keysStart = data_[i]->data() + UnsafeRow::kFieldWidthBytes;
      keyArray_[i] = std::string_view(keysStart, offsetToValues);
      valueArray_[i] = std::string_view(
          keysStart + offsetToValues,
          data_[i]->size() - offsetToValues - UnsafeRow::kFieldWidthBytes);
    }

    keysIteratorWrapper_ = std::dynamic_pointer_cast<ArrayBatchIterator>(
        getBatchIteratorPtr(keyArray_, ARRAY(type->childAt(0))));
    valuesIteratorWrapper_ = std::dynamic_pointer_cast<ArrayBatchIterator>(
        getBatchIteratorPtr(valueArray_, ARRAY(type->childAt(1))));

    numElements_.resize(numRows_);
    for (int32_t i = 0; i < numRows_; ++i) {
      numElements_[i] = keysIteratorWrapper_->size(i);
    }
  }

  /**
   * @return the keysIteratorWrapper.
   */
  const ArrayBatchIteratorPtr& keysIteratorWrapper() const {
    return keysIteratorWrapper_;
  }

  /**
   * @return the valuesIteratorWrapper.
   */
  const ArrayBatchIteratorPtr& valuesIteratorWrapper() const {
    return valuesIteratorWrapper_;
  }

  /**
   * @return the number of key-value pairs in the map.
   */
  size_t size(size_t idx) const override {
    return numElements_[idx];
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
};

inline DataBatchIteratorPtr getBatchIteratorPtr(
    const std::vector<std::optional<std::string_view>>& data,
    const TypePtr& type) {
  if (type->isPrimitiveType()) {
    return std::make_shared<PrimitiveBatchIterator>(data, type);
  } else if (type->isRow()) {
    return std::make_shared<StructBatchIterator>(data, type);
  } else if (type->isArray()) {
    return std::make_shared<ArrayBatchIterator>(data, type);
  } else if (type->isMap()) {
    return std::make_shared<MapBatchIterator>(data, type);
  }

  VELOX_NYI("Unknown data type " + type->toString());
  return nullptr;
}
} // namespace

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
    return reinterpret_cast<const T*>(data.data())[0];
  }

  /**
   * @param data
   * @return the value in velox::StringView
   */
  static StringView deserializeStringView(std::string_view data) {
    return StringView(data.data(), data.size());
  }

  /// Origins from java side BigInteger(byte[] var).
  /// The data is assumed to be in <i>big-endian</i> byte-order: the most
  /// significant byte is the element at index 0. The data is assumed to be
  /// unchanged.
  static int128_t deserializeLongDecimal(std::string_view data) {
    const uint8_t* bytesValue = reinterpret_cast<const uint8_t*>(data.data());
    int32_t length = data.size();
    int128_t bigEndianValue = static_cast<int8_t>(bytesValue[0]) >= 0 ? 0 : -1;
    memcpy(
        reinterpret_cast<char*>(&bigEndianValue) + sizeof(int128_t) - length,
        bytesValue,
        length);
    return bits::builtin_bswap128(bigEndianValue);
  }
};

/**
 * UnsafeRow dynamic deserializer using TypePtr, deserializes an UnsafeRow to
 * a Vector.
 */
struct UnsafeRowDeserializer {
  /**
   * Allocate and populate the metadata Vectors in ArrayVector or MapVector.
   * @param dataIterator iterator that points to whole column batch of data.
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
    BufferPtr offsets = allocateOffsets(size, pool);
    BufferPtr lengths = allocateSizes(size, pool);
    BufferPtr nulls = allocateNulls(size, pool);
    auto* offsetsPtr = offsets->asMutable<int32_t>();
    auto* lengthsPtr = lengths->asMutable<vector_size_t>();
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    for (int i = 0; i < size; i++) {
      auto isNull = dataIterator->isNull(i);
      bits::setNull(nullsPtr, i, isNull);
      nullCount += isNull;
      lengthsPtr[i] = isNull ? 0 : dataIterator->size(i);
      offsetsPtr[i] = i == 0 ? 0 : offsetsPtr[i - 1] + lengthsPtr[i - 1];
    }

    if (nullCount == 0) {
      nulls = nullptr;
    }

    return std::tuple(offsets, lengths, nulls, nullCount);
  }

  inline static BufferPtr populateNulls(
      const DataBatchIteratorPtr& dataIterator,
      memory::MemoryPool* pool,
      const size_t size) {
    BufferPtr nulls = allocateNulls(size, pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    for (int i = 0; i < size; i++) {
      auto isNull = dataIterator->isNull(i);
      bits::setNull(nullsPtr, i, isNull);
      nullCount += isNull;
    }

    if (nullCount == 0) {
      return nullptr;
    }

    return nulls;
  }

  /**
   * Converts a list of MapBatchIterators to Vectors.
   * @param dataIterator iterator that points to whole column batch of data.
   * @param pool
   * @return a MapVectorPtr
   */
  static VectorPtr convertMapIteratorsToVectors(
      const DataBatchIteratorPtr& dataIterator,
      memory::MemoryPool* pool) {
    const TypePtr& type = dataIterator->type();
    assert(type->isMap());

    auto* iteratorPtr = static_cast<MapBatchIterator*>(dataIterator.get());
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
        deserialize(keysIterator->nextColumnBatch(), type->childAt(0), pool),
        deserialize(valuesIterator->nextColumnBatch(), type->childAt(1), pool),
        nullCount);
  }

  /**
   * Converts a list of ArrayBatchIterators to Vectors.
   * @param dataIterator iterator that points to whole column batch of data.
   * process.
   * @param pool
   * @return an ArrayVectorPtr
   */
  static VectorPtr convertArrayIteratorsToVectors(
      const DataBatchIteratorPtr& dataIterator,
      memory::MemoryPool* pool) {
    const TypePtr& type = dataIterator->type();
    assert(type->isArray());

    auto* iteratorPtr = static_cast<ArrayBatchIterator*>(dataIterator.get());

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
        deserialize(iteratorPtr->nextColumnBatch(), elementType, pool),
        nullCount);
  }

  /**
   * Converts a list of StructBatchIterators to Vectors.
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
        static_cast<StructBatchIterator*>(dataIterator.get());
    const auto& rowType = type->asRow();
    size_t numFields = rowType.size();
    size_t numStructs = StructBatchIteratorPtr->numRows();

    auto nulls = populateNulls(dataIterator, pool, numStructs);

    std::vector<VectorPtr> columnVectors(numFields);
    for (size_t i = 0; i < numFields; ++i) {
      columnVectors[i] = deserialize(
          StructBatchIteratorPtr->nextColumnBatch(), rowType.childAt(i), pool);
    }

    return std::make_shared<RowVector>(
        pool, type, nulls, numStructs, std::move(columnVectors));
  }

  /**
   * Converts a list of PrimitiveBatchIterators to a FlatVector
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
    auto iterator =
        std::dynamic_pointer_cast<PrimitiveBatchIterator>(dataIterator);
    size_t size = iterator->numRows();
    auto vector = BaseVector::create(type, size, pool);
    using T = typename TypeTraits<Kind>::NativeType;
    using TypeTraits = ScalarTraits<Kind>;

    auto* flatResult = vector->asFlatVector<T>();

    for (int32_t i = 0; i < size; ++i) {
      if (iterator->isNull(i)) {
        vector->setNull(i, true);
        iterator->next();
      } else {
        vector->setNull(i, false);

        if constexpr (std::is_same_v<T, StringView>) {
          StringView val =
              UnsafeRowPrimitiveBatchDeserializer::deserializeStringView(
                  iterator->next().value());
          TypeTraits::set(flatResult, i, val);
        } else if constexpr (std::is_same_v<T, int128_t>) {
          int128_t val =
              UnsafeRowPrimitiveBatchDeserializer::deserializeLongDecimal(
                  iterator->next().value());
          TypeTraits::set(flatResult, i, val);
        } else {
          typename TypeTraits::SerializedType val =
              UnsafeRowPrimitiveBatchDeserializer::deserializeFixedWidth<
                  typename TypeTraits::SerializedType>(
                  iterator->next().value());
          TypeTraits::set(flatResult, i, val);
        }
      }
    }
    return vector;
  }

  static VectorPtr createFlatUnknownVector(
      const DataBatchIteratorPtr& dataIterator,
      memory::MemoryPool* pool) {
    auto iterator =
        std::dynamic_pointer_cast<PrimitiveBatchIterator>(dataIterator);
    size_t size = iterator->numRows();

    for (int32_t i = 0; i < size; ++i) {
      VELOX_CHECK(
          iterator->isNull(i), "UNKNOWN type supports only NULL values");
    }

    auto nulls = allocateNulls(size, pool, bits::kNull);
    return std::make_shared<FlatVector<UnknownValue>>(
        pool,
        UNKNOWN(),
        nulls,
        size,
        nullptr, // values
        std::vector<BufferPtr>{}); // stringBuffers
  }

  /**
   * Calls createFlatVector with the correct template argument.
   * @param dataIterator iterator that points to the first dataIterator to
   * process.
   * @param pool
   * @return A FlatVector
   */
  static VectorPtr convertPrimitiveIteratorsToVectors(
      const DataBatchIteratorPtr& dataIterator,
      memory::MemoryPool* pool) {
    const TypePtr& type = dataIterator->type();
    assert(type->isPrimitiveType());

    if (type->isUnKnown()) {
      return createFlatUnknownVector(dataIterator, pool);
    }

    return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
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
  static VectorPtr deserializeOne(
      std::optional<std::string_view> data,
      const TypePtr& type,
      memory::MemoryPool* pool) {
    std::vector<std::optional<std::string_view>> vectors{data};
    return deserialize(vectors, type, pool);
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
  static VectorPtr deserialize(
      const std::vector<std::optional<std::string_view>>& data,
      const TypePtr& type,
      memory::MemoryPool* pool) {
    return convertToVectors(getBatchIteratorPtr(data, type), pool);
  }
};

} // namespace facebook::velox::row
