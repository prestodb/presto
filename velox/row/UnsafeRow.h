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

#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::row {

template <TypeKind Kind>
struct ScalarTraits {
  using InMemoryType = typename TypeTraits<Kind>::NativeType;
  using SerializedType = InMemoryType;

  static SerializedType get(
      const FlatVector<InMemoryType>& v,
      vector_size_t i) {
    return v.valueAt(i);
  }

  static SerializedType get(const DecodedVector& v, vector_size_t i) {
    return v.valueAt<InMemoryType>(i);
  }

  static void set(VectorPtr v, vector_size_t i, SerializedType val) {
    auto flatVector = v->template asFlatVector<InMemoryType>();
    flatVector->set(i, val);
  }

  static void set(
      FlatVector<InMemoryType>* flatVector,
      vector_size_t i,
      SerializedType val) {
    flatVector->set(i, val);
  }
};

template <>
struct ScalarTraits<TypeKind::TIMESTAMP> {
  using InMemoryType = Timestamp;
  using SerializedType = int64_t;

  static int64_t get(const FlatVector<Timestamp>& v, vector_size_t i) {
    return v.valueAt(i).toMicros();
  }

  static int64_t get(const DecodedVector& v, vector_size_t i) {
    return v.valueAt<Timestamp>(i).toMicros();
  }

  static void set(VectorPtr v, vector_size_t i, SerializedType val) {
    auto flatVector = v->template asFlatVector<InMemoryType>();
    flatVector->set(i, Timestamp::fromMicros(val));
  }

  static void
  set(FlatVector<Timestamp>* flatVector, vector_size_t i, SerializedType val) {
    flatVector->set(i, Timestamp::fromMicros(val));
  }
};

// We deliberately do not specify the SerializedType to ensure it isn't used.
// We don't use the `valueAt` methods because they make copies of the
// StringView. This is required in the generic interface because it's not
// possible to return a reference to a bool.
#define SCALAR_TRAIT(VeloxKind)                                             \
  template <>                                                               \
  struct ScalarTraits<TypeKind::VeloxKind> {                                \
    using InMemoryType = StringView;                                        \
                                                                            \
    static const StringView& get(                                           \
        const FlatVector<StringView>& v,                                    \
        vector_size_t i) {                                                  \
      return v.rawValues()[i];                                              \
    }                                                                       \
                                                                            \
    static const StringView& get(const DecodedVector& v, vector_size_t i) { \
      return v.data<StringView>()[v.index(i)];                              \
    }                                                                       \
                                                                            \
    static void set(VectorPtr v, vector_size_t i, const StringView& val) {  \
      auto flatVector = v->template asFlatVector<StringView>();             \
      flatVector->set(i, val);                                              \
    }                                                                       \
                                                                            \
    static void set(                                                        \
        FlatVector<StringView>* flatVector,                                 \
        vector_size_t i,                                                    \
        const StringView& val) {                                            \
      flatVector->set(i, val);                                              \
    }                                                                       \
                                                                            \
    static void setNoCopy(                                                  \
        FlatVector<StringView>* flatVector,                                 \
        vector_size_t i,                                                    \
        const StringView& val) {                                            \
      flatVector->setNoCopy(i, val);                                        \
    }                                                                       \
  };

SCALAR_TRAIT(VARBINARY)
SCALAR_TRAIT(VARCHAR)

#undef SCALAR_TRAIT

/**
 * Supports Apache Spark UnsafeRow format. Memory management should be handled
 * by the caller, UnsafeRow does not have the notion of underlying buffer size.
 * The size function returns the number of bytes written and can be used to
 * check for buffer overflows.
 */
class UnsafeRow {
 public:
  /**
   * UnsafeRow field width in bytes.
   */
  static const size_t kFieldWidthBytes = 8;

  /**
   * Number of bits in a byte.
   */
  static const size_t kNumBitsInByte = 8;

  /**
   * UnsafeRow word size in bits.
   */
  static const size_t kWordSizeBits = kFieldWidthBytes * kNumBitsInByte;

  /**
   * UnsafeRow class constructor.
   * @param buffer pre-allocated buffer
   * @param elementCapacity number of elements in the row
   */
  UnsafeRow(char* buffer, size_t elementCapacity)
      : buffer_(buffer), elementCapacity_(elementCapacity) {
    nullSet_ = buffer;
    size_t nullLength = getNullLength(elementCapacity);
    fixedLengthData_ = nullSet_ + nullLength;
    variableLengthOffset_ = nullLength + elementCapacity_ * kFieldWidthBytes;
  }

  /**
   * UnsafeRow constructor for recursive sub-rows in the UnsafeRow, used when
   * writing or reading a complex type variable-length element.
   * @param buffer
   * @param nullSet location of the nulls
   * @param fixedLengthData location of the fixedLengthData
   * @param elementCapacity number of elements in the row
   * @param elementCount number of elements already written
   */
  UnsafeRow(
      char* buffer,
      char* nullSet,
      char* fixedLengthData,
      size_t elementCapacity,
      size_t elementCount = 0)
      : buffer_(buffer),
        nullSet_(nullSet),
        fixedLengthData_(fixedLengthData),
        elementCapacity_(elementCapacity),
        elementCount_(elementCount) {
    variableLengthOffset_ =
        fixedLengthData - buffer + elementCapacity * kFieldWidthBytes;
  }

  /**
   * @return the backing buffer.
   */
  char* buffer() const {
    return buffer_;
  }

  /**
   * @return the number of written elements.
   */
  size_t elementCount() const {
    return elementCount_;
  }

  /**
   * @return the element capacity.
   */
  size_t elementCapacity() const {
    return elementCapacity_;
  }

  /**
   * Increment the element count by count.  The row format is write-once
   * only so we do not need to support decrement.
   * @param count
   */
  void incrementElementCount(size_t count) {
    elementCount_ += count;
  }

  /**
   * @return pointer to the null set.
   */
  char* nullSet() const {
    return nullSet_;
  }

  /**
   * @return the size of the row in bytes
   */
  size_t size() const {
    return variableLengthOffset_;
  }

  /**
   * @return the size of the metadata, this is usually equal to the size of the
   * null set.
   */
  size_t metadataSize() const {
    return nullSet_ - buffer();
  }

  /**
   * @return the location where fixed length data is written.
   */
  char* fixedLengthDataLocation() const {
    return fixedLengthData_;
  }

  /**
   * Set element at the given position to null.
   * @param pos
   */
  void setNullAt(size_t pos) {
    bits::setBit(nullSet_, pos);
  }

  /**
   * Set element at the give position to not null.
   * @param pos
   */
  void setNotNullAt(size_t pos) {
    bits::clearBit(nullSet_, pos);
  }

  /**
   * Set all not null, used when a new unsafe is initialized.
   * @param pos
   */
  void setAllNotNull(size_t pos) {
    size_t nullLength = getNullLength(elementCapacity_);
    // clear the buffer, assume all elements are valid
    memset(nullSet_, 0, nullLength);
  }

  /**
   * @param pos
   * @return true if the element is null, false otherwise
   */
  bool isNullAt(size_t pos) const {
    return bits::isBitSet(nullSet_, pos);
  }

  /**
   * Reads the data at a given index.
   * @param pos
   * @param type the element type
   * @return a string_view over the data
   */
  const std::string_view readDataAt(size_t pos, const TypePtr& type) const {
    size_t cppSizeInBytes = type->isFixedWidth() ? type->cppSizeInBytes() : 0;
    return readDataAt(pos, type->isFixedWidth(), cppSizeInBytes);
  }

  /**
   * Reads the data at a given index.
   * @param pos
   * @param isFixedWidth whether the element is fixed width
   * @param fixedDataWidth 0 for variable-length data, native type width for
   * fixedLength data
   * @return a string_view of the data
   */
  const std::string_view
  readDataAt(size_t pos, bool isFixedWidth, size_t fixedDataWidth = 0) const {
    if (isNullAt(pos)) {
      return std::string_view();
    } else if (isFixedWidth) {
      return readFixedLengthDataAt(pos, fixedDataWidth);
    }
    return readVariableLengthDataAt(pos);
  }

  /**
   * Writes a string_view of serialized data at the given index. If the element
   * is fixed length, we write it at the fixed length data field. If the
   * element is variable length, we append the serialized string_view to the
   * unsafe row and write an offset pointer at the fixed length data field.
   * @param pos
   * @param type
   * @param data string_view over the serialized element
   */
  void writeSerializedDataAt(
      size_t pos,
      const TypePtr& type,
      const std::string_view& data) {
    writeOffsetAndNullAt(pos, data.size(), type->isFixedWidth());
    if (type->isFixedWidth()) {
      return writeFixedLengthDataAt(pos, data);
    }
    return appendVariableLengthData(data);
  }

  /**
   * Writes primitive data to the fixed length data field, bypasses the need to
   * serialize.
   * @tparam DataType a fundamental data type
   * @param pos
   * @param data
   */
  template <typename DataType>
  void writePrimitiveAt(size_t pos, const DataType data) {
    reinterpret_cast<uint64_t*>(fixedLengthData_)[pos] = data;
  }

  /**
   * Writes a Timestamp as seconds, bypasses the need to serialize.
   * @param pos
   * @param data
   */
  void writeTimestampSecondsAt(size_t pos, const Timestamp& data) {
    reinterpret_cast<uint64_t*>(fixedLengthData_)[pos] = data.getSeconds();
  }

  /**
   * Writes a Timestamp as nanos, bypasses the need to serialize.
   * @param pos
   * @param data
   */
  void writeTimestampNanosAt(size_t pos, const Timestamp& data) {
    reinterpret_cast<uint64_t*>(fixedLengthData_)[pos] = data.getNanos();
  }

  /**
   * @param pos
   * @param isFixedWidth
   * @return the location of the fixed size data if isFixedWidth is true,
   * return the first unwritten word for variable length data otherwise.
   */
  char* getSerializationLocation(size_t pos, bool isFixedWidth) {
    setVariableLengthOffset(alignToFieldWidth(variableLengthOffset_));
    return isFixedWidth ? reinterpret_cast<char*>(&reinterpret_cast<uint64_t*>(
                              fixedLengthData_)[pos])
                        : buffer_ + variableLengthOffset_;
  }

  /**
   * Write a variable length data offset pointer at the given index.
   * @param pos
   * @param size
   */
  void writeOffsetPointer(size_t pos, size_t size) {
    VELOX_CHECK_LE(variableLengthOffset_, UINT32_MAX);
    VELOX_CHECK_LE(size, UINT32_MAX);
    uint64_t dataPointer = variableLengthOffset_ << 32 | size;

    // write the data pointer
    reinterpret_cast<uint64_t*>(fixedLengthData_)[pos] = dataPointer;
    setVariableLengthOffset(variableLengthOffset_ + size);
  }

  /**
   * @param size
   * @return size aligned to field width.
   */
  static size_t alignToFieldWidth(size_t size) {
    return bits::roundUp(size, kFieldWidthBytes);
  }

  /**
   * Returns the length of the null set in bytes.
   * @param elementCount
   * @return the null set length in bytes
   */
  static const size_t getNullLength(size_t elementCount) {
    return bits::nwords(elementCount) * kFieldWidthBytes;
  }

  /**
   * If the element is variable length, write the offset and size. For all
   * element types, set null if the element is null.
   * @param pos
   * @param dataSize the serialized data size
   * @param isFixedWidth
   */
  void writeOffsetAndNullAt(
      size_t pos,
      std::optional<size_t> dataSize,
      bool isFixedWidth) {
    if (!dataSize.has_value()) {
      setNullAt(pos);
    } else {
      setNotNullAt(pos);
      if (!isFixedWidth) {
        writeOffsetPointer(pos, dataSize.value());
      }
    }

    incrementElementCount(1);
  }

 private:
  /*
   * Pre-allocated memory for the row.
   */
  char* buffer_;

  /**
   * Pointer to the start of the null indicators.
   */
  char* nullSet_;

  /**
   * Pointer to the start of fixed length data.
   */
  char* fixedLengthData_;

  /**
   * Offset to the start of unwritten variable length data region.
   */
  size_t variableLengthOffset_;

  /**
   * Capacity for the number of columns in the UnsafeRow.
   */
  size_t elementCapacity_;

  /**
   * Number of elements written to the Row.
   */
  size_t elementCount_ = 0;

  /**ata
   * Set variableLengthOffset_ to the given offset. Make sure the new value is
   * not smaller than the previous value, otherwise we might accidentally
   * overwrite data.
   * @param offset
   */
  void setVariableLengthOffset(size_t offset) {
    VELOX_CHECK_GE(offset, variableLengthOffset_);
    variableLengthOffset_ = offset;
  }

  /**
   * Given the elementSize, return the amount of padding (in bytes) required
   * so that the element aligns to field width.
   * @param elementSize
   * @return size_t
   */
  static const size_t getPaddingLength(size_t elementSize) {
    return alignToFieldWidth(elementSize) - elementSize;
  }

  /**
   * Reads the data field as a fixed length data.
   * @param pos
   * @param width The element width in bytes
   * @return a string_view of length width
   */
  const std::string_view readFixedLengthDataAt(size_t pos, size_t width) const {
    VELOX_CHECK_LE(width, 8);
    uint64_t* dataPointer = &reinterpret_cast<uint64_t*>(fixedLengthData_)[pos];
    return std::string_view(reinterpret_cast<char*>(dataPointer), width);
  }

  /**
   * Writes the data field as a fixed length data, vector should have exactly
   * one element
   * @param pos
   * @param data
   * @param width The element width in bytes
   */
  void writeFixedLengthDataAt(size_t pos, const std::string_view& data) {
    VELOX_CHECK_EQ(data.size(), 1);
    VELOX_CHECK_LE(data.size(), 8);
    std::memcpy(
        &reinterpret_cast<uint64_t*>(fixedLengthData_)[pos],
        data.begin(),
        data.size());
  }

  /**
   * Reads the data field as a variable length data type.
   * @param pos
   * @return a string_view over the variable length data.
   */
  const std::string_view readVariableLengthDataAt(size_t pos) const {
    // At the data field, the lower 4 bytes is size, upper 4 bytes is offset
    uint64_t* dataPointer = &reinterpret_cast<uint64_t*>(fixedLengthData_)[pos];
    uint32_t size = reinterpret_cast<uint32_t*>(dataPointer)[0];
    uint32_t offset = reinterpret_cast<uint32_t*>(dataPointer)[1];

    return std::string_view(buffer_ + offset, size);
  }

  /**
   * Write the data field as a continuous variable length data type at the end
   * of the row.
   * @param pos
   * @param data a string_view over the variable length data.
   */
  void appendVariableLengthData(const std::string_view& data) {
    std::memcpy(
        fixedLengthData_ + variableLengthOffset_, data.begin(), data.size());
  }
};

} // namespace facebook::velox::row
