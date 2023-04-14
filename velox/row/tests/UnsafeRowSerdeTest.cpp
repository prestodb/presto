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

#include <optional>

#include "velox/common/base/Nulls.h"
#include "velox/row/UnsafeRowDeserializers.h"
#include "velox/row/UnsafeRowSerializers.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace facebook::velox::row {

namespace {

class UnsafeRowSerializerTests : public testing::Test,
                                 public test::VectorTestBase {
 protected:
  UnsafeRowSerializerTests() {
    clearBuffer();
  }

  void clearBuffer() {
    std::memset(buffer_, 0, kBufferSize);
  }

  static size_t getSize(const VectorPtr& vector, vector_size_t index) {
    return UnsafeRowSerializer::getSize(vector->type(), vector, index);
  }

  std::optional<size_t> serialize(
      const VectorPtr& vector,
      vector_size_t index) {
    return UnsafeRowSerializer::serialize(vector, buffer_, index);
  }

  template <typename T>
  std::shared_ptr<LazyVector> lazyFlatVector(
      vector_size_t size,
      std::function<T(vector_size_t /*row*/)> valueAt,
      std::function<bool(vector_size_t /*row*/)> isNullAt = nullptr) {
    return vectorMaker_.lazyFlatVector(size, valueAt, isNullAt);
  }

  template <typename T>
  testing::AssertionResult checkFixedLength(
      std::optional<size_t> serializedSize,
      size_t expectedSize,
      const T* expectedValue) {
    if (serializedSize != expectedSize) {
      return testing::AssertionFailure()
          << "Expected serializedSize " << expectedSize << " but got "
          << serializedSize.value();
    }
    if (std::memcmp(buffer_, expectedValue, sizeof(T)) != 0) {
      return testing::AssertionFailure()
          << "Buffer is " << reinterpret_cast<T*>(buffer_)[0]
          << " but expected " << expectedValue[0];
    }
    return testing::AssertionSuccess();
  }

  template <typename T>
  testing::AssertionResult checkVariableLength(
      std::optional<size_t> serializedSize,
      size_t expectedSize,
      T* expectedValue) {
    if (serializedSize != expectedSize) {
      return testing::AssertionFailure()
          << "Expected serializedSize " << expectedSize << " but got "
          << serializedSize.value();
    }

    for (int i = 0; i < expectedSize; i++) {
      if (std::memcmp(
              buffer_ + i,
              reinterpret_cast<const uint8_t*>(expectedValue) + i,
              1) != 0) {
        return testing::AssertionFailure()
            << "Buffer and expectedValue differ at index " << i << " "
            << "got " << int(buffer_[i]) << " vs "
            << "expected "
            << int(reinterpret_cast<const uint8_t*>(expectedValue)[i]);
      }
    }
    return testing::AssertionSuccess();
  }

  ArrayVectorPtr makeArrayVectorPtr(
      size_t arrayVectorSize,
      memory::MemoryPool* pool,
      int32_t* offsetsValue,
      vector_size_t* lengthsValue,
      bool* nullsValue,
      const TypePtr type,
      VectorPtr elements) {
    BufferPtr offsets = AlignedBuffer::allocate<int32_t>(arrayVectorSize, pool);
    auto* offsetsPtr = offsets->asMutable<int32_t>();
    BufferPtr lengths =
        AlignedBuffer::allocate<vector_size_t>(arrayVectorSize, pool);
    auto* lengthsPtr = lengths->asMutable<vector_size_t>();
    BufferPtr nulls =
        AlignedBuffer::allocate<char>(bits::nbytes(arrayVectorSize), pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();

    size_t nullCount = 0;
    for (size_t i = 0; i < arrayVectorSize; i++) {
      offsetsPtr[i] = offsetsValue[i];
      lengthsPtr[i] = lengthsValue[i];
      bits::setNull(nullsPtr, i, nullsValue[i]);
      if (nullsValue[i]) {
        nullCount++;
      }
    }

    return std::make_shared<ArrayVector>(
        pool,
        type,
        nulls,
        arrayVectorSize,
        offsets,
        lengths,
        elements,
        nullCount);
  }

  MapVectorPtr makeMapVectorPtr(
      size_t mapVectorSize,
      memory::MemoryPool* pool,
      int32_t* offsetsValue,
      vector_size_t* lengthsValue,
      bool* nullsValue,
      const TypePtr type,
      VectorPtr keys,
      VectorPtr values) {
    BufferPtr offsets = AlignedBuffer::allocate<int32_t>(mapVectorSize, pool);
    auto* offsetsPtr = offsets->asMutable<int32_t>();
    BufferPtr lengths =
        AlignedBuffer::allocate<vector_size_t>(mapVectorSize, pool);
    auto* lengthsPtr = lengths->asMutable<vector_size_t>();
    BufferPtr nulls =
        AlignedBuffer::allocate<char>(bits::nbytes(mapVectorSize), pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();

    size_t nullCount = 0;
    for (size_t i = 0; i < mapVectorSize; i++) {
      offsetsPtr[i] = offsetsValue[i];
      lengthsPtr[i] = lengthsValue[i];
      bits::setNull(nullsPtr, i, nullsValue[i]);
      if (nullsValue[i]) {
        nullCount++;
      }
    }

    return std::make_shared<MapVector>(
        pool,
        type,
        nulls,
        mapVectorSize,
        offsets,
        lengths,
        keys,
        values,
        nullCount);
  }

  constexpr static size_t kBufferSize{1024};

  BufferPtr bufferPtr_ =
      AlignedBuffer::allocate<char>(kBufferSize, pool(), true);
  // Variable pointing to the row pointer held by the smart pointer BufferPtr.
  char* buffer_ = bufferPtr_->asMutable<char>();
};

TEST_F(UnsafeRowSerializerTests, fixedLengthPrimitive) {
  int16_t smallint = 0x1234;
  auto smallintSerialized =
      UnsafeRowSerializer::serialize<SmallintType>(smallint, buffer_);
  ASSERT_TRUE(checkFixedLength(smallintSerialized, 0, &smallint));

  float real = 3.4;
  auto realSerialized = UnsafeRowSerializer::serialize<RealType>(real, buffer_);
  EXPECT_TRUE(checkFixedLength(realSerialized, 0, &real));

  bool boolean = true;
  auto boolSerialized =
      UnsafeRowSerializer::serialize<BooleanType>(boolean, buffer_);
  EXPECT_TRUE(checkFixedLength(boolSerialized, 0, &boolean));
}

TEST_F(UnsafeRowSerializerTests, fixedLengthVectorPtr) {
  auto intVector = makeFlatVector<int32_t>(
      {0x01010101, 0x01010101, 0x01010101, 0x01234567, 0x01010101});

  auto intSerialized0 =
      UnsafeRowSerializer::serialize<IntegerType>(intVector, buffer_, 0);
  int intVal0 = 0x01010101;
  EXPECT_TRUE(checkFixedLength(intSerialized0, 0, &intVal0));

  auto intSerialized1 =
      UnsafeRowSerializer::serialize<IntegerType>(intVector, buffer_, 3);
  int intVal1 = 0x01234567;
  EXPECT_TRUE(checkFixedLength(intSerialized1, 0, &intVal1));

  // Test set null.
  intVector->setNull(2, true);
  auto nullSerialized =
      UnsafeRowSerializer::serialize<IntegerType>(intVector, buffer_, 2);
  EXPECT_FALSE(nullSerialized.has_value());
}

TEST_F(UnsafeRowSerializerTests, stringsDynamic) {
  VectorPtr stringVec = makeNullableFlatVector<StringView>(
      {StringView("Hello, World!", 13),
       StringView("", 0),
       std::nullopt,
       StringView("INLINE", 6)});

  auto row = makeRowVector({stringVec});

  for (auto i = 0; i < row->size(); ++i) {
    auto serialized =
        UnsafeRowSerializer::serialize(row, buffer_, i).value_or(0);
    auto size = UnsafeRowSerializer::getSizeRow(row.get(), i);
    ASSERT_EQ(serialized, size);
  }

  auto serialized0 =
      UnsafeRowSerializer::serialize<VarcharType>(stringVec, buffer_, 0);
  EXPECT_TRUE(checkVariableLength(serialized0, 13, u8"Hello, World!"));

  auto size = getSize(stringVec, 0);
  EXPECT_EQ(size, serialized0.value_or(0));

  auto serialized1 =
      UnsafeRowSerializer::serialize<VarcharType>(stringVec, buffer_, 1);
  EXPECT_TRUE(checkVariableLength(serialized1, 0, u8""));

  size = getSize(stringVec, 1);
  EXPECT_EQ(size, serialized1.value_or(0));

  auto serialized2 =
      UnsafeRowSerializer::serialize<VarcharType>(stringVec, buffer_, 2);
  EXPECT_FALSE(serialized2.has_value());

  size = getSize(stringVec, 2);
  EXPECT_EQ(size, serialized2.value_or(0));

  // velox::StringView inlines string prefix, check that we can handle inlining.
  auto serialized3 =
      UnsafeRowSerializer::serialize<VarcharType>(stringVec, buffer_, 3);
  EXPECT_TRUE(checkVariableLength(serialized3, 6, u8"INLINE"));

  size = getSize(stringVec, 3);
  EXPECT_EQ(size, serialized3.value_or(0));
}

TEST_F(UnsafeRowSerializerTests, timestamp) {
  auto timestampVec =
      makeNullableFlatVector<Timestamp>({Timestamp(1, 2'000), std::nullopt});

  auto serialized0 =
      UnsafeRowSerializer::serialize<TimestampType>(timestampVec, buffer_, 0);
  int64_t expected0 = 1'000'000 + 2; // 1s + 2000ns in micros.
  EXPECT_TRUE(checkFixedLength(serialized0, 0, &expected0));

  auto serialized1 =
      UnsafeRowSerializer::serialize<TimestampType>(timestampVec, buffer_, 1);
  EXPECT_FALSE(serialized1.has_value());

  auto timestamp = Timestamp(-1, 2'000);
  auto serialized3 =
      UnsafeRowSerializer::serialize<TimestampType>(timestamp, buffer_);
  int64_t expected3 = -1'000'000L + 2;
  EXPECT_TRUE(checkFixedLength(serialized3, 0, &expected3));
}

TEST_F(UnsafeRowSerializerTests, arrayStdContainers) {
  // [0x1666, 0x0777, null, 0x0999]
  std::array<std::optional<int16_t>, 4> array = {
      0x1666, 0x0777, std::nullopt, 0x0999};
  auto optionalArray = std::optional(array);
  auto serialized = UnsafeRowSerializer::serialize<Array<SmallintType>>(
      optionalArray, buffer_);

  uint8_t expected[3][8] = {
      {0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x66, 0x16, 0x77, 0x07, 0x00, 0x00, 0x99, 0x09}};
  EXPECT_TRUE(checkVariableLength(serialized, 3 * 8, *expected));
  // The third element (idx 2) is null.
  ASSERT_TRUE(bits::isBitSet(buffer_ + 8, 2));
  clearBuffer();

  //   [ [5, 6, 7], null, [8] ]
  uint8_t expectedNested[11][8] = {
      {0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x18, 0x00, 0x00, 0x00, 0x28, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x18, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00},
      {0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x05, 0x06, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

  std::vector<std::optional<int8_t>> element0 = {0x5, 0x6, 0x7};
  std::vector<std::optional<int8_t>> element2 = {0x8};
  auto optionalElement0 = std::optional(element0);
  auto optionalElement1 = std::optional(element2);
  std::vector<decltype(optionalElement0)> nestedArray = {
      element0, std::nullopt, element2};

  auto serializedNested =
      UnsafeRowSerializer::serialize<Array<Array<TinyintType>>>(
          nestedArray, buffer_);
  EXPECT_TRUE(checkVariableLength(serializedNested, 11 * 8, *expectedNested));
  clearBuffer();
}

TEST_F(UnsafeRowSerializerTests, mapStdContainers) {
  /// {1 : { 2 : 3, 4: null},
  ///     6: {7: 8}}
  using SmallintSmallintMap =
      std::multimap<std::optional<int16_t>, std::optional<int16_t>>;
  SmallintSmallintMap innermap0 = {{2, 3}, {4, std::nullopt}};
  SmallintSmallintMap innermap1 = {{7, 8}};

  std::multimap<std::optional<int16_t>, std::optional<SmallintSmallintMap>>
      outermap = {{1, innermap0}, {6, innermap1}};
}

TEST_F(UnsafeRowSerializerTests, arrayPrimitives) {
  auto arrayVector = makeNullableArrayVector<int16_t>({
      std::nullopt,
      {{0x0333, 0x1444, 0x0555}},
      {{0x1666, 0x0777, std::nullopt, 0x0999}},
  });

  // null
  auto serialized0 =
      UnsafeRowSerializer::serializeComplexVectors<Array<SmallintType>>(
          arrayVector, buffer_, 0);
  EXPECT_FALSE(serialized0.has_value());

  auto arraySize = getSize(arrayVector, 0);
  EXPECT_EQ(arraySize, serialized0.value_or(0));

  clearBuffer();

  auto dynamic0 = serialize(arrayVector, 0);
  EXPECT_FALSE(dynamic0.has_value());
  clearBuffer();

  // [0x0333, 0x1444, 0x0555]
  auto serialized1 =
      UnsafeRowSerializer::serializeComplexVectors<Array<SmallintType>>(
          arrayVector, buffer_, 1);
  uint8_t expected1[4][8] = {
      {0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x33, 0x03, 0x44, 0x14, 0x55, 0x05, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};
  EXPECT_TRUE(checkVariableLength(serialized1, 4 * 8, *expected1));
  clearBuffer();

  auto dynamic1 = serialize(arrayVector, 1);
  EXPECT_TRUE(checkVariableLength(dynamic1, 4 * 8, *expected1));

  arraySize = getSize(arrayVector, 1);
  EXPECT_EQ(arraySize, dynamic1);

  clearBuffer();

  // [0x1666, 0x0777, null, 0x0999]
  auto serialized2 =
      UnsafeRowSerializer::serializeComplexVectors<Array<SmallintType>>(
          arrayVector, buffer_, 2);
  uint8_t expected2[4][8] = {
      {0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x66, 0x16, 0x77, 0x07, 0x00, 0x00, 0x99, 0x09},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};
  EXPECT_TRUE(checkVariableLength(serialized2, 4 * 8, *expected2));
  // third element (idx 2) is null
  ASSERT_TRUE(bits::isBitSet(buffer_ + 8, 2));
  clearBuffer();

  auto dynamic2 = serialize(arrayVector, 2);

  arraySize = getSize(arrayVector, 2);
  EXPECT_EQ(arraySize, dynamic2);

  EXPECT_TRUE(checkVariableLength(dynamic2, 4 * 8, *expected2));
  // third element (idx 2) is null
  ASSERT_TRUE(bits::isBitSet(buffer_ + 8, 2));
  clearBuffer();
}

TEST_F(UnsafeRowSerializerTests, arrayStringView) {
  auto longString =
      StringView("This is a rather long string.  Quite long indeed.", 49);

  auto arrayVector = makeNullableArrayVector<StringView>({
      {{"Hello"_sv, longString, ""_sv, std::nullopt}},
      {{std::nullopt, "World"_sv}},
      std::nullopt,
  });

  // [ hello, longString, emptyString, null ]
  auto serialized0 =
      UnsafeRowSerializer::serializeComplexVectors<Array<VarcharType>>(
          arrayVector, buffer_, 0);

  uint8_t expected0[14][8] = {
      {0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x05, 0x00, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00},
      {0x31, 0x00, 0x00, 0x00, 0x38, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x70, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00},
      {0x54, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20},
      {0x61, 0x20, 0x72, 0x61, 0x74, 0x68, 0x65, 0x72},
      {0x20, 0x6c, 0x6f, 0x6e, 0x67, 0x20, 0x73, 0x74},
      {0x72, 0x69, 0x6e, 0x67, 0x2e, 0x20, 0x20, 0x51},
      {0x75, 0x69, 0x74, 0x65, 0x20, 0x6c, 0x6f, 0x6e},
      {0x67, 0x20, 0x69, 0x6e, 0x64, 0x65, 0x65, 0x64},
      {0x2e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

  EXPECT_TRUE(checkVariableLength(serialized0, 14 * 8, *expected0));
  // forth element (idx 3) is null
  ASSERT_TRUE(bits::isBitSet(buffer_ + 8, 3));
  clearBuffer();

  auto dynamic0 = serialize(arrayVector, 0);

  auto arraySize = getSize(arrayVector, 0);
  EXPECT_EQ(arraySize, dynamic0);

  EXPECT_TRUE(checkVariableLength(dynamic0, 14 * 8, *expected0));
  // forth element (idx 3) is null
  ASSERT_TRUE(bits::isBitSet(buffer_ + 8, 3));
  clearBuffer();

  // [null, world]
  auto serialized1 =
      UnsafeRowSerializer::serializeComplexVectors<Array<VarcharType>>(
          arrayVector, buffer_, 1);

  uint8_t expected1[5][8] = {
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x05, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00},
      {0x57, 0x6f, 0x72, 0x6c, 0x64, 0x00, 0x00, 0x00}};

  EXPECT_TRUE(checkVariableLength(serialized1, 5 * 8, *expected1));
  // first element (idx 0) is null
  ASSERT_TRUE(bits::isBitSet(buffer_ + 8, 0));
  clearBuffer();

  auto dynamic1 = serialize(arrayVector, 1);
  arraySize = getSize(arrayVector, 1);
  EXPECT_EQ(arraySize, dynamic1);

  EXPECT_TRUE(checkVariableLength(dynamic1, 5 * 8, *expected1));
  // first element (idx 0) is null
  ASSERT_TRUE(bits::isBitSet(buffer_ + 8, 0));
  clearBuffer();

  // null
  auto serialized2 =
      UnsafeRowSerializer::serializeComplexVectors<Array<VarcharType>>(
          arrayVector, buffer_, 2);
  arraySize = getSize(arrayVector, 2);
  EXPECT_EQ(arraySize, serialized2.value_or(0));
  EXPECT_FALSE(serialized2.has_value());
  clearBuffer();

  auto dynamic2 = serialize(arrayVector, 2);
  EXPECT_FALSE(dynamic2.has_value());
  clearBuffer();
}

TEST_F(UnsafeRowSerializerTests, nestedArray) {
  /// ArrayVector<ArrayVector<FlatVector<int8_t>>>
  /// [
  ///  [
  ///    [1, 2], [3, 4]
  ///   ],
  ///  [
  ///    [5, 6, 7], null, [8]
  ///   ],
  ///  [
  ///    [9, 10]
  ///   ],
  /// ]
  /// size: 3
  /// offsets: [0, 2, 5]
  /// lengths: [2, 3, 1]
  /// nullCount: 0
  /// // [1, 2,], [3, 4], [5, 6, 7], null, [8], [9, 10]
  /// ArrayVector<FlatVector<int8_t>>[0] == ArrayVector<FlatVector<int8_t>>[1]
  ///  size: 6
  ///  offsets: [0, 2, 4, 7, 7, 8]
  ///  lengths: [2, 2, 3, 0, 1, 2]
  ///  nulls: 0b001000
  ///  nullCount: 1
  ///  FlatVector<int8_t>
  ///    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
  ///    size: 10
  ///    nullCount: 0

  size_t flatVectorSize = 10;
  auto flatVector = makeFlatVector<int8_t>(
      {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0x10});

  size_t arrayVectorSize = 6;
  bool arrayNullsValue[6] = {0, 0, 0, 1, 0, 0};
  int32_t arrayOffsetsValue[6] = {0, 2, 4, 7, 7, 8};
  vector_size_t arrayLengthsValue[6] = {2, 2, 3, 0, 1, 2};
  auto arrayVector = makeArrayVectorPtr(
      arrayVectorSize,
      pool(),
      arrayOffsetsValue,
      arrayLengthsValue,
      arrayNullsValue,
      ARRAY(TINYINT()),
      flatVector);

  size_t arrayArrayVectorSize = 3;
  bool arrayArrayNullsValue[3] = {0, 0, 0};
  int32_t arrayArrayOffsetsValue[3] = {0, 2, 5};
  vector_size_t arrayArrayLengthsValue[3] = {2, 3, 1};
  auto arrayArrayVector = makeArrayVectorPtr(
      arrayArrayVectorSize,
      pool(),
      arrayArrayOffsetsValue,
      arrayArrayLengthsValue,
      arrayArrayNullsValue,
      ARRAY(ARRAY(TINYINT())),
      arrayVector);

  // [ [1, 2], [3, 4] ]
  auto serialized0 =
      UnsafeRowSerializer::serializeComplexVectors<Array<Array<TinyintType>>>(
          arrayArrayVector, buffer_, 0);
  auto arraySize = getSize(arrayArrayVector, 0);
  EXPECT_EQ(arraySize, serialized0);

  uint8_t expected0[12][8] = {
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x20, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00},
      {0x20, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x03, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

  EXPECT_TRUE(checkVariableLength(serialized0, 12 * 8, *expected0));
  clearBuffer();

  auto dynamic0 = serialize(arrayArrayVector, 0);
  EXPECT_TRUE(checkVariableLength(dynamic0, 12 * 8, *expected0));
  clearBuffer();

  //   [ [5, 6, 7], null, [8] ]
  auto serialized1 =
      UnsafeRowSerializer::serializeComplexVectors<Array<Array<TinyintType>>>(
          arrayArrayVector, buffer_, 1);

  arraySize = getSize(arrayArrayVector, 1);
  EXPECT_EQ(arraySize, serialized1);

  uint8_t expected1[13][8] = {
      {0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x20, 0x00, 0x00, 0x00, 0x28, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x20, 0x00, 0x00, 0x00, 0x48, 0x00, 0x00, 0x00},
      {0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x05, 0x06, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

  EXPECT_TRUE(checkVariableLength(serialized1, 13 * 8, *expected1));
  clearBuffer();

  auto dynamic1 = serialize(arrayArrayVector, 1);
  EXPECT_TRUE(checkVariableLength(dynamic1, 13 * 8, *expected1));
  clearBuffer();

  // [ [9, 10] ]
  auto serialized2 =
      UnsafeRowSerializer::serializeComplexVectors<Array<Array<TinyintType>>>(
          arrayArrayVector, buffer_, 2);

  arraySize = getSize(arrayArrayVector, 2);
  EXPECT_EQ(arraySize, serialized2);

  uint8_t expected2[7][8] = {
      {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x20, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x09, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

  EXPECT_TRUE(checkVariableLength(serialized2, 7 * 8, expected2));
  clearBuffer();

  auto dynamic2 = serialize(arrayArrayVector, 2);
  EXPECT_TRUE(checkVariableLength(dynamic2, 7 * 8, *expected2));
  clearBuffer();
}

TEST_F(UnsafeRowSerializerTests, map) {
  /// [{
  ///  hello: [0x11, 0x22]
  ///  world: [null, null, null]
  ///  null: [0x33]
  /// },
  /// null,
  /// {
  /// hello: [0x44]
  /// }
  ///
  /// MapVector:
  ///  keys: FlatVector<VARCHAR>
  ///  values: ArrayVector<FlatVector<TINYINT>>
  ///  size: 3
  ///  offsets: [0, 3, 3]
  ///  lengths: [3, 0, 1]
  ///  nulls: 0b010
  ///
  /// keys: FlatVector<VARCHAR>:
  ///  [hello, world, null, hello]
  ///  size: 4
  ///  nulls: 0b0100
  ///
  /// values: ArrayVector<FlatVector<TINYINT>>
  ///  [ [0x11, 0x22], [null, null, null], [0x33], [0x44] ]
  ///  size: 4
  ///  offsets: [0, 2, 5, 6]
  ///  lengths: [2, 3, 1, 1]
  ///  nulls: 0b0000
  ///  FlatVector<TINYINT>:
  ///    [0x11, 0x22, null, null, null, 0x33, 0x44]
  ///    size: 7
  ///    nulls: 0b00111000
  ///
  /// @TODO in the future if needed we can fix the map serializer/deserializer
  /// And add corresponding tests here again.

  auto hello = StringView("Hello", 5);
  auto world = StringView("World", 5);
  auto placeHolder = StringView();

  auto keysFlatVector =
      makeNullableFlatVector<StringView>({hello, world, std::nullopt, hello});
  auto valuesFlatVector = makeNullableFlatVector<int8_t>(
      {0x11, 0x22, std::nullopt, std::nullopt, std::nullopt, 0x33, 0x44});

  size_t valuesArrayVectorSize = 4;
  bool valuesNullsValue[4] = {false, false, false, false};
  int32_t valuesOffsetsValue[4] = {0, 2, 5, 6};
  vector_size_t valuesLengthsValue[4] = {2, 3, 1, 1};
  auto valuesArrayVector = makeArrayVectorPtr(
      valuesArrayVectorSize,
      pool(),
      valuesOffsetsValue,
      valuesLengthsValue,
      valuesNullsValue,
      ARRAY(TINYINT()),
      valuesFlatVector);

  size_t mapVectorSize = 3;
  bool mapNullsValue[3] = {false, true, false};
  int32_t mapOffsetsValue[3] = {0, 3, 3};
  vector_size_t mapLengthsValue[3] = {3, 0, 1};
  auto mapVector = makeMapVectorPtr(
      mapVectorSize,
      pool(),
      mapOffsetsValue,
      mapLengthsValue,
      mapNullsValue,
      MAP(VARCHAR(), ARRAY(TINYINT())),
      keysFlatVector,
      valuesArrayVector); // valuesArrayVector

  /// {
  ///  hello: [0x11, 0x22]
  ///  world: [null, null, null]
  ///  null: [0x33]
  /// }
  uint8_t expected0[25][8] = {
      {0x38, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x05, 0x00, 0x00, 0x00, 0x28, 0x00, 0x00, 0x00},
      {0x05, 0x00, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00},
      {0x57, 0x6f, 0x72, 0x6c, 0x64, 0x00, 0x00, 0x00},
      {0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x20, 0x00, 0x00, 0x00, 0x28, 0x00, 0x00, 0x00},
      {0x20, 0x00, 0x00, 0x00, 0x48, 0x00, 0x00, 0x00},
      {0x20, 0x00, 0x00, 0x00, 0x68, 0x00, 0x00, 0x00},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x11, 0x22, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

  auto dynamic0 = serialize(mapVector, 0);

  auto mapSize = getSize(mapVector, 0);
  EXPECT_EQ(mapSize, dynamic0);

  EXPECT_TRUE(checkVariableLength(dynamic0, 25 * 8, *expected0));
  clearBuffer();

  // null
  auto serialized1 = UnsafeRowSerializer::serializeComplexVectors<
      Map<VarcharType, Array<TinyintType>>>(mapVector, buffer_, 1);
  EXPECT_FALSE(serialized1.has_value());
  clearBuffer();

  auto dynamic1 = serialize(mapVector, 1);
  EXPECT_FALSE(dynamic1.has_value());

  mapSize = getSize(mapVector, 1);
  EXPECT_EQ(mapSize, dynamic1.value_or(0));

  clearBuffer();

  /// {
  /// hello: [0x44]
  /// }
  uint8_t expected2[12][8] = {
      {0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x05, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00},
      {0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00},
      {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x20, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00},
      {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x44, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

  auto dynamic2 = serialize(mapVector, 2);
  EXPECT_TRUE(checkVariableLength(dynamic2, 12 * 8, *expected2));

  mapSize = getSize(mapVector, 2);
  EXPECT_EQ(mapSize, dynamic2.value_or(0));

  clearBuffer();
}

TEST_F(UnsafeRowSerializerTests, rowFixedLength) {
  auto c0 = makeNullableFlatVector<int64_t>(
      {0x0101010101010101,
       std::nullopt,
       0x0101010101010101,
       0x0123456789ABCDEF,
       0x1111111111111111});
  auto c1 = makeNullableFlatVector<int32_t>(
      {std::nullopt, 0x0FFFFFFF, 0x0AAAAAAA, std::nullopt, 0x10101010});
  auto c2 = makeNullableFlatVector<int16_t>(
      {0x1111, 0x00FF, 0x7E00, 0x1234, std::nullopt});

  auto c3 = makeConstant<int32_t>(0x22222222, 5);

  auto c4 = makeNullConstant(TypeKind::INTEGER, 5);

  auto c5 = makeConstant(Timestamp(0, 0xFF * 1000), 5);

  auto c6 = makeNullConstant(TypeKind::TIMESTAMP, 5);

  auto rowVector = makeRowVector({c0, c1, c2, c3, c4, c5, c6});

  // row[0], 0b1010010
  // {0x0101010101010101, null, 0x1111, 0x22222222, null, 0xFF, null}
  uint8_t expected0[8][8] = {
      {0x52, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x11, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x22, 0x22, 0x22, 0x22, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
  };

  auto rowType = ROW(
      {BIGINT(),
       INTEGER(),
       SMALLINT(),
       INTEGER(),
       INTEGER(),
       TIMESTAMP(),
       TIMESTAMP()});

  for (auto index = 0; index < 5; index++) {
    auto rowSize = UnsafeRowSerializer::getSizeRow(rowVector.get(), index);
    // In the row of fixed values the size will be the null bits plus 64bit per
    // value
    EXPECT_EQ(rowSize, 8 + 7 * 8);
    EXPECT_EQ(rowSize, UnsafeRowSerializer::getSizeRow(rowVector.get(), index));
  }

  auto bytes0 = serialize(rowVector, 0);
  EXPECT_TRUE(checkVariableLength(bytes0, 8 * 8, *expected0));
  clearBuffer();

  // row[1], 0b1010001
  // {null, 0x0FFFFFFF, 0x00FF, 0x22222222, null, 0xFF, null}
  uint8_t expected1[8][8] = {
      {0x51, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0xFF, 0xFF, 0xFF, 0x0F, 0x00, 0x00, 0x00, 0x00},
      {0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x22, 0x22, 0x22, 0x22, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
  };
  auto bytes1 = serialize(rowVector, 1);
  EXPECT_TRUE(checkVariableLength(bytes1, 8 * 8, *expected1));
  clearBuffer();

  // row[2], 0b1010000
  // {0x0101010101010101, 0x0AAAAAAA, 0x7E00, 0x22222222, null, 0xFF, null}
  uint8_t expected2[8][8] = {
      {0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01},
      {0xAA, 0xAA, 0xAA, 0x0A, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x7E, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x22, 0x22, 0x22, 0x22, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
  };
  auto bytes2 = serialize(rowVector, 2);
  EXPECT_TRUE(checkVariableLength(bytes2, 8 * 8, *expected2));
  clearBuffer();

  // row[3], 0b1010010
  // {0x0123456789ABCDEF, null, 0x1234, 0x22222222, null, 0xFF, null}
  uint8_t expected3[8][8] = {
      {0x52, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x22, 0x22, 0x22, 0x22, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
  };
  auto bytes3 = serialize(rowVector, 3);
  EXPECT_TRUE(checkVariableLength(bytes3, 8 * 8, *expected3));
  clearBuffer();

  // row[4], 0b1010100
  // {0x1111111111111111, 0x10101010, null, 0x22222222, null, 0xFF, null}
  uint8_t expected4[8][8] = {
      {0x54, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11},
      {0x10, 0x10, 0x10, 0x10, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x22, 0x22, 0x22, 0x22, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
  };
  auto bytes4 = serialize(rowVector, 4);
  EXPECT_TRUE(checkVariableLength(bytes4, 8 * 8, *expected4));
}

TEST_F(UnsafeRowSerializerTests, rowVarLength) {
  /*
   * The StringView class reserves a 12-bytes space for inlined string,
   * logically separating into a 4-bytes prefix_ and a 8-bytes union value_.
   * If a string is less than 12 bytes, it is entirely copied into this reserved
   * space. Otherwise, the first 4 bytes of the string is copied into prefix_
   * and union value_ stores the pointer to the string (not 4-bytes after the
   * beginning of the string).
   * Function begin() returns prefix_ when inlined and value_ when not, so the
   * string doesn't get truncated.
   */
  auto c0 = makeNullableFlatVector<int64_t>({0x0101010101010101, std::nullopt});

  auto c1 = makeNullableFlatVector<StringView>(
      {std::nullopt, StringView("Hello World!")});

  auto c2 = makeFlatVector<int64_t>({0xABCDEF, 0xAAAAAAAAAA});

  auto c3 = makeConstant("1234"_sv, 2);

  auto c4 = makeNullConstant(TypeKind::VARCHAR, 2);

  auto c5 = makeFlatVector<StringView>({
      StringView("Im a string with 30 characters"),
      StringView("Pero yo tengo veinte"),
  });

  auto rowVector = makeRowVector({c0, c1, c2, c3, c4, c5});

  // row[0], 0b010010
  // {0x0101010101010101, null, 0xABCDEF, 56llu << 32 | 4, null, 64llu << 32 |
  // 30, "1234", "Im a string with 30 characters"}
  uint8_t expected0[12][8] = {
      {0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0xEF, 0xCD, 0xAB, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x04, 0x00, 0x00, 0x00, 0x38, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x1E, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00},
      {'1', '2', '3', '4', 0x00, 0x00, 0x00, 0x00},
      {'I', 'm', ' ', 'a', ' ', 's', 't', 'r'},
      {'i', 'n', 'g', ' ', 'w', 'i', 't', 'h'},
      {' ', '3', '0', ' ', 'c', 'h', 'a', 'r'},
      {'a', 'c', 't', 'e', 'r', 's', 0x00, 0x00},
  };

  auto bytes0 = serialize(rowVector, 0);
  EXPECT_TRUE(checkVariableLength(bytes0, 12 * 8, *expected0));
  clearBuffer();

  // row[1], 0b010001
  // {null, 56llu << 32 | 12, 0xAAAAAAAAAA, 72llu << 32 | 4, null, 80llu << 32 |
  // 20, "Hello World!", "1234", "Pero yo tengo veinte"}
  uint8_t expected1[13][8] = {
      {0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x0C, 0x00, 0x00, 0x00, 0x38, 0x00, 0x00, 0x00},
      {0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0x00, 0x00, 0x00},
      {0x04, 0x00, 0x00, 0x00, 0x48, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x14, 0x00, 0x00, 0x00, 0x50, 0x00, 0x00, 0x00},
      {'H', 'e', 'l', 'l', 'o', ' ', 'W', 'o'},
      {'r', 'l', 'd', '!', 0x00, 0x00, 0x00, 0x00},
      {'1', '2', '3', '4', 0x00, 0x00, 0x00, 0x00},
      {'P', 'e', 'r', 'o', ' ', 'y', 'o', ' '},
      {'t', 'e', 'n', 'g', 'o', ' ', 'v', 'e'},
      {'i', 'n', 't', 'e', 0x00, 0x00, 0x00, 0x00},
  };

  auto bytes1 = serialize(rowVector, 1);
  EXPECT_TRUE(checkVariableLength(bytes1, 13 * 8, *expected1));
}

TEST_F(UnsafeRowSerializerTests, lazyVector) {
  VectorPtr lazyVector0 = lazyFlatVector<StringView>(
      1, [](vector_size_t i) { return StringView("Hello, World!", 13); });

  auto serialized0 = serialize(lazyVector0, 0);
  EXPECT_TRUE(checkVariableLength(serialized0, 13, u8"Hello, World!"));

  VectorPtr lazyVector1 = lazyFlatVector<Timestamp>(
      1, [](vector_size_t i) { return Timestamp(2, 1'000); });

  auto serialized1 = serialize(lazyVector1, 0);
  int64_t expected1 = 2'000'001;
  EXPECT_TRUE(checkFixedLength(serialized1, 0, &expected1));

  VectorPtr lazyVector2 =
      lazyFlatVector<int32_t>(1, [](vector_size_t i) { return 0x01010101; });

  auto serialized2 =
      UnsafeRowSerializer::serialize<IntegerType>(lazyVector2, buffer_, 0);
  int intVal = 0x01010101;
  EXPECT_TRUE(checkFixedLength(serialized2, 0, &intVal));
}

TEST_F(UnsafeRowSerializerTests, complexNullsAndEncoding) {
  auto type = ROW(
      {VARCHAR(),
       VARCHAR(),
       VARCHAR(),
       VARCHAR(),
       VARCHAR(),
       INTEGER(),
       BIGINT(),
       REAL(),
       DOUBLE(),
       MAP(VARCHAR(), VARCHAR()),
       VARCHAR(),
       TIMESTAMP(),
       ARRAY(VARCHAR()),
       MAP(VARCHAR(), BOOLEAN())});

  auto nullVector = BaseVector::createNullConstant(type, 100, pool());
  auto serialized = serialize(nullVector, 0);
  EXPECT_FALSE(serialized.has_value());

  VectorPtr vp = BaseVector::wrapInConstant(1, 0, nullVector);
  serialized = serialize(vp, 0);
  EXPECT_FALSE(serialized.has_value());

  clearBuffer();
}

class UnsafeRowBatchDeserializerTest : public ::testing::Test {
 public:
  UnsafeRowBatchDeserializerTest()
      : pool_(memory::getDefaultMemoryPool()),
        bufferPtr_(AlignedBuffer::allocate<char>(1024, pool_.get(), true)),
        buffer_(bufferPtr_->asMutable<char>()) {}

 protected:
  /**
   * Checks the Vector metadata (i.e. size, offsets, sizes, nulls) in an
   * ArrayVector or MapVector.
   * @tparam ComplexVectorPtr ArrayVectorPtr or MapVectorPtr
   * @param vector
   * @param expectedSize
   * @param expectedOffsets
   * @param expectedSizes
   * @param expectedNulls
   * @return testing::AssertionFailure if any value is not as expected,
   * testing::AssertionSuccess otherwise
   */
  template <typename ComplexVectorPtr>
  testing::AssertionResult checkVectorMetadata(
      ComplexVectorPtr vector,
      size_t expectedSize,
      int32_t* expectedOffsets,
      vector_size_t* expectedSizes,
      bool* expectedNulls) {
    if (vector->size() != expectedSize) {
      return testing::AssertionFailure() << "Expected size is " << expectedSize
                                         << " but got " << vector->size();
    }

    auto offsets = (vector->offsets())->template as<int32_t>();
    auto sizes = (vector->sizes())->template as<vector_size_t>();

    for (int i = 0; i < expectedSize; i++) {
      if (std::memcmp(expectedOffsets + i, offsets + i, sizeof(int32_t)) != 0) {
        return testing::AssertionFailure()
            << "Vector offsets and expected offsets differ at index " << i;
      }
      if (std::memcmp(expectedSizes + i, sizes + i, sizeof(vector_size_t)) !=
          0) {
        return testing::AssertionFailure()
            << "Vector sizes and expected sizes differ at index " << i;
      }
      if (vector->isNullAt(i) != expectedNulls[i])
        return testing::AssertionFailure()
            << "Vector nulls and expected nulls differ at index " << i;
    }

    return testing::AssertionSuccess();
  }

  std::shared_ptr<memory::MemoryPool> pool_;

  BufferPtr bufferPtr_;

  // variable pointing to the row pointer held by the smart pointer BufferPtr
  char* buffer_;
};

template <typename T>
testing::AssertionResult checkVariableLengthData(
    std::optional<std::string_view> element,
    size_t expectedSize,
    T* expectedValue) {
  if (element->size() != expectedSize) {
    return testing::AssertionFailure()
        << "Expected serializedSize " << expectedSize << " but got "
        << element->size();
  }

  for (int i = 0; i < expectedSize; i++) {
    if (std::memcmp(
            element->data() + i,
            reinterpret_cast<const uint8_t*>(expectedValue) + i,
            1) != 0) {
      return testing::AssertionFailure()
          << "Buffer and expectedValue differ at index " << i;
    }
  }
  return testing::AssertionSuccess();
}

TEST_F(UnsafeRowBatchDeserializerTest, deserializePrimitives) {
  /*
   * UnsfafeRow with 7 elements:
   *  index | type        | value
   *  ------|-------------|-------
   *  0     | BOOLEAN     | true
   *  1     | TINYINT     | 0x1
   *  2     | SMALLINT    | 0x2222
   *  3     | INTEGER     | 0x33333333
   *  4     | BIGINT      | null
   *  5     | REAL        | 1.2345
   *  6     | DOUBLE      | null
   */
  // generated from Spark Java implementation
  uint8_t data[8][8] = {
      {0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x22, 0x22, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x33, 0x33, 0x33, 0x33, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x19, 0x04, 0x9e, 0x3f, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

  auto rowData = std::string_view(reinterpret_cast<const char*>(data), 8 * 8);
  std::vector<TypePtr> rowTypes{
      BOOLEAN(), TINYINT(), SMALLINT(), INTEGER(), BIGINT(), REAL(), DOUBLE()};

  UnsafeRowDynamicParser rowParser = UnsafeRowDynamicParser(rowTypes, rowData);

  ASSERT_FALSE(rowParser.isNullAt(0));
  auto val0 = UnsafeRowPrimitiveBatchDeserializer::deserializeFixedWidth<
      TypeTraits<TypeKind::BOOLEAN>::NativeType>(rowParser.dataAt(0));
  ASSERT_EQ(val0, true);

  ASSERT_FALSE(rowParser.isNullAt(1));
  auto val1 = UnsafeRowPrimitiveBatchDeserializer::deserializeFixedWidth<
      TypeTraits<TypeKind::TINYINT>::NativeType>(rowParser.dataAt(1));
  ASSERT_EQ(val1, 0x1);

  ASSERT_FALSE(rowParser.isNullAt(2));
  auto val2 = UnsafeRowPrimitiveBatchDeserializer::deserializeFixedWidth<
      TypeTraits<TypeKind::SMALLINT>::NativeType>(rowParser.dataAt(2));
  ASSERT_EQ(val2, 0x2222);

  ASSERT_FALSE(rowParser.isNullAt(3));
  auto val3 = UnsafeRowPrimitiveBatchDeserializer::deserializeFixedWidth<
      TypeTraits<TypeKind::INTEGER>::NativeType>(rowParser.dataAt(3));
  ASSERT_EQ(val3, 0x33333333);

  ASSERT_TRUE(rowParser.isNullAt(4));

  ASSERT_FALSE(rowParser.isNullAt(5));
  auto val5 = UnsafeRowPrimitiveBatchDeserializer::deserializeFixedWidth<
      TypeTraits<TypeKind::REAL>::NativeType>(rowParser.dataAt(5));
  ASSERT_EQ(val5, (float)1.2345);

  ASSERT_TRUE(rowParser.isNullAt(6));
}

TEST_F(UnsafeRowBatchDeserializerTest, deserializeStrings) {
  /*
   * index | string value
   * ------|-------------
   * 0     | u8"hello"
   * 1     | null
   * 2     | u8"This is a rather long string.  Quite long indeed."
   */

  uint8_t data[12][8] = {
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x05, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x31, 0x00, 0x00, 0x00, 0x28, 0x00, 0x00, 0x00},
      {0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00},
      {0x54, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20},
      {0x61, 0x20, 0x72, 0x61, 0x74, 0x68, 0x65, 0x72},
      {0x20, 0x6c, 0x6f, 0x6e, 0x67, 0x20, 0x73, 0x74},
      {0x72, 0x69, 0x6e, 0x67, 0x2e, 0x20, 0x20, 0x51},
      {0x75, 0x69, 0x74, 0x65, 0x20, 0x6c, 0x6f, 0x6e},
      {0x67, 0x20, 0x69, 0x6e, 0x64, 0x65, 0x65, 0x64},
      {0x2e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

  auto rowData = std::string_view(reinterpret_cast<const char*>(data), 12 * 8);
  std::vector<TypePtr> rowTypes{VARCHAR(), VARCHAR(), VARCHAR()};

  UnsafeRowDynamicParser rowParser = UnsafeRowDynamicParser(rowTypes, rowData);

  ASSERT_FALSE(rowParser.isNullAt(0));
  StringView val0 = UnsafeRowPrimitiveBatchDeserializer::deserializeStringView(
      rowParser.dataAt(0));
  checkVariableLengthData(
      std::string_view(val0.data(), val0.size()), 0x05, u8"hello");

  ASSERT_TRUE(rowParser.isNullAt(1));

  ASSERT_FALSE(rowParser.isNullAt(2));
  StringView val2 = UnsafeRowPrimitiveBatchDeserializer::deserializeStringView(
      rowParser.dataAt(2));
  checkVariableLengthData(
      std::string_view(val2.data(), val2.size()),
      0x31,
      u8"This is a rather long string.  Quite long indeed.");
}

TEST_F(UnsafeRowBatchDeserializerTest, fixedWidthArray) {
  /*
   * UnsafeRow with 2 elements (element 2 is ignored):
   * Element 1: Array of TinyInt with 5 elements
   *   [0x01, 0x02, null, 0x03, null],
   *
   * ArrayVector<FlatVector<int8_t>>:
   * offsets: 0
   * sizes: 5
   *    FlatVector<int8_t>>:
   *    0x01, 0x02, null, 0x03, null
   *
   * This test only checks for the first element
   */
  uint8_t data[10][8] = {
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x18, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00},
      {0x20, 0x00, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00},
      {0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x02, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00},
      {0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x11, 0x11, 0x22, 0x22, 0x00, 0x00, 0x44, 0x44},
      {0x55, 0x55, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

  auto rowData = std::string_view(reinterpret_cast<const char*>(data), 10 * 8);
  std::vector<TypePtr> rowTypes{ARRAY(TINYINT()), ARRAY(SMALLINT())};
  UnsafeRowDynamicParser rowParser = UnsafeRowDynamicParser(rowTypes, rowData);

  VectorPtr val0 = UnsafeRowDeserializer::deserializeOne(
      rowParser.dataAt(0), rowParser.typeAt(0), pool_.get());
  /*
   * ArrayVector<FlatVector<int8_t>>:
   * offsets: 0
   * sizes: 5
   */
  auto arrayVectorPtr = std::dynamic_pointer_cast<ArrayVector>(val0);
  ASSERT_TRUE(arrayVectorPtr);
  auto arrayVectorSize = 1;
  int32_t arrayVectorOffsets[1] = {0};
  vector_size_t arrayVectorLengths[1] = {5};
  bool arrayVectorNulls[1] = {0};
  ASSERT_TRUE(checkVectorMetadata(
      arrayVectorPtr,
      arrayVectorSize,
      arrayVectorOffsets,
      arrayVectorLengths,
      arrayVectorNulls));

  // FlatVector<int8_t>
  //   0x01, 0x02, null, 0x03, null
  auto arrayFlatVector = arrayVectorPtr->elements()->asFlatVector<int8_t>();
  ASSERT_TRUE(arrayFlatVector);
  ASSERT_FALSE(arrayFlatVector->isNullAt(0));
  ASSERT_EQ(arrayFlatVector->valueAt(0), 0x01);
  ASSERT_FALSE(arrayFlatVector->isNullAt(1));
  ASSERT_EQ(arrayFlatVector->valueAt(1), 0x02);
  ASSERT_TRUE(arrayFlatVector->isNullAt(2));
  ASSERT_FALSE(arrayFlatVector->isNullAt(3));
  ASSERT_EQ(arrayFlatVector->valueAt(3), 0x03);
  ASSERT_TRUE(arrayFlatVector->isNullAt(4));
}

TEST_F(UnsafeRowBatchDeserializerTest, nestedArray) {
  /*
   * type: Array->Array->Array->TinyInt
   * ArrayVector<ArrayVector<ArrayVector<FlatVector<int8_t>>>
   * [
   *  [
   *    [1, 2], [3, 4]
   *   ],
   *  [
   *    [5, 6, 7], null, [8]
   *   ],
   *  [
   *    [9, 10]
   *   ],
   * ]
   * ArrayVector<ArrayVector<ArrayVector<FlatVector<int8_t>>>
   * size: 1
   * offsets: [0]
   * lengths: [3]
   * // [[1, 2,], [3, 4]], [[5, 6, 7], null, [8]], [[9, 10]]
   * elements: ArrayVector<ArrayVector<FlatVector<int8_t>>:
   *   size: 3
   *   offsets: [0, 2, 5]
   *   lengths: [2, 3, 1]
   *   nullCount: 0
   *   // [1, 2,], [3, 4], [5, 6, 7], null, [8], [9, 10]
   *   elements: ArrayVector<FlatVector<int8_t>>
   *    size: 6
   *    offsets: [0, 2, 4, 7, 7, 8]
   *    lengths: [2, 2, 3, 0, 1, 2]
   *    nulls: 0b001000
   *    nullCount: 1
   *    FlatVector<int8_t>
   *      [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
   *      size: 10
   *      nullCount: 0
   */

  uint8_t data0[17 * 2][8] = {
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x01, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00},
      {0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x50, 0x00, 0x00, 0x00, 0x28, 0x00, 0x00, 0x00},
      {0x58, 0x00, 0x00, 0x00, 0x78, 0x00, 0x00, 0x00},
      {0x30, 0x00, 0x00, 0x00, 0xd0, 0x00, 0x00, 0x00},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x18, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00},
      {0x18, 0x00, 0x00, 0x00, 0x38, 0x00, 0x00, 0x00},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x03, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x18, 0x00, 0x00, 0x00, 0x28, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x18, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00},
      {0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x05, 0x06, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x18, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x09, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

  auto rowData =
      std::string_view(reinterpret_cast<const char*>(data0), 17 * 2 * 8);
  std::vector<TypePtr> rowTypes{ARRAY(ARRAY(ARRAY(TINYINT())))};
  UnsafeRowDynamicParser rowParser = UnsafeRowDynamicParser(rowTypes, rowData);

  VectorPtr val0 = UnsafeRowDeserializer::deserializeOne(
      rowParser.dataAt(0), rowParser.typeAt(0), pool_.get());

  /*
   * ArrayVector<ArrayVector<ArrayVector<FlatVector<int8_t>>>
   * size: 1
   * offsets: [0]
   * lengths: [3]
   */
  auto outermostArrayVectorPtr = std::dynamic_pointer_cast<ArrayVector>(val0);
  ASSERT_TRUE(outermostArrayVectorPtr);
  auto outermostArraySize = 1;
  int32_t outermostArrayOffsets[1] = {0};
  vector_size_t outermostArraySizes[1] = {3};
  bool outermostArrayNulls[1] = {0};
  ASSERT_TRUE(checkVectorMetadata(
      outermostArrayVectorPtr,
      outermostArraySize,
      outermostArrayOffsets,
      outermostArraySizes,
      outermostArrayNulls));

  /*
   * ArrayVector<ArrayVector<FlatVector<int8_t>>>
   * size: 3
   * offsets: [0, 2, 5]
   * lengths: [2, 3, 1]
   * nulls: [0, 0, 0]
   */
  auto outerArrayVectorPtr = std::dynamic_pointer_cast<ArrayVector>(
      outermostArrayVectorPtr->elements());
  ASSERT_TRUE(outerArrayVectorPtr);
  auto outerArraySize = 3;
  int32_t outerArrayOffsets[3] = {0, 2, 5};
  vector_size_t outerArraySizes[3] = {2, 3, 1};
  bool outerArrayNulls[3] = {0, 0, 0};
  ASSERT_TRUE(checkVectorMetadata(
      outerArrayVectorPtr,
      outerArraySize,
      outerArrayOffsets,
      outerArraySizes,
      outerArrayNulls));

  /*
   * [1, 2,], [3, 4], [5, 6, 7], null, [8], [9, 10]
   * ArrayVector<FlatVector<int8_t>>[0]
   * size: 6
   * offsets: [0, 2, 4, 7, 7, 8]
   * lengths: [2, 2, 3, 0, 1, 2]
   * nulls: 0b001000
   */
  auto innerArrayVectorPtr =
      std::dynamic_pointer_cast<ArrayVector>(outerArrayVectorPtr->elements());
  ASSERT_TRUE(innerArrayVectorPtr);
  auto innerArraySize = 6;
  int32_t innerArrayOffsets[6] = {0, 2, 4, 7, 7, 8};
  vector_size_t innerArraySizes[6] = {2, 2, 3, 0, 1, 2};
  bool innerArrayNulls[6] = {0, 0, 0, 1, 0, 0};
  ASSERT_TRUE(checkVectorMetadata(
      innerArrayVectorPtr,
      innerArraySize,
      innerArrayOffsets,
      innerArraySizes,
      innerArrayNulls));

  /*
   * FlatVector<int8_t>
   * [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
   * size: 10
   * nullCount: 0
   */
  auto innermostFlatVector =
      innerArrayVectorPtr->elements()->asFlatVector<int8_t>();
  ASSERT_TRUE(innermostFlatVector);
  ASSERT_EQ(innermostFlatVector->size(), 10);
  int8_t expectedValue[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  ASSERT_EQ(
      std::memcmp(
          innermostFlatVector->rawValues(), expectedValue, sizeof(int8_t) * 10),
      0);
}

TEST_F(UnsafeRowBatchDeserializerTest, nestedMap) {
  /*
   * TypePtr: Map<Short, Map<Short, Short>>
   * {
   *   1 : {
   *          2 : 3,
   *          4 : null
   *        },
   *   6 : {
   *          7 : 8
   *        }
   *  }
   * Map<Short, Map<Short, Short>>
   *  offsets: 0
   *  sizes: 2
   *  keys: FlatVector<Short>
   *    1, 6
   *  values: MapVector<Short, Short>
   *    offsets: 0, 2
   *    sizes: 2, 1
   *    nulls: 0, 0
   *    Keys: FlatVector<Short>
   *      2, 4, 7
   *    Values: FlatVector<Short>
   *      3, null, 8
   */

  uint8_t data0[12 * 2][8] = {
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0xb0, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00},
      {0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x38, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00},
      {0x38, 0x00, 0x00, 0x00, 0x58, 0x00, 0x00, 0x00},
      {0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x02, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

  auto rowData =
      std::string_view(reinterpret_cast<const char*>(data0), 12 * 2 * 8);
  std::vector<TypePtr> rowTypes{MAP(SMALLINT(), MAP(SMALLINT(), SMALLINT()))};
  UnsafeRowDynamicParser rowParser = UnsafeRowDynamicParser(rowTypes, rowData);

  VectorPtr val0 = UnsafeRowDeserializer::deserializeOne(
      rowParser.dataAt(0), rowParser.typeAt(0), pool_.get());

  /*
   * Map<Short, Map<Short, Short>>
   *  offsets: 0
   *  sizes: 2
   */
  auto outerMapVectorPtr = std::dynamic_pointer_cast<MapVector>(val0);
  assert(outerMapVectorPtr);
  auto outerMapSize = 1;
  int32_t outerMapOffsets[1] = {0};
  vector_size_t outerMapSizes[1] = {2};
  bool outerMapNulls[1] = {0};
  ASSERT_EQ(1, outerMapVectorPtr->size());
  EXPECT_EQ(
      outerMapVectorPtr->toString(0),
      "2 elements starting at 0 {1 => 2 elements starting at 0 {2 => 3, 4 => null}, 6 => 1 elements starting at 2 {7 => 8}}");
  ASSERT_TRUE(checkVectorMetadata(
      outerMapVectorPtr,
      outerMapSize,
      outerMapOffsets,
      outerMapSizes,
      outerMapNulls));

  /*
   * keys: FlatVector<Short>
   *    1, 6
   */
  auto outerKeys = outerMapVectorPtr->mapKeys()->asFlatVector<int16_t>();
  ASSERT_TRUE(outerKeys);
  ASSERT_EQ(outerKeys->size(), 2);
  int16_t expectedValue[2] = {1, 6};
  ASSERT_EQ(
      std::memcmp(outerKeys->rawValues(), expectedValue, sizeof(int16_t) * 2),
      0);

  /*
   *  values: MapVector<Short, Short>
   *    offsets: 0, 2
   *    sizes: 2, 1
   *    nulls: 0, 0
   */
  auto innerMapVectorPtr =
      std::dynamic_pointer_cast<MapVector>(outerMapVectorPtr->mapValues());
  ASSERT_TRUE(innerMapVectorPtr);
  auto innerMapSize = 2;
  int32_t innerMapOffsets[2] = {0, 2};
  vector_size_t innerMapSizes[2] = {2, 1};
  bool innerMapNulls[2] = {0, 0};
  ASSERT_TRUE(checkVectorMetadata(
      innerMapVectorPtr,
      innerMapSize,
      innerMapOffsets,
      innerMapSizes,
      innerMapNulls));
}

TEST_F(UnsafeRowBatchDeserializerTest, rowVector) {
  // row[0], 0b010010
  // {0x0101010101010101, null, 0xABCDEF, 56llu << 32 | 4, null, 64llu << 32 |
  // 60, "1234", "Make time for civilization, for civilization wont make time."}
  uint8_t data0[16][8] = {
      {0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0xEF, 0xCD, 0xAB, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x04, 0x00, 0x00, 0x00, 0x38, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x3C, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00},
      {'1', '2', '3', '4', 0x00, 0x00, 0x00, 0x00},
      {'M', 'a', 'k', 'e', ' ', 't', 'i', 'm'},
      {'e', ' ', 'f', 'o', 'r', ' ', 'c', 'i'},
      {'v', 'i', 'l', 'i', 'z', 'a', 't', 'i'},
      {'o', 'n', ',', ' ', 'f', 'o', 'r', ' '},
      {'c', 'i', 'v', 'i', 'l', 'i', 'z', 'a'},
      {'t', 'i', 'o', 'n', ' ', 'w', 'o', 'n'},
      {'t', ' ', 'm', 'a', 'k', 'e', ' ', 't'},
      {'i', 'm', 'e', '.', 0x00, 0x00, 0x00, 0x00}};

  // row[1], 0b010010
  // {0x0101010101010101, null, 0xABCDEF, 56llu << 32 | 4, null, 64llu << 32 |
  // 30, "1234", "Im a string with 30 characters"}
  uint8_t data1[12][8] = {
      {0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0xEF, 0xCD, 0xAB, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x04, 0x00, 0x00, 0x00, 0x38, 0x00, 0x00, 0x00},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x1E, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00},
      {'1', '2', '3', '4', 0x00, 0x00, 0x00, 0x00},
      {'I', 'm', ' ', 'a', ' ', 's', 't', 'r'},
      {'i', 'n', 'g', ' ', 'w', 'i', 't', 'h'},
      {' ', '3', '0', ' ', 'c', 'h', 'a', 'r'},
      {'a', 'c', 't', 'e', 'r', 's', 0x00, 0x00},
  };

  std::vector<bool> nulls{false, true, false, false, true, false};
  auto row0 = std::string_view(reinterpret_cast<const char*>(data0), 16 * 8);
  auto row1 = std::string_view(reinterpret_cast<const char*>(data1), 12 * 8);

  // Two rows
  std::vector<std::optional<std::string_view>> rows{row0, row1};

  auto rowType =
      ROW({BIGINT(), VARCHAR(), BIGINT(), VARCHAR(), VARCHAR(), VARCHAR()});

  VectorPtr val0 =
      UnsafeRowDeserializer::deserialize(rows, rowType, pool_.get());

  auto rowVectorPtr = std::dynamic_pointer_cast<RowVector>(val0);

  ASSERT_NE(rowVectorPtr, nullptr);
  ASSERT_EQ(rowVectorPtr->size(), 2);

  const auto& children = rowVectorPtr->children();
  ASSERT_EQ(children.size(), 6);
  for (size_t i = 0; i < 6; i++) {
    EXPECT_EQ(children[i]->type()->kind(), rowType->childAt(i)->kind());
    ASSERT_EQ(children[i]->size(), 2);
    EXPECT_EQ(children[i]->isNullAt(0), nulls[i]);
    EXPECT_EQ(children[i]->isNullAt(1), nulls[i]);
  }

  EXPECT_EQ(
      rowVectorPtr->toString(0),
      "{72340172838076673, null, 11259375, 1234, null, \
Make time for civilization, for civilization wont make time.}");
  EXPECT_EQ(
      rowVectorPtr->toString(1),
      "{72340172838076673, null, 11259375, 1234, null, \
Im a string with 30 characters}");
}

class UnsafeRowComplexBatchDeserializerTests : public testing::Test,
                                               public test::VectorTestBase {
 public:
  UnsafeRowComplexBatchDeserializerTests() {}

  constexpr static int kMaxBuffers = 10;

  RowVectorPtr createInputRow(
      int32_t batchSize,
      std::function<bool(vector_size_t /*row*/)> isNullAt = nullptr) {
    VELOX_CHECK(batchSize <= kMaxBuffers);
    auto intVector =
        makeFlatVector<int64_t>(batchSize, [](vector_size_t i) { return i; });
    auto stringVector =
        makeFlatVector<StringView>(batchSize, [](vector_size_t i) {
          return StringView::makeInline("string" + std::to_string(i));
        });
    auto intArrayVector = makeArrayVector<int64_t>(
        batchSize,
        [](vector_size_t row) { return row % 3; },
        [](vector_size_t row, vector_size_t index) { return row + index; });
    auto stringArrayVector = makeArrayVector<StringView>(
        batchSize,
        [](vector_size_t row) { return row % 5; },
        [](vector_size_t row, vector_size_t index) {
          return StringView::makeInline("str" + std::to_string(row + index));
        });
    return makeRowVector(
        {intVector, stringVector, intArrayVector, stringArrayVector}, isNullAt);
  }

  void testVectorSerde(const RowVectorPtr& inputVector) {
    std::vector<std::optional<std::string_view>> serializedVector;
    for (size_t i = 0; i < inputVector->size(); ++i) {
      // Serialize rowVector into bytes.
      auto rowSize =
          UnsafeRowSerializer::serialize(inputVector, buffers_[i], /*idx=*/i);
      if (rowSize) {
        serializedVector.push_back(
            std::string_view(buffers_[i], rowSize.value()));
      } else {
        serializedVector.push_back(std::nullopt);
      }
    }
    VectorPtr outputVector = UnsafeRowDeserializer::deserialize(
        serializedVector, inputVector->type(), pool());
    test::assertEqualVectors(inputVector, outputVector);
  }

  std::array<char[1024], kMaxBuffers> buffers_{};
};

TEST_F(UnsafeRowComplexBatchDeserializerTests, rows) {
  // Run 3 tests for serde with different batch sizes.
  for (int32_t batchSize : {1, 5, 10}) {
    const auto& inputVector = createInputRow(batchSize);
    testVectorSerde(inputVector);
  }
}

TEST_F(UnsafeRowComplexBatchDeserializerTests, nullRows) {
  // Test single level all nulls RowVector serde.
  for (auto& batchSize : {1, 5, 10}) {
    const auto& inputVector = createInputRow(batchSize, nullEvery(1));
    testVectorSerde(inputVector);
  }

  // Test RowVector containing another all nulls RowVector serde.
  for (auto& innerBatchSize : {1, 5, 10}) {
    const auto innerRowVector = createInputRow(innerBatchSize, nullEvery(1));
    const auto outerRowVector = makeRowVector({innerRowVector});
    testVectorSerde(outerRowVector);
  }
}

} // namespace
} // namespace facebook::velox::row
