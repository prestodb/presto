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

#include <gtest/gtest.h>
#include <vector>

#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/row/UnsafeRowBatchDeserializer.h"
#include "velox/row/UnsafeRowDynamicSerializer.h"
#include "velox/row/UnsafeRowParser.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/TypeAliases.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::row {
namespace {

using namespace facebook::velox::test;

class UnsafeRowBatchDeserializerTest : public ::testing::Test {
 public:
  UnsafeRowBatchDeserializerTest()
      : pool_(memory::getDefaultScopedMemoryPool()),
        bufferPtr(AlignedBuffer::allocate<char>(1024, pool_.get(), true)),
        buffer(bufferPtr->asMutable<char>()) {}

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

    auto offsets = (vector->offsets())->template asMutable<int32_t>();
    auto sizes = (vector->sizes())->template asMutable<vector_size_t>();

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

  std::unique_ptr<memory::ScopedMemoryPool> pool_;

  BufferPtr bufferPtr;

  // variable pointing to the row pointer held by the smart pointer BufferPtr
  char* buffer;
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

TEST_F(UnsafeRowBatchDeserializerTest, DeserializePrimitives) {
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

TEST_F(UnsafeRowBatchDeserializerTest, DeserializeStrings) {
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

TEST_F(UnsafeRowBatchDeserializerTest, FixedWidthArray) {
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

  VectorPtr val0 = UnsafeRowDynamicVectorBatchDeserializer::deserializeComplex(
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

TEST_F(UnsafeRowBatchDeserializerTest, NestedArray) {
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

  VectorPtr val0 = UnsafeRowDynamicVectorBatchDeserializer::deserializeComplex(
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

TEST_F(UnsafeRowBatchDeserializerTest, NestedMap) {
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

  VectorPtr val0 = UnsafeRowDynamicVectorBatchDeserializer::deserializeComplex(
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

TEST_F(UnsafeRowBatchDeserializerTest, RowVector) {
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

  VectorPtr val0 = UnsafeRowDynamicVectorBatchDeserializer::deserializeComplex(
      rows, rowType, pool_.get());

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

class UnsafeRowComplexBatchDeserializerTests
    : public exec::test::OperatorTestBase {
 public:
  UnsafeRowComplexBatchDeserializerTests() {}

  constexpr static int kMaxBuffers = 10;

  velox::RowVectorPtr createInputRow(int32_t batchSize) {
    VELOX_CHECK(batchSize <= kMaxBuffers);
    auto intVector =
        makeFlatVector<int64_t>(batchSize, [](vector_size_t i) { return i; });
    auto stringVector =
        makeFlatVector<StringView>(batchSize, [](vector_size_t i) {
          return StringView("string" + std::to_string(i));
        });
    auto intArrayVector = makeArrayVector<int64_t>(
        batchSize,
        [](vector_size_t row) { return row % 3; },
        [](vector_size_t row, vector_size_t index) { return row + index; });
    auto stringArrayVector = makeArrayVector<StringView>(
        batchSize,
        [](vector_size_t row) { return row % 5; },
        [](vector_size_t row, vector_size_t index) {
          return StringView("str" + std::to_string(row + index));
        });
    return makeRowVector(
        {intVector, stringVector, intArrayVector, stringArrayVector});
  }

  std::unique_ptr<memory::ScopedMemoryPool> pool_ =
      memory::getDefaultScopedMemoryPool();
  std::array<char[1024], kMaxBuffers> buffers_{};
};

TEST_F(
    UnsafeRowComplexBatchDeserializerTests,
    UnsafeRowDeserializationRowsTests) {
  std::vector<std::optional<std::string_view>> serializedVec;
  int32_t batchSize = 10;
  const auto& inputVector = createInputRow(batchSize);
  for (size_t i = 0; i < batchSize; ++i) {
    // Serialize rowVector into bytes.
    auto rowSize = UnsafeRowDynamicSerializer::serialize(
        inputVector->type(), inputVector, buffers_[i], /*idx=*/i);
    ASSERT_TRUE(rowSize.has_value());
    serializedVec.push_back(std::string_view(buffers_[i], rowSize.value()));
  }
  VectorPtr outputVector =
      UnsafeRowDynamicVectorBatchDeserializer::deserializeComplex(
          serializedVec, inputVector->type(), pool_.get());
  assertEqualVectors(inputVector, outputVector);
}

TEST_F(UnsafeRowComplexBatchDeserializerTests, UnsafeRowDeserializationTests) {
  const auto& inputVector = createInputRow(1);
  // Serialize rowVector into bytes.
  auto rowSize = UnsafeRowDynamicSerializer::serialize(
      inputVector->type(), inputVector, buffers_[0], /*idx=*/0);

  VectorPtr outputVector =
      UnsafeRowDynamicVectorBatchDeserializer::deserializeComplex(
          std::string_view(buffers_[0], rowSize.value()),
          inputVector->type(),
          pool_.get());
  assertEqualVectors(inputVector, outputVector);
}

TEST_F(
    UnsafeRowComplexBatchDeserializerTests,
    UnsafeRowDeserializationRows2Tests) {
  const auto& inputVector = createInputRow(2);
  // Serialize rowVector into bytes.
  auto rowSize = UnsafeRowDynamicSerializer::serialize(
      inputVector->type(), inputVector, buffers_[0], /*idx=*/0);

  auto nextRowSize = UnsafeRowDynamicSerializer::serialize(
      inputVector->type(), inputVector, buffers_[1], /*idx=*/1);

  VectorPtr outputVector =
      UnsafeRowDynamicVectorBatchDeserializer::deserializeComplex(
          {std::string_view(buffers_[0], rowSize.value()),
           std::string_view(buffers_[1], nextRowSize.value())},
          inputVector->type(),
          pool_.get());
  assertEqualVectors(inputVector, outputVector);
}

} // namespace
} // namespace facebook::velox::row
