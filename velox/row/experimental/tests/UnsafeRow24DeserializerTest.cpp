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

#include "velox/row/experimental/UnsafeRow24Deserializer.h"
#include <gtest/gtest.h>
#include <vector>
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/row/UnsafeRowSerializers.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include "velox/vector/BaseVector.h"
#include "velox/vector/TypeAliases.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::row;

namespace {

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

VectorPtr deserialize(
    const std::vector<std::optional<std::string_view>>& rows,
    TypePtr type,
    memory::MemoryPool* pool) {
  VELOX_CHECK(type->isRow());
  std::vector<const char*> row_data;
  for (auto& row : rows) {
    row_data.push_back(row.has_value() ? row.value().data() : nullptr);
  }
  return UnsafeRow24Deserializer::Create(
             std::dynamic_pointer_cast<const RowType>(type))
      ->DeserializeRows(pool, row_data);
}

} // namespace

class UnsafeRowVectorDeserializerTest : public ::testing::Test {
 public:
  UnsafeRowVectorDeserializerTest()
      : pool_(memory::getDefaultMemoryPool()),
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

  std::shared_ptr<memory::MemoryPool> pool_;

  BufferPtr bufferPtr;

  // variable pointing to the row pointer held by the smart pointer BufferPtr
  char* buffer;
};

TEST_F(UnsafeRowVectorDeserializerTest, nestedArray) {
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
  ;
  auto rowType = ROW({"f0"}, {ARRAY(ARRAY(ARRAY(TINYINT())))});
  VectorPtr val = deserialize({rowData}, rowType, pool_.get());
  VectorPtr val0 = val->as<RowVector>()->childAt(0);

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

TEST_F(UnsafeRowVectorDeserializerTest, nestedMap) {
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

  VectorPtr val = deserialize(
      {rowData},
      ROW({"f0"}, {MAP(SMALLINT(), MAP(SMALLINT(), SMALLINT()))}),
      this->pool_.get());
  VectorPtr val0 = val->as<RowVector>()->childAt(0);

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
  // print_buffer(reinterpret_cast<const char
  // *>(outerMapVectorPtr->sizes()->asMutable<vector_size_t>()), 128);
  bool outerMapNulls[1] = {0};
  ASSERT_TRUE(this->checkVectorMetadata(
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
  ASSERT_TRUE(this->checkVectorMetadata(
      innerMapVectorPtr,
      innerMapSize,
      innerMapOffsets,
      innerMapSizes,
      innerMapNulls));
}

TEST_F(UnsafeRowVectorDeserializerTest, rowVector) {
  {
    // row[0], 0b010010
    // {
    //  0x0101010101010101,
    //  null,
    //  0xABCDEF,
    //  56llu << 32 | 4,
    //  null,
    //  64llu << 32 | 60,
    //  "1234",
    //  "Make time for civilization, for civilization wont make time."
    // }
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
    // {
    //  0x0101010101010101,
    //  null,
    //  0xABCDEF,
    //  56llu << 32 | 4,
    //  null,
    //  64llu << 32 | 30,
    //  "1234",
    //  "Im a string with 30 characters"
    // }
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

    VectorPtr val0 = deserialize(rows, rowType, this->pool_.get());

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

  // When there is a single row. And it's null.
  {
    uint8_t dataNull[8] = {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    auto rowNull = std::string_view(reinterpret_cast<const char*>(dataNull), 8);
    std::vector<std::optional<std::string_view>> rows{rowNull};
    auto rowType = ROW({ROW({BIGINT(), BIGINT(), VARCHAR(), VARCHAR()})});
    VectorPtr val = deserialize(rows, rowType, this->pool_.get());
    auto rowVectorPtr = std::dynamic_pointer_cast<RowVector>(val);
    ASSERT_NE(rowVectorPtr, nullptr);
    ASSERT_EQ(rowVectorPtr->size(), 1);
    EXPECT_EQ(TypeKind::ROW, rowVectorPtr->childAt(0)->typeKind());
    auto innerRowVectorPtr =
        std::dynamic_pointer_cast<RowVector>(rowVectorPtr->childAt(0));
    ASSERT_NE(innerRowVectorPtr, nullptr);
    ASSERT_EQ(innerRowVectorPtr->size(), 1);
    EXPECT_EQ(true, innerRowVectorPtr->isNullAt(0));
  }
}

class UnsafeRowComplexDeserializerTests : public exec::test::OperatorTestBase {
 public:
  UnsafeRowComplexDeserializerTests() {}

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
          return StringView::makeInline("string" + std::to_string(row + index));
        });
    return makeRowVector(
        {intVector, stringVector, intArrayVector, stringArrayVector}, isNullAt);
  }

  void testVectorSerde(const RowVectorPtr& inputVector) {
    std::vector<std::optional<std::string_view>> serializedVector;
    for (size_t i = 0; i < inputVector->size(); ++i) {
      // Serialize rowVector into bytes.
      auto rowSize = UnsafeRowSerializer::serialize(
          inputVector, this->buffers_[i], /*idx=*/i);
      serializedVector.push_back(
          rowSize.has_value()
              ? std::optional<std::string_view>(
                    std::string_view(this->buffers_[i], rowSize.value()))
              : std::nullopt);
    }
    VectorPtr outputVector =
        deserialize(serializedVector, inputVector->type(), this->pool_.get());
    assertEqualVectors(inputVector, outputVector);
  }

  std::shared_ptr<memory::MemoryPool> pool_ = memory::getDefaultMemoryPool();
  std::array<char[1024], kMaxBuffers> buffers_{};
};

TEST_F(UnsafeRowComplexDeserializerTests, rows) {
  // Run 3 tests for serde with different batch sizes.
  for (int32_t batchSize : {1, 5, 10}) {
    std::vector<std::optional<std::string_view>> serializedVec;
    const auto& inputVector = this->createInputRow(batchSize);
    this->testVectorSerde(inputVector);
  }
}

TEST_F(UnsafeRowComplexDeserializerTests, nullRows) {
  // Test single level all nulls RowVector serde.
  for (auto& batchSize : {1, 5, 10}) {
    std::vector<std::optional<std::string_view>> serializedVector;
    const auto& inputVector =
        this->createInputRow(batchSize, VectorTestBase::nullEvery(1));
    this->testVectorSerde(inputVector);
  }

  // Test RowVector containing another all nulls RowVector serde.
  //
  // TODO: The serde is still buggy as tests with innerBatchSize larger than 1
  // is currently still failing. That is, when a RowVector(A) contains another
  // RowVector(b). And RowVector(b) contains more than one rows and all of them
  // are null. We should also fix that.
  for (auto& innerBatchSize : {1}) {
    std::vector<std::optional<std::string_view>> serializedVector;
    const auto& innerRowVector =
        this->createInputRow(innerBatchSize, VectorTestBase::nullEvery(1));
    const auto& outerRowVector = this->makeRowVector({innerRowVector});
    this->testVectorSerde(outerRowVector);
  }
}

TEST_F(UnsafeRowComplexDeserializerTests, DISABLED_Testfuzzer) {
  std::string buffer(100 << 20, '\0'); // Up to 100MB.
  VectorFuzzer fuzzer(
      {
          .nullRatio = 0.1,
          .containerHasNulls = false,
          .dictionaryHasNulls = false,
          .stringVariableLength = true,
          .containerVariableLength = true,
          .timestampPrecision =
              VectorFuzzer::Options::TimestampPrecision::kMicroSeconds,
      },
      this->pool_.get(),
      0);
  for (int i = 0; i < 100; ++i) {
    auto seed = i; // TODO: Switch to folly::Random::rand32().
    fuzzer.reSeed(seed);
    const auto type = fuzzer.randRowType();
    LOG(INFO) << "i=" << i << " seed=" << seed << " type=" << type->toString();
    const VectorPtr input = fuzzer.fuzzRow(type);
    std::vector<std::optional<std::string_view>> rowData;
    char* data = &buffer[0];
    for (int j = 0; j < input->size(); ++j) {
      auto size = UnsafeRowSerializer::serialize(input, data, j);
      ASSERT_TRUE(size);
      rowData.emplace_back(std::string_view(data, *size));
      data += *size;
    }
    auto output = deserialize(rowData, type, this->pool_.get());
    assertEqualVectors(input, output);
  }
}
