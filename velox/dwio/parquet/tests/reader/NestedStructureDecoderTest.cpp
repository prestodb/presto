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

#include "velox/dwio/parquet/reader/NestedStructureDecoder.h"
#include "velox/buffer/Buffer.h"
#include "velox/dwio/common/BufferUtil.h"
#include "velox/vector/TypeAliases.h"

#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::parquet;

class NestedStructureDecoderTest : public testing::Test {
 protected:
  static constexpr int32_t kMaxNumValues = 10'000;

 protected:
  void SetUp() override {
    pool_ = memory::getDefaultMemoryPool();

    dwio::common::ensureCapacity<bool>(
        nullsBuffer_, kMaxNumValues, pool_.get());
    dwio::common::ensureCapacity<vector_size_t>(
        offsetsBuffer_, kMaxNumValues + 1, pool_.get());
    dwio::common::ensureCapacity<vector_size_t>(
        lengthsBuffer_, kMaxNumValues, pool_.get());
  }

  void assertStructure(
      const uint8_t* definitionLevels,
      const uint8_t* repetitionLevels,
      int64_t numValues,
      uint8_t maxDefinition,
      uint8_t maxRepeat,
      std::vector<vector_size_t> expectedOffsets,
      std::vector<vector_size_t> expectedLengths,
      std::vector<bool> expectedNulls) {
    int64_t numCollections = NestedStructureDecoder::readOffsetsAndNulls(
        definitionLevels,
        repetitionLevels,
        numValues,
        maxDefinition,
        maxRepeat,
        offsetsBuffer_,
        lengthsBuffer_,
        nullsBuffer_,
        *pool_);

    assertBufferContent<vector_size_t>(
        offsetsBuffer_, numCollections + 1, expectedOffsets);
    assertBufferContent<vector_size_t>(
        lengthsBuffer_, numCollections, expectedLengths);
    assertNulls(nullsBuffer_, numCollections, expectedNulls);
  }

 private:
  template <typename T>
  void assertBufferContent(
      BufferPtr actual,
      size_t actualSize,
      std::vector<T> expected) {
    ASSERT_EQ(actualSize, expected.size());

    for (int i = 0; i < actualSize; ++i) {
      EXPECT_EQ(actual->as<T>()[i], expected[i]);
    }
  }

  template <typename T>
  void printArray(const T* values, int numValues) {
    std::cout << numValues << std::endl;
    for (int i = 0; i < numValues; i++) {
      std::cout << values[i] << " ";
    }
    std::cout << std::endl;
  }

  void
  assertNulls(BufferPtr actual, size_t actualSize, std::vector<bool> expected) {
    ASSERT_EQ(actualSize, expected.size());

    auto nullsBuffer = actual->as<uint64_t>();
    for (int i = 0; i < actualSize; ++i) {
      EXPECT_EQ(bits::isBitNull(nullsBuffer, i), expected[i]);
    }
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  BufferPtr offsetsBuffer_;
  BufferPtr lengthsBuffer_;
  BufferPtr nullsBuffer_;
};

// ------------------------
// ARRAY<INTEGER>
// [1, 2, 3, 4]
// [2,3]
// [null, 3, null]
// NULL
// [null]
// [null, null]
// [7, 8, 9]
// [8]
// [null]
TEST_F(NestedStructureDecoderTest, oneLevel) {
  uint8_t defs[] = {3, 3, 3, 3, 3, 3, 2, 3, 2, 0, 2, 2, 2, 3, 3, 3, 3, 2};
  uint8_t reps[] = {0, 1, 1, 1, 0, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1, 1, 0, 0};
  std::vector<vector_size_t> expectedOffsets(
      {0, 4, 6, 9, 9, 10, 12, 15, 16, 17});
  std::vector<vector_size_t> expectedLengths({4, 2, 3, 0, 1, 2, 3, 1, 1});
  std::vector<bool> expectedNulls(
      {false, false, false, true, false, false, false, false, false});

  assertStructure(
      defs, reps, 18, 1, 1, expectedOffsets, expectedLengths, expectedNulls);
}

//---------------------------
// ARRAY<ARRAY<INTEGER>>
// [[1], [2, 3]]
// [[2, null], [2]]
// [[3, null], null, [4, 5]]
// [[4, null], null, null]
// null
// [null, [null], null, [6]]
// [[7], null, [7]]
// [null, [8, 9]]
// This test would construct offsets/lengths/nulls for the second level ARRAY
TEST_F(NestedStructureDecoderTest, secondLevelInTwo) {
  uint8_t defs[] = {5, 5, 5, 5, 4, 5, 5, 4, 2, 5, 5, 5, 4,
                    2, 2, 0, 2, 4, 2, 5, 5, 2, 5, 2, 5, 5};
  uint8_t reps[] = {0, 1, 2, 0, 2, 1, 0, 2, 1, 1, 2, 0, 2,
                    1, 1, 0, 0, 1, 1, 1, 0, 1, 1, 0, 1, 2};
  std::vector<vector_size_t> expectedOffsets({0,  1,  3,  5,  6,  8,  8,
                                              10, 12, 12, 12, 12, 13, 13,
                                              14, 15, 15, 16, 16, 18});
  std::vector<vector_size_t> expectedLengths(
      {1, 2, 2, 1, 2, 0, 2, 2, 0, 0, 0, 1, 0, 1, 1, 0, 1, 0, 2});
  std::vector<bool> expectedNulls(
      {false,
       false,
       false,
       false,
       false,
       true,
       false,
       false,
       true,
       true,
       true,
       false,
       true,
       false,
       false,
       true,
       false,
       true,
       false});

  // tests the second level, where maxDefinition = 3 and maxRepeat = 2
  assertStructure(
      defs, reps, 26, 3, 2, expectedOffsets, expectedLengths, expectedNulls);
}

//---------------------------
// ARRAY<ARRAY<INTEGER>>
// [[2, null], [2]]
// [[3, null], null, [4, 5]]
// [[4, null], null, null]
// null
// [null, [null], null, [6]]
// [[7], null, [7]]
// [null, [8, 9]]
// [null]
// This test would construct offsets/lengths/nulls for the top level
TEST_F(NestedStructureDecoderTest, firstLevelInTwo) {
  uint8_t defs[] = {5, 4, 5, 5, 4, 2, 5, 5, 5, 4, 2, 2,
                    0, 2, 4, 2, 5, 5, 2, 5, 2, 5, 5, 2};
  uint8_t reps[] = {0, 2, 1, 0, 2, 1, 1, 2, 0, 2, 1, 1,
                    0, 0, 1, 1, 1, 0, 1, 1, 0, 1, 2, 0};
  std::vector<vector_size_t> expectedOffsets({0, 2, 5, 8, 8, 12, 15, 17, 18});
  std::vector<vector_size_t> expectedLengths({2, 3, 3, 0, 4, 3, 2, 1});
  std::vector<bool> expectedNulls({
      false,
      false,
      false,
      true,
      false,
      false,
      false,
      false,
  });

  // tests the second level, where maxDefinition = 1 and maxRepeat = 1
  assertStructure(
      defs, reps, 24, 1, 1, expectedOffsets, expectedLengths, expectedNulls);
}

// ------------------------
// ARRAY<ARRAY<INTEGER>>
// [[1, null], null, [2]]
// null   -- Empty row
// null   -- Empty row
// [[3]]
// This test would construct offsets/lengths/nulls for the second level
TEST_F(NestedStructureDecoderTest, emptyRowsInTheMiddle) {
  uint8_t defs[] = {5, 4, 2, 5, 0, 0, 5};
  uint8_t reps[] = {0, 2, 1, 1, 0, 0, 0};
  std::vector<vector_size_t> expectedOffsets({0, 2, 2, 3, 4});
  std::vector<vector_size_t> expectedLengths({2, 0, 1, 1});
  std::vector<bool> expectedNulls({false, true, false, false});

  // tests the second level, where maxDefinition = 3 and maxRepeat = 2
  assertStructure(
      defs, reps, 7, 3, 2, expectedOffsets, expectedLengths, expectedNulls);
}

// ------------------------
// ARRAY<ARRAY<INTEGER>>
// [[1, null], null]
// null   -- Empty row
// [[2]]
// This test would construct offsets/lengths/nulls for the second level
TEST_F(NestedStructureDecoderTest, emptyRowAfterNull) {
  uint8_t defs[] = {5, 4, 2, 0, 5};
  uint8_t reps[] = {0, 2, 1, 0, 0};
  std::vector<vector_size_t> expectedOffsets({0, 2, 2, 3});
  std::vector<vector_size_t> expectedLengths({2, 0, 1});
  std::vector<bool> expectedNulls({false, true, false});

  // tests the second level, where maxDefinition = 3 and maxRepeat = 2
  assertStructure(
      defs, reps, 5, 3, 2, expectedOffsets, expectedLengths, expectedNulls);
}

// ------------------------
// ARRAY<ARRAY<INTEGER>>
// [[1, null], null]
// null   -- Empty row
// This test would construct offsets/lengths/nulls for the second level
TEST_F(NestedStructureDecoderTest, emptyRowAtEnd) {
  uint8_t defs[] = {5, 4, 2, 0};
  uint8_t reps[] = {0, 2, 1, 0};
  std::vector<vector_size_t> expectedOffsets({0, 2, 2});
  std::vector<vector_size_t> expectedLengths({2, 0});
  std::vector<bool> expectedNulls({false, true});

  // tests the second level, where maxDefinition = 3 and maxRepeat = 2
  assertStructure(
      defs, reps, 4, 3, 2, expectedOffsets, expectedLengths, expectedNulls);
}
