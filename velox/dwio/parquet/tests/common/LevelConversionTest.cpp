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

// Adapted from Apache Arrow.

#include "velox/dwio/parquet/common/LevelConversion.h"

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/parquet/common/LevelComparison.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap.h"
#include "arrow/util/ubsan.h"

namespace facebook::velox::parquet {
namespace {

using ::arrow::internal::Bitmap;
using ::testing::ElementsAreArray;

std::string BitmapToString(const uint8_t* bitmap, int64_t bitCount) {
  return ::arrow::internal::Bitmap(bitmap, /*offset*/ 0, /*length=*/bitCount)
      .ToString();
}

std::string BitmapToString(
    const std::vector<uint8_t>& bitmap,
    int64_t bitCount) {
  return BitmapToString(bitmap.data(), bitCount);
}

TEST(TestColumnReader, DefLevelsToBitmap) {
  // Bugs in this function were exposed in ARROW-3930
  std::vector<int16_t> defLevels = {3, 3, 3, 2, 3, 3, 3, 3, 3};

  std::vector<uint8_t> validBits(2, 0);

  LevelInfo levelInfo;
  levelInfo.defLevel = 3;
  levelInfo.repLevel = 1;

  ValidityBitmapInputOutput io;
  io.valuesReadUpperBound = defLevels.size();
  io.valuesRead = -1;
  io.validBits = validBits.data();

  DefLevelsToBitmap(defLevels.data(), 9, levelInfo, &io);
  ASSERT_EQ(9, io.valuesRead);
  ASSERT_EQ(1, io.nullCount);

  // Call again with 0 definition levels, make sure that validBits is
  // unmodified
  const uint8_t current_byte = validBits[1];
  io.nullCount = 0;
  DefLevelsToBitmap(defLevels.data(), 0, levelInfo, &io);

  ASSERT_EQ(0, io.valuesRead);
  ASSERT_EQ(0, io.nullCount);
  ASSERT_EQ(current_byte, validBits[1]);
}

TEST(TestColumnReader, DefLevelsToBitmapPowerOfTwo) {
  // PARQUET-1623: Invalid memory access when decoding a valid bits vector that
  // has a length equal to a power of two and also using a non-zero
  // validBitsOffset.  This should not fail when run with ASAN or valgrind.
  std::vector<int16_t> defLevels = {3, 3, 3, 2, 3, 3, 3, 3};
  std::vector<uint8_t> validBits(1, 0);

  LevelInfo levelInfo;
  levelInfo.repLevel = 1;
  levelInfo.defLevel = 3;

  ValidityBitmapInputOutput io;
  io.valuesReadUpperBound = defLevels.size();
  io.valuesRead = -1;
  io.validBits = validBits.data();

  // Read the latter half of the validity bitmap
  DefLevelsToBitmap(defLevels.data() + 4, 4, levelInfo, &io);
  ASSERT_EQ(4, io.valuesRead);
  ASSERT_EQ(0, io.nullCount);
}

#if defined(ARROW_LITTLE_ENDIAN)
TEST(GreaterThanBitmap, GeneratesExpectedBitmasks) {
  std::vector<int16_t> levels = {
      0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5,
      6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3,
      4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7};
  EXPECT_EQ(GreaterThanBitmap(levels.data(), /*num_levels=*/0, /*rhs*/ 0), 0);
  EXPECT_EQ(GreaterThanBitmap(levels.data(), /*num_levels=*/64, /*rhs*/ 8), 0);
  EXPECT_EQ(
      GreaterThanBitmap(levels.data(), /*num_levels=*/64, /*rhs*/ -1),
      0xFFFFFFFFFFFFFFFF);
  // Should be zero padded.
  EXPECT_EQ(
      GreaterThanBitmap(levels.data(), /*num_levels=*/47, /*rhs*/ -1),
      0x7FFFFFFFFFFF);
  EXPECT_EQ(
      GreaterThanBitmap(levels.data(), /*num_levels=*/64, /*rhs*/ 6),
      0x8080808080808080);
}
#endif

TEST(DefLevelsToBitmap, WithRepetitionLevelFiltersOutEmptyListValues) {
  std::vector<uint8_t> validity_bitmap(/*count*/ 8, 0);

  ValidityBitmapInputOutput io;
  io.valuesReadUpperBound = 64;
  io.valuesRead = 1;
  io.nullCount = 5;
  io.validBits = validity_bitmap.data();
  io.validBitsOffset = 1;

  LevelInfo levelInfo;
  levelInfo.repeatedAncestorDefLevel = 1;
  levelInfo.defLevel = 2;
  levelInfo.repLevel = 1;
  // All zeros should be ignored, ones should be unset in the bitmp and 2 should
  // be set.
  std::vector<int16_t> defLevels = {0, 0, 0, 2, 2, 1, 0, 2};
  DefLevelsToBitmap(defLevels.data(), defLevels.size(), levelInfo, &io);

  EXPECT_EQ(BitmapToString(validity_bitmap, /*bitCount=*/8), "01101000");
  for (size_t x = 1; x < validity_bitmap.size(); x++) {
    EXPECT_EQ(validity_bitmap[x], 0) << "index: " << x;
  }
  EXPECT_EQ(io.nullCount, /*5 + 1 =*/6);
  EXPECT_EQ(io.valuesRead, 4); // value should get overwritten.
}

struct MultiLevelTestData {
 public:
  std::vector<int16_t> defLevels;
  std::vector<int16_t> repLevels;
};

MultiLevelTestData TriplyNestedList() {
  // Triply nested list values borrow from write_path
  // [null, [[1 , null, 3], []], []],
  // [[[]], [[], [1, 2]], null, [[3]]],
  // null,
  // []
  return MultiLevelTestData{
      /*defLevels=*/std::vector<int16_t>{
          2,
          7,
          6,
          7,
          5,
          3, // first row
          5,
          5,
          7,
          7,
          2,
          7, // second row
          0, // third row
          1},
      /*repLevels=*/
      std::vector<int16_t>{
          0,
          1,
          3,
          3,
          2,
          1, // first row
          0,
          1,
          2,
          3,
          1,
          1, // second row
          0,
          0}};
}

template <typename ConverterType>
class NestedListTest : public testing::Test {
 public:
  void InitForLength(int length) {
    this->validityBits_.clear();
    this->validityBits_.insert(this->validityBits_.end(), length, 0);
    validityIo_.validBits = validityBits_.data();
    validityIo_.valuesReadUpperBound = length;
    offsets_.clear();
    offsets_.insert(offsets_.end(), length + 1, 0);
  }

  typename ConverterType::OffsetsType* Run(
      const MultiLevelTestData& testData,
      LevelInfo levelInfo) {
    return this->converter_.ComputeListInfo(
        testData, levelInfo, &validityIo_, offsets_.data());
  }

  ConverterType converter_;
  ValidityBitmapInputOutput validityIo_;
  std::vector<uint8_t> validityBits_;
  std::vector<typename ConverterType::OffsetsType> offsets_;
};

template <typename IndexType>
struct RepDefLevelConverter {
  using OffsetsType = IndexType;
  OffsetsType* ComputeListInfo(
      const MultiLevelTestData& testData,
      LevelInfo levelInfo,
      ValidityBitmapInputOutput* output,
      IndexType* offsets) {
    DefRepLevelsToList(
        testData.defLevels.data(),
        testData.repLevels.data(),
        testData.defLevels.size(),
        levelInfo,
        output,
        offsets);
    return offsets + output->valuesRead;
  }
};

using ConverterTypes = ::testing::Types<
    RepDefLevelConverter</*list_length_type=*/int32_t>,
    RepDefLevelConverter</*list_length_type=*/int64_t>>;
TYPED_TEST_SUITE(NestedListTest, ConverterTypes);

TYPED_TEST(NestedListTest, OuterMostTest) {
  // [null, [[1 , null, 3], []], []],
  // [[[]], [[], [1, 2]], null, [[3]]],
  // null,
  // []
  // -> 4 outer most lists (len(3), len(4), null, len(0))
  LevelInfo levelInfo;
  levelInfo.repLevel = 1;
  levelInfo.defLevel = 2;

  this->InitForLength(4);
  typename TypeParam::OffsetsType* next_position =
      this->Run(TriplyNestedList(), levelInfo);

  EXPECT_EQ(next_position, this->offsets_.data() + 4);
  EXPECT_THAT(this->offsets_, testing::ElementsAre(0, 3, 7, 7, 7));

  EXPECT_EQ(this->validityIo_.valuesRead, 4);
  EXPECT_EQ(this->validityIo_.nullCount, 1);
  EXPECT_EQ(BitmapToString(this->validityIo_.validBits, /*length=*/4), "1101");
}

TYPED_TEST(NestedListTest, MiddleListTest) {
  // [null, [[1 , null, 3], []], []],
  // [[[]], [[], [1, 2]], null, [[3]]],
  // null,
  // []
  // -> middle lists (null, len(2), len(0),
  //                  len(1), len(2), null, len(1),
  //                  N/A,
  //                  N/A
  LevelInfo levelInfo;
  levelInfo.repLevel = 2;
  levelInfo.defLevel = 4;
  levelInfo.repeatedAncestorDefLevel = 2;

  this->InitForLength(7);
  typename TypeParam::OffsetsType* next_position =
      this->Run(TriplyNestedList(), levelInfo);

  EXPECT_EQ(next_position, this->offsets_.data() + 7);
  EXPECT_THAT(this->offsets_, testing::ElementsAre(0, 0, 2, 2, 3, 5, 5, 6));

  EXPECT_EQ(this->validityIo_.valuesRead, 7);
  EXPECT_EQ(this->validityIo_.nullCount, 2);
  EXPECT_EQ(
      BitmapToString(this->validityIo_.validBits, /*length=*/7), "0111101");
}

TYPED_TEST(NestedListTest, InnerMostListTest) {
  // [null, [[1, null, 3], []], []],
  // [[[]], [[], [1, 2]], null, [[3]]],
  // null,
  // []
  // -> 6 inner lists (N/A, [len(3), len(0)], N/A
  //                        len(0), [len(0), len(2)], N/A, len(1),
  //                        N/A,
  //                        N/A
  LevelInfo levelInfo;
  levelInfo.repLevel = 3;
  levelInfo.defLevel = 6;
  levelInfo.repeatedAncestorDefLevel = 4;

  this->InitForLength(6);
  typename TypeParam::OffsetsType* next_position =
      this->Run(TriplyNestedList(), levelInfo);

  EXPECT_EQ(next_position, this->offsets_.data() + 6);
  EXPECT_THAT(this->offsets_, testing::ElementsAre(0, 3, 3, 3, 3, 5, 6));

  EXPECT_EQ(this->validityIo_.valuesRead, 6);
  EXPECT_EQ(this->validityIo_.nullCount, 0);
  EXPECT_EQ(
      BitmapToString(this->validityIo_.validBits, /*length=*/6), "111111");
}

TYPED_TEST(NestedListTest, SimpleLongList) {
  LevelInfo levelInfo;
  levelInfo.repLevel = 1;
  levelInfo.defLevel = 2;
  levelInfo.repeatedAncestorDefLevel = 0;

  MultiLevelTestData testData;
  // No empty lists.
  testData.defLevels = std::vector<int16_t>(65 * 9, 2);
  for (int x = 0; x < 65; x++) {
    testData.repLevels.push_back(0);
    testData.repLevels.insert(
        testData.repLevels.end(),
        8,
        /*repLevel=*/1);
  }

  std::vector<typename TypeParam::OffsetsType> expected_offsets(66, 0);
  for (size_t x = 1; x < expected_offsets.size(); x++) {
    expected_offsets[x] = static_cast<typename TypeParam::OffsetsType>(x) * 9;
  }
  this->InitForLength(65);
  typename TypeParam::OffsetsType* next_position =
      this->Run(testData, levelInfo);

  EXPECT_EQ(next_position, this->offsets_.data() + 65);
  EXPECT_THAT(this->offsets_, testing::ElementsAreArray(expected_offsets));

  EXPECT_EQ(this->validityIo_.valuesRead, 65);
  EXPECT_EQ(this->validityIo_.nullCount, 0);
  EXPECT_EQ(
      BitmapToString(this->validityIo_.validBits, /*length=*/65),
      "11111111 "
      "11111111 "
      "11111111 "
      "11111111 "
      "11111111 "
      "11111111 "
      "11111111 "
      "11111111 "
      "1");
}

TYPED_TEST(NestedListTest, TestOverflow) {
  LevelInfo levelInfo;
  levelInfo.repLevel = 1;
  levelInfo.defLevel = 2;
  levelInfo.repeatedAncestorDefLevel = 0;

  MultiLevelTestData testData;
  testData.defLevels = std::vector<int16_t>{2};
  testData.repLevels = std::vector<int16_t>{0};

  this->InitForLength(2);
  // Offsets is populated as the cumulative sum of all elements,
  // so populating the offsets[0] with max-value impacts the
  // other values populated.
  this->offsets_[0] =
      std::numeric_limits<typename TypeParam::OffsetsType>::max();
  this->offsets_[1] =
      std::numeric_limits<typename TypeParam::OffsetsType>::max();
  ASSERT_THROW(this->Run(testData, levelInfo), VeloxException);

  ASSERT_THROW(this->Run(testData, levelInfo), VeloxException);

  // Same thing should happen if the list already existed.
  testData.repLevels = std::vector<int16_t>{1};
  ASSERT_THROW(this->Run(testData, levelInfo), VeloxException);

  // Should be OK because it shouldn't increment.
  testData.defLevels = std::vector<int16_t>{0};
  testData.repLevels = std::vector<int16_t>{0};
  this->Run(testData, levelInfo);
}

TEST(TestOnlyExtractBitsSoftware, BasicTest) {
  auto check =
      [](uint64_t bitmap, uint64_t selection, uint64_t expected) -> void {
    EXPECT_EQ(TestOnlyExtractBitsSoftware(bitmap, selection), expected);
  };
  check(0xFF, 0, 0);
  check(0xFF, ~uint64_t{0}, 0xFF);
  check(0xFF00FF, 0xAAAA, 0x000F);
  check(0xFF0AFF, 0xAFAA, 0x00AF);
  check(0xFFAAFF, 0xAFAA, 0x03AF);
  check(0xFECBDA9876543210ULL, 0xF00FF00FF00FF00FULL, 0xFBD87430ULL);
}

} // namespace
} // namespace facebook::velox::parquet
