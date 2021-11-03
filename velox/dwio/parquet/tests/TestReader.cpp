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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "velox/common/base/test_utils/GTestUtils.h"
#include "velox/dwio/dwrf/test/utils/DataFiles.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/type/fbhive/HiveTypeParser.h"
#include "velox/type/Type.h"
#include "velox/type/tests/FilterBuilder.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/VectorMaker.h"

#include <fmt/core.h>
#include <array>
#include <numeric>

using namespace ::testing;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwio::type::fbhive;
using namespace facebook::velox;
using namespace facebook::velox::parquet;

class ParquetReaderTest : public testing::Test {
 protected:
  std::string getExampleFilePath(const std::string& fileName) {
    return test::getDataFilePath(
        "velox/dwio/parquet/tests", "examples/" + fileName);
  }

  RowReaderOptions getSampleReaderOpts() {
    RowReaderOptions rowReaderOpts;
    auto rowType = ROW({"a", "b"}, {BIGINT(), DOUBLE()});
    auto cs = std::make_shared<ColumnSelector>(
        rowType, std::vector<std::string>{"a", "b"});
    rowReaderOpts.select(cs);
    return rowReaderOpts;
  }

  template <typename T>
  VectorPtr rangeVector(size_t size, T start) {
    std::vector<T> vals(size);
    for (size_t i = 0; i < size; ++i) {
      vals[i] = start + static_cast<T>(i);
    }
    return vectorMaker_->flatVector(vals);
  }

  // Check that actual vector is equal to a part of expected vector
  // at a specified offset.
  void assertEqualVectorPart(
      const VectorPtr& expected,
      const VectorPtr& actual,
      size_t offset) {
    ASSERT_GE(expected->size(), actual->size() + offset);
    ASSERT_EQ(expected->typeKind(), actual->typeKind());
    for (auto i = 0; i < actual->size(); i++) {
      ASSERT_TRUE(expected->equalValueAt(actual.get(), i + offset, i))
          << "at " << (i + offset) << ": expected "
          << expected->toString(i + offset) << ", but got "
          << actual->toString(i);
    }
  }

  void assertReadExpected(RowReader& reader, RowVectorPtr expected) {
    uint64_t total = 0;
    VectorPtr result;
    while (total < expected->size()) {
      auto part = reader.next(1000, result);
      EXPECT_GT(part, 0);
      assertEqualVectorPart(expected, result, total);
      total += part;
    }
    EXPECT_EQ(total, expected->size());
    EXPECT_EQ(reader.next(1000, result), 0);
  }

  std::unique_ptr<memory::ScopedMemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  std::unique_ptr<test::VectorMaker> vectorMaker_{
      std::make_unique<test::VectorMaker>(pool_.get())};
};

TEST_F(ParquetReaderTest, testReadSampleFull) {
  // sample.parquet holds two columns (a: BIGINT, b: DOUBLE) and
  // 20 rows (10 rows per group). Group offsets are 153 and 614.
  // Data is in plain uncompressed format:
  //   a: [1..20]
  //   b: [1.0..20.0]
  const std::string sample(getExampleFilePath("sample.parquet"));

  ReaderOptions readerOptions;
  ParquetReader reader(
      std::make_unique<FileInputStream>(sample), readerOptions);

  EXPECT_EQ(reader.numberOfRows(), 20ULL);

  auto type = reader.typeWithId();
  EXPECT_EQ(type->size(), 2ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type->kind(), TypeKind::BIGINT);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col1->type->kind(), TypeKind::DOUBLE);
  EXPECT_EQ(type->childByName("a"), col0);
  EXPECT_EQ(type->childByName("b"), col1);

  RowReaderOptions rowReaderOpts = getSampleReaderOpts();
  auto rowReader = reader.createRowReader(rowReaderOpts);
  auto expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(20, 1), rangeVector<double>(20, 1)});
  assertReadExpected(*rowReader, expected);
}

TEST_F(ParquetReaderTest, testReadSampleRange1) {
  const std::string sample(getExampleFilePath("sample.parquet"));

  ReaderOptions readerOptions;
  ParquetReader reader(
      std::make_unique<FileInputStream>(sample), readerOptions);

  RowReaderOptions rowReaderOpts = getSampleReaderOpts();
  rowReaderOpts.range(0, 200);
  auto rowReader = reader.createRowReader(rowReaderOpts);
  auto expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(10, 1), rangeVector<double>(10, 1)});
  assertReadExpected(*rowReader, expected);
}

TEST_F(ParquetReaderTest, testReadSampleRange2) {
  const std::string sample(getExampleFilePath("sample.parquet"));

  ReaderOptions readerOptions;
  ParquetReader reader(
      std::make_unique<FileInputStream>(sample), readerOptions);

  RowReaderOptions rowReaderOpts = getSampleReaderOpts();
  rowReaderOpts.range(200, 500);
  auto rowReader = reader.createRowReader(rowReaderOpts);
  auto expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(10, 11), rangeVector<double>(10, 11)});
  assertReadExpected(*rowReader, expected);
}

TEST_F(ParquetReaderTest, testReadSampleEmptyRange) {
  const std::string sample(getExampleFilePath("sample.parquet"));

  ReaderOptions readerOptions;
  ParquetReader reader(
      std::make_unique<FileInputStream>(sample), readerOptions);

  RowReaderOptions rowReaderOpts = getSampleReaderOpts();
  rowReaderOpts.range(300, 10);
  auto rowReader = reader.createRowReader(rowReaderOpts);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(1000, result), 0);
}

TEST_F(ParquetReaderTest, testReadSampleBigintRangeFilter) {
  const std::string sample(getExampleFilePath("sample.parquet"));

  ReaderOptions readerOptions;
  ParquetReader reader(
      std::make_unique<FileInputStream>(sample), readerOptions);

  RowReaderOptions rowReaderOpts = getSampleReaderOpts();
  common::ScanSpec scanSpec("");
  scanSpec.getOrCreateChild(common::Subfield("a"))
      ->setFilter(common::test::between(16, 20));
  scanSpec.getOrCreateChild(common::Subfield("b"));
  rowReaderOpts.setScanSpec(&scanSpec);
  auto rowReader = reader.createRowReader(rowReaderOpts);
  auto expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(5, 16), rangeVector<double>(5, 16)});
  assertReadExpected(*rowReader, expected);
}

TEST_F(ParquetReaderTest, testReadSampleEqualFilter) {
  const std::string sample(getExampleFilePath("sample.parquet"));

  ReaderOptions readerOptions;
  ParquetReader reader(
      std::make_unique<FileInputStream>(sample), readerOptions);

  RowReaderOptions rowReaderOpts = getSampleReaderOpts();
  common::ScanSpec scanSpec("");
  scanSpec.getOrCreateChild(common::Subfield("a"))
      ->setFilter(common::test::between(16, 16));
  scanSpec.getOrCreateChild(common::Subfield("b"));
  rowReaderOpts.setScanSpec(&scanSpec);
  auto rowReader = reader.createRowReader(rowReaderOpts);
  auto expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(1, 16), rangeVector<double>(1, 16)});
  assertReadExpected(*rowReader, expected);
}
