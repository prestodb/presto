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

#include "velox/dwio/parquet/reader/ParquetReader.h"
#include <gtest/gtest.h>
#include "velox/dwio/dwrf/test/utils/DataFiles.h"
#include "velox/type/Type.h"
#include "velox/type/tests/FilterBuilder.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/VectorMaker.h"

#include <fmt/core.h>
#include <array>

using namespace ::testing;
using namespace facebook::velox::dwio::common;
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

  RowReaderOptions getDateReaderOpts() {
    RowReaderOptions rowReaderOpts;
    auto rowType = ROW({"date"}, {DATE()});
    auto cs = std::make_shared<ColumnSelector>(
        rowType, std::vector<std::string>{"date"});
    rowReaderOpts.select(cs);
    return rowReaderOpts;
  }

  RowReaderOptions getIntReaderOpts() {
    RowReaderOptions rowReaderOpts;
    auto rowType = ROW({"int", "bigint"}, {INTEGER(), BIGINT()});
    auto cs = std::make_shared<ColumnSelector>(
        rowType, std::vector<std::string>{"int", "bigint"});
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

template <>
VectorPtr ParquetReaderTest::rangeVector<Date>(size_t size, Date start) {
  return vectorMaker_->flatVector<Date>(
      size, [&](auto row) { return Date(start.days() + row); });
}

TEST_F(ParquetReaderTest, readSampleFull) {
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

TEST_F(ParquetReaderTest, readSampleRange1) {
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

TEST_F(ParquetReaderTest, readSampleRange2) {
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

TEST_F(ParquetReaderTest, readSampleEmptyRange) {
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

TEST_F(ParquetReaderTest, readSampleBigintRangeFilter) {
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

TEST_F(ParquetReaderTest, readSampleEqualFilter) {
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

TEST_F(ParquetReaderTest, dateRead) {
  // date.parquet holds a single column (date: DATE) and
  // 25 rows.
  // Data is in plain uncompressed format:
  //   date: [1969-12-27 .. 1970-01-20]
  const std::string sample(getExampleFilePath("date.parquet"));

  ReaderOptions readerOptions;
  ParquetReader reader(
      std::make_unique<FileInputStream>(sample), readerOptions);

  EXPECT_EQ(reader.numberOfRows(), 25ULL);

  auto type = reader.typeWithId();
  EXPECT_EQ(type->size(), 1ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type->kind(), TypeKind::DATE);

  RowReaderOptions rowReaderOpts = getDateReaderOpts();
  auto rowReader = reader.createRowReader(rowReaderOpts);

  auto expected = vectorMaker_->rowVector({rangeVector<Date>(25, -5)});
  assertReadExpected(*rowReader, expected);
}

TEST_F(ParquetReaderTest, dateFilter) {
  const std::string sample(getExampleFilePath("date.parquet"));

  ReaderOptions readerOptions;
  ParquetReader reader(
      std::make_unique<FileInputStream>(sample), readerOptions);

  RowReaderOptions rowReaderOpts = getDateReaderOpts();
  common::ScanSpec scanSpec("");
  scanSpec.getOrCreateChild(common::Subfield("date"))
      ->setFilter(common::test::between(5, 14));
  rowReaderOpts.setScanSpec(&scanSpec);
  auto rowReader = reader.createRowReader(rowReaderOpts);

  auto expected = vectorMaker_->rowVector({rangeVector<Date>(10, 5)});
  assertReadExpected(*rowReader, expected);
}

TEST_F(ParquetReaderTest, intRead) {
  // int.parquet holds integer columns (int: INTEGER, bigint: BIGINT)
  // and 10 rows.
  // Data is in plain uncompressed format:
  //   int: [100 .. 109]
  //   bigint: [1000 .. 1009]
  const std::string sample(getExampleFilePath("int.parquet"));

  ReaderOptions readerOptions;
  ParquetReader reader(
      std::make_unique<FileInputStream>(sample), readerOptions);

  EXPECT_EQ(reader.numberOfRows(), 10ULL);

  auto type = reader.typeWithId();
  EXPECT_EQ(type->size(), 2ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type->kind(), TypeKind::INTEGER);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col1->type->kind(), TypeKind::BIGINT);

  RowReaderOptions rowReaderOpts = getIntReaderOpts();
  auto rowReader = reader.createRowReader(rowReaderOpts);

  auto expected = vectorMaker_->rowVector(
      {rangeVector<int32_t>(10, 100), rangeVector<int64_t>(10, 1000)});
  assertReadExpected(*rowReader, expected);
}

TEST_F(ParquetReaderTest, intMultipleFilters) {
  const std::string sample(getExampleFilePath("int.parquet"));

  ReaderOptions readerOptions;
  ParquetReader reader(
      std::make_unique<FileInputStream>(sample), readerOptions);

  RowReaderOptions rowReaderOpts = getIntReaderOpts();
  common::ScanSpec scanSpec("");
  scanSpec.getOrCreateChild(common::Subfield("int"))
      ->setFilter(common::test::between(102, 120));
  scanSpec.getOrCreateChild(common::Subfield("bigint"))
      ->setFilter(common::test::between(900, 1006));
  rowReaderOpts.setScanSpec(&scanSpec);
  auto rowReader = reader.createRowReader(rowReaderOpts);

  auto expected = vectorMaker_->rowVector(
      {rangeVector<int32_t>(5, 102), rangeVector<int64_t>(5, 1002)});
  assertReadExpected(*rowReader, expected);
}
