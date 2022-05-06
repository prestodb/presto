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
#include "velox/dwio/dwrf/test/utils/DataFiles.h"
#include "velox/type/Filter.h"
#include "velox/type/Type.h"
#include "velox/type/tests/FilterBuilder.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/VectorMaker.h"

#include <fmt/core.h>
#include <gtest/gtest.h>
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

  RowReaderOptions getReaderOpts(const RowTypePtr& rowType) {
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(
        std::make_shared<ColumnSelector>(rowType, rowType->names()));

    return rowReaderOpts;
  }

  static RowTypePtr sampleSchema() {
    return ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  }

  static RowTypePtr dateSchema() {
    return ROW({"date"}, {DATE()});
  }

  static RowTypePtr intSchema() {
    return ROW({"int", "bigint"}, {INTEGER(), BIGINT()});
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
      if (part > 0) {
        assertEqualVectorPart(expected, result, total);
        total += part;
      } else {
        break;
      }
    }
    EXPECT_EQ(total, expected->size());
    EXPECT_EQ(reader.next(1000, result), 0);
  }

  std::shared_ptr<common::ScanSpec> makeScanSpec(const RowTypePtr& rowType) {
    auto scanSpec = std::make_shared<common::ScanSpec>("");

    for (auto i = 0; i < rowType->size(); ++i) {
      auto child =
          scanSpec->getOrCreateChild(common::Subfield(rowType->nameOf(i)));
      child->setProjectOut(true);
      child->setChannel(i);
    }

    return scanSpec;
  }

  using FilterMap =
      std::unordered_map<std::string, std::unique_ptr<common::Filter>>;

  void assertReadWithFilters(
      const std::string& fileName,
      const RowTypePtr& fileSchema,
      FilterMap filters,
      const RowVectorPtr& expected) {
    const auto filePath(getExampleFilePath(fileName));

    ReaderOptions readerOptions;
    ParquetReader reader(
        std::make_unique<FileInputStream>(filePath), readerOptions);

    auto scanSpec = makeScanSpec(fileSchema);
    for (auto&& [column, filter] : filters) {
      scanSpec->getOrCreateChild(common::Subfield(column))
          ->setFilter(std::move(filter));
    }

    auto rowReaderOpts = getReaderOpts(fileSchema);
    rowReaderOpts.setScanSpec(scanSpec);
    auto rowReader = reader.createRowReader(rowReaderOpts);
    assertReadExpected(*rowReader, expected);
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

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
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

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
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

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
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

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  rowReaderOpts.range(300, 10);
  auto rowReader = reader.createRowReader(rowReaderOpts);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(1000, result), 0);
}

TEST_F(ParquetReaderTest, readSampleBigintRangeFilter) {
  // a BETWEEN 16 AND 20
  FilterMap filters;
  filters.insert({"a", common::test::between(16, 20)});

  auto expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(5, 16), rangeVector<double>(5, 16)});

  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, readSampleBigintValuesUsingBitmaskFilter) {
  // a in 16, 17, 18, 19, 20.
  std::vector<int64_t> values{16, 17, 18, 19, 20};
  auto bigintBitmaskFilter =
      std::make_unique<facebook::velox::common::BigintValuesUsingBitmask>(
          16, 20, std::move(values), false);
  FilterMap filters;
  filters.insert({"a", std::move(bigintBitmaskFilter)});

  auto expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(5, 16), rangeVector<double>(5, 16)});

  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, readSampleEqualFilter) {
  // a = 16
  FilterMap filters;
  filters.insert({"a", common::test::equal(16)});

  auto expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(1, 16), rangeVector<double>(1, 16)});

  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
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

  auto rowReaderOpts = getReaderOpts(dateSchema());
  auto scanSpec = makeScanSpec(dateSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader.createRowReader(rowReaderOpts);

  auto expected = vectorMaker_->rowVector({rangeVector<Date>(25, -5)});
  assertReadExpected(*rowReader, expected);
}

TEST_F(ParquetReaderTest, dateFilter) {
  // date BETWEEN 5 AND 14
  FilterMap filters;
  filters.insert({"date", common::test::between(5, 14)});

  auto expected = vectorMaker_->rowVector({rangeVector<Date>(10, 5)});

  assertReadWithFilters(
      "date.parquet", dateSchema(), std::move(filters), expected);
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

  auto rowReaderOpts = getReaderOpts(intSchema());
  auto scanSpec = makeScanSpec(intSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader.createRowReader(rowReaderOpts);

  auto expected = vectorMaker_->rowVector(
      {rangeVector<int32_t>(10, 100), rangeVector<int64_t>(10, 1000)});
  assertReadExpected(*rowReader, expected);
}

TEST_F(ParquetReaderTest, intMultipleFilters) {
  // int BETWEEN 102 AND 120 AND bigint BETWEEN 900 AND 1006
  FilterMap filters;
  filters.insert({"int", common::test::between(102, 120)});
  filters.insert({"bigint", common::test::between(900, 1006)});

  auto expected = vectorMaker_->rowVector(
      {rangeVector<int32_t>(5, 102), rangeVector<int64_t>(5, 1002)});

  assertReadWithFilters(
      "int.parquet", intSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, doubleFilters) {
  // b < 10.0
  FilterMap filters;
  filters.insert({"b", common::test::lessThanDouble(10.0)});

  auto expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(9, 1), rangeVector<double>(9, 1)});

  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);

  // b <= 10.0
  filters.insert({"b", common::test::lessThanOrEqualDouble(10.0)});

  expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(10, 1), rangeVector<double>(10, 1)});

  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);

  // b between 10.0 and 14.0
  filters.insert({"b", common::test::betweenDouble(10.0, 14.0)});

  expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(5, 10), rangeVector<double>(5, 10)});

  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);

  // b > 14.0
  filters.insert({"b", common::test::greaterThanDouble(14.0)});

  expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(6, 15), rangeVector<double>(6, 15)});

  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);

  // b >= 14.0
  filters.insert({"b", common::test::greaterThanOrEqualDouble(14.0)});

  expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(7, 14), rangeVector<double>(7, 14)});

  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, varcharFilters) {
  // name < 'CANADA'
  FilterMap filters;
  filters.insert({"name", common::test::lessThan("CANADA")});

  auto expected = vectorMaker_->rowVector({
      vectorMaker_->flatVector<int64_t>({0, 1, 2}),
      vectorMaker_->flatVector({"ALGERIA", "ARGENTINA", "BRAZIL"}),
      vectorMaker_->flatVector<int64_t>({0, 1, 1}),
  });

  auto rowType =
      ROW({"nationkey", "name", "regionkey"}, {BIGINT(), VARCHAR(), BIGINT()});

  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // name <= 'CANADA'
  filters.insert({"name", common::test::lessThanOrEqual("CANADA")});

  expected = vectorMaker_->rowVector({
      vectorMaker_->flatVector<int64_t>({0, 1, 2, 3}),
      vectorMaker_->flatVector({"ALGERIA", "ARGENTINA", "BRAZIL", "CANADA"}),
      vectorMaker_->flatVector<int64_t>({0, 1, 1, 1}),
  });

  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // name > UNITED KINGDOM
  filters.insert({"name", common::test::greaterThan("UNITED KINGDOM")});

  expected = vectorMaker_->rowVector({
      vectorMaker_->flatVector<int64_t>({21, 24}),
      vectorMaker_->flatVector({"VIETNAM", "UNITED STATES"}),
      vectorMaker_->flatVector<int64_t>({2, 1}),
  });

  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // name >= UNITED KINGDOM
  filters.insert({"name", common::test::greaterThanOrEqual("UNITED KINGDOM")});

  expected = vectorMaker_->rowVector({
      vectorMaker_->flatVector<int64_t>({21, 23, 24}),
      vectorMaker_->flatVector({"VIETNAM", "UNITED KINGDOM", "UNITED STATES"}),
      vectorMaker_->flatVector<int64_t>({2, 3, 1}),
  });

  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // name = 'CANADA'
  filters.insert({"name", common::test::equal("CANADA")});

  expected = vectorMaker_->rowVector({
      vectorMaker_->flatVector<int64_t>({3}),
      vectorMaker_->flatVector({"CANADA"}),
      vectorMaker_->flatVector<int64_t>({1}),
  });

  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // name IN ('CANADA', 'UNITED KINGDOM')
  filters.insert(
      {"name", common::test::in({std::string("CANADA"), "UNITED KINGDOM"})});

  expected = vectorMaker_->rowVector({
      vectorMaker_->flatVector<int64_t>({3, 23}),
      vectorMaker_->flatVector({"CANADA", "UNITED KINGDOM"}),
      vectorMaker_->flatVector<int64_t>({1, 3}),
  });

  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // name IN ('UNITED STATES', 'CANADA', 'INDIA', 'RUSSIA')
  filters.insert(
      {"name",
       common::test::in(
           {std::string("UNITED STATES"), "INDIA", "CANADA", "RUSSIA"})});

  expected = vectorMaker_->rowVector({
      vectorMaker_->flatVector<int64_t>({3, 8, 22, 24}),
      vectorMaker_->flatVector({"CANADA", "INDIA", "RUSSIA", "UNITED STATES"}),
      vectorMaker_->flatVector<int64_t>({1, 2, 3, 1}),
  });

  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);
}
