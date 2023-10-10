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
#include "velox/dwio/parquet/tests/ParquetReaderTestBase.h"
#include "velox/expression/ExprToSubfieldFilter.h"

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwio::parquet;
using namespace facebook::velox::parquet;

namespace {
auto defaultPool = memory::addDefaultLeafMemoryPool();
}

class ParquetReaderTest : public ParquetReaderTestBase {
 public:
  ParquetReader createReader(
      const std::string& path,
      const facebook::velox::dwio::common::ReaderOptions& opts) {
    return ParquetReader(
        std::make_unique<BufferedInput>(
            std::make_shared<LocalReadFile>(path), opts.getMemoryPool()),
        opts);
  }

  void assertReadWithFilters(
      const std::string& fileName,
      const RowTypePtr& fileSchema,
      FilterMap filters,
      const RowVectorPtr& expected) {
    const auto filePath(getExampleFilePath(fileName));
    facebook::velox::dwio::common::ReaderOptions readerOpts{defaultPool.get()};
    auto reader = createReader(filePath, readerOpts);
    assertReadWithReaderAndFilters(
        std::make_unique<ParquetReader>(reader),
        fileName,
        fileSchema,
        std::move(filters),
        expected);
  }
};

TEST_F(ParquetReaderTest, parseSample) {
  // sample.parquet holds two columns (a: BIGINT, b: DOUBLE) and
  // 20 rows (10 rows per group). Group offsets are 153 and 614.
  // Data is in plain uncompressed format:
  //   a: [1..20]
  //   b: [1.0..20.0]
  const std::string sample(getExampleFilePath("sample.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOptions{defaultPool.get()};
  ParquetReader reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader.numberOfRows(), 20ULL);

  auto type = reader.typeWithId();
  EXPECT_EQ(type->size(), 2ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::BIGINT);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col1->type()->kind(), TypeKind::DOUBLE);
  EXPECT_EQ(type->childByName("a"), col0);
  EXPECT_EQ(type->childByName("b"), col1);

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader.createRowReader(rowReaderOpts);
  auto expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(20, 1), rangeVector<double>(20, 1)});
  assertReadExpected(sampleSchema(), *rowReader, expected, *pool_);
}

TEST_F(ParquetReaderTest, parseSampleRange1) {
  const std::string sample(getExampleFilePath("sample.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOpts{defaultPool.get()};
  ParquetReader reader = createReader(sample, readerOpts);

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  rowReaderOpts.range(0, 200);
  auto rowReader = reader.createRowReader(rowReaderOpts);
  auto expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(10, 1), rangeVector<double>(10, 1)});
  assertReadExpected(sampleSchema(), *rowReader, expected, *pool_);
}

TEST_F(ParquetReaderTest, parseSampleRange2) {
  const std::string sample(getExampleFilePath("sample.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOpts{defaultPool.get()};
  ParquetReader reader = createReader(sample, readerOpts);

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  rowReaderOpts.range(200, 500);
  auto rowReader = reader.createRowReader(rowReaderOpts);
  auto expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(10, 11), rangeVector<double>(10, 11)});
  assertReadExpected(sampleSchema(), *rowReader, expected, *pool_);
}

TEST_F(ParquetReaderTest, parseSampleEmptyRange) {
  const std::string sample(getExampleFilePath("sample.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOpts{defaultPool.get()};
  ParquetReader reader = createReader(sample, readerOpts);

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  rowReaderOpts.range(300, 10);
  auto rowReader = reader.createRowReader(rowReaderOpts);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(1000, result), 0);
}

TEST_F(ParquetReaderTest, parseReadAsLowerCase) {
  // upper.parquet holds two columns (A: BIGINT, b: BIGINT) and
  // 2 rows.
  const std::string upper(getExampleFilePath("upper.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOptions{defaultPool.get()};
  readerOptions.setFileColumnNamesReadAsLowerCase(true);
  ParquetReader reader = createReader(upper, readerOptions);
  EXPECT_EQ(reader.numberOfRows(), 2ULL);

  auto type = reader.typeWithId();
  EXPECT_EQ(type->size(), 2ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::BIGINT);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col1->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(type->childByName("a"), col0);
  EXPECT_EQ(type->childByName("b"), col1);
}

TEST_F(ParquetReaderTest, parseRowMapArrayReadAsLowerCase) {
  // upper_complex.parquet holds one row of type
  // root
  //  |-- Cc: struct (nullable = true)
  //  |    |-- CcLong0: long (nullable = true)
  //  |    |-- CcMap1: map (nullable = true)
  //  |    |    |-- key: string
  //  |    |    |-- value: struct (valueContainsNull = true)
  //  |    |    |    |-- CcArray2: array (nullable = true)
  //  |    |    |    |    |-- element: struct (containsNull = true)
  //  |    |    |    |    |    |-- CcInt3: integer (nullable = true)
  // data
  // +-----------------------+
  // |Cc                     |
  // +-----------------------+
  // |{120, {key -> {[{1}]}}}|
  // +-----------------------+
  const std::string upper(getExampleFilePath("upper_complex.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOptions{defaultPool.get()};
  readerOptions.setFileColumnNamesReadAsLowerCase(true);
  ParquetReader reader = createReader(upper, readerOptions);

  EXPECT_EQ(reader.numberOfRows(), 1ULL);

  auto type = reader.typeWithId();
  EXPECT_EQ(type->size(), 1ULL);

  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(type->childByName("cc"), col0);

  auto col0_0 = col0->childAt(0);
  EXPECT_EQ(col0_0->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(col0->childByName("cclong0"), col0_0);

  auto col0_1 = col0->childAt(1);
  EXPECT_EQ(col0_1->type()->kind(), TypeKind::MAP);
  EXPECT_EQ(col0->childByName("ccmap1"), col0_1);

  auto col0_1_0 = col0_1->childAt(0);
  EXPECT_EQ(col0_1_0->type()->kind(), TypeKind::VARCHAR);

  auto col0_1_1 = col0_1->childAt(1);
  EXPECT_EQ(col0_1_1->type()->kind(), TypeKind::ROW);

  auto col0_1_1_0 = col0_1_1->childAt(0);
  EXPECT_EQ(col0_1_1_0->type()->kind(), TypeKind::ARRAY);
  EXPECT_EQ(col0_1_1->childByName("ccarray2"), col0_1_1_0);

  auto col0_1_1_0_0 = col0_1_1_0->childAt(0);
  EXPECT_EQ(col0_1_1_0_0->type()->kind(), TypeKind::ROW);
  auto col0_1_1_0_0_0 = col0_1_1_0_0->childAt(0);
  EXPECT_EQ(col0_1_1_0_0_0->type()->kind(), TypeKind::INTEGER);
  EXPECT_EQ(col0_1_1_0_0->childByName("ccint3"), col0_1_1_0_0_0);
}

TEST_F(ParquetReaderTest, parseEmpty) {
  // empty.parquet holds two columns (a: BIGINT, b: DOUBLE) and
  // 0 rows.
  const std::string empty(getExampleFilePath("empty.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOptions{defaultPool.get()};
  ParquetReader reader = createReader(empty, readerOptions);
  EXPECT_EQ(reader.numberOfRows(), 0ULL);

  auto type = reader.typeWithId();
  EXPECT_EQ(type->size(), 2ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::BIGINT);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col1->type()->kind(), TypeKind::DOUBLE);
  EXPECT_EQ(type->childByName("a"), col0);
  EXPECT_EQ(type->childByName("b"), col1);
}

TEST_F(ParquetReaderTest, parseInt) {
  // int.parquet holds integer columns (int: INTEGER, bigint: BIGINT)
  // and 10 rows.
  // Data is in plain uncompressed format:
  //   int: [100 .. 109]
  //   bigint: [1000 .. 1009]
  const std::string sample(getExampleFilePath("int.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOpts{defaultPool.get()};
  ParquetReader reader = createReader(sample, readerOpts);

  EXPECT_EQ(reader.numberOfRows(), 10ULL);

  auto type = reader.typeWithId();
  EXPECT_EQ(type->size(), 2ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::INTEGER);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col1->type()->kind(), TypeKind::BIGINT);

  auto rowReaderOpts = getReaderOpts(intSchema());
  auto scanSpec = makeScanSpec(intSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader.createRowReader(rowReaderOpts);

  auto expected = vectorMaker_->rowVector(
      {rangeVector<int32_t>(10, 100), rangeVector<int64_t>(10, 1000)});
  assertReadExpected(intSchema(), *rowReader, expected, *pool_);
}

TEST_F(ParquetReaderTest, parseDate) {
  // date.parquet holds a single column (date: DATE) and
  // 25 rows.
  // Data is in plain uncompressed format:
  //   date: [1969-12-27 .. 1970-01-20]
  const std::string sample(getExampleFilePath("date.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOptions{defaultPool.get()};
  ParquetReader reader = createReader(sample, readerOptions);

  EXPECT_EQ(reader.numberOfRows(), 25ULL);

  auto type = reader.typeWithId();
  EXPECT_EQ(type->size(), 1ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type(), DATE());
  EXPECT_EQ(type->childByName("date"), col0);

  auto rowReaderOpts = getReaderOpts(dateSchema());
  auto scanSpec = makeScanSpec(dateSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader.createRowReader(rowReaderOpts);

  auto expected = vectorMaker_->rowVector({rangeVector<int32_t>(25, -5)});
  assertReadExpected(dateSchema(), *rowReader, expected, *pool_);
}

TEST_F(ParquetReaderTest, parseRowMapArray) {
  // sample.parquet holds one row of type (ROW(BIGINT c0, MAP(VARCHAR,
  // ARRAY(INTEGER)) c1) c)
  const std::string sample(getExampleFilePath("row_map_array.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOptions{defaultPool.get()};
  ParquetReader reader = createReader(sample, readerOptions);

  EXPECT_EQ(reader.numberOfRows(), 1ULL);

  auto type = reader.typeWithId();
  EXPECT_EQ(type->size(), 1ULL);

  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(type->childByName("c"), col0);

  auto col0_0 = col0->childAt(0);
  EXPECT_EQ(col0_0->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(col0->childByName("c0"), col0_0);

  auto col0_1 = col0->childAt(1);
  EXPECT_EQ(col0_1->type()->kind(), TypeKind::MAP);
  EXPECT_EQ(col0->childByName("c1"), col0_1);

  auto col0_1_0 = col0_1->childAt(0);
  EXPECT_EQ(col0_1_0->type()->kind(), TypeKind::VARCHAR);

  auto col0_1_1 = col0_1->childAt(1);
  EXPECT_EQ(col0_1_1->type()->kind(), TypeKind::ARRAY);

  auto col0_1_1_0 = col0_1_1->childAt(0);
  EXPECT_EQ(col0_1_1_0->type()->kind(), TypeKind::INTEGER);
}

TEST_F(ParquetReaderTest, projectNoColumns) {
  // This is the case for count(*).
  auto rowType = ROW({}, {});
  facebook::velox::dwio::common::ReaderOptions readerOpts{defaultPool.get()};
  ParquetReader reader =
      createReader(getExampleFilePath("sample.parquet"), readerOpts);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader.createRowReader(rowReaderOpts);
  auto result = BaseVector::create(rowType, 1, pool_.get());
  constexpr int kBatchSize = 100;
  ASSERT_TRUE(rowReader->next(kBatchSize, result));
  EXPECT_EQ(result->size(), 10);
  ASSERT_TRUE(rowReader->next(kBatchSize, result));
  EXPECT_EQ(result->size(), 10);
  ASSERT_FALSE(rowReader->next(kBatchSize, result));
}

TEST_F(ParquetReaderTest, parseIntDecimal) {
  // decimal_dict.parquet two columns (a: DECIMAL(7,2), b: DECIMAL(14,2)) and
  // 6 rows.
  // The physical type of the decimal columns:
  //   a: int32
  //   b: int64
  // Data is in dictionary encoding:
  //   a: [11.11, 11.11, 22.22, 22.22, 33.33, 33.33]
  //   b: [11.11, 11.11, 22.22, 22.22, 33.33, 33.33]
  auto rowType = ROW({"a", "b"}, {DECIMAL(7, 2), DECIMAL(14, 2)});
  facebook::velox::dwio::common::ReaderOptions readerOpts{defaultPool.get()};
  const std::string decimal_dict(getExampleFilePath("decimal_dict.parquet"));

  ParquetReader reader = createReader(decimal_dict, readerOpts);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader.createRowReader(rowReaderOpts);

  EXPECT_EQ(reader.numberOfRows(), 6ULL);

  auto type = reader.typeWithId();
  EXPECT_EQ(type->size(), 2ULL);
  auto col0 = type->childAt(0);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col0->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(col1->type()->kind(), TypeKind::BIGINT);

  int64_t expectValues[3] = {1111, 2222, 3333};
  auto result = BaseVector::create(rowType, 1, pool_.get());
  rowReader->next(6, result);
  EXPECT_EQ(result->size(), 6ULL);
  auto decimals = result->as<RowVector>();
  auto a = decimals->childAt(0)->asFlatVector<int64_t>()->rawValues();
  auto b = decimals->childAt(1)->asFlatVector<int64_t>()->rawValues();
  for (int i = 0; i < 3; i++) {
    int index = 2 * i;
    EXPECT_EQ(a[index], expectValues[i]);
    EXPECT_EQ(a[index + 1], expectValues[i]);
    EXPECT_EQ(b[index], expectValues[i]);
    EXPECT_EQ(b[index + 1], expectValues[i]);
  }
}

TEST_F(ParquetReaderTest, readSampleBigintRangeFilter) {
  // Read sample.parquet with the int filter "a BETWEEN 16 AND 20".
  FilterMap filters;
  filters.insert({"a", exec::between(16, 20)});

  auto expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(5, 16), rangeVector<double>(5, 16)});
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, readSampleBigintValuesUsingBitmaskFilter) {
  // Read sample.parquet with the int filter "a in 16, 17, 18, 19, 20".
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
  // Read sample.parquet with the int filter "a = 16".
  FilterMap filters;
  filters.insert({"a", exec::equal(16)});

  auto expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(1, 16), rangeVector<double>(1, 16)});
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, dateFilters) {
  // Read date.parquet with the date filter "date BETWEEN 5 AND 14".
  FilterMap filters;
  filters.insert({"date", exec::between(5, 14)});

  auto expected = vectorMaker_->rowVector({rangeVector<int32_t>(10, 5)});
  assertReadWithFilters(
      "date.parquet", dateSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, intMultipleFilters) {
  // Filter int BETWEEN 102 AND 120 AND bigint BETWEEN 900 AND 1006.
  FilterMap filters;
  filters.insert({"int", exec::between(102, 120)});
  filters.insert({"bigint", exec::between(900, 1006)});

  auto expected = vectorMaker_->rowVector(
      {rangeVector<int32_t>(5, 102), rangeVector<int64_t>(5, 1002)});

  assertReadWithFilters(
      "int.parquet", intSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, doubleFilters) {
  // Read sample.parquet with the double filter "b < 10.0".
  FilterMap filters;
  filters.insert({"b", exec::lessThanDouble(10.0)});

  auto expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(9, 1), rangeVector<double>(9, 1)});
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);

  // Test "b <= 10.0".
  filters.insert({"b", exec::lessThanOrEqualDouble(10.0)});
  expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(10, 1), rangeVector<double>(10, 1)});
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);

  // Test "b between 10.0 and 14.0".
  filters.insert({"b", exec::betweenDouble(10.0, 14.0)});
  expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(5, 10), rangeVector<double>(5, 10)});
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);

  // Test "b > 14.0".
  filters.insert({"b", exec::greaterThanDouble(14.0)});
  expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(6, 15), rangeVector<double>(6, 15)});
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);

  // Test "b >= 14.0".
  filters.insert({"b", exec::greaterThanOrEqualDouble(14.0)});
  expected = vectorMaker_->rowVector(
      {rangeVector<int64_t>(7, 14), rangeVector<double>(7, 14)});
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, varcharFilters) {
  // Test "name < 'CANADA'".
  FilterMap filters;
  filters.insert({"name", exec::lessThan("CANADA")});

  auto expected = vectorMaker_->rowVector({
      vectorMaker_->flatVector<int64_t>({0, 1, 2}),
      vectorMaker_->flatVector({"ALGERIA", "ARGENTINA", "BRAZIL"}),
      vectorMaker_->flatVector<int64_t>({0, 1, 1}),
  });

  auto rowType =
      ROW({"nationkey", "name", "regionkey"}, {BIGINT(), VARCHAR(), BIGINT()});

  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // Test "name <= 'CANADA'".
  filters.insert({"name", exec::lessThanOrEqual("CANADA")});
  expected = vectorMaker_->rowVector({
      vectorMaker_->flatVector<int64_t>({0, 1, 2, 3}),
      vectorMaker_->flatVector({"ALGERIA", "ARGENTINA", "BRAZIL", "CANADA"}),
      vectorMaker_->flatVector<int64_t>({0, 1, 1, 1}),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // Test "name > UNITED KINGDOM".
  filters.insert({"name", exec::greaterThan("UNITED KINGDOM")});
  expected = vectorMaker_->rowVector({
      vectorMaker_->flatVector<int64_t>({21, 24}),
      vectorMaker_->flatVector({"VIETNAM", "UNITED STATES"}),
      vectorMaker_->flatVector<int64_t>({2, 1}),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // Test "name >= 'UNITED KINGDOM'".
  filters.insert({"name", exec::greaterThanOrEqual("UNITED KINGDOM")});
  expected = vectorMaker_->rowVector({
      vectorMaker_->flatVector<int64_t>({21, 23, 24}),
      vectorMaker_->flatVector({"VIETNAM", "UNITED KINGDOM", "UNITED STATES"}),
      vectorMaker_->flatVector<int64_t>({2, 3, 1}),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // Test "name = 'CANADA'".
  filters.insert({"name", exec::equal("CANADA")});
  expected = vectorMaker_->rowVector({
      vectorMaker_->flatVector<int64_t>({3}),
      vectorMaker_->flatVector({"CANADA"}),
      vectorMaker_->flatVector<int64_t>({1}),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // Test "name IN ('CANADA', 'UNITED KINGDOM')".
  filters.insert({"name", exec::in({std::string("CANADA"), "UNITED KINGDOM"})});
  expected = vectorMaker_->rowVector({
      vectorMaker_->flatVector<int64_t>({3, 23}),
      vectorMaker_->flatVector({"CANADA", "UNITED KINGDOM"}),
      vectorMaker_->flatVector<int64_t>({1, 3}),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);

  // Test "name IN ('UNITED STATES', 'CANADA', 'INDIA', 'RUSSIA')".
  filters.insert(
      {"name",
       exec::in({std::string("UNITED STATES"), "INDIA", "CANADA", "RUSSIA"})});
  expected = vectorMaker_->rowVector({
      vectorMaker_->flatVector<int64_t>({3, 8, 22, 24}),
      vectorMaker_->flatVector({"CANADA", "INDIA", "RUSSIA", "UNITED STATES"}),
      vectorMaker_->flatVector<int64_t>({1, 2, 3, 1}),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);
}

// This test is to verify filterRowGroups() doesn't throw the fileOffset Velox
// check failure
TEST_F(ParquetReaderTest, filterRowGroups) {
  // decimal_no_ColumnMetadata.parquet has one columns a: DECIMAL(9,1). It
  // doesn't have ColumnMetaData, and rowGroups_[0].columns[0].file_offset is 0.
  auto rowType = ROW({"_c0"}, {DECIMAL(9, 1)});
  facebook::velox::dwio::common::ReaderOptions readerOpts{defaultPool.get()};
  const std::string decimal_dict(
      getExampleFilePath("decimal_no_ColumnMetadata.parquet"));

  ParquetReader reader = createReader(decimal_dict, readerOpts);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader.createRowReader(rowReaderOpts);

  EXPECT_EQ(reader.numberOfRows(), 10ULL);
}

TEST_F(ParquetReaderTest, parseLongTagged) {
  // This is a case for long with annonation read
  const std::string sample(getExampleFilePath("tagged_long.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOptions{defaultPool.get()};
  ParquetReader reader = createReader(sample, readerOptions);

  EXPECT_EQ(reader.numberOfRows(), 4ULL);

  auto type = reader.typeWithId();
  EXPECT_EQ(type->size(), 1ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(type->childByName("_c0"), col0);
}

TEST_F(ParquetReaderTest, preloadSmallFile) {
  const std::string sample(getExampleFilePath("sample.parquet"));

  auto file = std::make_shared<LocalReadFile>(sample);
  auto input = std::make_unique<BufferedInput>(file, *defaultPool);

  facebook::velox::dwio::common::ReaderOptions readerOptions{defaultPool.get()};
  auto reader =
      std::make_unique<ParquetReader>(std::move(input), readerOptions);

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);

  // Ensure the input is small parquet file.
  const auto fileSize = file->size();
  ASSERT_TRUE(
      fileSize <= facebook::velox::dwio::common::ReaderOptions::
                      kDefaultFilePreloadThreshold ||
      fileSize <= facebook::velox::dwio::common::ReaderOptions::
                      kDefaultDirectorySizeGuess);

  // Check the whole file already loaded.
  ASSERT_EQ(file->bytesRead(), fileSize);

  // Reset bytes read to check for duplicate reads.
  file->resetBytesRead();

  constexpr int kBatchSize = 10;
  auto result = BaseVector::create(sampleSchema(), 1, pool_.get());
  while (rowReader->next(kBatchSize, result)) {
    // Check no duplicate reads.
    ASSERT_EQ(file->bytesRead(), 0);
  }
}

TEST_F(ParquetReaderTest, prefetchRowGroups) {
  auto rowType = ROW({"id"}, {BIGINT()});
  const std::string sample(getExampleFilePath("multiple_row_groups.parquet"));
  const int numRowGroups = 4;

  facebook::velox::dwio::common::ReaderOptions readerOptions{defaultPool.get()};
  // Disable preload of file.
  readerOptions.setFilePreloadThreshold(0);

  // Test different number of prefetch row groups.
  // 2: Less than total number of row groups.
  // 4: Exactly as total number of row groups.
  // 10: More than total number of row groups.
  const std::vector<int> numPrefetchRowGroups{
      facebook::velox::dwio::common::ReaderOptions::kDefaultPrefetchRowGroups,
      2,
      4,
      10};
  for (auto numPrefetch : numPrefetchRowGroups) {
    readerOptions.setPrefetchRowGroups(numPrefetch);

    ParquetReader reader = createReader(sample, readerOptions);
    EXPECT_EQ(reader.numberOfRowGroups(), numRowGroups);

    RowReaderOptions rowReaderOpts;
    rowReaderOpts.setScanSpec(makeScanSpec(rowType));
    auto rowReader = reader.createRowReader(rowReaderOpts);
    auto parquetRowReader = dynamic_cast<ParquetRowReader*>(rowReader.get());

    constexpr int kBatchSize = 1000;
    auto result = BaseVector::create(rowType, kBatchSize, pool_.get());

    for (int i = 0; i < numRowGroups; i++) {
      if (i > 0) {
        // If it's not the first row group, check if the previous row group has
        // been evicted.
        EXPECT_FALSE(parquetRowReader->isRowGroupBuffered(i - 1));
      }
      EXPECT_TRUE(parquetRowReader->isRowGroupBuffered(i));
      if (i < numRowGroups - 1) {
        // If it's not the last row group, check if the configured number of
        // row groups have been prefetched.
        for (int j = 1; j <= numPrefetch && i + j < numRowGroups; j++) {
          EXPECT_TRUE(parquetRowReader->isRowGroupBuffered(i + j));
        }
      }

      // Read current row group.
      auto actualRows = parquetRowReader->next(kBatchSize, result);
      // kBatchSize should be large enough to hold the entire row group.
      EXPECT_LE(actualRows, kBatchSize);
      // Advance to the next row group.
      parquetRowReader->nextRowNumber();
    }
  }
}
