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

#include "velox/dwio/parquet/tests/ParquetTestBase.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;

class ParquetReaderTest : public ParquetTestBase {
 public:
  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const std::string& fileName,
      const RowTypePtr& rowType) {
    const std::string sample(getExampleFilePath(fileName));

    facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
    auto reader = createReader(sample, readerOptions);

    RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(
        std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
            rowType, rowType->names()));
    rowReaderOpts.setScanSpec(makeScanSpec(rowType));
    auto rowReader = reader->createRowReader(rowReaderOpts);
    return rowReader;
  }

  void assertReadWithExpected(
      const std::string& fileName,
      const RowTypePtr& rowType,
      const RowVectorPtr& expected) {
    auto rowReader = createRowReader(fileName, rowType);
    assertReadWithReaderAndExpected(rowType, *rowReader, expected, *pool_);
  }

  void assertReadWithFilters(
      const std::string& fileName,
      const RowTypePtr& fileSchema,
      FilterMap filters,
      const RowVectorPtr& expected) {
    const auto filePath(getExampleFilePath(fileName));
    facebook::velox::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
    auto reader = createReader(filePath, readerOpts);
    assertReadWithReaderAndFilters(
        std::move(reader), fileName, fileSchema, std::move(filters), expected);
  }
};

TEST_F(ParquetReaderTest, parseSample) {
  // sample.parquet holds two columns (a: BIGINT, b: DOUBLE) and
  // 20 rows (10 rows per group). Group offsets are 153 and 614.
  // Data is in plain uncompressed format:
  //   a: [1..20]
  //   b: [1.0..20.0]
  const std::string sample(getExampleFilePath("sample.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 20ULL);

  auto type = reader->typeWithId();
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
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto expected = makeRowVector({
      makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
      makeFlatVector<double>(20, [](auto row) { return row + 1; }),
  });

  assertReadWithReaderAndExpected(
      sampleSchema(), *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, parseUnannotatedList) {
  // unannotated_list.parquet has the following the schema
  // the list is defined without the middle layer
  // message ParquetSchema {
  //   optional group self (LIST) {
  //     repeated group self_tuple {
  //       optional int64 a;
  //       optional boolean b;
  //       required binary c (STRING);
  //     }
  //   }
  // }
  const std::string sample(getExampleFilePath("unannotated_list.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto reader = createReader(sample, readerOpts);

  EXPECT_EQ(reader->numberOfRows(), 22ULL);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 1ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::ARRAY);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(col0)->name_, "self");

  EXPECT_EQ(col0->size(), 1ULL);
  EXPECT_EQ(col0->childAt(0)->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(col0->childAt(0))
          ->name_,
      "dummy");

  EXPECT_EQ(col0->childAt(0)->childAt(0)->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(
          col0->childAt(0)->childAt(0))
          ->name_,
      "a");

  EXPECT_EQ(col0->childAt(0)->childAt(1)->type()->kind(), TypeKind::BOOLEAN);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(
          col0->childAt(0)->childAt(1))
          ->name_,
      "b");

  EXPECT_EQ(col0->childAt(0)->childAt(2)->type()->kind(), TypeKind::VARCHAR);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(
          col0->childAt(0)->childAt(2))
          ->name_,
      "c");
}

TEST_F(ParquetReaderTest, parseUnannotatedMap) {
  // unannotated_map.parquet has the following the schema
  // the map is defined with a MAP_KEY_VALUE node
  // message hive_schema {
  // optional group test (MAP) {
  //   repeated group key_value (MAP_KEY_VALUE) {
  //       required binary key (STRING);
  //       optional int64 value;
  //   }
  //  }
  //}
  const std::string filename("unnotated_map.parquet");
  const std::string sample(getExampleFilePath(filename));

  facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 1ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::MAP);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(col0)->name_, "test");

  EXPECT_EQ(col0->size(), 2ULL);
  EXPECT_EQ(col0->childAt(0)->type()->kind(), TypeKind::VARCHAR);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(col0->childAt(0))
          ->name_,
      "key");

  EXPECT_EQ(col0->childAt(1)->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(col0->childAt(1))
          ->name_,
      "value");
}

TEST_F(ParquetReaderTest, parseLegacyListWithMultipleChildren) {
  // listmultiplechildren.parquet has the following the schema
  // message hive_schema {
  //  optional group test (LIST) {
  //    repeated group array {
  //      optional int64 a;
  //      optional boolean b;
  //      optional binary c (STRING);
  //    }
  //  }
  // }
  // Namely, node 'array' has >1 child
  const std::string filename("listmultiplechildren.parquet");
  const std::string sample(getExampleFilePath(filename));

  facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 1ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::ARRAY);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(col0)->name_, "test");

  EXPECT_EQ(col0->size(), 1ULL);
  EXPECT_EQ(col0->childAt(0)->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(col0->childAt(0))
          ->name_,
      "dummy");

  EXPECT_EQ(col0->childAt(0)->childAt(0)->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(
          col0->childAt(0)->childAt(0))
          ->name_,
      "a");

  EXPECT_EQ(col0->childAt(0)->childAt(1)->type()->kind(), TypeKind::BOOLEAN);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(
          col0->childAt(0)->childAt(1))
          ->name_,
      "b");

  EXPECT_EQ(col0->childAt(0)->childAt(2)->type()->kind(), TypeKind::VARCHAR);
  EXPECT_EQ(
      std::static_pointer_cast<const ParquetTypeWithId>(
          col0->childAt(0)->childAt(2))
          ->name_,
      "c");
}

TEST_F(ParquetReaderTest, parseSampleRange1) {
  const std::string sample(getExampleFilePath("sample.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto reader = createReader(sample, readerOpts);

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  rowReaderOpts.range(0, 200);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto expected = makeRowVector({
      makeFlatVector<int64_t>(10, [](auto row) { return row + 1; }),
      makeFlatVector<double>(10, [](auto row) { return row + 1; }),
  });
  assertReadWithReaderAndExpected(
      sampleSchema(), *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, parseSampleRange2) {
  const std::string sample(getExampleFilePath("sample.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto reader = createReader(sample, readerOpts);

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  rowReaderOpts.range(200, 500);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto expected = makeRowVector({
      makeFlatVector<int64_t>(10, [](auto row) { return row + 11; }),
      makeFlatVector<double>(10, [](auto row) { return row + 11; }),
  });
  assertReadWithReaderAndExpected(
      sampleSchema(), *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, parseSampleEmptyRange) {
  const std::string sample(getExampleFilePath("sample.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto reader = createReader(sample, readerOpts);

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  rowReaderOpts.range(300, 10);
  auto rowReader = reader->createRowReader(rowReaderOpts);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(1000, result), 0);
}

TEST_F(ParquetReaderTest, parseReadAsLowerCase) {
  // upper.parquet holds two columns (A: BIGINT, b: BIGINT) and
  // 2 rows.
  const std::string upper(getExampleFilePath("upper.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  readerOptions.setFileColumnNamesReadAsLowerCase(true);
  auto reader = createReader(upper, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 2ULL);

  auto type = reader->typeWithId();
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

  facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  readerOptions.setFileColumnNamesReadAsLowerCase(true);
  auto reader = createReader(upper, readerOptions);

  EXPECT_EQ(reader->numberOfRows(), 1ULL);

  auto type = reader->typeWithId();
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

  facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(empty, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 0ULL);

  auto type = reader->typeWithId();
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

  facebook::velox::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto reader = createReader(sample, readerOpts);

  EXPECT_EQ(reader->numberOfRows(), 10ULL);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 2ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::INTEGER);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col1->type()->kind(), TypeKind::BIGINT);

  auto rowReaderOpts = getReaderOpts(intSchema());
  auto scanSpec = makeScanSpec(intSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = makeRowVector({
      makeFlatVector<int32_t>(10, [](auto row) { return row + 100; }),
      makeFlatVector<int64_t>(10, [](auto row) { return row + 1000; }),
  });
  assertReadWithReaderAndExpected(
      intSchema(), *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, parseUnsignedInt1) {
  // uint.parquet holds unsigned integer columns (uint8: TINYINT, uint16:
  // SMALLINT, uint32: INTEGER, uint64: BIGINT) and 3 rows. Data is in plain
  // uncompressed format:
  //   uint8: [255, 3, 3]
  //   uint16: [65535, 2000, 3000]
  //   uint32: [4294967295, 2000000000, 3000000000]
  //   uint64: [18446744073709551615, 2000000000000000000, 3000000000000000000]
  const std::string sample(getExampleFilePath("uint.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);

  EXPECT_EQ(reader->numberOfRows(), 3ULL);
  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 4ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::TINYINT);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col1->type()->kind(), TypeKind::SMALLINT);
  auto col2 = type->childAt(2);
  EXPECT_EQ(col2->type()->kind(), TypeKind::INTEGER);
  auto col3 = type->childAt(3);
  EXPECT_EQ(col3->type()->kind(), TypeKind::BIGINT);

  auto rowType =
      ROW({"uint8", "uint16", "uint32", "uint64"},
          {TINYINT(), SMALLINT(), INTEGER(), BIGINT()});

  RowReaderOptions rowReaderOpts;
  rowReaderOpts.select(
      std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
          rowType, rowType->names()));
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = makeRowVector(
      {makeFlatVector<uint8_t>({255, 2, 3}),
       makeFlatVector<uint16_t>({65535, 2000, 3000}),
       makeFlatVector<uint32_t>({4294967295, 2000000000, 3000000000}),
       makeFlatVector<uint64_t>(
           {18446744073709551615ULL,
            2000000000000000000ULL,
            3000000000000000000ULL})});
  assertReadWithReaderAndExpected(rowType, *rowReader, expected, *pool_);
}

TEST_F(ParquetReaderTest, parseUnsignedInt2) {
  auto rowType =
      ROW({"uint8", "uint16", "uint32", "uint64"},
          {SMALLINT(), SMALLINT(), INTEGER(), BIGINT()});
  auto expected = makeRowVector(
      {makeFlatVector<uint16_t>({255, 2, 3}),
       makeFlatVector<uint16_t>({65535, 2000, 3000}),
       makeFlatVector<uint32_t>({4294967295, 2000000000, 3000000000}),
       makeFlatVector<uint64_t>(
           {18446744073709551615ULL,
            2000000000000000000ULL,
            3000000000000000000ULL})});
  assertReadWithExpected("uint.parquet", rowType, expected);
}

TEST_F(ParquetReaderTest, parseUnsignedInt3) {
  auto rowType =
      ROW({"uint8", "uint16", "uint32", "uint64"},
          {SMALLINT(), INTEGER(), INTEGER(), BIGINT()});
  auto expected = makeRowVector(
      {makeFlatVector<uint16_t>({255, 2, 3}),
       makeFlatVector<uint32_t>({65535, 2000, 3000}),
       makeFlatVector<uint32_t>({4294967295, 2000000000, 3000000000}),
       makeFlatVector<uint64_t>(
           {18446744073709551615ULL,
            2000000000000000000ULL,
            3000000000000000000ULL})});
  assertReadWithExpected("uint.parquet", rowType, expected);
}

TEST_F(ParquetReaderTest, parseUnsignedInt4) {
  auto rowType =
      ROW({"uint8", "uint16", "uint32", "uint64"},
          {SMALLINT(), INTEGER(), INTEGER(), DECIMAL(20, 0)});
  auto expected = makeRowVector(
      {makeFlatVector<uint16_t>({255, 2, 3}),
       makeFlatVector<uint32_t>({65535, 2000, 3000}),
       makeFlatVector<uint32_t>({4294967295, 2000000000, 3000000000}),
       makeFlatVector<uint128_t>(
           {18446744073709551615ULL,
            2000000000000000000ULL,
            3000000000000000000ULL})});
  assertReadWithExpected("uint.parquet", rowType, expected);
}

TEST_F(ParquetReaderTest, parseUnsignedInt5) {
  auto rowType =
      ROW({"uint8", "uint16", "uint32", "uint64"},
          {SMALLINT(), INTEGER(), BIGINT(), DECIMAL(20, 0)});
  auto expected = makeRowVector(
      {makeFlatVector<uint16_t>({255, 2, 3}),
       makeFlatVector<uint32_t>({65535, 2000, 3000}),
       makeFlatVector<uint64_t>({4294967295, 2000000000, 3000000000}),
       makeFlatVector<uint128_t>(
           {18446744073709551615ULL,
            2000000000000000000ULL,
            3000000000000000000ULL})});
  assertReadWithExpected("uint.parquet", rowType, expected);
}

TEST_F(ParquetReaderTest, parseDate) {
  // date.parquet holds a single column (date: DATE) and
  // 25 rows.
  // Data is in plain uncompressed format:
  //   date: [1969-12-27 .. 1970-01-20]
  const std::string sample(getExampleFilePath("date.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);

  EXPECT_EQ(reader->numberOfRows(), 25ULL);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 1ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type(), DATE());
  EXPECT_EQ(type->childByName("date"), col0);

  auto rowReaderOpts = getReaderOpts(dateSchema());
  auto scanSpec = makeScanSpec(dateSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto expected = makeRowVector({
      makeFlatVector<int32_t>(25, [](auto row) { return row - 5; }),
  });
  assertReadWithReaderAndExpected(
      dateSchema(), *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, parseRowMapArray) {
  // sample.parquet holds one row of type (ROW(BIGINT c0, MAP(VARCHAR,
  // ARRAY(INTEGER)) c1) c)
  const std::string sample(getExampleFilePath("row_map_array.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);

  EXPECT_EQ(reader->numberOfRows(), 1ULL);

  auto type = reader->typeWithId();
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
  facebook::velox::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto reader = createReader(getExampleFilePath("sample.parquet"), readerOpts);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto result = BaseVector::create(rowType, 1, leafPool_.get());
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
  facebook::velox::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  const std::string decimal_dict(getExampleFilePath("decimal_dict.parquet"));

  auto reader = createReader(decimal_dict, readerOpts);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  EXPECT_EQ(reader->numberOfRows(), 6ULL);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 2ULL);
  auto col0 = type->childAt(0);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col0->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(col1->type()->kind(), TypeKind::BIGINT);

  int64_t expectValues[3] = {1111, 2222, 3333};
  auto result = BaseVector::create(rowType, 1, leafPool_.get());
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

TEST_F(ParquetReaderTest, parseMapKeyValueAsMap) {
  // map_key_value.parquet holds a single map column (key: VARCHAR, b: BIGINT)
  // and 1 row that contains 8 map entries. It is with older version of Parquet
  // and uses MAP_KEY_VALUE instead of MAP as the map SchemaElement
  // converted_type. It has 5 SchemaElements in the schema, in the format of
  // schemaIdx: <repetition> <type> name (<converted type>):
  //
  // 0: REQUIRED BOOLEAN hive_schema (UTF8)
  // 1:   OPTIONAL BOOLEAN test (MAP_KEY_VALUE)
  // 2:     REPEATED BOOLEAN map (UTF8)
  // 3:       REQUIRED BYTE_ARRAY key (UTF8)
  // 4:       OPTIONAL INT64 value (UTF8)

  const std::string sample(getExampleFilePath("map_key_value.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 1ULL);

  auto rowType = reader->typeWithId();
  EXPECT_EQ(rowType->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(rowType->size(), 1ULL);

  auto mapColumnType = rowType->childAt(0);
  EXPECT_EQ(mapColumnType->type()->kind(), TypeKind::MAP);

  auto mapKeyType = mapColumnType->childAt(0);
  EXPECT_EQ(mapKeyType->type()->kind(), TypeKind::VARCHAR);

  auto mapValueType = mapColumnType->childAt(1);
  EXPECT_EQ(mapValueType->type()->kind(), TypeKind::BIGINT);

  auto fileSchema =
      ROW({"test"}, {createType<TypeKind::MAP>({VARCHAR(), BIGINT()})});
  auto rowReaderOpts = getReaderOpts(fileSchema);
  auto scanSpec = makeScanSpec(fileSchema);
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = makeRowVector({vectorMaker_.mapVector<std::string, int64_t>(
      {{{"0", 0},
        {"1", 1},
        {"2", 2},
        {"3", 3},
        {"4", 4},
        {"5", 5},
        {"6", 6},
        {"7", 7}}})});

  assertReadWithReaderAndExpected(fileSchema, *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, readSampleBigintRangeFilter) {
  // Read sample.parquet with the int filter "a BETWEEN 16 AND 20".
  FilterMap filters;
  filters.insert({"a", exec::between(16, 20)});
  auto expected = makeRowVector({
      makeFlatVector<int64_t>(5, [](auto row) { return row + 16; }),
      makeFlatVector<double>(5, [](auto row) { return row + 16; }),
  });
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
  auto expected = makeRowVector({
      makeFlatVector<int64_t>(5, [](auto row) { return row + 16; }),
      makeFlatVector<double>(5, [](auto row) { return row + 16; }),
  });
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, readSampleEqualFilter) {
  // Read sample.parquet with the int filter "a = 16".
  FilterMap filters;
  filters.insert({"a", exec::equal(16)});

  auto expected = makeRowVector({
      makeFlatVector<int64_t>(1, [](auto row) { return row + 16; }),
      makeFlatVector<double>(1, [](auto row) { return row + 16; }),
  });

  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, dateFilters) {
  // Read date.parquet with the date filter "date BETWEEN 5 AND 14".
  FilterMap filters;
  filters.insert({"date", exec::between(5, 14)});

  auto expected = makeRowVector({
      makeFlatVector<int32_t>(10, [](auto row) { return row + 5; }),
  });

  assertReadWithFilters(
      "date.parquet", dateSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, intMultipleFilters) {
  // Filter int BETWEEN 102 AND 120 AND bigint BETWEEN 900 AND 1006.
  FilterMap filters;
  filters.insert({"int", exec::between(102, 120)});
  filters.insert({"bigint", exec::between(900, 1006)});

  auto expected = makeRowVector({
      makeFlatVector<int32_t>(5, [](auto row) { return row + 102; }),
      makeFlatVector<int64_t>(5, [](auto row) { return row + 1002; }),
  });

  assertReadWithFilters(
      "int.parquet", intSchema(), std::move(filters), expected);
}

TEST_F(ParquetReaderTest, doubleFilters) {
  // Read sample.parquet with the double filter "b < 10.0".
  FilterMap filters;
  filters.insert({"b", exec::lessThanDouble(10.0)});

  auto expected = makeRowVector({
      makeFlatVector<int64_t>(9, [](auto row) { return row + 1; }),
      makeFlatVector<double>(9, [](auto row) { return row + 1; }),
  });

  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
  filters.clear();

  // Test "b <= 10.0".
  filters.insert({"b", exec::lessThanOrEqualDouble(10.0)});
  expected = makeRowVector({
      makeFlatVector<int64_t>(10, [](auto row) { return row + 1; }),
      makeFlatVector<double>(10, [](auto row) { return row + 1; }),
  });
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
  filters.clear();

  // Test "b between 10.0 and 14.0".
  filters.insert({"b", exec::betweenDouble(10.0, 14.0)});
  expected = makeRowVector({
      makeFlatVector<int64_t>(5, [](auto row) { return row + 10; }),
      makeFlatVector<double>(5, [](auto row) { return row + 10; }),
  });
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
  filters.clear();

  // Test "b > 14.0".
  filters.insert({"b", exec::greaterThanDouble(14.0)});
  expected = makeRowVector({
      makeFlatVector<int64_t>(6, [](auto row) { return row + 15; }),
      makeFlatVector<double>(6, [](auto row) { return row + 15; }),
  });
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
  filters.clear();

  // Test "b >= 14.0".
  filters.insert({"b", exec::greaterThanOrEqualDouble(14.0)});
  expected = makeRowVector({
      makeFlatVector<int64_t>(7, [](auto row) { return row + 14; }),
      makeFlatVector<double>(7, [](auto row) { return row + 14; }),
  });
  assertReadWithFilters(
      "sample.parquet", sampleSchema(), std::move(filters), expected);
  filters.clear();
}

TEST_F(ParquetReaderTest, varcharFilters) {
  // Test "name < 'CANADA'".
  FilterMap filters;
  filters.insert({"name", exec::lessThan("CANADA")});

  auto expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2}),
      makeFlatVector<std::string>({"ALGERIA", "ARGENTINA", "BRAZIL"}),
      makeFlatVector<int64_t>({0, 1, 1}),
  });

  auto rowType =
      ROW({"nationkey", "name", "regionkey"}, {BIGINT(), VARCHAR(), BIGINT()});

  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);
  filters.clear();

  // Test "name <= 'CANADA'".
  filters.insert({"name", exec::lessThanOrEqual("CANADA")});
  expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2, 3}),
      makeFlatVector<std::string>({"ALGERIA", "ARGENTINA", "BRAZIL", "CANADA"}),
      makeFlatVector<int64_t>({0, 1, 1, 1}),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);
  filters.clear();

  // Test "name > UNITED KINGDOM".
  filters.insert({"name", exec::greaterThan("UNITED KINGDOM")});
  expected = makeRowVector({
      makeFlatVector<int64_t>({21, 24}),
      makeFlatVector<std::string>({"VIETNAM", "UNITED STATES"}),
      makeFlatVector<int64_t>({2, 1}),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);
  filters.clear();

  // Test "name >= 'UNITED KINGDOM'".
  filters.insert({"name", exec::greaterThanOrEqual("UNITED KINGDOM")});
  expected = makeRowVector({
      makeFlatVector<int64_t>({21, 23, 24}),
      makeFlatVector<std::string>(
          {"VIETNAM", "UNITED KINGDOM", "UNITED STATES"}),
      makeFlatVector<int64_t>({2, 3, 1}),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);
  filters.clear();

  // Test "name = 'CANADA'".
  filters.insert({"name", exec::equal("CANADA")});
  expected = makeRowVector({
      makeFlatVector<int64_t>(1, [](auto row) { return row + 3; }),
      makeFlatVector<std::string>({"CANADA"}),
      makeFlatVector<int64_t>(1, [](auto row) { return row + 1; }),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);
  filters.clear();

  // Test "name IN ('CANADA', 'UNITED KINGDOM')".
  filters.insert({"name", exec::in({std::string("CANADA"), "UNITED KINGDOM"})});
  expected = makeRowVector({
      makeFlatVector<int64_t>({3, 23}),
      makeFlatVector<std::string>({"CANADA", "UNITED KINGDOM"}),
      makeFlatVector<int64_t>({1, 3}),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);
  filters.clear();

  // Test "name IN ('UNITED STATES', 'CANADA', 'INDIA', 'RUSSIA')".
  filters.insert(
      {"name",
       exec::in({std::string("UNITED STATES"), "INDIA", "CANADA", "RUSSIA"})});
  expected = makeRowVector({
      makeFlatVector<int64_t>({3, 8, 22, 24}),
      makeFlatVector<std::string>(
          {"CANADA", "INDIA", "RUSSIA", "UNITED STATES"}),
      makeFlatVector<int64_t>({1, 2, 3, 1}),
  });
  assertReadWithFilters(
      "nation.parquet", rowType, std::move(filters), expected);
  filters.clear();
}

TEST_F(ParquetReaderTest, readDifferentEncodingsWithFilter) {
  FilterMap filters;
  filters.insert({"n_1", exec::equal(1)});
  auto rowType = ROW({"n_0", "n_1", "n_2"}, {INTEGER(), INTEGER(), VARCHAR()});
  auto expected = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 1, 2, 3, 4, 6, 9}),
      makeFlatVector<int32_t>(10, [](auto /*row*/) { return 1; }),
      makeNullableFlatVector<std::string>(
          {"A",
           "B",
           std::nullopt,
           std::nullopt,
           "A",
           "B",
           std::nullopt,
           std::nullopt,
           "F",
           std::nullopt}),
  });
  assertReadWithFilters(
      "different_encodings_with_filter.parquet",
      rowType,
      std::move(filters),
      expected);
}

// This test is to verify filterRowGroups() doesn't throw the fileOffset Velox
// check failure
TEST_F(ParquetReaderTest, filterRowGroups) {
  // decimal_no_ColumnMetadata.parquet has one columns a: DECIMAL(9,1). It
  // doesn't have ColumnMetaData, and rowGroups_[0].columns[0].file_offset is 0.
  auto rowType = ROW({"_c0"}, {DECIMAL(9, 1)});
  facebook::velox::dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  const std::string decimal_dict(
      getExampleFilePath("decimal_no_ColumnMetadata.parquet"));

  auto reader = createReader(decimal_dict, readerOpts);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  EXPECT_EQ(reader->numberOfRows(), 10ULL);
}

TEST_F(ParquetReaderTest, parseLongTagged) {
  // This is a case for long with annonation read
  const std::string sample(getExampleFilePath("tagged_long.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);

  EXPECT_EQ(reader->numberOfRows(), 4ULL);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 1ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(type->childByName("_c0"), col0);
}

TEST_F(ParquetReaderTest, preloadSmallFile) {
  const std::string sample(getExampleFilePath("sample.parquet"));

  auto file = std::make_shared<LocalReadFile>(sample);
  auto input = std::make_unique<BufferedInput>(file, *leafPool_);

  facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
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
                      kDefaultFooterEstimatedSize);

  // Check the whole file already loaded.
  ASSERT_EQ(file->bytesRead(), fileSize);

  // Reset bytes read to check for duplicate reads.
  file->resetBytesRead();

  constexpr int kBatchSize = 10;
  auto result = BaseVector::create(sampleSchema(), 1, leafPool_.get());
  while (rowReader->next(kBatchSize, result)) {
    // Check no duplicate reads.
    ASSERT_EQ(file->bytesRead(), 0);
  }
}

TEST_F(ParquetReaderTest, prefetchRowGroups) {
  auto rowType = ROW({"id"}, {BIGINT()});
  const std::string sample(getExampleFilePath("multiple_row_groups.parquet"));
  const int numRowGroups = 4;

  facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
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

    auto reader = createReader(sample, readerOptions);
    EXPECT_EQ(reader->fileMetaData().numRowGroups(), numRowGroups);

    RowReaderOptions rowReaderOpts;
    rowReaderOpts.setScanSpec(makeScanSpec(rowType));
    auto rowReader = reader->createRowReader(rowReaderOpts);
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

TEST_F(ParquetReaderTest, testEmptyRowGroups) {
  // empty_row_groups.parquet contains empty row groups
  const std::string sample(getExampleFilePath("empty_row_groups.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 5ULL);

  auto rowType = reader->typeWithId();
  EXPECT_EQ(rowType->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(rowType->size(), 1ULL);

  auto integerType = rowType->childAt(0);
  EXPECT_EQ(integerType->type()->kind(), TypeKind::INTEGER);

  auto fileSchema = ROW({"a"}, {INTEGER()});
  auto rowReaderOpts = getReaderOpts(fileSchema);
  auto scanSpec = makeScanSpec(fileSchema);
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = makeRowVector({makeFlatVector<int32_t>({0, 3, 3, 3, 3})});

  assertReadWithReaderAndExpected(fileSchema, *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, testEnumType) {
  // enum_type.parquet contains 1 column (ENUM) with 3 rows.
  const std::string sample(getExampleFilePath("enum_type.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 3ULL);

  auto rowType = reader->typeWithId();
  EXPECT_EQ(rowType->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(rowType->size(), 1ULL);

  EXPECT_EQ(rowType->childAt(0)->type()->kind(), TypeKind::VARCHAR);

  auto fileSchema = ROW({"test"}, {VARCHAR()});
  auto rowReaderOpts = getReaderOpts(fileSchema);
  rowReaderOpts.setScanSpec(makeScanSpec(fileSchema));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected =
      makeRowVector({makeFlatVector<StringView>({"FOO", "BAR", "FOO"})});

  assertReadWithReaderAndExpected(fileSchema, *rowReader, expected, *leafPool_);
}

TEST_F(ParquetReaderTest, readVarbinaryFromFLBA) {
  const std::string filename("varbinary_flba.parquet");
  const std::string sample(getExampleFilePath(filename));

  facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 8ULL);
  auto flbaCol =
      std::static_pointer_cast<const ParquetTypeWithId>(type->childAt(6));
  EXPECT_EQ(flbaCol->name_, "flba_field");
  EXPECT_EQ(flbaCol->parquetType_, thrift::Type::FIXED_LEN_BYTE_ARRAY);

  auto selectedType = ROW({"flba_field"}, {VARBINARY()});
  auto rowReaderOpts = getReaderOpts(selectedType);
  rowReaderOpts.setScanSpec(makeScanSpec(selectedType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = std::string(1024, '*');
  VectorPtr result = BaseVector::create(selectedType, 0, &(*leafPool_));
  rowReader->next(1, result);
  EXPECT_EQ(
      expected,
      result->as<RowVector>()->childAt(0)->asFlatVector<StringView>()->valueAt(
          0));
}

TEST_F(ParquetReaderTest, testV2PageWithZeroMaxDefRep) {
  // enum_type.parquet contains 1 column (ENUM) with 3 rows.
  const std::string sample(getExampleFilePath("v2_page.parquet"));

  facebook::velox::dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 5ULL);

  auto rowType = reader->typeWithId();
  EXPECT_EQ(rowType->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(rowType->size(), 1ULL);

  EXPECT_EQ(rowType->childAt(0)->type()->kind(), TypeKind::BIGINT);

  auto outputRowType = ROW({"regionkey"}, {BIGINT()});
  auto rowReaderOpts = getReaderOpts(outputRowType);
  rowReaderOpts.setScanSpec(makeScanSpec(outputRowType));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto expected = makeRowVector({makeFlatVector<int64_t>({0, 1, 2, 3, 4})});

  assertReadWithReaderAndExpected(
      outputRowType, *rowReader, expected, *leafPool_);
}
