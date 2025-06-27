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

#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/dwio/text/RegisterTextReader.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

extern int daylight;
extern long timezone;

using namespace facebook::velox;
using namespace facebook::velox::test;

namespace facebook::velox::text {

namespace {

class TextReaderTest : public testing::Test,
                       public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    dwio::common::registerTextReaderFactory();
  }

  void TearDown() override {
    dwio::common::unregisterTextReaderFactory();
  }

  memory::MemoryPool& poolRef() {
    return *pool();
  }

  void setScanSpec(const Type& type, dwio::common::RowReaderOptions& options) {
    auto spec = std::make_shared<common::ScanSpec>("root");
    spec->addAllChildFields(type);
    options.setScanSpec(spec);
  }

 private:
  std::shared_ptr<LocalReadFile> readFile_;
};

TEST_F(TextReaderTest, DISABLED_basic) {
  auto expected = makeRowVector({
      makeFlatVector<std::string>(
          {"FOO",
           "FOO",
           "FOO",
           "FOO",
           "BAR",
           "BAR",
           "BAR",
           "BAR",
           "BAZ",
           "BAZ",
           "BAZ",
           "BAZ"}),
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
      makeFlatVector<double>(
          {1.123,
           2.333,
           -6.1,
           4.2,
           47.2,
           79.5,
           3.1415926,
           -221.145,
           93.12,
           -4123.11,
           950.2,
           43.66}),
      makeFlatVector<bool>(
          {true,
           true,
           false,
           true,
           false,
           false,
           true,
           true,
           false,
           false,
           false,
           true}),
  });

  auto type = ROW(
      {{"col_string", VARCHAR()},
       {"col_int", INTEGER()},
       {"col_float", DOUBLE()},
       {"col_bool", BOOLEAN()}});
  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);

  auto path = velox::test::getDataFilePath(
      "velox/dwio/text/tests/reader/",
      "examples/simple_types_compressed_file.gz");
  auto readFile = std::make_shared<LocalReadFile>(path);

  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(type);

  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = factory->createReader(std::move(input), readerOptions);
  dwio::common::RowReaderOptions rowReaderOptions;
  setScanSpec(*type, rowReaderOptions);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  EXPECT_EQ(*reader->rowType(), *type);

  VectorPtr result;

  // Try reading 10 rows each time.
  ASSERT_EQ(rowReader->next(10, result), 10);
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, i));
  }
  ASSERT_EQ(rowReader->next(10, result), 2);
  for (int i = 0; i < 2; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, 10 + i));
  }
  ASSERT_EQ(rowReader->next(10, result), 0);

  input = std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  reader = factory->createReader(std::move(input), readerOptions);
  rowReader = reader->createRowReader(rowReaderOptions);
  // Try reading 2, 3, 4, 5 rows at a time.
  ASSERT_EQ(rowReader->next(2, result), 2);
  for (int i = 0; i < 2; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, i));
  }
  ASSERT_EQ(rowReader->next(3, result), 3);
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, 2 + i));
  }
  ASSERT_EQ(rowReader->next(4, result), 4);
  for (int i = 0; i < 4; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, 5 + i));
  }
  ASSERT_EQ(rowReader->next(5, result), 3);
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, 9 + i));
  }
  ASSERT_EQ(rowReader->next(10, result), 0);
}

TEST_F(TextReaderTest, headerAndCustomNullString) {
  tzset();
  const auto tzOffsetPST = 28'800;
  const auto tzOffsetPDT = 25'200;
  auto expected = makeRowVector({
      makeFlatVector<std::string>(
          {"FOO",
           "FOO",
           "FOO",
           "FOO",
           "BAR",
           "BAR",
           "BAR",
           "BAR",
           "BAZ",
           "BAZ",
           "BAZ",
           "BAZ",
           ""}),
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}),
      makeNullableFlatVector<double>(
          {1.123,
           2.333,
           -6.1,
           4.2,
           47.2,
           79.5,
           3.1415926,
           -221.145,
           93.12,
           -4123.11,
           950.2,
           43.66,
           std::nullopt}),
      makeNullableFlatVector<Timestamp>({
          Timestamp{1'695'378'095 + tzOffsetPDT, 148'000'000},
          Timestamp{1'695'690'351 + tzOffsetPDT, 0},
          Timestamp{1'695'686'400 + tzOffsetPDT, 0},
          Timestamp{1'695'083'400 + tzOffsetPDT, 0},
          std::nullopt,
          Timestamp{1'695'657'091 + tzOffsetPDT, 209'000'000},
          Timestamp{1'695'690'437 + tzOffsetPDT, 469'123'000},
          Timestamp{1'696'540'679 + tzOffsetPDT, 976'000'000},
          Timestamp{1'695'657'171 + tzOffsetPDT, 637'000'000},
          Timestamp{1'695'693'225 + tzOffsetPDT, 745'123'000},
          std::nullopt,
          Timestamp{1'695'406'246 + tzOffsetPDT, 0},
          Timestamp{1'699'392'124 + tzOffsetPST, 736'000'000},
      }),
  });

  auto type = ROW(
      {{"col_string", VARCHAR()},
       {"col_int", INTEGER()},
       {"col_float", DOUBLE()},
       {"col_ts", TIMESTAMP()}});
  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);

  auto path = velox::test::getDataFilePath(
      "velox/dwio/text/tests/reader/", "examples/simple_types_with_header");
  auto readFile = std::make_shared<LocalReadFile>(path);

  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(type);
  auto rowReaderOptions = dwio::common::RowReaderOptions();
  setScanSpec(*type, rowReaderOptions);
  rowReaderOptions.setSkipRows(1);

  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = factory->createReader(std::move(input), readerOptions);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  EXPECT_EQ(*reader->rowType(), *type);

  VectorPtr result;

  // Try reading 10 rows each time.
  ASSERT_EQ(rowReader->next(10, result), 10);
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, i));
  }
  ASSERT_EQ(rowReader->next(10, result), 3);
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, 10 + i));
  }
  ASSERT_EQ(rowReader->next(10, result), 0);

  input = std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  reader = factory->createReader(std::move(input), readerOptions);
  rowReader = reader->createRowReader(rowReaderOptions);
  // Try reading 2, 3, 4, 5 rows at a time.
  ASSERT_EQ(rowReader->next(2, result), 2);
  for (int i = 0; i < 2; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, i));
  }
  ASSERT_EQ(rowReader->next(3, result), 3);
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, 2 + i));
  }
  ASSERT_EQ(rowReader->next(4, result), 4);
  for (int i = 0; i < 4; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, 5 + i));
  }
  ASSERT_EQ(rowReader->next(5, result), 4);
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, 9 + i));
  }
  ASSERT_EQ(rowReader->next(10, result), 0);

  // Try reading with an empty NULL string.
  auto serDeOptions = readerOptions.serDeOptions();
  serDeOptions.nullString = "";
  readerOptions.setSerDeOptions(serDeOptions);
  input = std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  reader = factory->createReader(std::move(input), readerOptions);
  rowReader = reader->createRowReader(rowReaderOptions);
  ASSERT_EQ(rowReader->next(15, result), 13);
  expected->childAt(0)->setNull(12, true);
  for (int i = 0; i < 13; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, i));
  }

  // Try reading with a custom NULL string.
  serDeOptions.nullString = "BAR";
  readerOptions.setSerDeOptions(serDeOptions);
  input = std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  reader = factory->createReader(std::move(input), readerOptions);
  rowReader = reader->createRowReader(rowReaderOptions);
  ASSERT_EQ(rowReader->next(15, result), 13);
  expected->childAt(0)->setNull(12, false);
  expected->childAt(0)->setNull(4, true);
  expected->childAt(0)->setNull(5, true);
  expected->childAt(0)->setNull(6, true);
  expected->childAt(0)->setNull(7, true);
  for (int i = 0; i < 13; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, i));
  }
}

TEST_F(TextReaderTest, complexTypesWithCustomDelimiters) {
  const vector_size_t length = 13;
  const auto keyVector = makeFlatVector<int64_t>(
      {1,   111,  22, 22222, 333, 33, 44,    5,   555, 66, 7777,     7,
       777, 8888, 88, 9,     99,  10, 10000, 111, 1,   11, 11122222, 123142});
  const auto valueVector = makeFlatVector<bool>(
      {false, true, true,  false, false, true,  false, true,
       false, true, false, true,  false, false, true,  true,
       false, true, false, true,  false, true,  true,  true});
  BufferPtr sizes = facebook::velox::allocateOffsets(length, pool());
  BufferPtr offsets = facebook::velox::allocateOffsets(length, pool());
  auto rawSizes = sizes->asMutable<vector_size_t>();
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  rawSizes[0] = 2;
  rawSizes[1] = 2;
  rawSizes[2] = 2;
  rawSizes[3] = 1;
  rawSizes[4] = 2;
  rawSizes[5] = 1;
  rawSizes[6] = 3;
  rawSizes[7] = 2;
  rawSizes[8] = 2;
  rawSizes[9] = 2;
  rawSizes[10] = 3;
  rawSizes[11] = 1;
  rawSizes[12] = 1;
  for (int i = 1; i < length; i++) {
    rawOffsets[i] = rawOffsets[i - 1] + rawSizes[i - 1];
  }

  const auto expected = makeRowVector({
      makeFlatVector<std::string>(
          {"FOO",
           "FOO",
           "FOO",
           "FOO",
           "BAR",
           "BAR",
           "BAR",
           "BAR",
           "BAZ",
           "BAZ",
           "BAZ",
           "FOO\\nBAZ",
           "FOO\n\nBAR\nBAZ"}),
      makeArrayVector<int64_t>(
          {{1, 11, 111},
           {22, 22222},
           {333, 33},
           {4444, 44},
           {5, 555},
           {666, 66, 66},
           {7777, 7, 777},
           {8888, 88},
           {9, 99},
           {10, 10000},
           {111, 1, 111},
           {12, 11122222, 222},
           {13, 11133333, 333}}),
      makeArrayVector<double>(
          {{1.123, 1.3123},
           {2.333, -5512, 1.23},
           {-6.1, 65.777},
           {4.2, 24, 324.11},
           {47.2, 213.23},
           {79.5, -44.11},
           {3.1415926, 441.124},
           {-221.145, 878.43, -11},
           {93.12, 632},
           {-4123.11, -177.1},
           {950.2, -4412},
           {43.66, 33121.43},
           {-42.11, -123.43}}),
      std::make_shared<MapVector>(
          pool(),
          MAP(keyVector->type(), valueVector->type()),
          nullptr,
          length,
          offsets,
          sizes,
          keyVector,
          valueVector),
  });

  const auto type = ROW(
      {{"col_string", VARCHAR()},
       {"col_bigint_arr", ARRAY(BIGINT())},
       {"col_double_arr", ARRAY(DOUBLE())},
       {"col_map", MAP(BIGINT(), BOOLEAN())}});
  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);

  auto path = velox::test::getDataFilePath(
      "velox/dwio/text/tests/reader/", "examples/custom_delimiters_file");
  auto readFile = std::make_shared<LocalReadFile>(path);

  auto serDeOptions = dwio::common::SerDeOptions('\t', '|', '#', '\\', true);
  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(type);
  readerOptions.setSerDeOptions(serDeOptions);

  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = factory->createReader(std::move(input), readerOptions);
  auto rowReaderOptions = dwio::common::RowReaderOptions();
  setScanSpec(*type, rowReaderOptions);
  rowReaderOptions.range(0, 544);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  EXPECT_EQ(*reader->rowType(), *type);

  VectorPtr result;

  // Try reading 10 rows each time.
  ASSERT_EQ(rowReader->next(10, result), 10);
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, i));
  }
  ASSERT_EQ(rowReader->next(10, result), 3);
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, 10 + i));
  }
  ASSERT_EQ(rowReader->next(10, result), 0);

  input = std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  reader = factory->createReader(std::move(input), readerOptions);
  auto rowReaderOptions2 = dwio::common::RowReaderOptions();
  setScanSpec(*type, rowReaderOptions2);
  rowReader = reader->createRowReader(rowReaderOptions2);
  // Try reading 2, 3, 4, 5 rows at a time.
  ASSERT_EQ(rowReader->next(2, result), 2);
  for (int i = 0; i < 2; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, i));
  }
  ASSERT_EQ(rowReader->next(3, result), 3);
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, 2 + i));
  }
  ASSERT_EQ(rowReader->next(4, result), 4);
  for (int i = 0; i < 4; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, 5 + i));
  }
  ASSERT_EQ(rowReader->next(5, result), 4);
  for (int i = 0; i < 4; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, 9 + i));
  }
  ASSERT_EQ(rowReader->next(10, result), 0);
}

TEST_F(TextReaderTest, projectComplexTypesWithCustomDelimiters) {
  const auto type = ROW(
      {{"col_string", VARCHAR()},
       {"col_bigint_arr", ARRAY(BIGINT())},
       {"col_double_arr", ARRAY(DOUBLE())},
       {"col_map", MAP(BIGINT(), BOOLEAN())}});

  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);
  auto path = velox::test::getDataFilePath(
      "velox/dwio/text/tests/reader/", "examples/custom_delimiters_file");
  auto readFile = std::make_shared<LocalReadFile>(path);

  auto serDeOptions = dwio::common::SerDeOptions('\t', '|', '#', '\\', true);
  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(type);
  readerOptions.setSerDeOptions(serDeOptions);

  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = factory->createReader(std::move(input), readerOptions);

  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addField("ds", 0)->setConstantValue(
      BaseVector::createConstant(VARCHAR(), "2023-07-18", 1, pool()));
  spec->addField("col_string", 1);
  spec->addField("col_map", 2);

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(spec);
  rowOptions.select(std::make_shared<dwio::common::ColumnSelector>(
      type, std::vector<std::string>({"col_string", "col_map"})));
  auto rowReader = reader->createRowReader(rowOptions);

  VectorPtr result;

  ASSERT_EQ(rowReader->next(13, result), 13);
  ASSERT_EQ(
      *result->type(),
      *ROW(
          {"ds", "col_string", "col_map"},
          {VARCHAR(), VARCHAR(), MAP(BIGINT(), BOOLEAN())}));

  const vector_size_t length = 13;
  const auto keyVector = makeFlatVector<int64_t>(
      {1,   111,  22, 22222, 333, 33, 44,    5,   555, 66, 7777,     7,
       777, 8888, 88, 9,     99,  10, 10000, 111, 1,   11, 11122222, 123142});
  const auto valueVector = makeFlatVector<bool>(
      {false, true, true,  false, false, true,  false, true,
       false, true, false, true,  false, false, true,  true,
       false, true, false, true,  false, true,  true,  true});
  BufferPtr sizes = facebook::velox::allocateOffsets(length, pool());
  BufferPtr offsets = facebook::velox::allocateOffsets(length, pool());
  auto rawSizes = sizes->asMutable<vector_size_t>();
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  rawSizes[0] = 2;
  rawSizes[1] = 2;
  rawSizes[2] = 2;
  rawSizes[3] = 1;
  rawSizes[4] = 2;
  rawSizes[5] = 1;
  rawSizes[6] = 3;
  rawSizes[7] = 2;
  rawSizes[8] = 2;
  rawSizes[9] = 2;
  rawSizes[10] = 3;
  rawSizes[11] = 1;
  rawSizes[12] = 1;
  for (int i = 1; i < length; i++) {
    rawOffsets[i] = rawOffsets[i - 1] + rawSizes[i - 1];
  }

  auto expected = makeRowVector({
      std::make_shared<ConstantVector<StringView>>(
          pool(), 13, false, VARCHAR(), "2023-07-18"),
      makeFlatVector<std::string>(
          {"FOO",
           "FOO",
           "FOO",
           "FOO",
           "BAR",
           "BAR",
           "BAR",
           "BAR",
           "BAZ",
           "BAZ",
           "BAZ",
           "FOO\\nBAZ",
           "FOO\n\nBAR\nBAZ"}),
      std::make_shared<MapVector>(
          pool(),
          MAP(keyVector->type(), valueVector->type()),
          nullptr,
          length,
          offsets,
          sizes,
          keyVector,
          valueVector),
  });

  ASSERT_EQ(result->size(), expected->size());
  for (int i = 0; i < 13; ++i) {
    ASSERT_TRUE(result->equalValueAt(expected.get(), i, i));
  }
}

TEST_F(TextReaderTest, DISABLED_projectColumns) {
  auto type = ROW(
      {{"col_string", VARCHAR()},
       {"col_int", INTEGER()},
       {"col_float", DOUBLE()},
       {"col_bool", BOOLEAN()}});
  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);
  auto path = velox::test::getDataFilePath(
      "velox/dwio/text/tests/reader/",
      "examples/simple_types_compressed_file.gz");
  auto readFile = std::make_shared<LocalReadFile>(path);
  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(type);
  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = factory->createReader(std::move(input), readerOptions);
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addField("ds", 0)->setConstantValue(
      BaseVector::createConstant(VARCHAR(), "2023-07-18", 1, pool()));
  spec->addField("col_float", 1);
  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(spec);
  rowOptions.select(std::make_shared<dwio::common::ColumnSelector>(
      type, std::vector<std::string>({"col_float"})));
  auto rowReader = reader->createRowReader(rowOptions);
  VectorPtr result;
  ASSERT_EQ(rowReader->next(10, result), 10);
  ASSERT_EQ(*result->type(), *ROW({"ds", "col_float"}, {VARCHAR(), DOUBLE()}));
  auto expected = makeRowVector({
      std::make_shared<ConstantVector<StringView>>(
          pool(), 10, false, VARCHAR(), "2023-07-18"),
      makeFlatVector<double>(
          {1.123,
           2.333,
           -6.1,
           4.2,
           47.2,
           79.5,
           3.1415926,
           -221.145,
           93.12,
           -4123.11,
           950.2,
           43.66}),
  });
  ASSERT_EQ(result->size(), expected->size());
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(result->equalValueAt(expected.get(), i, i));
  }
}

TEST_F(TextReaderTest, projectNone) {
  // Tests the case where none of the columns are projected, e.g. a basic
  // count(*) query.
  auto type = ROW(
      {{"col_int", INTEGER()},
       {"col_big_int", BIGINT()},
       {"col_tiny_int", TINYINT()},
       {"col_double", DOUBLE()}});
  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);

  auto path = velox::test::getDataFilePath(
      "velox/dwio/text/tests/reader/", "examples/simple_types");
  auto readFile = std::make_shared<LocalReadFile>(path);

  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(type);

  dwio::common::RowReaderOptions rowReaderOptions;
  // Project none of the columns.
  setScanSpec(*ROW({}, {}), rowReaderOptions);
  rowReaderOptions.select(
      std::make_shared<dwio::common::ColumnSelector>(ROW({}, {})));

  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = factory->createReader(std::move(input), readerOptions);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  VectorPtr result;
  // We expect to get 16 rows.
  ASSERT_EQ(rowReader->next(16, result), 16);
  ASSERT_EQ(rowReader->next(10, result), 0);
}

TEST_F(TextReaderTest, DISABLED_compressedProjectNone) {
  // Tests the case where none of the columns are projected, e.g. a basic
  // count(*) query.
  auto type = ROW(
      {{"col_string", VARCHAR()},
       {"col_int", INTEGER()},
       {"col_float", DOUBLE()},
       {"col_bool", BOOLEAN()}});
  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);

  auto path = velox::test::getDataFilePath(
      "velox/dwio/text/tests/reader/",
      "examples/simple_types_compressed_file.gz");
  auto readFile = std::make_shared<LocalReadFile>(path);

  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(type);

  dwio::common::RowReaderOptions rowReaderOptions;
  // Project none of the columns.
  setScanSpec(*ROW({}, {}), rowReaderOptions);
  rowReaderOptions.select(
      std::make_shared<dwio::common::ColumnSelector>(ROW({}, {})));

  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = factory->createReader(std::move(input), readerOptions);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  VectorPtr result;
  // We expect to get 12 rows.
  ASSERT_EQ(rowReader->next(12, result), 12);
  ASSERT_EQ(rowReader->next(10, result), 0);
}

TEST_F(TextReaderTest, DISABLED_compressedFilter) {
  auto type = ROW(
      {{"col_string", VARCHAR()},
       {"col_int", INTEGER()},
       {"col_float", DOUBLE()},
       {"col_bool", BOOLEAN()}});
  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);
  auto path = velox::test::getDataFilePath(
      "velox/dwio/text/tests/reader/",
      "examples/simple_types_compressed_file.gz");
  auto readFile = std::make_shared<LocalReadFile>(path);
  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(type);
  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = factory->createReader(std::move(input), readerOptions);
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addField("ds", 0)->setConstantValue(
      BaseVector::createConstant(VARCHAR(), "2023-07-18", 1, pool()));
  spec->addField("col_int", 1);
  spec->getOrCreateChild(common::Subfield("col_string"))
      ->setFilter(std::make_unique<common::BytesValues>(
          std::vector<std::string>({"BAR"}), false));
  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(spec);
  rowOptions.select(
      std::make_shared<dwio::common::ColumnSelector>(type, type->names()));
  auto rowReader = reader->createRowReader(rowOptions);
  VectorPtr result;
  ASSERT_EQ(rowReader->next(10, result), 10);
  ASSERT_EQ(*result->type(), *ROW({"ds", "col_int"}, {VARCHAR(), INTEGER()}));
  auto expected = makeRowVector({
      std::make_shared<ConstantVector<StringView>>(
          pool(), 4, false, VARCHAR(), "2023-07-18"),
      makeFlatVector<int32_t>({5, 6, 7, 8}),
  });
  ASSERT_EQ(result->size(), expected->size());
  for (int i = 0; i < expected->size(); ++i) {
    ASSERT_TRUE(result->equalValueAt(expected.get(), i, i));
  }
}

TEST_F(TextReaderTest, filter) {
  auto type = ROW(
      {{"col_string", VARCHAR()},
       {"col_big_int", BIGINT()},
       {"col_bool", BOOLEAN()},
       {"col_timestamp", TIMESTAMP()}});

  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);
  auto path = velox::test::getDataFilePath(
      "velox/dwio/text/tests/reader/", "examples/more_simple_types");
  auto readFile = std::make_shared<LocalReadFile>(path);

  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(type);

  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = factory->createReader(std::move(input), readerOptions);

  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addField("ds", 0)->setConstantValue(
      BaseVector::createConstant(VARCHAR(), "2023-07-18", 1, pool()));
  spec->addField("col_big_int", 1);
  spec->getOrCreateChild(common::Subfield("col_string"))
      ->setFilter(std::make_unique<common::BytesValues>(
          std::vector<std::string>({"BAR", "BAZ"}), false));

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(spec);
  rowOptions.select(
      std::make_shared<dwio::common::ColumnSelector>(type, type->names()));

  auto rowReader = reader->createRowReader(rowOptions);
  VectorPtr result;

  ASSERT_EQ(rowReader->next(15, result), 13);

  ASSERT_EQ(
      *result->type(), *ROW({"ds", "col_big_int"}, {VARCHAR(), BIGINT()}));
  auto expected = makeRowVector({
      std::make_shared<ConstantVector<StringView>>(
          pool(), 7, false, VARCHAR(), "2023-07-18"),
      makeFlatVector<int64_t>({
          4192,
          4193,
          4192,
          4192,
          4194,
          4192,
          4195,
      }),
  });

  ASSERT_EQ(result->size(), expected->size());
  for (int i = 0; i < expected->size(); ++i) {
    ASSERT_TRUE(result->equalValueAt(expected.get(), i, i));
  }
}

TEST_F(TextReaderTest, DISABLED_shrinkBatch) {
  auto type = ROW(
      {{"col_string", VARCHAR()},
       {"col_int", INTEGER()},
       {"col_float", DOUBLE()},
       {"col_bool", BOOLEAN()}});
  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);
  auto path = velox::test::getDataFilePath(
      "velox/dwio/text/tests/reader/",
      "examples/simple_types_compressed_file.gz");
  auto readFile = std::make_shared<LocalReadFile>(path);
  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(type);
  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = factory->createReader(std::move(input), readerOptions);
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(spec);
  rowOptions.select(
      std::make_shared<dwio::common::ColumnSelector>(ROW({}, {})));
  auto rowReader = reader->createRowReader(rowOptions);
  VectorPtr result;
  ASSERT_EQ(rowReader->next(6, result), 6);
  ASSERT_EQ(result->size(), 6);
  ASSERT_EQ(rowReader->next(4, result), 4);
  ASSERT_EQ(result->size(), 4);
  ASSERT_EQ(rowReader->next(4, result), 2);
  ASSERT_EQ(result->size(), 2);
  ASSERT_EQ(rowReader->next(4, result), 0);
}

TEST_F(TextReaderTest, DISABLED_emptyFile) {
  auto type = ROW({
      {"transaction_id", VARCHAR()},
      {"serial_number", VARCHAR()},
  });
  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);
  auto path = velox::test::getDataFilePath(
      "velox/dwio/text/tests/reader/", "examples/empty.gz");
  auto readFile = std::make_shared<LocalReadFile>(path);
  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(type);
  auto rowReaderOptions = dwio::common::RowReaderOptions();
  setScanSpec(*type, rowReaderOptions);
  rowReaderOptions.setSkipRows(1);
  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = factory->createReader(std::move(input), readerOptions);
  auto rowReader = reader->createRowReader(rowReaderOptions);
  EXPECT_EQ(*reader->rowType(), *type);
  VectorPtr result;
  // Try reading 10 rows each time.
  ASSERT_EQ(rowReader->next(10, result), 0);
}

TEST_F(TextReaderTest, readRanges) {
  auto expected = makeRowVector(
      {makeFlatVector<int64_t>(
           {11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}),
       makeFlatVector<int64_t>({
           4191,
           4192,
           4192,
           4196,
           4192,
           4193,
           4192,
           4192,
           4194,
           4192,
           4195,
           4192,
           4192,
           4192,
           4192,
       }),
       makeFlatVector<int8_t>({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})});

  auto type =
      ROW({{"id", BIGINT()}, {"org_id", BIGINT()}, {"deleted", TINYINT()}});
  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);

  auto path = velox::test::getDataFilePath(
      "velox/dwio/text/tests/reader/",
      "examples/simple_types_10_bytes_per_row");
  auto readFile = std::make_shared<LocalReadFile>(path);

  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(type);

  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = factory->createReader(std::move(input), readerOptions);
  const int bytesPerRows = 10;

  dwio::common::RowReaderOptions rowReaderOptions;
  VectorPtr result;

  // read from 1st row to 6th row
  rowReaderOptions.range(0, 5 * bytesPerRows);
  setScanSpec(*type, rowReaderOptions);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  uint64_t scanned = rowReader->next(1024, result);
  EXPECT_EQ(scanned, 6);
  for (int i = 0; i < 6; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, i));
  }

  // read from 6th row to 10th row
  rowReaderOptions.range(5 * bytesPerRows, 5 * bytesPerRows);
  setScanSpec(*type, rowReaderOptions);
  rowReader = reader->createRowReader(rowReaderOptions);
  scanned = rowReader->next(1024, result);
  EXPECT_EQ(scanned, 5);
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, 6 + i));
  }

  // read from 11th row to 15th row
  rowReaderOptions.range(10 * bytesPerRows, 5 * bytesPerRows);
  setScanSpec(*type, rowReaderOptions);
  rowReader = reader->createRowReader(rowReaderOptions);
  scanned = rowReader->next(1024, result);
  EXPECT_EQ(scanned, 4);
  for (int i = 0; i < 4; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, 11 + i));
  }
}

TEST_F(TextReaderTest, readFloatAsInt) {
  auto expected = makeRowVector({
      makeFlatVector<int32_t>(
          {11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26}),
      makeFlatVector<int64_t>(
          {4191,
           4192,
           4192,
           4196,
           4192,
           4193,
           4192,
           4192,
           4194,
           4192,
           4195,
           4192,
           4192,
           4192,
           4192,
           4192,
           4192}),
      makeFlatVector<int8_t>({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
      makeNullableFlatVector<int32_t>(
          {1,
           2,
           -6,
           4,
           std::nullopt,
           79,
           std::nullopt,
           3,
           93,
           -221,
           std::nullopt,
           950,
           -4123,
           43,
           std::nullopt,
           std::nullopt}),
  });

  auto type = ROW(
      {{"id", INTEGER()},
       {"org_id", BIGINT()},
       {"deleted", TINYINT()},
       {"ratio", INTEGER()}});

  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);

  auto path = velox::test::getDataFilePath(
      "velox/dwio/text/tests/reader/", "examples/simple_types");
  auto readFile = std::make_shared<LocalReadFile>(path);

  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(type);

  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = factory->createReader(std::move(input), readerOptions);
  dwio::common::RowReaderOptions rowReaderOptions;
  setScanSpec(*type, rowReaderOptions);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  EXPECT_EQ(*reader->rowType(), *type);

  VectorPtr result;

  ASSERT_EQ(rowReader->next(10, result), 10);
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, i));
  }
  ASSERT_EQ(rowReader->next(6, result), 6);
  for (int i = 0; i < 6; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, 10 + i));
  }
}

TEST_F(TextReaderTest, simpleTypes) {
  const auto tzOffsetPST = 28'800;
  const auto tzOffsetPDT = 25'200;

  auto expected = makeRowVector({
      makeFlatVector<std::string>(
          {"FOO",
           "FOO",
           "FOO",
           "FOO",
           "BAR",
           "BAR",
           "BAR",
           "BAR",
           "BAZ",
           "BAZ",
           "BAZ",
           "FOO",
           "FOOBARBAZ"}),
      makeFlatVector<int64_t>(
          {4191,
           4192,
           4192,
           4196,
           4192,
           4193,
           4192,
           4192,
           4194,
           4192,
           4195,
           4192,
           4192}),
      makeFlatVector<bool>(
          {true,
           true,
           true,
           true,
           true,
           true,
           false,
           false,
           false,
           false,
           true,
           false,
           true}),
      makeNullableFlatVector<Timestamp>(
          {Timestamp{1'695'378'095 + tzOffsetPDT, 148'000'000},
           Timestamp{1'695'690'351 + tzOffsetPDT, 0},
           Timestamp{1'695'686'400 + tzOffsetPDT, 0},
           Timestamp{1'695'083'400 + tzOffsetPDT, 0},
           std::nullopt,
           Timestamp{1'695'657'091 + tzOffsetPDT, 209'000'000},
           Timestamp{1'695'690'437 + tzOffsetPDT, 469'123'000},
           Timestamp{1'696'540'679 + tzOffsetPDT, 976'000'000},
           Timestamp{1'695'657'171 + tzOffsetPDT, 637'000'000},
           Timestamp{1'695'693'225 + tzOffsetPDT, 745'123'000},
           std::nullopt,
           Timestamp{1'695'406'246 + tzOffsetPDT, 0},
           Timestamp{1'699'392'124 + tzOffsetPST, 736'000'000}}),
  });

  auto type = ROW(
      {{"col_string", VARCHAR()},
       {"col_big_int", BIGINT()},
       {"col_bool", BOOLEAN()},
       {"col_timestamp", TIMESTAMP()}});

  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);

  auto path = velox::test::getDataFilePath(
      "velox/dwio/text/tests/reader/", "examples/more_simple_types");
  auto readFile = std::make_shared<LocalReadFile>(path);

  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(type);

  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = factory->createReader(std::move(input), readerOptions);
  dwio::common::RowReaderOptions rowReaderOptions;
  setScanSpec(*type, rowReaderOptions);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  EXPECT_EQ(*reader->rowType(), *type);

  VectorPtr result;

  ASSERT_EQ(rowReader->next(11, result), 11);
  for (int i = 0; i < 11; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, i));
  }
  ASSERT_EQ(rowReader->next(6, result), 2);
  for (int i = 0; i < 2; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, 11 + i));
  }
}

TEST_F(TextReaderTest, DISABLED_nestedComplexTypesWithCustomDelimiters) {
  // Inner maps for the arrays
  const auto innerMapKeys1 = makeFlatVector<int64_t>({1, 11, 22});
  const auto innerMapValues1 = makeFlatVector<bool>({true, false, true});
  const auto innerMapKeys2 = makeFlatVector<int64_t>({33, 44});
  const auto innerMapValues2 = makeFlatVector<bool>({false, true});
  const auto innerMapKeys3 = makeFlatVector<int64_t>({55, 66, 77, 88});
  const auto innerMapValues3 = makeFlatVector<bool>({true, true, false, true});
  const auto innerMapKeys4 = makeFlatVector<int64_t>(std::vector<int64_t>{99});
  const auto innerMapValues4 = makeFlatVector<bool>(std::vector<bool>{false});
  const auto innerMapKeys5 = makeFlatVector<int64_t>({100, 200});
  const auto innerMapValues5 = makeFlatVector<bool>({true, false});
  const auto innerMapKeys6 = makeFlatVector<int64_t>({300, 400, 500});
  const auto innerMapValues6 = makeFlatVector<bool>({false, false, true});

  // Combine all inner map keys and values
  const auto allInnerMapKeys = makeFlatVector<int64_t>(
      {1, 11, 22, 33, 44, 55, 66, 77, 88, 99, 100, 200, 300, 400, 500});
  const auto allInnerMapValues = makeFlatVector<bool>(
      {true,
       false,
       true,
       false,
       true,
       true,
       true,
       false,
       true,
       false,
       true,
       false,
       false,
       false,
       true});

  // Create inner maps with proper offsets and sizes
  BufferPtr innerMapSizes = allocateOffsets(6, pool());
  BufferPtr innerMapOffsets = allocateOffsets(6, pool());
  auto rawInnerMapSizes = innerMapSizes->asMutable<vector_size_t>();
  auto rawInnerMapOffsets = innerMapOffsets->asMutable<vector_size_t>();

  rawInnerMapSizes[0] = 3; // {1:true, 11:false, 22:true}
  rawInnerMapSizes[1] = 2; // {33:false, 44:true}
  rawInnerMapSizes[2] = 4; // {55:true, 66:true, 77:false, 88:true}
  rawInnerMapSizes[3] = 1; // {99:false}
  rawInnerMapSizes[4] = 2; // {100:true, 200:false}
  rawInnerMapSizes[5] = 3; // {300:false, 400:false, 500:true}

  rawInnerMapOffsets[0] = 0;
  for (int i = 1; i < 6; i++) {
    rawInnerMapOffsets[i] = rawInnerMapOffsets[i - 1] + rawInnerMapSizes[i - 1];
  }

  auto innerMapsVector = std::make_shared<MapVector>(
      pool(),
      MAP(BIGINT(), BOOLEAN()),
      nullptr,
      6,
      innerMapOffsets,
      innerMapSizes,
      allInnerMapKeys,
      allInnerMapValues);

  // Create arrays containing the inner maps
  // Array 1: [innerMap0, innerMap1] (maps at indices 0, 1)
  // Array 2: [innerMap2, innerMap3, innerMap4] (maps at indices 2, 3, 4)
  // Array 3: [innerMap5] (map at index 5)
  auto arrayVector = makeArrayVector({0, 2, 5, 6}, innerMapsVector);

  // Create the outer map keys
  const auto outerMapKeys = makeFlatVector<int64_t>({10, 20, 30});

  // Create the final data structure
  auto outerMapOffsets = allocateOffsets(3, pool());
  auto outerMapSizes = allocateOffsets(3, pool());
  auto rawOuterMapOffsets = outerMapOffsets->asMutable<vector_size_t>();
  auto rawOuterMapSizes = outerMapSizes->asMutable<vector_size_t>();

  // Set offsets: [0, 1, 2]
  rawOuterMapOffsets[0] = 0;
  rawOuterMapOffsets[1] = 1;
  rawOuterMapOffsets[2] = 2;

  // Set sizes: [1, 1, 1] (each outer map entry contains 1 array)
  rawOuterMapSizes[0] = 1;
  rawOuterMapSizes[1] = 1;
  rawOuterMapSizes[2] = 1;

  const auto expected = makeRowVector({std::make_shared<MapVector>(
      pool(),
      MAP(BIGINT(), ARRAY(MAP(BIGINT(), BOOLEAN()))),
      nullptr,
      3,
      outerMapOffsets,
      outerMapSizes,
      outerMapKeys,
      arrayVector)});

  const auto type =
      ROW({{"col_nested_map", MAP(BIGINT(), ARRAY(MAP(BIGINT(), BOOLEAN())))}});
  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);

  auto path = velox::test::getDataFilePath(
      "velox/dwio/text/tests/reader/",
      "examples/custom_delimiter_nested_complex_types");
  auto readFile = std::make_shared<LocalReadFile>(path);

  // Set up custom delimiters:
  // - Tab ('\t') for field separation (depth 0)
  // - Pipe ('|') for array element separation (depth 1)
  // - Hash ('#') for map key-value separation (depth 2)
  // - ',' for inner map key-value pair separation (depth 3)
  auto serDeOptions = dwio::common::SerDeOptions('\t', '=', '|', '\\', true);
  serDeOptions.separators[3] = ',';
  serDeOptions.separators[4] = ':';
  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(type);
  readerOptions.setSerDeOptions(serDeOptions);

  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = factory->createReader(std::move(input), readerOptions);
  auto rowReaderOptions = dwio::common::RowReaderOptions();
  setScanSpec(*type, rowReaderOptions);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  EXPECT_EQ(*reader->rowType(), *type);

  VectorPtr result;

  // Read all 3 rows
  ASSERT_EQ(rowReader->next(10, result), 3);
  for (int i = 0; i < 3; ++i) {
    LOG(INFO) << "Row " << i << ":\n"
              << std::static_pointer_cast<RowVector>(result)->toString(i)
              << "\nVS\n"
              << expected->toString(i);
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, i));
  }
  ASSERT_EQ(rowReader->next(10, result), 0);
}

TEST_F(TextReaderTest, nestedArraysWithCustomDelimiters) {
  // Create expected nested array structure
  // Row 1: [[1,2,3], [4,5], [6,7,8,9]]
  // Row 2: [[10,20], [30,40,50], [60]]
  // Row 3: [[100], [200,300,400], [500,600]]

  // Create inner arrays for each row
  auto innerArraysRow1 =
      makeArrayVector<int64_t>({{1, 2, 3}, {4, 5}, {6, 7, 8, 9}});
  auto innerArraysRow2 =
      makeArrayVector<int64_t>({{10, 20}, {30, 40, 50}, {60}});
  auto innerArraysRow3 =
      makeArrayVector<int64_t>({{100}, {200, 300, 400}, {500, 600}});

  // Combine all inner arrays into a single vector
  auto allInnerArrays = makeArrayVector<int64_t>({
      {1, 2, 3},
      {4, 5},
      {6, 7, 8, 9}, // Row 1 inner arrays
      {10, 20},
      {30, 40, 50},
      {60}, // Row 2 inner arrays
      {100},
      {200, 300, 400},
      {500, 600} // Row 3 inner arrays
  });

  // Create the outer array structure with proper offsets
  // Row 1: uses inner arrays 0, 1, 2
  // Row 2: uses inner arrays 3, 4, 5
  // Row 3: uses inner arrays 6, 7, 8
  auto outerArray = makeArrayVector({0, 3, 6, 9}, allInnerArrays);

  const auto expected = makeRowVector({outerArray});

  const auto type = ROW({{"col_nested_array", ARRAY(ARRAY(BIGINT()))}});
  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);

  auto path = velox::test::getDataFilePath(
      "velox/dwio/text/tests/reader/", "examples/nested_arrays_file");
  auto readFile = std::make_shared<LocalReadFile>(path);

  // Set up custom delimiters for nested arrays:
  // - Tab ('\t') for field separation (depth 0) - not used in single-column
  // case
  // - Pipe ('|') for outer array element separation (depth 1)
  // - Comma (',') for inner array element separation (depth 2)
  auto serDeOptions = dwio::common::SerDeOptions('\t', '|', ',', '\\', true);
  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(type);
  readerOptions.setSerDeOptions(serDeOptions);

  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = factory->createReader(std::move(input), readerOptions);
  auto rowReaderOptions = dwio::common::RowReaderOptions();
  setScanSpec(*type, rowReaderOptions);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  EXPECT_EQ(*reader->rowType(), *type);

  VectorPtr result;

  // Read all 3 rows
  ASSERT_EQ(rowReader->next(10, result), 3);
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, i));
  }
  ASSERT_EQ(rowReader->next(10, result), 0);
}

TEST_F(TextReaderTest, tripleNestedArraysWithCustomDelimiters) {
  // Create expected triple nested array structure
  // Row 1: [[[1,2], [3,4]], [[5,6,7], [8,9]], [[10,11,12,13], [14,15]]]
  // Row 2: [[[20,21], [22,23,24]], [[25,26]], [[27,28,29], [30,31,32]]]
  // Row 3: [[[100,101,102]], [[200,201], [300,301,302,303]], [[400,401,402],
  // [500,501]]]

  // Create the innermost arrays (level 3)
  auto innermostArrays = makeArrayVector<int64_t>({
      {1, 2},
      {3, 4}, // Row 1, outer array 0
      {5, 6, 7},
      {8, 9}, // Row 1, outer array 1
      {10, 11, 12, 13},
      {14, 15}, // Row 1, outer array 2
      {20, 21},
      {22, 23, 24}, // Row 2, outer array 0
      {25, 26}, // Row 2, outer array 1
      {27, 28, 29},
      {30, 31, 32}, // Row 2, outer array 2
      {100, 101, 102}, // Row 3, outer array 0
      {200, 201},
      {300, 301, 302, 303}, // Row 3, outer array 1
      {400, 401, 402},
      {500, 501} // Row 3, outer array 2
  });

  // Create middle level arrays (level 2) - each contains innermost arrays
  auto middleArrays = makeArrayVector(
      {
          0, // Row 1, outer array 0: contains innermost arrays [0,1]
          2, // Row 1, outer array 1: contains innermost arrays [2,3]
          4, // Row 1, outer array 2: contains innermost arrays [4,5]
          6, // Row 2, outer array 0: contains innermost arrays [6,7]
          8, // Row 2, outer array 1: contains innermost arrays [8]
          9, // Row 2, outer array 2: contains innermost arrays [9,10]
          11, // Row 3, outer array 0: contains innermost arrays [11]
          12, // Row 3, outer array 1: contains innermost arrays [12,13]
          14, // Row 3, outer array 2: contains innermost arrays [14,15]
          16 // End marker
      },
      innermostArrays);

  // Create outermost arrays (level 1) - each row contains middle arrays
  auto outerArray = makeArrayVector(
      {
          0, 3, 6, 9 // Row boundaries: Row 1 [0-2], Row 2 [3-5], Row 3 [6-8]
      },
      middleArrays);

  const auto expected = makeRowVector({outerArray});

  const auto type =
      ROW({{"col_triple_nested_array", ARRAY(ARRAY(ARRAY(BIGINT())))}});
  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);

  auto path = velox::test::getDataFilePath(
      "velox/dwio/text/tests/reader/", "examples/triple_nested_arrays_file");
  auto readFile = std::make_shared<LocalReadFile>(path);

  // Set up custom delimiters for triple nested arrays:
  // - Tab ('\t') for field separation (depth 0) - not used in single-column
  // case
  // - Pipe ('|') for outermost array element separation (depth 1)
  // - Comma (',') for middle array element separation (depth 2)
  // - Hash ('#') for innermost array element separation (depth 3)
  auto serDeOptions = dwio::common::SerDeOptions('\t', '|', ',', '\\', true);
  serDeOptions.separators[3] = '#';
  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(type);
  readerOptions.setSerDeOptions(serDeOptions);

  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = factory->createReader(std::move(input), readerOptions);
  auto rowReaderOptions = dwio::common::RowReaderOptions();
  setScanSpec(*type, rowReaderOptions);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  EXPECT_EQ(*reader->rowType(), *type);

  VectorPtr result;

  // Read all 3 rows
  ASSERT_EQ(rowReader->next(10, result), 3);
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, i));
  }
  ASSERT_EQ(rowReader->next(10, result), 0);
}

} // namespace

} // namespace facebook::velox::text
