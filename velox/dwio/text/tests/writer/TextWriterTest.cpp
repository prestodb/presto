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

#include "velox/dwio/text/writer/TextWriter.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/text/RegisterTextReader.h"
#include "velox/dwio/text/RegisterTextWriter.h"
#include "velox/dwio/text/tests/writer/FileReaderUtil.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

/// TODO: Add fuzzer test.

namespace facebook::velox::text {
class TextWriterTest : public testing::Test,
                       public velox::test::VectorTestBase {
 public:
  void SetUp() override {
    velox::filesystems::registerLocalFileSystem();
    registerTextWriterFactory();
    dwio::common::registerTextReaderFactory();
    rootPool_ = memory::memoryManager()->addRootPool("TextWriterTests");
    leafPool_ = rootPool_->addLeafChild("TextWriterTests");
    tempPath_ = exec::test::TempDirectoryPath::create();
  }

  void TearDown() override {
    dwio::common::unregisterTextReaderFactory();
    unregisterTextWriterFactory();
    dwio::common::unregisterTextReaderFactory();
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void setScanSpec(const Type& type, dwio::common::RowReaderOptions& options) {
    auto spec = std::make_shared<common::ScanSpec>("root");
    spec->addAllChildFields(type);
    options.setScanSpec(spec);
  }

  memory::MemoryPool& poolRef() {
    return *pool();
  }

  constexpr static float kInf = std::numeric_limits<float>::infinity();
  constexpr static double kNaN = std::numeric_limits<double>::quiet_NaN();
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::shared_ptr<exec::test::TempDirectoryPath> tempPath_;
};

TEST_F(TextWriterTest, write) {
  auto schema =
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"},
          {BOOLEAN(),
           TINYINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           TIMESTAMP(),
           VARCHAR(),
           VARBINARY()});
  auto data = makeRowVector(
      {"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"},
      {
          makeConstant(true, 3),
          makeFlatVector<int8_t>({1, 2, 3}),
          makeFlatVector<int16_t>({1, 2, 3}), // TODO null
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<int64_t>({1, 2, 3}),
          makeFlatVector<float>({1.1, kInf, 3.1}),
          makeFlatVector<double>({1.1, kNaN, 3.1}),
          makeFlatVector<Timestamp>(
              3, [](auto i) { return Timestamp(i, i * 1'000'000); }),
          makeFlatVector<StringView>({"hello", "world", "cpp"}, VARCHAR()),
          makeFlatVector<StringView>({"hello", "world", "cpp"}, VARBINARY()),
      });

  WriterOptions writerOptions;
  writerOptions.memoryPool = rootPool_.get();

  const auto tempPath = tempPath_->getPath();
  const auto filename = "test_text_writer.txt";
  auto filePath = fs::path(fmt::format("{}/{}", tempPath, filename));
  auto sink = std::make_unique<dwio::common::LocalFileSink>(
      filePath, dwio::common::FileSink::Options{.pool = leafPool_.get()});
  auto writer = std::make_unique<TextWriter>(
      schema,
      std::move(sink),
      std::make_shared<text::WriterOptions>(writerOptions));
  writer->write(data);
  writer->close();

  const auto fs = filesystems::getFileSystem(tempPath, nullptr);
  const auto& file = fs->openFileForRead(filePath.string());
  auto fileSize = file->size();

  BufferPtr charBuf = AlignedBuffer::allocate<char>(fileSize, pool());
  auto rawCharBuf = charBuf->asMutable<char>();
  std::vector<std::vector<std::string>> result =
      parseTextFile(tempPath, filename, rawCharBuf);

  EXPECT_EQ(result.size(), 3);
  EXPECT_EQ(result[0].size(), 10);

  // bool type
  EXPECT_EQ(result[0][0], "true");
  EXPECT_EQ(result[1][0], "true");
  EXPECT_EQ(result[2][0], "true");

  // tinyint
  EXPECT_EQ(result[0][1], "1");
  EXPECT_EQ(result[1][1], "2");
  EXPECT_EQ(result[2][1], "3");

  // smallint
  EXPECT_EQ(result[0][2], "1");
  EXPECT_EQ(result[1][2], "2");
  EXPECT_EQ(result[2][2], "3");

  // int
  EXPECT_EQ(result[0][3], "1");
  EXPECT_EQ(result[1][3], "2");
  EXPECT_EQ(result[2][3], "3");

  // bigint
  EXPECT_EQ(result[0][4], "1");
  EXPECT_EQ(result[1][4], "2");
  EXPECT_EQ(result[2][4], "3");

  // float
  EXPECT_EQ(result[0][5], "1.100000");
  EXPECT_EQ(result[1][5], "Infinity");
  EXPECT_EQ(result[2][5], "3.100000");

  // double
  EXPECT_EQ(result[0][6], "1.100000");
  EXPECT_EQ(result[1][6], "NaN");
  EXPECT_EQ(result[2][6], "3.100000");

  // timestamp
  EXPECT_EQ(result[0][7], "1969-12-31 16:00:00.000");
  EXPECT_EQ(result[1][7], "1969-12-31 16:00:01.001");
  EXPECT_EQ(result[2][7], "1969-12-31 16:00:02.002");

  // varchar
  EXPECT_EQ(result[0][8], "hello");
  EXPECT_EQ(result[1][8], "world");
  EXPECT_EQ(result[2][8], "cpp");

  // varbinary
  EXPECT_EQ(result[0][9], "aGVsbG8=");
  EXPECT_EQ(result[1][9], "d29ybGQ=");
  EXPECT_EQ(result[2][9], "Y3Bw");
}

TEST_F(TextWriterTest, verifyWriteWithTextReader) {
  auto schema =
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"},
          {
              TIMESTAMP(),
              BOOLEAN(),
              TINYINT(),
              SMALLINT(),
              INTEGER(),
              BIGINT(),
              REAL(),
              DOUBLE(),
              VARCHAR(),
              VARBINARY(),
          });
  auto data = makeRowVector(
      {"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"},
      {
          makeFlatVector<Timestamp>(
              3, [](auto i) { return Timestamp(i, i * 1'000'000); }),
          makeConstant(true, 3),
          makeFlatVector<int8_t>({1, 2, 3}),
          makeFlatVector<int16_t>({1, 2, 3}), // TODO null
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<int64_t>({1, 2, 3}),
          makeFlatVector<float>({1.1, kInf, 3.1}),
          makeFlatVector<double>({1.1, kNaN, 3.1}),
          makeFlatVector<StringView>({"hello", "world", "cpp"}, VARCHAR()),
          makeFlatVector<StringView>({"hello", "world", "cpp"}, VARBINARY()),
      });

  WriterOptions writerOptions;
  writerOptions.memoryPool = rootPool_.get();

  const auto tempPath = tempPath_->getPath();
  const auto filename = "test_text_writer.txt";
  auto filePath = fs::path(fmt::format("{}/{}", tempPath, filename));
  auto sink = std::make_unique<dwio::common::LocalFileSink>(
      filePath.string(),
      dwio::common::FileSink::Options{.pool = leafPool_.get()});
  auto writer = std::make_unique<TextWriter>(
      schema,
      std::move(sink),
      std::make_shared<text::WriterOptions>(writerOptions));
  writer->write(data);
  writer->close();

  // Set up reader.
  auto readerFactory =
      dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);
  auto readFile = std::make_shared<LocalReadFile>(filePath.string());
  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(schema);
  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = readerFactory->createReader(std::move(input), readerOptions);
  dwio::common::RowReaderOptions rowReaderOptions;
  setScanSpec(*schema, rowReaderOptions);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  EXPECT_EQ(*reader->rowType(), *schema);

  VectorPtr result;

  ASSERT_EQ(rowReader->next(3, result), 3);
  for (int i = 0; i < 3; ++i) {
    LOG(INFO) << std::static_pointer_cast<RowVector>(result)->toString(i)
              << "\nVS\n"
              << data->toString(i);
    EXPECT_TRUE(result->equalValueAt(data.get(), i, i));
  }
}

TEST_F(TextWriterTest, mapAndArrayComplexTypes) {
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

  const auto data = makeRowVector(
      {makeArrayVector<int64_t>(
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
           valueVector)});

  const auto schema = ROW(
      {{"col_bigint_arr", ARRAY(BIGINT())},
       {"col_double_arr", ARRAY(DOUBLE())},
       {"col_map", MAP(BIGINT(), BOOLEAN())}});

  WriterOptions writerOptions;
  writerOptions.memoryPool = rootPool_.get();
  const auto tempPath = tempPath_->getPath();
  const auto filename = "test_text_writer.txt";
  auto filePath = fs::path(fmt::format("{}/{}", tempPath, filename));

  auto sink = std::make_unique<dwio::common::LocalFileSink>(
      filePath.string(),
      dwio::common::FileSink::Options{.pool = leafPool_.get()});

  // use traits to specify delimiters when it is not nested
  const auto serDeOptions = dwio::common::SerDeOptions('\x01', '|', '#');
  auto writer = std::make_unique<TextWriter>(
      schema,
      std::move(sink),
      std::make_shared<text::WriterOptions>(writerOptions),
      serDeOptions);
  writer->write(data);
  writer->close();

  const auto fs = filesystems::getFileSystem(tempPath, nullptr);
  const auto& file = fs->openFileForRead(filePath.string());
  auto fileSize = file->size();

  BufferPtr charBuf = AlignedBuffer::allocate<char>(fileSize, pool());
  auto rawCharBuf = charBuf->asMutable<char>();
  std::vector<std::vector<std::string>> result =
      parseTextFile(tempPath, filename, rawCharBuf, serDeOptions);

  EXPECT_EQ(result.size(), 13);
  EXPECT_EQ(result[0].size(), 3);

  // col_bigint_arr
  EXPECT_EQ(result[0][0], "1|11|111");
  EXPECT_EQ(result[1][0], "22|22222");
  EXPECT_EQ(result[2][0], "333|33");
  EXPECT_EQ(result[3][0], "4444|44");
  EXPECT_EQ(result[4][0], "5|555");
  EXPECT_EQ(result[5][0], "666|66|66");
  EXPECT_EQ(result[6][0], "7777|7|777");
  EXPECT_EQ(result[7][0], "8888|88");
  EXPECT_EQ(result[8][0], "9|99");
  EXPECT_EQ(result[9][0], "10|10000");
  EXPECT_EQ(result[10][0], "111|1|111");
  EXPECT_EQ(result[11][0], "12|11122222|222");
  EXPECT_EQ(result[12][0], "13|11133333|333");

  // col_double_arr
  EXPECT_EQ(result[0][1], "1.123000|1.312300");
  EXPECT_EQ(result[1][1], "2.333000|-5512.000000|1.230000");
  EXPECT_EQ(result[2][1], "-6.100000|65.777000");
  EXPECT_EQ(result[3][1], "4.200000|24.000000|324.110000");
  EXPECT_EQ(result[4][1], "47.200000|213.230000");
  EXPECT_EQ(result[5][1], "79.500000|-44.110000");
  EXPECT_EQ(result[6][1], "3.141593|441.124000");
  EXPECT_EQ(result[7][1], "-221.145000|878.430000|-11.000000");
  EXPECT_EQ(result[8][1], "93.120000|632.000000");
  EXPECT_EQ(result[9][1], "-4123.110000|-177.100000");
  EXPECT_EQ(result[10][1], "950.200000|-4412.000000");
  EXPECT_EQ(result[11][1], "43.660000|33121.430000");
  EXPECT_EQ(result[12][1], "-42.110000|-123.430000");

  // col_map
  EXPECT_EQ(result[0][2], "1#false|111#true");
  EXPECT_EQ(result[1][2], "22#true|22222#false");
  EXPECT_EQ(result[2][2], "333#false|33#true");
  EXPECT_EQ(result[3][2], "44#false");
  EXPECT_EQ(result[4][2], "5#true|555#false");
  EXPECT_EQ(result[5][2], "66#true");
  EXPECT_EQ(result[6][2], "7777#false|7#true|777#false");
  EXPECT_EQ(result[7][2], "8888#false|88#true");
  EXPECT_EQ(result[8][2], "9#true|99#false");
  EXPECT_EQ(result[9][2], "10#true|10000#false");
  EXPECT_EQ(result[10][2], "111#true|1#false|11#true");
  EXPECT_EQ(result[11][2], "11122222#true");
  EXPECT_EQ(result[12][2], "123142#true");
}

TEST_F(TextWriterTest, verifyMapAndArrayComplexTypesWithTextReader) {
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

  const auto data = makeRowVector(
      {makeArrayVector<int64_t>(
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
           valueVector)});

  const auto schema = ROW(
      {{"col_bigint_arr", ARRAY(BIGINT())},
       {"col_double_arr", ARRAY(DOUBLE())},
       {"col_map", MAP(BIGINT(), BOOLEAN())}});

  WriterOptions writerOptions;
  writerOptions.memoryPool = rootPool_.get();
  auto filePath =
      fs::path(fmt::format("{}/test_text_writer.txt", tempPath_->getPath()));

  auto sink = std::make_unique<dwio::common::LocalFileSink>(
      filePath.string(),
      dwio::common::FileSink::Options{.pool = leafPool_.get()});

  // use traits to specify delimiters when it is not nested
  const auto serDeOptions = dwio::common::SerDeOptions('\x01', '|', '#');
  auto writer = std::make_unique<TextWriter>(
      schema,
      std::move(sink),
      std::make_shared<text::WriterOptions>(writerOptions),
      serDeOptions);
  writer->write(data);
  writer->close();

  // Set up reader.
  auto readerFactory =
      dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);
  auto readFile = std::make_shared<LocalReadFile>(filePath.string());
  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(schema);
  readerOptions.setSerDeOptions(serDeOptions);
  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = readerFactory->createReader(std::move(input), readerOptions);
  dwio::common::RowReaderOptions rowReaderOptions;
  setScanSpec(*schema, rowReaderOptions);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  // Change the expected value
  const auto expected = makeRowVector(
      {makeArrayVector<int64_t>(
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
            {3.141593, 441.124},
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
           valueVector)});

  EXPECT_EQ(*reader->rowType(), *schema);

  VectorPtr result;
  ASSERT_EQ(rowReader->next(13, result), 13);
  for (int i = 0; i < 13; ++i) {
    EXPECT_TRUE(result->equalValueAt(expected.get(), i, i));
  }
}

TEST_F(TextWriterTest, arrayTypes) {
  // Test specifically for ARRAY types with various element types
  const auto data = makeRowVector({
      makeArrayVector<int32_t>(
          {{1, 2, 3},
           {10, 20},
           {100, 200, 300, 400},
           {}, // empty array
           {42}}),
      makeArrayVector<std::string>(
          {{"hello", "world"},
           {"foo", "bar", "baz"},
           {"single"},
           {}, // empty array
           {"test", "array", "string"}}),
      makeArrayVector<double>(
          {{1.1, 2.2, 3.3},
           {10.5, 20.7},
           {}, // empty array
           {99.99},
           {1.0, 2.0, 3.0, 4.0, 5.0}}),
  });

  const auto schema = ROW(
      {{"int_array", ARRAY(INTEGER())},
       {"string_array", ARRAY(VARCHAR())},
       {"double_array", ARRAY(DOUBLE())}});

  WriterOptions writerOptions;
  writerOptions.memoryPool = rootPool_.get();
  const auto tempPath = tempPath_->getPath();
  const auto filename = "test_array_writer.txt";

  auto filePath = fs::path(fmt::format("{}/{}", tempPath, filename));

  auto sink = std::make_unique<dwio::common::LocalFileSink>(
      filePath, dwio::common::FileSink::Options{.pool = leafPool_.get()});

  const auto serDeOptions = dwio::common::SerDeOptions('\x01', '|', '#');
  auto writer = std::make_unique<TextWriter>(
      schema,
      std::move(sink),
      std::make_shared<text::WriterOptions>(writerOptions),
      serDeOptions);
  writer->write(data);
  writer->close();

  const auto fs = filesystems::getFileSystem(tempPath, nullptr);
  const auto& file = fs->openFileForRead(filePath.string());
  auto fileSize = file->size();

  BufferPtr charBuf = AlignedBuffer::allocate<char>(fileSize, pool());
  auto rawCharBuf = charBuf->asMutable<char>();
  std::vector<std::vector<std::string>> result =
      parseTextFile(tempPath, filename, rawCharBuf, serDeOptions);

  EXPECT_EQ(result.size(), 5);
  EXPECT_EQ(result[0].size(), 3);

  // int array type
  EXPECT_EQ(result[0][0], "1|2|3");
  EXPECT_EQ(result[1][0], "10|20");
  EXPECT_EQ(result[2][0], "100|200|300|400");
  EXPECT_EQ(result[3][0], "");
  EXPECT_EQ(result[4][0], "42");

  // varchar array type
  EXPECT_EQ(result[0][1], "hello|world");
  EXPECT_EQ(result[1][1], "foo|bar|baz");
  EXPECT_EQ(result[2][1], "single");
  EXPECT_EQ(result[3][1], "");
  EXPECT_EQ(result[4][1], "test|array|string");

  // double array type
  EXPECT_EQ(result[0][2], "1.100000|2.200000|3.300000");
  EXPECT_EQ(result[1][2], "10.500000|20.700000");
  EXPECT_EQ(result[2][2], "");
  EXPECT_EQ(result[3][2], "99.990000");
  EXPECT_EQ(result[4][2], "1.000000|2.000000|3.000000|4.000000|5.000000");
}

TEST_F(TextWriterTest, verifyArrayTypesWithTextReader) {
  // Test specifically for ARRAY types with various element types
  const auto data = makeRowVector({
      makeArrayVector<int32_t>(
          {{1, 2, 3},
           {10, 20},
           {100, 200, 300, 400},
           {}, // empty array
           {42}}),
      makeArrayVector<std::string>(
          {{"hello", "world"},
           {"foo", "bar", "baz"},
           {"single"},
           {}, // empty array
           {"test", "array", "string"}}),
      makeArrayVector<double>(
          {{1.1, 2.2, 3.3},
           {10.5, 20.7},
           {}, // empty array
           {99.99},
           {1.0, 2.0, 3.0, 4.0, 5.0}}),
  });

  const auto schema = ROW(
      {{"int_array", ARRAY(INTEGER())},
       {"string_array", ARRAY(VARCHAR())},
       {"double_array", ARRAY(DOUBLE())}});

  WriterOptions writerOptions;
  writerOptions.memoryPool = rootPool_.get();
  const auto tempPath = tempPath_->getPath();
  const auto filename = "test_array_writer.txt";

  auto filePath = fs::path(fmt::format("{}/{}", tempPath, filename));

  auto sink = std::make_unique<dwio::common::LocalFileSink>(
      filePath, dwio::common::FileSink::Options{.pool = leafPool_.get()});

  const auto serDeOptions = dwio::common::SerDeOptions('\x01', '|', '#');
  auto writer = std::make_unique<TextWriter>(
      schema,
      std::move(sink),
      std::make_shared<text::WriterOptions>(writerOptions),
      serDeOptions);
  writer->write(data);
  writer->close();

  // Set up reader.
  auto readerFactory =
      dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);
  auto readFile = std::make_shared<LocalReadFile>(filePath.string());
  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(schema);
  readerOptions.setSerDeOptions(serDeOptions);

  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = readerFactory->createReader(std::move(input), readerOptions);
  dwio::common::RowReaderOptions rowReaderOptions;
  setScanSpec(*schema, rowReaderOptions);

  auto rowReader = reader->createRowReader(rowReaderOptions);

  EXPECT_EQ(*reader->rowType(), *schema);

  VectorPtr result;

  ASSERT_EQ(rowReader->next(5, result), 5);
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(result->equalValueAt(data.get(), i, i));
  }
  ASSERT_EQ(rowReader->next(10, result), 0);
}

TEST_F(TextWriterTest, mapTypes) {
  // Test specifically for MAP types with various key-value combinations
  const vector_size_t length = 5;

  // Create key and value vectors for the maps
  const auto keyVector =
      makeFlatVector<int64_t>({1, 2, 3, 10, 20, 100, 42, 99, 123, 456});
  const auto valueVector = makeFlatVector<bool>(
      {true, false, true, false, true, true, false, true, false, true});

  // Set up offsets and sizes for each row
  BufferPtr sizes = facebook::velox::allocateOffsets(length, pool());
  BufferPtr offsets = facebook::velox::allocateOffsets(length, pool());
  auto rawSizes = sizes->asMutable<vector_size_t>();
  auto rawOffsets = offsets->asMutable<vector_size_t>();

  rawSizes[0] = 3; // Row 0: 3 key-value pairs
  rawSizes[1] = 2; // Row 1: 2 key-value pairs
  rawSizes[2] = 1; // Row 2: 1 key-value pair
  rawSizes[3] = 0; // Row 3: 0 key-value pairs (empty map)
  rawSizes[4] = 4; // Row 4: 4 key-value pairs

  rawOffsets[0] = 0;
  for (int i = 1; i < length; i++) {
    rawOffsets[i] = rawOffsets[i - 1] + rawSizes[i - 1];
  }

  // Create string keys for a second map column
  const auto stringKeyVector = makeFlatVector<std::string>(
      {"key1", "key2", "foo", "bar", "baz", "qux", "test"});
  const auto intValueVector =
      makeFlatVector<int32_t>({10, 20, 100, 1, 2, 3, 999});

  // Set up offsets and sizes for string key map
  BufferPtr stringSizes = facebook::velox::allocateOffsets(length, pool());
  BufferPtr stringOffsets = facebook::velox::allocateOffsets(length, pool());
  auto rawStringSizes = stringSizes->asMutable<vector_size_t>();
  auto rawStringOffsets = stringOffsets->asMutable<vector_size_t>();

  rawStringSizes[0] = 2; // Row 0: 2 key-value pairs
  rawStringSizes[1] = 1; // Row 1: 1 key-value pair
  rawStringSizes[2] = 3; // Row 2: 3 key-value pairs
  rawStringSizes[3] = 0; // Row 3: 0 key-value pairs (empty map)
  rawStringSizes[4] = 1; // Row 4: 1 key-value pair

  rawStringOffsets[0] = 0;
  for (int i = 1; i < length; i++) {
    rawStringOffsets[i] = rawStringOffsets[i - 1] + rawStringSizes[i - 1];
  }

  const auto data = makeRowVector(
      {std::make_shared<MapVector>(
           pool(),
           MAP(BIGINT(), BOOLEAN()),
           nullptr,
           length,
           offsets,
           sizes,
           keyVector,
           valueVector),
       std::make_shared<MapVector>(
           pool(),
           MAP(VARCHAR(), INTEGER()),
           nullptr,
           length,
           stringOffsets,
           stringSizes,
           stringKeyVector,
           intValueVector)});

  const auto schema = ROW(
      {{"int_bool_map", MAP(BIGINT(), BOOLEAN())},
       {"string_int_map", MAP(VARCHAR(), INTEGER())}});

  WriterOptions writerOptions;
  writerOptions.memoryPool = rootPool_.get();
  const auto tempPath = tempPath_->getPath();
  const auto filename = "test_map_writer.txt";

  auto filePath = fs::path(fmt::format("{}/{}", tempPath, filename));
  auto sink = std::make_unique<dwio::common::LocalFileSink>(
      filePath, dwio::common::FileSink::Options{.pool = leafPool_.get()});

  const auto serDeOptions = dwio::common::SerDeOptions('\x01', '|', '#');
  auto writer = std::make_unique<TextWriter>(
      schema,
      std::move(sink),
      std::make_shared<text::WriterOptions>(writerOptions),
      serDeOptions);
  writer->write(data);
  writer->close();

  const auto fs = filesystems::getFileSystem(tempPath, nullptr);
  const auto& file = fs->openFileForRead(filePath.string());
  auto fileSize = file->size();

  BufferPtr charBuf = AlignedBuffer::allocate<char>(fileSize, pool());
  auto rawCharBuf = charBuf->asMutable<char>();
  std::vector<std::vector<std::string>> result =
      parseTextFile(tempPath, filename, rawCharBuf, serDeOptions);

  EXPECT_EQ(result.size(), 5);
  EXPECT_EQ(result[0].size(), 2);

  // int_bool_map
  EXPECT_EQ(result[0][0], "1#true|2#false|3#true");
  EXPECT_EQ(result[1][0], "10#false|20#true");
  EXPECT_EQ(result[2][0], "100#true");
  EXPECT_EQ(result[3][0], "");
  EXPECT_EQ(result[4][0], "42#false|99#true|123#false|456#true");

  // string_int_map
  EXPECT_EQ(result[0][1], "key1#10|key2#20");
  EXPECT_EQ(result[1][1], "foo#100");
  EXPECT_EQ(result[2][1], "bar#1|baz#2|qux#3");
  EXPECT_EQ(result[3][1], "");
  EXPECT_EQ(result[4][1], "test#999");
}

TEST_F(TextWriterTest, verifyMapTypesWithTextReader) {
  // Test specifically for MAP types with various key-value combinations
  const vector_size_t length = 5;

  // Create key and value vectors for the maps
  const auto keyVector =
      makeFlatVector<int64_t>({1, 2, 3, 10, 20, 100, 42, 99, 123, 456});
  const auto valueVector = makeFlatVector<bool>(
      {true, false, true, false, true, true, false, true, false, true});

  // Set up offsets and sizes for each row
  BufferPtr sizes = facebook::velox::allocateOffsets(length, pool());
  BufferPtr offsets = facebook::velox::allocateOffsets(length, pool());
  auto rawSizes = sizes->asMutable<vector_size_t>();
  auto rawOffsets = offsets->asMutable<vector_size_t>();

  rawSizes[0] = 3; // Row 0: 3 key-value pairs
  rawSizes[1] = 2; // Row 1: 2 key-value pairs
  rawSizes[2] = 1; // Row 2: 1 key-value pair
  rawSizes[3] = 0; // Row 3: 0 key-value pairs (empty map)
  rawSizes[4] = 4; // Row 4: 4 key-value pairs

  rawOffsets[0] = 0;
  for (int i = 1; i < length; i++) {
    rawOffsets[i] = rawOffsets[i - 1] + rawSizes[i - 1];
  }

  // Create string keys for a second map column
  const auto stringKeyVector = makeFlatVector<std::string>(
      {"key1", "key2", "foo", "bar", "baz", "qux", "test"});
  const auto intValueVector =
      makeFlatVector<int32_t>({10, 20, 100, 1, 2, 3, 999});

  // Set up offsets and sizes for string key map
  BufferPtr stringSizes = facebook::velox::allocateOffsets(length, pool());
  BufferPtr stringOffsets = facebook::velox::allocateOffsets(length, pool());
  auto rawStringSizes = stringSizes->asMutable<vector_size_t>();
  auto rawStringOffsets = stringOffsets->asMutable<vector_size_t>();

  rawStringSizes[0] = 2; // Row 0: 2 key-value pairs
  rawStringSizes[1] = 1; // Row 1: 1 key-value pair
  rawStringSizes[2] = 3; // Row 2: 3 key-value pairs
  rawStringSizes[3] = 0; // Row 3: 0 key-value pairs (empty map)
  rawStringSizes[4] = 1; // Row 4: 1 key-value pair

  rawStringOffsets[0] = 0;
  for (int i = 1; i < length; i++) {
    rawStringOffsets[i] = rawStringOffsets[i - 1] + rawStringSizes[i - 1];
  }

  const auto data = makeRowVector(
      {std::make_shared<MapVector>(
           pool(),
           MAP(BIGINT(), BOOLEAN()),
           nullptr,
           length,
           offsets,
           sizes,
           keyVector,
           valueVector),
       std::make_shared<MapVector>(
           pool(),
           MAP(VARCHAR(), INTEGER()),
           nullptr,
           length,
           stringOffsets,
           stringSizes,
           stringKeyVector,
           intValueVector)});

  const auto schema = ROW(
      {{"int_bool_map", MAP(BIGINT(), BOOLEAN())},
       {"string_int_map", MAP(VARCHAR(), INTEGER())}});

  WriterOptions writerOptions;
  writerOptions.memoryPool = rootPool_.get();
  const auto tempPath = tempPath_->getPath();
  const auto filename = "test_map_writer.txt";

  auto filePath = fs::path(fmt::format("{}/{}", tempPath, filename));
  auto sink = std::make_unique<dwio::common::LocalFileSink>(
      filePath, dwio::common::FileSink::Options{.pool = leafPool_.get()});

  const auto serDeOptions = dwio::common::SerDeOptions('\x01', '|', '#');
  auto writer = std::make_unique<TextWriter>(
      schema,
      std::move(sink),
      std::make_shared<text::WriterOptions>(writerOptions),
      serDeOptions);
  writer->write(data);
  writer->close();

  // Set up reader
  auto readerFactory =
      dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);
  auto readFile = std::make_shared<LocalReadFile>(filePath.string());
  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(schema);
  readerOptions.setSerDeOptions(serDeOptions);

  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = readerFactory->createReader(std::move(input), readerOptions);
  dwio::common::RowReaderOptions rowReaderOptions;
  setScanSpec(*schema, rowReaderOptions);

  auto rowReader = reader->createRowReader(rowReaderOptions);

  EXPECT_EQ(*reader->rowType(), *schema);

  VectorPtr result;

  ASSERT_EQ(rowReader->next(5, result), 5);
  for (int i = 0; i < 5; ++i) {
    LOG(INFO) << std::static_pointer_cast<RowVector>(result)->toString(i);
    EXPECT_TRUE(result->equalValueAt(data.get(), i, i));
  }
  ASSERT_EQ(rowReader->next(10, result), 0);
}

TEST_F(TextWriterTest, nestedRowTypes) {
  // Test specifically for nested ROW types
  auto nestedRowChildren = std::vector<VectorPtr>{
      makeFlatVector<int32_t>({42, 100, -5, 0, 999}),
      makeFlatVector<bool>({true, false, true, false, true}),
      makeArrayVector<double>(
          {{3.14159, 2.71828},
           {2.71828, 1.41421, 0.0},
           {1.41421, -123.456},
           {0.0, 999.999},
           {-123.456, 42.0, 3.14159}})};
  auto nestedRowVector = makeRowVector(
      {"nested_int", "nested_bool", "nested_arr_double"}, nestedRowChildren);

  const auto data = makeRowVector(
      {makeFlatVector<std::string>(
           {"hello", "world", "test", "sample", "data"}),
       nestedRowVector,
       makeFlatVector<bool>({false, true, false, true, false})});

  const auto schema = ROW(
      {{"col_varchar", VARCHAR()},
       {"col_nested_row",
        ROW(
            {{"nested_int", INTEGER()},
             {"nested_bool", BOOLEAN()},
             {"nested_arr_double", ARRAY(DOUBLE())}})},
       {"col_bool", BOOLEAN()}});

  WriterOptions writerOptions;
  writerOptions.memoryPool = rootPool_.get();
  const auto tempPath = tempPath_->getPath();
  const auto filename = "test_nested_row_writer.txt";

  auto filePath = fs::path(fmt::format("{}/{}", tempPath, filename));
  auto sink = std::make_unique<dwio::common::LocalFileSink>(
      filePath, dwio::common::FileSink::Options{.pool = leafPool_.get()});

  const auto serDeOptions = dwio::common::SerDeOptions(',', '|', '#');
  auto writer = std::make_unique<TextWriter>(
      schema,
      std::move(sink),
      std::make_shared<text::WriterOptions>(writerOptions),
      serDeOptions);
  writer->write(data);
  writer->close();

  const auto fs = filesystems::getFileSystem(tempPath, nullptr);
  const auto& file = fs->openFileForRead(filePath.string());
  auto fileSize = file->size();

  BufferPtr charBuf = AlignedBuffer::allocate<char>(fileSize, pool());
  auto rawCharBuf = charBuf->asMutable<char>();
  std::vector<std::vector<std::string>> result =
      parseTextFile(tempPath, filename, rawCharBuf, serDeOptions);

  EXPECT_EQ(result.size(), 5);
  EXPECT_EQ(result[0].size(), 3);

  EXPECT_EQ(result[0][0], "hello");
  EXPECT_EQ(result[1][0], "world");
  EXPECT_EQ(result[2][0], "test");
  EXPECT_EQ(result[3][0], "sample");
  EXPECT_EQ(result[4][0], "data");

  // nested row
  EXPECT_EQ(result[0][1], "42|true|3.141590#2.718280");
  EXPECT_EQ(result[1][1], "100|false|2.718280#1.414210#0.000000");
  EXPECT_EQ(result[2][1], "-5|true|1.414210#-123.456000");
  EXPECT_EQ(result[3][1], "0|false|0.000000#999.999000");
  EXPECT_EQ(result[4][1], "999|true|-123.456000#42.000000#3.141590");

  EXPECT_EQ(result[0][2], "false");
  EXPECT_EQ(result[1][2], "true");
  EXPECT_EQ(result[2][2], "false");
  EXPECT_EQ(result[3][2], "true");
  EXPECT_EQ(result[4][2], "false");
}

TEST_F(TextWriterTest, verifyNestedRowTypesWithTextReader) {
  // Test specifically for nested ROW types
  auto nestedRowChildren = std::vector<VectorPtr>{
      makeFlatVector<int32_t>({42, 100, -5, 0, 999}),
      makeFlatVector<bool>({true, false, true, false, true}),
      makeArrayVector<double>(
          {{3.14159, 2.71828},
           {2.71828, 1.41421, 0.0},
           {1.41421, -123.456},
           {0.0, 999.999},
           {-123.456, 42.0, 3.14159}})};
  auto nestedRowVector = makeRowVector(
      {"nested_int", "nested_bool", "nested_arr_double"}, nestedRowChildren);

  const auto data = makeRowVector(
      {makeFlatVector<std::string>(
           {"hello", "world", "test", "sample", "data"}),
       nestedRowVector,
       makeFlatVector<bool>({false, true, false, true, false})});

  const auto schema = ROW(
      {{"col_varchar", VARCHAR()},
       {"col_nested_row",
        ROW(
            {{"nested_int", INTEGER()},
             {"nested_bool", BOOLEAN()},
             {"nested_arr_double", ARRAY(DOUBLE())}})},
       {"col_bool", BOOLEAN()}});

  WriterOptions writerOptions;
  writerOptions.memoryPool = rootPool_.get();
  const auto tempPath = tempPath_->getPath();
  const auto filename = "test_nested_row_writer.txt";

  auto filePath = fs::path(fmt::format("{}/{}", tempPath, filename));
  auto sink = std::make_unique<dwio::common::LocalFileSink>(
      filePath, dwio::common::FileSink::Options{.pool = leafPool_.get()});

  // Use custom delimiters: field separator '\x01', nested row field separator
  // '\x02'
  const auto serDeOptions = dwio::common::SerDeOptions('\x01', '\x02', '#');
  auto writer = std::make_unique<TextWriter>(
      schema,
      std::move(sink),
      std::make_shared<text::WriterOptions>(writerOptions),
      serDeOptions);
  writer->write(data);
  writer->close();

  // Set up reader
  auto readerFactory =
      dwio::common::getReaderFactory(dwio::common::FileFormat::TEXT);
  auto readFile = std::make_shared<LocalReadFile>(filePath.string());
  auto readerOptions = dwio::common::ReaderOptions(pool());
  readerOptions.setFileSchema(schema);
  readerOptions.setSerDeOptions(serDeOptions);

  auto input =
      std::make_unique<dwio::common::BufferedInput>(readFile, poolRef());
  auto reader = readerFactory->createReader(std::move(input), readerOptions);
  dwio::common::RowReaderOptions rowReaderOptions;
  setScanSpec(*schema, rowReaderOptions);

  auto rowReader = reader->createRowReader(rowReaderOptions);

  EXPECT_EQ(*reader->rowType(), *schema);

  VectorPtr result;

  ASSERT_EQ(rowReader->next(5, result), 5);
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(result->equalValueAt(data.get(), i, i));
  }
}

TEST_F(TextWriterTest, abort) {
  auto schema = ROW({"c0", "c1"}, {BIGINT(), BOOLEAN()});
  auto data = makeRowVector(
      {"c0", "c1"},
      {
          makeFlatVector<int64_t>({1, 2, 3}),
          makeConstant(true, 3),
      });

  WriterOptions writerOptions;
  writerOptions.memoryPool = rootPool_.get();
  writerOptions.defaultFlushCount = 10;

  const auto tempPath = tempPath_->getPath();
  const auto filename = "test_text_writer_abort.txt";
  auto filePath = fs::path(fmt::format("{}/{}", tempPath, filename));

  auto sink = std::make_unique<dwio::common::LocalFileSink>(
      filePath.string(),
      dwio::common::FileSink::Options{.pool = leafPool_.get()});
  auto writer = std::make_unique<TextWriter>(
      schema,
      std::move(sink),
      std::make_shared<text::WriterOptions>(writerOptions));
  writer->write(data);
  writer->abort();

  uint64_t result = readFile(tempPath, filePath.filename().string());

  // With defaultFlushCount as 10, it will trigger two times of flushes before
  // abort, and abort will discard the remaining 5 characters in buffer. The
  // written file would have:
  // 1^Atrue\n
  // 2^Atrue\n
  // 3^A
  EXPECT_EQ(result, 16);
}
} // namespace facebook::velox::text
