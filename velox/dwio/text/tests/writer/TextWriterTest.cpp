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
#include "velox/dwio/text/reader/TextReader.h"
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
