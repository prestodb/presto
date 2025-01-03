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
#include <gtest/gtest.h>
#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/text/tests/writer/FileReaderUtil.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::text {
// TODO: add fuzzer test once text reader is move in OSS
class TextWriterTest : public testing::Test,
                       public velox::test::VectorTestBase {
 public:
  void SetUp() override {
    velox::filesystems::registerLocalFileSystem();
    dwio::common::LocalFileSink::registerFactory();
    rootPool_ = memory::memoryManager()->addRootPool("TextWriterTests");
    leafPool_ = rootPool_->addLeafChild("TextWriterTests");
    tempPath_ = exec::test::TempDirectoryPath::create();
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
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
  auto filePath =
      fs::path(fmt::format("{}/test_text_writer.txt", tempPath_->getPath()));
  auto sink = std::make_unique<dwio::common::LocalFileSink>(
      filePath, dwio::common::FileSink::Options{.pool = leafPool_.get()});
  auto writer = std::make_unique<TextWriter>(
      schema,
      std::move(sink),
      std::make_shared<text::WriterOptions>(writerOptions));
  writer->write(data);
  writer->close();

  std::vector<std::vector<std::string>> result = parseTextFile(filePath);
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
  EXPECT_EQ(result[0][7], "1970-01-01 00:00:00.000");
  EXPECT_EQ(result[1][7], "1970-01-01 00:00:01.001");
  EXPECT_EQ(result[2][7], "1970-01-01 00:00:02.002");

  // varchar
  EXPECT_EQ(result[0][8], "hello");
  EXPECT_EQ(result[1][8], "world");
  EXPECT_EQ(result[2][8], "cpp");

  // varbinary
  EXPECT_EQ(result[0][9], "aGVsbG8=");
  EXPECT_EQ(result[1][9], "d29ybGQ=");
  EXPECT_EQ(result[2][9], "Y3Bw");
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
  auto filePath = fs::path(
      fmt::format("{}/test_text_writer_abort.txt", tempPath_->getPath()));
  auto sink = std::make_unique<dwio::common::LocalFileSink>(
      filePath, dwio::common::FileSink::Options{.pool = leafPool_.get()});
  auto writer = std::make_unique<TextWriter>(
      schema,
      std::move(sink),
      std::make_shared<text::WriterOptions>(writerOptions));
  writer->write(data);
  writer->abort();

  std::string result = readFile(filePath);
  // With defaultFlushCount as 10, it will trigger two times of flushes before
  // abort, and abort will discard the remaining 5 characters in buffer. The
  // written file would have:
  // 1^Atrue
  // 2^Atrue
  // 3^A
  EXPECT_EQ(result.size(), 14);
}
} // namespace facebook::velox::text
