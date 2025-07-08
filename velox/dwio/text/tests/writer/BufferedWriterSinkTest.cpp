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

#include <gtest/gtest.h>
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/text/tests/writer/FileReaderUtil.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::text {

class BufferedWriterSinkTest : public testing::Test,
                               public velox::test::VectorTestBase {
 public:
  void SetUp() override {
    velox::filesystems::registerLocalFileSystem();
    dwio::common::LocalFileSink::registerFactory();
    rootPool_ = memory::memoryManager()->addRootPool("BufferedWriterSinkTest");
    leafPool_ = rootPool_->addLeafChild("BufferedWriterSinkTest");
    tempPath_ = exec::test::TempDirectoryPath::create();
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::shared_ptr<exec::test::TempDirectoryPath> tempPath_;
};

TEST_F(BufferedWriterSinkTest, write) {
  const auto tempPath = tempPath_->getPath();
  const auto filename = "test_buffered_writer.txt";

  auto filePath = fs::path(fmt::format("{}/{}", tempPath, filename));
  auto sink = std::make_unique<dwio::common::LocalFileSink>(
      filePath, dwio::common::FileSink::Options{.pool = leafPool_.get()});

  auto bufferedWriterSink = std::make_unique<BufferedWriterSink>(
      std::move(sink), rootPool_->addLeafChild("bufferedWriterSinkTest"), 15);

  bufferedWriterSink->write("hello world", 10);
  bufferedWriterSink->write("this is writer", 10);
  bufferedWriterSink->close();

  uint64_t result = readFile(tempPath, filename);
  EXPECT_EQ(result, 20);
}

TEST_F(BufferedWriterSinkTest, abort) {
  const auto tempPath = tempPath_->getPath();
  const auto filename = "test_buffered_abort.txt";

  auto filePath = fs::path(fmt::format("{}/{}", tempPath, filename));
  auto sink = std::make_unique<dwio::common::LocalFileSink>(
      filePath, dwio::common::FileSink::Options{.pool = leafPool_.get()});

  auto bufferedWriterSink = std::make_unique<BufferedWriterSink>(
      std::move(sink), rootPool_->addLeafChild("bufferedWriterSinkTest"), 15);

  bufferedWriterSink->write("hello world", 10);
  bufferedWriterSink->write("this is writer", 10);
  bufferedWriterSink->abort();

  uint64_t result = readFile(tempPath_->getPath(), filename);
  EXPECT_EQ(result, 10);
}
} // namespace facebook::velox::text
