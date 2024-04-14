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

#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/InputStream.h"
#include "velox/exec/tests/utils/TempFilePath.h"

#include <string_view>
#include "folly/io/Cursor.h"

#include "gtest/gtest.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;
using facebook::velox::common::Region;

class ReadFileInputStreamTest : public testing::Test {
 protected:
  void SetUp() {
    filesystems::registerLocalFileSystem();
  }
};

TEST_F(ReadFileInputStreamTest, LocalReadFile) {
  auto tempFile = exec::test::TempFilePath::create();
  const auto& filename = tempFile->getPath();
  remove(filename.c_str());
  {
    LocalWriteFile writeFile(filename);
    for (int i = 0; i < 1027; ++i)
      writeFile.append("abc");
    ASSERT_EQ(writeFile.size(), static_cast<uint64_t>(3081));
  }

  auto readFile = std::make_shared<LocalReadFile>(filename);
  ASSERT_EQ(static_cast<uint64_t>(3081), readFile->size());

  auto readStream =
      std::make_shared<facebook::velox::dwio::common::ReadFileInputStream>(
          readFile);
  std::vector<Region> regions;
  for (int i = 0; i < 3081; i += 3) {
    regions.push_back(
        Region(static_cast<uint64_t>(i), static_cast<uint64_t>(1)));
  }
  ASSERT_GT(regions.size(), IOV_MAX);

  std::vector<folly::Range<char*>> buffers;
  std::vector<folly::IOBuf> iobufs(regions.size());

  readStream->vread(regions, {iobufs.data(), iobufs.size()}, LogType::TEST);
  remove(filename.c_str());
}

TEST(ReadFileInputStream, SimpleUsage) {
  std::string fileData;
  {
    InMemoryWriteFile writeFile(&fileData);
    writeFile.append("aaaaa");
    writeFile.append("bbbbb");
    writeFile.append("ccccc");
  }
  auto readFile = std::make_shared<InMemoryReadFile>(fileData);
  ReadFileInputStream inputStream(readFile);
  ASSERT_EQ(inputStream.getLength(), 15);
  auto buf = std::make_unique<char[]>(15);

  inputStream.read(buf.get(), 7, 4, LogType::STREAM);
  std::string_view read_value(buf.get(), 7);
  ASSERT_EQ(read_value, "abbbbbc");

  inputStream.read(buf.get(), 15, 0, LogType::STREAM);
  read_value = {buf.get(), 15};
  ASSERT_EQ(read_value, "aaaaabbbbbccccc");
}

TEST(ReadFileInputStream, VReadIOBufs) {
  std::string fileData;
  {
    InMemoryWriteFile writeFile(&fileData);
    writeFile.append("aaaaa");
    writeFile.append("bbbbb");
    writeFile.append("ccccc");
  }
  auto readFile = std::make_shared<InMemoryReadFile>(fileData);
  ReadFileInputStream inputStream(readFile);
  ASSERT_EQ(inputStream.getLength(), 15);
  std::vector<Region> regions = {{0, 6}, {9, 5}};
  std::vector<folly::IOBuf> iobufs(regions.size());
  inputStream.vread(regions, {iobufs.data(), iobufs.size()}, LogType::STREAM);
  std::vector<std::string> result;
  std::transform(
      iobufs.cbegin(),
      iobufs.cend(),
      std::back_inserter(result),
      [](const auto& iobuf) {
        folly::io::Cursor cursor(&iobuf);
        return cursor.readFixedString(cursor.totalLength());
      });
  std::vector<std::string> expected = {"aaaaab", "bcccc"};
  EXPECT_EQ(result, expected);
}
