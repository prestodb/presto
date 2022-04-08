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

#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/TempFilePath.h"

#include "gtest/gtest.h"

using namespace facebook::velox;

constexpr int kOneMB = 1 << 20;

void writeData(WriteFile* writeFile) {
  writeFile->append("aaaaa");
  writeFile->append("bbbbb");
  writeFile->append(std::string(kOneMB, 'c'));
  writeFile->append("ddddd");
  ASSERT_EQ(writeFile->size(), 15 + kOneMB);
}

void readData(ReadFile* readFile) {
  ASSERT_EQ(readFile->size(), 15 + kOneMB);
  char buffer1[5];
  ASSERT_EQ(readFile->pread(10 + kOneMB, 5, &buffer1), "ddddd");
  char buffer2[10];
  ASSERT_EQ(readFile->pread(0, 10, &buffer2), "aaaaabbbbb");
  char buffer3[kOneMB];
  ASSERT_EQ(readFile->pread(10, kOneMB, &buffer3), std::string(kOneMB, 'c'));
  ASSERT_EQ(readFile->size(), 15 + kOneMB);
  char buffer4[10];
  const std::string_view arf = readFile->pread(5, 10, &buffer4);
  const std::string zarf = readFile->pread(kOneMB, 15);
  auto buf = std::make_unique<char[]>(8);
  const std::string_view warf = readFile->pread(4, 8, buf.get());
  const std::string_view warfFromBuf(buf.get(), 8);
  ASSERT_EQ(arf, "bbbbbccccc");
  ASSERT_EQ(zarf, "ccccccccccddddd");
  ASSERT_EQ(warf, "abbbbbcc");
  ASSERT_EQ(warfFromBuf, "abbbbbcc");
  char head[12];
  char middle[4];
  char tail[7];
  std::vector<folly::Range<char*>> buffers = {
      folly::Range<char*>(head, sizeof(head)),
      folly::Range<char*>(nullptr, 500000),
      folly::Range<char*>(middle, sizeof(middle)),
      folly::Range<char*>(
          nullptr,
          15 + kOneMB - 500000 - sizeof(head) - sizeof(middle) - sizeof(tail)),
      folly::Range<char*>(tail, sizeof(tail))};
  ASSERT_EQ(15 + kOneMB, readFile->preadv(0, buffers));
  ASSERT_EQ(std::string_view(head, sizeof(head)), "aaaaabbbbbcc");
  ASSERT_EQ(std::string_view(middle, sizeof(middle)), "cccc");
  ASSERT_EQ(std::string_view(tail, sizeof(tail)), "ccddddd");
}

// We could template this test, but that's kinda overkill for how simple it is.

TEST(InMemoryFile, writeAndRead) {
  std::string buf;
  {
    InMemoryWriteFile writeFile(&buf);
    writeData(&writeFile);
  }
  InMemoryReadFile readFile(buf);
  readData(&readFile);
}

TEST(LocalFile, writeAndRead) {
  auto tempFile = ::exec::test::TempFilePath::create();
  const auto& filename = tempFile->path.c_str();
  remove(filename);
  {
    LocalWriteFile writeFile(filename);
    writeData(&writeFile);
  }
  LocalReadFile readFile(filename);
  readData(&readFile);
}

TEST(LocalFile, viaRegistry) {
  filesystems::registerLocalFileSystem();
  auto tempFile = ::exec::test::TempFilePath::create();
  const auto& filename = tempFile->path.c_str();
  remove(filename);
  auto lfs = filesystems::getFileSystem(filename, nullptr);
  {
    auto writeFile = lfs->openFileForWrite(filename);
    writeFile->append("snarf");
  }
  auto readFile = lfs->openFileForRead(filename);
  ASSERT_EQ(readFile->size(), 5);
  char buffer1[5];
  ASSERT_EQ(readFile->pread(0, 5, &buffer1), "snarf");
  lfs->remove(filename);
}
