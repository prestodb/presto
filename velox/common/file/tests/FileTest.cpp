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

#include <fcntl.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/exec/tests/utils/TempFilePath.h"

#include "gtest/gtest.h"

using namespace facebook::velox;
using facebook::velox::common::Region;

constexpr int kOneMB = 1 << 20;

void writeData(WriteFile* writeFile, bool useIOBuf = false) {
  if (useIOBuf) {
    std::unique_ptr<folly::IOBuf> buf = folly::IOBuf::copyBuffer("aaaaa");
    buf->appendToChain(folly::IOBuf::copyBuffer("bbbbb"));
    buf->appendToChain(folly::IOBuf::copyBuffer(std::string(kOneMB, 'c')));
    buf->appendToChain(folly::IOBuf::copyBuffer("ddddd"));
    writeFile->append(std::move(buf));
    ASSERT_EQ(writeFile->size(), 15 + kOneMB);
  } else {
    writeFile->append("aaaaa");
    writeFile->append("bbbbb");
    writeFile->append(std::string(kOneMB, 'c'));
    writeFile->append("ddddd");
    ASSERT_EQ(writeFile->size(), 15 + kOneMB);
  }
}

void readData(ReadFile* readFile, bool checkFileSize = true) {
  if (checkFileSize) {
    ASSERT_EQ(readFile->size(), 15 + kOneMB);
  }
  char buffer1[5];
  ASSERT_EQ(readFile->pread(10 + kOneMB, 5, &buffer1), "ddddd");
  char buffer2[10];
  ASSERT_EQ(readFile->pread(0, 10, &buffer2), "aaaaabbbbb");
  char buffer3[kOneMB];
  ASSERT_EQ(readFile->pread(10, kOneMB, &buffer3), std::string(kOneMB, 'c'));
  if (checkFileSize) {
    ASSERT_EQ(readFile->size(), 15 + kOneMB);
  }
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
      folly::Range<char*>(nullptr, (char*)(uint64_t)500000),
      folly::Range<char*>(middle, sizeof(middle)),
      folly::Range<char*>(
          nullptr,
          (char*)(uint64_t)(15 + kOneMB - 500000 - sizeof(head) - sizeof(middle) - sizeof(tail))),
      folly::Range<char*>(tail, sizeof(tail))};
  ASSERT_EQ(15 + kOneMB, readFile->preadv(0, buffers));
  ASSERT_EQ(std::string_view(head, sizeof(head)), "aaaaabbbbbcc");
  ASSERT_EQ(std::string_view(middle, sizeof(middle)), "cccc");
  ASSERT_EQ(std::string_view(tail, sizeof(tail)), "ccddddd");
}

// We could templated this test, but that's kinda overkill for how simple it is.
TEST(InMemoryFile, writeAndRead) {
  for (bool useIOBuf : {true, false}) {
    std::string buf;
    {
      InMemoryWriteFile writeFile(&buf);
      writeData(&writeFile, useIOBuf);
    }
    InMemoryReadFile readFile(buf);
    readData(&readFile);
  }
}

TEST(InMemoryFile, preadv) {
  std::string buf;
  {
    InMemoryWriteFile writeFile(&buf);
    writeData(&writeFile);
  }
  // aaaaa bbbbb c*1MB ddddd
  InMemoryReadFile readFile(buf);
  std::vector<std::string> expected = {"ab", "a", "cccdd", "ddd"};
  std::vector<Region> readRegions = std::vector<Region>{
      {4, 2UL, {}},
      {0, 1UL, {}},
      {5 + 5 + kOneMB - 3, 5UL, {}},
      {5 + 5 + kOneMB + 2, 3UL, {}}};

  std::vector<folly::IOBuf> iobufs(readRegions.size());
  readFile.preadv(readRegions, {iobufs.data(), iobufs.size()});
  std::vector<std::string> values;
  values.reserve(iobufs.size());
  for (auto& iobuf : iobufs) {
    values.push_back(std::string{
        reinterpret_cast<const char*>(iobuf.data()), iobuf.length()});
  }

  EXPECT_EQ(expected, values);
}

TEST(LocalFile, writeAndRead) {
  for (bool useIOBuf : {true, false}) {
    auto tempFile = ::exec::test::TempFilePath::create();
    const auto& filename = tempFile->path.c_str();
    remove(filename);
    {
      LocalWriteFile writeFile(filename);
      writeData(&writeFile, useIOBuf);
    }
    LocalReadFile readFile(filename);
    readData(&readFile);
  }
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

TEST(LocalFile, rename) {
  filesystems::registerLocalFileSystem();
  auto tempFolder = ::exec::test::TempDirectoryPath::create();
  auto a = fmt::format("{}/a", tempFolder->path);
  auto b = fmt::format("{}/b", tempFolder->path);
  auto newA = fmt::format("{}/newA", tempFolder->path);
  const std::string data("aaaaa");
  auto localFs = filesystems::getFileSystem(a, nullptr);
  {
    auto writeFile = localFs->openFileForWrite(a);
    writeFile = localFs->openFileForWrite(b);
    writeFile->append(data);
    writeFile->close();
  }
  ASSERT_TRUE(localFs->exists(a));
  ASSERT_TRUE(localFs->exists(b));
  ASSERT_FALSE(localFs->exists(newA));
  EXPECT_THROW(localFs->rename(a, b), VeloxUserError);
  localFs->rename(a, newA);
  ASSERT_FALSE(localFs->exists(a));
  ASSERT_TRUE(localFs->exists(b));
  ASSERT_TRUE(localFs->exists(newA));
  localFs->rename(b, newA, true);
  auto readFile = localFs->openFileForRead(newA);
  char buffer[5];
  ASSERT_EQ(readFile->pread(0, 5, &buffer), data);
}

TEST(LocalFile, exists) {
  filesystems::registerLocalFileSystem();
  auto tempFolder = ::exec::test::TempDirectoryPath::create();
  auto a = fmt::format("{}/a", tempFolder->path);
  auto b = fmt::format("{}/b", tempFolder->path);
  auto localFs = filesystems::getFileSystem(a, nullptr);
  {
    auto writeFile = localFs->openFileForWrite(a);
    writeFile = localFs->openFileForWrite(b);
  }
  ASSERT_TRUE(localFs->exists(a));
  ASSERT_TRUE(localFs->exists(b));
  localFs->remove(a);
  ASSERT_FALSE(localFs->exists(a));
  ASSERT_TRUE(localFs->exists(b));
  localFs->remove(b);
  ASSERT_FALSE(localFs->exists(a));
  ASSERT_FALSE(localFs->exists(b));
}

TEST(LocalFile, list) {
  filesystems::registerLocalFileSystem();
  auto tempFolder = ::exec::test::TempDirectoryPath::create();
  auto a = fmt::format("{}/1", tempFolder->path);
  auto b = fmt::format("{}/2", tempFolder->path);
  auto localFs = filesystems::getFileSystem(a, nullptr);
  {
    auto writeFile = localFs->openFileForWrite(a);
    writeFile = localFs->openFileForWrite(b);
  }
  auto files = localFs->list(std::string_view(tempFolder->path));
  std::sort(files.begin(), files.end());
  ASSERT_EQ(files, std::vector<std::string>({a, b}));
  localFs->remove(a);
  ASSERT_EQ(
      localFs->list(std::string_view(tempFolder->path)),
      std::vector<std::string>({b}));
  localFs->remove(b);
  ASSERT_TRUE(localFs->list(std::string_view(tempFolder->path)).empty());
}

TEST(LocalFile, readFileDestructor) {
  auto tempFile = ::exec::test::TempFilePath::create();
  const auto& filename = tempFile->path.c_str();
  remove(filename);
  {
    LocalWriteFile writeFile(filename);
    writeData(&writeFile);
  }

  {
    LocalReadFile readFile(filename);
    readData(&readFile);
  }

  int32_t readFd;
  {
    std::unique_ptr<char[]> buf(new char[tempFile->path.size() + 1]);
    buf[tempFile->path.size()] = 0;
    memcpy(buf.get(), tempFile->path.data(), tempFile->path.size());
    readFd = open(buf.get(), O_RDONLY);
  }
  {
    LocalReadFile readFile(readFd);
    readData(&readFile, false);
  }
  {
    // Can't read again from a closed file descriptor.
    LocalReadFile readFile(readFd);
    ASSERT_ANY_THROW(readData(&readFile, false));
  }
}

TEST(LocalFile, mkdir) {
  filesystems::registerLocalFileSystem();
  auto tempFolder = ::exec::test::TempDirectoryPath::create();

  std::string path = tempFolder->path;
  auto localFs = filesystems::getFileSystem(path, nullptr);

  // Create 3 levels of directories and ensure they exist.
  path += "/level1/level2/level3";
  EXPECT_NO_THROW(localFs->mkdir(path));
  EXPECT_TRUE(localFs->exists(path));

  // Create a completely existing directory - we should not throw.
  EXPECT_NO_THROW(localFs->mkdir(path));

  // Write a file to our directory to double check it exist.
  path += "/a.txt";
  const std::string data("aaaaa");
  {
    auto writeFile = localFs->openFileForWrite(path);
    writeFile->append(data);
    writeFile->close();
  }
  EXPECT_TRUE(localFs->exists(path));
}

TEST(LocalFile, rmdir) {
  filesystems::registerLocalFileSystem();
  auto tempFolder = ::exec::test::TempDirectoryPath::create();

  std::string path = tempFolder->path;
  auto localFs = filesystems::getFileSystem(path, nullptr);

  // Create 3 levels of directories and ensure they exist.
  path += "/level1/level2/level3";
  EXPECT_NO_THROW(localFs->mkdir(path));
  EXPECT_TRUE(localFs->exists(path));

  // Write a file to our directory to double check it exist.
  path += "/a.txt";
  const std::string data("aaaaa");
  {
    auto writeFile = localFs->openFileForWrite(path);
    writeFile->append(data);
    writeFile->close();
  }
  EXPECT_TRUE(localFs->exists(path));

  // Now delete the whole temp folder and ensure it is gone.
  EXPECT_NO_THROW(localFs->rmdir(tempFolder->path));
  EXPECT_FALSE(localFs->exists(tempFolder->path));

  // Delete a non-existing directory.
  path += "/does_not_exist/subdir";
  EXPECT_FALSE(localFs->exists(path));
  // The function does not throw, but will return zero files and folders
  // deleted, which is not an error.
  EXPECT_NO_THROW(localFs->rmdir(tempFolder->path));
}

TEST(LocalFile, fileNotFound) {
  filesystems::registerLocalFileSystem();
  auto tempFolder = ::exec::test::TempDirectoryPath::create();
  auto path = fmt::format("{}/file", tempFolder->path);
  auto localFs = filesystems::getFileSystem(path, nullptr);
  VELOX_ASSERT_RUNTIME_THROW_CODE(
      localFs->openFileForRead(path),
      error_code::kFileNotFound,
      "No such file or directory");
}
