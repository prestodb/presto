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
#include "velox/common/file/tests/FaultyFileSystem.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/exec/tests/utils/TempFilePath.h"

#include "gtest/gtest.h"

using namespace facebook::velox;
using facebook::velox::common::Region;
using namespace facebook::velox::tests::utils;

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

void writeDataWithOffset(WriteFile* writeFile) {
  ASSERT_EQ(writeFile->size(), 0);
  writeFile->truncate(15 + kOneMB);
  std::vector<iovec> iovecs;
  std::string s1 = "aaaaa";
  std::string s2 = "bbbbb";
  std::string s3 = std::string(kOneMB, 'c');
  std::string s4 = "ddddd";
  iovecs.push_back({s3.data(), s3.length()});
  iovecs.push_back({s4.data(), s4.length()});
  writeFile->write(iovecs, 10, 5 + kOneMB);
  iovecs.clear();
  iovecs.push_back({s1.data(), s1.length()});
  iovecs.push_back({s2.data(), s2.length()});
  writeFile->write(iovecs, 0, 10);
  ASSERT_EQ(writeFile->size(), 15 + kOneMB);
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
          (char*)(uint64_t)(15 + kOneMB - 500000 - sizeof(head) -
                            sizeof(middle) - sizeof(tail))),
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

class LocalFileTest : public ::testing::TestWithParam<bool> {
 protected:
  LocalFileTest() : useFaultyFs_(GetParam()) {}

  static void SetUpTestCase() {
    filesystems::registerLocalFileSystem();
    tests::utils::registerFaultyFileSystem();
  }

  const bool useFaultyFs_;
};

TEST_P(LocalFileTest, writeAndRead) {
  struct {
    bool useIOBuf;
    bool withOffset;

    std::string debugString() const {
      return fmt::format("useIOBuf {}, withOffset {}", useIOBuf, withOffset);
    }
  } testSettings[] = {{false, false}, {true, false}, {false, true}};
  for (auto testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto tempFile = exec::test::TempFilePath::create(useFaultyFs_);
    const auto& filename = tempFile->getPath();
    auto fs = filesystems::getFileSystem(filename, {});
    fs->remove(filename);
    {
      auto writeFile = fs->openFileForWrite(filename);
      if (testData.withOffset) {
        writeDataWithOffset(writeFile.get());
      } else {
        writeData(writeFile.get(), testData.useIOBuf);
      }
      writeFile->close();
      ASSERT_EQ(writeFile->size(), 15 + kOneMB);
    }
    auto readFile = fs->openFileForRead(filename);
    readData(readFile.get());
  }
}

TEST_P(LocalFileTest, viaRegistry) {
  auto tempFile = exec::test::TempFilePath::create(useFaultyFs_);
  const auto& filename = tempFile->getPath();
  auto fs = filesystems::getFileSystem(filename, {});
  fs->remove(filename);
  {
    auto writeFile = fs->openFileForWrite(filename);
    writeFile->append("snarf");
  }
  auto readFile = fs->openFileForRead(filename);
  ASSERT_EQ(readFile->size(), 5);
  char buffer1[5];
  ASSERT_EQ(readFile->pread(0, 5, &buffer1), "snarf");
  fs->remove(filename);
}

TEST_P(LocalFileTest, rename) {
  const auto tempFolder = ::exec::test::TempDirectoryPath::create(useFaultyFs_);
  const auto a = fmt::format("{}/a", tempFolder->getPath());
  const auto b = fmt::format("{}/b", tempFolder->getPath());
  const auto newA = fmt::format("{}/newA", tempFolder->getPath());
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
  VELOX_ASSERT_USER_THROW(localFs->rename(a, b), "");
  localFs->rename(a, newA);
  ASSERT_FALSE(localFs->exists(a));
  ASSERT_TRUE(localFs->exists(b));
  ASSERT_TRUE(localFs->exists(newA));
  localFs->rename(b, newA, true);
  auto readFile = localFs->openFileForRead(newA);
  char buffer[5];
  ASSERT_EQ(readFile->pread(0, 5, &buffer), data);
}

TEST_P(LocalFileTest, exists) {
  auto tempFolder = ::exec::test::TempDirectoryPath::create(useFaultyFs_);
  auto a = fmt::format("{}/a", tempFolder->getPath());
  auto b = fmt::format("{}/b", tempFolder->getPath());
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

TEST_P(LocalFileTest, list) {
  const auto tempFolder = ::exec::test::TempDirectoryPath::create(useFaultyFs_);
  const auto a = fmt::format("{}/1", tempFolder->getPath());
  const auto b = fmt::format("{}/2", tempFolder->getPath());
  auto localFs = filesystems::getFileSystem(a, nullptr);
  {
    auto writeFile = localFs->openFileForWrite(a);
    writeFile = localFs->openFileForWrite(b);
  }
  auto files = localFs->list(std::string_view(tempFolder->getPath()));
  std::sort(files.begin(), files.end());
  ASSERT_EQ(files, std::vector<std::string>({a, b}));
  localFs->remove(a);
  ASSERT_EQ(
      localFs->list(std::string_view(tempFolder->getPath())),
      std::vector<std::string>({b}));
  localFs->remove(b);
  ASSERT_TRUE(localFs->list(std::string_view(tempFolder->getPath())).empty());
}

TEST_P(LocalFileTest, readFileDestructor) {
  if (useFaultyFs_) {
    return;
  }
  auto tempFile = exec::test::TempFilePath::create(useFaultyFs_);
  const auto& filename = tempFile->getPath();
  auto fs = filesystems::getFileSystem(filename, {});
  fs->remove(filename);
  {
    auto writeFile = fs->openFileForWrite(filename);
    writeData(writeFile.get());
  }

  {
    auto readFile = fs->openFileForRead(filename);
    readData(readFile.get());
  }

  int32_t readFd;
  {
    std::unique_ptr<char[]> buf(new char[tempFile->getPath().size() + 1]);
    buf[tempFile->getPath().size()] = 0;
    ::memcpy(
        buf.get(), tempFile->getPath().c_str(), tempFile->getPath().size());
    readFd = ::open(buf.get(), O_RDONLY);
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

TEST_P(LocalFileTest, mkdir) {
  auto tempFolder = exec::test::TempDirectoryPath::create(useFaultyFs_);

  std::string path = tempFolder->getPath();
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

TEST_P(LocalFileTest, rmdir) {
  auto tempFolder = exec::test::TempDirectoryPath::create(useFaultyFs_);

  std::string path = tempFolder->getPath();
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
  EXPECT_NO_THROW(localFs->rmdir(tempFolder->getPath()));
  EXPECT_FALSE(localFs->exists(tempFolder->getPath()));

  // Delete a non-existing directory.
  path += "/does_not_exist/subdir";
  EXPECT_FALSE(localFs->exists(path));
  // The function does not throw, but will return zero files and folders
  // deleted, which is not an error.
  EXPECT_NO_THROW(localFs->rmdir(tempFolder->getPath()));
}

TEST_P(LocalFileTest, fileNotFound) {
  auto tempFolder = exec::test::TempDirectoryPath::create(useFaultyFs_);
  auto path = fmt::format("{}/file", tempFolder->getPath());
  auto localFs = filesystems::getFileSystem(path, nullptr);
  VELOX_ASSERT_RUNTIME_THROW_CODE(
      localFs->openFileForRead(path),
      error_code::kFileNotFound,
      "No such file or directory");
}

TEST_P(LocalFileTest, attributes) {
  auto tempFile = exec::test::TempFilePath::create(useFaultyFs_);
  const auto& filename = tempFile->getPath();
  auto fs = filesystems::getFileSystem(filename, {});
  fs->remove(filename);
  auto writeFile = fs->openFileForWrite(filename);
  ASSERT_FALSE(
      LocalWriteFile::Attributes::cowDisabled(writeFile->getAttributes()));
  try {
    writeFile->setAttributes(
        {{std::string(LocalWriteFile::Attributes::kNoCow), "true"}});
  } catch (const std::exception& /*e*/) {
    // Flags like FS_IOC_SETFLAGS might not be supported for certain
    // file systems (e.g., EXT4, XFS).
  }
  ASSERT_TRUE(
      LocalWriteFile::Attributes::cowDisabled(writeFile->getAttributes()));
  writeFile->close();
}

INSTANTIATE_TEST_SUITE_P(
    LocalFileTestSuite,
    LocalFileTest,
    ::testing::Values(false, true));

class FaultyFsTest : public ::testing::Test {
 protected:
  FaultyFsTest() {}

  static void SetUpTestCase() {
    filesystems::registerLocalFileSystem();
    tests::utils::registerFaultyFileSystem();
  }

  void SetUp() {
    dir_ = exec::test::TempDirectoryPath::create(true);
    fs_ = std::dynamic_pointer_cast<tests::utils::FaultyFileSystem>(
        filesystems::getFileSystem(dir_->getPath(), {}));
    VELOX_CHECK_NOT_NULL(fs_);
    readFilePath_ = fmt::format("{}/faultyTestReadFile", dir_->getPath());
    writeFilePath_ = fmt::format("{}/faultyTestWriteFile", dir_->getPath());
    const int bufSize = 1024;
    buffer_.resize(bufSize);
    for (int i = 0; i < bufSize; ++i) {
      buffer_[i] = i % 256;
    }
    {
      auto writeFile = fs_->openFileForWrite(readFilePath_, {});
      writeData(writeFile.get());
    }
    auto readFile = fs_->openFileForRead(readFilePath_, {});
    readData(readFile.get(), true);
    try {
      VELOX_FAIL("InjectedFaultFileError");
    } catch (VeloxRuntimeError&) {
      fileError_ = std::current_exception();
    }
  }

  void TearDown() {
    fs_->clearFileFaultInjections();
  }

  void writeData(WriteFile* file) {
    file->append(std::string_view(buffer_));
    file->flush();
  }

  void readData(ReadFile* file, bool useReadv = false) {
    char readBuf[buffer_.size()];
    if (!useReadv) {
      file->pread(0, buffer_.size(), readBuf);
    } else {
      std::vector<folly::Range<char*>> buffers;
      buffers.push_back(folly::Range<char*>(readBuf, buffer_.size()));
      file->preadv(0, buffers);
    }
    for (int i = 0; i < buffer_.size(); ++i) {
      if (buffer_[i] != readBuf[i]) {
        VELOX_FAIL("Data Mismatch");
      }
    }
  }

  std::shared_ptr<exec::test::TempDirectoryPath> dir_;
  std::string readFilePath_;
  std::string writeFilePath_;
  std::shared_ptr<tests::utils::FaultyFileSystem> fs_;
  std::string buffer_;
  std::exception_ptr fileError_;
};

TEST_F(FaultyFsTest, schemCheck) {
  ASSERT_TRUE(
      filesystems::isPathSupportedByRegisteredFileSystems("faulty:/test"));
  ASSERT_FALSE(
      filesystems::isPathSupportedByRegisteredFileSystems("other:/test"));
}

TEST_F(FaultyFsTest, fileReadErrorInjection) {
  // Set read error.
  fs_->setFileInjectionError(fileError_, {FaultFileOperation::Type::kRead});
  {
    auto readFile = fs_->openFileForRead(readFilePath_, {});
    VELOX_ASSERT_THROW(
        readData(readFile.get(), false), "InjectedFaultFileError");
  }
  {
    auto readFile = fs_->openFileForRead(readFilePath_, {});
    // We only inject error for pread API so preadv should be fine.
    readData(readFile.get(), true);
  }

  // Set readv error
  fs_->setFileInjectionError(fileError_, {FaultFileOperation::Type::kReadv});
  {
    auto readFile = fs_->openFileForRead(readFilePath_, {});
    VELOX_ASSERT_THROW(
        readData(readFile.get(), true), "InjectedFaultFileError");
  }
  {
    auto readFile = fs_->openFileForRead(readFilePath_, {});
    // We only inject error for preadv API so pread should be fine.
    readData(readFile.get(), false);
  }

  // Set error for all kinds of operations.
  fs_->setFileInjectionError(fileError_);
  auto readFile = fs_->openFileForRead(readFilePath_, {});
  VELOX_ASSERT_THROW(readData(readFile.get(), true), "InjectedFaultFileError");
  VELOX_ASSERT_THROW(readData(readFile.get(), false), "InjectedFaultFileError");
  fs_->remove(readFilePath_);
}

TEST_F(FaultyFsTest, fileReadDelayInjection) {
  // Set 2 seconds delay.
  const uint64_t injectDelay{2'000'000};
  fs_->setFileInjectionDelay(injectDelay, {FaultFileOperation::Type::kRead});
  {
    auto readFile = fs_->openFileForRead(readFilePath_, {});
    uint64_t readDurationUs{0};
    {
      MicrosecondTimer readTimer(&readDurationUs);
      readData(readFile.get(), false);
    }
    ASSERT_GE(readDurationUs, injectDelay);
  }
  {
    auto readFile = fs_->openFileForRead(readFilePath_, {});
    // We only inject error for pread API so preadv should be fine.
    uint64_t readDurationUs{0};
    {
      MicrosecondTimer readTimer(&readDurationUs);
      readData(readFile.get(), true);
    }
    ASSERT_LT(readDurationUs, injectDelay);
  }

  // Set readv error
  fs_->setFileInjectionDelay(injectDelay, {FaultFileOperation::Type::kReadv});
  {
    auto readFile = fs_->openFileForRead(readFilePath_, {});
    uint64_t readDurationUs{0};
    {
      MicrosecondTimer readTimer(&readDurationUs);
      readData(readFile.get(), true);
    }
    ASSERT_GE(readDurationUs, injectDelay);
  }
  {
    auto readFile = fs_->openFileForRead(readFilePath_, {});
    // We only inject error for pread API so preadv should be fine.
    uint64_t readDurationUs{0};
    {
      MicrosecondTimer readTimer(&readDurationUs);
      readData(readFile.get(), false);
    }
    ASSERT_LT(readDurationUs, injectDelay);
  }

  // Set error for all kinds of operations.
  fs_->setFileInjectionDelay(injectDelay);
  {
    auto readFile = fs_->openFileForRead(readFilePath_, {});
    // We only inject error for pread API so preadv should be fine.
    uint64_t readDurationUs{0};
    {
      MicrosecondTimer readTimer(&readDurationUs);
      readData(readFile.get(), false);
    }
    ASSERT_GE(readDurationUs, injectDelay);
  }
  {
    auto readFile = fs_->openFileForRead(readFilePath_, {});
    // We only inject error for pread API so preadv should be fine.
    uint64_t readDurationUs{0};
    {
      MicrosecondTimer readTimer(&readDurationUs);
      readData(readFile.get(), false);
    }
    ASSERT_GE(readDurationUs, injectDelay);
  }
}

TEST_F(FaultyFsTest, fileReadFaultHookInjection) {
  const std::string path1 = fmt::format("{}/hookFile1", dir_->getPath());
  {
    auto writeFile = fs_->openFileForWrite(path1, {});
    writeData(writeFile.get());
    auto readFile = fs_->openFileForRead(path1, {});
    readData(readFile.get());
  }
  const std::string path2 = fmt::format("{}/hookFile2", dir_->getPath());
  {
    auto writeFile = fs_->openFileForWrite(path2, {});
    writeData(writeFile.get());
    auto readFile = fs_->openFileForRead(path2, {});
    readData(readFile.get());
  }
  // Set read error.
  fs_->setFileInjectionHook([&](FaultFileOperation* op) {
    // Only inject error for readv.
    if (op->type != FaultFileOperation::Type::kReadv) {
      return;
    }
    // Only inject error for path2.
    if (op->path != path2) {
      return;
    }
    VELOX_FAIL("inject hook read failure");
  });
  {
    auto readFile = fs_->openFileForRead(path1, {});
    readData(readFile.get(), false);
    readData(readFile.get(), true);
  }
  {
    auto readFile = fs_->openFileForRead(path2, {});
    // Verify only throw for readv.
    readData(readFile.get(), false);
    VELOX_ASSERT_THROW(
        readData(readFile.get(), true), "inject hook read failure");
  }

  // Set to return fake data.
  fs_->setFileInjectionHook([&](FaultFileOperation* op) {
    // Only inject error for path1.
    if (op->path != path1) {
      return;
    }
    // Only inject error for read.
    if (op->type != FaultFileOperation::Type::kRead) {
      return;
    }
    auto* readOp = static_cast<FaultFileReadOperation*>(op);
    char* readBuf = static_cast<char*>(readOp->buf);
    for (int i = 0; i < readOp->length; ++i) {
      readBuf[i] = 0;
    }
    readOp->delegate = false;
  });

  {
    auto readFile = fs_->openFileForRead(path2, {});
    readData(readFile.get(), false);
    readData(readFile.get(), true);
  }
  {
    auto readFile = fs_->openFileForRead(path1, {});
    // Verify only throw for read.
    readData(readFile.get(), true);
    VELOX_ASSERT_THROW(readData(readFile.get(), false), "Data Mismatch");
  }
}

TEST_F(FaultyFsTest, fileWriteErrorInjection) {
  // Set write error.
  fs_->setFileInjectionError(fileError_, {FaultFileOperation::Type::kWrite});
  {
    auto writeFile = fs_->openFileForWrite(writeFilePath_, {});
    VELOX_ASSERT_THROW(writeFile->append("hello"), "InjectedFaultFileError");
    fs_->remove(writeFilePath_);
  }
  // Set error for all kinds of operations.
  fs_->setFileInjectionError(fileError_);
  {
    auto writeFile = fs_->openFileForWrite(writeFilePath_, {});
    VELOX_ASSERT_THROW(writeFile->append("hello"), "InjectedFaultFileError");
    fs_->remove(writeFilePath_);
  }
}

TEST_F(FaultyFsTest, fileWriteDelayInjection) {
  // Set 2 seconds delay.
  const uint64_t injectDelay{2'000'000};
  fs_->setFileInjectionDelay(injectDelay, {FaultFileOperation::Type::kWrite});
  {
    auto writeFile = fs_->openFileForWrite(writeFilePath_, {});
    uint64_t readDurationUs{0};
    {
      MicrosecondTimer readTimer(&readDurationUs);
      writeFile->append("hello");
    }
    ASSERT_GE(readDurationUs, injectDelay);
    fs_->remove(writeFilePath_);
  }
}

TEST_F(FaultyFsTest, fileWriteFaultHookInjection) {
  const std::string path1 = fmt::format("{}/hookFile1", dir_->getPath());
  const std::string path2 = fmt::format("{}/hookFile2", dir_->getPath());
  // Set to write fake data.
  fs_->setFileInjectionHook([&](FaultFileOperation* op) {
    // Only inject for write.
    if (op->type != FaultFileOperation::Type::kWrite) {
      return;
    }
    // Only inject for path2.
    if (op->path != path2) {
      return;
    }
    auto* writeOp = static_cast<FaultFileWriteOperation*>(op);
    *writeOp->data = "Error data";
  });
  {
    auto writeFile = fs_->openFileForWrite(path1, {});
    writeFile->append("hello");
    writeFile->close();
    auto readFile = fs_->openFileForRead(path1, {});
    char buffer[5];
    ASSERT_EQ(readFile->size(), 5);
    ASSERT_EQ(readFile->pread(0, 5, &buffer), "hello");
    fs_->remove(path1);
  }
  {
    auto writeFile = fs_->openFileForWrite(path2, {});
    writeFile->append("hello");
    writeFile->close();
    auto readFile = fs_->openFileForRead(path2, {});
    char buffer[10];
    ASSERT_EQ(readFile->size(), 10);
    ASSERT_EQ(readFile->pread(0, 10, &buffer), "Error data");
    fs_->remove(path2);
  }

  // Set to not delegate.
  fs_->setFileInjectionHook([&](FaultFileOperation* op) {
    // Only inject for write.
    if (op->type != FaultFileOperation::Type::kWrite) {
      return;
    }
    // Only inject for path2.
    if (op->path != path2) {
      return;
    }
    auto* writeOp = static_cast<FaultFileWriteOperation*>(op);
    writeOp->delegate = false;
  });
  {
    auto writeFile = fs_->openFileForWrite(path1, {});
    writeFile->append("hello");
    writeFile->close();
    auto readFile = fs_->openFileForRead(path1, {});
    char buffer[5];
    ASSERT_EQ(readFile->size(), 5);
    ASSERT_EQ(readFile->pread(0, 5, &buffer), "hello");
    fs_->remove(path1);
  }
  {
    auto writeFile = fs_->openFileForWrite(path2, {});
    writeFile->append("hello");
    writeFile->close();
    auto readFile = fs_->openFileForRead(path2, {});
    ASSERT_EQ(readFile->size(), 0);
    fs_->remove(path2);
  }
}
