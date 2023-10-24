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

#include "velox/connectors/hive/storage_adapters/abfs/AbfsFileSystem.h"
#include "gtest/gtest.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsReadFile.h"
#include "velox/connectors/hive/storage_adapters/abfs/tests/AzuriteServer.h"
#include "velox/exec/tests/utils/PortUtil.h"
#include "velox/exec/tests/utils/TempFilePath.h"

#include <atomic>
#include <random>

using namespace facebook::velox;

using ::facebook::velox::common::Region;

constexpr int kOneMB = 1 << 20;
static const std::string filePath = "test_file.txt";
static const std::string fullFilePath =
    facebook::velox::filesystems::test::AzuriteABFSEndpoint + filePath;

class AbfsFileSystemTest : public testing::Test {
 public:
  static std::shared_ptr<const Config> hiveConfig(
      const std::unordered_map<std::string, std::string> configOverride = {}) {
    std::unordered_map<std::string, std::string> config({});

    // Update the default config map with the supplied configOverride map
    for (const auto& item : configOverride) {
      config[item.first] = item.second;
      std::cout << "config " + item.first + " value " + item.second
                << std::endl;
    }

    return std::make_shared<const core::MemConfig>(std::move(config));
  }

 public:
  std::shared_ptr<facebook::velox::filesystems::test::AzuriteServer>
      azuriteServer;

  void SetUp() override {
    auto port = facebook::velox::exec::test::getFreePort();
    azuriteServer =
        std::make_shared<facebook::velox::filesystems::test::AzuriteServer>(
            port);
    azuriteServer->start();
    auto tempFile = createFile();
    azuriteServer->addFile(tempFile->path, filePath);
  }

  void TearDown() override {
    azuriteServer->stop();
  }

 private:
  static std::shared_ptr<::exec::test::TempFilePath> createFile() {
    auto tempFile = ::exec::test::TempFilePath::create();
    tempFile->append("aaaaa");
    tempFile->append("bbbbb");
    tempFile->append(std::string(kOneMB, 'c'));
    tempFile->append("ddddd");
    return tempFile;
  }
};

void readData(ReadFile* readFile) {
  ASSERT_EQ(readFile->size(), 15 + kOneMB);
  char buffer1[5];
  ASSERT_EQ(readFile->pread(10 + kOneMB, 5, &buffer1), "ddddd");
  char buffer2[10];
  ASSERT_EQ(readFile->pread(0, 10, &buffer2), "aaaaabbbbb");
  auto buffer3 = new char[kOneMB];
  ASSERT_EQ(readFile->pread(10, kOneMB, buffer3), std::string(kOneMB, 'c'));
  delete[] buffer3;
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

  char buff1[10];
  char buff2[10];
  std::vector<folly::Range<char*>> buffers = {
      folly::Range<char*>(buff1, 10),
      folly::Range<char*>(nullptr, kOneMB - 5),
      folly::Range<char*>(buff2, 10)};
  ASSERT_EQ(10 + kOneMB - 5 + 10, readFile->preadv(0, buffers));
  ASSERT_EQ(std::string_view(buff1, sizeof(buff1)), "aaaaabbbbb");
  ASSERT_EQ(std::string_view(buff2, sizeof(buff2)), "cccccddddd");

  std::vector<folly::IOBuf> iobufs(2);
  std::vector<Region> regions = {{0, 10}, {10, 5}};
  readFile->preadv(
      {regions.data(), regions.size()}, {iobufs.data(), iobufs.size()});
  ASSERT_EQ(
      std::string_view(
          reinterpret_cast<const char*>(iobufs[0].writableData()),
          iobufs[0].length()),
      "aaaaabbbbb");
  ASSERT_EQ(
      std::string_view(
          reinterpret_cast<const char*>(iobufs[1].writableData()),
          iobufs[1].length()),
      "ccccc");
}

TEST_F(AbfsFileSystemTest, readFile) {
  auto hiveConfig = AbfsFileSystemTest::hiveConfig(
      {{"fs.azure.account.key.test.dfs.core.windows.net",
        azuriteServer->connectionStr()}});
  auto abfs = std::make_shared<filesystems::abfs::AbfsFileSystem>(hiveConfig);
  auto readFile = abfs->openFileForRead(fullFilePath);
  readData(readFile.get());
}

TEST_F(AbfsFileSystemTest, multipleThreadsWithReadFile) {
  std::atomic<bool> startThreads = false;
  auto hiveConfig = AbfsFileSystemTest::hiveConfig(
      {{"fs.azure.account.key.test.dfs.core.windows.net",
        azuriteServer->connectionStr()}});
  auto abfs = std::make_shared<filesystems::abfs::AbfsFileSystem>(hiveConfig);

  std::vector<std::thread> threads;
  std::mt19937 generator(std::random_device{}());
  std::vector<int> sleepTimesInMicroseconds = {0, 500, 5000};
  std::uniform_int_distribution<std::size_t> distribution(
      0, sleepTimesInMicroseconds.size() - 1);
  for (int i = 0; i < 10; i++) {
    auto thread = std::thread([&] {
      int index = distribution(generator);
      while (!startThreads) {
        std::this_thread::yield();
      }
      std::this_thread::sleep_for(
          std::chrono::microseconds(sleepTimesInMicroseconds[index]));
      auto readFile = abfs->openFileForRead(fullFilePath);
      readData(readFile.get());
    });
    threads.emplace_back(std::move(thread));
  }
  startThreads = true;
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(AbfsFileSystemTest, missingFile) {
  try {
    auto hiveConfig = AbfsFileSystemTest::hiveConfig(
        {{"fs.azure.account.key.test.dfs.core.windows.net",
          azuriteServer->connectionStr()}});
    const std::string abfsFile =
        facebook::velox::filesystems::test::AzuriteABFSEndpoint + "test.txt";
    auto abfs = std::make_shared<filesystems::abfs::AbfsFileSystem>(hiveConfig);
    auto readFile = abfs->openFileForRead(abfsFile);
    FAIL() << "Expected VeloxException";
  } catch (VeloxException const& err) {
    EXPECT_TRUE(err.message().find("404") != std::string::npos);
  }
}

TEST_F(AbfsFileSystemTest, openFileForWriteNotImplemented) {
  auto hiveConfig = AbfsFileSystemTest::hiveConfig(
      {{"fs.azure.account.key.test.dfs.core.windows.net",
        azuriteServer->connectionStr()}});
  auto abfs = std::make_shared<filesystems::abfs::AbfsFileSystem>(hiveConfig);
  VELOX_ASSERT_THROW(
      abfs->openFileForWrite(fullFilePath), "write for abfs not implemented");
}

TEST_F(AbfsFileSystemTest, renameNotImplemented) {
  auto hiveConfig = AbfsFileSystemTest::hiveConfig(
      {{"fs.azure.account.key.test.dfs.core.windows.net",
        azuriteServer->connectionStr()}});
  auto abfs = std::make_shared<filesystems::abfs::AbfsFileSystem>(hiveConfig);
  VELOX_ASSERT_THROW(
      abfs->rename("text", "text2"), "rename for abfs not implemented");
}

TEST_F(AbfsFileSystemTest, removeNotImplemented) {
  auto hiveConfig = AbfsFileSystemTest::hiveConfig(
      {{"fs.azure.account.key.test.dfs.core.windows.net",
        azuriteServer->connectionStr()}});
  auto abfs = std::make_shared<filesystems::abfs::AbfsFileSystem>(hiveConfig);
  VELOX_ASSERT_THROW(abfs->remove("text"), "remove for abfs not implemented");
}

TEST_F(AbfsFileSystemTest, existsNotImplemented) {
  auto hiveConfig = AbfsFileSystemTest::hiveConfig(
      {{"fs.azure.account.key.test.dfs.core.windows.net",
        azuriteServer->connectionStr()}});
  auto abfs = std::make_shared<filesystems::abfs::AbfsFileSystem>(hiveConfig);
  VELOX_ASSERT_THROW(abfs->exists("text"), "exists for abfs not implemented");
}

TEST_F(AbfsFileSystemTest, listNotImplemented) {
  auto hiveConfig = AbfsFileSystemTest::hiveConfig(
      {{"fs.azure.account.key.test.dfs.core.windows.net",
        azuriteServer->connectionStr()}});
  auto abfs = std::make_shared<filesystems::abfs::AbfsFileSystem>(hiveConfig);
  VELOX_ASSERT_THROW(abfs->list("dir"), "list for abfs not implemented");
}

TEST_F(AbfsFileSystemTest, mkdirNotImplemented) {
  auto hiveConfig = AbfsFileSystemTest::hiveConfig(
      {{"fs.azure.account.key.test.dfs.core.windows.net",
        azuriteServer->connectionStr()}});
  auto abfs = std::make_shared<filesystems::abfs::AbfsFileSystem>(hiveConfig);
  VELOX_ASSERT_THROW(abfs->mkdir("dir"), "mkdir for abfs not implemented");
}

TEST_F(AbfsFileSystemTest, rmdirNotImplemented) {
  auto hiveConfig = AbfsFileSystemTest::hiveConfig(
      {{"fs.azure.account.key.test.dfs.core.windows.net",
        azuriteServer->connectionStr()}});
  auto abfs = std::make_shared<filesystems::abfs::AbfsFileSystem>(hiveConfig);
  VELOX_ASSERT_THROW(abfs->rmdir("dir"), "rmdir for abfs not implemented");
}

TEST_F(AbfsFileSystemTest, credNotFOund) {
  const std::string abfsFile =
      std::string("abfs://test@test1.dfs.core.windows.net/test");
  auto hiveConfig = AbfsFileSystemTest::hiveConfig({});
  auto abfs =
      std::make_shared<facebook::velox::filesystems::abfs::AbfsFileSystem>(
          hiveConfig);
  VELOX_ASSERT_THROW(
      abfs->openFileForRead(abfsFile), "Failed to find storage credentials");
}
