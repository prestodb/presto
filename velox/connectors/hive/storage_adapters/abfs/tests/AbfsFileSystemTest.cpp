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
#include <atomic>
#include <filesystem>
#include <random>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsPath.h"

#include "connectors/hive/storage_adapters/abfs/AzureClientProviderFactories.h"
#include "connectors/hive/storage_adapters/abfs/AzureClientProviderImpl.h"
#include "connectors/hive/storage_adapters/abfs/RegisterAbfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsReadFile.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsWriteFile.h"
#include "velox/connectors/hive/storage_adapters/abfs/RegisterAbfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/abfs/tests/AzuriteServer.h"
#include "velox/connectors/hive/storage_adapters/abfs/tests/MockDataLakeFileClient.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/exec/tests/utils/PortUtil.h"
#include "velox/exec/tests/utils/TempFilePath.h"

using namespace facebook::velox;
using namespace facebook::velox::filesystems;
using ::facebook::velox::common::Region;

namespace {

constexpr int kOneMB = 1 << 20;

class TestAzureClientProvider final : public AzureClientProvider {
 public:
  explicit TestAzureClientProvider() {
    delegated_ = std::make_unique<SharedKeyAzureClientProvider>();
  }

  std::unique_ptr<AzureBlobClient> getReadFileClient(
      const std::shared_ptr<AbfsPath>& path,
      const config::ConfigBase& config) override {
    return delegated_->getReadFileClient(path, config);
  }

  std::unique_ptr<AzureDataLakeFileClient> getWriteFileClient(
      const std::shared_ptr<AbfsPath>& path,
      const config::ConfigBase& config) override {
    return std::make_unique<MockDataLakeFileClient>();
  }

 private:
  std::unique_ptr<AzureClientProvider> delegated_;
};

} // namespace

class AbfsFileSystemTest : public testing::Test {
 public:
  std::shared_ptr<AzuriteServer> azuriteServer_;
  std::unique_ptr<AbfsFileSystem> abfs_;

  static void SetUpTestCase() {
    registerAbfsFileSystem();
    registerAzureClientProviderFactory("test", [](const std::string&) {
      return std::make_unique<TestAzureClientProvider>();
    });
  }

  void SetUp() override {
    auto port = facebook::velox::exec::test::getFreePort();
    azuriteServer_ = std::make_shared<AzuriteServer>(port);
    azuriteServer_->start();
    auto tempFile = createFile();
    azuriteServer_->addFile(tempFile->getPath());
    abfs_ = std::make_unique<AbfsFileSystem>(azuriteServer_->hiveConfig());
  }

  void TearDown() override {
    azuriteServer_->stop();
  }

  static std::string generateRandomData(int size) {
    static const char charset[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    std::string data(size, ' ');

    for (int i = 0; i < size; ++i) {
      int index = rand() % (sizeof(charset) - 1);
      data[i] = charset[index];
    }

    return data;
  }

 private:
  static std::shared_ptr<::exec::test::TempFilePath> createFile() {
    auto tempFile = exec::test::TempFilePath::create();
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
  char buffer3[kOneMB];
  ASSERT_EQ(readFile->pread(10, kOneMB, buffer3), std::string(kOneMB, 'c'));
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
  ASSERT_EQ(
      10 + 5,
      readFile->preadv(
          {regions.data(), regions.size()}, {iobufs.data(), iobufs.size()}));
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
  auto readFile = abfs_->openFileForRead(azuriteServer_->fileURI());
  readData(readFile.get());
}

TEST_F(AbfsFileSystemTest, openFileForReadWithOptions) {
  FileOptions options;
  options.fileSize = 15 + kOneMB;
  auto readFile = abfs_->openFileForRead(azuriteServer_->fileURI(), options);
  readData(readFile.get());
}

TEST_F(AbfsFileSystemTest, openFileForReadWithInvalidOptions) {
  FileOptions options;
  options.fileSize = -kOneMB;
  VELOX_ASSERT_THROW(
      abfs_->openFileForRead(azuriteServer_->fileURI(), options),
      "File size must be non-negative");
}

TEST_F(AbfsFileSystemTest, fileHandleWithProperties) {
  FileHandleFactory factory(
      std::make_unique<SimpleLRUCache<FileHandleKey, FileHandle>>(1),
      std::make_unique<FileHandleGenerator>(azuriteServer_->hiveConfig()));
  FileProperties properties = {15 + kOneMB, 1};
  FileHandleKey key{azuriteServer_->fileURI()};
  auto fileHandleProperties = factory.generate(key, &properties);
  readData(fileHandleProperties->file.get());

  auto fileHandleWithoutProperties = factory.generate(key);
  readData(fileHandleWithoutProperties->file.get());
}

TEST_F(AbfsFileSystemTest, multipleThreadsWithReadFile) {
  std::atomic<bool> startThreads = false;
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
      auto readFile = abfs_->openFileForRead(azuriteServer_->fileURI());
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
  const std::string abfsFile = azuriteServer_->URI() + "test.txt";
  VELOX_ASSERT_RUNTIME_THROW_CODE(
      abfs_->openFileForRead(abfsFile), error_code::kFileNotFound, "404");
}

TEST(AbfsWriteFileTest, openFileForWriteTest) {
  std::string_view kAbfsFile =
      "abfs://test@test.dfs.core.windows.net/test/writetest.txt";
  std::unique_ptr<AzureDataLakeFileClient> mockClient =
      std::make_unique<MockDataLakeFileClient>();
  auto mockClientPath =
      reinterpret_cast<MockDataLakeFileClient*>(mockClient.get())->path();
  AbfsWriteFile abfsWriteFile(kAbfsFile, mockClient);
  EXPECT_EQ(abfsWriteFile.size(), 0);
  std::string dataContent = "";
  uint64_t totalSize = 0;
  std::string randomData =
      AbfsFileSystemTest::generateRandomData(1 * 1024 * 1024);
  for (int i = 0; i < 8; ++i) {
    abfsWriteFile.append(randomData);
    dataContent += randomData;
  }
  totalSize = randomData.size() * 8;
  abfsWriteFile.flush();
  EXPECT_EQ(abfsWriteFile.size(), totalSize);

  randomData = AbfsFileSystemTest::generateRandomData(9 * 1024 * 1024);
  dataContent += randomData;
  abfsWriteFile.append(randomData);
  totalSize += randomData.size();
  randomData = AbfsFileSystemTest::generateRandomData(2 * 1024 * 1024);
  dataContent += randomData;
  totalSize += randomData.size();
  abfsWriteFile.append(randomData);
  abfsWriteFile.flush();
  EXPECT_EQ(abfsWriteFile.size(), totalSize);
  abfsWriteFile.flush();
  abfsWriteFile.close();
  VELOX_ASSERT_THROW(abfsWriteFile.append("abc"), "File is not open");

  std::unique_ptr<AzureDataLakeFileClient> mockClientCopy =
      std::make_unique<MockDataLakeFileClient>(mockClientPath);
  VELOX_ASSERT_THROW(
      AbfsWriteFile(kAbfsFile, mockClientCopy), "File already exists");
  MockDataLakeFileClient readClient(mockClientPath);
  auto fileContent = readClient.readContent();
  ASSERT_EQ(fileContent.size(), dataContent.size());
  ASSERT_EQ(fileContent, dataContent);
}

TEST_F(AbfsFileSystemTest, renameNotImplemented) {
  VELOX_ASSERT_THROW(
      abfs_->rename("text", "text2"), "rename for abfs not implemented");
}

TEST_F(AbfsFileSystemTest, notImplemented) {
  VELOX_ASSERT_THROW(abfs_->remove("text"), "remove for abfs not implemented");
  VELOX_ASSERT_THROW(abfs_->exists("text"), "exists for abfs not implemented");
  VELOX_ASSERT_THROW(abfs_->list("dir"), "list for abfs not implemented");
  VELOX_ASSERT_THROW(abfs_->mkdir("dir"), "mkdir for abfs not implemented");
  VELOX_ASSERT_THROW(abfs_->rmdir("dir"), "rmdir for abfs not implemented");
}

TEST_F(AbfsFileSystemTest, clientProviderFactoryNotRegistered) {
  const std::string abfsFile =
      std::string("abfs://test@test1.dfs.core.windows.net/test");
  VELOX_ASSERT_THROW(
      abfs_->openFileForRead(abfsFile),
      "No AzureClientProviderFactory registered for account 'test1'.");
}

TEST_F(AbfsFileSystemTest, registerAbfsFileSink) {
  static const std::vector<std::string> paths = {
      "abfs://test@test.dfs.core.windows.net/test",
      "abfss://test@test.dfs.core.windows.net/test"};
  std::unordered_map<std::string, std::string> config(
      {{"fs.azure.account.key.test.dfs.core.windows.net", "NDU2"}});
  auto hiveConfig =
      std::make_shared<const config::ConfigBase>(std::move(config));
  for (const auto& path : paths) {
    auto sink = dwio::common::FileSink::create(
        path, {.connectorProperties = hiveConfig});
    auto writeFileSink = dynamic_cast<dwio::common::WriteFileSink*>(sink.get());
    auto writeFile = writeFileSink->toWriteFile();
    auto abfsWriteFile = dynamic_cast<AbfsWriteFile*>(writeFile.get());
    ASSERT_TRUE(abfsWriteFile != nullptr);
  }
}
