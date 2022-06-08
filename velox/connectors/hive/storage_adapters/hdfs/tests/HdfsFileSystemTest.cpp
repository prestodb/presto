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
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h"
#include <boost/format.hpp>
#include <connectors/hive/storage_adapters/hdfs/HdfsReadFile.h>
#include <gmock/gmock-matchers.h>
#include <hdfs/hdfs.h>
#include <atomic>
#include <random>
#include "HdfsMiniCluster.h"
#include "gtest/gtest.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/TempFilePath.h"

using namespace facebook::velox;

constexpr int kOneMB = 1 << 20;
static const std::string destinationPath = "/test_file.txt";
static const std::string hdfsPort = "7878";
static const std::string localhost = "localhost";
static const std::string fullDestinationPath =
    "hdfs://" + localhost + ":" + hdfsPort + destinationPath;
static const std::unordered_map<std::string, std::string> configurationValues(
    {{"hive.hdfs.host", localhost}, {"hive.hdfs.port", hdfsPort}});

class HdfsFileSystemTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    if (miniCluster == nullptr) {
      miniCluster = std::make_shared<
          facebook::velox::filesystems::test::HdfsMiniCluster>();
      miniCluster->start();
      auto tempFile = createFile();
      miniCluster->addFile(tempFile->path, destinationPath);
    }
  }

  void SetUp() override {
    if (!miniCluster->isRunning()) {
      miniCluster->start();
    }
  }

  static void TearDownTestSuite() {
    miniCluster->stop();
  }
  static std::atomic<bool> startThreads;
  static std::shared_ptr<facebook::velox::filesystems::test::HdfsMiniCluster>
      miniCluster;

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

std::shared_ptr<facebook::velox::filesystems::test::HdfsMiniCluster>
    HdfsFileSystemTest::miniCluster = nullptr;
std::atomic<bool> HdfsFileSystemTest::startThreads = false;

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
}

void checkReadErrorMessages(
    ReadFile* readFile,
    std::string errorMessage,
    int endpoint) {
  try {
    readFile->pread(10 + kOneMB, endpoint);
    FAIL() << "expected VeloxException";
  } catch (facebook::velox::VeloxException const& error) {
    EXPECT_THAT(error.message(), testing::HasSubstr(errorMessage));
  }
  try {
    auto buf = std::make_unique<char[]>(8);
    readFile->pread(10 + kOneMB, endpoint, buf.get());
    FAIL() << "expected VeloxException";
  } catch (facebook::velox::VeloxException const& error) {
    EXPECT_THAT(error.message(), testing::HasSubstr(errorMessage));
  }
}

void verifyFailures(ReadFile* readFile) {
  auto startPoint = 10 + kOneMB;
  auto size = 15 + kOneMB;
  auto endpoint = 10 + 2 * kOneMB;
  auto offsetErrorMessage =
      (boost::format(
           "(%d vs. %d) Cannot read HDFS file beyond its size: %d, offset: %d, end point: %d") %
       size % endpoint % size % startPoint % endpoint)
          .str();
  auto serverAddress = (boost::format("%s:%s") % localhost % hdfsPort).str();
  auto readFailErrorMessage =
      (boost::format(
           "Unable to open file %s. got error: HdfsIOException: InputStreamImpl: cannot open file: %s.\t"
           "Caused by: Hdfs::HdfsRpcException: HdfsFailoverException: Failed to invoke RPC call \"getBlockLocations\" on server \"%s\"\t\t"
           "Caused by: HdfsNetworkConnectException: Connect to \"%s\" failed") %
       destinationPath % destinationPath % serverAddress % serverAddress)
          .str();
  auto builderErrorMessage =
      (boost::format(
           "Unable to connect to HDFS, got error: Hdfs::HdfsRpcException: HdfsFailoverException: "
           "Failed to invoke RPC call \"getFsStats\" on server \"%s\"\tCaused by: "
           "HdfsNetworkConnectException: Connect to \"%s\" failed") %
       serverAddress % serverAddress)
          .str();
  checkReadErrorMessages(readFile, offsetErrorMessage, kOneMB);
  HdfsFileSystemTest::miniCluster->stop();
  checkReadErrorMessages(readFile, readFailErrorMessage, 1);
  try {
    auto memConfig =
        std::make_shared<const core::MemConfig>(configurationValues);
    facebook::velox::filesystems::HdfsFileSystem hdfsFileSystem(memConfig);
    FAIL() << "expected VeloxException";
  } catch (facebook::velox::VeloxException const& error) {
    EXPECT_THAT(error.message(), testing::HasSubstr(builderErrorMessage));
  }
}

TEST_F(HdfsFileSystemTest, read) {
  struct hdfsBuilder* builder = hdfsNewBuilder();
  hdfsBuilderSetNameNode(builder, localhost.c_str());
  hdfsBuilderSetNameNodePort(builder, 7878);
  auto hdfs = hdfsBuilderConnect(builder);
  HdfsReadFile readFile(hdfs, destinationPath);
  readData(&readFile);
}

TEST_F(HdfsFileSystemTest, viaFileSystem) {
  facebook::velox::filesystems::registerHdfsFileSystem();
  auto memConfig = std::make_shared<const core::MemConfig>(configurationValues);
  auto hdfsFileSystem =
      filesystems::getFileSystem(fullDestinationPath, memConfig);
  auto readFile = hdfsFileSystem->openFileForRead(fullDestinationPath);
  readData(readFile.get());
}

TEST_F(HdfsFileSystemTest, missingFileViaFileSystem) {
  try {
    facebook::velox::filesystems::registerHdfsFileSystem();
    auto memConfig =
        std::make_shared<const core::MemConfig>(configurationValues);
    auto hdfsFileSystem =
        filesystems::getFileSystem(fullDestinationPath, memConfig);
    auto readFile = hdfsFileSystem->openFileForRead(
        "hdfs://localhost:7777/path/that/does/not/exist");
    FAIL() << "expected VeloxException";
  } catch (facebook::velox::VeloxException const& error) {
    EXPECT_THAT(
        error.message(),
        testing::HasSubstr(
            "Unable to get file path info for file: /path/that/does/not/exist. got error: FileNotFoundException: Path /path/that/does/not/exist does not exist."));
  }
}

TEST_F(HdfsFileSystemTest, missingHost) {
  try {
    facebook::velox::filesystems::registerHdfsFileSystem();
    std::unordered_map<std::string, std::string> missingHostConfiguration(
        {{"hive.hdfs.port", hdfsPort}});
    auto memConfig =
        std::make_shared<const core::MemConfig>(missingHostConfiguration);
    facebook::velox::filesystems::HdfsFileSystem hdfsFileSystem(memConfig);
    FAIL() << "expected VeloxException";
  } catch (facebook::velox::VeloxException const& error) {
    EXPECT_THAT(
        error.message(),
        testing::HasSubstr(
            "hdfsHost is empty, configuration missing for hdfs host"));
  }
}

TEST_F(HdfsFileSystemTest, missingPort) {
  try {
    facebook::velox::filesystems::registerHdfsFileSystem();
    std::unordered_map<std::string, std::string> missingPortConfiguration(
        {{"hive.hdfs.host", localhost}});
    auto memConfig =
        std::make_shared<const core::MemConfig>(missingPortConfiguration);
    facebook::velox::filesystems::HdfsFileSystem hdfsFileSystem(memConfig);
    FAIL() << "expected VeloxException";
  } catch (facebook::velox::VeloxException const& error) {
    EXPECT_THAT(
        error.message(),
        testing::HasSubstr(
            "hdfsPort is empty, configuration missing for hdfs port"));
  }
}

TEST_F(HdfsFileSystemTest, missingFileViaReadFile) {
  try {
    struct hdfsBuilder* builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, localhost.c_str());
    hdfsBuilderSetNameNodePort(builder, std::stoi(hdfsPort));
    auto hdfs = hdfsBuilderConnect(builder);
    HdfsReadFile readFile(hdfs, "/path/that/does/not/exist");
    FAIL() << "expected VeloxException";
  } catch (facebook::velox::VeloxException const& error) {
    EXPECT_THAT(
        error.message(),
        testing::HasSubstr(
            "Unable to get file path info for file: /path/that/does/not/exist. got error: FileNotFoundException: Path /path/that/does/not/exist does not exist."));
  }
}

TEST_F(HdfsFileSystemTest, schemeMatching) {
  try {
    auto fs =
        std::dynamic_pointer_cast<facebook::velox::filesystems::HdfsFileSystem>(
            filesystems::getFileSystem("/", nullptr));
    FAIL() << "expected VeloxException";
  } catch (facebook::velox::VeloxException const& error) {
    EXPECT_THAT(
        error.message(),
        testing::HasSubstr(
            "No registered file system matched with filename '/'"));
  }
  auto fs =
      std::dynamic_pointer_cast<facebook::velox::filesystems::HdfsFileSystem>(
          filesystems::getFileSystem(fullDestinationPath, nullptr));
  ASSERT_TRUE(fs->isHdfsFile(fullDestinationPath));
}

TEST_F(HdfsFileSystemTest, writeNotSupported) {
  try {
    facebook::velox::filesystems::registerHdfsFileSystem();
    auto memConfig =
        std::make_shared<const core::MemConfig>(configurationValues);
    auto hdfsFileSystem =
        filesystems::getFileSystem(fullDestinationPath, memConfig);
    hdfsFileSystem->openFileForWrite("/path");
  } catch (facebook::velox::VeloxException const& error) {
    EXPECT_EQ(error.message(), "Write to HDFS is unsupported");
  }
}

TEST_F(HdfsFileSystemTest, removeNotSupported) {
  try {
    facebook::velox::filesystems::registerHdfsFileSystem();
    auto memConfig =
        std::make_shared<const core::MemConfig>(configurationValues);
    auto hdfsFileSystem =
        filesystems::getFileSystem(fullDestinationPath, memConfig);
    hdfsFileSystem->remove("/path");
  } catch (facebook::velox::VeloxException const& error) {
    EXPECT_EQ(error.message(), "Does not support removing files from hdfs");
  }
}

TEST_F(HdfsFileSystemTest, multipleThreadsWithReadFile) {
  startThreads = false;
  struct hdfsBuilder* builder = hdfsNewBuilder();
  hdfsBuilderSetNameNode(builder, localhost.c_str());
  hdfsBuilderSetNameNodePort(builder, 7878);
  auto hdfs = hdfsBuilderConnect(builder);
  HdfsReadFile readFile(hdfs, destinationPath);
  std::vector<std::thread> threads;
  std::mt19937 generator(std::random_device{}());
  std::vector<int> sleepTimesInMicroseconds = {0, 500, 50000};
  std::uniform_int_distribution<std::size_t> distribution(
      0, sleepTimesInMicroseconds.size() - 1);
  for (int i = 0; i < 25; i++) {
    auto thread = std::thread(
        [&readFile, &distribution, &generator, &sleepTimesInMicroseconds] {
          int index = distribution(generator);
          while (!HdfsFileSystemTest::startThreads) {
            std::this_thread::yield();
          }
          std::this_thread::sleep_for(
              std::chrono::microseconds(sleepTimesInMicroseconds[index]));
          readData(&readFile);
        });
    threads.emplace_back(std::move(thread));
  }
  startThreads = true;
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(HdfsFileSystemTest, multipleThreadsWithFileSystem) {
  startThreads = false;
  facebook::velox::filesystems::registerHdfsFileSystem();
  auto memConfig = std::make_shared<const core::MemConfig>(configurationValues);
  auto hdfsFileSystem =
      filesystems::getFileSystem(fullDestinationPath, memConfig);

  std::vector<std::thread> threads;
  std::mt19937 generator(std::random_device{}());
  std::vector<int> sleepTimesInMicroseconds = {0, 500, 50000};
  std::uniform_int_distribution<std::size_t> distribution(
      0, sleepTimesInMicroseconds.size() - 1);
  for (int i = 0; i < 25; i++) {
    auto thread = std::thread([&hdfsFileSystem,
                               &distribution,
                               &generator,
                               &sleepTimesInMicroseconds] {
      int index = distribution(generator);
      while (!HdfsFileSystemTest::startThreads) {
        std::this_thread::yield();
      }
      std::this_thread::sleep_for(
          std::chrono::microseconds(sleepTimesInMicroseconds[index]));
      auto readFile = hdfsFileSystem->openFileForRead(fullDestinationPath);
      readData(readFile.get());
    });
    threads.emplace_back(std::move(thread));
  }
  startThreads = true;
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(HdfsFileSystemTest, readFailures) {
  struct hdfsBuilder* builder = hdfsNewBuilder();
  hdfsBuilderSetNameNode(builder, localhost.c_str());
  hdfsBuilderSetNameNodePort(builder, stoi(hdfsPort));
  auto hdfs = hdfsBuilderConnect(builder);
  HdfsReadFile readFile(hdfs, destinationPath);
  verifyFailures(&readFile);
}
