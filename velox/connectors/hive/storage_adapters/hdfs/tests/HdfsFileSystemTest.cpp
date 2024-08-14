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
#include <gmock/gmock-matchers.h>
#include <hdfs/hdfs.h>
#include <atomic>
#include <random>
#include "HdfsMiniCluster.h"
#include "gtest/gtest.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsReadFile.h"
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/tests/utils/TempFilePath.h"

using namespace facebook::velox;

constexpr int kOneMB = 1 << 20;
static const std::string destinationPath = "/test_file.txt";
static const std::string hdfsPort = "7878";
static const std::string localhost = "localhost";
static const std::string fullDestinationPath =
    "hdfs://" + localhost + ":" + hdfsPort + destinationPath;
static const std::string simpleDestinationPath = "hdfs:///" + destinationPath;
static const std::unordered_map<std::string, std::string> configurationValues(
    {{"hive.hdfs.host", localhost}, {"hive.hdfs.port", hdfsPort}});

class HdfsFileSystemTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    if (miniCluster == nullptr) {
      miniCluster = std::make_shared<filesystems::test::HdfsMiniCluster>();
      miniCluster->start();
      auto tempFile = createFile();
      miniCluster->addFile(tempFile->getPath(), destinationPath);
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
  static std::shared_ptr<filesystems::test::HdfsMiniCluster> miniCluster;

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

std::shared_ptr<filesystems::test::HdfsMiniCluster>
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

std::unique_ptr<WriteFile> openFileForWrite(std::string_view path) {
  auto config = std::make_shared<const config::ConfigBase>(
      std::unordered_map<std::string, std::string>(configurationValues));
  std::string hdfsFilePath =
      "hdfs://" + localhost + ":" + hdfsPort + std::string(path);
  auto hdfsFileSystem = filesystems::getFileSystem(hdfsFilePath, config);
  return hdfsFileSystem->openFileForWrite(path);
}

void checkReadErrorMessages(
    ReadFile* readFile,
    std::string errorMessage,
    int endpoint) {
  try {
    readFile->pread(10 + kOneMB, endpoint);
    FAIL() << "expected VeloxException";
  } catch (VeloxException const& error) {
    EXPECT_THAT(error.message(), testing::HasSubstr(errorMessage));
  }
  try {
    auto buf = std::make_unique<char[]>(8);
    readFile->pread(10 + kOneMB, endpoint, buf.get());
    FAIL() << "expected VeloxException";
  } catch (VeloxException const& error) {
    EXPECT_THAT(error.message(), testing::HasSubstr(errorMessage));
  }
}

void verifyFailures(hdfsFS hdfs) {
  HdfsReadFile readFile(hdfs, destinationPath);
  HdfsReadFile readFile2(hdfs, destinationPath);
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
           "Unable to connect to HDFS: %s, got error: Hdfs::HdfsRpcException: HdfsFailoverException: "
           "Failed to invoke RPC call \"getFsStats\" on server \"%s\"\tCaused by: "
           "HdfsNetworkConnectException: Connect to \"%s\" failed") %
       serverAddress % serverAddress % serverAddress)
          .str();
  checkReadErrorMessages(&readFile, offsetErrorMessage, kOneMB);
  HdfsFileSystemTest::miniCluster->stop();
  checkReadErrorMessages(&readFile2, readFailErrorMessage, 1);
  try {
    auto config = std::make_shared<const config::ConfigBase>(
        std::unordered_map<std::string, std::string>(configurationValues));
    filesystems::HdfsFileSystem hdfsFileSystem(
        config,
        filesystems::HdfsFileSystem::getServiceEndpoint(
            simpleDestinationPath, config.get()));
    FAIL() << "expected VeloxException";
  } catch (VeloxException const& error) {
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
  auto config = std::make_shared<const config::ConfigBase>(
      std::unordered_map<std::string, std::string>(configurationValues));
  auto hdfsFileSystem = filesystems::getFileSystem(fullDestinationPath, config);
  auto readFile = hdfsFileSystem->openFileForRead(fullDestinationPath);
  readData(readFile.get());
}

TEST_F(HdfsFileSystemTest, initializeFsWithEndpointInfoInFilePath) {
  // Without host/port configured.
  auto config = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>());
  auto hdfsFileSystem = filesystems::getFileSystem(fullDestinationPath, config);
  auto readFile = hdfsFileSystem->openFileForRead(fullDestinationPath);
  readData(readFile.get());

  // Wrong endpoint info specified in hdfs file path.
  const std::string wrongFullDestinationPath =
      "hdfs://not_exist_host:" + hdfsPort + destinationPath;
  VELOX_ASSERT_THROW(
      filesystems::getFileSystem(wrongFullDestinationPath, config),
      "Unable to connect to HDFS");
}

TEST_F(HdfsFileSystemTest, fallbackToUseConfig) {
  auto config = std::make_shared<const config::ConfigBase>(
      std::unordered_map<std::string, std::string>(configurationValues));
  auto hdfsFileSystem =
      filesystems::getFileSystem(simpleDestinationPath, config);
  auto readFile = hdfsFileSystem->openFileForRead(simpleDestinationPath);
  readData(readFile.get());
}

TEST_F(HdfsFileSystemTest, oneFsInstanceForOneEndpoint) {
  auto hdfsFileSystem1 =
      filesystems::getFileSystem(fullDestinationPath, nullptr);
  auto hdfsFileSystem2 =
      filesystems::getFileSystem(fullDestinationPath, nullptr);
  ASSERT_TRUE(hdfsFileSystem1 == hdfsFileSystem2);
}

TEST_F(HdfsFileSystemTest, missingFileViaFileSystem) {
  auto config = std::make_shared<const config::ConfigBase>(
      std::unordered_map<std::string, std::string>(configurationValues));
  auto hdfsFileSystem = filesystems::getFileSystem(fullDestinationPath, config);
  VELOX_ASSERT_RUNTIME_THROW_CODE(
      hdfsFileSystem->openFileForRead(
          "hdfs://localhost:7777/path/that/does/not/exist"),
      error_code::kFileNotFound,
      "Unable to get file path info for file: /path/that/does/not/exist. got error: FileNotFoundException: Path /path/that/does/not/exist does not exist.");
}

TEST_F(HdfsFileSystemTest, missingHost) {
  try {
    std::unordered_map<std::string, std::string> missingHostConfiguration(
        {{"hive.hdfs.port", hdfsPort}});
    auto config = std::make_shared<const config::ConfigBase>(
        std::move(missingHostConfiguration));
    filesystems::HdfsFileSystem hdfsFileSystem(
        config,
        filesystems::HdfsFileSystem::getServiceEndpoint(
            simpleDestinationPath, config.get()));
    FAIL() << "expected VeloxException";
  } catch (VeloxException const& error) {
    EXPECT_THAT(
        error.message(),
        testing::HasSubstr(
            "hdfsHost is empty, configuration missing for hdfs host"));
  }
}

TEST_F(HdfsFileSystemTest, missingPort) {
  try {
    std::unordered_map<std::string, std::string> missingPortConfiguration(
        {{"hive.hdfs.host", localhost}});
    auto config = std::make_shared<const config::ConfigBase>(
        std::move(missingPortConfiguration));
    filesystems::HdfsFileSystem hdfsFileSystem(
        config,
        filesystems::HdfsFileSystem::getServiceEndpoint(
            simpleDestinationPath, config.get()));
    FAIL() << "expected VeloxException";
  } catch (VeloxException const& error) {
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
  } catch (VeloxException const& error) {
    EXPECT_THAT(
        error.message(),
        testing::HasSubstr(
            "Unable to get file path info for file: /path/that/does/not/exist. got error: FileNotFoundException: Path /path/that/does/not/exist does not exist."));
  }
}

TEST_F(HdfsFileSystemTest, schemeMatching) {
  try {
    auto fs = std::dynamic_pointer_cast<filesystems::HdfsFileSystem>(
        filesystems::getFileSystem("/", nullptr));
    FAIL() << "expected VeloxException";
  } catch (VeloxException const& error) {
    EXPECT_THAT(
        error.message(),
        testing::HasSubstr(
            "No registered file system matched with file path '/'"));
  }
  auto fs = std::dynamic_pointer_cast<filesystems::HdfsFileSystem>(
      filesystems::getFileSystem(fullDestinationPath, nullptr));
  ASSERT_TRUE(fs->isHdfsFile(fullDestinationPath));
}

TEST_F(HdfsFileSystemTest, writeNotSupported) {
  try {
    auto config = std::make_shared<const config::ConfigBase>(
        std::unordered_map<std::string, std::string>(configurationValues));
    auto hdfsFileSystem =
        filesystems::getFileSystem(fullDestinationPath, config);
    hdfsFileSystem->openFileForWrite("/path");
  } catch (VeloxException const& error) {
    EXPECT_EQ(error.message(), "Write to HDFS is unsupported");
  }
}

TEST_F(HdfsFileSystemTest, removeNotSupported) {
  try {
    auto config = std::make_shared<const config::ConfigBase>(
        std::unordered_map<std::string, std::string>(configurationValues));
    auto hdfsFileSystem =
        filesystems::getFileSystem(fullDestinationPath, config);
    hdfsFileSystem->remove("/path");
  } catch (VeloxException const& error) {
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
  auto config = std::make_shared<const config::ConfigBase>(
      std::unordered_map<std::string, std::string>(configurationValues));
  auto hdfsFileSystem = filesystems::getFileSystem(fullDestinationPath, config);

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

TEST_F(HdfsFileSystemTest, write) {
  std::string path = "/a.txt";
  auto writeFile = openFileForWrite(path);
  std::string data = "abcdefghijk";
  writeFile->append(data);
  writeFile->flush();
  ASSERT_EQ(writeFile->size(), 0);
  writeFile->append(data);
  writeFile->append(data);
  writeFile->flush();
  writeFile->close();
  ASSERT_EQ(writeFile->size(), data.size() * 3);
}

TEST_F(HdfsFileSystemTest, missingFileForWrite) {
  const std::string filePath = "hdfs://localhost:7777/path/that/does/not/exist";
  const std::string errorMsg =
      "Failed to open hdfs file: hdfs://localhost:7777/path/that/does/not/exist";
  VELOX_ASSERT_THROW(openFileForWrite(filePath), errorMsg);
}

TEST_F(HdfsFileSystemTest, writeDataFailures) {
  auto writeFile = openFileForWrite("/a.txt");
  writeFile->close();
  VELOX_ASSERT_THROW(
      writeFile->append("abcde"),
      "Cannot append to HDFS file because file handle is null, file path: /a.txt");
}

TEST_F(HdfsFileSystemTest, writeFlushFailures) {
  auto writeFile = openFileForWrite("/a.txt");
  writeFile->close();
  VELOX_ASSERT_THROW(
      writeFile->flush(),
      "Cannot flush HDFS file because file handle is null, file path: /a.txt");
}

TEST_F(HdfsFileSystemTest, writeWithParentDirNotExist) {
  std::string path = "/parent/directory/that/does/not/exist/a.txt";
  auto writeFile = openFileForWrite(path);
  std::string data = "abcdefghijk";
  writeFile->append(data);
  writeFile->flush();
  ASSERT_EQ(writeFile->size(), 0);
  writeFile->append(data);
  writeFile->append(data);
  writeFile->flush();
  writeFile->close();
  ASSERT_EQ(writeFile->size(), data.size() * 3);
}

TEST_F(HdfsFileSystemTest, readFailures) {
  struct hdfsBuilder* builder = hdfsNewBuilder();
  hdfsBuilderSetNameNode(builder, localhost.c_str());
  hdfsBuilderSetNameNodePort(builder, stoi(hdfsPort));
  auto hdfs = hdfsBuilderConnect(builder);
  verifyFailures(hdfs);
}
