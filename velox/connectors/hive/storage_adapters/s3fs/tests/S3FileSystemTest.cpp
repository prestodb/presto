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

#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3WriteFile.h"
#include "velox/connectors/hive/storage_adapters/s3fs/tests/S3Test.h"

#include <gtest/gtest.h>

namespace facebook::velox {
namespace {

class S3FileSystemTest : public S3Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    S3Test::SetUp();
    auto hiveConfig = minioServer_->hiveConfig({{"hive.s3.log-level", "Info"}});
    filesystems::initializeS3(hiveConfig.get());
  }

  static void TearDownTestSuite() {
    filesystems::finalizeS3();
  }
};
} // namespace

TEST_F(S3FileSystemTest, writeAndRead) {
  /// The hive config used for Minio defaults to turning
  /// off using proxy settings if the environment provides them.
  setenv("HTTP_PROXY", "http://test:test@127.0.0.1:8888", 1);
  const char* bucketName = "data";
  const char* file = "test.txt";
  const std::string filename = localPath(bucketName) + "/" + file;
  const std::string s3File = s3URI(bucketName, file);
  addBucket(bucketName);
  {
    LocalWriteFile writeFile(filename);
    writeData(&writeFile);
  }
  auto hiveConfig = minioServer_->hiveConfig();
  filesystems::S3FileSystem s3fs(hiveConfig);
  auto readFile = s3fs.openFileForRead(s3File);
  readData(readFile.get());
}

TEST_F(S3FileSystemTest, invalidCredentialsConfig) {
  {
    std::unordered_map<std::string, std::string> config(
        {{"hive.s3.use-instance-credentials", "true"},
         {"hive.s3.iam-role", "dummy-iam-role"}});
    auto hiveConfig =
        std::make_shared<const config::ConfigBase>(std::move(config));

    // Both instance credentials and iam-role cannot be specified
    VELOX_ASSERT_THROW(
        filesystems::S3FileSystem(hiveConfig),
        "Invalid configuration: specify only one among 'access/secret keys', 'use instance credentials', 'IAM role'");
  }
  {
    std::unordered_map<std::string, std::string> config(
        {{"hive.s3.aws-secret-key", "dummy-key"},
         {"hive.s3.aws-access-key", "dummy-key"},
         {"hive.s3.iam-role", "dummy-iam-role"}});
    auto hiveConfig =
        std::make_shared<const config::ConfigBase>(std::move(config));
    // Both access/secret keys and iam-role cannot be specified
    VELOX_ASSERT_THROW(
        filesystems::S3FileSystem(hiveConfig),
        "Invalid configuration: specify only one among 'access/secret keys', 'use instance credentials', 'IAM role'");
  }
  {
    std::unordered_map<std::string, std::string> config(
        {{"hive.s3.aws-secret-key", "dummy"},
         {"hive.s3.aws-access-key", "dummy"},
         {"hive.s3.use-instance-credentials", "true"}});
    auto hiveConfig =
        std::make_shared<const config::ConfigBase>(std::move(config));
    // Both access/secret keys and instance credentials cannot be specified
    VELOX_ASSERT_THROW(
        filesystems::S3FileSystem(hiveConfig),
        "Invalid configuration: specify only one among 'access/secret keys', 'use instance credentials', 'IAM role'");
  }
  {
    std::unordered_map<std::string, std::string> config(
        {{"hive.s3.aws-secret-key", "dummy"}});
    auto hiveConfig =
        std::make_shared<const config::ConfigBase>(std::move(config));
    // Both access key and secret key must be specified
    VELOX_ASSERT_THROW(
        filesystems::S3FileSystem(hiveConfig),
        "Invalid configuration: both access key and secret key must be specified");
  }
}

TEST_F(S3FileSystemTest, missingFile) {
  const char* bucketName = "data1";
  const char* file = "i-do-not-exist.txt";
  const std::string s3File = s3URI(bucketName, file);
  addBucket(bucketName);
  auto hiveConfig = minioServer_->hiveConfig();
  filesystems::S3FileSystem s3fs(hiveConfig);
  VELOX_ASSERT_RUNTIME_THROW_CODE(
      s3fs.openFileForRead(s3File),
      error_code::kFileNotFound,
      "Failed to get metadata for S3 object due to: 'Resource not found'. Path:'s3://data1/i-do-not-exist.txt', SDK Error Type:16, HTTP Status Code:404, S3 Service:'MinIO', Message:'No response body.'");
}

TEST_F(S3FileSystemTest, missingBucket) {
  auto hiveConfig = minioServer_->hiveConfig();
  filesystems::S3FileSystem s3fs(hiveConfig);
  VELOX_ASSERT_RUNTIME_THROW_CODE(
      s3fs.openFileForRead(kDummyPath),
      error_code::kFileNotFound,
      "Failed to get metadata for S3 object due to: 'Resource not found'. Path:'s3://dummy/foo.txt', SDK Error Type:16, HTTP Status Code:404, S3 Service:'MinIO', Message:'No response body.'");
}

TEST_F(S3FileSystemTest, invalidAccessKey) {
  auto hiveConfig =
      minioServer_->hiveConfig({{"hive.s3.aws-access-key", "dummy-key"}});
  filesystems::S3FileSystem s3fs(hiveConfig);
  // Minio credentials are wrong and this should throw
  VELOX_ASSERT_THROW(
      s3fs.openFileForRead(kDummyPath),
      "Failed to get metadata for S3 object due to: 'Access denied'. Path:'s3://dummy/foo.txt', SDK Error Type:15, HTTP Status Code:403, S3 Service:'MinIO', Message:'No response body.'");
}

TEST_F(S3FileSystemTest, invalidSecretKey) {
  auto hiveConfig =
      minioServer_->hiveConfig({{"hive.s3.aws-secret-key", "dummy-key"}});
  filesystems::S3FileSystem s3fs(hiveConfig);
  // Minio credentials are wrong and this should throw.
  VELOX_ASSERT_THROW(
      s3fs.openFileForRead("s3://dummy/foo.txt"),
      "Failed to get metadata for S3 object due to: 'Access denied'. Path:'s3://dummy/foo.txt', SDK Error Type:15, HTTP Status Code:403, S3 Service:'MinIO', Message:'No response body.'");
}

TEST_F(S3FileSystemTest, noBackendServer) {
  auto hiveConfig =
      minioServer_->hiveConfig({{"hive.s3.aws-secret-key", "dummy-key"}});
  filesystems::S3FileSystem s3fs(hiveConfig);
  // Stop Minio and check error.
  minioServer_->stop();
  VELOX_ASSERT_THROW(
      s3fs.openFileForRead(kDummyPath),
      "Failed to get metadata for S3 object due to: 'Network connection'. Path:'s3://dummy/foo.txt', SDK Error Type:99, HTTP Status Code:-1, S3 Service:'Unknown', Message:'curlCode: 7, Couldn't connect to server'");
  // Start Minio again.
  minioServer_->start();
}

TEST_F(S3FileSystemTest, logLevel) {
  std::unordered_map<std::string, std::string> config;
  auto checkLogLevelName = [&config](std::string_view expected) {
    auto s3Config =
        std::make_shared<const config::ConfigBase>(std::move(config));
    filesystems::S3FileSystem s3fs(s3Config);
    EXPECT_EQ(s3fs.getLogLevelName(), expected);
  };

  // Test is configured with INFO.
  checkLogLevelName("INFO");

  // S3 log level is set once during initialization.
  // It does not change with a new config.
  config["hive.s3.log-level"] = "Trace";
  checkLogLevelName("INFO");
}

TEST_F(S3FileSystemTest, writeFileAndRead) {
  const auto bucketName = "writedata";
  const auto file = "test.txt";
  const auto filename = localPath(bucketName) + "/" + file;
  const auto s3File = s3URI(bucketName, file);

  auto hiveConfig = minioServer_->hiveConfig();
  filesystems::S3FileSystem s3fs(hiveConfig);
  auto pool = memory::memoryManager()->addLeafPool("S3FileSystemTest");
  auto writeFile =
      s3fs.openFileForWrite(s3File, {{}, pool.get(), std::nullopt});
  auto s3WriteFile = dynamic_cast<filesystems::S3WriteFile*>(writeFile.get());
  std::string dataContent =
      "Dance me to your beauty with a burning violin"
      "Dance me through the panic till I'm gathered safely in"
      "Lift me like an olive branch and be my homeward dove"
      "Dance me to the end of love";

  EXPECT_EQ(writeFile->size(), 0);
  std::int64_t contentSize = dataContent.length();
  // dataContent length is 178.
  EXPECT_EQ(contentSize, 178);

  // Append and flush a small batch of data.
  writeFile->append(dataContent.substr(0, 10));
  EXPECT_EQ(writeFile->size(), 10);
  writeFile->append(dataContent.substr(10, contentSize - 10));
  EXPECT_EQ(writeFile->size(), contentSize);
  writeFile->flush();
  // No parts must have been uploaded.
  EXPECT_EQ(s3WriteFile->numPartsUploaded(), 0);

  // Append data 178 * 100'000 ~ 16MiB.
  // Should have 1 part in total with kUploadPartSize = 10MiB.
  for (int i = 0; i < 100'000; ++i) {
    writeFile->append(dataContent);
  }
  EXPECT_EQ(s3WriteFile->numPartsUploaded(), 1);
  EXPECT_EQ(writeFile->size(), 100'001 * contentSize);

  // Append a large data buffer 178 * 150'000 ~ 25MiB (2 parts).
  std::vector<char> largeBuffer(contentSize * 150'000);
  for (int i = 0; i < 150'000; ++i) {
    memcpy(
        largeBuffer.data() + (i * contentSize),
        dataContent.data(),
        contentSize);
  }

  writeFile->append({largeBuffer.data(), largeBuffer.size()});
  EXPECT_EQ(writeFile->size(), 250'001 * contentSize);
  // Total data = ~41 MB = 5 parts.
  // But parts uploaded will be 4.
  EXPECT_EQ(s3WriteFile->numPartsUploaded(), 4);

  // Upload the last part.
  writeFile->close();
  EXPECT_EQ(s3WriteFile->numPartsUploaded(), 5);

  VELOX_ASSERT_THROW(
      writeFile->append(dataContent.substr(0, 10)), "File is closed");

  auto readFile = s3fs.openFileForRead(s3File);
  ASSERT_EQ(readFile->size(), contentSize * 250'001);
  // Sample and verify every 1'000 dataContent chunks.
  for (int i = 0; i < 250; ++i) {
    ASSERT_EQ(
        readFile->pread(i * (1'000 * contentSize), contentSize), dataContent);
  }
  // Verify the last chunk.
  ASSERT_EQ(readFile->pread(contentSize * 250'000, contentSize), dataContent);
}

TEST_F(S3FileSystemTest, invalidConnectionSettings) {
  auto hiveConfig =
      minioServer_->hiveConfig({{"hive.s3.connect-timeout", "400"}});
  VELOX_ASSERT_THROW(filesystems::S3FileSystem(hiveConfig), "Invalid duration");

  hiveConfig = minioServer_->hiveConfig({{"hive.s3.socket-timeout", "abc"}});
  VELOX_ASSERT_THROW(filesystems::S3FileSystem(hiveConfig), "Invalid duration");
}
} // namespace facebook::velox
