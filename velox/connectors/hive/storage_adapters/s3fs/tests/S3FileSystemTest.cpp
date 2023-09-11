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

#include "velox/connectors/hive/storage_adapters/s3fs/tests/S3Test.h"

#include "gtest/gtest.h"

using namespace facebook::velox;

static constexpr std::string_view kMinioConnectionString = "127.0.0.1:9000";

class S3FileSystemTest : public S3Test {
 protected:
  static void SetUpTestSuite() {
    if (minioServer_ == nullptr) {
      minioServer_ = std::make_shared<MinioServer>(kMinioConnectionString);
      minioServer_->start();
    }
    auto hiveConfig = minioServer_->hiveConfig({{"hive.s3.log-level", "Info"}});
    filesystems::initializeS3(hiveConfig.get());
  }

  static void TearDownTestSuite() {
    if (minioServer_ != nullptr) {
      minioServer_->stop();
      minioServer_ = nullptr;
    }
  }
};

TEST_F(S3FileSystemTest, writeAndRead) {
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
    const std::unordered_map<std::string, std::string> config(
        {{"hive.s3.use-instance-credentials", "true"},
         {"hive.s3.iam-role", "dummy-iam-role"}});
    auto hiveConfig =
        std::make_shared<const core::MemConfig>(std::move(config));

    // Both instance credentials and iam-role cannot be specified
    VELOX_ASSERT_THROW(
        filesystems::S3FileSystem(hiveConfig),
        "Invalid configuration: specify only one among 'access/secret keys', 'use instance credentials', 'IAM role'");
  }
  {
    const std::unordered_map<std::string, std::string> config(
        {{"hive.s3.aws-secret-key", "dummy-key"},
         {"hive.s3.aws-access-key", "dummy-key"},
         {"hive.s3.iam-role", "dummy-iam-role"}});
    auto hiveConfig =
        std::make_shared<const core::MemConfig>(std::move(config));
    // Both access/secret keys and iam-role cannot be specified
    VELOX_ASSERT_THROW(
        filesystems::S3FileSystem(hiveConfig),
        "Invalid configuration: specify only one among 'access/secret keys', 'use instance credentials', 'IAM role'");
  }
  {
    const std::unordered_map<std::string, std::string> config(
        {{"hive.s3.aws-secret-key", "dummy"},
         {"hive.s3.aws-access-key", "dummy"},
         {"hive.s3.use-instance-credentials", "true"}});
    auto hiveConfig =
        std::make_shared<const core::MemConfig>(std::move(config));
    // Both access/secret keys and instance credentials cannot be specified
    VELOX_ASSERT_THROW(
        filesystems::S3FileSystem(hiveConfig),
        "Invalid configuration: specify only one among 'access/secret keys', 'use instance credentials', 'IAM role'");
  }
  {
    const std::unordered_map<std::string, std::string> config(
        {{"hive.s3.aws-secret-key", "dummy"}});
    auto hiveConfig =
        std::make_shared<const core::MemConfig>(std::move(config));
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
  VELOX_ASSERT_THROW(
      s3fs.openFileForRead(s3File),
      "Failed to get metadata for S3 object due to: 'Resource not found'. Path:'s3://data1/i-do-not-exist.txt', SDK Error Type:16, HTTP Status Code:404, S3 Service:'MinIO', Message:'No response body.'");
}

TEST_F(S3FileSystemTest, missingBucket) {
  auto hiveConfig = minioServer_->hiveConfig();
  filesystems::S3FileSystem s3fs(hiveConfig);
  VELOX_ASSERT_THROW(
      s3fs.openFileForRead(kDummyPath),
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
    auto s3Config = std::make_shared<const core::MemConfig>(config);
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

// This test has to be the last one as it invokes finalizeS3.
TEST_F(S3FileSystemTest, finalize) {
  auto s3Config = minioServer_->hiveConfig();
  ASSERT_FALSE(filesystems::initializeS3(s3Config.get()));
  {
    filesystems::S3FileSystem s3fs(s3Config);
    VELOX_ASSERT_THROW(
        filesystems::finalizeS3(), "Cannot finalize S3 while in use");
  }
  filesystems::finalizeS3();
  VELOX_ASSERT_THROW(
      filesystems::initializeS3(s3Config.get()),
      "Attempt to initialize S3 after it has been finalized.");
}
