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

#include "connectors/hive/storage_adapters/s3fs/S3FileSystem.h"
#include "connectors/hive/storage_adapters/s3fs/S3Util.h"
#include "connectors/hive/storage_adapters/s3fs/tests/MinioServer.h"
#include "velox/common/file/File.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/exec/tests/utils/TempFilePath.h"

#include "gtest/gtest.h"

using namespace facebook::velox;

constexpr int kOneMB = 1 << 20;

class S3FileSystemTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    if (minioServer_ == nullptr) {
      minioServer_ = std::make_shared<MinioServer>();
      minioServer_->start();
    }
    filesystems::registerS3FileSystem();
  }

  static void TearDownTestSuite() {
    if (minioServer_ != nullptr) {
      minioServer_->stop();
      minioServer_ = nullptr;
    }
  }

  void addBucket(const char* bucket) {
    minioServer_->addBucket(bucket);
  }

  std::string localPath(const char* directory) {
    return minioServer_->path() + "/" + directory;
  }

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

  static std::shared_ptr<MinioServer> minioServer_;
};

std::shared_ptr<MinioServer> S3FileSystemTest::minioServer_ = nullptr;

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
  s3fs.initializeClient();
  auto readFile = s3fs.openFileForRead(s3File);
  readData(readFile.get());
}

TEST_F(S3FileSystemTest, viaRegistry) {
  const char* bucketName = "data2";
  const char* file = "test.txt";
  const std::string filename = localPath(bucketName) + "/" + file;
  const std::string s3File = s3URI(bucketName, file);
  addBucket(bucketName);
  {
    LocalWriteFile writeFile(filename);
    writeData(&writeFile);
  }
  auto hiveConfig = minioServer_->hiveConfig();
  auto s3fs = filesystems::getFileSystem(s3File, hiveConfig);
  auto readFile = s3fs->openFileForRead(s3File);
  readData(readFile.get());
}

TEST_F(S3FileSystemTest, fileHandle) {
  const char* bucketName = "data3";
  const char* file = "test.txt";
  const std::string filename = localPath(bucketName) + "/" + file;
  const std::string s3File = s3URI(bucketName, file);
  addBucket(bucketName);
  {
    LocalWriteFile writeFile(filename);
    writeData(&writeFile);
  }
  auto hiveConfig = minioServer_->hiveConfig();
  FileHandleFactory factory(
      std::make_unique<SimpleLRUCache<std::string, FileHandle>>(1000),
      std::make_unique<FileHandleGenerator>(hiveConfig));
  auto fileHandle = factory.generate(s3File);
  readData(fileHandle->file.get());
}

TEST_F(S3FileSystemTest, invalidCredentialsConfig) {
  {
    const std::unordered_map<std::string, std::string> config(
        {{"hive.s3.use-instance-credentials", "true"},
         {"hive.s3.iam-role", "dummy-iam-role"}});
    auto hiveConfig =
        std::make_shared<const core::MemConfig>(std::move(config));
    filesystems::S3FileSystem s3fs(hiveConfig);
    // Both instance credentials and iam-role cannot be specified
    try {
      s3fs.initializeClient();
      FAIL() << "Expected VeloxException";
    } catch (VeloxException const& err) {
      EXPECT_EQ(
          err.message(),
          std::string(
              "Invalid configuration: specify only one among 'access/secret keys', 'use instance credentials', 'IAM role'"));
    }
  }
  {
    const std::unordered_map<std::string, std::string> config(
        {{"hive.s3.aws-secret-key", "dummy-key"},
         {"hive.s3.aws-access-key", "dummy-key"},
         {"hive.s3.iam-role", "dummy-iam-role"}});
    auto hiveConfig =
        std::make_shared<const core::MemConfig>(std::move(config));
    filesystems::S3FileSystem s3fs(hiveConfig);
    // Both access/secret keys and iam-role cannot be specified
    try {
      s3fs.initializeClient();
      FAIL() << "Expected VeloxException";
    } catch (VeloxException const& err) {
      EXPECT_EQ(
          err.message(),
          std::string(
              "Invalid configuration: specify only one among 'access/secret keys', 'use instance credentials', 'IAM role'"));
    }
  }
  {
    const std::unordered_map<std::string, std::string> config(
        {{"hive.s3.aws-secret-key", "dummy"},
         {"hive.s3.aws-access-key", "dummy"},
         {"hive.s3.use-instance-credentials", "true"}});
    auto hiveConfig =
        std::make_shared<const core::MemConfig>(std::move(config));
    filesystems::S3FileSystem s3fs(hiveConfig);
    // Both access/secret keys and instance credentials cannot be specified
    try {
      s3fs.initializeClient();
      FAIL() << "Expected VeloxException";
    } catch (VeloxException const& err) {
      EXPECT_EQ(
          err.message(),
          std::string(
              "Invalid configuration: specify only one among 'access/secret keys', 'use instance credentials', 'IAM role'"));
    }
  }
  {
    const std::unordered_map<std::string, std::string> config(
        {{"hive.s3.aws-secret-key", "dummy"}});
    auto hiveConfig =
        std::make_shared<const core::MemConfig>(std::move(config));
    filesystems::S3FileSystem s3fs(hiveConfig);
    // Both access key and secret key must be specified
    try {
      s3fs.initializeClient();
      FAIL() << "Expected VeloxException";
    } catch (VeloxException const& err) {
      EXPECT_EQ(
          err.message(),
          std::string(
              "Invalid configuration: both access key and secret key must be specified"));
    }
  }
}

TEST_F(S3FileSystemTest, missingFile) {
  const char* bucketName = "data1";
  const char* file = "i-do-not-exist.txt";
  const std::string s3File = s3URI(bucketName, file);
  addBucket(bucketName);
  auto hiveConfig = minioServer_->hiveConfig();
  filesystems::S3FileSystem s3fs(hiveConfig);
  s3fs.initializeClient();
  try {
    s3fs.openFileForRead(s3File);
    FAIL() << "Expected VeloxException";
  } catch (VeloxException const& err) {
    EXPECT_EQ(
        err.message(),
        std::string(
            "Failed to get metadata for S3 object due to: 'Resource not found'. Path:'s3://data1/i-do-not-exist.txt', SDK Error Type:16, HTTP Status Code:404, S3 Service:'MinIO', Message:'No response body.'"));
  }
}

TEST_F(S3FileSystemTest, missingBucket) {
  auto hiveConfig = minioServer_->hiveConfig();
  filesystems::S3FileSystem s3fs(hiveConfig);
  s3fs.initializeClient();
  try {
    const char* s3File = "s3://dummy/foo.txt";
    s3fs.openFileForRead(s3File);
    FAIL() << "Expected VeloxException";
  } catch (VeloxException const& err) {
    EXPECT_EQ(
        err.message(),
        std::string(
            "Failed to get metadata for S3 object due to: 'Resource not found'. Path:'s3://dummy/foo.txt', SDK Error Type:16, HTTP Status Code:404, S3 Service:'MinIO', Message:'No response body.'"));
  }
}

TEST_F(S3FileSystemTest, invalidAccessKey) {
  auto hiveConfig =
      minioServer_->hiveConfig({{"hive.s3.aws-access-key", "dummy-key"}});
  filesystems::S3FileSystem s3fs(hiveConfig);
  s3fs.initializeClient();
  // Minio credentials are wrong and this should throw
  try {
    const char* s3File = "s3://dummy/foo.txt";
    s3fs.openFileForRead(s3File);
    FAIL() << "Expected VeloxException";
  } catch (VeloxException const& err) {
    EXPECT_EQ(
        err.message(),
        std::string(
            "Failed to get metadata for S3 object due to: 'Access denied'. Path:'s3://dummy/foo.txt', SDK Error Type:15, HTTP Status Code:403, S3 Service:'MinIO', Message:'No response body.'"));
  }
}

TEST_F(S3FileSystemTest, invalidSecretKey) {
  auto hiveConfig =
      minioServer_->hiveConfig({{"hive.s3.aws-secret-key", "dummy-key"}});
  filesystems::S3FileSystem s3fs(hiveConfig);
  s3fs.initializeClient();
  // Minio credentials are wrong and this should throw
  try {
    const char* s3File = "s3://dummy/foo.txt";
    s3fs.openFileForRead(s3File);
    FAIL() << "Expected VeloxException";
  } catch (VeloxException const& err) {
    EXPECT_EQ(
        err.message(),
        std::string(
            "Failed to get metadata for S3 object due to: 'Access denied'. Path:'s3://dummy/foo.txt', SDK Error Type:15, HTTP Status Code:403, S3 Service:'MinIO', Message:'No response body.'"));
  }
}

TEST_F(S3FileSystemTest, noBackendServer) {
  auto hiveConfig =
      minioServer_->hiveConfig({{"hive.s3.aws-secret-key", "dummy-key"}});
  filesystems::S3FileSystem s3fs(hiveConfig);
  s3fs.initializeClient();
  // stop Minio and check error
  minioServer_->stop();
  try {
    const char* s3File = "s3://dummy/foo.txt";
    s3fs.openFileForRead(s3File);
    FAIL() << "Expected VeloxException";
  } catch (VeloxException const& err) {
    EXPECT_EQ(
        err.message(),
        std::string(
            "Failed to get metadata for S3 object due to: 'Network connection'. Path:'s3://dummy/foo.txt', SDK Error Type:99, HTTP Status Code:-1, S3 Service:'Unknown', Message:'curlCode: 7, Couldn't connect to server'"));
  }
}
