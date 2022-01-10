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

  std::string s3URI(const char* bucket) {
    return std::string(kS3Scheme) + bucket;
  }

  void writeData(WriteFile* writeFile) {
    writeFile->append("aaaaa");
    writeFile->append("bbbbb");
    writeFile->append(std::string(kOneMB, 'c'));
    writeFile->append("ddddd");
    ASSERT_EQ(writeFile->size(), 15 + kOneMB);
  }

  void readData(ReadFile* readFile) {
    Arena arena;
    ASSERT_EQ(readFile->size(), 15 + kOneMB);
    ASSERT_EQ(readFile->pread(10 + kOneMB, 5, &arena), "ddddd");
    ASSERT_EQ(readFile->pread(0, 10, &arena), "aaaaabbbbb");
    ASSERT_EQ(readFile->pread(10, kOneMB, &arena), std::string(kOneMB, 'c'));
    ASSERT_EQ(readFile->size(), 15 + kOneMB);
    const std::string_view arf = readFile->pread(5, 10, &arena);
    const std::string zarf = readFile->pread(kOneMB, 15);
    auto buf = std::make_unique<char[]>(8);
    const std::string_view warf = readFile->pread(4, 8, buf.get());
    const std::string_view warfFromBuf(buf.get(), 8);
    ASSERT_EQ(arf, "bbbbbccccc");
    ASSERT_EQ(zarf, "ccccccccccddddd");
    ASSERT_EQ(warf, "abbbbbcc");
    ASSERT_EQ(warfFromBuf, "abbbbbcc");
  }

  static std::shared_ptr<MinioServer> minioServer_;
};

std::shared_ptr<MinioServer> S3FileSystemTest::minioServer_ = nullptr;

TEST_F(S3FileSystemTest, writeAndRead) {
  const char* bucketName = "data";
  const char* file = "test.txt";
  const std::string filename = localPath(bucketName) + "/" + file;
  const std::string s3File = s3URI(bucketName) + "/" + file;
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

TEST_F(S3FileSystemTest, missingFile) {
  const char* bucketName = "data1";
  const char* file = "i-do-not-exist.txt";
  const std::string s3File = s3URI(bucketName) + "/" + file;
  addBucket(bucketName);
  auto hiveConfig = minioServer_->hiveConfig();
  filesystems::S3FileSystem s3fs(hiveConfig);
  s3fs.initializeClient();
  EXPECT_THROW(s3fs.openFileForRead(s3File), VeloxException);
}

TEST_F(S3FileSystemTest, viaRegistry) {
  const char* bucketName = "data2";
  const char* file = "test.txt";
  const std::string filename = localPath(bucketName) + "/" + file;
  const std::string s3File = s3URI(bucketName) + "/" + file;
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
  const std::string s3File = s3URI(bucketName) + "/" + file;
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
