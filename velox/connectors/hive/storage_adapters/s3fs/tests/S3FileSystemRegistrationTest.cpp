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

#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/tests/S3Test.h"

namespace facebook::velox::filesystems {
namespace {

std::string cacheKeyFunc(
    std::shared_ptr<const config::ConfigBase> config,
    std::string_view path) {
  return config->get<std::string>("hive.s3.endpoint").value();
}

class S3FileSystemRegistrationTest : public S3Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    filesystems::registerS3FileSystem(cacheKeyFunc);
  }

  static void TearDownTestCase() {
    filesystems::finalizeS3FileSystem();
  }
};
} // namespace

TEST_F(S3FileSystemRegistrationTest, readViaRegistry) {
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
  {
    auto s3fs = filesystems::getFileSystem(s3File, hiveConfig);
    auto readFile = s3fs->openFileForRead(s3File);
    readData(readFile.get());
  }
}

TEST_F(S3FileSystemRegistrationTest, fileHandle) {
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
      std::make_unique<SimpleLRUCache<FileHandleKey, FileHandle>>(1000),
      std::make_unique<FileHandleGenerator>(hiveConfig));
  FileHandleKey key{s3File};
  auto fileHandleCachePtr = factory.generate(key);
  readData(fileHandleCachePtr->file.get());
}

TEST_F(S3FileSystemRegistrationTest, cacheKey) {
  auto hiveConfig = minioServer_->hiveConfig();
  auto s3fs = filesystems::getFileSystem(kDummyPath, hiveConfig);
  std::string_view kDummyPath2 = "s3://dummy2/foo.txt";
  auto s3fs_new = filesystems::getFileSystem(kDummyPath2, hiveConfig);
  // The cacheKeyFunc function allows fs caching based on the endpoint value.
  ASSERT_EQ(s3fs, s3fs_new);
}

TEST_F(S3FileSystemRegistrationTest, finalize) {
  auto hiveConfig = minioServer_->hiveConfig();
  auto s3fs = filesystems::getFileSystem(kDummyPath, hiveConfig);
  VELOX_ASSERT_THROW(
      filesystems::finalizeS3FileSystem(),
      "Cannot finalize S3FileSystem while in use");
}
} // namespace facebook::velox::filesystems
