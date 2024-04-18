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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/File.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"
#include "velox/connectors/hive/storage_adapters/s3fs/tests/MinioServer.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/TempFilePath.h"

#include "gtest/gtest.h"

using namespace facebook::velox;

constexpr int kOneMB = 1 << 20;

static constexpr std::string_view kDummyPath = "s3://dummy/foo.txt";

class S3Test : public testing::Test, public ::test::VectorTestBase {
 protected:
  void SetUp() override {
    minioServer_ = std::make_unique<MinioServer>();
    minioServer_->start();
    ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(3);
  }

  void TearDown() override {
    minioServer_->stop();
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
            (char*)(uint64_t)(15 + kOneMB - 500000 - sizeof(head) -
                              sizeof(middle) - sizeof(tail))),
        folly::Range<char*>(tail, sizeof(tail))};
    ASSERT_EQ(15 + kOneMB, readFile->preadv(0, buffers));
    ASSERT_EQ(std::string_view(head, sizeof(head)), "aaaaabbbbbcc");
    ASSERT_EQ(std::string_view(middle, sizeof(middle)), "cccc");
    ASSERT_EQ(std::string_view(tail, sizeof(tail)), "ccddddd");
  }

  std::unique_ptr<MinioServer> minioServer_;
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
};
