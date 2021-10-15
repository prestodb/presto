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

#include "connectors/hive/storage_adapters/s3fs/S3Util.h"

#include "gtest/gtest.h"

using namespace facebook::velox;

TEST(S3UtilTest, isS3File) {
  EXPECT_FALSE(isS3File("s3:"));
  EXPECT_FALSE(isS3File("s3::/bucket"));
  EXPECT_FALSE(isS3File("s3:/bucket"));
  EXPECT_TRUE(isS3File("s3://bucket/file.txt"));
}

TEST(S3UtilTest, s3Path) {
  auto path = s3Path("s3://bucket/file.txt");
  EXPECT_EQ(path, "bucket/file.txt");
}

TEST(S3UtilTest, bucketAndKeyFromS3Path) {
  std::string bucket, key;
  auto path = "bucket/file.txt";
  bucketAndKeyFromS3Path(path, bucket, key);
  EXPECT_EQ(bucket, "bucket");
  EXPECT_EQ(key, "file.txt");
}
