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

// TODO: Each prefix should be implemented as its own filesystem.
TEST(S3UtilTest, isS3File) {
  EXPECT_FALSE(isS3File("ss3://"));
  EXPECT_FALSE(isS3File("s3:/"));
  EXPECT_FALSE(isS3File("oss:"));
  EXPECT_FALSE(isS3File("S3A://bucket/some/file.txt"));
  EXPECT_FALSE(isS3File("OSS://other-bucket/some/file.txt"));
  EXPECT_FALSE(isS3File("s3::/bucket"));
  EXPECT_FALSE(isS3File("s3:/bucket"));
  EXPECT_FALSE(isS3File("file://bucket"));
  EXPECT_TRUE(isS3File("s3://bucket/file.txt"));
}

TEST(S3UtilTest, isS3AwsFile) {
  EXPECT_FALSE(isS3AwsFile("s3:"));
  EXPECT_FALSE(isS3AwsFile("s3::/bucket"));
  EXPECT_FALSE(isS3AwsFile("s3:/bucket"));
  EXPECT_TRUE(isS3AwsFile("s3://bucket/file.txt"));
}

TEST(S3UtilTest, isS3aFile) {
  EXPECT_FALSE(isS3aFile("s3a:"));
  EXPECT_FALSE(isS3aFile("s3a::/bucket"));
  EXPECT_FALSE(isS3aFile("s3a:/bucket"));
  EXPECT_FALSE(isS3aFile("S3A://bucket-name/file.txt"));
  EXPECT_TRUE(isS3aFile("s3a://bucket/file.txt"));
}

TEST(S3UtilTest, isOssFile) {
  EXPECT_FALSE(isOssFile("oss:"));
  EXPECT_FALSE(isOssFile("oss::/bucket"));
  EXPECT_FALSE(isOssFile("oss:/bucket"));
  EXPECT_FALSE(isOssFile("OSS://BUCKET/sub-key/file.txt"));
  EXPECT_TRUE(isOssFile("oss://bucket/file.txt"));
}

// TODO: Each prefix should be implemented as its own filesystem.
TEST(S3UtilTest, s3Path) {
  auto path_0 = s3Path("s3://bucket/file.txt");
  auto path_1 = s3Path("oss://bucket-name/file.txt");
  auto path_2 = s3Path("S3A://bucket-NAME/sub-PATH/my-file.txt");
  EXPECT_EQ(path_0, "bucket/file.txt");
  EXPECT_EQ(path_1, "bucket-name/file.txt");
  EXPECT_NE(path_2, "bucket-NAME/sub-PATH/my-file.txt");
}

TEST(S3UtilTest, bucketAndKeyFromS3Path) {
  std::string bucket, key;
  auto path = "bucket/file.txt";
  bucketAndKeyFromS3Path(path, bucket, key);
  EXPECT_EQ(bucket, "bucket");
  EXPECT_EQ(key, "file.txt");
}
