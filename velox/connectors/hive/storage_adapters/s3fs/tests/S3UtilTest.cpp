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

#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"

#include "gtest/gtest.h"

namespace facebook::velox {

// TODO: Each prefix should be implemented as its own filesystem.
TEST(S3UtilTest, isS3File) {
  EXPECT_FALSE(isS3File("ss3://"));
  EXPECT_FALSE(isS3File("s3:/"));
  EXPECT_FALSE(isS3File("oss:"));
  EXPECT_FALSE(isS3File("cos:"));
  EXPECT_FALSE(isS3File("cosn:"));
  EXPECT_FALSE(isS3File("S3A://bucket/some/file.txt"));
  EXPECT_FALSE(isS3File("OSS://other-bucket/some/file.txt"));
  EXPECT_FALSE(isS3File("COS://other-bucket/some/file.txt"));
  EXPECT_FALSE(isS3File("COSN://other-bucket/some/file.txt"));
  EXPECT_FALSE(isS3File("s3::/bucket"));
  EXPECT_FALSE(isS3File("s3:/bucket"));
  EXPECT_FALSE(isS3File("file://bucket"));
  EXPECT_TRUE(isS3File("s3://bucket/file.txt"));
  EXPECT_TRUE(isS3File("s3n://bucket/file.txt"));
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

TEST(S3UtilTest, isS3nFile) {
  EXPECT_FALSE(isS3nFile("s3n:"));
  EXPECT_FALSE(isS3nFile("s3n::/bucket"));
  EXPECT_FALSE(isS3nFile("s3n:/bucket"));
  EXPECT_FALSE(isS3nFile("S3N://bucket-name/file.txt"));
  EXPECT_TRUE(isS3nFile("s3n://bucket/file.txt"));
}

TEST(S3UtilTest, isOssFile) {
  EXPECT_FALSE(isOssFile("oss:"));
  EXPECT_FALSE(isOssFile("oss::/bucket"));
  EXPECT_FALSE(isOssFile("oss:/bucket"));
  EXPECT_FALSE(isOssFile("OSS://BUCKET/sub-key/file.txt"));
  EXPECT_TRUE(isOssFile("oss://bucket/file.txt"));
}

TEST(S3UtilTest, isCosFile) {
  EXPECT_FALSE(isCosFile("cos:"));
  EXPECT_FALSE(isCosFile("cos::/bucket"));
  EXPECT_FALSE(isCosFile("cos:/bucket"));
  EXPECT_FALSE(isCosFile("COS://BUCKET/sub-key/file.txt"));
  EXPECT_TRUE(isCosFile("cos://bucket/file.txt"));
}

TEST(S3UtilTest, isCosNFile) {
  EXPECT_FALSE(isCosNFile("cosn:"));
  EXPECT_FALSE(isCosNFile("cosn::/bucket"));
  EXPECT_FALSE(isCosNFile("cosn:/bucket"));
  EXPECT_FALSE(isCosNFile("COSN://BUCKET/sub-key/file.txt"));
  EXPECT_TRUE(isCosNFile("cosn://bucket/file.txt"));
}

// TODO: Each prefix should be implemented as its own filesystem.
TEST(S3UtilTest, s3Path) {
  auto path_0 = s3Path("s3://bucket/file.txt");
  auto path_1 = s3Path("oss://bucket-name/file.txt");
  auto path_2 = s3Path("S3A://bucket-NAME/sub-PATH/my-file.txt");
  auto path_3 = s3Path("s3N://bucket-NAME/sub-PATH/my-file.txt");
  auto path_4 = s3Path("cos://bucket-name/file.txt");
  auto path_5 = s3Path("cosn://bucket-name/file.txt");
  EXPECT_EQ(path_0, "bucket/file.txt");
  EXPECT_EQ(path_1, "bucket-name/file.txt");
  EXPECT_NE(path_2, "bucket-NAME/sub-PATH/my-file.txt");
  EXPECT_NE(path_3, "bucket-NAME/sub-PATH/my-file.txt");
  EXPECT_EQ(path_4, "bucket-name/file.txt");
  EXPECT_EQ(path_5, "bucket-name/file.txt");
}

TEST(S3UtilTest, bucketAndKeyFromS3Path) {
  std::string bucket, key;
  auto path = "bucket/file.txt";
  getBucketAndKeyFromS3Path(path, bucket, key);
  EXPECT_EQ(bucket, "bucket");
  EXPECT_EQ(key, "file.txt");
}
} // namespace facebook::velox
