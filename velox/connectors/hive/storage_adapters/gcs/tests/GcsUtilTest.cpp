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

#include "velox/connectors/hive/storage_adapters/gcs/GcsUtil.h"

#include "gtest/gtest.h"

using namespace facebook::velox;

TEST(GcsUtilTest, isGcsFile) {
  EXPECT_FALSE(isGcsFile("gs:"));
  EXPECT_FALSE(isGcsFile("gs::/bucket"));
  EXPECT_FALSE(isGcsFile("gs:/bucket"));
  EXPECT_TRUE(isGcsFile("gs://bucket/file.txt"));
}

TEST(GcsUtilTest, setBucketAndKeyFromGcsPath) {
  std::string bucket, key;
  auto path = "bucket/file.txt";
  setBucketAndKeyFromGcsPath(path, bucket, key);
  EXPECT_EQ(bucket, "bucket");
  EXPECT_EQ(key, "file.txt");
}
