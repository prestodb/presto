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

#include "velox/connectors/hive/storage_adapters/s3fs/S3Config.h"
#include "velox/common/config/Config.h"

#include <gtest/gtest.h>

namespace facebook::velox::filesystems {
namespace {
TEST(S3ConfigTest, defaultConfig) {
  auto config = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>());
  auto s3Config = S3Config("", config);
  ASSERT_EQ(s3Config.useVirtualAddressing(), true);
  ASSERT_EQ(s3Config.useSSL(), true);
  ASSERT_EQ(s3Config.useInstanceCredentials(), false);
  ASSERT_EQ(s3Config.endpoint(), "");
  ASSERT_EQ(s3Config.accessKey(), std::nullopt);
  ASSERT_EQ(s3Config.secretKey(), std::nullopt);
  ASSERT_EQ(s3Config.iamRole(), std::nullopt);
  ASSERT_EQ(s3Config.iamRoleSessionName(), "velox-session");
}

TEST(HiveConfigTest, overrideConfig) {
  std::unordered_map<std::string, std::string> configFromFile = {
      {S3Config::baseConfigKey(S3Config::Keys::kPathStyleAccess), "true"},
      {S3Config::baseConfigKey(S3Config::Keys::kSSLEnabled), "false"},
      {S3Config::baseConfigKey(S3Config::Keys::kUseInstanceCredentials),
       "true"},
      {S3Config::baseConfigKey(S3Config::Keys::kEndpoint), "hey"},
      {S3Config::baseConfigKey(S3Config::Keys::kAccessKey), "hello"},
      {S3Config::baseConfigKey(S3Config::Keys::kSecretKey), "hello"},
      {S3Config::baseConfigKey(S3Config::Keys::kIamRole), "hello"},
      {S3Config::baseConfigKey(S3Config::Keys::kIamRoleSessionName), "velox"}};
  auto s3Config = S3Config(
      "", std::make_shared<config::ConfigBase>(std::move(configFromFile)));
  ASSERT_EQ(s3Config.useVirtualAddressing(), false);
  ASSERT_EQ(s3Config.useSSL(), false);
  ASSERT_EQ(s3Config.useInstanceCredentials(), true);
  ASSERT_EQ(s3Config.endpoint(), "hey");
  ASSERT_EQ(s3Config.accessKey(), std::optional("hello"));
  ASSERT_EQ(s3Config.secretKey(), std::optional("hello"));
  ASSERT_EQ(s3Config.iamRole(), std::optional("hello"));
  ASSERT_EQ(s3Config.iamRoleSessionName(), "velox");
}

TEST(HiveConfigTest, overrideBucketConfig) {
  std::string_view bucket = "bucket";
  std::unordered_map<std::string, std::string> bucketConfigFromFile = {
      {S3Config::baseConfigKey(S3Config::Keys::kPathStyleAccess), "true"},
      {S3Config::baseConfigKey(S3Config::Keys::kSSLEnabled), "false"},
      {S3Config::baseConfigKey(S3Config::Keys::kUseInstanceCredentials),
       "true"},
      {S3Config::baseConfigKey(S3Config::Keys::kEndpoint), "hey"},
      {S3Config::bucketConfigKey(S3Config::Keys::kEndpoint, bucket),
       "bucket-hey"},
      {S3Config::baseConfigKey(S3Config::Keys::kAccessKey), "hello"},
      {S3Config::bucketConfigKey(S3Config::Keys::kAccessKey, bucket),
       "bucket-hello"},
      {S3Config::baseConfigKey(S3Config::Keys::kSecretKey), "secret-hello"},
      {S3Config::bucketConfigKey(S3Config::Keys::kSecretKey, bucket),
       "bucket-secret-hello"},
      {S3Config::baseConfigKey(S3Config::Keys::kIamRole), "hello"},
      {S3Config::baseConfigKey(S3Config::Keys::kIamRoleSessionName), "velox"}};
  auto s3Config = S3Config(
      bucket,
      std::make_shared<config::ConfigBase>(std::move(bucketConfigFromFile)));
  ASSERT_EQ(s3Config.useVirtualAddressing(), false);
  ASSERT_EQ(s3Config.useSSL(), false);
  ASSERT_EQ(s3Config.useInstanceCredentials(), true);
  ASSERT_EQ(s3Config.endpoint(), "bucket-hey");
  ASSERT_EQ(s3Config.accessKey(), std::optional("bucket-hello"));
  ASSERT_EQ(s3Config.secretKey(), std::optional("bucket-secret-hello"));
  ASSERT_EQ(s3Config.iamRole(), std::optional("hello"));
  ASSERT_EQ(s3Config.iamRoleSessionName(), "velox");
}

} // namespace
} // namespace facebook::velox::filesystems
