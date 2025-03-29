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
  ASSERT_EQ(s3Config.endpoint(), std::nullopt);
  ASSERT_EQ(s3Config.endpointRegion(), std::nullopt);
  ASSERT_EQ(s3Config.accessKey(), std::nullopt);
  ASSERT_EQ(s3Config.secretKey(), std::nullopt);
  ASSERT_EQ(s3Config.iamRole(), std::nullopt);
  ASSERT_EQ(s3Config.iamRoleSessionName(), "velox-session");
  ASSERT_EQ(s3Config.payloadSigningPolicy(), "Never");
  ASSERT_EQ(s3Config.cacheKey("foo", config), "foo");
  ASSERT_EQ(s3Config.bucket(), "");
}

TEST(S3ConfigTest, overrideConfig) {
  std::unordered_map<std::string, std::string> configFromFile = {
      {S3Config::baseConfigKey(S3Config::Keys::kPathStyleAccess), "true"},
      {S3Config::baseConfigKey(S3Config::Keys::kSSLEnabled), "false"},
      {S3Config::baseConfigKey(S3Config::Keys::kUseInstanceCredentials),
       "true"},
      {"hive.s3.payload-signing-policy", "RequestDependent"},
      {S3Config::baseConfigKey(S3Config::Keys::kEndpoint), "endpoint"},
      {S3Config::baseConfigKey(S3Config::Keys::kEndpointRegion), "region"},
      {S3Config::baseConfigKey(S3Config::Keys::kAccessKey), "access"},
      {S3Config::baseConfigKey(S3Config::Keys::kSecretKey), "secret"},
      {S3Config::baseConfigKey(S3Config::Keys::kIamRole), "iam"},
      {S3Config::baseConfigKey(S3Config::Keys::kIamRoleSessionName), "velox"},
      {S3Config::baseConfigKey(S3Config::Keys::kCredentialsProvider),
       "my-credentials-provider"}};
  auto configBase =
      std::make_shared<config::ConfigBase>(std::move(configFromFile));
  auto s3Config = S3Config("bucket", configBase);
  ASSERT_EQ(s3Config.useVirtualAddressing(), false);
  ASSERT_EQ(s3Config.useSSL(), false);
  ASSERT_EQ(s3Config.useInstanceCredentials(), true);
  ASSERT_EQ(s3Config.endpoint(), "endpoint");
  ASSERT_EQ(s3Config.endpointRegion(), "region");
  ASSERT_EQ(s3Config.accessKey(), std::optional("access"));
  ASSERT_EQ(s3Config.secretKey(), std::optional("secret"));
  ASSERT_EQ(s3Config.iamRole(), std::optional("iam"));
  ASSERT_EQ(s3Config.iamRoleSessionName(), "velox");
  ASSERT_EQ(s3Config.payloadSigningPolicy(), "RequestDependent");
  ASSERT_EQ(s3Config.cacheKey("foo", configBase), "endpoint-foo");
  ASSERT_EQ(s3Config.cacheKey("bar", configBase), "endpoint-bar");
  ASSERT_EQ(s3Config.bucket(), "bucket");
  ASSERT_EQ(s3Config.credentialsProvider(), "my-credentials-provider");
}

TEST(S3ConfigTest, overrideBucketConfig) {
  std::string_view bucket = "bucket";
  std::unordered_map<std::string, std::string> bucketConfigFromFile = {
      {S3Config::baseConfigKey(S3Config::Keys::kPathStyleAccess), "true"},
      {S3Config::baseConfigKey(S3Config::Keys::kSSLEnabled), "false"},
      {S3Config::baseConfigKey(S3Config::Keys::kUseInstanceCredentials),
       "true"},
      {S3Config::baseConfigKey(S3Config::Keys::kEndpoint), "endpoint"},
      {S3Config::bucketConfigKey(S3Config::Keys::kEndpoint, bucket),
       "bucket.s3-region.amazonaws.com"},
      {S3Config::baseConfigKey(S3Config::Keys::kAccessKey), "access"},
      {S3Config::bucketConfigKey(S3Config::Keys::kAccessKey, bucket),
       "bucket-access"},
      {"hive.s3.payload-signing-policy", "Always"},
      {S3Config::baseConfigKey(S3Config::Keys::kSecretKey), "secret"},
      {S3Config::bucketConfigKey(S3Config::Keys::kSecretKey, bucket),
       "bucket-secret"},
      {S3Config::baseConfigKey(S3Config::Keys::kIamRole), "iam"},
      {S3Config::baseConfigKey(S3Config::Keys::kIamRoleSessionName), "velox"},
      {S3Config::baseConfigKey(S3Config::Keys::kCredentialsProvider),
       "my-credentials-provider"},
      {S3Config::bucketConfigKey(S3Config::Keys::kCredentialsProvider, bucket),
       "override-credentials-provider"}};
  auto configBase =
      std::make_shared<config::ConfigBase>(std::move(bucketConfigFromFile));
  auto s3Config = S3Config(bucket, configBase);
  ASSERT_EQ(s3Config.useVirtualAddressing(), false);
  ASSERT_EQ(s3Config.useSSL(), false);
  ASSERT_EQ(s3Config.useInstanceCredentials(), true);
  ASSERT_EQ(s3Config.endpoint(), "bucket.s3-region.amazonaws.com");
  // Inferred from the endpoint.
  ASSERT_EQ(s3Config.endpointRegion(), "region");
  ASSERT_EQ(s3Config.accessKey(), std::optional("bucket-access"));
  ASSERT_EQ(s3Config.secretKey(), std::optional("bucket-secret"));
  ASSERT_EQ(s3Config.iamRole(), std::optional("iam"));
  ASSERT_EQ(s3Config.iamRoleSessionName(), "velox");
  ASSERT_EQ(s3Config.payloadSigningPolicy(), "Always");
  ASSERT_EQ(
      s3Config.cacheKey(bucket, configBase),
      "bucket.s3-region.amazonaws.com-bucket");
  ASSERT_EQ(s3Config.cacheKey("foo", configBase), "endpoint-foo");
  ASSERT_EQ(s3Config.credentialsProvider(), "override-credentials-provider");
}

} // namespace
} // namespace facebook::velox::filesystems
