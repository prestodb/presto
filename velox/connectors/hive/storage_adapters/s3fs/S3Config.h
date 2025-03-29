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
#pragma once

#include <optional>
#include <string>
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::config {
class ConfigBase;
}

namespace facebook::velox::filesystems {

/// Build config required to initialize an S3FileSystem instance.
/// All hive.s3 options can be set on a per-bucket basis.
/// The bucket-specific option is set by replacing the hive.s3. prefix on an
/// option with hive.s3.bucket.BUCKETNAME., where BUCKETNAME is the name of the
/// bucket.
/// When connecting to a bucket, all options explicitly set will override the
/// base hive.s3. values.
/// These semantics are similar to the Apache Hadoop-Aws module.
/// https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html
class S3Config {
 public:
  S3Config() = delete;

  /// S3 config prefix.
  static constexpr const char* kS3Prefix = "hive.s3.";

  /// S3 bucket config prefix
  static constexpr const char* kS3BucketPrefix = "hive.s3.bucket.";

  /// Log granularity of AWS C++ SDK.
  static constexpr const char* kS3LogLevel = "hive.s3.log-level";

  /// Payload signing policy.
  static constexpr const char* kS3PayloadSigningPolicy =
      "hive.s3.payload-signing-policy";

  /// S3FileSystem default identity.
  static constexpr const char* kDefaultS3Identity = "s3-default-identity";

  /// Log location of AWS C++ SDK.
  static constexpr const char* kS3LogLocation = "hive.s3.log-location";

  /// Keys to identify the config.
  enum class Keys {
    kBegin,
    kEndpoint = kBegin,
    kEndpointRegion,
    kAccessKey,
    kSecretKey,
    kPathStyleAccess,
    kSSLEnabled,
    kUseInstanceCredentials,
    kIamRole,
    kIamRoleSessionName,
    kConnectTimeout,
    kSocketTimeout,
    kMaxConnections,
    kMaxAttempts,
    kRetryMode,
    kUseProxyFromEnv,
    kCredentialsProvider,
    kEnd
  };

  /// Map of keys -> <suffixString, optional defaultValue>.
  /// New config must be added here along with a getter function below.
  static const std::unordered_map<
      Keys,
      std::pair<std::string_view, std::optional<std::string_view>>>&
  configTraits() {
    static const std::unordered_map<
        Keys,
        std::pair<std::string_view, std::optional<std::string_view>>>
        config = {
            {Keys::kEndpoint, std::make_pair("endpoint", std::nullopt)},
            {Keys::kEndpointRegion,
             std::make_pair("endpoint.region", std::nullopt)},
            {Keys::kAccessKey, std::make_pair("aws-access-key", std::nullopt)},
            {Keys::kSecretKey, std::make_pair("aws-secret-key", std::nullopt)},
            {Keys::kPathStyleAccess,
             std::make_pair("path-style-access", "false")},
            {Keys::kSSLEnabled, std::make_pair("ssl.enabled", "true")},
            {Keys::kUseInstanceCredentials,
             std::make_pair("use-instance-credentials", "false")},
            {Keys::kIamRole, std::make_pair("iam-role", std::nullopt)},
            {Keys::kIamRoleSessionName,
             std::make_pair("iam-role-session-name", "velox-session")},
            {Keys::kConnectTimeout,
             std::make_pair("connect-timeout", std::nullopt)},
            {Keys::kSocketTimeout,
             std::make_pair("socket-timeout", std::nullopt)},
            {Keys::kMaxConnections,
             std::make_pair("max-connections", std::nullopt)},
            {Keys::kMaxAttempts, std::make_pair("max-attempts", std::nullopt)},
            {Keys::kRetryMode, std::make_pair("retry-mode", std::nullopt)},
            {Keys::kUseProxyFromEnv,
             std::make_pair("use-proxy-from-env", "false")},
            {Keys::kCredentialsProvider,
             std::make_pair("aws-credentials-provider", std::nullopt)},
        };
    return config;
  }

  S3Config(
      std::string_view bucket,
      std::shared_ptr<const config::ConfigBase> config);

  /// cacheKey is used as a key for the S3FileSystem instance map.
  /// This will be the bucket endpoint or the base endpoint if they exist plus
  /// bucket name.
  static std::string cacheKey(
      std::string_view bucket,
      std::shared_ptr<const config::ConfigBase> config);

  /// Return the base config for the input Key.
  static std::string baseConfigKey(Keys key) {
    std::stringstream buffer;
    buffer << kS3Prefix << configTraits().find(key)->second.first;
    return buffer.str();
  }

  /// Return the bucket config for the input key.
  static std::string bucketConfigKey(Keys key, std::string_view bucket) {
    std::stringstream buffer;
    buffer << kS3BucketPrefix << bucket << "."
           << configTraits().find(key)->second.first;
    return buffer.str();
  }

  /// The S3 storage endpoint server. This can be used to connect to an
  /// S3-compatible storage system instead of AWS.
  std::optional<std::string> endpoint() const {
    return config_.find(Keys::kEndpoint)->second;
  }

  /// The S3 storage endpoint region.
  std::optional<std::string> endpointRegion() const;

  /// Access key to use.
  std::optional<std::string> accessKey() const {
    return config_.find(Keys::kAccessKey)->second;
  }

  /// Secret key to use
  std::optional<std::string> secretKey() const {
    return config_.find(Keys::kSecretKey)->second;
  }

  /// Virtual addressing is used for AWS S3 and is the default
  /// (path-style-access is false). Path access style is used for some on-prem
  /// systems like Minio.
  bool useVirtualAddressing() const {
    auto value = config_.find(Keys::kPathStyleAccess)->second.value();
    return !folly::to<bool>(value);
  }

  /// Use HTTPS to communicate with the S3 API.
  bool useSSL() const {
    auto value = config_.find(Keys::kSSLEnabled)->second.value();
    return folly::to<bool>(value);
  }

  /// Use the EC2 metadata service to retrieve API credentials.
  bool useInstanceCredentials() const {
    auto value = config_.find(Keys::kUseInstanceCredentials)->second.value();
    return folly::to<bool>(value);
  }

  /// IAM role to assume.
  std::optional<std::string> iamRole() const {
    return config_.find(Keys::kIamRole)->second;
  }

  /// Session name associated with the IAM role.
  std::string iamRoleSessionName() const {
    return config_.find(Keys::kIamRoleSessionName)->second.value();
  }

  /// Socket connect timeout.
  std::optional<std::string> connectTimeout() const {
    return config_.find(Keys::kConnectTimeout)->second;
  }

  /// Socket read timeout.
  std::optional<std::string> socketTimeout() const {
    return config_.find(Keys::kSocketTimeout)->second;
  }

  /// Maximum concurrent TCP connections for a single http client.
  std::optional<uint32_t> maxConnections() const {
    auto val = config_.find(Keys::kMaxConnections)->second;
    if (val.has_value()) {
      return folly::to<uint32_t>(val.value());
    }
    return std::optional<uint32_t>();
  }

  /// Maximum retry attempts for a single http client.
  std::optional<int32_t> maxAttempts() const {
    auto val = config_.find(Keys::kMaxAttempts)->second;
    if (val.has_value()) {
      return folly::to<int32_t>(val.value());
    }
    return std::optional<int32_t>();
  }

  /// Retry mode for a single http client.
  std::optional<std::string> retryMode() const {
    return config_.find(Keys::kRetryMode)->second;
  }

  bool useProxyFromEnv() const {
    auto value = config_.find(Keys::kUseProxyFromEnv)->second.value();
    return folly::to<bool>(value);
  }

  std::string payloadSigningPolicy() const {
    return payloadSigningPolicy_;
  }

  std::string bucket() const {
    return bucket_;
  }

  std::optional<std::string> credentialsProvider() const {
    return config_.find(Keys::kCredentialsProvider)->second;
  }

 private:
  std::unordered_map<Keys, std::optional<std::string>> config_;
  std::string payloadSigningPolicy_;
  std::string bucket_;
};

} // namespace facebook::velox::filesystems
