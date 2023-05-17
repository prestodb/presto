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

#include <folly/Optional.h>
#include <string>

namespace facebook::velox {
class Config;
}

namespace facebook::velox::connector::hive {

/// Hive connector configs.
class HiveConfig {
 public:
  enum class InsertExistingPartitionsBehavior {
    kError,
    kOverwrite,
  };

  /// Behavior on insert into existing partitions.
  static constexpr const char* kInsertExistingPartitionsBehavior =
      "insert_existing_partitions_behavior";

  /// Maximum number of partitions per a single table writer instance.
  static constexpr const char* kMaxPartitionsPerWriters =
      "max_partitions_per_writers";

  /// Whether new data can be inserted into an unpartition table.
  /// Velox currently does not support appending data to existing partitions.
  static constexpr const char* kImmutablePartitions = "immutable_partitions";

  /// Virtual addressing is used for AWS S3 and is the default
  /// (path-style-access is false). Path access style is used for some on-prem
  /// systems like Minio.
  static constexpr const char* kS3PathStyleAccess = "hive.s3.path-style-access";

  /// Log granularity of AWS C++ SDK.
  static constexpr const char* kS3LogLevel = "hive.s3.log-level";

  /// Use HTTPS to communicate with the S3 API.
  static constexpr const char* kS3SSLEnabled = "hive.s3.ssl.enabled";

  /// Use the EC2 metadata service to retrieve API credentials.
  static constexpr const char* kS3UseInstanceCredentials =
      "hive.s3.use-instance-credentials";

  /// The S3 storage endpoint server. This can be used to connect to an
  /// S3-compatible storage system instead of AWS.
  static constexpr const char* kS3Endpoint = "hive.s3.endpoint";

  /// Default AWS access key to use.
  static constexpr const char* kS3AwsAccessKey = "hive.s3.aws-access-key";

  /// Default AWS secret key to use.
  static constexpr const char* kS3AwsSecretKey = "hive.s3.aws-secret-key";

  /// IAM role to assume.
  static constexpr const char* kS3IamRole = "hive.s3.iam-role";

  /// Session name associated with the IAM role.
  static constexpr const char* kS3IamRoleSessionName =
      "hive.s3.iam-role-session-name";

  static InsertExistingPartitionsBehavior insertExistingPartitionsBehavior(
      const Config* config);

  static uint32_t maxPartitionsPerWriters(const Config* config);

  static bool immutablePartitions(const Config* config);

  static bool s3UseVirtualAddressing(const Config* config);

  static std::string s3GetLogLevel(const Config* config);

  static bool s3UseSSL(const Config* config);

  static bool s3UseInstanceCredentials(const Config* config);

  static std::string s3Endpoint(const Config* config);

  static std::optional<std::string> s3AccessKey(const Config* config);

  static std::optional<std::string> s3SecretKey(const Config* config);

  static std::optional<std::string> s3IAMRole(const Config* config);

  static std::string s3IAMRoleSessionName(const Config* config);
};

} // namespace facebook::velox::connector::hive
