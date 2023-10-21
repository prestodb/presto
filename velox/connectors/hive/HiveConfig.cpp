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

#include "velox/connectors/hive/HiveConfig.h"
#include "velox/core/Config.h"
#include "velox/core/QueryConfig.h"

#include <boost/algorithm/string.hpp>

namespace facebook::velox::connector::hive {

namespace {

HiveConfig::InsertExistingPartitionsBehavior
stringToInsertExistingPartitionsBehavior(const std::string& strValue) {
  auto upperValue = boost::algorithm::to_upper_copy(strValue);
  if (upperValue == "ERROR") {
    return HiveConfig::InsertExistingPartitionsBehavior::kError;
  }
  if (upperValue == "OVERWRITE") {
    return HiveConfig::InsertExistingPartitionsBehavior::kOverwrite;
  }
  VELOX_UNSUPPORTED(
      "Unsupported insert existing partitions behavior: {}.", strValue);
}

} // namespace

// static
HiveConfig::InsertExistingPartitionsBehavior
HiveConfig::insertExistingPartitionsBehavior(const Config* config) {
  const auto behavior =
      config->get<std::string>(kInsertExistingPartitionsBehavior);
  return behavior.has_value()
      ? stringToInsertExistingPartitionsBehavior(behavior.value())
      : InsertExistingPartitionsBehavior::kError;
}

// static
std::string HiveConfig::insertExistingPartitionsBehaviorString(
    InsertExistingPartitionsBehavior behavior) {
  switch (behavior) {
    case InsertExistingPartitionsBehavior::kError:
      return "ERROR";
    case InsertExistingPartitionsBehavior::kOverwrite:
      return "OVERWRITE";
    default:
      return fmt::format("UNKNOWN BEHAVIOR {}", static_cast<int>(behavior));
  }
}

// static
uint32_t HiveConfig::maxPartitionsPerWriters(const Config* config) {
  return config->get<uint32_t>(kMaxPartitionsPerWriters, 100);
}

// static
bool HiveConfig::immutablePartitions(const Config* config) {
  return config->get<bool>(kImmutablePartitions, false);
}

// static
bool HiveConfig::s3UseVirtualAddressing(const Config* config) {
  return !config->get(kS3PathStyleAccess, false);
}

// static
std::string HiveConfig::s3GetLogLevel(const Config* config) {
  return config->get(kS3LogLevel, std::string("FATAL"));
}

// static
bool HiveConfig::s3UseSSL(const Config* config) {
  return config->get(kS3SSLEnabled, true);
}

// static
bool HiveConfig::s3UseInstanceCredentials(const Config* config) {
  return config->get(kS3UseInstanceCredentials, false);
}

// static
std::string HiveConfig::s3Endpoint(const Config* config) {
  return config->get(kS3Endpoint, std::string(""));
}

// static
std::optional<std::string> HiveConfig::s3AccessKey(const Config* config) {
  if (config->isValueExists(kS3AwsAccessKey)) {
    return config->get(kS3AwsAccessKey).value();
  }
  return {};
}

// static
std::optional<std::string> HiveConfig::s3SecretKey(const Config* config) {
  if (config->isValueExists(kS3AwsSecretKey)) {
    return config->get(kS3AwsSecretKey).value();
  }
  return {};
}

// static
std::optional<std::string> HiveConfig::s3IAMRole(const Config* config) {
  if (config->isValueExists(kS3IamRole)) {
    return config->get(kS3IamRole).value();
  }
  return {};
}

// static
std::string HiveConfig::s3IAMRoleSessionName(const Config* config) {
  return config->get(kS3IamRoleSessionName, std::string("velox-session"));
}

// static
std::string HiveConfig::gcsEndpoint(const Config* config) {
  return config->get<std::string>(kGCSEndpoint, std::string(""));
}

// static
std::string HiveConfig::gcsScheme(const Config* config) {
  return config->get<std::string>(kGCSScheme, std::string("https"));
}

// static
std::string HiveConfig::gcsCredentials(const Config* config) {
  return config->get<std::string>(kGCSCredentials, std::string(""));
}

// static.
bool HiveConfig::isOrcUseColumnNames(const Config* config) {
  return config->get<bool>(kOrcUseColumnNames, false);
}

// static.
bool HiveConfig::isFileColumnNamesReadAsLowerCase(const Config* config) {
  return config->get<bool>(kFileColumnNamesReadAsLowerCase, false);
}

// static.
int64_t HiveConfig::maxCoalescedBytes(const Config* config) {
  return config->get<int64_t>(kMaxCoalescedBytes, 128 << 20);
}

// static.
int32_t HiveConfig::maxCoalescedDistanceBytes(const Config* config) {
  return config->get<int32_t>(kMaxCoalescedDistanceBytes, 512 << 10);
}

// static.
int32_t HiveConfig::numCacheFileHandles(const Config* config) {
  return config->get<int32_t>(kNumCacheFileHandles, 20'000);
}

uint64_t HiveConfig::fileWriterFlushThresholdBytes(const Config* config) {
  return config->get<int32_t>(kFileWriterFlushThresholdBytes, 96L << 20);
}

uint64_t HiveConfig::getOrcWriterMaxStripeSize(
    const Config* connectorQueryCtxConfig,
    const Config* connectorPropertiesConfig) {
  if (connectorQueryCtxConfig != nullptr &&
      connectorQueryCtxConfig->isValueExists(kOrcWriterMaxStripeSize)) {
    return toCapacity(
        connectorQueryCtxConfig->get<std::string>(kOrcWriterMaxStripeSize)
            .value(),
        core::CapacityUnit::BYTE);
  }
  if (connectorPropertiesConfig != nullptr &&
      connectorPropertiesConfig->isValueExists(kOrcWriterMaxStripeSizeConfig)) {
    return toCapacity(
        connectorPropertiesConfig
            ->get<std::string>(kOrcWriterMaxStripeSizeConfig)
            .value(),
        core::CapacityUnit::BYTE);
  }
  return 64L * 1024L * 1024L;
}

uint64_t HiveConfig::getOrcWriterMaxDictionaryMemory(
    const Config* connectorQueryCtxConfig,
    const Config* connectorPropertiesConfig) {
  if (connectorQueryCtxConfig != nullptr &&
      connectorQueryCtxConfig->isValueExists(kOrcWriterMaxDictionaryMemory)) {
    return toCapacity(
        connectorQueryCtxConfig->get<std::string>(kOrcWriterMaxDictionaryMemory)
            .value(),
        core::CapacityUnit::BYTE);
  }
  if (connectorPropertiesConfig != nullptr &&
      connectorPropertiesConfig->isValueExists(
          kOrcWriterMaxDictionaryMemoryConfig)) {
    return toCapacity(
        connectorPropertiesConfig
            ->get<std::string>(kOrcWriterMaxDictionaryMemoryConfig)
            .value(),
        core::CapacityUnit::BYTE);
  }
  return 16L * 1024L * 1024L;
}

} // namespace facebook::velox::connector::hive
