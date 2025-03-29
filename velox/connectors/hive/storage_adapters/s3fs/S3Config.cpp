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
#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"

namespace facebook::velox::filesystems {

std::string S3Config::cacheKey(
    std::string_view bucket,
    std::shared_ptr<const config::ConfigBase> config) {
  auto bucketEndpoint = bucketConfigKey(Keys::kEndpoint, bucket);
  if (config->valueExists(bucketEndpoint)) {
    return fmt::format(
        "{}-{}", config->get<std::string>(bucketEndpoint).value(), bucket);
  }
  auto baseEndpoint = baseConfigKey(Keys::kEndpoint);
  if (config->valueExists(baseEndpoint)) {
    return fmt::format(
        "{}-{}", config->get<std::string>(baseEndpoint).value(), bucket);
  }
  return std::string(bucket);
}

S3Config::S3Config(
    std::string_view bucket,
    const std::shared_ptr<const config::ConfigBase> properties)
    : bucket_(bucket) {
  for (int key = static_cast<int>(Keys::kBegin);
       key < static_cast<int>(Keys::kEnd);
       key++) {
    auto s3Key = static_cast<Keys>(key);
    auto value = S3Config::configTraits().find(s3Key)->second;
    auto configSuffix = value.first;
    auto configDefault = value.second;

    // Set bucket S3 config "hive.s3.bucket.*" if present.
    std::stringstream bucketConfig;
    bucketConfig << kS3BucketPrefix << bucket << "." << configSuffix;
    auto configVal = static_cast<std::optional<std::string>>(
        properties->get<std::string>(bucketConfig.str()));
    if (configVal.has_value()) {
      config_[s3Key] = configVal.value();
    } else {
      // Set base config "hive.s3.*" if present.
      std::stringstream baseConfig;
      baseConfig << kS3Prefix << configSuffix;
      configVal = static_cast<std::optional<std::string>>(
          properties->get<std::string>(baseConfig.str()));
      if (configVal.has_value()) {
        config_[s3Key] = configVal.value();
      } else {
        // Set the default value.
        config_[s3Key] = configDefault;
      }
    }
  }
  payloadSigningPolicy_ =
      properties->get<std::string>(kS3PayloadSigningPolicy, "Never");
}

std::optional<std::string> S3Config::endpointRegion() const {
  auto region = config_.find(Keys::kEndpointRegion)->second;
  if (!region.has_value()) {
    // If region is not set, try inferring from the endpoint value for AWS
    // endpoints.
    auto endpointValue = endpoint();
    if (endpointValue.has_value()) {
      region = parseAWSStandardRegionName(endpointValue.value());
    }
  }
  return region;
}

} // namespace facebook::velox::filesystems
