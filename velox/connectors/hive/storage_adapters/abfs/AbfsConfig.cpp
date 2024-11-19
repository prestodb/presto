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

#include "velox/connectors/hive/storage_adapters/abfs/AbfsConfig.h"

#include "velox/common/config/Config.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsUtil.h"

namespace facebook::velox::filesystems {

AbfsConfig::AbfsConfig(
    std::string_view path,
    const config::ConfigBase& config) {
  std::string_view file;
  bool isHttps = true;
  if (path.find(kAbfssScheme) == 0) {
    file = path.substr(kAbfssScheme.size());
  } else if (path.find(kAbfsScheme) == 0) {
    file = path.substr(kAbfsScheme.size());
    isHttps = false;
  } else {
    VELOX_FAIL("Invalid ABFS Path {}", path);
  }

  auto firstAt = file.find_first_of("@");
  fileSystem_ = file.substr(0, firstAt);
  auto firstSep = file.find_first_of("/");
  filePath_ = file.substr(firstSep + 1);

  auto accountNameWithSuffix = file.substr(firstAt + 1, firstSep - firstAt - 1);
  auto firstDot = accountNameWithSuffix.find_first_of(".");
  auto accountName = accountNameWithSuffix.substr(0, firstDot);
  auto endpointSuffix = accountNameWithSuffix.substr(firstDot + 5);
  auto credKey = fmt::format("fs.azure.account.key.{}", accountNameWithSuffix);
  std::stringstream ss;
  ss << "DefaultEndpointsProtocol=" << (isHttps ? "https" : "http");
  ss << ";AccountName=" << accountName;

  if (config.valueExists(credKey)) {
    ss << ";AccountKey=" << config.get<std::string>(credKey).value();
  } else {
    VELOX_USER_FAIL("Config {} not found", credKey);
  }

  ss << ";EndpointSuffix=" << endpointSuffix;

  if (config.valueExists(kAzureBlobEndpoint)) {
    ss << ";BlobEndpoint="
       << config.get<std::string>(kAzureBlobEndpoint).value();
  }
  ss << ";";
  connectionString_ = ss.str();
}

} // namespace facebook::velox::filesystems
