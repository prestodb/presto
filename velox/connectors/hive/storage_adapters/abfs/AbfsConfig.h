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

#include <folly/hash/Hash.h>
#include <string>

namespace facebook::velox::config {
class ConfigBase;
}

namespace facebook::velox::filesystems {

// This is used to specify the Azurite endpoint in testing.
static std::string kAzureBlobEndpoint{"fs.azure.blob-endpoint"};

class AbfsConfig {
 public:
  explicit AbfsConfig(std::string_view path, const config::ConfigBase& config);

  std::string identity() const {
    const auto hash = folly::Hash();
    return std::to_string(hash(connectionString_));
  }

  std::string connectionString() const {
    return connectionString_;
  }

  std::string fileSystem() const {
    return fileSystem_;
  }

  std::string filePath() const {
    return filePath_;
  }

 private:
  // Container name is called FileSystem in some Azure API.
  std::string fileSystem_;
  std::string filePath_;
  std::string connectionString_;
};

} // namespace facebook::velox::filesystems
