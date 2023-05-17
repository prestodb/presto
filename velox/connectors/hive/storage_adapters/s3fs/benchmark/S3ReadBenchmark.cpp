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

#include "velox/connectors/hive/storage_adapters/s3fs/benchmark/S3ReadBenchmark.h"
#include "velox/core/Config.h"

#include <fstream>

DEFINE_string(s3_config, "", "Path of S3 config file");

namespace facebook::velox {

// From presto-cpp
std::shared_ptr<Config> readConfig(const std::string& filePath) {
  std::ifstream configFile(filePath);
  if (!configFile.is_open()) {
    throw std::runtime_error(
        fmt::format("Couldn't open config file {} for reading.", filePath));
  }

  std::unordered_map<std::string, std::string> properties;
  std::string line;
  while (getline(configFile, line)) {
    line.erase(std::remove_if(line.begin(), line.end(), isspace), line.end());
    if (line[0] == '#' || line.empty()) {
      continue;
    }
    auto delimiterPos = line.find('=');
    auto name = line.substr(0, delimiterPos);
    auto value = line.substr(delimiterPos + 1);
    properties.emplace(name, value);
  }

  return std::make_shared<facebook::velox::core::MemConfig>(properties);
}

} // namespace facebook::velox
