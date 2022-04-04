/*
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

#include "presto_cpp/main/common/ConfigReader.h"
#include <fstream>
#include "velox/core/Context.h"

namespace facebook::presto::util {

std::unordered_map<std::string, std::string> readConfig(
    const std::string& filePath) {
  // https://teradata.github.io/presto/docs/141t/configuration/configuration.html

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

  return properties;
}

std::string requiredProperty(
    const std::unordered_map<std::string, std::string>& properties,
    const std::string& name) {
  auto it = properties.find(name);
  if (it == properties.end()) {
    throw std::runtime_error(
        std::string("Missing configuration property ") + name);
  }
  return it->second;
}

std::string requiredProperty(
    const velox::Config& properties,
    const std::string& name) {
  auto value = properties.get(name);
  if (!value.hasValue()) {
    throw std::runtime_error(
        std::string("Missing configuration property ") + name);
  }
  return value.value();
}

std::string getOptionalProperty(
    const std::unordered_map<std::string, std::string>& properties,
    const std::string& name,
    const std::function<std::string()>& func) {
  auto it = properties.find(name);
  if (it == properties.end()) {
    return func();
  }
  return it->second;
}

std::string getOptionalProperty(
    const std::unordered_map<std::string, std::string>& properties,
    const std::string& name,
    const std::string& defaultValue) {
  auto it = properties.find(name);
  if (it == properties.end()) {
    return defaultValue;
  }
  return it->second;
}

std::string getOptionalProperty(
    const velox::Config& properties,
    const std::string& name,
    const std::string& defaultValue) {
  auto value = properties.get(name);
  if (!value.hasValue()) {
    return defaultValue;
  }
  return value.value();
}

} // namespace facebook::presto::util
