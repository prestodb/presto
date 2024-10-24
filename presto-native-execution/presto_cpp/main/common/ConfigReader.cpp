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
#include <fmt/format.h>
#include <fstream>
#include "velox/common/base/Exceptions.h"
#include "velox/common/config/Config.h"

namespace facebook::presto::util {

namespace {
// Replaces strings of the form "${VAR}"
// with the value of the environment variable "VAR" (if it exists).
// Does nothing if the input doesn't look like "${...}".
std::string replaceIfEnvironmentVariable(std::string_view str) {
  if (str.size() >= 3 && str.substr(0, 2) == "${" &&
      str[str.size() - 1] == '}') {
    auto env_name = std::string(str.substr(2, str.size() - 3));

    char* envval = std::getenv(env_name.c_str());
    if (envval) {
      return std::string(envval);
    }
  }
  return std::string(str);
}
} // namespace

std::unordered_map<std::string, std::string> readConfig(
    const std::string& filePath) {
  // https://teradata.github.io/presto/docs/141t/configuration/configuration.html

  std::ifstream configFile(filePath);
  if (!configFile.is_open()) {
    VELOX_USER_FAIL("Couldn't open config file {} for reading.", filePath);
  }

  std::unordered_map<std::string, std::string> properties;
  std::string line;
  while (getline(configFile, line)) {
    line.erase(std::remove_if(line.begin(), line.end(), isspace), line.end());
    if (line.empty() || line[0] == '#') {
      continue;
    }

    const auto delimiterPos = line.find('=');
    VELOX_CHECK_NE(
        delimiterPos,
        std::string::npos,
        "No '=' sign found for property pair '{}'",
        line);
    const auto name = line.substr(0, delimiterPos);
    VELOX_CHECK(!name.empty(), "property pair '{}' has empty key", line);
    const auto value = line.substr(delimiterPos + 1);
    const auto adjustedValue = replaceIfEnvironmentVariable(value);
    properties.emplace(name, adjustedValue);
  }

  return properties;
}

std::string requiredProperty(
    const std::unordered_map<std::string, std::string>& properties,
    const std::string& name) {
  auto it = properties.find(name);
  if (it == properties.end()) {
    VELOX_USER_FAIL("Missing configuration property {}", name);
  }
  return it->second;
}

std::string requiredProperty(
    const velox::config::ConfigBase& properties,
    const std::string& name) {
  auto value = properties.get<std::string>(name);
  if (!value.hasValue()) {
    VELOX_USER_FAIL("Missing configuration property {}", name);
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
    const velox::config::ConfigBase& properties,
    const std::string& name,
    const std::string& defaultValue) {
  auto value = properties.get<std::string>(name);
  if (!value.hasValue()) {
    return defaultValue;
  }
  return value.value();
}

} // namespace facebook::presto::util
