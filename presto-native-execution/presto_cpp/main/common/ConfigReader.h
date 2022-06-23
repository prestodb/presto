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

#pragma once
#include <functional>
#include <string>
#include <unordered_map>

namespace facebook::velox {
class Config;
}

namespace facebook::presto::util {

std::unordered_map<std::string, std::string> readConfig(
    const std::string& filePath);

std::string requiredProperty(
    const std::unordered_map<std::string, std::string>& properties,
    const std::string& name);

std::string requiredProperty(
    const velox::Config& properties,
    const std::string& name);

std::string getOptionalProperty(
    const std::unordered_map<std::string, std::string>& properties,
    const std::string& name,
    const std::function<std::string()>& func);

std::string getOptionalProperty(
    const std::unordered_map<std::string, std::string>& properties,
    const std::string& name,
    const std::string& defaultValue);

std::string getOptionalProperty(
    const velox::Config& properties,
    const std::string& name,
    const std::string& defaultValue);

} // namespace facebook::presto::util
