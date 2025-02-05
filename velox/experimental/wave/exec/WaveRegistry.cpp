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

#include "velox/experimental/wave/exec/ToWave.h"

namespace facebook::velox::wave {

FunctionMetadata WaveRegistry::metadata(const FunctionKey& key) const {
  auto it = data_.find(key);
  VELOX_CHECK(it != data_.end());
  return it->second.metadata;
}

void WaveRegistry::registerFunction(
    const FunctionKey& key,
    FunctionMetadata& metadata,
    const std::string& includeLine,
    const std::string& text) {
  FunctionEntry entry{
      .metadata = metadata, .includeLine = includeLine, .text = text};
  data_[key] = std::move(entry);
}

std::string
replaceAll(std::string str, const std::string& from, const std::string& to) {
  size_t start_pos = 0;
  while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
    str.replace(start_pos, from.length(), to);
    start_pos += to.length();
  }
  return str;
}

FunctionDefinition WaveRegistry::makeDefinition(
    const FunctionKey& key,
    const TypePtr returnType) const {
  auto it = data_.find(key);
  if (it == data_.end()) {
    std::stringstream signature;
    for (auto& arg : key.types) {
      signature << arg->toString() << " ";
    }
    VELOX_FAIL("No definition in Wave for {}({})", key.name, signature.str());
  }
  auto& entry = it->second;
  auto replaced = entry.text;
  replaced = replaceAll(replaced, "$R$", cudaTypeName(*returnType));
  for (auto i = 0; i < key.types.size(); ++i) {
    replaced = replaceAll(
        replaced, fmt::format("${}$", i + 1), cudaTypeName(*key.types[i]));
  }
  return {replaced, entry.includeLine};
}
bool WaveRegistry::registerMessage(int32_t key, std::string message) {
  auto previous = messages_[key];
  if (!previous.empty() && previous != message) {
    VELOX_FAIL("Duplicate message registration for key {}", key);
  }
  messages_[key] = std::move(message);
  return true;
}

std::string WaveRegistry::message(int32_t key) {
  auto it = messages_.find(key);
  if (it == messages_.end()) {
    return fmt::format("No message for code {}", key);
  }
  return it->second;
}

} // namespace facebook::velox::wave
