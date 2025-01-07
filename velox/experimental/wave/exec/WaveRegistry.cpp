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

} // namespace facebook::velox::wave
