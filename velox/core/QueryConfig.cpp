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
#include "velox/core/QueryConfig.h"

namespace facebook::velox::core {

QueryConfig::QueryConfig(
    const std::unordered_map<std::string, std::string>& values)
    : config_{std::make_unique<MemConfig>(values)} {}

QueryConfig::QueryConfig(std::unordered_map<std::string, std::string>&& values)
    : config_{std::make_unique<MemConfig>(std::move(values))} {}

void QueryConfig::testingOverrideConfigUnsafe(
    std::unordered_map<std::string, std::string>&& values) {
  config_ = std::make_unique<MemConfig>(std::move(values));
}

} // namespace facebook::velox::core
