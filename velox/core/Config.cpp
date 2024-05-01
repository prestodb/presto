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
#include "velox/core/Config.h"
#include "velox/core/QueryConfig.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::core {

folly::Optional<std::string> MemConfig::get(const std::string& key) const {
  folly::Optional<std::string> val;
  auto it = values_.find(key);
  if (it != values_.end()) {
    val = it->second;
  }
  return val;
}

bool MemConfig::isValueExists(const std::string& key) const {
  return values_.find(key) != values_.end();
}

folly::Optional<std::string> MemConfigMutable::get(
    const std::string& key) const {
  auto lockedValues = values_.rlock();
  folly::Optional<std::string> val;
  auto it = lockedValues->find(key);
  if (it != lockedValues->end()) {
    val = it->second;
  }
  return val;
}

bool MemConfigMutable::isValueExists(const std::string& key) const {
  auto lockedValues = values_.rlock();
  return lockedValues->find(key) != lockedValues->end();
}

void MemConfig::validateConfig() {
  // Validate if timezone name can be recognized.
  if (isValueExists(QueryConfig::kSessionTimezone)) {
    util::getTimeZoneID(values_[QueryConfig::kSessionTimezone]);
  }
}

} // namespace facebook::velox::core
