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

#include "velox/type/tz/TimeZoneMap.h"
#include <fmt/core.h>
#include <unordered_map>

namespace facebook::velox::util {

// Defined on TimeZoneDatabase.cpp
extern const std::unordered_map<int64_t, std::string>& getTimeZoneDB();

std::string getTimeZoneName(int64_t timeZoneID) {
  const auto& tzDB = getTimeZoneDB();
  auto it = tzDB.find(timeZoneID);
  if (it == tzDB.end()) {
    throw std::runtime_error(
        fmt::format("Unable to resolve timeZoneID '{}'.", timeZoneID));
  }
  return it->second;
}

} // namespace facebook::velox::util
