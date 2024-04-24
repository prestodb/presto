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

#include "velox/expression/PrestoCastHooks.h"
#include "velox/external/date/tz.h"
#include "velox/type/TimestampConversion.h"

namespace facebook::velox::exec {

PrestoCastHooks::PrestoCastHooks(const core::QueryConfig& config)
    : CastHooks(), legacyCast_(config.isLegacyCast()) {
  if (!legacyCast_) {
    options_.zeroPaddingYear = true;
    options_.dateTimeSeparator = ' ';
    const auto sessionTzName = config.sessionTimezone();
    if (config.adjustTimestampToTimezone() && !sessionTzName.empty()) {
      options_.timeZone = date::locate_zone(sessionTzName);
    }
  }
}

Timestamp PrestoCastHooks::castStringToTimestamp(const StringView& view) const {
  auto result = util::fromTimestampWithTimezoneString(view.data(), view.size());

  // If the parsed string has timezone information, convert the timestamp at
  // GMT at that time. For example, "1970-01-01 00:00:00 -00:01" is 60 seconds
  // at GMT.
  if (result.second != -1) {
    result.first.toGMT(result.second);

  }
  // If no timezone information is available in the input string, check if we
  // should understand it as being at the session timezone, and if so, convert
  // to GMT.
  else if (options_.timeZone != nullptr) {
    result.first.toGMT(*options_.timeZone);
  }
  return result.first;
}

int32_t PrestoCastHooks::castStringToDate(const StringView& dateString) const {
  // Cast from string to date allows only complete ISO 8601 formatted strings:
  // [+-](YYYY-MM-DD).
  return util::castFromDateString(dateString, util::ParseMode::kStandardCast);
}

bool PrestoCastHooks::legacy() const {
  return legacyCast_;
}

StringView PrestoCastHooks::removeWhiteSpaces(const StringView& view) const {
  return view;
}

const TimestampToStringOptions& PrestoCastHooks::timestampToStringOptions()
    const {
  return options_;
}

bool PrestoCastHooks::truncate() const {
  return false;
}
} // namespace facebook::velox::exec
