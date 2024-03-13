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
  return util::fromTimestampString(view.data(), view.size());
}

int32_t PrestoCastHooks::castStringToDate(const StringView& dateString) const {
  // Cast from string to date allows only ISO 8601 formatted strings:
  // [+-](YYYY-MM-DD).
  return util::castFromDateString(dateString, true /*isIso8601*/);
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
