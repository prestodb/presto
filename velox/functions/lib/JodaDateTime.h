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
#pragma once

#include <string>
#include <string_view>
#include <vector>
#include "velox/type/Timestamp.h"

namespace facebook::velox::functions {

// Follow the Joda time library, as described in:
//  http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html
enum class JodaFormatSpecifier : uint8_t {
  // Era, e.g: "AD"
  ERA = 0,

  // Century of era (>=0), e.g: 20
  CENTURY_OF_ERA = 1,

  // Year of era (>=0), e.g: 1996
  YEAR_OF_ERA = 2,

  // Year of era (>=0), e.g: 1996
  WEEK_YEAR = 3,

  // Week of weekyear, e.g: 27
  WEEK_OF_WEEK_YEAR = 4,

  // Day of week, e.g: 2
  DAY_OF_WEEK = 5,

  // Day of week, e.g: "Tuesday" or "Tue"
  DAY_OF_WEEK_TEXT = 6,

  // Year, e.g: 1996
  YEAR = 7,

  // Day of year, e.g: 189
  DAY_OF_YEAR = 8,

  // Month of year, e.g: "July", "Jul" or 07
  MONTH_OF_YEAR = 9,

  // Day of month, e.g: 10
  DAY_OF_MONTH = 10,

  // Halfday of day, e.g: "PM"
  HALFDAY_OF_DAY = 11,

  // Hour of halfday (0~11)
  HOUR_OF_HALFDAY = 12,

  // Clockhour of halfday (1~12)
  CLOCK_HOUR_OF_HALFDAY = 13,

  // Hour of day (0~23)
  HOUR_OF_DAY = 14,

  // Clockhour of day (1~24)
  CLOCK_HOUR_OF_DAY = 15,

  // Minute of hour, e.g: 30
  MINUTE_OF_HOUR = 16,

  // Second of minute, e.g: 55
  SECOND_OF_MINUTE = 17,

  // Fraction of second, e.g: 978
  FRACTION_OF_SECOND = 18,

  // Timezone, e.g: "Pacific Standard Time" or "PST"
  TIMEZONE = 19,

  // Timezone offset/id, e.g: "-0800", "-08:00" or "America/Los_Angeles"
  TIMEZONE_OFFSET_ID = 20
};

struct JodaResult {
  Timestamp timestamp;
  int64_t timezoneId{-1};
};

/// Compiles a Joda-compatible datetime format string.
class JodaFormatter {
 public:
  explicit JodaFormatter(std::string format) : format_(std::move(format)) {
    initialize();
  }

  explicit JodaFormatter(StringView format)
      : format_(format.data(), format.size()) {
    initialize();
  }

  const std::vector<std::string_view>& literalTokens() const {
    return literalTokens_;
  }

  const std::vector<JodaFormatSpecifier>& patternTokens() const {
    return patternTokens_;
  }

  const std::vector<size_t>& patternTokensCount() const {
    return patternTokensCount_;
  }

  // Parses `input` according to the format specified in the constructor. Throws
  // in case the input couldn't be parsed.
  JodaResult parse(const std::string& input);

 private:
  void initialize();

  const std::string format_;

  // Stores the literal tokens (substrings) found while parsing `format_`.
  // There are always `patternTokens_ + 1` literal tokens, to follow the
  // format below:
  //
  //   l[0] - p[0] - l[1] - p[1] - ... - p[n] - l[n+1]
  //
  // for instance, the format string "YYYY-MM-dd" would be stored as:
  //
  //  literals: {"", "-", "-", ""}
  //  patterns: {YEAR_OF_ERA, MONTH_OF_YEAR, DAY_OF_MONTH}
  //  patternCount: {4, 2, 2}
  //
  std::vector<std::string_view> literalTokens_;
  std::vector<JodaFormatSpecifier> patternTokens_;

  // Stores the number of times each pattern token was read, e.g: "Y" (1) vs
  // "YYYY" (4).
  std::vector<size_t> patternTokensCount_;
};

} // namespace facebook::velox::functions
