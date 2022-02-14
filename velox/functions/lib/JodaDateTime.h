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

struct JodaPattern {
  JodaFormatSpecifier specifier;
  size_t count;
};

struct JodaToken {
  enum class Type { kPattern, kLiteral } type;
  union {
    JodaPattern pattern;
    std::string_view literal;
  };

  explicit JodaToken(const JodaPattern& pattern)
      : type(Type::kPattern), pattern(pattern) {}

  explicit JodaToken(const std::string_view& literal)
      : type(Type::kLiteral), literal(literal) {}
};

struct JodaResult {
  Timestamp timestamp;
  int64_t timezoneId{-1};
};

/// Compiles a Joda-compatible datetime format string.
class JodaFormatter {
 public:
  explicit JodaFormatter(const char* data) {
    tokenize(data);
  }

  explicit JodaFormatter(const std::string& format) {
    tokenize(format);
  }

  explicit JodaFormatter(StringView format) {
    tokenize(std::string_view(format));
  }

  const std::vector<JodaToken>& tokens() const {
    return tokens_;
  }

  // Parses `input` according to the format specified in the constructor. Throws
  // in case the input couldn't be parsed.
  JodaResult parse(const std::string& input);

 private:
  void tokenize(const std::string_view&);

  std::string literals_;
  std::vector<JodaToken> tokens_;
};

} // namespace facebook::velox::functions
