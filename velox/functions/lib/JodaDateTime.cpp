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

#include "velox/functions/lib/JodaDateTime.h"
#include <cctype>
#include <unordered_map>
#include "velox/common/base/Exceptions.h"
#include "velox/type/TimestampConversion.h"

namespace facebook::velox::functions {

namespace {

static constexpr size_t kJodaReserveSize{8};

inline bool characterIsDigit(char c) {
  return c >= '0' && c <= '9';
}

inline JodaFormatSpecifier getSpecifier(char c) {
  static std::unordered_map<char, JodaFormatSpecifier> specifierMap{
      {'G', JodaFormatSpecifier::ERA},
      {'C', JodaFormatSpecifier::CENTURY_OF_ERA},
      {'Y', JodaFormatSpecifier::YEAR_OF_ERA},
      {'x', JodaFormatSpecifier::WEEK_YEAR},
      {'w', JodaFormatSpecifier::WEEK_OF_WEEK_YEAR},
      {'e', JodaFormatSpecifier::DAY_OF_WEEK},
      {'E', JodaFormatSpecifier::DAY_OF_WEEK_TEXT},
      {'y', JodaFormatSpecifier::YEAR},
      {'D', JodaFormatSpecifier::DAY_OF_YEAR},
      {'M', JodaFormatSpecifier::MONTH_OF_YEAR},
      {'d', JodaFormatSpecifier::DAY_OF_MONTH},
      {'a', JodaFormatSpecifier::HALFDAY_OF_DAY},
      {'K', JodaFormatSpecifier::HOUR_OF_HALFDAY},
      {'h', JodaFormatSpecifier::CLOCK_HOUR_OF_HALFDAY},
      {'H', JodaFormatSpecifier::HOUR_OF_DAY},
      {'k', JodaFormatSpecifier::CLOCK_HOUR_OF_DAY},
      {'m', JodaFormatSpecifier::MINUTE_OF_HOUR},
      {'s', JodaFormatSpecifier::SECOND_OF_MINUTE},
      {'S', JodaFormatSpecifier::FRACTION_OF_SECOND},
      {'z', JodaFormatSpecifier::TIMEZONE},
      {'Z', JodaFormatSpecifier::TIMEZONE_OFFSET_ID},
  };

  auto it = specifierMap.find(c);
  if (it == specifierMap.end()) {
    VELOX_USER_FAIL("Illegal pattern component: '{}'", c);
  }
  return it->second;
}

} // namespace

JodaFormatter::JodaFormatter(std::string format) : format_(std::move(format)) {
  literalTokens_.reserve(kJodaReserveSize);
  patternTokens_.reserve(kJodaReserveSize);
  patternTokensCount_.reserve(kJodaReserveSize);

  if (format_.empty()) {
    VELOX_USER_FAIL("Invalid pattern specification.");
  }

  const char* cur = format_.data();
  const char* end = cur + format_.size();

  // While we don't exhaust the buffer, we always interleave reading one
  // literal token, and one pattern, even if the literal token is empty.
  //
  // - pattern tokens are consecutive runs of the same letter (a-zA-Z).
  // - literal tokens are anything else in between.
  while (cur < end) {
    // 1. Always try to read and add a literal token first, if it's empty.
    const char* literalEnd = cur;

    while ((literalEnd < end) && !std::isalpha(*literalEnd)) {
      ++literalEnd;
    }

    literalTokens_.emplace_back(cur, literalEnd - cur);
    cur = literalEnd;

    // 2. If we still have at least one char left, try to read a pattern.
    if (cur < end) {
      const char* patternEnd = cur;

      while ((patternEnd < end) && (*cur == *patternEnd)) {
        ++patternEnd;
      }

      patternTokens_.emplace_back(getSpecifier(*cur));
      patternTokensCount_.emplace_back(patternEnd - cur);
      cur = patternEnd;
    }
  }

  // The first and last tokens are always literals, even if they are empty.
  if (literalTokens_.size() == patternTokens_.size()) {
    literalTokens_.emplace_back("");
  }
  VELOX_CHECK_EQ(literalTokens_.size(), patternTokens_.size() + 1);
}

Timestamp JodaFormatter::parse(const std::string& input) {
  struct {
    int32_t year = -1;
    int32_t month = 1;
    int32_t day = 1;

    int32_t hour = 0;
    int32_t minute = 0;
    int32_t second = 0;
    int32_t microsecond = 0;
  } jodaDate;
  const char* cur = input.data();
  const char* end = cur + input.size();

  for (size_t i = 0; i < literalTokens_.size(); ++i) {
    // First ensure that the literal token matches.
    const auto& curToken = literalTokens_[i];

    if (curToken.size() > (end - cur) ||
        std::memcmp(cur, curToken.data(), curToken.size()) != 0) {
      VELOX_USER_FAIL(
          "Invalid format: \"{}\" is malformed at \"{}\"", format_, input);
    }
    cur += curToken.size();

    // If there's still a pattern to be read, read it and add to
    // `jodaDate`.
    if (i < patternTokens_.size()) {
      const auto& curPattern = patternTokens_[i];
      uint64_t number = 0;
      auto startPos = cur;

      // TODO: For now we only support numeric specifiers.
      while (cur < end && characterIsDigit(*cur)) {
        number = number * 10 + (*cur - '0');
        ++cur;
      }

      // Need to have read at least one digit.
      VELOX_USER_CHECK_GT(
          cur,
          startPos,
          "Invalid format: \"{}\" is malformed at \"{}\"",
          input,
          std::string(cur, end - cur));

      switch (curPattern) {
        case JodaFormatSpecifier::YEAR_OF_ERA:
          jodaDate.year = number;
          break;

        case JodaFormatSpecifier::MONTH_OF_YEAR:
          jodaDate.month = number;

          // Joda has this weird behavior where it returns 1970 as the year by
          // default (if no year is specified), but if either day or month are
          // specified, it fallsback to 2000.
          if (jodaDate.year == -1) {
            jodaDate.year = 2000;
          }
          break;

        case JodaFormatSpecifier::DAY_OF_MONTH:
          jodaDate.day = number;
          if (jodaDate.year == -1) {
            jodaDate.year = 2000;
          }
          break;

        case JodaFormatSpecifier::HOUR_OF_DAY:
          jodaDate.hour = number;
          break;

        case JodaFormatSpecifier::MINUTE_OF_HOUR:
          jodaDate.minute = number;
          break;

        case JodaFormatSpecifier::SECOND_OF_MINUTE:
          jodaDate.second = number;
          break;

        default:
          VELOX_NYI("Numeric Joda specifier not implemented yet.");
      }
    }
  }

  // Ensure all input was consumed.
  VELOX_USER_CHECK(
      cur == end,
      "Invalid format: \"{}\" is malformed at \"{}\"",
      input,
      std::string(cur, end - cur));

  // Date time validations for Joda compatibility. Additional date checks will
  // be performed below when converting it to timestamp.
  if (jodaDate.year == -1) {
    jodaDate.year = 1970;
  }

  // Enforce Joda's year range.
  if (jodaDate.year > 294247 || jodaDate.year < 1) {
    VELOX_USER_FAIL(
        "Value {} for yearOfEra must be in the range [1,294247]",
        jodaDate.year);
  }

  if (jodaDate.hour > 23 || jodaDate.hour < 0) {
    VELOX_USER_FAIL(
        "Value {} for hourOfDay must be in the range [0,23]", jodaDate.hour);
  }

  if (jodaDate.minute > 59 || jodaDate.minute < 0) {
    VELOX_USER_FAIL(
        "Value {} for minuteOfHour must be in the range [0,59]",
        jodaDate.minute);
  }

  if (jodaDate.second > 59 || jodaDate.second < 0) {
    VELOX_USER_FAIL(
        "Value {} for secondOfMinute must be in the range [0,59]",
        jodaDate.second);
  }

  // Convert the parsed date/time into a timestamp.
  int32_t daysSinceEpoch =
      util::fromDate(jodaDate.year, jodaDate.month, jodaDate.day);
  int64_t microsSinceMidnight = util::fromTime(
      jodaDate.hour, jodaDate.minute, jodaDate.second, jodaDate.microsecond);
  return util::fromDatetime(daysSinceEpoch, microsSinceMidnight);
}

} // namespace facebook::velox::functions
