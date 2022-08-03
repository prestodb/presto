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
#include <string>
#include <unordered_map>
#include "velox/common/base/Exceptions.h"
#include "velox/type/TimestampConversion.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions {

static thread_local std::string timezoneBuffer = "+00:00";
static const char* defaultTrailingOffset = "00";

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

// According to JodaFormatSpecifier enum class
std::string_view getSpecifierName(JodaFormatSpecifier spec) {
  static std::unordered_map<JodaFormatSpecifier, std::string_view>
      specifierNameMap{
          {JodaFormatSpecifier::ERA, "ERA"},
          {JodaFormatSpecifier::CENTURY_OF_ERA, "CENTURY_OF_ERA"},
          {JodaFormatSpecifier::YEAR_OF_ERA, "YEAR_OF_ERA"},
          {JodaFormatSpecifier::WEEK_YEAR, "WEEK_YEAR"},
          {JodaFormatSpecifier::WEEK_OF_WEEK_YEAR, "WEEK_OF_WEEK_YEAR"},
          {JodaFormatSpecifier::DAY_OF_WEEK, "DAY_OF_WEEK"},
          {JodaFormatSpecifier::DAY_OF_WEEK_TEXT, "DAY_OF_WEEK_TEXT"},
          {JodaFormatSpecifier::YEAR, "YEAR"},
          {JodaFormatSpecifier::DAY_OF_YEAR, "DAY_OF_YEAR"},
          {JodaFormatSpecifier::MONTH_OF_YEAR, "MONTH_OF_YEAR"},
          {JodaFormatSpecifier::DAY_OF_MONTH, "DAY_OF_MONTH"},
          {JodaFormatSpecifier::HALFDAY_OF_DAY, "HALFDAY_OF_DAY"},
          {JodaFormatSpecifier::HOUR_OF_HALFDAY, "HOUR_OF_HALFDAY"},
          {JodaFormatSpecifier::CLOCK_HOUR_OF_HALFDAY, "CLOCK_HOUR_OF_HALFDAY"},
          {JodaFormatSpecifier::HOUR_OF_DAY, "HOUR_OF_DAY"},
          {JodaFormatSpecifier::CLOCK_HOUR_OF_DAY, "CLOCK_HOUR_OF_DAY"},
          {JodaFormatSpecifier::MINUTE_OF_HOUR, "MINUTE_OF_HOUR"},
          {JodaFormatSpecifier::SECOND_OF_MINUTE, "SECOND_OF_MINUTE"},
          {JodaFormatSpecifier::FRACTION_OF_SECOND, "FRACTION_OF_SECOND"},
          {JodaFormatSpecifier::TIMEZONE, "TIMEZONE"},
          {JodaFormatSpecifier::TIMEZONE_OFFSET_ID, "TIMEZONE_OFFSET_ID"},
      };

  auto it = specifierNameMap.find(spec);
  return it != specifierNameMap.end() ? it->second
                                      : std::string_view("[UNKNOWN]");
}

bool specAllowsNegative(JodaFormatSpecifier s) {
  switch (s) {
    case JodaFormatSpecifier::YEAR:
      return true;
    default:
      return false;
  }
}

std::string_view suffix(const std::string& s, int len) {
  return std::string_view(&s[s.size() - len], len);
}

void addLiteralToken(
    const std::string_view& token,
    std::string& literals,
    std::vector<JodaToken>& tokens) {
  literals += token;
  if (!tokens.empty() && tokens.back().type == JodaToken::Type::kLiteral) {
    // Extend the previous literal string view.
    auto& prev = tokens.back().literal;
    tokens.back().literal =
        std::string_view(prev.data(), prev.size() + token.size());
  } else {
    tokens.emplace_back(suffix(literals, token.size()));
  }
}

} // namespace

void JodaFormatter::tokenize(const std::string_view& format) {
  literals_.reserve(format.size());
  tokens_.reserve(kJodaReserveSize);

  if (format.empty()) {
    VELOX_USER_FAIL("Invalid pattern specification.");
  }

  const char* cur = format.data();
  const char* end = cur + format.size();

  while (cur < end) {
    auto tokenEnd = cur;
    if (std::isalpha(*tokenEnd)) { // pattern
      while (tokenEnd < end && *tokenEnd == *cur) {
        ++tokenEnd;
      }
      auto specifier = getSpecifier(*cur);
      tokens_.emplace_back(
          JodaPattern{specifier, static_cast<size_t>(tokenEnd - cur)});
    } else if (*tokenEnd == '\'') { // quoted/escaped literal
      ++tokenEnd;
      while (tokenEnd < end && *tokenEnd != '\'') {
        ++tokenEnd;
      }
      if (tokenEnd == end) {
        VELOX_USER_FAIL("Unmatched single quote in pattern");
      }
      ++tokenEnd; // skip ending quote
      if (tokenEnd - cur == 2) { // escaped quote
        addLiteralToken("'", literals_, tokens_);
      } else { // quoted literal
        addLiteralToken(
            std::string_view(cur + 1, tokenEnd - cur - 2), literals_, tokens_);
      }
    } else { // unquoted literal
      while (tokenEnd < end && !isalpha(*tokenEnd) && *tokenEnd != '\'') {
        ++tokenEnd;
      }
      addLiteralToken(
          std::string_view(cur, tokenEnd - cur), literals_, tokens_);
    }
    cur = tokenEnd;
  }
}

namespace {

struct JodaDate {
 private:
  int32_t year_ = 1970;
  int32_t month_ = 1;
  int32_t day_ = 1;

  bool hasYear_ = false; // Whether year was explicitly specified.

 public:
  void setYear(int32_t year) {
    hasYear_ = true;
    year_ = year;
  }

  int32_t getYear() const {
    return year_;
  }

  void setMonth(int32_t month) {
    // Joda has this weird behavior where it returns 1970 as the year by
    // default (if no year is specified), but if either day or month are
    // specified, it falls back to 2000.
    if (!hasYear_) {
      hasYear_ = true;
      year_ = 2000;
    }
    month_ = month;
  }

  int32_t getMonth() const {
    return month_;
  }

  void setDay(int32_t day) {
    if (!hasYear_) {
      hasYear_ = true;
      year_ = 2000;
    }
    day_ = day;
  }

  int32_t getDay() const {
    return day_;
  }

  bool isYearOfEra = false; // Year of era cannot be zero or negative.

  int32_t hour = 0;
  int32_t minute = 0;
  int32_t second = 0;
  int32_t microsecond = 0;
  int64_t timezoneId = -1;
};

void parseFail(const std::string& input, const char* cur, const char* end) {
  VELOX_USER_FAIL(
      "Invalid format: \"{}\" is malformed at \"{}\"",
      input,
      std::string_view(cur, end - cur));
}

// Returns number of characters consumed, or calls parseFail() if timezone
// offset id could not be parsed.
int64_t parseTimezoneOffset(
    const std::string& input,
    const char* cur,
    const char* end,
    JodaDate& jodaDate) {
  // For timezone offset ids, there are four formats allowed by Joda:
  //
  // 1. '+' or '-' followed by two digits: "+00"
  // 2. '+' or '-' followed by two digits, ":", then two more digits:
  //    "+00:00"
  // 3. '+' or '-' followed by four digits:
  //    "+0000"
  // 4. 'Z': means GMT
  try {
    if (cur < end) {
      if ((*cur == '-' || *cur == '+')) {
        // Long format: "+00:00"
        if ((end - cur) >= 6 && *(cur + 3) == ':') {
          // Fast path for the common case ("+00:00" or "-00:00"), to prevent
          // calling getTimeZoneID(), which does a map lookup.
          if (std::strncmp(cur + 1, "00:00", 5) == 0) {
            jodaDate.timezoneId = 0;
          } else {
            jodaDate.timezoneId = util::getTimeZoneID(std::string_view(cur, 6));
          }
          return 6;
        }
        // Long format without colon: "+0000"
        else if ((end - cur) >= 5 && *(cur + 3) != ':') {
          // Same fast path described above.
          if (std::strncmp(cur + 1, "0000", 4) == 0) {
            jodaDate.timezoneId = 0;
          } else {
            // We need to concatenate the 3 first chars with ":" followed by the
            // last 2 chars before calling getTimeZoneID, so we use a static
            // thread_local buffer to prevent extra allocations.
            std::memcpy(&timezoneBuffer[0], cur, 3);
            std::memcpy(&timezoneBuffer[4], cur + 3, 2);

            jodaDate.timezoneId = util::getTimeZoneID(timezoneBuffer);
          }
          return 5;
        }
        // Short format: "+00"
        else if ((end - cur) >= 3) {
          // Same fast path described above.
          if (std::strncmp(cur + 1, "00", 2) == 0) {
            jodaDate.timezoneId = 0;
          } else {
            // We need to concatenate the 3 first chars with a trailing ":00"
            // before calling getTimeZoneID, so we use a static thread_local
            // buffer to prevent extra allocations.
            std::memcpy(&timezoneBuffer[0], cur, 3);
            std::memcpy(&timezoneBuffer[4], defaultTrailingOffset, 2);
            jodaDate.timezoneId = util::getTimeZoneID(timezoneBuffer);
          }
          return 3;
        }
      }
      // A single 'Z' means GMT.
      else if (*cur == 'Z') {
        jodaDate.timezoneId = 0;
        return 1;
      }
    }
  } catch (const std::exception&) {
  }
  parseFail(input, cur, end);
  return 0; // non-reachable.
}

int64_t parseMonthText(
    const std::string& input,
    const char* cur,
    const char* end,
    JodaDate& jodaDate) {
  // Ensure there are at least 3 characters left.
  if (cur < end && (end - cur >= 3)) {
    // For month strings, Joda supports either a 3 letter prefix ("Jan") or the
    // full month name ("January").
    //
    // In addition, it supports three variations: (a) capitalized ("Jan"), (b)
    // upper case ("JAN"), or (c) lower case ("jan"). Hard coding all these
    // variation so we don't need to copy and lower-case each input string.
    //
    // The table also splits prefix and suffix so we can quickly check if the
    // prefix matches, and optionally check if the suffix also does.
    static std::
        unordered_map<std::string_view, std::pair<std::string_view, int64_t>>
            monthMap{
                // Capitalized.
                {"Jan", {"uary", 1}},
                {"Feb", {"ruary", 2}},
                {"Mar", {"rch", 3}},
                {"Apr", {"il", 4}},
                {"May", {"", 5}},
                {"Jun", {"e", 6}},
                {"Jul", {"y", 7}},
                {"Aug", {"ust", 8}},
                {"Sep", {"tember", 9}},
                {"Oct", {"ober", 10}},
                {"Nov", {"ember", 11}},
                {"Dec", {"ember", 12}},

                // Lower case.
                {"jan", {"uary", 1}},
                {"feb", {"ruary", 2}},
                {"mar", {"rch", 3}},
                {"apr", {"il", 4}},
                {"may", {"", 5}},
                {"jun", {"e", 6}},
                {"jul", {"y", 7}},
                {"aug", {"ust", 8}},
                {"sep", {"tember", 9}},
                {"oct", {"ober", 10}},
                {"nov", {"ember", 11}},
                {"dec", {"ember", 12}},

                // Upper case.
                {"JAN", {"UARY", 1}},
                {"FEB", {"RUARY", 2}},
                {"MAR", {"RCH", 3}},
                {"APR", {"IL", 4}},
                {"MAY", {"", 5}},
                {"JUN", {"E", 6}},
                {"JUL", {"Y", 7}},
                {"AUG", {"UST", 8}},
                {"SEP", {"TEMBER", 9}},
                {"OCT", {"OBER", 10}},
                {"NOV", {"EMBER", 11}},
                {"DEC", {"EMBER", 12}},
            };

    // First check is there's a prefix match.
    auto it = monthMap.find(std::string_view(cur, 3));
    if (it != monthMap.end()) {
      jodaDate.setMonth(it->second.second);

      // If there was a match in the first 3 character prefix, we check if we
      // can consume the suffix.
      if (end - cur >= it->second.first.size() + 3) {
        if (std::strncmp(
                cur + 3, it->second.first.data(), it->second.first.size()) ==
            0) {
          return it->second.first.size() + 3;
        }
      }
      // If the suffix didn't match, still ok. Return a prefix match.
      return 3;
    }
  }
  parseFail(input, cur, end);
  return 0; // non-reachable.
}

void parseFromPattern(
    const JodaPattern& pattern,
    const std::string& input,
    const char*& cur,
    const char* end,
    JodaDate& jodaDate) {
  if (pattern.specifier == JodaFormatSpecifier::TIMEZONE_OFFSET_ID) {
    cur += parseTimezoneOffset(input, cur, end, jodaDate);
  }
  // Following Joda format, for 3 or more 'M's, use text, otherwise use number.
  else if (
      (pattern.specifier == JodaFormatSpecifier::MONTH_OF_YEAR) &&
      (pattern.count > 2)) {
    cur += parseMonthText(input, cur, end, jodaDate);
  }
  // All other numeric patterns.
  else {
    int64_t number = 0;
    bool negative = false;

    if (cur < end && specAllowsNegative(pattern.specifier) && *cur == '-') {
      negative = true;
      ++cur;
    }

    auto startPos = cur;

    while (cur < end && characterIsDigit(*cur)) {
      number = number * 10 + (*cur - '0');
      ++cur;
    }

    // Need to have read at least one digit.
    if (cur <= startPos) {
      parseFail(input, cur, end);
    }

    if (negative) {
      number *= -1L;
    }

    switch (pattern.specifier) {
      case JodaFormatSpecifier::YEAR:
      case JodaFormatSpecifier::YEAR_OF_ERA:
        jodaDate.isYearOfEra =
            (pattern.specifier == JodaFormatSpecifier::YEAR_OF_ERA);
        jodaDate.setYear(number);
        break;

      case JodaFormatSpecifier::MONTH_OF_YEAR:
        jodaDate.setMonth(number);
        break;

      case JodaFormatSpecifier::DAY_OF_MONTH:
        jodaDate.setDay(number);
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

      case JodaFormatSpecifier::FRACTION_OF_SECOND:
        jodaDate.microsecond = number * util::kMicrosPerMsec;
        break;

      default:
        VELOX_NYI(
            "Numeric Joda specifier JodaFormatSpecifier::{} "
            "not implemented yet.",
            getSpecifierName(pattern.specifier));
    }
  }
}

} // namespace

JodaResult JodaFormatter::parse(const std::string& input) {
  JodaDate jodaDate;
  const char* cur = input.data();
  const char* end = cur + input.size();

  for (const auto& tok : tokens_) {
    switch (tok.type) {
      case JodaToken::Type::kLiteral:
        if (tok.literal.size() > end - cur ||
            std::memcmp(cur, tok.literal.data(), tok.literal.size()) != 0) {
          parseFail(input, cur, end);
        }
        cur += tok.literal.size();
        break;
      case JodaToken::Type::kPattern:
        parseFromPattern(tok.pattern, input, cur, end, jodaDate);
        break;
    }
  }

  // Ensure all input was consumed.
  if (cur < end) {
    parseFail(input, cur, end);
  }

  // Enforce Joda's year range if year was specified as "year of era".
  if (jodaDate.isYearOfEra &&
      (jodaDate.getYear() > 292278993 || jodaDate.getYear() < 1)) {
    VELOX_USER_FAIL(
        "Value {} for yearOfEra must be in the range [1,292278993]",
        jodaDate.getYear());
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
  int32_t daysSinceEpoch = util::fromDate(
      jodaDate.getYear(), jodaDate.getMonth(), jodaDate.getDay());
  int64_t microsSinceMidnight = util::fromTime(
      jodaDate.hour, jodaDate.minute, jodaDate.second, jodaDate.microsecond);
  return {
      util::fromDatetime(daysSinceEpoch, microsSinceMidnight),
      jodaDate.timezoneId};
}

} // namespace facebook::velox::functions
