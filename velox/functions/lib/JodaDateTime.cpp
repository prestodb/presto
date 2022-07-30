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
  int32_t year = 1970;
  int32_t month = 1;
  int32_t day = 1;

  bool isYearOfEra = false; // Year of era cannot be zero or negative.
  bool hasYear = false; // Whether year was explicitly specified.

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

// Returns number of characters consumed, or throws if timezone offset id could
// not be parsed.
int64_t
parseTimezoneOffset(const char* cur, const char* end, JodaDate& jodaDate) {
  // For timezone offset ids, there are four formats allowed by Joda:
  //
  // 1. '+' or '-' followed by two digits: "+00"
  // 2. '+' or '-' followed by two digits, ":", then two more digits:
  //    "+00:00"
  // 3. '+' or '-' followed by four digits:
  //    "+0000"
  // 4. 'Z': means GMT
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
  throw std::runtime_error("Unable to parse timezone offset id.");
}

// According to JodaFormatSpecifier enum class
std::string getSpecifierName(int enumInt) {
  switch (enumInt) {
    case 0:
      return "ERA";
    case 1:
      return "CENTURY_OF_ERA";
    case 2:
      return "YEAR_OF_ERA";
    case 3:
      return "WEEK_YEAR";
    case 4:
      return "WEEK_OF_WEEK_YEAR";
    case 5:
      return "DAY_OF_WEEK";
    case 6:
      return "DAY_OF_WEEK_TEXT";
    case 7:
      return "YEAR";
    case 8:
      return "DAY_OF_YEAR";
    case 9:
      return "MONTH_OF_YEAR";
    case 10:
      return "DAY_OF_MONTH";
    case 11:
      return "HALFDAY_OF_DAY";
    case 12:
      return "HOUR_OF_HALFDAY";
    case 13:
      return "CLOCK_HOUR_OF_HALFDAY";
    case 14:
      return "HOUR_OF_DAY";
    case 15:
      return "CLOCK_HOUR_OF_DAY";
    case 16:
      return "MINUTE_OF_HOUR";
    case 17:
      return "SECOND_OF_MINUTE";
    case 18:
      return "FRACTION_OF_SECOND";
    case 19:
      return "TIMEZONE";
    case 20:
      return "TIMEZONE_OFFSET_ID";
    default:
      return "[Specifier not updated to conversion function yet]";
  }
}

void parseFromPattern(
    JodaFormatSpecifier curPattern,
    const std::string& input,
    const char*& cur,
    const char* end,
    JodaDate& jodaDate) {
  // For now, timezone offset is the only non-numeric token supported.
  if (curPattern != JodaFormatSpecifier::TIMEZONE_OFFSET_ID) {
    int64_t number = 0;
    bool negative = false;

    if (cur < end && specAllowsNegative(curPattern) && *cur == '-') {
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

    switch (curPattern) {
      case JodaFormatSpecifier::YEAR:
      case JodaFormatSpecifier::YEAR_OF_ERA:
        jodaDate.isYearOfEra = (curPattern == JodaFormatSpecifier::YEAR_OF_ERA);
        jodaDate.hasYear = true;
        jodaDate.year = number;
        break;

      case JodaFormatSpecifier::MONTH_OF_YEAR:
        jodaDate.month = number;

        // Joda has this weird behavior where it returns 1970 as the year by
        // default (if no year is specified), but if either day or month are
        // specified, it fallsback to 2000.
        if (!jodaDate.hasYear) {
          jodaDate.hasYear = true;
          jodaDate.year = 2000;
        }
        break;

      case JodaFormatSpecifier::DAY_OF_MONTH:
        jodaDate.day = number;
        if (!jodaDate.hasYear) {
          jodaDate.hasYear = true;
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

      case JodaFormatSpecifier::FRACTION_OF_SECOND:
        jodaDate.microsecond = number * util::kMicrosPerMsec;
        break;

      default:
        VELOX_NYI(
            "Numeric Joda specifier JodaFormatSpecifier::" +
            getSpecifierName(static_cast<int>(curPattern)) +
            " not implemented yet.");
    }
  } else {
    try {
      cur += parseTimezoneOffset(cur, end, jodaDate);
    } catch (...) {
      parseFail(input, cur, end);
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
        parseFromPattern(tok.pattern.specifier, input, cur, end, jodaDate);
        break;
    }
  }

  // Ensure all input was consumed.
  if (cur < end) {
    parseFail(input, cur, end);
  }

  // Enforce Joda's year range if year was specified as "year of era".
  if (jodaDate.isYearOfEra &&
      (jodaDate.year > 292278993 || jodaDate.year < 1)) {
    VELOX_USER_FAIL(
        "Value {} for yearOfEra must be in the range [1,292278993]",
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
  return {
      util::fromDatetime(daysSinceEpoch, microsSinceMidnight),
      jodaDate.timezoneId};
}

} // namespace facebook::velox::functions
