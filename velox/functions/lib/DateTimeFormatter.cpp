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

#include "velox/functions/lib/DateTimeFormatter.h"
#include <folly/String.h>
#include <velox/common/base/Exceptions.h>
#include <charconv>
#include <cstring>
#include <stdexcept>
#include "velox/common/base/CountBits.h"
#include "velox/external/date/date.h"
#include "velox/external/date/tz.h"
#include "velox/functions/lib/DateTimeFormatterBuilder.h"
#include "velox/type/TimestampConversion.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions {

static thread_local std::string timezoneBuffer = "+00:00";
static const char* defaultTrailingOffset = "00";

namespace {

struct Date {
  int32_t year = 1970;
  int32_t month = 1;
  int32_t day = 1;
  bool isAd = true; // AD -> true, BC -> false.

  int32_t week = 1;
  int32_t dayOfWeek = 1;
  bool weekDateFormat = false;

  int32_t dayOfYear = 1;
  bool dayOfYearFormat = false;

  bool centuryFormat = false;

  bool isYearOfEra = false; // Year of era cannot be zero or negative.
  bool hasYear = false; // Whether year was explicitly specified.

  int32_t hour = 0;
  int32_t minute = 0;
  int32_t second = 0;
  int32_t microsecond = 0;
  bool isAm = true; // AM -> true, PM -> false
  int64_t timezoneId = -1;

  bool isClockHour = false; // Whether most recent hour specifier is clockhour
  bool isHourOfHalfDay =
      true; // Whether most recent hour specifier is of half day.

  std::vector<int32_t> dayOfMonthValues;
  std::vector<int32_t> dayOfYearValues;
};

constexpr std::string_view weekdaysFull[] = {
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday"};
constexpr std::string_view weekdaysShort[] =
    {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
static std::
    unordered_map<std::string_view, std::pair<std::string_view, int64_t>>
        dayOfWeekMap{
            // Capitalized.
            {"Mon", {"day", 1}},
            {"Tue", {"sday", 2}},
            {"Wed", {"nesday", 3}},
            {"Thu", {"rsday", 4}},
            {"Fri", {"day", 5}},
            {"Sat", {"urday", 6}},
            {"Sun", {"day", 7}},

            // Lower case.
            {"mon", {"day", 1}},
            {"tue", {"sday", 2}},
            {"wed", {"nesday", 3}},
            {"thu", {"rsday", 4}},
            {"fri", {"day", 5}},
            {"sat", {"urday", 6}},
            {"sun", {"day", 7}},

            // Upper case.
            {"MON", {"DAY", 1}},
            {"TUE", {"SDAY", 2}},
            {"WED", {"NESDAY", 3}},
            {"THU", {"RSDAY", 4}},
            {"FRI", {"DAY", 5}},
            {"SAT", {"URDAY", 6}},
            {"SUN", {"DAY", 7}},
        };

constexpr std::string_view monthsFull[] = {
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December"};
constexpr std::string_view monthsShort[] = {
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec"};
static std::
    unordered_map<std::string_view, std::pair<std::string_view, int64_t>>
        monthMap{
            // Capitalized.
            {"Jan", {"uary", 1}},
            {"Feb", {"ruary", 2}},
            {"Mar", {"ch", 3}},
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

// Pads the content with desired padding characters. E.g. if we need to pad 999
// with three 0s in front, the result will be '000999'.
// @param content the content that is going to be padded.
// @param padding the padding that is going to be used to pad the content.
// @param totalDigits the total number of digits the padded result is desired
// to be. If totalDigits is already smaller than content length, the original
// content will be returned with no padding.
// @param maxResultEnd the end pointer to result.
// @param result the pointer to string result.
// @param padFront if the padding is in front of the content or back of the
// content.
template <typename T>
int32_t padContent(
    const T& content,
    char padding,
    const size_t totalDigits,
    char* maxResultEnd,
    char* result,
    const bool padFront = true) {
  const bool isNegative = content < 0;
  const auto digitLength =
      isNegative ? countDigits(-(__int128_t)content) : countDigits(content);
  const auto contentLength = isNegative ? digitLength + 1 : digitLength;
  if (contentLength == 0) {
    std::fill(result, result + totalDigits, padding);
    return totalDigits;
  }

  std::to_chars_result toStatus;
  if (digitLength >= totalDigits) {
    toStatus = std::to_chars(result, maxResultEnd, content);
    return toStatus.ptr - result;
  }
  const auto paddingSize = totalDigits - digitLength;
  if (padFront) {
    if (isNegative) {
      *result = '-';
      std::fill(result + 1, result + 1 + paddingSize, padding);
      toStatus =
          std::to_chars(result + 1 + paddingSize, maxResultEnd, -content);
    } else {
      std::fill(result, result + paddingSize, padding);
      toStatus = std::to_chars(result + paddingSize, maxResultEnd, content);
    }
    return toStatus.ptr - result;
  }
  toStatus = std::to_chars(result, maxResultEnd, content);
  std::fill(toStatus.ptr, toStatus.ptr + paddingSize, padding);
  return toStatus.ptr - result + paddingSize;
}

size_t countOccurence(const std::string_view& base, const std::string& target) {
  int occurrences = 0;
  std::string::size_type pos = 0;
  while ((pos = base.find(target, pos)) != std::string::npos) {
    ++occurrences;
    pos += target.length();
  }
  return occurrences;
}

int64_t numLiteralChars(
    // Counts the number of literal characters until the next closing literal
    // sequence single quote.
    const char* cur,
    const char* end) {
  int64_t count = 0;
  while (cur < end) {
    if (*cur == '\'') {
      if (cur + 1 < end && *(cur + 1) == '\'') {
        count += 2;
        cur += 2;
      } else {
        return count;
      }
    } else {
      ++count;
      ++cur;
      // No end literal single quote found
      if (cur == end) {
        return -1;
      }
    }
  }
  return count;
}

inline bool characterIsDigit(char c) {
  return c >= '0' && c <= '9';
}

bool specAllowsNegative(DateTimeFormatSpecifier s) {
  switch (s) {
    case DateTimeFormatSpecifier::YEAR:
    case DateTimeFormatSpecifier::WEEK_YEAR:
      return true;
    default:
      return false;
  }
}

bool specAllowsPlusSign(DateTimeFormatSpecifier s, bool specifierNext) {
  if (specifierNext) {
    return false;
  } else {
    switch (s) {
      case DateTimeFormatSpecifier::YEAR:
      case DateTimeFormatSpecifier::WEEK_YEAR:
        return true;
      default:
        return false;
    }
  }
}

void parseFail(
    const std::string_view& input,
    const char* cur,
    const char* end,
    const bool failOnError) {
  if (failOnError) {
    VELOX_DCHECK_LE(cur, end);
    VELOX_USER_FAIL(
        "Invalid date format: '{}' is malformed at '{}'",
        input,
        std::string_view(cur, end - cur));
  }
}

// Joda only supports parsing a few three-letter prefixes. The list is available
// here:
//
//  https://github.com/JodaOrg/joda-time/blob/main/src/main/java/org/joda/time/DateTimeUtils.java#L437
//
// Full timezone names (e.g. "America/Los_Angeles") are not supported by Joda
// when parsing, so we don't implement them here.
int64_t parseTimezone(const char* cur, const char* end, Date& date) {
  if (cur < end) {
    // If there are at least 3 letters left.
    if (end - cur >= 3) {
      static std::unordered_map<std::string_view, int64_t> defaultTzNames{
          {"UTC", 0},
          {"GMT", 0},
          {"EST", util::getTimeZoneID("America/New_York")},
          {"EDT", util::getTimeZoneID("America/New_York")},
          {"CST", util::getTimeZoneID("America/Chicago")},
          {"CDT", util::getTimeZoneID("America/Chicago")},
          {"MST", util::getTimeZoneID("America/Denver")},
          {"MDT", util::getTimeZoneID("America/Denver")},
          {"PST", util::getTimeZoneID("America/Los_Angeles")},
          {"PDT", util::getTimeZoneID("America/Los_Angeles")},
      };

      auto it = defaultTzNames.find(std::string_view(cur, 3));
      if (it != defaultTzNames.end()) {
        date.timezoneId = it->second;
        return 3;
      }
    }
    // The format 'UT' is also accepted for UTC.
    else if ((end - cur == 2) && (*cur == 'U') && (*(cur + 1) == 'T')) {
      date.timezoneId = 0;
      return 2;
    }
  }
  return -1;
}

int64_t parseTimezoneOffset(const char* cur, const char* end, Date& date) {
  // For timezone offset ids, there are three formats allowed by Joda:
  //
  // 1. '+' or '-' followed by two digits: "+00"
  // 2. '+' or '-' followed by two digits, ":", then two more digits:
  //    "+00:00"
  // 3. '+' or '-' followed by four digits:
  //    "+0000"
  if (cur < end) {
    if (*cur == '-' || *cur == '+') {
      // Long format: "+00:00"
      if ((end - cur) >= 6 && *(cur + 3) == ':') {
        // Fast path for the common case ("+00:00" or "-00:00"), to prevent
        // calling getTimeZoneID(), which does a map lookup.
        if (std::strncmp(cur + 1, "00:00", 5) == 0) {
          date.timezoneId = 0;
        } else {
          date.timezoneId =
              util::getTimeZoneID(std::string_view(cur, 6), false);
          if (date.timezoneId == -1) {
            return -1;
          }
        }
        return 6;
      }
      // Long format without colon: "+0000"
      else if ((end - cur) >= 5 && *(cur + 3) != ':') {
        // Same fast path described above.
        if (std::strncmp(cur + 1, "0000", 4) == 0) {
          date.timezoneId = 0;
        } else {
          // We need to concatenate the 3 first chars with ":" followed by the
          // last 2 chars before calling getTimeZoneID, so we use a static
          // thread_local buffer to prevent extra allocations.
          std::memcpy(&timezoneBuffer[0], cur, 3);
          std::memcpy(&timezoneBuffer[4], cur + 3, 2);
          date.timezoneId = util::getTimeZoneID(timezoneBuffer, false);
          if (date.timezoneId == -1) {
            return -1;
          }
        }
        return 5;
      }
      // Short format: "+00"
      else if ((end - cur) >= 3) {
        // Same fast path described above.
        if (std::strncmp(cur + 1, "00", 2) == 0) {
          date.timezoneId = 0;
        } else {
          // We need to concatenate the 3 first chars with a trailing ":00"
          // before calling getTimeZoneID, so we use a static thread_local
          // buffer to prevent extra allocations.
          std::memcpy(&timezoneBuffer[0], cur, 3);
          std::memcpy(&timezoneBuffer[4], defaultTrailingOffset, 2);
          date.timezoneId = util::getTimeZoneID(timezoneBuffer, false);
          if (date.timezoneId == -1) {
            return -1;
          }
        }
        return 3;
      }
    }
    // Single 'Z' character maps to GMT.
    else if (*cur == 'Z') {
      date.timezoneId = 0;
      return 1;
    }
    // "UTC", "UCT", "GMT" and "GMT0" are also acceptable by joda.
    else if ((end - cur) >= 3) {
      if (std::strncmp(cur, "UTC", 3) == 0 ||
          std::strncmp(cur, "UCT", 3) == 0) {
        date.timezoneId = 0;
        return 3;
      } else if (std::strncmp(cur, "GMT", 3) == 0) {
        date.timezoneId = 0;
        if ((end - cur) >= 4 && *(cur + 3) == '0') {
          return 4;
        }
        return 3;
      }
    }
  }
  return -1;
}

int64_t parseEra(const char* cur, const char* end, Date& date) {
  if ((end - cur) >= 2) {
    if (std::strncmp(cur, "AD", 2) == 0 || std::strncmp(cur, "ad", 2) == 0) {
      date.isAd = true;
      return 2;
    } else if (
        std::strncmp(cur, "BC", 2) == 0 || std::strncmp(cur, "bc", 2) == 0) {
      date.isAd = false;
      return 2;
    } else {
      return -1;
    }
  } else {
    return -1;
  }
}

int64_t parseMonthText(const char* cur, const char* end, Date& date) {
  if ((end - cur) >= 3) {
    auto it = monthMap.find(std::string_view(cur, 3));
    if (it != monthMap.end()) {
      date.month = it->second.second;
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
  return -1;
}

int64_t parseDayOfWeekText(const char* cur, const char* end, Date& date) {
  if ((end - cur) >= 3) {
    auto it = dayOfWeekMap.find(std::string_view(cur, 3));
    if (it != dayOfWeekMap.end()) {
      date.dayOfWeek = it->second.second;
      if (end - cur >= it->second.first.size() + 3) {
        if (std::strncmp(
                cur + 3, it->second.first.data(), it->second.first.size()) ==
            0) {
          return it->second.first.size() + 3;
        }
      }
      return 3;
    }
  }
  return -1;
}

int64_t parseHalfDayOfDay(const char* cur, const char* end, Date& date) {
  if ((end - cur) >= 2) {
    if (std::strncmp(cur, "AM", 2) == 0 || std::strncmp(cur, "am", 2) == 0) {
      date.isAm = true;
      return 2;
    } else if (
        std::strncmp(cur, "PM", 2) == 0 || std::strncmp(cur, "pm", 2) == 0) {
      date.isAm = false;
      return 2;
    } else {
      return -1;
    }
  } else {
    return -1;
  }
}

std::string formatFractionOfSecond(
    uint16_t subseconds,
    size_t minRepresentDigits) {
  char toAdd[minRepresentDigits > 3 ? minRepresentDigits + 1 : 4];

  if (subseconds < 10) {
    toAdd[0] = '0';
    toAdd[1] = '0';
    toAdd[2] = char(subseconds + '0');
  } else if (subseconds < 100) {
    toAdd[2] = char(subseconds % 10 + '0');
    toAdd[1] = char((subseconds / 10) % 10 + '0');
    toAdd[0] = '0';
  } else {
    toAdd[2] = char(subseconds % 10 + '0');
    toAdd[1] = char((subseconds / 10) % 10 + '0');
    toAdd[0] = char((subseconds / 100) % 10 + '0');
  }

  if (minRepresentDigits > 3) {
    memset(toAdd + 3, '0', minRepresentDigits - 3);
  }

  toAdd[minRepresentDigits] = '\0';
  return toAdd;
}

// According to DateTimeFormatSpecifier enum class
std::string getSpecifierName(DateTimeFormatSpecifier specifier) {
  switch (specifier) {
    case DateTimeFormatSpecifier::ERA:
      return "ERA";
    case DateTimeFormatSpecifier::CENTURY_OF_ERA:
      return "CENTURY_OF_ERA";
    case DateTimeFormatSpecifier::YEAR_OF_ERA:
      return "YEAR_OF_ERA";
    case DateTimeFormatSpecifier::WEEK_YEAR:
      return "WEEK_YEAR";
    case DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR:
      return "WEEK_OF_WEEK_YEAR";
    case DateTimeFormatSpecifier::DAY_OF_WEEK_0_BASED:
      return "DAY_OF_WEEK_0_BASED";
    case DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED:
      return "DAY_OF_WEEK_1_BASED";
    case DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT:
      return "DAY_OF_WEEK_TEXT";
    case DateTimeFormatSpecifier::YEAR:
      return "YEAR";
    case DateTimeFormatSpecifier::DAY_OF_YEAR:
      return "DAY_OF_YEAR";
    case DateTimeFormatSpecifier::MONTH_OF_YEAR:
      return "MONTH_OF_YEAR";
    case DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT:
      return "MONTH_OF_YEAR_TEXT";
    case DateTimeFormatSpecifier::DAY_OF_MONTH:
      return "DAY_OF_MONTH";
    case DateTimeFormatSpecifier::HALFDAY_OF_DAY:
      return "HALFDAY_OF_DAY";
    case DateTimeFormatSpecifier::HOUR_OF_HALFDAY:
      return "HOUR_OF_HALFDAY";
    case DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY:
      return "CLOCK_HOUR_OF_HALFDAY";
    case DateTimeFormatSpecifier::HOUR_OF_DAY:
      return "HOUR_OF_DAY";
    case DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY:
      return "CLOCK_HOUR_OF_DAY";
    case DateTimeFormatSpecifier::MINUTE_OF_HOUR:
      return "MINUTE_OF_HOUR";
    case DateTimeFormatSpecifier::SECOND_OF_MINUTE:
      return "SECOND_OF_MINUTE";
    case DateTimeFormatSpecifier::FRACTION_OF_SECOND:
      return "FRACTION_OF_SECOND";
    case DateTimeFormatSpecifier::TIMEZONE:
      return "TIMEZONE";
    case DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID:
      return "TIMEZONE_OFFSET_ID";
    case DateTimeFormatSpecifier::LITERAL_PERCENT:
      return "LITERAL_PERCENT";
    default: {
      VELOX_UNREACHABLE("[Unexpected date format specifier]");
      return ""; // Make compiler happy.
    }
  }
}

int getMaxDigitConsume(
    FormatPattern curPattern,
    bool specifierNext,
    DateTimeFormatterType type) {
  // Does not support WEEK_YEAR, WEEK_OF_WEEK_YEAR, time zone names
  switch (curPattern.specifier) {
    case DateTimeFormatSpecifier::CENTURY_OF_ERA:
    case DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED:
    case DateTimeFormatSpecifier::FRACTION_OF_SECOND:
      return curPattern.minRepresentDigits;

    case DateTimeFormatSpecifier::YEAR_OF_ERA:
    case DateTimeFormatSpecifier::YEAR:
    case DateTimeFormatSpecifier::WEEK_YEAR:
      if (specifierNext) {
        return curPattern.minRepresentDigits;
      } else {
        if (type == DateTimeFormatterType::MYSQL) {
          // MySQL format will try to read in at most 4 digits when supplied a
          // year, never more.
          return 4;
        }
        return curPattern.minRepresentDigits > 9 ? curPattern.minRepresentDigits
                                                 : 9;
      }

    case DateTimeFormatSpecifier::MONTH_OF_YEAR:
      return 2;

    case DateTimeFormatSpecifier::DAY_OF_YEAR:
      return curPattern.minRepresentDigits > 3 ? curPattern.minRepresentDigits
                                               : 3;

    case DateTimeFormatSpecifier::DAY_OF_MONTH:
    case DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR:
    case DateTimeFormatSpecifier::HOUR_OF_HALFDAY:
    case DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY:
    case DateTimeFormatSpecifier::HOUR_OF_DAY:
    case DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY:
    case DateTimeFormatSpecifier::MINUTE_OF_HOUR:
    case DateTimeFormatSpecifier::SECOND_OF_MINUTE:
      return curPattern.minRepresentDigits > 2 ? curPattern.minRepresentDigits
                                               : 2;

    default:
      return 1;
  }
}

// If failOnError is true, throws exception for parsing error. Otherwise,
// returns -1. Returns 0 if no parsing error.
int32_t parseFromPattern(
    FormatPattern curPattern,
    const std::string_view& input,
    const char*& cur,
    const char* end,
    Date& date,
    bool specifierNext,
    DateTimeFormatterType type) {
  if (curPattern.specifier == DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID) {
    auto size = parseTimezoneOffset(cur, end, date);
    if (size == -1) {
      return -1;
    }
    cur += size;
  } else if (curPattern.specifier == DateTimeFormatSpecifier::TIMEZONE) {
    auto size = parseTimezone(cur, end, date);
    if (size == -1) {
      return -1;
    }
    cur += size;
  } else if (curPattern.specifier == DateTimeFormatSpecifier::ERA) {
    auto size = parseEra(cur, end, date);
    if (size == -1) {
      return -1;
    }
    cur += size;
  } else if (
      curPattern.specifier == DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT) {
    auto size = parseMonthText(cur, end, date);
    if (size == -1) {
      return -1;
    }
    cur += size;
    if (!date.hasYear) {
      date.hasYear = true;
      date.year = 2000;
    }
  } else if (curPattern.specifier == DateTimeFormatSpecifier::HALFDAY_OF_DAY) {
    auto size = parseHalfDayOfDay(cur, end, date);
    if (size == -1) {
      return -1;
    }
    cur += size;
  } else if (
      curPattern.specifier == DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT) {
    auto size = parseDayOfWeekText(cur, end, date);
    if (size == -1) {
      return -1;
    }
    cur += size;
    date.weekDateFormat = true;
    date.dayOfYearFormat = false;
    if (!date.hasYear) {
      date.hasYear = true;
      date.year = 2000;
    }
  } else {
    // Numeric specifier case
    bool negative = false;

    if (cur < end && specAllowsNegative(curPattern.specifier) && *cur == '-') {
      negative = true;
      ++cur;
    } else if (
        cur < end && specAllowsPlusSign(curPattern.specifier, specifierNext) &&
        *cur == '+') {
      negative = false;
      ++cur;
    }

    auto startPos = cur;
    int64_t number = 0;
    int maxDigitConsume = getMaxDigitConsume(curPattern, specifierNext, type);

    if (curPattern.specifier == DateTimeFormatSpecifier::FRACTION_OF_SECOND) {
      int count = 0;
      while (cur < end && cur < startPos + maxDigitConsume &&
             characterIsDigit(*cur)) {
        if (count < 3) {
          number = number * 10 + (*cur - '0');
        }
        ++cur;
        ++count;
      }
      number *= std::pow(10, 3 - count);
    } else if (
        (curPattern.specifier == DateTimeFormatSpecifier::YEAR ||
         curPattern.specifier == DateTimeFormatSpecifier::YEAR_OF_ERA ||
         curPattern.specifier == DateTimeFormatSpecifier::WEEK_YEAR) &&
        curPattern.minRepresentDigits == 2) {
      // If abbreviated two year digit is provided in format string, try to read
      // in two digits of year and convert to appropriate full length year The
      // two-digit mapping is as follows: [00, 69] -> [2000, 2069]
      //                                  [70, 99] -> [1970, 1999]
      // If more than two digits are provided, then simply read in full year
      // normally without conversion
      int count = 0;
      while (cur < end && cur < startPos + maxDigitConsume &&
             characterIsDigit(*cur)) {
        number = number * 10 + (*cur - '0');
        ++cur;
        ++count;
      }
      if (count == 2) {
        if (number >= 70) {
          number += 1900;
        } else if (number >= 0 && number < 70) {
          number += 2000;
        }
      } else if (type == DateTimeFormatterType::MYSQL) {
        // In MySQL format, year read in must have exactly two digits, otherwise
        // return -1 to indicate parsing error.
        if (count > 2) {
          // Larger than expected, print suffix.
          cur = cur - count + 2;
          return -1;
        } else {
          // Smaller than expected, print prefix.
          cur = cur - count;
          return -1;
        }
      }
    } else {
      while (cur < end && cur < startPos + maxDigitConsume &&
             characterIsDigit(*cur)) {
        number = number * 10 + (*cur - '0');
        ++cur;
      }
    }

    // Need to have read at least one digit.
    if (cur <= startPos) {
      return -1;
    }

    if (negative) {
      number *= -1L;
    }

    switch (curPattern.specifier) {
      case DateTimeFormatSpecifier::CENTURY_OF_ERA:
        // Enforce Joda's year range if year was specified as "century of year".
        if (number < 0 || number > 2922789) {
          return -1;
        }
        date.centuryFormat = true;
        date.year = number * 100;
        date.hasYear = true;
        break;

      case DateTimeFormatSpecifier::YEAR:
      case DateTimeFormatSpecifier::YEAR_OF_ERA:
        date.centuryFormat = false;
        date.isYearOfEra =
            (curPattern.specifier == DateTimeFormatSpecifier::YEAR_OF_ERA);
        // Enforce Joda's year range if year was specified as "year of era".
        if (date.isYearOfEra && (number > 292278993 || number < 1)) {
          return -1;
        }
        // Enforce Joda's year range if year was specified as "year".
        if (!date.isYearOfEra && (number > 292278994 || number < -292275055)) {
          return -1;
        }
        date.hasYear = true;
        date.year = number;
        break;

      case DateTimeFormatSpecifier::MONTH_OF_YEAR:
        if (number < 1 || number > 12) {
          return -1;
        }
        date.month = number;
        date.weekDateFormat = false;
        date.dayOfYearFormat = false;
        // Joda has this weird behavior where it returns 1970 as the year by
        // default (if no year is specified), but if either day or month are
        // specified, it fallsback to 2000.
        if (!date.hasYear) {
          date.hasYear = true;
          date.year = 2000;
        }
        break;

      case DateTimeFormatSpecifier::DAY_OF_MONTH:
        date.dayOfMonthValues.push_back(number);
        date.day = number;
        date.weekDateFormat = false;
        date.dayOfYearFormat = false;
        // Joda has this weird behavior where it returns 1970 as the year by
        // default (if no year is specified), but if either day or month are
        // specified, it fallsback to 2000.
        if (!date.hasYear) {
          date.hasYear = true;
          date.year = 2000;
        }
        break;

      case DateTimeFormatSpecifier::DAY_OF_YEAR:
        date.dayOfYearValues.push_back(number);
        date.dayOfYear = number;
        date.dayOfYearFormat = true;
        date.weekDateFormat = false;
        // Joda has this weird behavior where it returns 1970 as the year by
        // default (if no year is specified), but if either day or month are
        // specified, it fallsback to 2000.
        if (!date.hasYear) {
          date.hasYear = true;
          date.year = 2000;
        }
        break;

      case DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY:
        if (number > 24 || number < 1) {
          return -1;
        }
        date.isClockHour = true;
        date.isHourOfHalfDay = false;
        date.hour = number % 24;
        break;

      case DateTimeFormatSpecifier::HOUR_OF_DAY:
        if (number > 23 || number < 0) {
          return -1;
        }
        date.isClockHour = false;
        date.isHourOfHalfDay = false;
        date.hour = number;
        break;

      case DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY:
        if (number > 12 || number < 1) {
          return -1;
        }
        date.isClockHour = true;
        date.isHourOfHalfDay = true;
        date.hour = number % 12;
        break;

      case DateTimeFormatSpecifier::HOUR_OF_HALFDAY:
        if (number > 11 || number < 0) {
          return -1;
        }
        date.isClockHour = false;
        date.isHourOfHalfDay = true;
        date.hour = number;
        break;

      case DateTimeFormatSpecifier::MINUTE_OF_HOUR:
        if (number > 59 || number < 0) {
          return -1;
        }
        date.minute = number;
        break;

      case DateTimeFormatSpecifier::SECOND_OF_MINUTE:
        if (number > 59 || number < 0) {
          return -1;
        }
        date.second = number;
        break;

      case DateTimeFormatSpecifier::FRACTION_OF_SECOND:
        date.microsecond = number * util::kMicrosPerMsec;
        break;

      case DateTimeFormatSpecifier::WEEK_YEAR:
        // Enforce Joda's year range if year was specified as "week year".
        if (number < -292275054 || number > 292278993) {
          return -1;
        }
        date.year = number;
        date.weekDateFormat = true;
        date.dayOfYearFormat = false;
        date.centuryFormat = false;
        date.hasYear = true;
        break;

      case DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR:
        if (number < 1 || number > 52) {
          return -1;
        }
        date.week = number;
        date.weekDateFormat = true;
        date.dayOfYearFormat = false;
        if (!date.hasYear) {
          date.hasYear = true;
          date.year = 2000;
        }
        break;

      case DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED:
        if (number < 1 || number > 7) {
          return -1;
        }
        date.dayOfWeek = number;
        date.weekDateFormat = true;
        date.dayOfYearFormat = false;
        if (!date.hasYear) {
          date.hasYear = true;
          date.year = 2000;
        }
        break;

      default:
        VELOX_NYI(
            "Numeric Joda specifier DateTimeFormatSpecifier::" +
            getSpecifierName(curPattern.specifier) + " not implemented yet.");
    }
  }
  return 0;
}

} // namespace

uint32_t DateTimeFormatter::maxResultSize(
    const date::time_zone* timezone) const {
  uint32_t size = 0;
  for (const auto& token : tokens_) {
    if (token.type == DateTimeToken::Type::kLiteral) {
      size += token.literal.size();
      continue;
    }
    switch (token.pattern.specifier) {
      case DateTimeFormatSpecifier::ERA:
      case DateTimeFormatSpecifier::HALFDAY_OF_DAY:
        // Fixed size.
        size += 2;
        break;
      case DateTimeFormatSpecifier::YEAR_OF_ERA:
        // Timestamp is in [-32767-01-01, 32767-12-31] range.
        size += std::max((int)token.pattern.minRepresentDigits, 6);
        break;
      case DateTimeFormatSpecifier::DAY_OF_WEEK_0_BASED:
      case DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED:
        size += std::max((int)token.pattern.minRepresentDigits, 1);
        break;
      case DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT:
      case DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT:
        // 9 is the max size of elements in weekdaysFull or monthsFull.
        size += token.pattern.minRepresentDigits <= 3 ? 3 : 9;
        break;
      case DateTimeFormatSpecifier::YEAR:
        // Timestamp is in [-32767-01-01, 32767-12-31] range.
        size += token.pattern.minRepresentDigits == 2
            ? 2
            : std::max((int)token.pattern.minRepresentDigits, 6);
        break;
      case DateTimeFormatSpecifier::CENTURY_OF_ERA:
      case DateTimeFormatSpecifier::DAY_OF_YEAR:
        size += std::max((int)token.pattern.minRepresentDigits, 3);
        break;
      case DateTimeFormatSpecifier::MONTH_OF_YEAR:
      case DateTimeFormatSpecifier::DAY_OF_MONTH:
      case DateTimeFormatSpecifier::HOUR_OF_HALFDAY:
      case DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY:
      case DateTimeFormatSpecifier::HOUR_OF_DAY:
      case DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY:
      case DateTimeFormatSpecifier::MINUTE_OF_HOUR:
      case DateTimeFormatSpecifier::SECOND_OF_MINUTE:
        size += std::max((int)token.pattern.minRepresentDigits, 2);
        break;
      case DateTimeFormatSpecifier::FRACTION_OF_SECOND:
        // Nanosecond is considered.
        size += std::max((int)token.pattern.minRepresentDigits, 9);
        break;
      case DateTimeFormatSpecifier::TIMEZONE:
        if (timezone == nullptr) {
          VELOX_USER_FAIL("Timezone unknown")
        }
        size += std::max(
            token.pattern.minRepresentDigits, timezone->name().length());
        break;
      // Not supported.
      case DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID:
      case DateTimeFormatSpecifier::WEEK_YEAR:
      case DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR:
      default:
        VELOX_UNSUPPORTED(
            "Date format specifier is not supported: {}",
            getSpecifierName(token.pattern.specifier));
    }
  }
  return size;
}

int32_t DateTimeFormatter::format(
    const Timestamp& timestamp,
    const date::time_zone* timezone,
    const uint32_t maxResultSize,
    char* result,
    bool allowOverflow) const {
  Timestamp t = timestamp;
  if (timezone != nullptr) {
    t.toTimezone(*timezone);
  }
  const auto timePoint = t.toTimePoint(allowOverflow);
  const auto daysTimePoint = date::floor<date::days>(timePoint);

  const auto durationInTheDay = date::make_time(timePoint - daysTimePoint);
  const date::year_month_day calDate(daysTimePoint);
  const date::weekday weekday(daysTimePoint);

  const char* resultStart = result;
  char* maxResultEnd = result + maxResultSize;
  for (auto& token : tokens_) {
    if (token.type == DateTimeToken::Type::kLiteral) {
      std::memcpy(result, token.literal.data(), token.literal.size());
      result += token.literal.size();
    } else {
      switch (token.pattern.specifier) {
        case DateTimeFormatSpecifier::ERA: {
          const std::string_view piece =
              static_cast<signed>(calDate.year()) > 0 ? "AD" : "BC";
          std::memcpy(result, piece.data(), piece.length());
          result += piece.length();
        } break;
        case DateTimeFormatSpecifier::CENTURY_OF_ERA: {
          auto year = static_cast<signed>(calDate.year());
          year = (year < 0 ? -year : year);
          auto century = year / 100;
          result += padContent(
              century,
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
        } break;

        case DateTimeFormatSpecifier::YEAR_OF_ERA: {
          auto year = static_cast<signed>(calDate.year());
          if (token.pattern.minRepresentDigits == 2) {
            result +=
                padContent(std::abs(year) % 100, '0', 2, maxResultEnd, result);
          } else {
            year = year <= 0 ? std::abs(year - 1) : year;
            result += padContent(
                year,
                '0',
                token.pattern.minRepresentDigits,
                maxResultEnd,
                result);
          }
        } break;

        case DateTimeFormatSpecifier::DAY_OF_WEEK_0_BASED:
        case DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED: {
          auto weekdayNum = weekday.c_encoding();
          if (weekdayNum == 0 &&
              token.pattern.specifier ==
                  DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED) {
            weekdayNum = 7;
          }
          result += padContent(
              weekdayNum,
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
        } break;

        case DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT: {
          auto weekdayNum = weekday.c_encoding();
          std::string_view piece;
          if (token.pattern.minRepresentDigits <= 3) {
            piece = weekdaysShort[weekdayNum];
          } else {
            piece = weekdaysFull[weekdayNum];
          }
          std::memcpy(result, piece.data(), piece.length());
          result += piece.length();
        } break;

        case DateTimeFormatSpecifier::YEAR: {
          auto year = static_cast<signed>(calDate.year());
          if (token.pattern.minRepresentDigits == 2) {
            year = std::abs(year);
            auto twoDigitYear = year % 100;
            result += padContent(
                twoDigitYear,
                '0',
                token.pattern.minRepresentDigits,
                maxResultEnd,
                result);
          } else {
            result += padContent(
                static_cast<signed>(calDate.year()),
                '0',
                token.pattern.minRepresentDigits,
                maxResultEnd,
                result);
          }
        } break;

        case DateTimeFormatSpecifier::DAY_OF_YEAR: {
          auto firstDayOfTheYear = date::year_month_day(
              calDate.year(), date::month(1), date::day(1));
          auto delta =
              (date::sys_days{calDate} - date::sys_days{firstDayOfTheYear})
                  .count();
          delta += 1;
          result += padContent(
              delta,
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
        } break;

        case DateTimeFormatSpecifier::MONTH_OF_YEAR:
          result += padContent(
              static_cast<unsigned>(calDate.month()),
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
          break;

        case DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT: {
          std::string_view piece;
          if (token.pattern.minRepresentDigits <= 3) {
            piece = monthsShort[static_cast<unsigned>(calDate.month()) - 1];
          } else {
            piece = monthsFull[static_cast<unsigned>(calDate.month()) - 1];
          }
          std::memcpy(result, piece.data(), piece.length());
          result += piece.length();
        } break;

        case DateTimeFormatSpecifier::DAY_OF_MONTH:
          result += padContent(
              static_cast<unsigned>(calDate.day()),
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
          break;

        case DateTimeFormatSpecifier::HALFDAY_OF_DAY: {
          const std::string_view piece =
              durationInTheDay.hours().count() < 12 ? "AM" : "PM";
          std::memcpy(result, piece.data(), piece.length());
          result += piece.length();
        } break;

        case DateTimeFormatSpecifier::HOUR_OF_HALFDAY:
        case DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY:
        case DateTimeFormatSpecifier::HOUR_OF_DAY:
        case DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY: {
          auto hourNum = durationInTheDay.hours().count();
          if (token.pattern.specifier ==
              DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY) {
            hourNum = (hourNum + 11) % 12 + 1;
          } else if (
              token.pattern.specifier ==
              DateTimeFormatSpecifier::HOUR_OF_HALFDAY) {
            hourNum = hourNum % 12;
          } else if (
              token.pattern.specifier ==
              DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY) {
            hourNum = (hourNum + 23) % 24 + 1;
          }
          result += padContent(
              hourNum,
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
        } break;

        case DateTimeFormatSpecifier::MINUTE_OF_HOUR:
          result += padContent(
              durationInTheDay.minutes().count() % 60,
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
          break;

        case DateTimeFormatSpecifier::SECOND_OF_MINUTE:
          result += padContent(
              durationInTheDay.seconds().count() % 60,
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
          break;

        case DateTimeFormatSpecifier::FRACTION_OF_SECOND: {
          const auto& piece = formatFractionOfSecond(
              durationInTheDay.subseconds().count(),
              token.pattern.minRepresentDigits);
          std::memcpy(result, piece.data(), piece.length());
          result += piece.length();
        } break;

        case DateTimeFormatSpecifier::TIMEZONE: {
          // TODO: implement short name time zone, need a map from full name to
          // short name
          if (token.pattern.minRepresentDigits <= 3) {
            VELOX_UNSUPPORTED("short name time zone is not yet supported")
          }
          if (timezone == nullptr) {
            VELOX_USER_FAIL("Timezone unknown")
          }
          const auto& piece = timezone->name();
          std::memcpy(result, piece.data(), piece.length());
          result += piece.length();
        } break;

        case DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID:
          // TODO: implement timezone offset id formatting, need a map from full
          // name to offset time
        case DateTimeFormatSpecifier::WEEK_YEAR:
        case DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR:
        default:
          VELOX_UNSUPPORTED(
              "format is not supported for specifier {}",
              token.pattern.specifier);
      }
    }
  }
  auto resultSize = result - resultStart;
  VELOX_CHECK_LE(resultSize, maxResultSize, "Bad allocation size for result.");
  return resultSize;
}

std::optional<DateTimeResult> DateTimeFormatter::parse(
    const std::string_view& input,
    const bool failOnError) const {
  Date date;
  const char* cur = input.data();
  const char* end = cur + input.size();

  for (int i = 0; i < tokens_.size(); i++) {
    auto& tok = tokens_[i];
    switch (tok.type) {
      case DateTimeToken::Type::kLiteral:
        if (tok.literal.size() > end - cur ||
            std::memcmp(cur, tok.literal.data(), tok.literal.size()) != 0) {
          parseFail(input, cur, end, failOnError);
          return std::nullopt;
        }
        cur += tok.literal.size();
        break;
      case DateTimeToken::Type::kPattern:
        if (i + 1 < tokens_.size() &&
            tokens_[i + 1].type == DateTimeToken::Type::kPattern) {
          if (parseFromPattern(
                  tok.pattern, input, cur, end, date, true, type_) == -1) {
            parseFail(input, cur, end, failOnError);
            return std::nullopt;
          }
        } else {
          if (parseFromPattern(
                  tok.pattern, input, cur, end, date, false, type_) == -1) {
            parseFail(input, cur, end, failOnError);
            return std::nullopt;
          }
        }
        break;
    }
  }

  // Ensure all input was consumed.
  if (cur < end) {
    parseFail(input, cur, end, failOnError);
    return std::nullopt;
  }

  // Era is BC and year of era is provided
  if (date.isYearOfEra && !date.isAd) {
    date.year = -1 * (date.year - 1);
  }

  if (date.isHourOfHalfDay) {
    if (!date.isAm) {
      date.hour += 12;
    }
  }

  // Ensure all day of month values are valid for ending month value
  for (int i = 0; i < date.dayOfMonthValues.size(); i++) {
    if (!util::isValidDate(date.year, date.month, date.dayOfMonthValues[i])) {
      if (!failOnError) {
        return std::nullopt;
      }
      VELOX_USER_FAIL(
          "Value {} for dayOfMonth must be in the range [1,{}] "
          "for year {} and month {}.",
          date.dayOfMonthValues[i],
          util::getMaxDayOfMonth(date.year, date.month),
          date.year,
          date.month);
    }
  }

  // Ensure all day of year values are valid for ending year value
  for (int i = 0; i < date.dayOfYearValues.size(); i++) {
    if (!util::isValidDayOfYear(date.year, date.dayOfYearValues[i])) {
      if (!failOnError) {
        return std::nullopt;
      }
      VELOX_USER_FAIL(
          "Value {} for dayOfMonth must be in the range [1,{}] "
          "for year {} and month {}.",
          date.dayOfYearValues[i],
          util::isLeapYear(date.year) ? 366 : 365,
          date.year,
          date.month);
    }
  }

  // Convert the parsed date/time into a timestamp.
  int64_t daysSinceEpoch;
  Status status;
  if (date.weekDateFormat) {
    status = util::daysSinceEpochFromWeekDate(
        date.year, date.week, date.dayOfWeek, daysSinceEpoch);
  } else if (date.dayOfYearFormat) {
    status = util::daysSinceEpochFromDayOfYear(
        date.year, date.dayOfYear, daysSinceEpoch);
  } else {
    status = util::daysSinceEpochFromDate(
        date.year, date.month, date.day, daysSinceEpoch);
  }
  if (!status.ok()) {
    VELOX_DCHECK(status.isUserError());
    if (!failOnError) {
      return std::nullopt;
    }
    VELOX_USER_FAIL(status.message());
  }

  int64_t microsSinceMidnight =
      util::fromTime(date.hour, date.minute, date.second, date.microsecond);
  return DateTimeResult{
      util::fromDatetime(daysSinceEpoch, microsSinceMidnight), date.timezoneId};
}

std::shared_ptr<DateTimeFormatter> buildMysqlDateTimeFormatter(
    const std::string_view& format) {
  if (format.empty()) {
    VELOX_USER_FAIL("Both printing and parsing not supported");
  }

  // For %r we should reserve 1 extra space because it has 3 literals ':' ':'
  // and ' '
  DateTimeFormatterBuilder builder(
      format.size() + countOccurence(format, "%r"));

  const char* cur = format.data();
  const char* end = cur + format.size();
  while (cur < end) {
    auto tokenEnd = cur;
    if (*tokenEnd == '%') { // pattern
      ++tokenEnd;
      if (tokenEnd == end) {
        break;
      }
      switch (*tokenEnd) {
        case 'a':
          builder.appendDayOfWeekText(3);
          break;
        case 'b':
          builder.appendMonthOfYearText(3);
          break;
        case 'c':
          builder.appendMonthOfYear(1);
          break;
        case 'd':
          builder.appendDayOfMonth(2);
          break;
        case 'e':
          builder.appendDayOfMonth(1);
          break;
        case 'f':
          builder.appendFractionOfSecond(6);
          break;
        case 'H':
          builder.appendHourOfDay(2);
          break;
        case 'h':
        case 'I':
          builder.appendClockHourOfHalfDay(2);
          break;
        case 'i':
          builder.appendMinuteOfHour(2);
          break;
        case 'j':
          builder.appendDayOfYear(3);
          break;
        case 'k':
          builder.appendHourOfDay(1);
          break;
        case 'l':
          builder.appendClockHourOfHalfDay(1);
          break;
        case 'M':
          builder.appendMonthOfYearText(4);
          break;
        case 'm':
          builder.appendMonthOfYear(2);
          break;
        case 'p':
          builder.appendHalfDayOfDay();
          break;
        case 'r':
          builder.appendClockHourOfHalfDay(2);
          builder.appendLiteral(":");
          builder.appendMinuteOfHour(2);
          builder.appendLiteral(":");
          builder.appendSecondOfMinute(2);
          builder.appendLiteral(" ");
          builder.appendHalfDayOfDay();
          break;
        case 'S':
        case 's':
          builder.appendSecondOfMinute(2);
          break;
        case 'T':
          builder.appendHourOfDay(2);
          builder.appendLiteral(":");
          builder.appendMinuteOfHour(2);
          builder.appendLiteral(":");
          builder.appendSecondOfMinute(2);
          break;
        case 'v':
          builder.appendWeekOfWeekYear(2);
          break;
        case 'W':
          builder.appendDayOfWeekText(4);
          break;
        case 'x':
          builder.appendWeekYear(4);
          break;
        case 'Y':
          builder.appendYear(4);
          break;
        case 'y':
          builder.appendYear(2);
          break;
        case '%':
          builder.appendLiteral("%");
          break;
        case 'D':
        case 'U':
        case 'u':
        case 'V':
        case 'w':
        case 'X':
          VELOX_UNSUPPORTED(
              "Date format specifier is not supported: %{}", *tokenEnd)
        default:
          builder.appendLiteral(tokenEnd, 1);
          break;
      }
      ++tokenEnd;
    } else {
      while (tokenEnd < end && *tokenEnd != '%') {
        ++tokenEnd;
      }
      builder.appendLiteral(cur, tokenEnd - cur);
    }
    cur = tokenEnd;
  }
  return builder.setType(DateTimeFormatterType::MYSQL).build();
}

std::shared_ptr<DateTimeFormatter> buildJodaDateTimeFormatter(
    const std::string_view& format) {
  if (format.empty()) {
    VELOX_USER_FAIL("Invalid pattern specification");
  }

  DateTimeFormatterBuilder builder(format.size());
  const char* cur = format.data();
  const char* end = cur + format.size();

  while (cur < end) {
    const char* startTokenPtr = cur;

    // Literal case
    if (*startTokenPtr == '\'') {
      // Case 1: 2 consecutive single quote
      if (cur + 1 < end && *(cur + 1) == '\'') {
        builder.appendLiteral("'");
        cur += 2;
      } else {
        // Case 2: find closing single quote
        int64_t count = numLiteralChars(startTokenPtr + 1, end);
        if (count == -1) {
          VELOX_USER_FAIL("No closing single quote for literal");
        } else {
          for (int64_t i = 1; i <= count; i++) {
            builder.appendLiteral(startTokenPtr + i, 1);
            if (*(startTokenPtr + i) == '\'') {
              i += 1;
            }
          }
          cur += count + 2;
        }
      }
    } else {
      int count = 1;
      ++cur;
      while (cur < end && *startTokenPtr == *cur) {
        ++count;
        ++cur;
      }
      switch (*startTokenPtr) {
        case 'G':
          builder.appendEra();
          break;
        case 'C':
          builder.appendCenturyOfEra(count);
          break;
        case 'Y':
          builder.appendYearOfEra(count);
          break;
        case 'x':
          builder.appendWeekYear(count);
          break;
        case 'w':
          builder.appendWeekOfWeekYear(count);
          break;
        case 'e':
          builder.appendDayOfWeek1Based(count);
          break;
        case 'E':
          builder.appendDayOfWeekText(count);
          break;
        case 'y':
          builder.appendYear(count);
          break;
        case 'D':
          builder.appendDayOfYear(count);
          break;
        case 'M':
          if (count <= 2) {
            builder.appendMonthOfYear(count);
          } else {
            builder.appendMonthOfYearText(count);
          }
          break;
        case 'd':
          builder.appendDayOfMonth(count);
          break;
        case 'a':
          builder.appendHalfDayOfDay();
          break;
        case 'K':
          builder.appendHourOfHalfDay(count);
          break;
        case 'h':
          builder.appendClockHourOfHalfDay(count);
          break;
        case 'H':
          builder.appendHourOfDay(count);
          break;
        case 'k':
          builder.appendClockHourOfDay(count);
          break;
        case 'm':
          builder.appendMinuteOfHour(count);
          break;
        case 's':
          builder.appendSecondOfMinute(count);
          break;
        case 'S':
          builder.appendFractionOfSecond(count);
          break;
        case 'z':
          builder.appendTimeZone(count);
          break;
        case 'Z':
          builder.appendTimeZoneOffsetId(count);
          break;
        default:
          if (isalpha(*startTokenPtr)) {
            VELOX_UNSUPPORTED("Specifier {} is not supported.", *startTokenPtr);
          } else {
            builder.appendLiteral(startTokenPtr, cur - startTokenPtr);
          }
          break;
      }
    }
  }
  return builder.setType(DateTimeFormatterType::JODA).build();
}

} // namespace facebook::velox::functions
