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
#include <velox/type/Date.h>
#include <cstring>
#include <stdexcept>
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
      false; // Whether most recent hour specifier is of half day.

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
    "December",
};
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
    "Dec",
};
constexpr int monthsFullLength[] = {7, 8, 5, 5, 3, 4, 4, 6, 9, 7, 8, 8};

/// Pads the content with desired padding characters. E.g. if we need to pad 999
/// with three 0s in front, the result will be '000999'
/// \param content the content that is going to be padded.
/// \param padding the padding that is going to be used to pad the content.
/// \param totalDigits the total number of digits the padded result is desired
/// to be. If totalDigits is already smaller than content length, the original
/// content will be returned with no padding
/// \param padFront if the padding is in front of the content or back of the
/// content.
template <typename T>
std::string padContent(
    const T& content,
    char padding,
    size_t totalDigits,
    bool padFront = true) {
  std::string strContent = folly::to<std::string>(content);
  auto contentLength = strContent.size();
  if (contentLength == 0) {
    return std::string(totalDigits, padding);
  }

  bool isNegative = strContent[0] == '-';
  auto digitLength = contentLength - (isNegative ? 1 : 0);
  if (digitLength >= totalDigits) {
    return strContent;
  }
  std::string paddingStr(totalDigits - digitLength, padding);
  if (padFront) {
    return strContent.insert((isNegative ? 1 : 0), paddingStr);
  } else {
    return strContent.append(paddingStr);
  }
}

void validateTimePoint(const std::chrono::time_point<
                       std::chrono::system_clock,
                       std::chrono::milliseconds>& timePoint) {
  // Due to the limit of std::chrono we can only represent time in
  // [-32767-01-01, 32767-12-31] date range
  const auto minTimePoint = date::sys_days{
      date::year_month_day(date::year::min(), date::month(1), date::day(1))};
  const auto maxTimePoint = date::sys_days{
      date::year_month_day(date::year::max(), date::month(12), date::day(31))};
  if (timePoint < minTimePoint || timePoint > maxTimePoint) {
    VELOX_USER_FAIL(
        "Cannot format time out of range of [{}-{}-{}, {}-{}-{}]",
        (int)date::year::min(),
        "01",
        "01",
        (int)date::year::max(),
        "12",
        "31");
  }
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
    const char* end) {
  VELOX_USER_FAIL(
      "Invalid format: \"{}\" is malformed at \"{}\"",
      input,
      std::string_view(cur, end - cur));
}

int64_t parseTimezoneOffset(const char* cur, const char* end, Date& date) {
  // For timezone offset ids, there are three formats allowed by Joda:
  //
  // 1. '+' or '-' followed by two digits: "+00"
  // 2. '+' or '-' followed by two digits, ":", then two more digits:
  //    "+00:00"
  // 3. '+' or '-' followed by four digits:
  //    "+0000"
  if (cur < end && (*cur == '-' || *cur == '+')) {
    // Long format: "+00:00"
    if ((end - cur) >= 6 && *(cur + 3) == ':') {
      // Fast path for the common case ("+00:00" or "-00:00"), to prevent
      // calling getTimeZoneID(), which does a map lookup.
      if (std::strncmp(cur + 1, "00:00", 5) == 0) {
        date.timezoneId = 0;
      } else {
        date.timezoneId = util::getTimeZoneID(std::string_view(cur, 6));
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

        date.timezoneId = util::getTimeZoneID(timezoneBuffer);
      }
      return 5;
    }
    // Short format: "+00"
    else if ((end - cur) >= 3) {
      // Same fast path described above.
      if (std::strncmp(cur + 1, "00", 2) == 0) {
        date.timezoneId = 0;
      } else {
        // We need to concatenate the 3 first chars with a trailing ":00" before
        // calling getTimeZoneID, so we use a static thread_local buffer to
        // prevent extra allocations.
        std::memcpy(&timezoneBuffer[0], cur, 3);
        std::memcpy(&timezoneBuffer[4], defaultTrailingOffset, 2);
        date.timezoneId = util::getTimeZoneID(timezoneBuffer);
      }
      return 3;
    }
  }
  throw std::runtime_error("Unable to parse timezone offset id.");
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
      throw std::runtime_error("Unable to parse era.");
    }
  } else {
    throw std::runtime_error("Unable to parse era.");
  }
}

int64_t parseMonthText(const char* cur, const char* end, Date& date) {
  if ((end - cur) >= 3) {
    for (int i = 0; i < 12; i++) {
      if (std::strncmp(cur, monthsShort[i].data(), 3) == 0) {
        auto length = monthsFullLength[i];
        date.month = i + 1;
        if ((end - cur) >= length &&
            std::strncmp(cur, monthsFull[i].data(), length) == 0) {
          return length;
        } else {
          return 3;
        }
      }
    }
    throw std::runtime_error("Unable to parse month.");
  } else {
    throw std::runtime_error("Unable to parse month.");
  }
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
      throw std::runtime_error("Unable to parse halfday of day.");
    }
  } else {
    throw std::runtime_error("Unable to parse halfday of day.");
  }
}

// According to DateTimeFormatSpecifier enum class
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
      return "DAY_OF_WEEK_0_BASED";
    case 6:
      return "DAY_OF_WEEK_1_BASED";
    case 7:
      return "DAY_OF_WEEK_TEXT";
    case 8:
      return "YEAR";
    case 9:
      return "DAY_OF_YEAR";
    case 10:
      return "MONTH_OF_YEAR";
    case 11:
      return "MONTH_OF_YEAR_TEXT";
    case 12:
      return "DAY_OF_MONTH";
    case 13:
      return "HALFDAY_OF_DAY";
    case 14:
      return "HOUR_OF_HALFDAY";
    case 15:
      return "CLOCK_HOUR_OF_HALFDAY";
    case 16:
      return "HOUR_OF_DAY";
    case 17:
      return "CLOCK_HOUR_OF_DAY";
    case 18:
      return "MINUTE_OF_HOUR";
    case 19:
      return "SECOND_OF_MINUTE";
    case 20:
      return "FRACTION_OF_SECOND";
    case 21:
      return "TIMEZONE";
    case 22:
      return "TIMEZONE_OFFSET_ID";
    case 23:
      return "LITERAL_PERCENT";
    default:
      return "[Specifier not updated to conversion function yet]";
  }
}

int getMaxDigitConsume(FormatPattern curPattern, bool specifierNext) {
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

void parseFromPattern(
    FormatPattern curPattern,
    const std::string_view& input,
    const char*& cur,
    const char* end,
    Date& date,
    bool specifierNext) {
  if (curPattern.specifier == DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID) {
    try {
      cur += parseTimezoneOffset(cur, end, date);
    } catch (...) {
      parseFail(input, cur, end);
    }
  } else if (curPattern.specifier == DateTimeFormatSpecifier::ERA) {
    try {
      cur += parseEra(cur, end, date);
    } catch (...) {
      parseFail(input, cur, end);
    }
  } else if (
      curPattern.specifier == DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT) {
    try {
      cur += parseMonthText(cur, end, date);
      if (!date.hasYear) {
        date.hasYear = true;
        date.year = 2000;
      }
    } catch (...) {
      parseFail(input, cur, end);
    }
  } else if (curPattern.specifier == DateTimeFormatSpecifier::HALFDAY_OF_DAY) {
    try {
      cur += parseHalfDayOfDay(cur, end, date);
    } catch (...) {
      parseFail(input, cur, end);
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
    int maxDigitConsume = getMaxDigitConsume(curPattern, specifierNext);

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
      parseFail(input, cur, end);
    }

    if (negative) {
      number *= -1L;
    }

    switch (curPattern.specifier) {
      case DateTimeFormatSpecifier::CENTURY_OF_ERA:
        // Enforce Joda's year range if year was specified as "century of year".
        if (number < 0 || number > 2922789) {
          VELOX_USER_FAIL(
              "Value {} for year must be in the range [0,2922789]", number);
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
          VELOX_USER_FAIL(
              "Value {} for yearOfEra must be in the range [1,292278993]",
              number);
        }
        // Enforce Joda's year range if year was specified as "year".
        if (!date.isYearOfEra && (number > 292278994 || number < -292275055)) {
          VELOX_USER_FAIL(
              "Value {} for year must be in the range [-292275055,292278994]",
              number);
        }
        date.hasYear = true;
        date.year = number;
        break;

      case DateTimeFormatSpecifier::MONTH_OF_YEAR:
        if (number < 1 || number > 12) {
          VELOX_USER_FAIL(
              "Value {} for monthOfYear must be in the range [1,12]", number);
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
          VELOX_USER_FAIL(
              "Value {} for clockHourOfDay must be in the range [1,24]",
              number);
        }
        date.isClockHour = true;
        date.isHourOfHalfDay = false;
        date.hour = number % 24;
        break;

      case DateTimeFormatSpecifier::HOUR_OF_DAY:
        if (number > 23 || number < 0) {
          VELOX_USER_FAIL(
              "Value {} for clockHourOfDay must be in the range [0,23]",
              number);
        }
        date.isClockHour = false;
        date.isHourOfHalfDay = false;
        date.hour = number;
        break;

      case DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY:
        if (number > 12 || number < 1) {
          VELOX_USER_FAIL(
              "Value {} for clockHourOfHalfDay must be in the range [1,12]",
              number);
        }
        date.isClockHour = true;
        date.isHourOfHalfDay = true;
        date.hour = number % 12;
        break;

      case DateTimeFormatSpecifier::HOUR_OF_HALFDAY:
        if (number > 11 || number < 0) {
          VELOX_USER_FAIL(
              "Value {} for hourOfHalfDay must be in the range [0,11]", number);
        }
        date.isClockHour = false;
        date.isHourOfHalfDay = true;
        date.hour = number;
        break;

      case DateTimeFormatSpecifier::MINUTE_OF_HOUR:
        if (number > 59 || number < 0) {
          VELOX_USER_FAIL(
              "Value {} for minuteOfHour must be in the range [0,59]", number);
        }
        date.minute = number;
        break;

      case DateTimeFormatSpecifier::SECOND_OF_MINUTE:
        if (number > 59 || number < 0) {
          VELOX_USER_FAIL(
              "Value {} for secondOfMinute must be in the range [0,59]",
              number);
        }
        date.second = number;
        break;

      case DateTimeFormatSpecifier::FRACTION_OF_SECOND:
        date.microsecond = number * util::kMicrosPerMsec;
        break;

      case DateTimeFormatSpecifier::WEEK_YEAR:
        // Enforce Joda's year range if year was specified as "week year".
        if (number < -292275054 || number > 292278993) {
          VELOX_USER_FAIL(
              "Value {} for year must be in the range [-292275054,292278993]",
              number);
        }
        date.year = number;
        date.weekDateFormat = true;
        date.dayOfYearFormat = false;
        date.centuryFormat = false;
        date.hasYear = true;
        break;

      case DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR:
        if (number < 1 || number > 52) {
          VELOX_USER_FAIL(
              "Value {} for weekOfWeekYear must be in the range [1,52]",
              number);
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
          VELOX_USER_FAIL(
              "Value {} for weekOfWeekYear must be in the range [1,7]", number);
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
            getSpecifierName(static_cast<int>(curPattern.specifier)) +
            " not implemented yet.");
    }
  }
}

} // namespace

std::string DateTimeFormatter::format(
    const Timestamp& timestamp,
    const date::time_zone* timezone) const {
  const std::chrono::
      time_point<std::chrono::system_clock, std::chrono::milliseconds>
          timePoint(std::chrono::milliseconds(timestamp.toMillis()));
  validateTimePoint(timePoint);
  const auto daysTimePoint = date::floor<date::days>(timePoint);

  const auto durationInTheDay = date::make_time(timePoint - daysTimePoint);
  const date::year_month_day calDate(daysTimePoint);
  const date::weekday weekday(daysTimePoint);

  std::string result;
  for (auto& token : tokens_) {
    if (token.type == DateTimeToken::Type::kLiteral) {
      result += token.literal;
    } else {
      switch (token.pattern.specifier) {
        case DateTimeFormatSpecifier::ERA:
          result += static_cast<signed>(calDate.year()) > 0 ? "AD" : "BC";
          break;

        case DateTimeFormatSpecifier::CENTURY_OF_ERA: {
          auto year = static_cast<signed>(calDate.year());
          year = (year < 0 ? -year : year);
          auto century = year / 100;
          result += padContent(century, '0', token.pattern.minRepresentDigits);
        } break;

        case DateTimeFormatSpecifier::YEAR_OF_ERA: {
          auto year = static_cast<signed>(calDate.year());
          if (token.pattern.minRepresentDigits == 2) {
            result += padContent(std::abs(year) % 100, '0', 2);
          } else {
            year = year <= 0 ? std::abs(year - 1) : year;
            result += padContent(year, '0', token.pattern.minRepresentDigits);
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
          result +=
              padContent(weekdayNum, '0', token.pattern.minRepresentDigits);
        } break;

        case DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT: {
          auto weekdayNum = weekday.c_encoding();
          if (token.pattern.minRepresentDigits <= 3) {
            result += weekdaysShort[weekdayNum];
          } else {
            result += weekdaysFull[weekdayNum];
          }
        } break;

        case DateTimeFormatSpecifier::YEAR: {
          auto year = static_cast<signed>(calDate.year());
          if (token.pattern.minRepresentDigits == 2) {
            year = std::abs(year);
            auto twoDigitYear = year % 100;
            result +=
                padContent(twoDigitYear, '0', token.pattern.minRepresentDigits);
          } else {
            result += padContent(
                static_cast<signed>(calDate.year()),
                '0',
                token.pattern.minRepresentDigits);
          }
        } break;

        case DateTimeFormatSpecifier::DAY_OF_YEAR: {
          auto firstDayOfTheYear = date::year_month_day(
              calDate.year(), date::month(1), date::day(1));
          auto delta =
              (date::sys_days{calDate} - date::sys_days{firstDayOfTheYear})
                  .count();
          delta += 1;
          result += padContent(delta, '0', token.pattern.minRepresentDigits);
        } break;

        case DateTimeFormatSpecifier::MONTH_OF_YEAR:
          result += padContent(
              static_cast<unsigned>(calDate.month()),
              '0',
              token.pattern.minRepresentDigits);
          break;

        case DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT:
          if (token.pattern.minRepresentDigits <= 3) {
            result += monthsShort[static_cast<unsigned>(calDate.month()) - 1];
          } else {
            result += monthsFull[static_cast<unsigned>(calDate.month()) - 1];
          }
          break;

        case DateTimeFormatSpecifier::DAY_OF_MONTH:
          result += padContent(
              static_cast<unsigned>(calDate.day()),
              '0',
              token.pattern.minRepresentDigits);
          break;

        case DateTimeFormatSpecifier::HALFDAY_OF_DAY:
          result += durationInTheDay.hours().count() < 12 ? "AM" : "PM";
          break;

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
          result += padContent(hourNum, '0', token.pattern.minRepresentDigits);
        } break;

        case DateTimeFormatSpecifier::MINUTE_OF_HOUR:
          result += padContent(
              durationInTheDay.minutes().count() % 60,
              '0',
              token.pattern.minRepresentDigits);
          break;

        case DateTimeFormatSpecifier::SECOND_OF_MINUTE:
          result += padContent(
              durationInTheDay.seconds().count() % 60,
              '0',
              token.pattern.minRepresentDigits);
          break;

        case DateTimeFormatSpecifier::FRACTION_OF_SECOND:
          result += padContent(
                        durationInTheDay.subseconds().count(),
                        '0',
                        token.pattern.minRepresentDigits,
                        false)
                        .substr(0, token.pattern.minRepresentDigits);
          break;
        case DateTimeFormatSpecifier::TIMEZONE:
          // TODO: implement short name time zone, need a map from full name to
          // short name
          if (token.pattern.minRepresentDigits <= 3) {
            VELOX_UNSUPPORTED("short name time zone is not yet supported")
          }
          if (timezone == nullptr) {
            VELOX_USER_FAIL("Timezone unknown")
          }
          result += timezone->name();
          break;

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
  return result;
}

DateTimeResult DateTimeFormatter::parse(const std::string_view& input) const {
  Date date;
  const char* cur = input.data();
  const char* end = cur + input.size();

  for (int i = 0; i < tokens_.size(); i++) {
    auto& tok = tokens_[i];
    switch (tok.type) {
      case DateTimeToken::Type::kLiteral:
        if (tok.literal.size() > end - cur ||
            std::memcmp(cur, tok.literal.data(), tok.literal.size()) != 0) {
          parseFail(input, cur, end);
        }
        cur += tok.literal.size();
        break;
      case DateTimeToken::Type::kPattern:
        if (i + 1 < tokens_.size() &&
            tokens_[i + 1].type == DateTimeToken::Type::kPattern) {
          parseFromPattern(tok.pattern, input, cur, end, date, true);
        } else {
          parseFromPattern(tok.pattern, input, cur, end, date, false);
        }
        break;
    }
  }

  // Ensure all input was consumed.
  if (cur < end) {
    parseFail(input, cur, end);
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
      VELOX_USER_FAIL(
          "Value {} for dayOfMonth must be in the range [1,{}]",
          date.dayOfMonthValues[i],
          util::getMaxDayOfMonth(date.year, date.month));
    }
  }

  // Ensure all day of year values are valid for ending year value
  for (int i = 0; i < date.dayOfYearValues.size(); i++) {
    if (!util::isValidDayOfYear(date.year, date.dayOfYearValues[i])) {
      VELOX_USER_FAIL(
          "Value {} for dayOfMonth must be in the range [1,{}]",
          date.dayOfYearValues[i],
          util::isLeapYear(date.year) ? 366 : 365);
    }
  }

  LOG(INFO) << "START LOG" << std::endl;
  LOG(INFO) << "INPUT: " << input << std::endl;
  LOG(INFO) << "YEAR: " << date.year << std::endl;
  LOG(INFO) << "MONTH: " << date.month << std::endl;
  LOG(INFO) << "DAY: " << date.day << std::endl;
  LOG(INFO) << "END LOG" << std::endl << std::endl;

  // Convert the parsed date/time into a timestamp.
  int64_t daysSinceEpoch;
  if (date.weekDateFormat) {
    daysSinceEpoch =
        util::daysSinceEpochFromWeekDate(date.year, date.week, date.dayOfWeek);
  } else if (date.dayOfYearFormat) {
    daysSinceEpoch =
        util::daysSinceEpochFromDayOfYear(date.year, date.dayOfYear);
  } else {
    daysSinceEpoch =
        util::daysSinceEpochFromDate(date.year, date.month, date.day);
  }

  int64_t microsSinceMidnight =
      util::fromTime(date.hour, date.minute, date.second, date.microsecond);
  return {
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
          VELOX_UNSUPPORTED("Specifier {} is not supported.", *tokenEnd)
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
  return builder.build();
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
  return builder.build();
}

} // namespace facebook::velox::functions
