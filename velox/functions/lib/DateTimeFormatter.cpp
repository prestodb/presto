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
#include "velox/external/date/date.h"
#include "velox/external/date/tz.h"
#include "velox/functions/lib/DateTimeFormatterBuilder.h"

namespace facebook::velox::functions {

namespace {

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

        case DateTimeFormatSpecifier::YEAR_OF_ERA:
          result += padContent(
              std::abs(static_cast<signed>(calDate.year())),
              '0',
              token.pattern.minRepresentDigits);
          break;

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

        case DateTimeFormatSpecifier::YEAR:
          result += padContent(
              static_cast<signed>(calDate.year()),
              '0',
              token.pattern.minRepresentDigits);
          break;

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

std::shared_ptr<DateTimeFormatter> buildMysqlDateTimeFormatter(
    const std::string_view& format) {
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

} // namespace facebook::velox::functions
