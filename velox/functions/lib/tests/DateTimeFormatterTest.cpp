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
#include <velox/common/base/VeloxException.h>
#include <velox/type/StringView.h>
#include "velox/common/base/Exceptions.h"
#include "velox/external/date/tz.h"
#include "velox/functions/lib/DateTimeFormatterBuilder.h"
#include "velox/type/TimestampConversion.h"
#include "velox/type/tz/TimeZoneMap.h"

#include <gtest/gtest.h>
#include <string>

using namespace facebook::velox;

namespace facebook::velox::functions {

class DateTimeFormatterTest : public testing::Test {
 protected:
  static constexpr std::string_view monthsFull[] = {
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

  static constexpr std::string_view monthsShort[] = {
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

  void testTokenRange(
      char specifier,
      int numTokenStart,
      int numTokenEnd,
      const DateTimeFormatSpecifier& token) {
    for (size_t i = numTokenStart; i <= numTokenEnd; i++) {
      std::string pattern(i, specifier);
      std::vector<DateTimeToken> expected;
      expected = {DateTimeToken(FormatPattern{token, i})};
      EXPECT_EQ(expected, buildJodaDateTimeFormatter(pattern)->tokens());
    }
  }

  DateTimeResult parseJoda(
      const std::string_view& input,
      const std::string_view& format) {
    return buildJodaDateTimeFormatter(format)->parse(input);
  }

  Timestamp parseMysql(
      const std::string_view& input,
      const std::string_view& format) {
    return buildMysqlDateTimeFormatter(format)->parse(input).timestamp;
  }

  // Parses and returns the timezone converted back to string, to ease
  // verifiability.
  std::string parseTZ(
      const std::string_view& input,
      const std::string_view& format) {
    auto result = buildJodaDateTimeFormatter(format)->parse(input);
    if (result.timezoneId == 0) {
      return "+00:00";
    }
    return util::getTimeZoneName(result.timezoneId);
  }
};

TEST_F(DateTimeFormatterTest, fixedLengthTokenBuilder) {
  DateTimeFormatterBuilder builder(100);
  std::string expectedLiterals;
  std::vector<DateTimeToken> expectedTokens;

  // Test fixed length tokens
  builder.appendEra();
  builder.appendLiteral("-");
  auto formatter =
      builder.appendHalfDayOfDay().setType(DateTimeFormatterType::JODA).build();

  expectedLiterals = "-";
  std::string_view actualLiterals(
      formatter->literalBuf().get(), formatter->bufSize());
  EXPECT_EQ(actualLiterals, expectedLiterals);
  expectedTokens = {
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::ERA, 2}),
      DateTimeToken("-"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::HALFDAY_OF_DAY, 2})};
  EXPECT_EQ(formatter->tokens(), expectedTokens);
}

TEST_F(DateTimeFormatterTest, variableLengthTokenBuilder) {
  // Test variable length tokens
  DateTimeFormatterBuilder builder(100);
  std::string expectedLiterals;
  std::vector<DateTimeToken> expectedTokens;

  auto formatter = builder.appendCenturyOfEra(3)
                       .appendLiteral("-")
                       .appendYearOfEra(4)
                       .appendLiteral("/")
                       .appendWeekYear(3)
                       .appendLiteral("//")
                       .appendWeekOfWeekYear(3)
                       .appendLiteral("-00-")
                       .appendDayOfWeek0Based(3)
                       .appendDayOfWeek1Based(4)
                       .appendLiteral("--")
                       .appendDayOfWeekText(6)
                       .appendLiteral("---")
                       .appendYear(5)
                       .appendLiteral("///")
                       .appendDayOfYear(4)
                       .appendMonthOfYear(2)
                       .appendMonthOfYearText(4)
                       .appendDayOfMonth(4)
                       .appendHourOfHalfDay(2)
                       .appendClockHourOfHalfDay(3)
                       .appendClockHourOfDay(2)
                       .appendHourOfDay(2)
                       .appendMinuteOfHour(2)
                       .appendSecondOfMinute(1)
                       .appendFractionOfSecond(6)
                       .appendTimeZone(3)
                       .appendTimeZoneOffsetId(3)
                       .setType(DateTimeFormatterType::JODA)
                       .build();

  expectedLiterals = "-///-00------///";
  auto actualLiterals =
      std::string_view(formatter->literalBuf().get(), formatter->bufSize());
  EXPECT_EQ(actualLiterals, expectedLiterals);
  expectedTokens = {
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::CENTURY_OF_ERA, 3}),
      DateTimeToken("-"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR_OF_ERA, 4}),
      DateTimeToken("/"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::WEEK_YEAR, 3}),
      DateTimeToken("//"),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR, 3}),
      DateTimeToken("-00-"),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::DAY_OF_WEEK_0_BASED, 3}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED, 4}),
      DateTimeToken("--"),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT, 6}),
      DateTimeToken("---"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 5}),
      DateTimeToken("///"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::DAY_OF_YEAR, 4}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::MONTH_OF_YEAR, 2}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT, 4}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::DAY_OF_MONTH, 4}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::HOUR_OF_HALFDAY, 2}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY, 3}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY, 2}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::HOUR_OF_DAY, 2}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::MINUTE_OF_HOUR, 2}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::SECOND_OF_MINUTE, 1}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::FRACTION_OF_SECOND, 6}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::TIMEZONE, 3}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID, 3})};
  EXPECT_EQ(formatter->tokens(), expectedTokens);
}

class JodaDateTimeFormatterTest : public DateTimeFormatterTest {};

TEST_F(JodaDateTimeFormatterTest, validJodaBuild) {
  std::vector<DateTimeToken> expected;

  // G specifier case
  expected = {DateTimeToken(FormatPattern{DateTimeFormatSpecifier::ERA, 2})};
  EXPECT_EQ(expected, buildJodaDateTimeFormatter("G")->tokens());
  // minRepresentDigits should be unchanged with higher number of specifier for
  // ERA
  expected = {DateTimeToken(FormatPattern{DateTimeFormatSpecifier::ERA, 2})};
  EXPECT_EQ(expected, buildJodaDateTimeFormatter("GGGG")->tokens());

  // C specifier case
  testTokenRange('C', 1, 3, DateTimeFormatSpecifier::CENTURY_OF_ERA);

  // Y specifier case
  testTokenRange('Y', 1, 4, DateTimeFormatSpecifier::YEAR_OF_ERA);

  // x specifier case
  testTokenRange('x', 1, 4, DateTimeFormatSpecifier::WEEK_YEAR);

  // w specifier case
  testTokenRange('w', 1, 4, DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR);

  // e specifier case
  testTokenRange('e', 1, 4, DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED);

  // E specifier case
  testTokenRange('E', 1, 4, DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT);

  // y specifier case
  testTokenRange('y', 1, 4, DateTimeFormatSpecifier::YEAR);

  // D specifier case
  testTokenRange('D', 1, 4, DateTimeFormatSpecifier::DAY_OF_YEAR);

  // M specifier case
  testTokenRange('M', 1, 2, DateTimeFormatSpecifier::MONTH_OF_YEAR);
  testTokenRange('M', 3, 4, DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT);

  // d specifier case
  testTokenRange('d', 1, 4, DateTimeFormatSpecifier::DAY_OF_MONTH);

  // a specifier case
  expected = {
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::HALFDAY_OF_DAY, 2})};
  EXPECT_EQ(expected, buildJodaDateTimeFormatter("a")->tokens());
  // minRepresentDigits should be unchanged with higher number of specifier for
  // HALFDAY_OF_DAY
  expected = {
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::HALFDAY_OF_DAY, 2})};
  EXPECT_EQ(expected, buildJodaDateTimeFormatter("aa")->tokens());

  // K specifier case
  testTokenRange('K', 1, 4, DateTimeFormatSpecifier::HOUR_OF_HALFDAY);

  // h specifier case
  testTokenRange('h', 1, 4, DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY);

  // H specifier case
  testTokenRange('H', 1, 4, DateTimeFormatSpecifier::HOUR_OF_DAY);

  // k specifier case
  testTokenRange('k', 1, 4, DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY);

  // m specifier case
  testTokenRange('m', 1, 4, DateTimeFormatSpecifier::MINUTE_OF_HOUR);

  // s specifier
  testTokenRange('s', 1, 4, DateTimeFormatSpecifier::SECOND_OF_MINUTE);

  // S specifier
  testTokenRange('S', 1, 4, DateTimeFormatSpecifier::FRACTION_OF_SECOND);

  // z specifier
  testTokenRange('z', 1, 4, DateTimeFormatSpecifier::TIMEZONE);

  // Z specifier
  testTokenRange('Z', 1, 4, DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID);

  // Literal case
  expected = {DateTimeToken(" ")};
  EXPECT_EQ(expected, buildJodaDateTimeFormatter(" ")->tokens());
  expected = {DateTimeToken("1234567890")};
  EXPECT_EQ(expected, buildJodaDateTimeFormatter("1234567890")->tokens());
  expected = {DateTimeToken("'")};
  EXPECT_EQ(expected, buildJodaDateTimeFormatter("''")->tokens());
  expected = {DateTimeToken("abcdefghijklmnopqrstuvwxyz")};
  EXPECT_EQ(
      expected,
      buildJodaDateTimeFormatter("'abcdefghijklmnopqrstuvwxyz'")->tokens());
  expected = {DateTimeToken("'abcdefg'hijklmnop'qrstuv'wxyz'")};
  EXPECT_EQ(
      expected,
      buildJodaDateTimeFormatter("'''abcdefg''hijklmnop''qrstuv''wxyz'''")
          ->tokens());
  expected = {DateTimeToken("'1234abcd")};
  EXPECT_EQ(expected, buildJodaDateTimeFormatter("''1234'abcd'")->tokens());

  // Specifier combinations
  expected = {
      DateTimeToken("'"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::CENTURY_OF_ERA, 3}),
      DateTimeToken("-"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR_OF_ERA, 4}),
      DateTimeToken("/"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::WEEK_YEAR, 3}),
      DateTimeToken("//"),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR, 3}),
      DateTimeToken("-00-"),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED, 4}),
      DateTimeToken("--"),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT, 6}),
      DateTimeToken("---"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 5}),
      DateTimeToken("///"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::DAY_OF_YEAR, 4}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::MONTH_OF_YEAR, 2}),
      DateTimeToken("-"),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT, 4}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::DAY_OF_MONTH, 4}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::HOUR_OF_HALFDAY, 2}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY, 3}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY, 2}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::HOUR_OF_DAY, 2}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::MINUTE_OF_HOUR, 2}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::SECOND_OF_MINUTE, 1}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::FRACTION_OF_SECOND, 6}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::TIMEZONE, 3}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID, 3}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::ERA, 2}),
      DateTimeToken("abcdefghijklmnopqrstuvwxyz"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::HALFDAY_OF_DAY, 2}),
  };

  EXPECT_EQ(
      expected,
      buildJodaDateTimeFormatter(
          "''CCC-YYYY/xxx//www-00-eeee--EEEEEE---yyyyy///DDDDMM-MMMMddddKKhhhkkHHmmsSSSSSSzzzZZZGGGG'abcdefghijklmnopqrstuvwxyz'aaa")
          ->tokens());
}

TEST_F(JodaDateTimeFormatterTest, invalidJodaBuild) {
  // Invalid specifiers
  EXPECT_THROW(buildJodaDateTimeFormatter("q"), VeloxUserError);
  EXPECT_THROW(buildJodaDateTimeFormatter("r"), VeloxUserError);
  EXPECT_THROW(buildJodaDateTimeFormatter("g"), VeloxUserError);

  // Unclosed literal sequence
  EXPECT_THROW(buildJodaDateTimeFormatter("'abcd"), VeloxUserError);

  // Empty format string
  EXPECT_THROW(buildJodaDateTimeFormatter(""), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, invalid) {
  // Parse:
  EXPECT_THROW(parseJoda("", ""), VeloxUserError);
  EXPECT_THROW(parseJoda(" ", ""), VeloxUserError);
  EXPECT_THROW(parseJoda("", " "), VeloxUserError);
  EXPECT_THROW(parseJoda("", "Y '"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseJodaEra) {
  // Normal era cases
  EXPECT_EQ(
      util::fromTimestampString("-100-01-01"),
      parseJoda("BC 101", "G Y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("101-01-01"),
      parseJoda("AD 101", "G Y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-100-01-01"),
      parseJoda("bc 101", "G Y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("101-01-01"),
      parseJoda("ad 101", "G Y").timestamp);

  // Era specifier with 'y' specifier
  EXPECT_EQ(
      util::fromTimestampString("101-01-01"),
      parseJoda("BC 101", "G y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2012-01-01"),
      parseJoda("BC 2012", "G y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-101-01-01"),
      parseJoda("AD 2012 -101", "G Y y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2012-01-01"),
      parseJoda("BC 101 2012", "G Y y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-2011-01-01"),
      parseJoda("BC 2000 2012", "G y Y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-2011-01-01"),
      parseJoda("BC 2000 2012 BC", "G y Y G").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-2014-01-01"),
      parseJoda("BC 1 BC 2015", "G y G Y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2015-01-01"),
      parseJoda("BC 0 BC 2015 AD", "G y G Y G").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2015-01-01"),
      parseJoda("AD 0 AD 2015", "G y G Y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-2011-01-01"),
      parseJoda("BC 0 BC 2015 2 2012 BC", "G y G Y y Y G").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2012-01-01"),
      parseJoda("AD 0 AD 2015 2 2012 AD", "G y G Y y Y G").timestamp);

  // Invalid cases
  EXPECT_THROW(parseJoda("FG", "G"), VeloxUserError);
  EXPECT_THROW(parseJoda("AC", "G"), VeloxUserError);
  EXPECT_THROW(parseJoda("BD", "G"), VeloxUserError);
  EXPECT_THROW(parseJoda("aD", "G"), VeloxUserError);
  EXPECT_THROW(parseJoda("Ad", "G"), VeloxUserError);
  EXPECT_THROW(parseJoda("bC", "G"), VeloxUserError);
  EXPECT_THROW(parseJoda("Bc", "G"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseYearOfEra) {
  // By the default, assume epoch.
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01"), parseJoda(" ", " ").timestamp);

  // Number of times the token is repeated doesn't change the parsing behavior.
  EXPECT_EQ(
      util::fromTimestampString("2134-01-01"),
      parseJoda("2134", "Y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2134-01-01"),
      parseJoda("2134", "YYYYYYYY").timestamp);

  // Probe the year of era range. Joda only supports positive years.
  EXPECT_EQ(
      util::fromTimestampString("294247-01-01"),
      parseJoda("294247", "Y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0001-01-01"), parseJoda("1", "Y").timestamp);
  EXPECT_THROW(parseJoda("292278994", "Y"), VeloxUserError);
  EXPECT_THROW(parseJoda("0", "Y"), VeloxUserError);
  EXPECT_THROW(parseJoda("-1", "Y"), VeloxUserError);
  EXPECT_THROW(parseJoda("  ", " Y "), VeloxUserError);
  EXPECT_THROW(parseJoda(" 1 2", "Y Y"), VeloxUserError);

  // 2 'Y' token case
  EXPECT_EQ(
      util::fromTimestampString("2012-01-01"), parseJoda("12", "YY").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2069-01-01"), parseJoda("69", "YY").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01"), parseJoda("70", "YY").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1999-01-01"), parseJoda("99", "YY").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0002-01-01"), parseJoda("2", "YY").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0210-01-01"),
      parseJoda("210", "YY").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0001-01-01"), parseJoda("1", "YY").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2001-01-01"), parseJoda("01", "YY").timestamp);

  // Last token read overwrites:
  EXPECT_EQ(
      util::fromTimestampString("0005-01-01"),
      parseJoda("1 2 3 4 5", "Y Y Y Y Y").timestamp);

  // Throws on consumption of plus sign
  EXPECT_THROW(parseJoda("+100", "Y"), VeloxUserError);
}

// Same semantic as YEAR_OF_ERA, except that it accepts zero and negative years.
TEST_F(JodaDateTimeFormatterTest, parseYear) {
  EXPECT_EQ(
      util::fromTimestampString("123-01-01"), parseJoda("123", "y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("321-01-01"),
      parseJoda("321", "yyyyyyyy").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("0-01-01"), parseJoda("0", "y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1-01-01"), parseJoda("-1", "y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1234-01-01"),
      parseJoda("-1234", "y").timestamp);

  // Last token read overwrites:
  EXPECT_EQ(
      util::fromTimestampString("0-01-01"),
      parseJoda("123 0", "Y y").timestamp);

  // 2 'y' token case
  EXPECT_EQ(
      util::fromTimestampString("2012-01-01"), parseJoda("12", "yy").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2069-01-01"), parseJoda("69", "yy").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01"), parseJoda("70", "yy").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1999-01-01"), parseJoda("99", "yy").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0002-01-01"), parseJoda("2", "yy").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0210-01-01"),
      parseJoda("210", "yy").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0001-01-01"), parseJoda("1", "yy").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2001-01-01"), parseJoda("01", "yy").timestamp);

  // Plus sign consumption valid when y operator is not followed by another
  // specifier
  EXPECT_EQ(
      util::fromTimestampString("10-01-01"), parseJoda("+10", "y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("99-02-01"),
      parseJoda("+99 02", "y M").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("10-10-01"),
      parseJoda("10 +10", "M y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("100-02-01"),
      parseJoda("2+100", "My").timestamp);
  EXPECT_THROW(parseJoda("+10001", "yM"), VeloxUserError);
  EXPECT_THROW(parseJoda("++100", "y"), VeloxUserError);

  // Probe the year range
  EXPECT_THROW(parseJoda("-292275056", "y"), VeloxUserError);
  EXPECT_THROW(parseJoda("292278995", "y"), VeloxUserError);
  EXPECT_EQ(
      util::fromTimestampString("292278994-01-01"),
      parseJoda("292278994", "y").timestamp);
}

TEST_F(JodaDateTimeFormatterTest, parseWeekYear) {
  // Covers entire range of possible week year start dates (12-29 to 01-04)
  EXPECT_EQ(
      util::fromTimestampString("1969-12-29 00:00:00"),
      parseJoda("1970", "x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2024-12-30 00:00:00"),
      parseJoda("2025", "x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1934-12-31 00:00:00"),
      parseJoda("1935", "x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1990-01-01 00:00:00"),
      parseJoda("1990", "x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0204-01-02 00:00:00"),
      parseJoda("204", "x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-0102-01-03 00:00:00"),
      parseJoda("-102", "x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-0108-01-04 00:00:00"),
      parseJoda("-108", "x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1002-12-31 00:00:00"),
      parseJoda("-1001", "x").timestamp);

  // 2 'x' token case
  EXPECT_EQ(
      util::fromTimestampString("2012-01-02"), parseJoda("12", "xx").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2068-12-31"), parseJoda("69", "xx").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1969-12-29"), parseJoda("70", "xx").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1999-01-04"), parseJoda("99", "xx").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0001-12-31"), parseJoda("2", "xx").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0210-01-01"),
      parseJoda("210", "xx").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0001-01-01"), parseJoda("1", "xx").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2001-01-01"), parseJoda("01", "xx").timestamp);

  // Plus sign consumption valid when x operator is not followed by another
  // specifier
  EXPECT_EQ(
      util::fromTimestampString("10-01-04"), parseJoda("+10", "x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0098-12-29"),
      parseJoda("+99 01", "x w").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0099-01-05"),
      parseJoda("+99 02", "x w").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("10-03-08"),
      parseJoda("10 +10", "w x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("100-01-11"),
      parseJoda("2+100", "wx").timestamp);
  EXPECT_THROW(parseJoda("+10001", "xM"), VeloxUserError);
  EXPECT_THROW(parseJoda("++100", "x"), VeloxUserError);

  // Probe week year range
  EXPECT_THROW(parseJoda("-292275055", "x"), VeloxUserError);
  EXPECT_THROW(parseJoda("292278994", "x"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseCenturyOfEra) {
  // Probe century range
  EXPECT_EQ(
      util::fromTimestampString("292278900-01-01 00:00:00"),
      parseJoda("2922789", "CCCCCCC").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("00-01-01 00:00:00"),
      parseJoda("0", "C").timestamp);

  // Invalid century values
  EXPECT_THROW(parseJoda("-1", "CCCCCCC"), VeloxUserError);
  EXPECT_THROW(parseJoda("2922790", "CCCCCCC"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseJodaMonth) {
  // Joda has this weird behavior where if minute or hour is specified, year
  // falls back to 2000, instead of epoch (1970)  ¯\_(ツ)_/¯
  EXPECT_EQ(
      util::fromTimestampString("2000-01-01"), parseJoda("1", "M").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-07-01"),
      parseJoda(" 7", " MM").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-11-01"),
      parseJoda("11-", "M-").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-12-01"),
      parseJoda("-12-", "-M-").timestamp);

  EXPECT_THROW(parseJoda("0", "M"), VeloxUserError);
  EXPECT_THROW(parseJoda("13", "M"), VeloxUserError);
  EXPECT_THROW(parseJoda("12345", "M"), VeloxUserError);

  // Ensure MMM and MMMM specifiers consume both short- and long-form month
  // names
  for (int i = 0; i < 12; i++) {
    StringView buildString("2000-" + std::to_string(i + 1) + "-01");
    EXPECT_EQ(
        util::fromTimestampString(buildString),
        parseJoda(monthsShort[i], "MMM").timestamp);
    EXPECT_EQ(
        util::fromTimestampString(buildString),
        parseJoda(monthsFull[i], "MMM").timestamp);
    EXPECT_EQ(
        util::fromTimestampString(buildString),
        parseJoda(monthsShort[i], "MMMM").timestamp);
    EXPECT_EQ(
        util::fromTimestampString(buildString),
        parseJoda(monthsFull[i], "MMMM").timestamp);
  }

  // Month name invalid parse
  EXPECT_THROW(parseJoda("Decembr", "MMM"), VeloxUserError);
  EXPECT_THROW(parseJoda("Decembr", "MMMM"), VeloxUserError);
  EXPECT_THROW(parseJoda("Decemberary", "MMM"), VeloxUserError);
  EXPECT_THROW(parseJoda("Decemberary", "MMMM"), VeloxUserError);
  EXPECT_THROW(parseJoda("asdf", "MMM"), VeloxUserError);
  EXPECT_THROW(parseJoda("asdf", "MMMM"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseDayOfMonth) {
  EXPECT_EQ(
      util::fromTimestampString("2000-01-01"), parseJoda("1", "d").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-07"),
      parseJoda("7 ", "dd ").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-11"),
      parseJoda("/11", "/dd").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-31"),
      parseJoda("/31/", "/d/").timestamp);

  EXPECT_THROW(parseJoda("0", "d"), VeloxUserError);
  EXPECT_THROW(parseJoda("32", "d"), VeloxUserError);
  EXPECT_THROW(parseJoda("12345", "d"), VeloxUserError);

  EXPECT_THROW(parseJoda("02-31", "M-d"), VeloxUserError);
  EXPECT_THROW(parseJoda("04-31", "M-d"), VeloxUserError);

  // Ensure all days of month are checked against final selected month
  EXPECT_THROW(parseJoda("1 31 20 2", "M d d M"), VeloxUserError);
  EXPECT_THROW(parseJoda("2 31 20 4", "M d d M"), VeloxUserError);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-31"),
      parseJoda("2 31 1", "M d M").timestamp);

  // Probe around leap year.
  EXPECT_EQ(
      util::fromTimestampString("2000-02-29"),
      parseJoda("2000-02-29", "Y-M-d").timestamp);
  EXPECT_THROW(parseJoda("2001-02-29", "Y-M-d"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseDayOfYear) {
  // Just day of year specifier should default to 2000. Also covers leap year
  // case
  EXPECT_EQ(
      util::fromTimestampString("2000-01-01"), parseJoda("1", "D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-07"),
      parseJoda("7 ", "DD ").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-11"),
      parseJoda("/11", "/DD").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-31"),
      parseJoda("/31/", "/DDD/").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-02-01"), parseJoda("32", "D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-02-29"), parseJoda("60", "D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-12-30"), parseJoda("365", "D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-12-31"), parseJoda("366", "D").timestamp);

  // Year specified cases
  EXPECT_EQ(
      util::fromTimestampString("1950-01-01"),
      parseJoda("1950 1", "y D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1950-01-07"),
      parseJoda("1950 7 ", "y DD ").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1950-01-11"),
      parseJoda("1950 /11", "y /DD").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1950-01-31"),
      parseJoda("1950 /31/", "y /DDD/").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1950-02-01"),
      parseJoda("1950 32", "y D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1950-03-01"),
      parseJoda("1950 60", "y D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1950-12-31"),
      parseJoda("1950 365", "y D").timestamp);
  EXPECT_THROW(parseJoda("1950 366", "Y D"), VeloxUserError);

  // Negative year specified cases
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-01"),
      parseJoda("-1950 1", "y D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-07"),
      parseJoda("-1950 7 ", "y DD ").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-11"),
      parseJoda("-1950 /11", "y /DD").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-31"),
      parseJoda("-1950 /31/", "y /DDD/").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1950-02-01"),
      parseJoda("-1950 32", "y D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1950-03-01"),
      parseJoda("-1950 60", "y D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1950-12-31"),
      parseJoda("-1950 365", "y D").timestamp);
  EXPECT_THROW(parseJoda("-1950 366", "Y D"), VeloxUserError);

  // Ensure all days of year are checked against final selected year
  EXPECT_THROW(parseJoda("2000 366 2001", "y D y"), VeloxUserError);
  EXPECT_EQ(
      util::fromTimestampString("2000-12-31"),
      parseJoda("2001 366 2000", "y D y").timestamp);

  EXPECT_THROW(parseJoda("0", "d"), VeloxUserError);
  EXPECT_THROW(parseJoda("367", "d"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseHourOfDay) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseJoda("7", "H").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 23:00:00"),
      parseJoda("23", "HH").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseJoda("0", "HHH").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"),
      parseJoda("10", "HHHHHHHH").timestamp);

  // Hour of day invalid
  EXPECT_THROW(parseJoda("24", "H"), VeloxUserError);
  EXPECT_THROW(parseJoda("-1", "H"), VeloxUserError);
  EXPECT_THROW(parseJoda("123456789", "H"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseClockHourOfDay) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseJoda("7", "k").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseJoda("24", "kk").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"),
      parseJoda("1", "kkk").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"),
      parseJoda("10", "kkkkkkkk").timestamp);

  // Clock hour of day invalid
  EXPECT_THROW(parseJoda("25", "k"), VeloxUserError);
  EXPECT_THROW(parseJoda("0", "k"), VeloxUserError);
  EXPECT_THROW(parseJoda("123456789", "k"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseHourOfHalfDay) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseJoda("7", "K").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 11:00:00"),
      parseJoda("11", "KK").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseJoda("0", "KKK").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"),
      parseJoda("10", "KKKKKKKK").timestamp);

  // Hour of half day invalid
  EXPECT_THROW(parseJoda("12", "K"), VeloxUserError);
  EXPECT_THROW(parseJoda("-1", "K"), VeloxUserError);
  EXPECT_THROW(parseJoda("123456789", "K"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseClockHourOfHalfDay) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseJoda("7", "h").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseJoda("12", "hh").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"),
      parseJoda("1", "hhh").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"),
      parseJoda("10", "hhhhhhhh").timestamp);

  // Clock hour of half day invalid
  EXPECT_THROW(parseJoda("13", "h"), VeloxUserError);
  EXPECT_THROW(parseJoda("0", "h"), VeloxUserError);
  EXPECT_THROW(parseJoda("123456789", "h"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseHalfOfDay) {
  // Half of day has no effect if hour or clockhour of day is provided
  // hour of day tests
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseJoda("7 PM", "H a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseJoda("7 AM", "H a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseJoda("7 pm", "H a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseJoda("7 am", "H a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseJoda("0 PM", "H a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseJoda("0 AM", "H a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseJoda("0 pm", "H a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseJoda("0 am", "H a").timestamp);

  // clock hour of day tests
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseJoda("7 PM", "k a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseJoda("7 AM", "k a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseJoda("7 pm", "k a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseJoda("7 am", "k a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseJoda("24 PM", "k a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseJoda("24 AM", "k a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseJoda("24 pm", "k a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseJoda("24 am", "k a").timestamp);

  // Half of day has effect if hour or clockhour of halfday is provided
  // hour of halfday tests
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00"),
      parseJoda("0 PM", "K a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseJoda("0 AM", "K a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 18:00:00"),
      parseJoda("6 PM", "K a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 06:00:00"),
      parseJoda("6 AM", "K a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 23:00:00"),
      parseJoda("11 PM", "K a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 11:00:00"),
      parseJoda("11 AM", "K a").timestamp);

  // clockhour of halfday tests
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00"),
      parseJoda("1 PM", "h a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"),
      parseJoda("1 AM", "h a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 18:00:00"),
      parseJoda("6 PM", "h a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 06:00:00"),
      parseJoda("6 AM", "h a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00"),
      parseJoda("12 PM", "h a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseJoda("12 AM", "h a").timestamp);

  // time gives precendent to most recent time specifier
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"),
      parseJoda("0 1 AM", "H h a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00"),
      parseJoda("12 1 PM", "H h a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseJoda("1 AM 0", "h a H").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00"),
      parseJoda("1 AM 12", "h a H").timestamp);
}

TEST_F(JodaDateTimeFormatterTest, parseMinute) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:08:00"),
      parseJoda("8", "m").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:59:00"),
      parseJoda("59", "mm").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseJoda("0/", "mmm/").timestamp);

  EXPECT_THROW(parseJoda("60", "m"), VeloxUserError);
  EXPECT_THROW(parseJoda("-1", "m"), VeloxUserError);
  EXPECT_THROW(parseJoda("123456789", "m"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseSecond) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:09"),
      parseJoda("9", "s").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:58"),
      parseJoda("58", "ss").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseJoda("0/", "s/").timestamp);

  EXPECT_THROW(parseJoda("60", "s"), VeloxUserError);
  EXPECT_THROW(parseJoda("-1", "s"), VeloxUserError);
  EXPECT_THROW(parseJoda("123456789", "s"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseTimezone) {
  // Broken timezone offfsets; allowed formats are either "+00:00" or "+00".
  EXPECT_THROW(parseJoda("", "ZZ"), VeloxUserError);
  EXPECT_THROW(parseJoda("0", "ZZ"), VeloxUserError);
  EXPECT_THROW(parseJoda("00", "ZZ"), VeloxUserError);
  EXPECT_THROW(parseJoda(":00", "ZZ"), VeloxUserError);
  EXPECT_THROW(parseJoda("+0", "ZZ"), VeloxUserError);
  EXPECT_THROW(parseJoda("+00:", "ZZ"), VeloxUserError);
  EXPECT_THROW(parseJoda("+00:0", "ZZ"), VeloxUserError);
  EXPECT_THROW(parseJoda("12", "YYZZ"), VeloxUserError);
  EXPECT_THROW(parseJoda("ZZ", "Z"), VeloxUserError);
  EXPECT_THROW(parseJoda("ZZ", "ZZ"), VeloxUserError);

  // GMT
  EXPECT_EQ("+00:00", parseTZ("+00:00", "ZZ"));
  EXPECT_EQ("+00:00", parseTZ("-00:00", "ZZ"));
  EXPECT_EQ("+00:00", parseTZ("Z", "Z"));
  EXPECT_EQ("+00:00", parseTZ("Z", "ZZ"));

  // Valid long format:
  EXPECT_EQ("+00:01", parseTZ("+00:01", "ZZ"));
  EXPECT_EQ("-00:01", parseTZ("-00:01", "ZZ"));
  EXPECT_EQ("+01:00", parseTZ("+01:00", "ZZ"));
  EXPECT_EQ("+13:59", parseTZ("+13:59", "ZZ"));
  EXPECT_EQ("-07:32", parseTZ("-07:32", "ZZ"));
  EXPECT_EQ("+14:00", parseTZ("+14:00", "ZZ"));
  EXPECT_EQ("-14:00", parseTZ("-14:00", "ZZ"));

  // Valid long format without colon:
  EXPECT_EQ("+00:01", parseTZ("+0001", "ZZ"));
  EXPECT_EQ("+11:00", parseTZ("+1100", "ZZ"));
  EXPECT_EQ("-04:30", parseTZ("-0430", "ZZ"));

  // Invalid long format:
  EXPECT_THROW(parseTZ("+14:01", "ZZ"), VeloxUserError);
  EXPECT_THROW(parseTZ("-14:01", "ZZ"), VeloxUserError);
  EXPECT_THROW(parseTZ("+00:60", "ZZ"), VeloxUserError);
  EXPECT_THROW(parseTZ("+00:99", "ZZ"), VeloxUserError);
  EXPECT_THROW(parseTZ("+00:100", "ZZ"), VeloxUserError);
  EXPECT_THROW(parseTZ("+15:00", "ZZ"), VeloxUserError);
  EXPECT_THROW(parseTZ("+16:00", "ZZ"), VeloxUserError);
  EXPECT_THROW(parseTZ("-15:00", "ZZ"), VeloxUserError);

  // GMT short format:
  EXPECT_EQ("+00:00", parseTZ("+00", "ZZ"));
  EXPECT_EQ("+00:00", parseTZ("-00", "ZZ"));

  // Valid short format:
  EXPECT_EQ("+13:00", parseTZ("+13", "ZZ"));
  EXPECT_EQ("-01:00", parseTZ("-01", "ZZ"));
  EXPECT_EQ("-10:00", parseTZ("-10", "ZZ"));
  EXPECT_EQ("+03:00", parseTZ("+03", "ZZ"));
  EXPECT_EQ("-14:00", parseTZ("-14", "ZZ"));
  EXPECT_EQ("+14:00", parseTZ("+14", "ZZ"));

  // Invalid short format:
  EXPECT_THROW(parseTZ("-15", "ZZ"), VeloxUserError);
  EXPECT_THROW(parseTZ("+15", "ZZ"), VeloxUserError);
  EXPECT_THROW(parseTZ("+16", "ZZ"), VeloxUserError);
  EXPECT_THROW(parseTZ("-16", "ZZ"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseMixedYMDFormat) {
  // Common patterns found.
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 23:00:00"),
      parseJoda("2021-01-04+23:00", "YYYY-MM-dd+HH:mm").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2019-07-03 11:04:10"),
      parseJoda("2019-07-03 11:04:10", "YYYY-MM-dd HH:mm:ss").timestamp);

  // Backwards, just for fun:
  EXPECT_EQ(
      util::fromTimestampString("2019-07-03 11:04:10"),
      parseJoda("10:04:11 03-07-2019", "ss:mm:HH dd-MM-YYYY").timestamp);

  // Include timezone.
  auto result = parseJoda("2021-11-05+01:00+09:00", "YYYY-MM-dd+HH:mmZZ");
  EXPECT_EQ(util::fromTimestampString("2021-11-05 01:00:00"), result.timestamp);
  EXPECT_EQ("+09:00", util::getTimeZoneName(result.timezoneId));

  // Timezone offset in -hh:mm format.
  result = parseJoda("-07:232021-11-05+01:00", "ZZYYYY-MM-dd+HH:mm");
  EXPECT_EQ(util::fromTimestampString("2021-11-05 01:00:00"), result.timestamp);
  EXPECT_EQ("-07:23", util::getTimeZoneName(result.timezoneId));

  // Timezone offset in +hhmm format.
  result = parseJoda("+01332022-03-08+13:00", "ZZYYYY-MM-dd+HH:mm");
  EXPECT_EQ(util::fromTimestampString("2022-03-08 13:00:00"), result.timestamp);
  EXPECT_EQ("+01:33", util::getTimeZoneName(result.timezoneId));

  // Z in the input means GMT in Joda.
  EXPECT_EQ(
      util::fromTimestampString("2022-07-29 20:03:54.667"),
      parseJoda("2022-07-29T20:03:54.667Z", "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
          .timestamp);
}

TEST_F(JodaDateTimeFormatterTest, parseMixedWeekFormat) {
  // Common patterns found.
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parseJoda("2021 1 1 13:29:21.213", "x w e HH:mm:ss.SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parseJoda("2021 22 1 13:29:21.213", "x w e HH:mm:ss.SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parseJoda("2021 22 4 13:29:21.213", "x w e HH:mm:ss.SSS").timestamp);

  // Day of week short text normal capitlization
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parseJoda("2021 1 Mon 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parseJoda("2021 22 Mon 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parseJoda("2021 22 Thu 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  // Day of week long text normal capitlization
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parseJoda("2021 1 Monday 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parseJoda("2021 22 Monday 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parseJoda("2021 22 Thursday 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  // Day of week short text upper case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parseJoda("2021 1 MON 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parseJoda("2021 22 MON 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parseJoda("2021 22 THU 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  // Day of week long text upper case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parseJoda("2021 1 MONDAY 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parseJoda("2021 22 MONDAY 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parseJoda("2021 22 THURSDAY 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  // Day of week short text lower case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parseJoda("2021 1 mon 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parseJoda("2021 22 mon 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parseJoda("2021 22 thu 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  // Day of week long text lower case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parseJoda("2021 1 monday 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parseJoda("2021 22 monday 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parseJoda("2021 22 thursday 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  // Invalid day of week throw cases
  EXPECT_THROW(parseJoda("mOn", "E"), VeloxUserError);
  EXPECT_THROW(parseJoda("tuE", "E"), VeloxUserError);
  EXPECT_THROW(parseJoda("WeD", "E"), VeloxUserError);
  EXPECT_THROW(parseJoda("WEd", "E"), VeloxUserError);
  EXPECT_THROW(parseJoda("MONday", "EEE"), VeloxUserError);
  EXPECT_THROW(parseJoda("monDAY", "EEE"), VeloxUserError);
  EXPECT_THROW(parseJoda("frIday", "EEE"), VeloxUserError);

  // Backwards, just for fun:
  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parseJoda("213.21:29:13 1 22 2021", "SSS.ss:mm:HH e w x").timestamp);

  // Include timezone.
  auto result =
      parseJoda("2021 22 1 13:29:21.213+09:00", "x w e HH:mm:ss.SSSZZ");
  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"), result.timestamp);
  EXPECT_EQ("+09:00", util::getTimeZoneName(result.timezoneId));

  // Timezone offset in -hh:mm format.
  result = parseJoda("-07:232021 22 1 13:29:21.213", "ZZx w e HH:mm:ss.SSS");
  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"), result.timestamp);
  EXPECT_EQ("-07:23", util::getTimeZoneName(result.timezoneId));

  // Timezone offset in +hhmm format.
  result = parseJoda("+01332021 22 1 13:29:21.213", "ZZx w e HH:mm:ss.SSS");
  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"), result.timestamp);
  EXPECT_EQ("+01:33", util::getTimeZoneName(result.timezoneId));
}

TEST_F(JodaDateTimeFormatterTest, parseFractionOfSecond) {
  // Valid milliseconds and timezone with positive offset.
  auto result =
      parseJoda("2022-02-23T12:15:00.364+04:00", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
  EXPECT_EQ(
      util::fromTimestampString("2022-02-23 12:15:00.364"), result.timestamp);
  EXPECT_EQ("+04:00", util::getTimeZoneName(result.timezoneId));

  // Valid milliseconds and timezone with negative offset.
  result =
      parseJoda("2022-02-23T12:15:00.776-14:00", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
  EXPECT_EQ(
      util::fromTimestampString("2022-02-23 12:15:00.776"), result.timestamp);
  EXPECT_EQ("-14:00", util::getTimeZoneName(result.timezoneId));

  // Valid milliseconds.
  EXPECT_EQ(
      util::fromTimestampString("2022-02-24 02:19:33.283"),
      parseJoda("2022-02-24 02:19:33.283", "yyyy-MM-dd HH:mm:ss.SSS")
          .timestamp);

  // Test without milliseconds.
  EXPECT_EQ(
      util::fromTimestampString("2022-02-23 20:30:00"),
      parseJoda("2022-02-23T20:30:00", "yyyy-MM-dd'T'HH:mm:ss").timestamp);

  // Assert on difference in milliseconds.
  EXPECT_NE(
      util::fromTimestampString("2022-02-23 12:15:00.223"),
      parseJoda("2022-02-23T12:15:00.776", "yyyy-MM-dd'T'HH:mm:ss.SSS")
          .timestamp);

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.000"),
      parseJoda("000", "SSS").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.001"),
      parseJoda("001", "SSS").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.999"),
      parseJoda("999", "SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.045"),
      parseJoda("045", "SSS").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.450"),
      parseJoda("45", "SS").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.450"),
      parseJoda("45", "SSSS").timestamp);

  EXPECT_THROW(parseJoda("-1", "S"), VeloxUserError);
  EXPECT_THROW(parseJoda("999", "S"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseConsecutiveSpecifiers) {
  EXPECT_EQ(
      util::fromTimestampString("2012-12-01"),
      parseJoda("1212", "YYM").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0012-12-01"),
      parseJoda("1212", "MY").timestamp);
  EXPECT_THROW(parseJoda("1212", "YM"), VeloxUserError);

  EXPECT_EQ(
      util::fromTimestampString("2012-01-01 12:00:00"),
      parseJoda("1212", "YYH").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0012-01-01 12:00:00"),
      parseJoda("1212", "HY").timestamp);
  EXPECT_THROW(parseJoda("1212", "YH"), VeloxUserError);

  EXPECT_EQ(
      util::fromTimestampString("2012-01-01 12:00:00"),
      parseJoda("1212", "yyH").timestamp);
  EXPECT_THROW(parseJoda("12312", "yyH"), VeloxUserError);
}

class MysqlDateTimeTest : public DateTimeFormatterTest {};

TEST_F(MysqlDateTimeTest, validBuild) {
  std::vector<DateTimeToken> expected;

  expected = {DateTimeToken(" ")};
  EXPECT_EQ(expected, buildMysqlDateTimeFormatter(" ")->tokens());

  expected = {
      DateTimeToken(" "),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 4}),
  };
  EXPECT_EQ(expected, buildMysqlDateTimeFormatter(" %Y")->tokens());

  expected = {
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 2}),
      DateTimeToken(" "),
  };
  EXPECT_EQ(expected, buildMysqlDateTimeFormatter("%y ")->tokens());

  expected = {DateTimeToken(" 132&2618*673 *--+= }{[]\\:")};
  EXPECT_EQ(
      expected,
      buildMysqlDateTimeFormatter(" 132&2618*673 *--+= }{[]\\:")->tokens());

  expected = {
      DateTimeToken("   "),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 4}),
      DateTimeToken(" &^  "),
  };
  EXPECT_EQ(expected, buildMysqlDateTimeFormatter("   %Y &^  ")->tokens());

  expected = {
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 2}),
      DateTimeToken("   & "),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 4}),
      DateTimeToken(" "),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 4}),
  };
  EXPECT_EQ(expected, buildMysqlDateTimeFormatter("%y  % & %Y %Y%")->tokens());

  expected = {
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 4}),
      DateTimeToken(" 'T'"),
  };
  EXPECT_EQ(expected, buildMysqlDateTimeFormatter("%Y 'T'")->tokens());

  expected = {DateTimeToken("1''2")};
  EXPECT_EQ(expected, buildMysqlDateTimeFormatter("1''2")->tokens());

  expected = {
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 4}),
      DateTimeToken("-"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::MONTH_OF_YEAR, 2}),
      DateTimeToken("-"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::DAY_OF_MONTH, 2}),
      DateTimeToken(" "),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::MINUTE_OF_HOUR, 2}),
      DateTimeToken(" "),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 2}),
  };
  EXPECT_EQ(expected, buildMysqlDateTimeFormatter("%Y-%m-%d %i %y")->tokens());
}

TEST_F(MysqlDateTimeTest, invalidBuild) {
  // Unsupported specifiers
  EXPECT_THROW(buildMysqlDateTimeFormatter("%D"), VeloxUserError);
  EXPECT_THROW(buildMysqlDateTimeFormatter("%U"), VeloxUserError);
  EXPECT_THROW(buildMysqlDateTimeFormatter("%u"), VeloxUserError);
  EXPECT_THROW(buildMysqlDateTimeFormatter("%V"), VeloxUserError);
  EXPECT_THROW(buildMysqlDateTimeFormatter("%w"), VeloxUserError);

  // Empty format string
  EXPECT_THROW(buildMysqlDateTimeFormatter(""), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, formatYear) {
  auto* timezone = date::locate_zone("GMT");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%Y")->format(
          util::fromTimestampString("0-01-01"), timezone),
      "0000");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%Y")->format(
          util::fromTimestampString("1-01-01"), timezone),
      "0001");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%Y")->format(
          util::fromTimestampString("199-01-01"), timezone),
      "0199");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%Y")->format(
          util::fromTimestampString("9999-01-01"), timezone),
      "9999");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%Y")->format(
          util::fromTimestampString("-1-01-01"), timezone),
      "-0001");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%Y")->format(
          util::fromTimestampString("19999-01-01"), timezone),
      "19999");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%Y")->format(
          util::fromTimestampString("-19999-01-01"), timezone),
      "-19999");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%Y")->format(
          util::fromTimestampString("-1-01-01"), timezone),
      "-0001");
  EXPECT_THROW(
      buildMysqlDateTimeFormatter("%Y")->format(
          util::fromTimestampString("-99999-01-01"), timezone),
      VeloxUserError);
  EXPECT_THROW(
      buildMysqlDateTimeFormatter("%Y")->format(
          util::fromTimestampString("99999-01-01"), timezone),
      VeloxUserError);
}

TEST_F(MysqlDateTimeTest, formatMonthDay) {
  auto* timezone = date::locate_zone("GMT");

  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%m~%d")->format(
          util::fromTimestampString("0-01-01"), timezone),
      "01~01");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%m~%d")->format(
          util::fromTimestampString("1-10-24"), timezone),
      "10~24");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%m~%d")->format(
          util::fromTimestampString("199-09-30"), timezone),
      "09~30");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%m~%d")->format(
          util::fromTimestampString("9999-01-01"), timezone),
      "01~01");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%m~%d")->format(
          util::fromTimestampString("-1-01-01"), timezone),
      "01~01");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%m~%d")->format(
          util::fromTimestampString("19999-01-01"), timezone),
      "01~01");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%m~%d")->format(
          util::fromTimestampString("-19999-01-01"), timezone),
      "01~01");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%m~%d")->format(
          util::fromTimestampString("-1-01-01"), timezone),
      "01~01");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%m~%d")->format(
          util::fromTimestampString("2000-02-29"), timezone),
      "02~29");
  EXPECT_THROW(
      buildMysqlDateTimeFormatter("%m~%d")->format(
          util::fromTimestampString("-1-13-01"), timezone),
      VeloxUserError);
  EXPECT_THROW(
      buildMysqlDateTimeFormatter("%m~%d")->format(
          util::fromTimestampString("1999-02-50"), timezone),
      VeloxUserError);
  EXPECT_THROW(
      buildMysqlDateTimeFormatter("%m~%d")->format(
          util::fromTimestampString("1999-02-29"), timezone),
      VeloxUserError);
}

TEST_F(MysqlDateTimeTest, formatWeekday) {
  auto* timezone = date::locate_zone("GMT");

  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%a..->%W")
          ->format(util::fromTimestampString("1999-01-04"), timezone),
      "Mon..->Monday");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%a..->%W")
          ->format(util::fromTimestampString("1999-01-05"), timezone),
      "Tue..->Tuesday");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%a..->%W")
          ->format(util::fromTimestampString("1999-01-06"), timezone),
      "Wed..->Wednesday");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%a..->%W")
          ->format(util::fromTimestampString("1999-01-07"), timezone),
      "Thu..->Thursday");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%a..->%W")
          ->format(util::fromTimestampString("1999-01-08"), timezone),
      "Fri..->Friday");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%a..->%W")
          ->format(util::fromTimestampString("1999-01-09"), timezone),
      "Sat..->Saturday");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%a..->%W")
          ->format(util::fromTimestampString("1999-01-10"), timezone),
      "Sun..->Sunday");
}

TEST_F(MysqlDateTimeTest, formatMonth) {
  auto* timezone = date::locate_zone("GMT");

  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%c-%m-%M")
          ->format(util::fromTimestampString("1999-01-04"), timezone),
      "1-01-January");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%c-%m-%M")
          ->format(util::fromTimestampString("1999-02-04"), timezone),
      "2-02-February");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%c-%m-%M")
          ->format(util::fromTimestampString("1999-03-04"), timezone),
      "3-03-March");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%c-%m-%M")
          ->format(util::fromTimestampString("1999-04-04"), timezone),
      "4-04-April");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%c-%m-%M")
          ->format(util::fromTimestampString("1999-05-04"), timezone),
      "5-05-May");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%c-%m-%M")
          ->format(util::fromTimestampString("1999-06-04"), timezone),
      "6-06-June");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%c-%m-%M")
          ->format(util::fromTimestampString("1999-07-04"), timezone),
      "7-07-July");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%c-%m-%M")
          ->format(util::fromTimestampString("1999-08-04"), timezone),
      "8-08-August");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%c-%m-%M")
          ->format(util::fromTimestampString("1999-09-04"), timezone),
      "9-09-September");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%c-%m-%M")
          ->format(util::fromTimestampString("1999-10-04"), timezone),
      "10-10-October");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%c-%m-%M")
          ->format(util::fromTimestampString("1999-11-04"), timezone),
      "11-11-November");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%c-%m-%M")
          ->format(util::fromTimestampString("1999-12-04"), timezone),
      "12-12-December");
}

TEST_F(MysqlDateTimeTest, formatDayOfMonth) {
  auto* timezone = date::locate_zone("GMT");

  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%d-%e")->format(
          util::fromTimestampString("2000-02-01"), timezone),
      "01-1");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%d-%e")->format(
          util::fromTimestampString("2000-02-29"), timezone),
      "29-29");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%d-%e")->format(
          util::fromTimestampString("2000-12-31"), timezone),
      "31-31");
}

TEST_F(MysqlDateTimeTest, formatFractionOfSecond) {
  auto* timezone = date::locate_zone("GMT");

  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%f")->format(
          util::fromTimestampString("2000-02-01 00:00:00.987"), timezone),
      "987000");

  // As our current precision is 3 decimal places.
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%f")->format(
          util::fromTimestampString("2000-02-01 00:00:00.987654"), timezone),
      "987000");

  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%f")->format(
          util::fromTimestampString("2000-02-01 00:00:00.900654"), timezone),
      "900000");

  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%f")->format(
          util::fromTimestampString("2000-02-01 00:00:00.090654"), timezone),
      "090000");

  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%f")->format(
          util::fromTimestampString("2000-02-01 00:00:00.009654"), timezone),
      "009000");

  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%f")->format(
          util::fromTimestampString("2000-02-01 00:00:00.000654"), timezone),
      "000000");
}

TEST_F(MysqlDateTimeTest, formatHour) {
  auto* timezone = date::locate_zone("GMT");

  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%h--%H--%I--%k--%l")
          ->format(util::fromTimestampString("2000-02-01 00:00:00"), timezone),
      "12--00--12--0--12");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%h--%H--%I--%k--%l")
          ->format(util::fromTimestampString("2000-02-01 12:12:01"), timezone),
      "12--12--12--12--12");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%h--%H--%I--%k--%l")
          ->format(util::fromTimestampString("2000-02-01 23:23:01"), timezone),
      "11--23--11--23--11");
}

TEST_F(MysqlDateTimeTest, formatMinute) {
  auto* timezone = date::locate_zone("GMT");

  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%i")->format(
          util::fromTimestampString("2000-02-01 00:00:00"), timezone),
      "00");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%i")->format(
          util::fromTimestampString("2000-02-01 00:09:00"), timezone),
      "09");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%i")->format(
          util::fromTimestampString("2000-02-01 00:31:00"), timezone),
      "31");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%i")->format(
          util::fromTimestampString("2000-02-01 00:59:00"), timezone),
      "59");
}

TEST_F(MysqlDateTimeTest, formatDayOfYear) {
  auto* timezone = date::locate_zone("GMT");

  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%j")->format(
          util::fromTimestampString("2000-01-01 00:00:00"), timezone),
      "001");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%j")->format(
          util::fromTimestampString("2000-12-31 00:09:00"), timezone),
      "366");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%j")->format(
          util::fromTimestampString("1999-12-31 00:31:00"), timezone),
      "365");
}

TEST_F(MysqlDateTimeTest, formatAmPm) {
  auto* timezone = date::locate_zone("GMT");

  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%p")->format(
          util::fromTimestampString("2000-01-01 00:00:00"), timezone),
      "AM");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%p")->format(
          util::fromTimestampString("2000-01-01 11:59:59"), timezone),
      "AM");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%p")->format(
          util::fromTimestampString("2000-01-01 12:00:00"), timezone),
      "PM");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%p")->format(
          util::fromTimestampString("2000-01-01 23:59:59"), timezone),
      "PM");
}

TEST_F(MysqlDateTimeTest, formatSecond) {
  auto* timezone = date::locate_zone("GMT");

  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%s-%S")->format(
          util::fromTimestampString("2000-01-01 00:00:00"), timezone),
      "00-00");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%s-%S")->format(
          util::fromTimestampString("2000-01-01 00:00:30"), timezone),
      "30-30");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%s-%S")->format(
          util::fromTimestampString("2000-01-01 00:00:59"), timezone),
      "59-59");
}

TEST_F(MysqlDateTimeTest, formatCompositeTime) {
  auto* timezone = date::locate_zone("GMT");

  // 12 hour %r
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%r")->format(
          util::fromTimestampString("2000-01-01 00:00:00"), timezone),
      "12:00:00 AM");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%r")->format(
          util::fromTimestampString("2000-01-01 11:59:59"), timezone),
      "11:59:59 AM");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%r")->format(
          util::fromTimestampString("2000-01-01 12:00:00"), timezone),
      "12:00:00 PM");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%r")->format(
          util::fromTimestampString("2000-01-01 23:59:59"), timezone),
      "11:59:59 PM");

  // 24 hour %T
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%T")->format(
          util::fromTimestampString("2000-01-01 00:00:00"), timezone),
      "00:00:00");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%T")->format(
          util::fromTimestampString("2000-01-01 11:59:59"), timezone),
      "11:59:59");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%T")->format(
          util::fromTimestampString("2000-01-01 12:00:00"), timezone),
      "12:00:00");
  EXPECT_EQ(
      buildMysqlDateTimeFormatter("%T")->format(
          util::fromTimestampString("2000-01-01 23:59:59"), timezone),
      "23:59:59");
}

// Same semantic as YEAR_OF_ERA, except that it accepts zero and negative years.
TEST_F(MysqlDateTimeTest, parseFourDigitYear) {
  EXPECT_EQ(util::fromTimestampString("123-01-01"), parseMysql("123", "%Y"));
  EXPECT_EQ(util::fromTimestampString("321-01-01"), parseMysql("321", "%Y"));

  EXPECT_EQ(util::fromTimestampString("0-01-01"), parseMysql("0", "%Y"));
  EXPECT_EQ(util::fromTimestampString("-1-01-01"), parseMysql("-1", "%Y"));
  EXPECT_EQ(
      util::fromTimestampString("-1234-01-01"), parseMysql("-1234", "%Y"));

  // Last token read overwrites:
  EXPECT_EQ(util::fromTimestampString("0-01-01"), parseMysql("123 0", "%Y %Y"));

  // Plus sign consumption valid when %Y operator is not followed by another
  // specifier
  EXPECT_EQ(util::fromTimestampString("10-01-01"), parseMysql("+10", "%Y"));
  EXPECT_EQ(
      util::fromTimestampString("99-02-01"), parseMysql("+99 02", "%Y %m"));
  EXPECT_EQ(
      util::fromTimestampString("10-10-01"), parseMysql("10 +10", "%m %Y"));
  EXPECT_EQ(
      util::fromTimestampString("100-02-01"), parseMysql("2+100", "%m%Y"));
  EXPECT_THROW(parseMysql("+10001", "%Y%m"), VeloxUserError);
  EXPECT_THROW(parseMysql("++100", "%Y"), VeloxUserError);

  // Probe the year range
  EXPECT_THROW(parseMysql("-10000", "%Y"), VeloxUserError);
  EXPECT_THROW(parseMysql("10000", "%Y"), VeloxUserError);
  EXPECT_EQ(util::fromTimestampString("9999-01-01"), parseMysql("9999", "%Y"));
}

TEST_F(MysqlDateTimeTest, parseTwoDigitYear) {
  EXPECT_EQ(util::fromTimestampString("1970-01-01"), parseMysql("70", "%y"));
  EXPECT_EQ(util::fromTimestampString("2069-01-01"), parseMysql("69", "%y"));
  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parseMysql("00", "%y"));

  // Last token read overwrites:
  EXPECT_EQ(
      util::fromTimestampString("2030-01-01"), parseMysql("80 30", "%y %y"));
}

TEST_F(MysqlDateTimeTest, parseWeekYear) {
  // Covers entire range of possible week year start dates (12-29 to 01-04)
  EXPECT_EQ(
      util::fromTimestampString("1969-12-29 00:00:00"),
      parseMysql("1970", "%x"));
  EXPECT_EQ(
      util::fromTimestampString("2024-12-30 00:00:00"),
      parseMysql("2025", "%x"));
  EXPECT_EQ(
      util::fromTimestampString("1934-12-31 00:00:00"),
      parseMysql("1935", "%x"));
  EXPECT_EQ(
      util::fromTimestampString("1990-01-01 00:00:00"),
      parseMysql("1990", "%x"));
  EXPECT_EQ(
      util::fromTimestampString("0204-01-02 00:00:00"),
      parseMysql("204", "%x"));
  EXPECT_EQ(
      util::fromTimestampString("-0102-01-03 00:00:00"),
      parseMysql("-102", "%x"));
  EXPECT_EQ(
      util::fromTimestampString("-0108-01-04 00:00:00"),
      parseMysql("-108", "%x"));
  EXPECT_EQ(
      util::fromTimestampString("-1002-12-31 00:00:00"),
      parseMysql("-1001", "%x"));

  // Plus sign consumption valid when %x operator is not followed by another
  // specifier
  EXPECT_EQ(util::fromTimestampString("10-01-04"), parseMysql("+10", "%x"));
  EXPECT_EQ(
      util::fromTimestampString("0098-12-29"), parseMysql("+99 01", "%x %v"));
  EXPECT_EQ(
      util::fromTimestampString("0099-01-05"), parseMysql("+99 02", "%x %v"));
  EXPECT_EQ(
      util::fromTimestampString("10-03-08"), parseMysql("10 +10", "%v %x"));
  EXPECT_EQ(
      util::fromTimestampString("100-01-11"), parseMysql("2+100", "%v%x"));
  EXPECT_THROW(parseMysql("+10001", "%x%m"), VeloxUserError);
  EXPECT_THROW(parseMysql("++100", "%x"), VeloxUserError);

  // Probe week year range
  EXPECT_THROW(parseMysql("-292275055", "%x"), VeloxUserError);
  EXPECT_THROW(parseMysql("292278994", "%x"), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseMonth) {
  // Joda has this weird behavior where if minute or hour is specified, year
  // falls back to 2000, instead of epoch (1970)  ¯\_(ツ)_/¯
  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parseMysql("1", "%m"));
  EXPECT_EQ(util::fromTimestampString("2000-07-01"), parseMysql(" 7", " %m"));
  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parseMysql("01", "%m"));
  EXPECT_EQ(util::fromTimestampString("2000-07-01"), parseMysql(" 07", " %m"));
  EXPECT_EQ(util::fromTimestampString("2000-11-01"), parseMysql("11-", "%m-"));
  EXPECT_EQ(
      util::fromTimestampString("2000-12-01"), parseMysql("-12-", "-%m-"));

  EXPECT_THROW(parseMysql("0", "%m"), VeloxUserError);
  EXPECT_THROW(parseMysql("13", "%m"), VeloxUserError);
  EXPECT_THROW(parseMysql("12345", "%m"), VeloxUserError);

  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parseMysql("1", "%c"));
  EXPECT_EQ(util::fromTimestampString("2000-07-01"), parseMysql(" 7", " %c"));
  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parseMysql("01", "%c"));
  EXPECT_EQ(util::fromTimestampString("2000-07-01"), parseMysql(" 07", " %c"));
  EXPECT_EQ(util::fromTimestampString("2000-11-01"), parseMysql("11-", "%c-"));
  EXPECT_EQ(
      util::fromTimestampString("2000-12-01"), parseMysql("-12-", "-%c-"));

  EXPECT_THROW(parseMysql("0", "%c"), VeloxUserError);
  EXPECT_THROW(parseMysql("13", "%c"), VeloxUserError);
  EXPECT_THROW(parseMysql("12345", "%c"), VeloxUserError);

  // Ensure %b and %M specifiers consume both short- and long-form month
  // names
  for (int i = 0; i < 12; i++) {
    StringView buildString("2000-" + std::to_string(i + 1) + "-01");
    EXPECT_EQ(
        util::fromTimestampString(buildString),
        parseMysql(monthsShort[i], "%b"));
    EXPECT_EQ(
        util::fromTimestampString(buildString),
        parseMysql(monthsFull[i], "%b"));
    EXPECT_EQ(
        util::fromTimestampString(buildString),
        parseMysql(monthsShort[i], "%M"));
    EXPECT_EQ(
        util::fromTimestampString(buildString),
        parseMysql(monthsFull[i], "%M"));
  }

  // Month name invalid parse
  EXPECT_THROW(parseMysql("Decembr", "%b"), VeloxUserError);
  EXPECT_THROW(parseMysql("Decembr", "%M"), VeloxUserError);
  EXPECT_THROW(parseMysql("Decemberary", "%b"), VeloxUserError);
  EXPECT_THROW(parseMysql("Decemberary", "%M"), VeloxUserError);
  EXPECT_THROW(parseMysql("asdf", "%b"), VeloxUserError);
  EXPECT_THROW(parseMysql("asdf", "%M"), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseDayOfMonth) {
  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parseMysql("1", "%d"));
  EXPECT_EQ(util::fromTimestampString("2000-01-07"), parseMysql("7 ", "%d "));
  EXPECT_EQ(util::fromTimestampString("2000-01-11"), parseMysql("/11", "/%d"));
  EXPECT_EQ(
      util::fromTimestampString("2000-01-31"), parseMysql("/31/", "/%d/"));

  EXPECT_THROW(parseMysql("0", "%d"), VeloxUserError);
  EXPECT_THROW(parseMysql("32", "%d"), VeloxUserError);
  EXPECT_THROW(parseMysql("12345", "%d"), VeloxUserError);

  EXPECT_THROW(parseMysql("02-31", "%m-%d"), VeloxUserError);
  EXPECT_THROW(parseMysql("04-31", "%m-%d"), VeloxUserError);

  // Ensure all days of month are checked against final selected month
  EXPECT_THROW(parseMysql("1 31 20 2", "%m %d %d %m"), VeloxUserError);
  EXPECT_THROW(parseMysql("2 31 20 4", "%m %d %d %m"), VeloxUserError);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-31"),
      parseMysql("2 31 1", "%m %d %m"));

  // Probe around leap year.
  EXPECT_EQ(
      util::fromTimestampString("2000-02-29"),
      parseMysql("2000-02-29", "%Y-%m-%d"));
  EXPECT_THROW(parseMysql("2001-02-29", "%Y-%m-%d"), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseDayOfYear) {
  // Just day of year specifier should default to 2000. Also covers leap year
  // case
  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parseMysql("1", "%j"));
  EXPECT_EQ(util::fromTimestampString("2000-01-07"), parseMysql("7 ", "%j "));
  EXPECT_EQ(util::fromTimestampString("2000-01-11"), parseMysql("/11", "/%j"));
  EXPECT_EQ(
      util::fromTimestampString("2000-01-31"), parseMysql("/31/", "/%j/"));
  EXPECT_EQ(util::fromTimestampString("2000-02-01"), parseMysql("32", "%j"));
  EXPECT_EQ(util::fromTimestampString("2000-02-29"), parseMysql("60", "%j"));
  EXPECT_EQ(util::fromTimestampString("2000-12-30"), parseMysql("365", "%j"));
  EXPECT_EQ(util::fromTimestampString("2000-12-31"), parseMysql("366", "%j"));

  // Year specified cases
  EXPECT_EQ(
      util::fromTimestampString("1950-01-01"), parseMysql("1950 1", "%Y %j"));
  EXPECT_EQ(
      util::fromTimestampString("1950-01-07"), parseMysql("1950 7 ", "%Y %j "));
  EXPECT_EQ(
      util::fromTimestampString("1950-01-11"),
      parseMysql("1950 /11", "%Y /%j"));
  EXPECT_EQ(
      util::fromTimestampString("1950-01-31"),
      parseMysql("1950 /31/", "%Y /%j/"));
  EXPECT_EQ(
      util::fromTimestampString("1950-02-01"), parseMysql("1950 32", "%Y %j"));
  EXPECT_EQ(
      util::fromTimestampString("1950-03-01"), parseMysql("1950 60", "%Y %j"));
  EXPECT_EQ(
      util::fromTimestampString("1950-12-31"), parseMysql("1950 365", "%Y %j"));
  EXPECT_THROW(parseMysql("1950 366", "%Y %j"), VeloxUserError);

  // Negative year specified cases
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-01"), parseMysql("-1950 1", "%Y %j"));
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-07"),
      parseMysql("-1950 7 ", "%Y %j "));
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-11"),
      parseMysql("-1950 /11", "%Y /%j"));
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-31"),
      parseMysql("-1950 /31/", "%Y /%j/"));
  EXPECT_EQ(
      util::fromTimestampString("-1950-02-01"),
      parseMysql("-1950 32", "%Y %j"));
  EXPECT_EQ(
      util::fromTimestampString("-1950-03-01"),
      parseMysql("-1950 60", "%Y %j"));
  EXPECT_EQ(
      util::fromTimestampString("-1950-12-31"),
      parseMysql("-1950 365", "%Y %j"));
  EXPECT_THROW(parseMysql("-1950 366", "%Y %j"), VeloxUserError);

  // Ensure all days of year are checked against final selected year
  EXPECT_THROW(parseMysql("2000 366 2001", "%Y %j %Y"), VeloxUserError);
  EXPECT_EQ(
      util::fromTimestampString("2000-12-31"),
      parseMysql("2001 366 2000", "%Y %j %Y"));

  EXPECT_THROW(parseMysql("0", "%j"), VeloxUserError);
  EXPECT_THROW(parseMysql("367", "%j"), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseHourOfDay) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"), parseMysql("7", "%H"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 23:00:00"), parseMysql("23", "%H"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parseMysql("0", "%H"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"), parseMysql("10", "%H"));

  // Hour of day invalid
  EXPECT_THROW(parseMysql("24", "%H"), VeloxUserError);
  EXPECT_THROW(parseMysql("-1", "%H"), VeloxUserError);
  EXPECT_THROW(parseMysql("123456789", "%H"), VeloxUserError);

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"), parseMysql("7", "%k"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 23:00:00"), parseMysql("23", "%k"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parseMysql("0", "%k"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"), parseMysql("10", "%k"));

  // Hour of day invalid
  EXPECT_THROW(parseMysql("24", "%k"), VeloxUserError);
  EXPECT_THROW(parseMysql("-1", "%k"), VeloxUserError);
  EXPECT_THROW(parseMysql("123456789", "%k"), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseClockHourOfHalfDay) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"), parseMysql("7", "%h"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parseMysql("12", "%h"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"), parseMysql("1", "%h"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"), parseMysql("10", "%h"));

  // Clock hour of half day invalid
  EXPECT_THROW(parseMysql("13", "%h"), VeloxUserError);
  EXPECT_THROW(parseMysql("0", "%h"), VeloxUserError);
  EXPECT_THROW(parseMysql("123456789", "%h"), VeloxUserError);

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"), parseMysql("7", "%I"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parseMysql("12", "%I"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"), parseMysql("1", "%I"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"), parseMysql("10", "%I"));

  // Clock hour of half day invalid
  EXPECT_THROW(parseMysql("13", "%l"), VeloxUserError);
  EXPECT_THROW(parseMysql("0", "%l"), VeloxUserError);
  EXPECT_THROW(parseMysql("123456789", "%l"), VeloxUserError);

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"), parseMysql("7", "%l"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parseMysql("12", "%l"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"), parseMysql("1", "%l"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"), parseMysql("10", "%l"));

  // Clock hour of half day invalid
  EXPECT_THROW(parseMysql("13", "%l"), VeloxUserError);
  EXPECT_THROW(parseMysql("0", "%l"), VeloxUserError);
  EXPECT_THROW(parseMysql("123456789", "%l"), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseHalfOfDay) {
  // Half of day has no effect if hour of day is provided
  // hour of day tests
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseMysql("7 PM", "%H %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseMysql("7 AM", "%H %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseMysql("7 pm", "%H %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseMysql("7 am", "%H %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseMysql("0 PM", "%H %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseMysql("0 AM", "%H %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseMysql("0 pm", "%H %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseMysql("0 am", "%H %p"));

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseMysql("7 PM", "%k %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseMysql("7 AM", "%k %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseMysql("7 pm", "%k %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parseMysql("7 am", "%k %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseMysql("0 PM", "%k %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseMysql("0 AM", "%k %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseMysql("0 pm", "%k %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseMysql("0 am", "%k %p"));

  // Half of day has effect if clockhour of halfday is provided
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00"),
      parseMysql("1 PM", "%h %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"),
      parseMysql("1 AM", "%h %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 18:00:00"),
      parseMysql("6 PM", "%h %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 06:00:00"),
      parseMysql("6 AM", "%h %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00"),
      parseMysql("12 PM", "%h %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseMysql("12 AM", "%h %p"));

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00"),
      parseMysql("1 PM", "%I %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"),
      parseMysql("1 AM", "%I %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 18:00:00"),
      parseMysql("6 PM", "%I %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 06:00:00"),
      parseMysql("6 AM", "%I %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00"),
      parseMysql("12 PM", "%I %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseMysql("12 AM", "%I %p"));

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00"),
      parseMysql("1 PM", "%l %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"),
      parseMysql("1 AM", "%l %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 18:00:00"),
      parseMysql("6 PM", "%l %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 06:00:00"),
      parseMysql("6 AM", "%l %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00"),
      parseMysql("12 PM", "%l %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseMysql("12 AM", "%l %p"));

  // time gives precendent to most recent time specifier
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"),
      parseMysql("0 1 AM", "%H %h %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00"),
      parseMysql("12 1 PM", "%H %h %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseMysql("1 AM 0", "%h %p %H"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00"),
      parseMysql("1 AM 12", "%h %p %H"));
}

TEST_F(MysqlDateTimeTest, parseMinute) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:08:00"), parseMysql("8", "%i"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:59:00"), parseMysql("59", "%i"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseMysql("0/", "%i/"));

  EXPECT_THROW(parseMysql("60", "%i"), VeloxUserError);
  EXPECT_THROW(parseMysql("-1", "%i"), VeloxUserError);
  EXPECT_THROW(parseMysql("123456789", "%i"), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseSecond) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:09"), parseMysql("9", "%s"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:58"), parseMysql("58", "%s"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseMysql("0/", "%s/"));

  EXPECT_THROW(parseMysql("60", "%s"), VeloxUserError);
  EXPECT_THROW(parseMysql("-1", "%s"), VeloxUserError);
  EXPECT_THROW(parseMysql("123456789", "%s"), VeloxUserError);

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:09"), parseMysql("9", "%S"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:58"), parseMysql("58", "%S"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parseMysql("0/", "%S/"));

  EXPECT_THROW(parseMysql("60", "%S"), VeloxUserError);
  EXPECT_THROW(parseMysql("-1", "%S"), VeloxUserError);
  EXPECT_THROW(parseMysql("123456789", "%S"), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseMixedYMDFormat) {
  // Common patterns found.
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 23:00:00"),
      parseMysql("2021-01-04+23:00:00", "%Y-%m-%d+%H:%i:%s"));

  EXPECT_EQ(
      util::fromTimestampString("2019-07-03 11:04:10"),
      parseMysql("2019-07-03 11:04:10", "%Y-%m-%d %H:%i:%s"));

  // Backwards, just for fun:
  EXPECT_EQ(
      util::fromTimestampString("2019-07-03 11:04:10"),
      parseMysql("10:04:11 03-07-2019", "%s:%i:%H %d-%m-%Y"));
}

TEST_F(MysqlDateTimeTest, parseMixedWeekFormat) {
  // Common patterns found.
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parseMysql("2021 1 13:29:21.213", "%x %v %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parseMysql("2021 22 13:29:21.213", "%x %v %H:%i:%s.%f"));

  // Day of week short text normal capitlization
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parseMysql("2021 1 Mon 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parseMysql("2021 22 Mon 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parseMysql("2021 22 Thu 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  // Day of week long text normal capitlization
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parseMysql("2021 1 Monday 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parseMysql("2021 22 Monday 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parseMysql("2021 22 Thursday 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  // Day of week short text upper case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parseMysql("2021 1 MON 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parseMysql("2021 22 MON 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parseMysql("2021 22 THU 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  // Day of week long text upper case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parseMysql("2021 1 MONDAY 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parseMysql("2021 22 MONDAY 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parseMysql("2021 22 THURSDAY 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  // Day of week short text lower case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parseMysql("2021 1 mon 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parseMysql("2021 22 mon 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parseMysql("2021 22 thu 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  // Day of week long text lower case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parseMysql("2021 1 monday 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parseMysql("2021 22 monday 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parseMysql("2021 22 thursday 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  // Invalid day of week throw cases
  EXPECT_THROW(parseMysql("mOn", "E"), VeloxUserError);
  EXPECT_THROW(parseMysql("tuE", "E"), VeloxUserError);
  EXPECT_THROW(parseMysql("WeD", "E"), VeloxUserError);
  EXPECT_THROW(parseMysql("WEd", "E"), VeloxUserError);
  EXPECT_THROW(parseMysql("MONday", "EEE"), VeloxUserError);
  EXPECT_THROW(parseMysql("monDAY", "EEE"), VeloxUserError);
  EXPECT_THROW(parseMysql("frIday", "EEE"), VeloxUserError);

  // Backwards, just for fun:
  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parseMysql("213.21:29:13 22 2021", "%f.%s:%i:%H %v %x"));
}

TEST_F(MysqlDateTimeTest, parseFractionOfSecond) {
  // Assert on difference in milliseconds.
  EXPECT_NE(
      util::fromTimestampString("2022-02-23 12:15:00.223"),
      parseMysql("2022-02-23T12:15:00.776", "%Y-%m-%dT%H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.000"),
      parseMysql("000", "%f"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.001"),
      parseMysql("001", "%f"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.999"),
      parseMysql("999", "%f"));

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.045"),
      parseMysql("045", "%f"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.450"),
      parseMysql("45", "%f"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.450"),
      parseMysql("45", "%f"));

  EXPECT_THROW(parseMysql("-1", "%f"), VeloxUserError);
  EXPECT_THROW(parseMysql("9999999", "%f"), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseConsecutiveSpecifiers) {
  EXPECT_EQ(
      util::fromTimestampString("2012-12-01"), parseMysql("1212", "%y%m"));
  EXPECT_EQ(
      util::fromTimestampString("0012-12-01"), parseMysql("1212", "%m%Y"));
  EXPECT_THROW(parseMysql("1212", "%Y%m"), VeloxUserError);

  EXPECT_EQ(
      util::fromTimestampString("2012-01-01 12:00:00"),
      parseMysql("1212", "%y%H"));
  EXPECT_EQ(
      util::fromTimestampString("0012-01-01 12:00:00"),
      parseMysql("1212", "%H%Y"));
  EXPECT_THROW(parseMysql("1212", "%Y%H"), VeloxUserError);
}

} // namespace facebook::velox::functions
