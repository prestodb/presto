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

  auto parseAll(
      const std::string_view& input,
      const std::string_view& format,
      bool joda = true) {
    if (joda) {
      return buildJodaDateTimeFormatter(format)->parse(input);
    } else {
      return buildMysqlDateTimeFormatter(format)->parse(input);
    }
  }

  Timestamp parse(
      const std::string_view& input,
      const std::string_view& format,
      bool joda = true) {
    return parseAll(input, format, joda).timestamp;
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
  auto formatter = builder.appendHalfDayOfDay().build();

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
  EXPECT_THROW(parse("", ""), VeloxUserError);
  EXPECT_THROW(parse(" ", ""), VeloxUserError);
  EXPECT_THROW(parse("", " "), VeloxUserError);
  EXPECT_THROW(parse("", "Y '"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseEra) {
  // Normal era cases
  EXPECT_EQ(util::fromTimestampString("-100-01-01"), parse("BC 101", "G Y"));
  EXPECT_EQ(util::fromTimestampString("101-01-01"), parse("AD 101", "G Y"));
  EXPECT_EQ(util::fromTimestampString("-100-01-01"), parse("bc 101", "G Y"));
  EXPECT_EQ(util::fromTimestampString("101-01-01"), parse("ad 101", "G Y"));

  // Era specifier with 'y' specifier
  EXPECT_EQ(util::fromTimestampString("101-01-01"), parse("BC 101", "G y"));
  EXPECT_EQ(util::fromTimestampString("2012-01-01"), parse("BC 2012", "G y"));
  EXPECT_EQ(
      util::fromTimestampString("-101-01-01"), parse("AD 2012 -101", "G Y y"));
  EXPECT_EQ(
      util::fromTimestampString("2012-01-01"), parse("BC 101 2012", "G Y y"));
  EXPECT_EQ(
      util::fromTimestampString("-2011-01-01"), parse("BC 2000 2012", "G y Y"));
  EXPECT_EQ(
      util::fromTimestampString("-2011-01-01"),
      parse("BC 2000 2012 BC", "G y Y G"));
  EXPECT_EQ(
      util::fromTimestampString("-2014-01-01"),
      parse("BC 1 BC 2015", "G y G Y"));
  EXPECT_EQ(
      util::fromTimestampString("2015-01-01"),
      parse("BC 0 BC 2015 AD", "G y G Y G"));
  EXPECT_EQ(
      util::fromTimestampString("2015-01-01"),
      parse("AD 0 AD 2015", "G y G Y"));
  EXPECT_EQ(
      util::fromTimestampString("-2011-01-01"),
      parse("BC 0 BC 2015 2 2012 BC", "G y G Y y Y G"));
  EXPECT_EQ(
      util::fromTimestampString("2012-01-01"),
      parse("AD 0 AD 2015 2 2012 AD", "G y G Y y Y G"));

  // Invalid cases
  EXPECT_THROW(parse("FG", "G"), VeloxUserError);
  EXPECT_THROW(parse("AC", "G"), VeloxUserError);
  EXPECT_THROW(parse("BD", "G"), VeloxUserError);
  EXPECT_THROW(parse("aD", "G"), VeloxUserError);
  EXPECT_THROW(parse("Ad", "G"), VeloxUserError);
  EXPECT_THROW(parse("bC", "G"), VeloxUserError);
  EXPECT_THROW(parse("Bc", "G"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseYearOfEra) {
  // By the default, assume epoch.
  EXPECT_EQ(util::fromTimestampString("1970-01-01"), parse(" ", " "));

  // Number of times the token is repeated doesn't change the parsing behavior.
  EXPECT_EQ(util::fromTimestampString("2134-01-01"), parse("2134", "Y"));
  EXPECT_EQ(util::fromTimestampString("2134-01-01"), parse("2134", "YYYYYYYY"));

  // Probe the year of era range. Joda only supports positive years.
  EXPECT_EQ(util::fromTimestampString("294247-01-01"), parse("294247", "Y"));
  EXPECT_EQ(util::fromTimestampString("0001-01-01"), parse("1", "Y"));
  EXPECT_THROW(parse("292278994", "Y"), VeloxUserError);
  EXPECT_THROW(parse("0", "Y"), VeloxUserError);
  EXPECT_THROW(parse("-1", "Y"), VeloxUserError);
  EXPECT_THROW(parse("  ", " Y "), VeloxUserError);
  EXPECT_THROW(parse(" 1 2", "Y Y"), VeloxUserError);

  // 2 'Y' token case
  EXPECT_EQ(util::fromTimestampString("2012-01-01"), parse("12", "YY"));
  EXPECT_EQ(util::fromTimestampString("2069-01-01"), parse("69", "YY"));
  EXPECT_EQ(util::fromTimestampString("1970-01-01"), parse("70", "YY"));
  EXPECT_EQ(util::fromTimestampString("1999-01-01"), parse("99", "YY"));
  EXPECT_EQ(util::fromTimestampString("0002-01-01"), parse("2", "YY"));
  EXPECT_EQ(util::fromTimestampString("0210-01-01"), parse("210", "YY"));
  EXPECT_EQ(util::fromTimestampString("0001-01-01"), parse("1", "YY"));
  EXPECT_EQ(util::fromTimestampString("2001-01-01"), parse("01", "YY"));

  // Last token read overwrites:
  EXPECT_EQ(
      util::fromTimestampString("0005-01-01"), parse("1 2 3 4 5", "Y Y Y Y Y"));

  // Throws on consumption of plus sign
  EXPECT_THROW(parse("+100", "Y"), VeloxUserError);
}

// Same semantic as YEAR_OF_ERA, except that it accepts zero and negative years.
TEST_F(JodaDateTimeFormatterTest, parseYear) {
  EXPECT_EQ(util::fromTimestampString("123-01-01"), parse("123", "y"));
  EXPECT_EQ(util::fromTimestampString("321-01-01"), parse("321", "yyyyyyyy"));

  EXPECT_EQ(util::fromTimestampString("0-01-01"), parse("0", "y"));
  EXPECT_EQ(util::fromTimestampString("-1-01-01"), parse("-1", "y"));
  EXPECT_EQ(util::fromTimestampString("-1234-01-01"), parse("-1234", "y"));

  // Last token read overwrites:
  EXPECT_EQ(util::fromTimestampString("0-01-01"), parse("123 0", "Y y"));

  // 2 'y' token case
  EXPECT_EQ(util::fromTimestampString("2012-01-01"), parse("12", "yy"));
  EXPECT_EQ(util::fromTimestampString("2069-01-01"), parse("69", "yy"));
  EXPECT_EQ(util::fromTimestampString("1970-01-01"), parse("70", "yy"));
  EXPECT_EQ(util::fromTimestampString("1999-01-01"), parse("99", "yy"));
  EXPECT_EQ(util::fromTimestampString("0002-01-01"), parse("2", "yy"));
  EXPECT_EQ(util::fromTimestampString("0210-01-01"), parse("210", "yy"));
  EXPECT_EQ(util::fromTimestampString("0001-01-01"), parse("1", "yy"));
  EXPECT_EQ(util::fromTimestampString("2001-01-01"), parse("01", "yy"));

  // Plus sign consumption valid when y operator is not followed by another
  // specifier
  EXPECT_EQ(util::fromTimestampString("10-01-01"), parse("+10", "y"));
  EXPECT_EQ(util::fromTimestampString("99-02-01"), parse("+99 02", "y M"));
  EXPECT_EQ(util::fromTimestampString("10-10-01"), parse("10 +10", "M y"));
  EXPECT_EQ(util::fromTimestampString("100-02-01"), parse("2+100", "My"));
  EXPECT_THROW(parse("+10001", "yM"), VeloxUserError);
  EXPECT_THROW(parse("++100", "y"), VeloxUserError);

  // Probe the year range
  EXPECT_THROW(parse("-292275056", "y"), VeloxUserError);
  EXPECT_THROW(parse("292278995", "y"), VeloxUserError);
  EXPECT_EQ(
      util::fromTimestampString("292278994-01-01"), parse("292278994", "y"));
}

TEST_F(JodaDateTimeFormatterTest, parseWeekYear) {
  // Covers entire range of possible week year start dates (12-29 to 01-04)
  EXPECT_EQ(
      util::fromTimestampString("1969-12-29 00:00:00"), parse("1970", "x"));
  EXPECT_EQ(
      util::fromTimestampString("2024-12-30 00:00:00"), parse("2025", "x"));
  EXPECT_EQ(
      util::fromTimestampString("1934-12-31 00:00:00"), parse("1935", "x"));
  EXPECT_EQ(
      util::fromTimestampString("1990-01-01 00:00:00"), parse("1990", "x"));
  EXPECT_EQ(
      util::fromTimestampString("0204-01-02 00:00:00"), parse("204", "x"));
  EXPECT_EQ(
      util::fromTimestampString("-0102-01-03 00:00:00"), parse("-102", "x"));
  EXPECT_EQ(
      util::fromTimestampString("-0108-01-04 00:00:00"), parse("-108", "x"));
  EXPECT_EQ(
      util::fromTimestampString("-1002-12-31 00:00:00"), parse("-1001", "x"));

  // 2 'x' token case
  EXPECT_EQ(util::fromTimestampString("2012-01-02"), parse("12", "xx"));
  EXPECT_EQ(util::fromTimestampString("2068-12-31"), parse("69", "xx"));
  EXPECT_EQ(util::fromTimestampString("1969-12-29"), parse("70", "xx"));
  EXPECT_EQ(util::fromTimestampString("1999-01-04"), parse("99", "xx"));
  EXPECT_EQ(util::fromTimestampString("0001-12-31"), parse("2", "xx"));
  EXPECT_EQ(util::fromTimestampString("0210-01-01"), parse("210", "xx"));
  EXPECT_EQ(util::fromTimestampString("0001-01-01"), parse("1", "xx"));
  EXPECT_EQ(util::fromTimestampString("2001-01-01"), parse("01", "xx"));

  // Plus sign consumption valid when x operator is not followed by another
  // specifier
  EXPECT_EQ(util::fromTimestampString("10-01-04"), parse("+10", "x"));
  EXPECT_EQ(util::fromTimestampString("0098-12-29"), parse("+99 01", "x w"));
  EXPECT_EQ(util::fromTimestampString("0099-01-05"), parse("+99 02", "x w"));
  EXPECT_EQ(util::fromTimestampString("10-03-08"), parse("10 +10", "w x"));
  EXPECT_EQ(util::fromTimestampString("100-01-11"), parse("2+100", "wx"));
  EXPECT_THROW(parse("+10001", "xM"), VeloxUserError);
  EXPECT_THROW(parse("++100", "x"), VeloxUserError);

  // Probe week year range
  EXPECT_THROW(parse("-292275055", "x"), VeloxUserError);
  EXPECT_THROW(parse("292278994", "x"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseCenturyOfEra) {
  // Probe century range
  EXPECT_EQ(
      util::fromTimestampString("292278900-01-01 00:00:00"),
      parse("2922789", "CCCCCCC"));
  EXPECT_EQ(util::fromTimestampString("00-01-01 00:00:00"), parse("0", "C"));

  // Invalid century values
  EXPECT_THROW(parse("-1", "CCCCCCC"), VeloxUserError);
  EXPECT_THROW(parse("2922790", "CCCCCCC"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseMonth) {
  // Joda has this weird behavior where if minute or hour is specified, year
  // falls back to 2000, instead of epoch (1970)  ¯\_(ツ)_/¯
  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parse("1", "M"));
  EXPECT_EQ(util::fromTimestampString("2000-07-01"), parse(" 7", " MM"));
  EXPECT_EQ(util::fromTimestampString("2000-11-01"), parse("11-", "M-"));
  EXPECT_EQ(util::fromTimestampString("2000-12-01"), parse("-12-", "-M-"));

  EXPECT_THROW(parse("0", "M"), VeloxUserError);
  EXPECT_THROW(parse("13", "M"), VeloxUserError);
  EXPECT_THROW(parse("12345", "M"), VeloxUserError);

  // Ensure MMM and MMMM specifiers consume both short- and long-form month
  // names
  for (int i = 0; i < 12; i++) {
    StringView buildString("2000-" + std::to_string(i + 1) + "-01");
    EXPECT_EQ(
        util::fromTimestampString(buildString), parse(monthsShort[i], "MMM"));
    EXPECT_EQ(
        util::fromTimestampString(buildString), parse(monthsFull[i], "MMM"));
    EXPECT_EQ(
        util::fromTimestampString(buildString), parse(monthsShort[i], "MMMM"));
    EXPECT_EQ(
        util::fromTimestampString(buildString), parse(monthsFull[i], "MMMM"));
  }

  // Month name invalid parse
  EXPECT_THROW(parse("Decembr", "MMM"), VeloxUserError);
  EXPECT_THROW(parse("Decembr", "MMMM"), VeloxUserError);
  EXPECT_THROW(parse("Decemberary", "MMM"), VeloxUserError);
  EXPECT_THROW(parse("Decemberary", "MMMM"), VeloxUserError);
  EXPECT_THROW(parse("asdf", "MMM"), VeloxUserError);
  EXPECT_THROW(parse("asdf", "MMMM"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseDayOfMonth) {
  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parse("1", "d"));
  EXPECT_EQ(util::fromTimestampString("2000-01-07"), parse("7 ", "dd "));
  EXPECT_EQ(util::fromTimestampString("2000-01-11"), parse("/11", "/dd"));
  EXPECT_EQ(util::fromTimestampString("2000-01-31"), parse("/31/", "/d/"));

  EXPECT_THROW(parse("0", "d"), VeloxUserError);
  EXPECT_THROW(parse("32", "d"), VeloxUserError);
  EXPECT_THROW(parse("12345", "d"), VeloxUserError);

  EXPECT_THROW(parse("02-31", "M-d"), VeloxUserError);
  EXPECT_THROW(parse("04-31", "M-d"), VeloxUserError);

  // Ensure all days of month are checked against final selected month
  EXPECT_THROW(parse("1 31 20 2", "M d d M"), VeloxUserError);
  EXPECT_THROW(parse("2 31 20 4", "M d d M"), VeloxUserError);
  EXPECT_EQ(util::fromTimestampString("2000-01-31"), parse("2 31 1", "M d M"));

  // Probe around leap year.
  EXPECT_EQ(
      util::fromTimestampString("2000-02-29"), parse("2000-02-29", "Y-M-d"));
  EXPECT_THROW(parse("2001-02-29", "Y-M-d"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseDayOfYear) {
  // Just day of year specifier should default to 2000. Also covers leap year
  // case
  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parse("1", "D"));
  EXPECT_EQ(util::fromTimestampString("2000-01-07"), parse("7 ", "DD "));
  EXPECT_EQ(util::fromTimestampString("2000-01-11"), parse("/11", "/DD"));
  EXPECT_EQ(util::fromTimestampString("2000-01-31"), parse("/31/", "/DDD/"));
  EXPECT_EQ(util::fromTimestampString("2000-02-01"), parse("32", "D"));
  EXPECT_EQ(util::fromTimestampString("2000-02-29"), parse("60", "D"));
  EXPECT_EQ(util::fromTimestampString("2000-12-30"), parse("365", "D"));
  EXPECT_EQ(util::fromTimestampString("2000-12-31"), parse("366", "D"));

  // Year specified cases
  EXPECT_EQ(util::fromTimestampString("1950-01-01"), parse("1950 1", "y D"));
  EXPECT_EQ(util::fromTimestampString("1950-01-07"), parse("1950 7 ", "y DD "));
  EXPECT_EQ(
      util::fromTimestampString("1950-01-11"), parse("1950 /11", "y /DD"));
  EXPECT_EQ(
      util::fromTimestampString("1950-01-31"), parse("1950 /31/", "y /DDD/"));
  EXPECT_EQ(util::fromTimestampString("1950-02-01"), parse("1950 32", "y D"));
  EXPECT_EQ(util::fromTimestampString("1950-03-01"), parse("1950 60", "y D"));
  EXPECT_EQ(util::fromTimestampString("1950-12-31"), parse("1950 365", "y D"));
  EXPECT_THROW(parse("1950 366", "Y D"), VeloxUserError);

  // Negative year specified cases
  EXPECT_EQ(util::fromTimestampString("-1950-01-01"), parse("-1950 1", "y D"));
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-07"), parse("-1950 7 ", "y DD "));
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-11"), parse("-1950 /11", "y /DD"));
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-31"), parse("-1950 /31/", "y /DDD/"));
  EXPECT_EQ(util::fromTimestampString("-1950-02-01"), parse("-1950 32", "y D"));
  EXPECT_EQ(util::fromTimestampString("-1950-03-01"), parse("-1950 60", "y D"));
  EXPECT_EQ(
      util::fromTimestampString("-1950-12-31"), parse("-1950 365", "y D"));
  EXPECT_THROW(parse("-1950 366", "Y D"), VeloxUserError);

  // Ensure all days of year are checked against final selected year
  EXPECT_THROW(parse("2000 366 2001", "y D y"), VeloxUserError);
  EXPECT_EQ(
      util::fromTimestampString("2000-12-31"), parse("2001 366 2000", "y D y"));

  EXPECT_THROW(parse("0", "d"), VeloxUserError);
  EXPECT_THROW(parse("367", "d"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseHourOfDay) {
  EXPECT_EQ(util::fromTimestampString("1970-01-01 07:00:00"), parse("7", "H"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 23:00:00"), parse("23", "HH"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("0", "HHH"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"),
      parse("10", "HHHHHHHH"));

  // Hour of day invalid
  EXPECT_THROW(parse("24", "H"), VeloxUserError);
  EXPECT_THROW(parse("-1", "H"), VeloxUserError);
  EXPECT_THROW(parse("123456789", "H"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseClockHourOfDay) {
  EXPECT_EQ(util::fromTimestampString("1970-01-01 07:00:00"), parse("7", "k"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("24", "kk"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"), parse("1", "kkk"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"),
      parse("10", "kkkkkkkk"));

  // Clock hour of day invalid
  EXPECT_THROW(parse("25", "k"), VeloxUserError);
  EXPECT_THROW(parse("0", "k"), VeloxUserError);
  EXPECT_THROW(parse("123456789", "k"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseHourOfHalfDay) {
  EXPECT_EQ(util::fromTimestampString("1970-01-01 07:00:00"), parse("7", "K"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 11:00:00"), parse("11", "KK"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("0", "KKK"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"),
      parse("10", "KKKKKKKK"));

  // Hour of half day invalid
  EXPECT_THROW(parse("12", "K"), VeloxUserError);
  EXPECT_THROW(parse("-1", "K"), VeloxUserError);
  EXPECT_THROW(parse("123456789", "K"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseClockHourOfHalfDay) {
  EXPECT_EQ(util::fromTimestampString("1970-01-01 07:00:00"), parse("7", "h"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("12", "hh"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"), parse("1", "hhh"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"),
      parse("10", "hhhhhhhh"));

  // Clock hour of half day invalid
  EXPECT_THROW(parse("13", "h"), VeloxUserError);
  EXPECT_THROW(parse("0", "h"), VeloxUserError);
  EXPECT_THROW(parse("123456789", "h"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseHalfOfDay) {
  // Half of day has no effect if hour or clockhour of day is provided
  // hour of day tests
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"), parse("7 PM", "H a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"), parse("7 AM", "H a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"), parse("7 pm", "H a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"), parse("7 am", "H a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("0 PM", "H a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("0 AM", "H a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("0 pm", "H a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("0 am", "H a"));

  // clock hour of day tests
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"), parse("7 PM", "k a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"), parse("7 AM", "k a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"), parse("7 pm", "k a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"), parse("7 am", "k a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("24 PM", "k a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("24 AM", "k a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("24 pm", "k a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("24 am", "k a"));

  // Half of day has effect if hour or clockhour of halfday is provided
  // hour of halfday tests
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00"), parse("0 PM", "K a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("0 AM", "K a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 18:00:00"), parse("6 PM", "K a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 06:00:00"), parse("6 AM", "K a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 23:00:00"), parse("11 PM", "K a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 11:00:00"), parse("11 AM", "K a"));

  // clockhour of halfday tests
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00"), parse("1 PM", "h a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"), parse("1 AM", "h a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 18:00:00"), parse("6 PM", "h a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 06:00:00"), parse("6 AM", "h a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00"), parse("12 PM", "h a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("12 AM", "h a"));

  // time gives precendent to most recent time specifier
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"),
      parse("0 1 AM", "H h a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00"),
      parse("12 1 PM", "H h a"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("1 AM 0", "h a H"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00"),
      parse("1 AM 12", "h a H"));
}

TEST_F(JodaDateTimeFormatterTest, parseMinute) {
  EXPECT_EQ(util::fromTimestampString("1970-01-01 00:08:00"), parse("8", "m"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:59:00"), parse("59", "mm"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("0/", "mmm/"));

  EXPECT_THROW(parse("60", "m"), VeloxUserError);
  EXPECT_THROW(parse("-1", "m"), VeloxUserError);
  EXPECT_THROW(parse("123456789", "m"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseSecond) {
  EXPECT_EQ(util::fromTimestampString("1970-01-01 00:00:09"), parse("9", "s"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:58"), parse("58", "ss"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("0/", "s/"));

  EXPECT_THROW(parse("60", "s"), VeloxUserError);
  EXPECT_THROW(parse("-1", "s"), VeloxUserError);
  EXPECT_THROW(parse("123456789", "s"), VeloxUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseTimezone) {
  // Broken timezone offfsets; allowed formats are either "+00:00" or "+00".
  EXPECT_THROW(parse("", "ZZ"), VeloxUserError);
  EXPECT_THROW(parse("0", "ZZ"), VeloxUserError);
  EXPECT_THROW(parse("00", "ZZ"), VeloxUserError);
  EXPECT_THROW(parse(":00", "ZZ"), VeloxUserError);
  EXPECT_THROW(parse("+0", "ZZ"), VeloxUserError);
  EXPECT_THROW(parse("+00:", "ZZ"), VeloxUserError);
  EXPECT_THROW(parse("+00:0", "ZZ"), VeloxUserError);
  EXPECT_THROW(parse("12", "YYZZ"), VeloxUserError);
  EXPECT_THROW(parse("ZZ", "Z"), VeloxUserError);
  EXPECT_THROW(parse("ZZ", "ZZ"), VeloxUserError);

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
      parse("2021-01-04+23:00", "YYYY-MM-dd+HH:mm"));

  EXPECT_EQ(
      util::fromTimestampString("2019-07-03 11:04:10"),
      parse("2019-07-03 11:04:10", "YYYY-MM-dd HH:mm:ss"));

  // Backwards, just for fun:
  EXPECT_EQ(
      util::fromTimestampString("2019-07-03 11:04:10"),
      parse("10:04:11 03-07-2019", "ss:mm:HH dd-MM-YYYY"));

  // Include timezone.
  auto result = parseAll("2021-11-05+01:00+09:00", "YYYY-MM-dd+HH:mmZZ");
  EXPECT_EQ(util::fromTimestampString("2021-11-05 01:00:00"), result.timestamp);
  EXPECT_EQ("+09:00", util::getTimeZoneName(result.timezoneId));

  // Timezone offset in -hh:mm format.
  result = parseAll("-07:232021-11-05+01:00", "ZZYYYY-MM-dd+HH:mm");
  EXPECT_EQ(util::fromTimestampString("2021-11-05 01:00:00"), result.timestamp);
  EXPECT_EQ("-07:23", util::getTimeZoneName(result.timezoneId));

  // Timezone offset in +hhmm format.
  result = parseAll("+01332022-03-08+13:00", "ZZYYYY-MM-dd+HH:mm");
  EXPECT_EQ(util::fromTimestampString("2022-03-08 13:00:00"), result.timestamp);
  EXPECT_EQ("+01:33", util::getTimeZoneName(result.timezoneId));

  // Z in the input means GMT in Joda.
  EXPECT_EQ(
      util::fromTimestampString("2022-07-29 20:03:54.667"),
      parse("2022-07-29T20:03:54.667Z", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
}

TEST_F(JodaDateTimeFormatterTest, parseMixedWeekFormat) {
  // Common patterns found.
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parse("2021 1 1 13:29:21.213", "x w e HH:mm:ss.SSS"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parse("2021 22 1 13:29:21.213", "x w e HH:mm:ss.SSS"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parse("2021 22 4 13:29:21.213", "x w e HH:mm:ss.SSS"));

  // Day of week short text normal capitlization
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parse("2021 1 Mon 13:29:21.213", "x w E HH:mm:ss.SSS"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parse("2021 22 Mon 13:29:21.213", "x w E HH:mm:ss.SSS"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parse("2021 22 Thu 13:29:21.213", "x w E HH:mm:ss.SSS"));

  // Day of week long text normal capitlization
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parse("2021 1 Monday 13:29:21.213", "x w EEE HH:mm:ss.SSS"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parse("2021 22 Monday 13:29:21.213", "x w EEE HH:mm:ss.SSS"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parse("2021 22 Thursday 13:29:21.213", "x w EEE HH:mm:ss.SSS"));

  // Day of week short text upper case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parse("2021 1 MON 13:29:21.213", "x w E HH:mm:ss.SSS"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parse("2021 22 MON 13:29:21.213", "x w E HH:mm:ss.SSS"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parse("2021 22 THU 13:29:21.213", "x w E HH:mm:ss.SSS"));

  // Day of week long text upper case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parse("2021 1 MONDAY 13:29:21.213", "x w EEE HH:mm:ss.SSS"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parse("2021 22 MONDAY 13:29:21.213", "x w EEE HH:mm:ss.SSS"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parse("2021 22 THURSDAY 13:29:21.213", "x w EEE HH:mm:ss.SSS"));

  // Day of week short text lower case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parse("2021 1 mon 13:29:21.213", "x w E HH:mm:ss.SSS"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parse("2021 22 mon 13:29:21.213", "x w E HH:mm:ss.SSS"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parse("2021 22 thu 13:29:21.213", "x w E HH:mm:ss.SSS"));

  // Day of week long text lower case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parse("2021 1 monday 13:29:21.213", "x w EEE HH:mm:ss.SSS"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parse("2021 22 monday 13:29:21.213", "x w EEE HH:mm:ss.SSS"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parse("2021 22 thursday 13:29:21.213", "x w EEE HH:mm:ss.SSS"));

  // Invalid day of week throw cases
  EXPECT_THROW(parse("mOn", "E"), VeloxUserError);
  EXPECT_THROW(parse("tuE", "E"), VeloxUserError);
  EXPECT_THROW(parse("WeD", "E"), VeloxUserError);
  EXPECT_THROW(parse("WEd", "E"), VeloxUserError);
  EXPECT_THROW(parse("MONday", "EEE"), VeloxUserError);
  EXPECT_THROW(parse("monDAY", "EEE"), VeloxUserError);
  EXPECT_THROW(parse("frIday", "EEE"), VeloxUserError);

  // Backwards, just for fun:
  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parse("213.21:29:13 1 22 2021", "SSS.ss:mm:HH e w x"));

  // Include timezone.
  auto result =
      parseAll("2021 22 1 13:29:21.213+09:00", "x w e HH:mm:ss.SSSZZ");
  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"), result.timestamp);
  EXPECT_EQ("+09:00", util::getTimeZoneName(result.timezoneId));

  // Timezone offset in -hh:mm format.
  result = parseAll("-07:232021 22 1 13:29:21.213", "ZZx w e HH:mm:ss.SSS");
  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"), result.timestamp);
  EXPECT_EQ("-07:23", util::getTimeZoneName(result.timezoneId));

  // Timezone offset in +hhmm format.
  result = parseAll("+01332021 22 1 13:29:21.213", "ZZx w e HH:mm:ss.SSS");
  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"), result.timestamp);
  EXPECT_EQ("+01:33", util::getTimeZoneName(result.timezoneId));
}

TEST_F(JodaDateTimeFormatterTest, parseFractionOfSecond) {
  // Valid milliseconds and timezone with positive offset.
  auto result =
      parseAll("2022-02-23T12:15:00.364+04:00", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
  EXPECT_EQ(
      util::fromTimestampString("2022-02-23 12:15:00.364"), result.timestamp);
  EXPECT_EQ("+04:00", util::getTimeZoneName(result.timezoneId));

  // Valid milliseconds and timezone with negative offset.
  result =
      parseAll("2022-02-23T12:15:00.776-14:00", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
  EXPECT_EQ(
      util::fromTimestampString("2022-02-23 12:15:00.776"), result.timestamp);
  EXPECT_EQ("-14:00", util::getTimeZoneName(result.timezoneId));

  // Valid milliseconds.
  EXPECT_EQ(
      util::fromTimestampString("2022-02-24 02:19:33.283"),
      parse("2022-02-24 02:19:33.283", "yyyy-MM-dd HH:mm:ss.SSS"));

  // Test without milliseconds.
  EXPECT_EQ(
      util::fromTimestampString("2022-02-23 20:30:00"),
      parse("2022-02-23T20:30:00", "yyyy-MM-dd'T'HH:mm:ss"));

  // Assert on difference in milliseconds.
  EXPECT_NE(
      util::fromTimestampString("2022-02-23 12:15:00.223"),
      parse("2022-02-23T12:15:00.776", "yyyy-MM-dd'T'HH:mm:ss.SSS"));

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.000"),
      parse("000", "SSS"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.001"),
      parse("001", "SSS"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.999"),
      parse("999", "SSS"));

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.045"),
      parse("045", "SSS"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.450"), parse("45", "SS"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.450"),
      parse("45", "SSSS"));

  EXPECT_THROW(parse("-1", "S"), VeloxUserError);
  EXPECT_THROW(parse("999", "S"), VeloxUserError);
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
  EXPECT_EQ(util::fromTimestampString("123-01-01"), parse("123", "%Y", false));
  EXPECT_EQ(util::fromTimestampString("321-01-01"), parse("321", "%Y", false));

  EXPECT_EQ(util::fromTimestampString("0-01-01"), parse("0", "%Y", false));
  EXPECT_EQ(util::fromTimestampString("-1-01-01"), parse("-1", "%Y", false));
  EXPECT_EQ(
      util::fromTimestampString("-1234-01-01"), parse("-1234", "%Y", false));

  // Last token read overwrites:
  EXPECT_EQ(
      util::fromTimestampString("0-01-01"), parse("123 0", "%Y %Y", false));

  // Plus sign consumption valid when %Y operator is not followed by another
  // specifier
  EXPECT_EQ(util::fromTimestampString("10-01-01"), parse("+10", "%Y", false));
  EXPECT_EQ(
      util::fromTimestampString("99-02-01"), parse("+99 02", "%Y %m", false));
  EXPECT_EQ(
      util::fromTimestampString("10-10-01"), parse("10 +10", "%m %Y", false));
  EXPECT_EQ(
      util::fromTimestampString("100-02-01"), parse("2+100", "%m%Y", false));
  EXPECT_THROW(parse("+10001", "%Y%m", false), VeloxUserError);
  EXPECT_THROW(parse("++100", "%Y", false), VeloxUserError);

  // Probe the year range
  EXPECT_THROW(parse("-10000", "%Y"), VeloxUserError);
  EXPECT_THROW(parse("10000", "%Y"), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseTwoDigitYear) {
  EXPECT_EQ(util::fromTimestampString("1970-01-01"), parse("70", "%y", false));
  EXPECT_EQ(util::fromTimestampString("2069-01-01"), parse("69", "%y", false));
  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parse("00", "%y", false));

  // Last token read overwrites:
  EXPECT_EQ(
      util::fromTimestampString("2030-01-01"), parse("80 30", "%y %y", false));
}

TEST_F(MysqlDateTimeTest, parseWeekYear) {
  // Covers entire range of possible week year start dates (12-29 to 01-04)
  EXPECT_EQ(
      util::fromTimestampString("1969-12-29 00:00:00"),
      parse("1970", "%x", false));
  EXPECT_EQ(
      util::fromTimestampString("2024-12-30 00:00:00"),
      parse("2025", "%x", false));
  EXPECT_EQ(
      util::fromTimestampString("1934-12-31 00:00:00"),
      parse("1935", "%x", false));
  EXPECT_EQ(
      util::fromTimestampString("1990-01-01 00:00:00"),
      parse("1990", "%x", false));
  EXPECT_EQ(
      util::fromTimestampString("0204-01-02 00:00:00"),
      parse("204", "%x", false));
  EXPECT_EQ(
      util::fromTimestampString("-0102-01-03 00:00:00"),
      parse("-102", "%x", false));
  EXPECT_EQ(
      util::fromTimestampString("-0108-01-04 00:00:00"),
      parse("-108", "%x", false));
  EXPECT_EQ(
      util::fromTimestampString("-1002-12-31 00:00:00"),
      parse("-1001", "%x", false));

  // Plus sign consumption valid when %x operator is not followed by another
  // specifier
  EXPECT_EQ(util::fromTimestampString("10-01-04"), parse("+10", "%x", false));
  EXPECT_EQ(
      util::fromTimestampString("0098-12-29"), parse("+99 01", "%x %v", false));
  EXPECT_EQ(
      util::fromTimestampString("0099-01-05"), parse("+99 02", "%x %v", false));
  EXPECT_EQ(
      util::fromTimestampString("10-03-08"), parse("10 +10", "%v %x", false));
  EXPECT_EQ(
      util::fromTimestampString("100-01-11"), parse("2+100", "%v%x", false));
  EXPECT_THROW(parse("+10001", "%x%m", false), VeloxUserError);
  EXPECT_THROW(parse("++100", "%x", false), VeloxUserError);

  // Probe week year range
  EXPECT_THROW(parse("-292275055", "%x", false), VeloxUserError);
  EXPECT_THROW(parse("292278994", "%x", false), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseMonth) {
  // Joda has this weird behavior where if minute or hour is specified, year
  // falls back to 2000, instead of epoch (1970)  ¯\_(ツ)_/¯
  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parse("1", "%m", false));
  EXPECT_EQ(util::fromTimestampString("2000-07-01"), parse(" 7", " %m", false));
  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parse("01", "%m", false));
  EXPECT_EQ(
      util::fromTimestampString("2000-07-01"), parse(" 07", " %m", false));
  EXPECT_EQ(
      util::fromTimestampString("2000-11-01"), parse("11-", "%m-", false));
  EXPECT_EQ(
      util::fromTimestampString("2000-12-01"), parse("-12-", "-%m-", false));

  EXPECT_THROW(parse("0", "%m", false), VeloxUserError);
  EXPECT_THROW(parse("13", "%m", false), VeloxUserError);
  EXPECT_THROW(parse("12345", "%m", false), VeloxUserError);

  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parse("1", "%c", false));
  EXPECT_EQ(util::fromTimestampString("2000-07-01"), parse(" 7", " %c", false));
  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parse("01", "%c", false));
  EXPECT_EQ(
      util::fromTimestampString("2000-07-01"), parse(" 07", " %c", false));
  EXPECT_EQ(
      util::fromTimestampString("2000-11-01"), parse("11-", "%c-", false));
  EXPECT_EQ(
      util::fromTimestampString("2000-12-01"), parse("-12-", "-%c-", false));

  EXPECT_THROW(parse("0", "%c", false), VeloxUserError);
  EXPECT_THROW(parse("13", "%c", false), VeloxUserError);
  EXPECT_THROW(parse("12345", "%c", false), VeloxUserError);

  // Ensure %b and %M specifiers consume both short- and long-form month
  // names
  for (int i = 0; i < 12; i++) {
    StringView buildString("2000-" + std::to_string(i + 1) + "-01");
    EXPECT_EQ(
        util::fromTimestampString(buildString),
        parse(monthsShort[i], "%b", false));
    EXPECT_EQ(
        util::fromTimestampString(buildString),
        parse(monthsFull[i], "%b", false));
    EXPECT_EQ(
        util::fromTimestampString(buildString),
        parse(monthsShort[i], "%M", false));
    EXPECT_EQ(
        util::fromTimestampString(buildString),
        parse(monthsFull[i], "%M", false));
  }

  // Month name invalid parse
  EXPECT_THROW(parse("Decembr", "%b", false), VeloxUserError);
  EXPECT_THROW(parse("Decembr", "%M", false), VeloxUserError);
  EXPECT_THROW(parse("Decemberary", "%b", false), VeloxUserError);
  EXPECT_THROW(parse("Decemberary", "%M", false), VeloxUserError);
  EXPECT_THROW(parse("asdf", "%b", false), VeloxUserError);
  EXPECT_THROW(parse("asdf", "%M", false), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseDayOfMonth) {
  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parse("1", "%d", false));
  EXPECT_EQ(util::fromTimestampString("2000-01-07"), parse("7 ", "%d ", false));
  EXPECT_EQ(
      util::fromTimestampString("2000-01-11"), parse("/11", "/%d", false));
  EXPECT_EQ(
      util::fromTimestampString("2000-01-31"), parse("/31/", "/%d/", false));

  EXPECT_THROW(parse("0", "%d", false), VeloxUserError);
  EXPECT_THROW(parse("32", "%d", false), VeloxUserError);
  EXPECT_THROW(parse("12345", "%d", false), VeloxUserError);

  EXPECT_THROW(parse("02-31", "%m-%d", false), VeloxUserError);
  EXPECT_THROW(parse("04-31", "%m-%d", false), VeloxUserError);

  // Ensure all days of month are checked against final selected month
  EXPECT_THROW(parse("1 31 20 2", "%m %d %d %m", false), VeloxUserError);
  EXPECT_THROW(parse("2 31 20 4", "%m %d %d %m", false), VeloxUserError);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-31"),
      parse("2 31 1", "%m %d %m", false));

  // Probe around leap year.
  EXPECT_EQ(
      util::fromTimestampString("2000-02-29"),
      parse("2000-02-29", "%Y-%m-%d", false));
  EXPECT_THROW(parse("2001-02-29", "%Y-%m-%d", false), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseDayOfYear) {
  // Just day of year specifier should default to 2000. Also covers leap year
  // case
  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parse("1", "%j", false));
  EXPECT_EQ(util::fromTimestampString("2000-01-07"), parse("7 ", "%j ", false));
  EXPECT_EQ(
      util::fromTimestampString("2000-01-11"), parse("/11", "/%j", false));
  EXPECT_EQ(
      util::fromTimestampString("2000-01-31"), parse("/31/", "/%j/", false));
  EXPECT_EQ(util::fromTimestampString("2000-02-01"), parse("32", "%j", false));
  EXPECT_EQ(util::fromTimestampString("2000-02-29"), parse("60", "%j", false));
  EXPECT_EQ(util::fromTimestampString("2000-12-30"), parse("365", "%j", false));
  EXPECT_EQ(util::fromTimestampString("2000-12-31"), parse("366", "%j", false));

  // Year specified cases
  EXPECT_EQ(
      util::fromTimestampString("1950-01-01"), parse("1950 1", "%Y %j", false));
  EXPECT_EQ(
      util::fromTimestampString("1950-01-07"),
      parse("1950 7 ", "%Y %j ", false));
  EXPECT_EQ(
      util::fromTimestampString("1950-01-11"),
      parse("1950 /11", "%Y /%j", false));
  EXPECT_EQ(
      util::fromTimestampString("1950-01-31"),
      parse("1950 /31/", "%Y /%j/", false));
  EXPECT_EQ(
      util::fromTimestampString("1950-02-01"),
      parse("1950 32", "%Y %j", false));
  EXPECT_EQ(
      util::fromTimestampString("1950-03-01"),
      parse("1950 60", "%Y %j", false));
  EXPECT_EQ(
      util::fromTimestampString("1950-12-31"),
      parse("1950 365", "%Y %j", false));
  EXPECT_THROW(parse("1950 366", "%Y %j"), VeloxUserError);

  // Negative year specified cases
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-01"),
      parse("-1950 1", "%Y %j", false));
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-07"),
      parse("-1950 7 ", "%Y %j ", false));
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-11"),
      parse("-1950 /11", "%Y /%j", false));
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-31"),
      parse("-1950 /31/", "%Y /%j/", false));
  EXPECT_EQ(
      util::fromTimestampString("-1950-02-01"),
      parse("-1950 32", "%Y %j", false));
  EXPECT_EQ(
      util::fromTimestampString("-1950-03-01"),
      parse("-1950 60", "%Y %j", false));
  EXPECT_EQ(
      util::fromTimestampString("-1950-12-31"),
      parse("-1950 365", "%Y %j", false));
  EXPECT_THROW(parse("-1950 366", "%Y %j", false), VeloxUserError);

  // Ensure all days of year are checked against final selected year
  EXPECT_THROW(parse("2000 366 2001", "%Y %j %Y", false), VeloxUserError);
  EXPECT_EQ(
      util::fromTimestampString("2000-12-31"),
      parse("2001 366 2000", "%Y %j %Y", false));

  EXPECT_THROW(parse("0", "%j", false), VeloxUserError);
  EXPECT_THROW(parse("367", "%j", false), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseHourOfDay) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parse("7", "%H", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 23:00:00"),
      parse("23", "%H", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("0", "%H", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"),
      parse("10", "%H", false));

  // Hour of day invalid
  EXPECT_THROW(parse("24", "%H", false), VeloxUserError);
  EXPECT_THROW(parse("-1", "%H", false), VeloxUserError);
  EXPECT_THROW(parse("123456789", "%H", false), VeloxUserError);

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parse("7", "%k", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 23:00:00"),
      parse("23", "%k", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("0", "%k", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"),
      parse("10", "%k", false));

  // Hour of day invalid
  EXPECT_THROW(parse("24", "%k", false), VeloxUserError);
  EXPECT_THROW(parse("-1", "%k", false), VeloxUserError);
  EXPECT_THROW(parse("123456789", "%k", false), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseClockHourOfHalfDay) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parse("7", "%h", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("12", "%h", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"),
      parse("1", "%h", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"),
      parse("10", "%h", false));

  // Clock hour of half day invalid
  EXPECT_THROW(parse("13", "%h", false), VeloxUserError);
  EXPECT_THROW(parse("0", "%h", false), VeloxUserError);
  EXPECT_THROW(parse("123456789", "%h", false), VeloxUserError);

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parse("7", "%I", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("12", "%I", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"),
      parse("1", "%I", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"),
      parse("10", "%I", false));

  // Clock hour of half day invalid
  EXPECT_THROW(parse("13", "%l", false), VeloxUserError);
  EXPECT_THROW(parse("0", "%l", false), VeloxUserError);
  EXPECT_THROW(parse("123456789", "%l", false), VeloxUserError);

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parse("7", "%l", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("12", "%l", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"),
      parse("1", "%l", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00"),
      parse("10", "%l", false));

  // Clock hour of half day invalid
  EXPECT_THROW(parse("13", "%l", false), VeloxUserError);
  EXPECT_THROW(parse("0", "%l", false), VeloxUserError);
  EXPECT_THROW(parse("123456789", "%l", false), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseHalfOfDay) {
  // Half of day has no effect if hour of day is provided
  // hour of day tests
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parse("7 PM", "%H %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parse("7 AM", "%H %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parse("7 pm", "%H %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parse("7 am", "%H %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("0 PM", "%H %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("0 AM", "%H %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("0 pm", "%H %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("0 am", "%H %p", false));

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parse("7 PM", "%k %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parse("7 AM", "%k %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parse("7 pm", "%k %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00"),
      parse("7 am", "%k %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("0 PM", "%k %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("0 AM", "%k %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("0 pm", "%k %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("0 am", "%k %p", false));

  // Half of day has effect if clockhour of halfday is provided
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00"),
      parse("1 PM", "%h %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"),
      parse("1 AM", "%h %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 18:00:00"),
      parse("6 PM", "%h %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 06:00:00"),
      parse("6 AM", "%h %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00"),
      parse("12 PM", "%h %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("12 AM", "%h %p", false));

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00"),
      parse("1 PM", "%I %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"),
      parse("1 AM", "%I %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 18:00:00"),
      parse("6 PM", "%I %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 06:00:00"),
      parse("6 AM", "%I %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00"),
      parse("12 PM", "%I %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("12 AM", "%I %p", false));

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00"),
      parse("1 PM", "%l %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"),
      parse("1 AM", "%l %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 18:00:00"),
      parse("6 PM", "%l %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 06:00:00"),
      parse("6 AM", "%l %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00"),
      parse("12 PM", "%l %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("12 AM", "%l %p", false));

  // time gives precendent to most recent time specifier
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00"),
      parse("0 1 AM", "%H %h %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00"),
      parse("12 1 PM", "%H %h %p", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("1 AM 0", "%h %p %H", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00"),
      parse("1 AM 12", "%h %p %H", false));
}

TEST_F(MysqlDateTimeTest, parseMinute) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:08:00"),
      parse("8", "%i", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:59:00"),
      parse("59", "%i", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("0/", "%i/", false));

  EXPECT_THROW(parse("60", "%i", false), VeloxUserError);
  EXPECT_THROW(parse("-1", "%i", false), VeloxUserError);
  EXPECT_THROW(parse("123456789", "%i", false), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseSecond) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:09"),
      parse("9", "%s", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:58"),
      parse("58", "%s", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("0/", "%s/", false));

  EXPECT_THROW(parse("60", "%s", false), VeloxUserError);
  EXPECT_THROW(parse("-1", "%s", false), VeloxUserError);
  EXPECT_THROW(parse("123456789", "%s", false), VeloxUserError);

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:09"),
      parse("9", "%S", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:58"),
      parse("58", "%S", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"),
      parse("0/", "%S/", false));

  EXPECT_THROW(parse("60", "%S", false), VeloxUserError);
  EXPECT_THROW(parse("-1", "%S", false), VeloxUserError);
  EXPECT_THROW(parse("123456789", "%S", false), VeloxUserError);
}

TEST_F(MysqlDateTimeTest, parseMixedYMDFormat) {
  // Common patterns found.
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 23:00:00"),
      parse("2021-01-04+23:00:00", "%Y-%m-%d+%H:%i:%s", false));

  EXPECT_EQ(
      util::fromTimestampString("2019-07-03 11:04:10"),
      parse("2019-07-03 11:04:10", "%Y-%m-%d %H:%i:%s", false));

  // Backwards, just for fun:
  EXPECT_EQ(
      util::fromTimestampString("2019-07-03 11:04:10"),
      parse("10:04:11 03-07-2019", "%s:%i:%H %d-%m-%Y", false));
}

TEST_F(MysqlDateTimeTest, parseMixedWeekFormat) {
  // Common patterns found.
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parse("2021 1 13:29:21.213", "%x %v %H:%i:%s.%f", false));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parse("2021 22 13:29:21.213", "%x %v %H:%i:%s.%f", false));

  // Day of week short text normal capitlization
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parse("2021 1 Mon 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parse("2021 22 Mon 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parse("2021 22 Thu 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  // Day of week long text normal capitlization
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parse("2021 1 Monday 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parse("2021 22 Monday 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parse("2021 22 Thursday 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  // Day of week short text upper case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parse("2021 1 MON 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parse("2021 22 MON 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parse("2021 22 THU 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  // Day of week long text upper case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parse("2021 1 MONDAY 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parse("2021 22 MONDAY 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parse("2021 22 THURSDAY 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  // Day of week short text lower case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parse("2021 1 mon 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parse("2021 22 mon 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parse("2021 22 thu 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  // Day of week long text lower case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213"),
      parse("2021 1 monday 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parse("2021 22 monday 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213"),
      parse("2021 22 thursday 13:29:21.213", "%x %v %W %H:%i:%s.%f", false));

  // Invalid day of week throw cases
  EXPECT_THROW(parse("mOn", "E"), VeloxUserError);
  EXPECT_THROW(parse("tuE", "E"), VeloxUserError);
  EXPECT_THROW(parse("WeD", "E"), VeloxUserError);
  EXPECT_THROW(parse("WEd", "E"), VeloxUserError);
  EXPECT_THROW(parse("MONday", "EEE"), VeloxUserError);
  EXPECT_THROW(parse("monDAY", "EEE"), VeloxUserError);
  EXPECT_THROW(parse("frIday", "EEE"), VeloxUserError);

  // Backwards, just for fun:
  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213"),
      parse("213.21:29:13 22 2021", "%f.%s:%i:%H %v %x", false));
}

TEST_F(MysqlDateTimeTest, parseFractionOfSecond) {
  // Assert on difference in milliseconds.
  EXPECT_NE(
      util::fromTimestampString("2022-02-23 12:15:00.223"),
      parse("2022-02-23T12:15:00.776", "%Y-%m-%dT%H:%i:%s.%f", false));

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.000"),
      parse("000", "%f", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.001"),
      parse("001", "%f", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.999"),
      parse("999", "%f", false));

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.045"),
      parse("045", "%f", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.450"),
      parse("45", "%f", false));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.450"),
      parse("45", "%f", false));

  EXPECT_THROW(parse("-1", "%f", false), VeloxUserError);
  EXPECT_THROW(parse("9999999", "%f", false), VeloxUserError);
}

} // namespace facebook::velox::functions
