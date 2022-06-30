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
#include "velox/common/base/Exceptions.h"
#include "velox/external/date/tz.h"
#include "velox/functions/lib/DateTimeFormatterBuilder.h"
#include "velox/type/TimestampConversion.h"

#include <gtest/gtest.h>

using namespace facebook::velox;

namespace facebook::velox::functions {

class DateTimeFormatterTest : public testing::Test {
 protected:
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

} // namespace facebook::velox::functions
