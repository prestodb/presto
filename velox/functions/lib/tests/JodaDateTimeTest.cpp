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
#include "velox/common/base/Exceptions.h"
#include "velox/type/TimestampConversion.h"
#include "velox/type/tz/TimeZoneMap.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace facebook::velox;

namespace facebook::velox::functions {

bool operator==(const JodaPattern& a, const JodaPattern& b) {
  return a.specifier == b.specifier && a.count == b.count;
}

bool operator==(const JodaToken& a, const JodaToken& b) {
  if (a.type != b.type)
    return false;
  switch (a.type) {
    case JodaToken::Type::kPattern:
      return a.pattern == b.pattern;
    case JodaToken::Type::kLiteral:
      return a.literal == b.literal;
  }
  VELOX_UNREACHABLE();
}

namespace {

class JodaDateTimeTest : public testing::Test {
 protected:
  auto parseAll(const std::string& input, const std::string& format) {
    return JodaFormatter(format).parse(input);
  }

  Timestamp parse(const std::string& input, const std::string& format) {
    return parseAll(input, format).timestamp;
  }

  // Parses and returns the timezone converted back to string, to ease
  // verifiability.
  std::string parseTZ(const std::string& input, const std::string& format) {
    auto result = JodaFormatter(format).parse(input);
    if (result.timezoneId == 0) {
      return "+00:00";
    }
    return util::getTimeZoneName(result.timezoneId);
  }
};

TEST_F(JodaDateTimeTest, tokens) {
  std::vector<JodaToken> expected;

  expected = {JodaToken(" ")};
  EXPECT_EQ(expected, JodaFormatter(" ").tokens());

  expected = {
      JodaToken(" "),
      JodaToken(JodaPattern{JodaFormatSpecifier::YEAR_OF_ERA, 1}),
  };
  EXPECT_EQ(expected, JodaFormatter(" Y").tokens());

  expected = {
      JodaToken(JodaPattern{JodaFormatSpecifier::YEAR_OF_ERA, 2}),
      JodaToken(" "),
  };
  EXPECT_EQ(expected, JodaFormatter("YY ").tokens());

  expected = {JodaToken(" 132&2618*673 *--+= }{[]\\:")};
  EXPECT_EQ(expected, JodaFormatter(" 132&2618*673 *--+= }{[]\\:").tokens());

  expected = {
      JodaToken("   "),
      JodaToken(JodaPattern{JodaFormatSpecifier::YEAR_OF_ERA, 4}),
      JodaToken(" &^  "),
  };
  EXPECT_EQ(expected, JodaFormatter("   YYYY &^  ").tokens());

  expected = {
      JodaToken(JodaPattern{JodaFormatSpecifier::YEAR_OF_ERA, 1}),
      JodaToken("  % & "),
      JodaToken(JodaPattern{JodaFormatSpecifier::YEAR_OF_ERA, 3}),
      JodaToken(" "),
      JodaToken(JodaPattern{JodaFormatSpecifier::YEAR_OF_ERA, 5}),
  };
  EXPECT_EQ(expected, JodaFormatter("Y  % & YYY YYYYY").tokens());

  expected = {
      JodaToken(JodaPattern{JodaFormatSpecifier::YEAR_OF_ERA, 1}),
      JodaToken(" T"),
  };
  EXPECT_EQ(expected, JodaFormatter("Y 'T'").tokens());

  expected = {JodaToken("1'2")};
  EXPECT_EQ(expected, JodaFormatter("1''2").tokens());

  expected = {
      JodaToken(JodaPattern{JodaFormatSpecifier::YEAR_OF_ERA, 4}),
      JodaToken("-"),
      JodaToken(JodaPattern{JodaFormatSpecifier::MONTH_OF_YEAR, 2}),
      JodaToken("-"),
      JodaToken(JodaPattern{JodaFormatSpecifier::DAY_OF_MONTH, 2}),
      JodaToken(" "),
      JodaToken(JodaPattern{JodaFormatSpecifier::MINUTE_OF_HOUR, 1}),
      JodaToken(" "),
      JodaToken(JodaPattern{JodaFormatSpecifier::YEAR_OF_ERA, 10}),
  };
  EXPECT_EQ(expected, JodaFormatter("YYYY-MM-dd m YYYYYYYYYY").tokens());
}

TEST_F(JodaDateTimeTest, invalid) {
  // Format:
  EXPECT_THROW(JodaFormatter(""), VeloxUserError);
  EXPECT_THROW(JodaFormatter("p"), VeloxUserError);
  EXPECT_THROW(JodaFormatter("P"), VeloxUserError);
  EXPECT_THROW(JodaFormatter("YDM u"), VeloxUserError);

  // Parse:
  EXPECT_THROW(parse("", ""), VeloxUserError);
  EXPECT_THROW(parse(" ", ""), VeloxUserError);
  EXPECT_THROW(parse("", " "), VeloxUserError);
  EXPECT_THROW(parse("", "Y '"), VeloxUserError);
}

TEST_F(JodaDateTimeTest, parseYearOfEra) {
  // By the default, assume epoch.
  EXPECT_EQ(util::fromTimestampString("1970-01-01"), parse(" ", " "));

  // Number of times the token is repeated doesn't change the parsing behavior.
  EXPECT_EQ(util::fromTimestampString("2134-01-01"), parse("2134", "Y"));
  EXPECT_EQ(util::fromTimestampString("2134-01-01"), parse("2134", "YYYYYYYY"));

  // Probe the year range. Joda only supports positive years.
  EXPECT_EQ(util::fromTimestampString("294247-01-01"), parse("294247", "Y"));
  EXPECT_EQ(util::fromTimestampString("0001-01-01"), parse("1", "Y"));
  EXPECT_THROW(parse("292278994", "Y"), VeloxUserError);
  EXPECT_THROW(parse("0", "Y"), VeloxUserError);
  EXPECT_THROW(parse("-1", "Y"), VeloxUserError);
  EXPECT_THROW(parse("  ", " Y "), VeloxUserError);
  EXPECT_THROW(parse(" 1 2", "Y Y"), VeloxUserError);

  // Last token read overwrites:
  EXPECT_EQ(
      util::fromTimestampString("0005-01-01"), parse("1 2 3 4 5", "Y Y Y Y Y"));
}

// Same semantic as YEAR_OF_ERA, except that it accepts zero and negative years.
TEST_F(JodaDateTimeTest, parseYear) {
  EXPECT_EQ(util::fromTimestampString("123-01-01"), parse("123", "y"));
  EXPECT_EQ(util::fromTimestampString("321-01-01"), parse("321", "yyyyyyyy"));

  EXPECT_EQ(util::fromTimestampString("0-01-01"), parse("0", "y"));
  EXPECT_EQ(util::fromTimestampString("-1-01-01"), parse("-1", "y"));
  EXPECT_EQ(util::fromTimestampString("-1234-01-01"), parse("-1234", "y"));

  // Last token read overwrites:
  EXPECT_EQ(util::fromTimestampString("0-01-01"), parse("123 0", "Y y"));
}

TEST_F(JodaDateTimeTest, parseMonth) {
  // Joda has this weird behavior where if minute or hour is specified, year
  // falls back to 2000, instead of epoch (1970)  ¯\_(ツ)_/¯
  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parse("1", "M"));
  EXPECT_EQ(util::fromTimestampString("2000-07-01"), parse(" 7", " MM"));
  EXPECT_EQ(util::fromTimestampString("2000-11-01"), parse("11-", "M-"));
  EXPECT_EQ(util::fromTimestampString("2000-12-01"), parse("-12-", "-M-"));

  EXPECT_THROW(parse("0", "M"), VeloxUserError);
  EXPECT_THROW(parse("13", "M"), VeloxUserError);
  EXPECT_THROW(parse("12345", "M"), VeloxUserError);
}

TEST_F(JodaDateTimeTest, parseDay) {
  EXPECT_EQ(util::fromTimestampString("2000-01-01"), parse("1", "d"));
  EXPECT_EQ(util::fromTimestampString("2000-01-07"), parse("7 ", "dd "));
  EXPECT_EQ(util::fromTimestampString("2000-01-11"), parse("/11", "/dd"));
  EXPECT_EQ(util::fromTimestampString("2000-01-31"), parse("/31/", "/d/"));

  EXPECT_THROW(parse("0", "d"), VeloxUserError);
  EXPECT_THROW(parse("32", "d"), VeloxUserError);
  EXPECT_THROW(parse("12345", "d"), VeloxUserError);

  EXPECT_THROW(parse("02-31", "M-d"), VeloxUserError);
  EXPECT_THROW(parse("04-31", "M-d"), VeloxUserError);

  // Probe around leap year.
  EXPECT_EQ(
      util::fromTimestampString("2000-02-29"), parse("2000-02-29", "Y-M-d"));
  EXPECT_THROW(parse("2001-02-29", "Y-M-d"), VeloxUserError);
}

TEST_F(JodaDateTimeTest, parseHour) {
  EXPECT_EQ(util::fromTimestampString("1970-01-01 07:00:00"), parse("7", "H"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 23:00:00"), parse("23", "HH"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("0", "HHH"));

  EXPECT_THROW(parse("24", "H"), VeloxUserError);
  EXPECT_THROW(parse("-1", "H"), VeloxUserError);
  EXPECT_THROW(parse("123456789", "H"), VeloxUserError);
}

TEST_F(JodaDateTimeTest, parseMinute) {
  EXPECT_EQ(util::fromTimestampString("1970-01-01 00:08:00"), parse("8", "m"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:59:00"), parse("59", "mm"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("0/", "mmm/"));

  EXPECT_THROW(parse("60", "m"), VeloxUserError);
  EXPECT_THROW(parse("-1", "m"), VeloxUserError);
  EXPECT_THROW(parse("123456789", "m"), VeloxUserError);
}

TEST_F(JodaDateTimeTest, parseSecond) {
  EXPECT_EQ(util::fromTimestampString("1970-01-01 00:00:09"), parse("9", "s"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:58"), parse("58", "ss"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00"), parse("0/", "s/"));

  EXPECT_THROW(parse("60", "s"), VeloxUserError);
  EXPECT_THROW(parse("-1", "s"), VeloxUserError);
  EXPECT_THROW(parse("123456789", "s"), VeloxUserError);
}

TEST_F(JodaDateTimeTest, parseTimezone) {
  // Broken timezone offfsets; allowed formats are "+00:00", "+00", "+0000" or
  // "Z".
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
  EXPECT_EQ("+00:00", parseTZ("Z", "ZZ"));
  EXPECT_EQ("+00:00", parseTZ("Z", "Z"));

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

TEST_F(JodaDateTimeTest, parseMixed) {
  // Common patterns found.
  EXPECT_EQ(
      util::fromTimestampString("2021-11-04 23:00:00"),
      parse("2021-11-04+23:00", "YYYY-MM-dd+HH:mm"));

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
}

TEST_F(JodaDateTimeTest, parseFractionOfSecond) {
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

  // Z in the input means GMT in Joda.
  EXPECT_EQ(
      util::fromTimestampString("2022-07-29 20:03:54.667"),
      parse("2022-07-29T20:03:54.667Z", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
}

} // namespace

} // namespace facebook::velox::functions
