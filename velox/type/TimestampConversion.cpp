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

// Timestamp conversion code inspired by DuckDB's date/time/timestamp conversion
// libraries. License below:

/*
 * Copyright 2018 DuckDB Contributors
 * (see https://github.com/cwida/duckdb/graphs/contributors)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "velox/type/TimestampConversion.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::util {

constexpr int32_t kLeapDays[] =
    {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
constexpr int32_t kNormalDays[] =
    {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
constexpr int32_t kCumulativeDays[] =
    {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
constexpr int32_t kCumulativeLeapDays[] =
    {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};

constexpr int32_t kCumulativeYearDays[] = {
    0,      365,    730,    1096,   1461,   1826,   2191,   2557,   2922,
    3287,   3652,   4018,   4383,   4748,   5113,   5479,   5844,   6209,
    6574,   6940,   7305,   7670,   8035,   8401,   8766,   9131,   9496,
    9862,   10227,  10592,  10957,  11323,  11688,  12053,  12418,  12784,
    13149,  13514,  13879,  14245,  14610,  14975,  15340,  15706,  16071,
    16436,  16801,  17167,  17532,  17897,  18262,  18628,  18993,  19358,
    19723,  20089,  20454,  20819,  21184,  21550,  21915,  22280,  22645,
    23011,  23376,  23741,  24106,  24472,  24837,  25202,  25567,  25933,
    26298,  26663,  27028,  27394,  27759,  28124,  28489,  28855,  29220,
    29585,  29950,  30316,  30681,  31046,  31411,  31777,  32142,  32507,
    32872,  33238,  33603,  33968,  34333,  34699,  35064,  35429,  35794,
    36160,  36525,  36890,  37255,  37621,  37986,  38351,  38716,  39082,
    39447,  39812,  40177,  40543,  40908,  41273,  41638,  42004,  42369,
    42734,  43099,  43465,  43830,  44195,  44560,  44926,  45291,  45656,
    46021,  46387,  46752,  47117,  47482,  47847,  48212,  48577,  48942,
    49308,  49673,  50038,  50403,  50769,  51134,  51499,  51864,  52230,
    52595,  52960,  53325,  53691,  54056,  54421,  54786,  55152,  55517,
    55882,  56247,  56613,  56978,  57343,  57708,  58074,  58439,  58804,
    59169,  59535,  59900,  60265,  60630,  60996,  61361,  61726,  62091,
    62457,  62822,  63187,  63552,  63918,  64283,  64648,  65013,  65379,
    65744,  66109,  66474,  66840,  67205,  67570,  67935,  68301,  68666,
    69031,  69396,  69762,  70127,  70492,  70857,  71223,  71588,  71953,
    72318,  72684,  73049,  73414,  73779,  74145,  74510,  74875,  75240,
    75606,  75971,  76336,  76701,  77067,  77432,  77797,  78162,  78528,
    78893,  79258,  79623,  79989,  80354,  80719,  81084,  81450,  81815,
    82180,  82545,  82911,  83276,  83641,  84006,  84371,  84736,  85101,
    85466,  85832,  86197,  86562,  86927,  87293,  87658,  88023,  88388,
    88754,  89119,  89484,  89849,  90215,  90580,  90945,  91310,  91676,
    92041,  92406,  92771,  93137,  93502,  93867,  94232,  94598,  94963,
    95328,  95693,  96059,  96424,  96789,  97154,  97520,  97885,  98250,
    98615,  98981,  99346,  99711,  100076, 100442, 100807, 101172, 101537,
    101903, 102268, 102633, 102998, 103364, 103729, 104094, 104459, 104825,
    105190, 105555, 105920, 106286, 106651, 107016, 107381, 107747, 108112,
    108477, 108842, 109208, 109573, 109938, 110303, 110669, 111034, 111399,
    111764, 112130, 112495, 112860, 113225, 113591, 113956, 114321, 114686,
    115052, 115417, 115782, 116147, 116513, 116878, 117243, 117608, 117974,
    118339, 118704, 119069, 119435, 119800, 120165, 120530, 120895, 121260,
    121625, 121990, 122356, 122721, 123086, 123451, 123817, 124182, 124547,
    124912, 125278, 125643, 126008, 126373, 126739, 127104, 127469, 127834,
    128200, 128565, 128930, 129295, 129661, 130026, 130391, 130756, 131122,
    131487, 131852, 132217, 132583, 132948, 133313, 133678, 134044, 134409,
    134774, 135139, 135505, 135870, 136235, 136600, 136966, 137331, 137696,
    138061, 138427, 138792, 139157, 139522, 139888, 140253, 140618, 140983,
    141349, 141714, 142079, 142444, 142810, 143175, 143540, 143905, 144271,
    144636, 145001, 145366, 145732, 146097,
};

namespace {

inline bool characterIsSpace(char c) {
  return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' ||
      c == '\r';
}

inline bool characterIsDigit(char c) {
  return c >= '0' && c <= '9';
}

bool parseDoubleDigit(
    const char* buf,
    size_t len,
    size_t& pos,
    int32_t& result) {
  if (pos < len && characterIsDigit(buf[pos])) {
    result = buf[pos++] - '0';
    if (pos < len && characterIsDigit(buf[pos])) {
      result = (buf[pos++] - '0') + result * 10;
    }
    return true;
  }
  return false;
}

bool isValidWeekDate(int32_t weekYear, int32_t weekOfYear, int32_t dayOfWeek) {
  if (dayOfWeek < 1 || dayOfWeek > 7) {
    return false;
  }
  if (weekOfYear < 1 || weekOfYear > 52) {
    return false;
  }
  if (weekYear < kMinYear || weekYear > kMaxYear) {
    return false;
  }
  return true;
}

bool tryParseDateString(
    const char* buf,
    size_t len,
    size_t& pos,
    int64_t& daysSinceEpoch,
    bool strict) {
  pos = 0;
  if (len == 0) {
    return false;
  }

  int32_t day = 0;
  int32_t month = -1;
  int32_t year = 0;
  bool yearneg = false;
  int sep;

  // Skip leading spaces.
  while (pos < len && characterIsSpace(buf[pos])) {
    pos++;
  }

  if (pos >= len) {
    return false;
  }
  if (buf[pos] == '-') {
    yearneg = true;
    pos++;
    if (pos >= len) {
      return false;
    }
  }
  if (!characterIsDigit(buf[pos])) {
    return false;
  }
  // First parse the year.
  for (; pos < len && characterIsDigit(buf[pos]); pos++) {
    year = (buf[pos] - '0') + year * 10;
    if (year > kMaxYear) {
      break;
    }
  }
  if (yearneg) {
    year = -year;
    if (year < kMinYear) {
      return false;
    }
  }

  if (pos >= len) {
    return false;
  }

  // Fetch the separator.
  sep = buf[pos++];
  if (sep != ' ' && sep != '-' && sep != '/' && sep != '\\') {
    // Invalid separator.
    return false;
  }

  // Parse the month.
  if (!parseDoubleDigit(buf, len, pos, month)) {
    return false;
  }

  if (pos >= len) {
    return false;
  }

  if (buf[pos++] != sep) {
    return false;
  }

  if (pos >= len) {
    return false;
  }

  // Now parse the day.
  if (!parseDoubleDigit(buf, len, pos, day)) {
    return false;
  }

  // Check for an optional trailing " (BC)".
  if (len - pos >= 5 && characterIsSpace(buf[pos]) && buf[pos + 1] == '(' &&
      buf[pos + 2] == 'B' && buf[pos + 3] == 'C' && buf[pos + 4] == ')') {
    if (yearneg || year == 0) {
      return false;
    }
    year = -year + 1;
    pos += 5;

    if (year < kMinYear) {
      return false;
    }
  }

  // In strict mode, check remaining string for non-space characters.
  if (strict) {
    // Skip trailing spaces.
    while (pos < len && characterIsSpace(buf[pos])) {
      pos++;
    }
    // Check position. if end was not reached, non-space chars remaining.
    if (pos < len) {
      return false;
    }
  } else {
    // In non-strict mode, check for any direct trailing digits.
    if (pos < len && characterIsDigit(buf[pos])) {
      return false;
    }
  }

  daysSinceEpoch = daysSinceEpochFromDate(year, month, day);
  return true;
}

// String format is hh:mm:ss.microseconds (microseconds are optional).
// ISO 8601
bool tryParseTimeString(
    const char* buf,
    size_t len,
    size_t& pos,
    int64_t& result,
    bool strict) {
  int32_t hour = -1, min = -1, sec = -1, micros = -1;
  pos = 0;

  if (len == 0) {
    return false;
  }

  // Skip leading spaces.
  while (pos < len && characterIsSpace(buf[pos])) {
    pos++;
  }

  if (pos >= len) {
    return false;
  }

  if (!characterIsDigit(buf[pos])) {
    return false;
  }

  if (!parseDoubleDigit(buf, len, pos, hour)) {
    return false;
  }
  if (hour < 0 || hour >= 24) {
    return false;
  }

  if (pos >= len) {
    return false;
  }

  // Fetch the separator.
  int sep = buf[pos++];
  if (sep != ':') {
    // Invalid separator.
    return false;
  }

  if (!parseDoubleDigit(buf, len, pos, min)) {
    return false;
  }
  if (min < 0 || min >= 60) {
    return false;
  }

  if (pos >= len) {
    return false;
  }

  if (buf[pos++] != sep) {
    return false;
  }

  if (!parseDoubleDigit(buf, len, pos, sec)) {
    return false;
  }
  if (sec < 0 || sec > 60) {
    return false;
  }

  micros = 0;
  if (pos < len && buf[pos] == '.') {
    pos++;
    // We expect microseconds.
    int32_t mult = 100000;
    for (; pos < len && characterIsDigit(buf[pos]); pos++, mult /= 10) {
      if (mult > 0) {
        micros += (buf[pos] - '0') * mult;
      }
    }
  }

  // In strict mode, check remaining string for non-space characters.
  if (strict) {
    // Skip trailing spaces.
    while (pos < len && characterIsSpace(buf[pos])) {
      pos++;
    }

    // Check position. If end was not reached, non-space chars remaining.
    if (pos < len) {
      return false;
    }
  }
  result = fromTime(hour, min, sec, micros);
  return true;
}

bool tryParseUTCOffsetString(
    const char* buf,
    size_t& pos,
    size_t len,
    int& hourOffset,
    int& minuteOffset) {
  minuteOffset = 0;
  size_t curpos = pos;

  // Parse the next 3 characters.
  if (curpos + 3 > len) {
    // No characters left to parse.
    return false;
  }

  char sign_char = buf[curpos];
  if (sign_char != '+' && sign_char != '-') {
    // Expected either + or -
    return false;
  }

  curpos++;
  if (!characterIsDigit(buf[curpos]) || !characterIsDigit(buf[curpos + 1])) {
    // Expected +HH or -HH
    return false;
  }

  hourOffset = (buf[curpos] - '0') * 10 + (buf[curpos + 1] - '0');

  if (sign_char == '-') {
    hourOffset = -hourOffset;
  }
  curpos += 2;

  // Optional minute specifier: expected either "MM" or ":MM".
  if (curpos >= len) {
    // Done, nothing left.
    pos = curpos;
    return true;
  }
  if (buf[curpos] == ':') {
    curpos++;
  }

  if (curpos + 2 > len || !characterIsDigit(buf[curpos]) ||
      !characterIsDigit(buf[curpos + 1])) {
    // No MM specifier.
    pos = curpos;
    return true;
  }

  // We have an MM specifier: parse it.
  minuteOffset = (buf[curpos] - '0') * 10 + (buf[curpos + 1] - '0');
  if (sign_char == '-') {
    minuteOffset = -minuteOffset;
  }
  pos = curpos + 2;
  return true;
}

} // namespace

bool isLeapYear(int32_t year) {
  return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

bool isValidDate(int32_t year, int32_t month, int32_t day) {
  if (month < 1 || month > 12) {
    return false;
  }
  if (year < kMinYear || year > kMaxYear) {
    return false;
  }
  if (day < 1) {
    return false;
  }
  return isLeapYear(year) ? day <= kLeapDays[month] : day <= kNormalDays[month];
}

bool isValidDayOfYear(int32_t year, int32_t dayOfYear) {
  if (year < kMinYear || year > kMaxYear) {
    return false;
  }
  if (dayOfYear < 1 || dayOfYear > 365 + isLeapYear(year)) {
    return false;
  }
  return true;
}

int32_t getMaxDayOfMonth(int32_t year, int32_t month) {
  return isLeapYear(year) ? kLeapDays[month] : kNormalDays[month];
}

int64_t daysSinceEpochFromDate(int32_t year, int32_t month, int32_t day) {
  int64_t daysSinceEpoch = 0;

  if (!isValidDate(year, month, day)) {
    VELOX_USER_FAIL("Date out of range: {}-{}-{}", year, month, day);
  }
  while (year < 1970) {
    year += kYearInterval;
    daysSinceEpoch -= kDaysPerYearInterval;
  }
  while (year >= 2370) {
    year -= kYearInterval;
    daysSinceEpoch += kDaysPerYearInterval;
  }
  daysSinceEpoch += kCumulativeYearDays[year - 1970];
  daysSinceEpoch += isLeapYear(year) ? kCumulativeLeapDays[month - 1]
                                     : kCumulativeDays[month - 1];
  daysSinceEpoch += day - 1;
  return daysSinceEpoch;
}

int64_t daysSinceEpochFromWeekDate(
    int32_t weekYear,
    int32_t weekOfYear,
    int32_t dayOfWeek) {
  if (!isValidWeekDate(weekYear, weekOfYear, dayOfWeek)) {
    VELOX_USER_FAIL(
        "Date out of range: {}-{}-{}", weekYear, weekOfYear, dayOfWeek);
  }

  int64_t daysSinceEpochOfJanFourth = daysSinceEpochFromDate(weekYear, 1, 4);
  int32_t firstDayOfWeekYear =
      extractISODayOfTheWeek(daysSinceEpochOfJanFourth);

  return daysSinceEpochOfJanFourth - (firstDayOfWeekYear - 1) +
      7 * (weekOfYear - 1) + dayOfWeek - 1;
}

int64_t daysSinceEpochFromDayOfYear(int32_t year, int32_t dayOfYear) {
  if (!isValidDayOfYear(year, dayOfYear)) {
    VELOX_USER_FAIL("Day of year out of range: {}", dayOfYear);
  }
  int64_t startOfYear = daysSinceEpochFromDate(year, 1, 1);
  return startOfYear + (dayOfYear - 1);
}

int64_t fromDateString(const char* str, size_t len) {
  int64_t daysSinceEpoch;
  size_t pos = 0;

  if (!tryParseDateString(str, len, pos, daysSinceEpoch, true)) {
    VELOX_USER_FAIL(
        "Unable to parse date value: \"{}\", expected format is (YYYY-MM-DD)",
        std::string(str, len));
  }
  return daysSinceEpoch;
}

int32_t extractISODayOfTheWeek(int32_t daysSinceEpoch) {
  // date of 0 is 1970-01-01, which was a Thursday (4)
  // -7 = 4
  // -6 = 5
  // -5 = 6
  // -4 = 7
  // -3 = 1
  // -2 = 2
  // -1 = 3
  // 0  = 4
  // 1  = 5
  // 2  = 6
  // 3  = 7
  // 4  = 1
  // 5  = 2
  // 6  = 3
  // 7  = 4
  if (daysSinceEpoch < 0) {
    // negative date: start off at 4 and cycle downwards
    return (7 - ((-int64_t(daysSinceEpoch) + 3) % 7));
  } else {
    // positive date: start off at 4 and cycle upwards
    return ((int64_t(daysSinceEpoch) + 3) % 7) + 1;
  }
}

int64_t
fromTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds) {
  int64_t result;
  result = hour; // hours
  result = result * kMinsPerHour + minute; // hours -> minutes
  result = result * kSecsPerMinute + second; // minutes -> seconds
  result = result * kMicrosPerSec + microseconds; // seconds -> microseconds
  return result;
}

int64_t fromTimeString(const char* str, size_t len) {
  int64_t microsSinceMidnight;
  size_t pos;

  if (!tryParseTimeString(str, len, pos, microsSinceMidnight, true)) {
    VELOX_USER_FAIL(
        "Unable to parse time value: \"{}\", "
        "expected format is (HH:MM:SS[.MS])",
        std::string(str, len));
  }
  return microsSinceMidnight;
}

Timestamp fromDatetime(int64_t daysSinceEpoch, int64_t microsSinceMidnight) {
  int64_t secondsSinceEpoch =
      static_cast<int64_t>(daysSinceEpoch) * kSecsPerDay;
  secondsSinceEpoch += microsSinceMidnight / kMicrosPerSec;
  return Timestamp(
      secondsSinceEpoch,
      (microsSinceMidnight % kMicrosPerSec) * kNanosPerMicro);
}

namespace {

void parserError(const char* str, size_t len) {
  VELOX_USER_FAIL(
      "Unable to parse timestamp value: \"{}\", "
      "expected format is (YYYY-MM-DD HH:MM:SS[.MS])",
      std::string(str, len));
}

} // namespace

Timestamp fromTimestampString(const char* str, size_t len) {
  size_t pos;
  int64_t daysSinceEpoch;
  int64_t microsSinceMidnight;

  if (!tryParseDateString(str, len, pos, daysSinceEpoch, false)) {
    parserError(str, len);
  }

  if (pos == len) {
    // No time: only a date.
    return fromDatetime(daysSinceEpoch, 0);
  }

  // Try to parse a time field.
  if (str[pos] == ' ' || str[pos] == 'T') {
    pos++;
  }

  size_t timePos = 0;
  if (!tryParseTimeString(
          str + pos, len - pos, timePos, microsSinceMidnight, false)) {
    parserError(str, len);
  }

  pos += timePos;
  auto timestamp = fromDatetime(daysSinceEpoch, microsSinceMidnight);

  if (pos < len) {
    // Skip a "Z" at the end (as per the ISO 8601 specs).
    if (str[pos] == 'Z') {
      pos++;
    }
    int hourOffset, minuteOffset;
    if (tryParseUTCOffsetString(str, pos, len, hourOffset, minuteOffset)) {
      int32_t secondOffset =
          (hourOffset * kSecsPerHour) + (minuteOffset * kSecsPerMinute);
      timestamp = Timestamp(
          timestamp.getSeconds() - secondOffset, timestamp.getNanos());
    }

    // Skip any spaces at the end.
    while (pos < len && characterIsSpace(str[pos])) {
      pos++;
    }
    if (pos < len) {
      parserError(str, len);
    }
  }
  return timestamp;
}

} // namespace facebook::velox::util
