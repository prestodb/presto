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
#include "velox/type/Timestamp.h"
#include <charconv>
#include <chrono>
#include "velox/common/base/CountBits.h"
#include "velox/external/date/tz.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox {
namespace {

// Assuming tzID is in [1, 1680] range.
// tzID - PrestoDB time zone ID.
inline int64_t getPrestoTZOffsetInSeconds(int16_t tzID) {
  // TODO(spershin): Maybe we need something better if we can (we could use
  //  precomputed vector for PrestoDB timezones, for instance).

  // PrestoDb time zone ids require some custom code.
  // Mapping is 1-based and covers [-14:00, +14:00] range without 00:00.
  return ((tzID <= 840) ? (tzID - 841) : (tzID - 840)) * 60;
}

} // namespace

// static
Timestamp Timestamp::now() {
  auto now = std::chrono::system_clock::now();
  auto epochMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                     now.time_since_epoch())
                     .count();
  return fromMillis(epochMs);
}

void Timestamp::toGMT(const date::time_zone& zone) {
  // Magic number -2^39 + 24*3600. This number and any number lower than that
  // will cause time_zone::to_sys() to SIGABRT. We don't want that to happen.
  VELOX_USER_CHECK_GT(
      seconds_,
      -1096193779200l + 86400l,
      "Timestamp seconds out of range for time zone adjustment");

  VELOX_USER_CHECK_LE(
      seconds_,
      kMaxSeconds,
      "Timestamp seconds out of range for time zone adjustment");

  date::local_time<std::chrono::seconds> localTime{
      std::chrono::seconds(seconds_)};
  std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds>
      sysTime;
  try {
    sysTime = zone.to_sys(localTime);
  } catch (const date::ambiguous_local_time&) {
    // If the time is ambiguous, pick the earlier possibility to be consistent
    // with Presto.
    sysTime = zone.to_sys(localTime, date::choose::earliest);
  } catch (const date::nonexistent_local_time& error) {
    // If the time does not exist, fail the conversion.
    VELOX_USER_FAIL(error.what());
  }
  seconds_ = sysTime.time_since_epoch().count();
}

void Timestamp::toGMT(int16_t tzID) {
  if (tzID == 0) {
    // No conversion required for time zone id 0, as it is '+00:00'.
  } else if (tzID <= 1680) {
    seconds_ -= getPrestoTZOffsetInSeconds(tzID);
  } else {
    // Other ids go this path.
    toGMT(*date::locate_zone(util::getTimeZoneName(tzID)));
  }
}

namespace {
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
        "Timestamp is outside of supported range of [{}-{}-{}, {}-{}-{}]",
        (int)date::year::min(),
        "01",
        "01",
        (int)date::year::max(),
        "12",
        "31");
  }
}
} // namespace

std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
Timestamp::toTimePoint(bool allowOverflow) const {
  using namespace std::chrono;
  auto tp = time_point<system_clock, milliseconds>(
      milliseconds(allowOverflow ? toMillisAllowOverflow() : toMillis()));
  validateTimePoint(tp);
  return tp;
}

void Timestamp::toTimezone(const date::time_zone& zone) {
  auto tp = toTimePoint();
  auto epoch = zone.to_local(tp).time_since_epoch();
  // NOTE: Round down to get the seconds of the current time point.
  seconds_ = std::chrono::floor<std::chrono::seconds>(epoch).count();
}

void Timestamp::toTimezone(int16_t tzID) {
  if (tzID == 0) {
    // No conversion required for time zone id 0, as it is '+00:00'.
  } else if (tzID <= 1680) {
    seconds_ += getPrestoTZOffsetInSeconds(tzID);
  } else {
    // Other ids go this path.
    toTimezone(*date::locate_zone(util::getTimeZoneName(tzID)));
  }
}

const date::time_zone& Timestamp::defaultTimezone() {
  static const date::time_zone* kDefault = ({
    // TODO: We are hard-coding PST/PDT here to be aligned with the current
    // behavior in DWRF reader/writer.  Once they are fixed, we can use
    // date::current_zone() here.
    //
    // See https://github.com/facebookincubator/velox/issues/8127
    auto* tz = date::locate_zone("America/Los_Angeles");
    VELOX_CHECK_NOT_NULL(tz);
    tz;
  });
  return *kDefault;
}

namespace {

constexpr int kTmYearBase = 1900;
constexpr int64_t kLeapYearOffset = 4000000000ll;

inline bool isLeap(int64_t y) {
  return y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
}

inline int64_t leapThroughEndOf(int64_t y) {
  // Add a large offset to make the calculation for negative years correct.
  y += kLeapYearOffset;
  VELOX_DCHECK_GE(y, 0);
  return y / 4 - y / 100 + y / 400;
}

const int monthLengths[][12] = {
    {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
    {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
};

// clang-format off
const char intToStr[][3] = {
    "00", "01", "02", "03", "04", "05", "06", "07", "08", "09",
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
    "20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
    "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
    "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
    "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
    "60", "61",
};
// clang-format on

void appendSmallInt(int n, std::string& out) {
  VELOX_DCHECK_LE(n, 61);
  out.append(intToStr[n], 2);
}
} // namespace

bool Timestamp::epochToUtc(int64_t epoch, std::tm& tm) {
  constexpr int kSecondsPerHour = 3600;
  constexpr int kSecondsPerDay = 24 * kSecondsPerHour;
  constexpr int kDaysPerYear = 365;
  int64_t days = epoch / kSecondsPerDay;
  int64_t rem = epoch % kSecondsPerDay;
  while (rem < 0) {
    rem += kSecondsPerDay;
    --days;
  }
  tm.tm_hour = rem / kSecondsPerHour;
  rem = rem % kSecondsPerHour;
  tm.tm_min = rem / 60;
  tm.tm_sec = rem % 60;
  tm.tm_wday = (4 + days) % 7;
  if (tm.tm_wday < 0) {
    tm.tm_wday += 7;
  }
  int64_t y = 1970;
  if (y + days / kDaysPerYear <= -kLeapYearOffset + 10) {
    return false;
  }
  bool leapYear;
  while (days < 0 || days >= kDaysPerYear + (leapYear = isLeap(y))) {
    auto newy = y + days / kDaysPerYear - (days < 0);
    days -= (newy - y) * kDaysPerYear + leapThroughEndOf(newy - 1) -
        leapThroughEndOf(y - 1);
    y = newy;
  }
  y -= kTmYearBase;
  if (y > std::numeric_limits<decltype(tm.tm_year)>::max() ||
      y < std::numeric_limits<decltype(tm.tm_year)>::min()) {
    return false;
  }
  tm.tm_year = y;
  tm.tm_yday = days;
  auto* ip = monthLengths[leapYear];
  for (tm.tm_mon = 0; days >= ip[tm.tm_mon]; ++tm.tm_mon) {
    days = days - ip[tm.tm_mon];
  }
  tm.tm_mday = days + 1;
  tm.tm_isdst = 0;
  return true;
}

StringView Timestamp::tmToStringView(
    const std::tm& tmValue,
    uint64_t nanos,
    const TimestampToStringOptions& options,
    char* const startPosition) {
  VELOX_DCHECK_LT(nanos, 1'000'000'000);

  const auto appendDigits = [](const int value,
                               const std::optional<uint32_t> minWidth,
                               char* const position) {
    const auto numDigits = countDigits(value);
    uint32_t offset = 0;
    // Append leading zeros when there is the requirement for minumum width.
    if (minWidth.has_value() && numDigits < minWidth.value()) {
      const auto leadingZeros = minWidth.value() - numDigits;
      std::memset(position, '0', leadingZeros);
      offset += leadingZeros;
    }
    const auto [endPosition, errorCode] =
        std::to_chars(position + offset, position + offset + numDigits, value);
    VELOX_DCHECK_EQ(
        errorCode,
        std::errc(),
        "Failed to convert value to varchar: {}.",
        std::make_error_code(errorCode).message());
    offset += numDigits;
    return offset;
  };

  char* writePosition = startPosition;
  if (options.mode != TimestampToStringOptions::Mode::kTimeOnly) {
    int year = kTmYearBase + tmValue.tm_year;
    const bool leadingPositiveSign = options.leadingPositiveSign && year > 9999;
    const bool negative = year < 0;

    // Sign.
    if (negative) {
      *writePosition++ = '-';
      year = -year;
    } else if (leadingPositiveSign) {
      *writePosition++ = '+';
    }

    // Year.
    writePosition += appendDigits(
        year,
        options.zeroPaddingYear ? std::optional<uint32_t>(4) : std::nullopt,
        writePosition);

    // Month.
    *writePosition++ = '-';
    writePosition += appendDigits(1 + tmValue.tm_mon, 2, writePosition);

    // Day.
    *writePosition++ = '-';
    writePosition += appendDigits(tmValue.tm_mday, 2, writePosition);

    if (options.mode == TimestampToStringOptions::Mode::kDateOnly) {
      return StringView(startPosition, writePosition - startPosition);
    }
    *writePosition++ = options.dateTimeSeparator;
  }

  // Hour.
  writePosition += appendDigits(tmValue.tm_hour, 2, writePosition);

  // Minute.
  *writePosition++ = ':';
  writePosition += appendDigits(tmValue.tm_min, 2, writePosition);

  // Second.
  *writePosition++ = ':';
  writePosition += appendDigits(tmValue.tm_sec, 2, writePosition);

  if (options.precision == TimestampToStringOptions::Precision::kMilliseconds) {
    nanos /= 1'000'000;
  } else if (
      options.precision == TimestampToStringOptions::Precision::kMicroseconds) {
    nanos /= 1'000;
  }
  if (options.skipTrailingZeros && nanos == 0) {
    return StringView(startPosition, writePosition - startPosition);
  }

  // Fractional part.
  *writePosition++ = '.';
  // Append leading zeros.
  const auto numDigits = countDigits(nanos);
  const auto precisionWidth = static_cast<int8_t>(options.precision);
  std::memset(writePosition, '0', precisionWidth - numDigits);
  writePosition += precisionWidth - numDigits;

  // Append the remaining numeric digits.
  if (options.skipTrailingZeros) {
    std::optional<uint32_t> nonZeroOffset = std::nullopt;
    int32_t offset = numDigits - 1;
    // Write non-zero digits from end to start.
    while (nanos > 0) {
      if (nonZeroOffset.has_value() || nanos % 10 != 0) {
        *(writePosition + offset) = '0' + nanos % 10;
        if (!nonZeroOffset.has_value()) {
          nonZeroOffset = offset;
        }
      }
      --offset;
      nanos /= 10;
    }
    writePosition += nonZeroOffset.value() + 1;
  } else {
    const auto [position, errorCode] =
        std::to_chars(writePosition, writePosition + numDigits, nanos);
    VELOX_DCHECK_EQ(
        errorCode,
        std::errc(),
        "Failed to convert fractional part to chars: {}.",
        std::make_error_code(errorCode).message());
    writePosition = position;
  }
  return StringView(startPosition, writePosition - startPosition);
}

StringView Timestamp::tsToStringView(
    const Timestamp& ts,
    const TimestampToStringOptions& options,
    char* const startPosition) {
  std::tm tmValue;
  VELOX_USER_CHECK(
      epochToUtc(ts.getSeconds(), tmValue),
      "Can't convert seconds to time: {}",
      ts.getSeconds());
  const uint64_t nanos = ts.getNanos();
  return tmToStringView(tmValue, nanos, options, startPosition);
}

std::string::size_type getMaxStringLength(
    const TimestampToStringOptions& options) {
  const auto precisionWidth = static_cast<int8_t>(options.precision);
  switch (options.mode) {
    case TimestampToStringOptions::Mode::kDateOnly:
      // Date format is %y-mm-dd, where y has 10 digits at maximum for int32.
      // Possible sign is considered.
      return 17;
    case TimestampToStringOptions::Mode::kTimeOnly:
      // hh:mm:ss.precision
      return 9 + precisionWidth;
    case TimestampToStringOptions::Mode::kFull:
      // Timestamp format is %y-%m-%dT%h:%m:%s.precision, where y has 10 digits
      // at maximum for int32. Possible sign is considered.
      return 27 + precisionWidth;
    default:
      VELOX_UNREACHABLE();
  }
}

void parseTo(folly::StringPiece in, ::facebook::velox::Timestamp& out) {
  // TODO Implement
}

} // namespace facebook::velox

namespace std {
std::string to_string(const ::facebook::velox::Timestamp& ts) {
  return ts.toString();
}

} // namespace std
