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

// static
Timestamp Timestamp::fromDaysAndNanos(int32_t days, int64_t nanos) {
  int64_t seconds =
      (days - kJulianToUnixEpochDays) * kSecondsInDay + nanos / kNanosInSecond;
  int64_t remainingNanos = nanos % kNanosInSecond;
  if (remainingNanos < 0) {
    remainingNanos += kNanosInSecond;
    seconds--;
  }
  return Timestamp(seconds, remainingNanos);
}

// static
Timestamp Timestamp::now() {
  auto now = std::chrono::system_clock::now();
  auto epochMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                     now.time_since_epoch())
                     .count();
  return fromMillis(epochMs);
}

void Timestamp::toGMT(const tz::TimeZone& zone) {
  std::chrono::seconds sysSeconds;

  try {
    sysSeconds = zone.to_sys(std::chrono::seconds(seconds_));
  } catch (const date::ambiguous_local_time&) {
    // If the time is ambiguous, pick the earlier possibility to be consistent
    // with Presto.
    sysSeconds = zone.to_sys(
        std::chrono::seconds(seconds_), tz::TimeZone::TChoose::kEarliest);
  } catch (const date::nonexistent_local_time& error) {
    // If the time does not exist, fail the conversion.
    VELOX_USER_FAIL(error.what());
  } catch (const std::invalid_argument& e) {
    // Invalid argument means we hit a conversion not supported by
    // external/date. Need to throw a RuntimeError so that try() statements do
    // not suppress it.
    VELOX_FAIL(e.what());
  }
  seconds_ = sysSeconds.count();
}

std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
Timestamp::toTimePointMs(bool allowOverflow) const {
  using namespace std::chrono;
  auto tp = time_point<system_clock, milliseconds>(
      milliseconds(allowOverflow ? toMillisAllowOverflow() : toMillis()));
  tz::validateRange(tp);
  return tp;
}

void Timestamp::toTimezone(const tz::TimeZone& zone) {
  try {
    seconds_ = zone.to_local(std::chrono::seconds(seconds_)).count();
  } catch (const std::invalid_argument& e) {
    // Invalid argument means we hit a conversion not supported by
    // external/date. This is a special case where we intentionally throw
    // VeloxRuntimeError to avoid it being suppressed by TRY().
    VELOX_FAIL_UNSUPPORTED_INPUT_UNCATCHABLE(e.what());
  }
}

const tz::TimeZone& Timestamp::defaultTimezone() {
  static const tz::TimeZone* kDefault = ({
    // TODO: We are hard-coding PST/PDT here to be aligned with the current
    // behavior in DWRF reader/writer.  Once they are fixed, we can use
    // date::current_zone() here.
    //
    // See https://github.com/facebookincubator/velox/issues/8127
    auto* tz = tz::locateZone("America/Los_Angeles");
    VELOX_CHECK_NOT_NULL(tz);
    tz;
  });
  return *kDefault;
}

namespace {

constexpr int kTmYearBase = 1900;
constexpr int64_t kLeapYearOffset = 4000000000ll;
constexpr int64_t kSecondsPerHour = 3600;
constexpr int64_t kSecondsPerDay = 24 * kSecondsPerHour;

inline bool isLeap(int64_t y) {
  return y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
}

inline int64_t leapThroughEndOf(int64_t y) {
  // Add a large offset to make the calculation for negative years correct.
  y += kLeapYearOffset;
  VELOX_DCHECK_GE(y, 0);
  return y / 4 - y / 100 + y / 400;
}

inline int64_t daysBetweenYears(int64_t y1, int64_t y2) {
  return 365 * (y2 - y1) + leapThroughEndOf(y2 - 1) - leapThroughEndOf(y1 - 1);
}

const int16_t daysBeforeFirstDayOfMonth[][12] = {
    {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334},
    {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335},
};

} // namespace

bool Timestamp::epochToCalendarUtc(int64_t epoch, std::tm& tm) {
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
    days -= daysBetweenYears(y, newy);
    y = newy;
  }
  y -= kTmYearBase;
  if (y > std::numeric_limits<decltype(tm.tm_year)>::max() ||
      y < std::numeric_limits<decltype(tm.tm_year)>::min()) {
    return false;
  }
  tm.tm_year = y;
  tm.tm_yday = days;
  auto* months = daysBeforeFirstDayOfMonth[leapYear];
  tm.tm_mon = std::upper_bound(months, months + 12, days) - months - 1;
  tm.tm_mday = days - months[tm.tm_mon] + 1;
  tm.tm_isdst = 0;
  return true;
}

// static
int64_t Timestamp::calendarUtcToEpoch(const std::tm& tm) {
  static_assert(sizeof(decltype(tm.tm_year)) == 4);
  // tm_year stores number of years since 1900.
  int64_t year = tm.tm_year + 1900LL;
  int64_t month = tm.tm_mon;
  if (FOLLY_UNLIKELY(month > 11)) {
    year += month / 12;
    month %= 12;
  } else if (FOLLY_UNLIKELY(month < 0)) {
    auto yearsDiff = (-month + 11) / 12;
    year -= yearsDiff;
    month += 12 * yearsDiff;
  }
  // Getting number of days since beginning of the year.
  auto dayOfYear =
      -1ll + daysBeforeFirstDayOfMonth[isLeap(year)][month] + tm.tm_mday;
  // Number of days since 1970-01-01.
  auto daysSinceEpoch = daysBetweenYears(1970, year) + dayOfYear;
  return kSecondsPerDay * daysSinceEpoch + kSecondsPerHour * tm.tm_hour +
      60ll * tm.tm_min + tm.tm_sec;
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
      epochToCalendarUtc(ts.getSeconds(), tmValue),
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
