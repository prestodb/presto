/*
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
#pragma once
#include <re2/re2.h>
#include <chrono>

namespace facebook::presto::protocol {

enum class TimeUnit {
  NANOSECONDS,
  MICROSECONDS,
  MILLISECONDS,
  SECONDS,
  MINUTES,
  HOURS,
  DAYS
};

class DurationTimeUnitUnsupported : public std::invalid_argument {
 public:
  DurationTimeUnitUnsupported()
      : std::invalid_argument("Unsupported time unit") {}
};

class DurationStringInvalid : public std::invalid_argument {
 public:
  explicit DurationStringInvalid(const std::string& duration)
      : std::invalid_argument("Invalid duration string: " + duration) {}
};

class Duration {
 public:
  Duration() = default;

  Duration(double value, TimeUnit timeUnit)
      : value_(value), timeUnit_(timeUnit) {}

  explicit Duration(const std::string& string);

  double getValue() const {
    return value_;
  }

  TimeUnit getTimeUnit() const {
    return timeUnit_;
  }

  std::string toString() const;

  double getValue(TimeUnit timeUnit) const {
    return value_ *
        (toMillisPerTimeUnit(timeUnit_) / toMillisPerTimeUnit(timeUnit));
  }

  static double toMillisPerTimeUnit(TimeUnit timeUnit);

  TimeUnit valueOfTimeUnit(const std::string& timeUnitString) const;
  std::string timeUnitToString(TimeUnit timeUnit) const;

  template <typename T>
  T asChronoDuration() const {
    if (std::is_same<T, std::chrono::nanoseconds>::value) {
      return T((size_t)getValue(TimeUnit::NANOSECONDS));
    } else if (std::is_same<T, std::chrono::microseconds>::value) {
      return T((size_t)getValue(TimeUnit::MICROSECONDS));
    } else if (std::is_same<T, std::chrono::milliseconds>::value) {
      return T((size_t)getValue(TimeUnit::MILLISECONDS));
    } else if (std::is_same<T, std::chrono::seconds>::value) {
      return T((size_t)getValue(TimeUnit::SECONDS));
    } else if (std::is_same<T, std::chrono::minutes>::value) {
      return T((size_t)getValue(TimeUnit::MINUTES));
    } else if (std::is_same<T, std::chrono::hours>::value) {
      return T((size_t)getValue(TimeUnit::HOURS));
    } else {
      throw DurationTimeUnitUnsupported();
    }
  }

  bool operator==(const Duration& other) const {
    return value_ == other.value_ && timeUnit_ == other.timeUnit_;
  }

 private:
  double value_ = 0;
  TimeUnit timeUnit_ = TimeUnit::SECONDS;
};

} // namespace facebook::presto::protocol
