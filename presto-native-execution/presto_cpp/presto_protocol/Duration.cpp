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
#include "presto_cpp/presto_protocol/Duration.h"

namespace facebook::presto::protocol {

Duration::Duration(const std::string& duration) {
  static const RE2 kPattern(R"(^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$)");

  std::string unit;
  if (!RE2::FullMatch(duration, kPattern, &value_, &unit)) {
    throw DurationStringInvalid(duration);
  }

  timeUnit_ = valueOfTimeUnit(unit);
}

std::string Duration::toString() const {
  char buffer[32];
  snprintf(
      buffer,
      sizeof(buffer),
      "%.2f%s",
      value_,
      timeUnitToString(timeUnit_).c_str());
  return std::string(buffer);
}

double Duration::toMillisPerTimeUnit(TimeUnit timeUnit) {
  switch (timeUnit) {
    case TimeUnit::NANOSECONDS:
      return 1.0 / 1000000.0;
    case TimeUnit::MICROSECONDS:
      return 1.0 / 1000.0;
    case TimeUnit::MILLISECONDS:
      return 1;
    case TimeUnit::SECONDS:
      return 1000;
    case TimeUnit::MINUTES:
      return 1000 * 60;
    case TimeUnit::HOURS:
      return 1000 * 60 * 60;
    case TimeUnit::DAYS:
      return 1000 * 60 * 60 * 24;
    default:
      throw DurationTimeUnitUnsupported();
  }
}

TimeUnit Duration::valueOfTimeUnit(const std::string& timeUnitString) const {
  if (timeUnitString == "ns") {
    return TimeUnit::NANOSECONDS;
  }
  if (timeUnitString == "us") {
    return TimeUnit::MICROSECONDS;
  }
  if (timeUnitString == "ms") {
    return TimeUnit::MILLISECONDS;
  }
  if (timeUnitString == "s") {
    return TimeUnit::SECONDS;
  }
  if (timeUnitString == "m") {
    return TimeUnit::MINUTES;
  }
  if (timeUnitString == "h") {
    return TimeUnit::HOURS;
  }
  if (timeUnitString == "d") {
    return TimeUnit::DAYS;
  }
  throw DurationTimeUnitUnsupported();
}

std::string Duration::timeUnitToString(TimeUnit timeUnit) const {
  switch (timeUnit) {
    case TimeUnit::NANOSECONDS:
      return "ns";
    case TimeUnit::MICROSECONDS:
      return "us";
    case TimeUnit::MILLISECONDS:
      return "ms";
    case TimeUnit::SECONDS:
      return "s";
    case TimeUnit::MINUTES:
      return "m";
    case TimeUnit::HOURS:
      return "h";
    case TimeUnit::DAYS:
      return "d";
    default:
      throw DurationTimeUnitUnsupported();
  }
}

} // namespace facebook::presto::protocol
