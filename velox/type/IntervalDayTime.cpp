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
#include "velox/type/IntervalDayTime.h"
#include "velox/type/TimestampConversion.h"

namespace facebook::velox {

void parseTo(folly::StringPiece /*in*/, IntervalDayTime& /*out*/) {
  VELOX_NYI();
}

static const char* kIntervalFormat = "%d %02d:%02d:%02d.%03d";

int64_t IntervalDayTime::days() const {
  return milliseconds_ / kMillisInDay;
}

bool IntervalDayTime::hasWholeDays() const {
  return (milliseconds_ % kMillisInDay) == 0;
}

std::string IntervalDayTime::toString() const {
  int64_t remainMillis = milliseconds_;
  const int64_t days = remainMillis / kMillisInDay;
  remainMillis -= days * kMillisInDay;
  const int64_t hours = remainMillis / kMillisInHour;
  remainMillis -= hours * kMillisInHour;
  const int64_t minutes = remainMillis / kMillisInMinute;
  remainMillis -= minutes * kMillisInMinute;
  const int64_t seconds = remainMillis / kMillisInSecond;
  remainMillis -= seconds * kMillisInSecond;

  char buf[64];
  snprintf(
      buf,
      sizeof(buf),
      kIntervalFormat,
      days,
      hours,
      minutes,
      seconds,
      remainMillis);

  return buf;
}

} // namespace facebook::velox

namespace std {
std::string to_string(const ::facebook::velox::IntervalDayTime& interval) {
  return interval.toString();
}

} // namespace std
