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
#include "velox/type/Date.h"

namespace facebook::velox {

void parseTo(folly::StringPiece in, Date& out) {
  VELOX_NYI();
}

std::string Date::toString() const {
  // Find the number of seconds for the days_;
  int64_t daySeconds = days_ * 86400;
  auto tmValue = gmtime((const time_t*)&daySeconds);
  if (!tmValue) {
    VELOX_FAIL("Can't convert days to time: {}", days_);
  }

  // return ISO 8601 time format.
  // %F - equivalent to "%Y-%m-%d" (the ISO 8601 date format)
  std::ostringstream oss;
  oss << std::put_time(tmValue, "%F");
  return oss.str();
}

} // namespace facebook::velox

namespace std {
std::string to_string(const ::facebook::velox::Date& date) {
  return date.toString();
}

} // namespace std
