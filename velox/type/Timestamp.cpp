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
#include "velox/type/Timestamp.h"
#include <chrono>
#include "velox/external/date/tz.h"

namespace facebook::velox {

void Timestamp::toTimezone(const date::time_zone& zone) {
  auto tp = std::chrono::time_point<std::chrono::system_clock>(
      std::chrono::seconds(seconds_));
  auto epoch = zone.to_local(tp).time_since_epoch();
  int64_t delta = seconds_ -
      std::chrono::duration_cast<std::chrono::seconds>(epoch).count();
  seconds_ += delta;
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
