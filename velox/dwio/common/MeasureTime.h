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

#pragma once

#include <chrono>
#include <functional>
#include <optional>

namespace facebook {
namespace velox {
namespace dwio {
namespace common {

class MeasureTime {
 public:
  explicit MeasureTime(
      const std::function<void(std::chrono::high_resolution_clock::duration)>&
          callback)
      : callback_{callback},
        startTime_{std::chrono::high_resolution_clock::now()} {}

  MeasureTime(const MeasureTime&) = delete;
  MeasureTime(MeasureTime&&) = delete;
  MeasureTime& operator=(const MeasureTime&) = delete;
  MeasureTime& operator=(MeasureTime&& other) = delete;

  ~MeasureTime() {
    callback_(std::chrono::high_resolution_clock::now() - startTime_);
  }

 private:
  const std::function<void(std::chrono::high_resolution_clock::duration)>&
      callback_;
  const std::chrono::time_point<std::chrono::high_resolution_clock> startTime_;
};

// Make sure you don't pass a lambda to this function, because that will cause a
// std::function to be created on the fly (implicitly), and when we return from
// this function that std::function won't exist anymore. So when MeasureTime is
// destroyed, it will try to access a non-existing std::function.
inline std::optional<MeasureTime> measureTimeIfCallback(
    const std::function<void(std::chrono::high_resolution_clock::duration)>&
        callback) {
  if (callback) {
    return std::make_optional<MeasureTime>(callback);
  }
  return std::nullopt;
}

} // namespace common
} // namespace dwio
} // namespace velox
} // namespace facebook
