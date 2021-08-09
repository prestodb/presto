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

#include <folly/chrono/Hardware.h>
#include <cstdint>

namespace facebook {
namespace velox {

class SelectivityInfo {
 public:
  void addOutput(uint64_t numOut) {
    numOut_ += numOut;
  }

  float timeToDropValue() const {
    if (numIn_ == numOut_) {
      return timeClocks_;
    }
    return timeClocks_ / static_cast<float>(numIn_ - numOut_);
  }

  bool operator<(const SelectivityInfo& right) const {
    return timeToDropValue() < right.timeToDropValue();
  }

  uint64_t numIn() const {
    return numIn_;
  }

  uint64_t numOut() const {
    return numOut_;
  }

 private:
  uint64_t numIn_ = 0;
  uint64_t numOut_ = 0;
  uint64_t timeClocks_ = 0;

  friend class SelectivityTimer;
};

class SelectivityTimer {
 public:
  SelectivityTimer(SelectivityInfo& info, uint64_t numIn)
      : startClocks_(folly::hardware_timestamp()),
        totalClocks_(&info.timeClocks_) {
    info.numIn_ += numIn;
  }

  ~SelectivityTimer() {
    *totalClocks_ += folly::hardware_timestamp() - startClocks_;
  }

  void subtract(uint64_t clocks) {
    startClocks_ += clocks;
  }

 private:
  uint64_t startClocks_;
  uint64_t* const totalClocks_;
};

} // namespace velox
} // namespace facebook
