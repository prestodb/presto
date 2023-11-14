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

#include "velox/common/time/Timer.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::velox {

using namespace std::chrono;

#ifndef NDEBUG
bool ScopedTestTime::enabled_ = false;
std::optional<size_t> ScopedTestTime::testTimeUs_ = {};

ScopedTestTime::ScopedTestTime() {
  VELOX_CHECK(!enabled_, "Only one ScopedTestTime can be active at a time");
  enabled_ = true;
}

ScopedTestTime::~ScopedTestTime() {
  testTimeUs_.reset();
  enabled_ = false;
}

void ScopedTestTime::setCurrentTestTimeMs(size_t currentTimeMs) {
  setCurrentTestTimeMicro(currentTimeMs * 1000);
}

void ScopedTestTime::setCurrentTestTimeMicro(size_t currentTimeUs) {
  testTimeUs_ = currentTimeUs;
}

std::optional<size_t> ScopedTestTime::getCurrentTestTimeMs() {
  return testTimeUs_.has_value() ? std::make_optional(*testTimeUs_ / 1000L)
                                 : testTimeUs_;
}
std::optional<size_t> ScopedTestTime::getCurrentTestTimeMicro() {
  return testTimeUs_;
}

size_t getCurrentTimeMs() {
  return ScopedTestTime::getCurrentTestTimeMs().value_or(
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count());
}

size_t getCurrentTimeMicro() {
  return ScopedTestTime::getCurrentTestTimeMicro().value_or(
      duration_cast<microseconds>(system_clock::now().time_since_epoch())
          .count());
}
#else
size_t getCurrentTimeMs() {
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch())
      .count();
}

size_t getCurrentTimeMicro() {
  return duration_cast<microseconds>(system_clock::now().time_since_epoch())
      .count();
}
#endif

} // namespace facebook::velox
