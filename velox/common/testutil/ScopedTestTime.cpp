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

#include "velox/common/testutil/ScopedTestTime.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::common::testutil {
bool ScopedTestTime::enabled_ = false;
std::optional<size_t> ScopedTestTime::testTimeUs_ = {};

ScopedTestTime::ScopedTestTime() {
#ifndef NDEBUG
  VELOX_CHECK(!enabled_, "Only one ScopedTestTime can be active at a time");
  enabled_ = true;
#else
  VELOX_UNREACHABLE("ScopedTestTime should only be used in debug mode");
#endif
}

ScopedTestTime::~ScopedTestTime() {
  testTimeUs_.reset();
  enabled_ = false;
}

void ScopedTestTime::setCurrentTestTimeSec(size_t currentTimeSec) {
  setCurrentTestTimeMicro(currentTimeSec * 1000000);
}

void ScopedTestTime::setCurrentTestTimeMs(size_t currentTimeMs) {
  setCurrentTestTimeMicro(currentTimeMs * 1000);
}

void ScopedTestTime::setCurrentTestTimeMicro(size_t currentTimeUs) {
  testTimeUs_ = currentTimeUs;
}

std::optional<size_t> ScopedTestTime::getCurrentTestTimeSec() {
  return testTimeUs_.has_value() ? std::make_optional(*testTimeUs_ / 1000000L)
                                 : testTimeUs_;
}
std::optional<size_t> ScopedTestTime::getCurrentTestTimeMs() {
  return testTimeUs_.has_value() ? std::make_optional(*testTimeUs_ / 1000L)
                                 : testTimeUs_;
}

std::optional<size_t> ScopedTestTime::getCurrentTestTimeMicro() {
  return testTimeUs_;
}
} // namespace facebook::velox::common::testutil
