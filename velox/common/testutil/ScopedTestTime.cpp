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
std::optional<uint64_t> ScopedTestTime::testTimeNs_ = {};

ScopedTestTime::ScopedTestTime() {
#ifndef NDEBUG
  VELOX_CHECK(!enabled_, "Only one ScopedTestTime can be active at a time");
  enabled_ = true;
#else
  VELOX_UNREACHABLE("ScopedTestTime should only be used in debug mode");
#endif
}

ScopedTestTime::~ScopedTestTime() {
  testTimeNs_.reset();
  enabled_ = false;
}

void ScopedTestTime::setCurrentTestTimeSec(uint64_t currentTimeSec) {
  setCurrentTestTimeNano(currentTimeSec * 1'000'000'000UL);
}

void ScopedTestTime::setCurrentTestTimeMs(uint64_t currentTimeMs) {
  setCurrentTestTimeNano(currentTimeMs * 1'000'000UL);
}

void ScopedTestTime::setCurrentTestTimeMicro(uint64_t currentTimeUs) {
  setCurrentTestTimeNano(currentTimeUs * 1'000UL);
}

void ScopedTestTime::setCurrentTestTimeNano(uint64_t currentTimeNs) {
  testTimeNs_ = currentTimeNs;
}

std::optional<uint64_t> ScopedTestTime::getCurrentTestTimeSec() {
  return testTimeNs_.has_value()
      ? std::make_optional(*testTimeNs_ / 1'000'000'000L)
      : testTimeNs_;
}
std::optional<uint64_t> ScopedTestTime::getCurrentTestTimeMs() {
  return testTimeNs_.has_value() ? std::make_optional(*testTimeNs_ / 1000'000L)
                                 : testTimeNs_;
}

std::optional<uint64_t> ScopedTestTime::getCurrentTestTimeMicro() {
  return testTimeNs_.has_value() ? std::make_optional(*testTimeNs_ / 1000L)
                                 : testTimeNs_;
}

std::optional<uint64_t> ScopedTestTime::getCurrentTestTimeNano() {
  return testTimeNs_;
}
} // namespace facebook::velox::common::testutil
