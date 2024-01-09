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

#include "velox/common/testutil/ScopedTestTime.h"

namespace facebook::velox {

using namespace std::chrono;
using common::testutil::ScopedTestTime;

#ifndef NDEBUG

size_t getCurrentTimeSec() {
  return ScopedTestTime::getCurrentTestTimeSec().value_or(
      duration_cast<seconds>(system_clock::now().time_since_epoch()).count());
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

size_t getCurrentTimeSec() {
  return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

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
