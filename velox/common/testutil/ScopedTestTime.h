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

#include <cstdint>
#include <optional>

namespace facebook::velox::common::testutil {
// Used to override the current time for testing purposes.
class ScopedTestTime {
 public:
  ScopedTestTime();
  ~ScopedTestTime();

  void setCurrentTestTimeSec(uint64_t currentTimeSec);
  void setCurrentTestTimeMs(uint64_t currentTimeMs);
  void setCurrentTestTimeMicro(uint64_t currentTimeUs);
  void setCurrentTestTimeNano(uint64_t currentTimeNs);

  static std::optional<uint64_t> getCurrentTestTimeSec();
  static std::optional<uint64_t> getCurrentTestTimeMs();
  static std::optional<uint64_t> getCurrentTestTimeMicro();
  static std::optional<uint64_t> getCurrentTestTimeNano();

 private:
  // Used to verify only one instance of ScopedTestTime exists at a time.
  static bool enabled_;
  // The overridden value of current time only.
  static std::optional<uint64_t> testTimeNs_;
};
} // namespace facebook::velox::common::testutil
