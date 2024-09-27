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

#include "velox/common/time/CpuWallTimer.h"

namespace facebook::velox {

CpuWallTimer::CpuWallTimer(CpuWallTiming& timing) : timing_(timing) {
  ++timing_.count;
  cpuTimeStart_ = process::threadCpuNanos();
  wallTimeStart_ = std::chrono::steady_clock::now();
}

CpuWallTimer::~CpuWallTimer() {
  timing_.cpuNanos += process::threadCpuNanos() - cpuTimeStart_;
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - wallTimeStart_);
  timing_.wallNanos += duration.count();
}

} // namespace facebook::velox
