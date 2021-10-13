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

namespace facebook::velox {

class MicrosecondTimer {
 public:
  MicrosecondTimer(uint64_t* timer) : timer_(timer) {
    start_ = std::chrono::steady_clock::now();
  }

  ~MicrosecondTimer() {
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start_);

    (*timer_) += duration.count();
  }

 private:
  std::chrono::steady_clock::time_point start_;
  uint64_t* timer_;
};

// Returns the current epoch time in milliseconds.
size_t getCurrentTimeMs();

// Returns the current epoch time in microseconds.
size_t getCurrentTimeMicro();

} // namespace facebook::velox
