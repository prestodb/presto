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

#include <atomic>
#include <vector>

namespace facebook::presto {

/// Used to keep track of the system's CPU usage.
class CPUMon {
 public:
  /// Call this periodically to update the CPU load. Not thread-safe.
  void update();

  /// Returns the current (latest) CPU load. Thread-safe.
  inline double getCPULoadPct() {
    return cpuLoadPct_.load();
  }

 private:
  std::vector<uint64_t> prev_{8};
  bool firstTime_{true};
  std::atomic<double> cpuLoadPct_{0.0};
};

} // namespace facebook::presto
